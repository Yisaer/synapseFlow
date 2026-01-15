//! SinkProcessor - routes collections to SinkConnectors and forwards results.
use crate::connector::SinkConnector;
use crate::model::Collection;
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

const SINK_READY_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const SINK_READY_TIMEOUT: Duration = Duration::from_secs(2);

struct ReadyState {
    ready: bool,
    last_error: Option<String>,
    drop_logged: bool,
}

impl ReadyState {
    fn new() -> Self {
        Self {
            ready: false,
            last_error: None,
            drop_logged: false,
        }
    }

    fn is_ready(&self) -> bool {
        self.ready
    }

    fn mark_ready(&mut self, processor_id: &str) {
        if self.last_error.is_some() {
            tracing::info!(processor_id = %processor_id, "sink connector ready");
        }
        self.ready = true;
        self.last_error = None;
        self.drop_logged = false;
    }

    fn record_ready_error(
        &mut self,
        processor_id: &str,
        stats: &ProcessorStats,
        message: String,
    ) {
        let should_report = self
            .last_error
            .as_ref()
            .map_or(true, |prev| prev != &message);
        if should_report {
            tracing::warn!(
                processor_id = %processor_id,
                error = %message,
                "sink connector ready error"
            );
            stats.record_error(message.clone());
        }
        self.last_error = Some(message);
        self.drop_logged = false;
    }

    fn should_drop_data(&mut self, processor_id: &str, stats: &ProcessorStats) -> bool {
        if self.ready {
            return false;
        }
        if !self.drop_logged {
            let message = self
                .last_error
                .clone()
                .unwrap_or_else(|| "sink connector not ready".to_string());
            tracing::warn!(
                processor_id = %processor_id,
                error = %message,
                "sink connector not ready; dropping data"
            );
            stats.record_error(message);
            self.drop_logged = true;
        }
        true
    }
}

enum ControlAction {
    Continue,
    Deactivate,
    Stop,
}

struct ConnectorBinding {
    connector: Box<dyn SinkConnector>,
}

impl ConnectorBinding {
    async fn ready(&mut self) -> Result<(), ProcessorError> {
        self.conn_ready().await
    }

    async fn conn_ready(&mut self) -> Result<(), ProcessorError> {
        self.connector
            .ready()
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    async fn publish(&mut self, payload: &[u8]) -> Result<(), ProcessorError> {
        self.connector
            .send(payload)
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

        Ok(())
    }

    async fn publish_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        self.connector
            .send_collection(collection)
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    async fn close(&mut self) -> Result<(), ProcessorError> {
        self.connector
            .close()
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }
}

/// Processor that fans out collections to registered sink connectors.
///
/// The processor exposes a single logical input/output so it can sit between
/// the PhysicalPlan root and the result collector. Every `Collection` routed
/// through it is encoded and delivered to each connector binding.
pub struct SinkProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    connector: Option<ConnectorBinding>,
    forward_to_result: bool,
    stats: Arc<ProcessorStats>,
}

impl SinkProcessor {
    /// Create a new sink processor with the provided identifier.
    pub fn new(id: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            connector: None,
            forward_to_result: false,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    /// Enable forwarding collections/control signals to downstream consumers (tests).
    pub fn enable_result_forwarding(&mut self) {
        self.forward_to_result = true;
    }

    /// Disable forwarding to downstream consumers (default for production).
    pub fn disable_result_forwarding(&mut self) {
        self.forward_to_result = false;
    }

    /// Register a connector binding.
    pub fn add_connector(&mut self, connector: Box<dyn SinkConnector>) {
        self.connector = Some(ConnectorBinding { connector });
    }

    async fn handle_payload(
        connector: &mut ConnectorBinding,
        payload: &[u8],
    ) -> Result<(), ProcessorError> {
        connector.publish(payload).await?;
        Ok(())
    }

    async fn handle_collection(
        connector: &mut ConnectorBinding,
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        connector.publish_collection(collection).await?;
        Ok(())
    }

    async fn handle_terminal(connector: &mut ConnectorBinding) -> Result<(), ProcessorError> {
        connector.close().await
    }

    fn new_ready_interval() -> tokio::time::Interval {
        let mut interval = interval(SINK_READY_RETRY_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    }

    async fn attempt_ready(connector: &mut ConnectorBinding) -> Result<(), String> {
        match timeout(SINK_READY_TIMEOUT, connector.ready()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.to_string()),
            Err(_) => Err(format!(
                "sink connector ready timeout after {:?}",
                SINK_READY_TIMEOUT
            )),
        }
    }

    async fn handle_control_item(
        processor_id: &str,
        control_output: &broadcast::Sender<ControlSignal>,
        connector: &mut ConnectorBinding,
        item: Option<Result<ControlSignal, BroadcastStreamRecvError>>,
    ) -> Result<ControlAction, ProcessorError> {
        let Some(Ok(control_signal)) = item else {
            return Ok(ControlAction::Deactivate);
        };
        let is_terminal = control_signal.is_terminal();
        send_control_with_backpressure(control_output, control_signal).await?;
        if is_terminal {
            tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
            Self::handle_terminal(connector).await?;
            tracing::info!(processor_id = %processor_id, "stopped");
            return Ok(ControlAction::Stop);
        }
        Ok(ControlAction::Continue)
    }

    async fn handle_input_item(
        processor_id: &str,
        stats: &ProcessorStats,
        ready_state: &mut ReadyState,
        connector: &mut ConnectorBinding,
        forward_data: bool,
        output: &broadcast::Sender<StreamData>,
        data: StreamData,
    ) -> Result<bool, ProcessorError> {
        log_received_data(processor_id, &data);
        if let Some(rows) = data.num_rows_hint() {
            stats.record_in(rows);
        }
        match data {
            StreamData::EncodedBytes { payload, num_rows } => {
                if ready_state.should_drop_data(processor_id, stats) {
                    return Ok(false);
                }
                if let Err(err) = Self::handle_payload(connector, payload.as_ref()).await {
                    tracing::error!(
                        processor_id = %processor_id,
                        error = %err,
                        "payload handling error"
                    );
                    stats.record_error(err.to_string());
                    return Ok(false);
                }
                stats.record_out(1);
                if forward_data {
                    send_with_backpressure(
                        output,
                        StreamData::EncodedBytes { payload, num_rows },
                    )
                    .await?;
                }
            }
            StreamData::Collection(collection) => {
                if ready_state.should_drop_data(processor_id, stats) {
                    return Ok(false);
                }
                let in_rows = collection.num_rows() as u64;
                if let Err(err) = Self::handle_collection(connector, collection.as_ref()).await {
                    tracing::error!(
                        processor_id = %processor_id,
                        error = %err,
                        "collection handling error"
                    );
                    stats.record_error(err.to_string());
                    return Ok(false);
                }
                stats.record_out(in_rows);
                if forward_data {
                    send_with_backpressure(output, StreamData::Collection(collection)).await?;
                }
            }
            data => {
                let is_terminal = data.is_terminal();
                send_with_backpressure(output, data).await?;
                if is_terminal {
                    tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                    Self::handle_terminal(connector).await?;
                    tracing::info!(processor_id = %processor_id, "stopped");
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

impl Processor for SinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let forward_data = self.forward_to_result;
        let control_output = self.control_output.clone();
        let stats = Arc::clone(&self.stats);

        let Some(mut connector) = self.connector.take() else {
            return tokio::spawn(async {
                Err(ProcessorError::InvalidConfiguration(
                    "sink connector missing".to_string(),
                ))
            });
        };
        let processor_id = self.id.clone();
        tracing::info!(processor_id = %processor_id, "sink processor starting");

        tokio::spawn(async move {
            let mut ready_state = ReadyState::new();
            let mut ready_interval = Self::new_ready_interval();
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match Self::handle_control_item(
                            &processor_id,
                            &control_output,
                            &mut connector,
                            control_item,
                        )
                        .await?
                        {
                            ControlAction::Continue => {}
                            ControlAction::Deactivate => control_active = false,
                            ControlAction::Stop => return Ok(()),
                        };
                    }
                    _ = ready_interval.tick(), if !ready_state.is_ready() => {
                        match Self::attempt_ready(&mut connector).await {
                            Ok(()) => ready_state.mark_ready(&processor_id),
                            Err(message) => {
                                ready_state.record_ready_error(&processor_id, stats.as_ref(), message);
                            }
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                if Self::handle_input_item(
                                    &processor_id,
                                    stats.as_ref(),
                                    &mut ready_state,
                                    &mut connector,
                                    forward_data,
                                    &output,
                                    data,
                                )
                                .await?
                                {
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "sink data input");
                                continue;
                            }
                            None => {
                                Self::handle_terminal(&mut connector).await?;
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
