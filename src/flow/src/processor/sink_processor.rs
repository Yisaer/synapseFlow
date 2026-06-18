//! SinkProcessor - routes collections to SinkConnectors and forwards results.
use crate::connector::SinkConnector;
use crate::model::Collection;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, EncodedDeliveryFlags, Processor, ProcessorError, ProcessorStart, ProcessorStats,
    StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

const SINK_READY_TIMEOUT: Duration = Duration::from_secs(2);

enum ControlAction {
    Continue,
    Deactivate,
    Stop,
}

struct SinkForwarding<'a> {
    forward_data: bool,
    output: &'a LinkOutput<StreamData>,
    data_channel_capacity: usize,
}

struct ConnectorBinding {
    connector: Box<dyn SinkConnector>,
    active_delivery: bool,
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
        if self.active_delivery {
            self.connector.abort_delivery().await;
            self.active_delivery = false;
        }
        self.connector
            .close()
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    async fn handle_delivery(
        &mut self,
        flags: EncodedDeliveryFlags,
        bytes: &[u8],
    ) -> Result<bool, ProcessorError> {
        if flags.contains(EncodedDeliveryFlags::ABORT) {
            self.connector.abort_delivery().await;
            self.active_delivery = false;
            return Ok(false);
        }

        if flags.contains(EncodedDeliveryFlags::START) {
            if self.active_delivery {
                self.connector.abort_delivery().await;
                self.active_delivery = false;
                return Err(ProcessorError::ProcessingError(
                    "encoded delivery protocol error: START received while delivery is active"
                        .to_string(),
                ));
            }
            self.connector
                .start_delivery()
                .await
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
            self.active_delivery = true;
        }

        if !bytes.is_empty() {
            if !self.active_delivery {
                return Err(ProcessorError::ProcessingError(
                    "encoded delivery protocol error: chunk received without active delivery"
                        .to_string(),
                ));
            }
            if let Err(err) = self.connector.write_chunk(bytes).await {
                self.connector.abort_delivery().await;
                self.active_delivery = false;
                return Err(ProcessorError::ProcessingError(err.to_string()));
            }
        }

        if flags.contains(EncodedDeliveryFlags::END) {
            if !self.active_delivery {
                return Err(ProcessorError::ProcessingError(
                    "encoded delivery protocol error: END received without active delivery"
                        .to_string(),
                ));
            }
            match self.connector.finish_delivery().await {
                Ok(_) => {
                    self.active_delivery = false;
                    return Ok(true);
                }
                Err(err) => {
                    self.connector.abort_delivery().await;
                    self.active_delivery = false;
                    return Err(ProcessorError::ProcessingError(err.to_string()));
                }
            }
        }

        Ok(false)
    }
}

/// Processor that fans out collections to registered sink connectors.
///
/// The processor exposes a single logical input/output so it can sit between
/// the PhysicalPlan root and the result collector. Every `Collection` routed
/// through it is encoded and delivered to each connector binding.
pub struct SinkProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    connector: Option<ConnectorBinding>,
    forward_to_result: bool,
    stats: Arc<ProcessorStats>,
}

impl SinkProcessor {
    /// Create a new sink processor with the provided identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self::new_with_channel_capacities(id, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
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
        self.connector = Some(ConnectorBinding {
            connector,
            active_delivery: false,
        });
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

    async fn attempt_ready_with_timeout(
        connector: &mut ConnectorBinding,
        timeout_duration: Duration,
    ) -> Result<(), String> {
        match timeout(timeout_duration, connector.ready()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.to_string()),
            Err(_) => Err(format!(
                "sink connector ready timeout after {:?}",
                timeout_duration
            )),
        }
    }

    async fn handle_control_item(
        processor_id: &str,
        control_output: &LinkOutput<ControlSignal>,
        control_channel_capacity: usize,
        connector: &mut ConnectorBinding,
        item: Option<Result<ControlSignal, BroadcastStreamRecvError>>,
    ) -> Result<ControlAction, ProcessorError> {
        let Some(Ok(control_signal)) = item else {
            return Ok(ControlAction::Deactivate);
        };
        let is_terminal = control_signal.is_terminal();
        send_control_with_backpressure(control_output, control_channel_capacity, control_signal)
            .await?;
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
        connector: &mut ConnectorBinding,
        forwarding: SinkForwarding<'_>,
        data: StreamData,
    ) -> Result<bool, ProcessorError> {
        log_received_data(processor_id, &data);
        if let Some(rows) = data.num_rows_hint() {
            stats.record_in(rows);
        }
        match data {
            StreamData::EncodedDelivery { flags, bytes } => {
                let handle_start = std::time::Instant::now();
                match connector.handle_delivery(flags, bytes.as_ref()).await {
                    Ok(completed) => {
                        if completed {
                            stats.record_out(1);
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            processor_id = %processor_id,
                            error = %err,
                            "encoded delivery handling error"
                        );
                        stats.record_handle_duration(handle_start.elapsed());
                        stats.record_error(err.to_string());
                        return Ok(false);
                    }
                }
                if forwarding.forward_data {
                    let send_res = send_with_backpressure(
                        forwarding.output,
                        forwarding.data_channel_capacity,
                        StreamData::EncodedDelivery { flags, bytes },
                        Some(stats),
                    )
                    .await;
                    // For synchronous processors, handle duration includes downstream send/backpressure time.
                    stats.record_handle_duration(handle_start.elapsed());
                    send_res?;
                } else {
                    // For synchronous processors, handle duration includes downstream send/backpressure time.
                    stats.record_handle_duration(handle_start.elapsed());
                }
            }
            StreamData::Collection(collection) => {
                let in_rows = collection.num_rows() as u64;
                let handle_start = std::time::Instant::now();
                if let Err(err) = Self::handle_collection(connector, collection.as_ref()).await {
                    tracing::error!(
                        processor_id = %processor_id,
                        error = %err,
                        "collection handling error"
                    );
                    stats.record_handle_duration(handle_start.elapsed());
                    stats.record_error(err.to_string());
                    return Ok(false);
                }
                stats.record_out(in_rows);
                if forwarding.forward_data {
                    let send_res = send_with_backpressure(
                        forwarding.output,
                        forwarding.data_channel_capacity,
                        StreamData::Collection(collection),
                        Some(stats),
                    )
                    .await;
                    // For synchronous processors, handle duration includes downstream send/backpressure time.
                    stats.record_handle_duration(handle_start.elapsed());
                    send_res?;
                } else {
                    // For synchronous processors, handle duration includes downstream send/backpressure time.
                    stats.record_handle_duration(handle_start.elapsed());
                }
            }
            data => {
                let is_terminal = data.is_terminal();
                send_with_backpressure(
                    forwarding.output,
                    forwarding.data_channel_capacity,
                    data,
                    Some(stats),
                )
                .await?;
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

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let forward_data = self.forward_to_result;
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);

        let Some(mut connector) = self.connector.take() else {
            return ProcessorStart::failed(
                spawner,
                ProcessorError::InvalidConfiguration("sink connector missing".to_string()),
            );
        };
        let processor_id = self.id.clone();
        tracing::info!(processor_id = %processor_id, "sink processor starting");

        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = spawner.spawn(async move {
            match Self::attempt_ready_with_timeout(&mut connector, SINK_READY_TIMEOUT).await {
                Ok(()) => {
                    tracing::info!(processor_id = %processor_id, "sink connector ready");
                    let _ = ready_tx.send(Ok(()));
                }
                Err(message) => {
                    tracing::warn!(
                        processor_id = %processor_id,
                        error = %message,
                        "sink connector ready error"
                    );
                    stats.record_error(message.clone());
                    let err = ProcessorError::ProcessingError(message);
                    let _ = ready_tx.send(Err(err.clone()));
                    return Err(err);
                }
            }
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match Self::handle_control_item(
                            &processor_id,
                            &control_output,
                            channel_capacities.control,
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
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                if Self::handle_input_item(
                                    &processor_id,
                                    stats.as_ref(),
                                    &mut connector,
                                    SinkForwarding {
                                        forward_data,
                                        output: &output,
                                        data_channel_capacity: channel_capacities.data,
                                    },
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
        });
        ProcessorStart::with_ready(handle, ready_rx)
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}
