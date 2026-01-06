//! SinkProcessor - routes collections to SinkConnectors and forwards results.
use crate::connector::SinkConnector;
use crate::model::Collection;
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

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

static SINK_RECORDS_IN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "sink_processor_records_in_total",
        "Rows received by sink processors",
        &["processor"]
    )
    .expect("create sink records_in counter vec")
});

static SINK_RECORDS_OUT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "sink_processor_records_out_total",
        "Rows forwarded downstream by sink processors",
        &["processor"]
    )
    .expect("create sink records_out counter vec")
});

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
        processor_id: &str,
        connector: &mut ConnectorBinding,
        payload: &[u8],
        row_count: u64,
    ) -> Result<(), ProcessorError> {
        SINK_RECORDS_IN
            .with_label_values(&[processor_id])
            .inc_by(row_count);
        connector.publish(payload).await?;

        SINK_RECORDS_OUT
            .with_label_values(&[processor_id])
            .inc_by(row_count);
        Ok(())
    }

    async fn handle_collection(
        processor_id: &str,
        connector: &mut ConnectorBinding,
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        let row_count = collection.num_rows() as u64;
        SINK_RECORDS_IN
            .with_label_values(&[processor_id])
            .inc_by(row_count);
        connector.publish_collection(collection).await?;
        SINK_RECORDS_OUT
            .with_label_values(&[processor_id])
            .inc_by(row_count);
        Ok(())
    }

    async fn handle_terminal(connector: &mut ConnectorBinding) -> Result<(), ProcessorError> {
        connector.close().await
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
            connector.ready().await?;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                Self::handle_terminal(&mut connector).await?;
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::EncodedBytes { payload, num_rows } => {
                                        let rows = num_rows;
                                        if let Err(err) = Self::handle_payload(
                                            &processor_id,
                                            &mut connector,
                                            payload.as_ref(),
                                            rows,
                                        )
                                        .await
                                        {
                                            tracing::error!(processor_id = %processor_id, error = %err, "payload handling error");
                                            forward_error(&output, &processor_id, err.to_string())
                                                .await?;
                                            continue;
                                        }
                                        stats.record_out(1);

                                        if forward_data {
                                            send_with_backpressure(
                                                &output,
                                                StreamData::EncodedBytes { payload, num_rows },
                                            )
                                            .await?;
                                        }
                                    }
                                    StreamData::Collection(collection) => {
                                        let in_rows = collection.num_rows() as u64;
                                        if let Err(err) = Self::handle_collection(
                                            &processor_id,
                                            &mut connector,
                                            collection.as_ref(),
                                        )
                                        .await
                                        {
                                            tracing::error!(processor_id = %processor_id, error = %err, "collection handling error");
                                            forward_error(&output, &processor_id, err.to_string())
                                                .await?;
                                            continue;
                                        }
                                        stats.record_out(in_rows);

                                        if forward_data {
                                            send_with_backpressure(
                                                &output,
                                                StreamData::Collection(collection),
                                            )
                                            .await?;
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        send_with_backpressure(&output, data).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            Self::handle_terminal(&mut connector).await?;
                                            tracing::info!(processor_id = %processor_id, "stopped");
                                            return Ok(());
                                        }
                                    }
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
