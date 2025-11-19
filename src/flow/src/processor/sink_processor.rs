//! SinkProcessor - routes collections to SinkConnectors and forwards results.

use crate::codec::encoder::CollectionEncoder;
use crate::connector::SinkConnector;
use crate::model::Collection;
use crate::processor::base::{fan_in_streams, DEFAULT_CHANNEL_CAPACITY};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

struct ConnectorBinding {
    connector: Box<dyn SinkConnector>,
    encoder: Arc<dyn CollectionEncoder>,
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

    async fn publish(&mut self, collection: &dyn Collection) -> Result<(), ProcessorError> {
        let payload = self
            .encoder
            .encode(collection)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

        self.connector
            .send(&payload)
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

        Ok(())
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
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<StreamData>,
    connectors: Vec<ConnectorBinding>,
    forward_to_result: bool,
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
            connectors: Vec::new(),
            forward_to_result: false,
        }
    }

    /// Enable forwarding collections/control signals to downstream consumers (tests).
    pub fn enable_result_forwarding(&mut self) {
        self.forward_to_result = true;
    }

    /// Disable forwarding to downstream consumers (default for production).
    pub fn disable_result_forwarding(&mut self) {
        self.forward_to_result = false;
    }

    /// Register a connector + encoder pair.
    pub fn add_connector(
        &mut self,
        connector: Box<dyn SinkConnector>,
        encoder: Arc<dyn CollectionEncoder>,
    ) {
        self.connectors
            .push(ConnectorBinding { connector, encoder });
    }

    async fn handle_collection(
        processor_id: &str,
        connectors: &mut [ConnectorBinding],
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        let row_count = collection.num_rows() as u64;
        SINK_RECORDS_IN
            .with_label_values(&[processor_id])
            .inc_by(row_count);
        for connector in connectors.iter_mut() {
            connector.publish(collection).await?;
        }

        SINK_RECORDS_OUT
            .with_label_values(&[processor_id])
            .inc_by(row_count);
        Ok(())
    }

    async fn handle_terminal(connectors: &mut [ConnectorBinding]) -> Result<(), ProcessorError> {
        for connector in connectors.iter_mut() {
            connector.close().await?;
        }
        Ok(())
    }
}

impl Processor for SinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = if self.forward_to_result {
            Some(self.output.clone())
        } else {
            None
        };
        let control_output = self.control_output.clone();

        let mut connectors = std::mem::take(&mut self.connectors);
        let processor_id = self.id.clone();

        tokio::spawn(async move {
            for binding in connectors.iter_mut() {
                binding.ready().await?;
            }
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(result) = control_item {
                            let control_data = match result {
                                Ok(data) => data,
                                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                                    return Err(ProcessorError::ProcessingError(format!(
                                        "SinkProcessor control input lagged by {} messages",
                                        skipped
                                    )))
                                }
                            };
                            let is_terminal = control_data.is_terminal();
                            let _ = control_output.send(control_data);
                            if is_terminal {
                                println!("[SinkProcessor:{processor_id}] received StreamEnd (control)");
                                Self::handle_terminal(&mut connectors).await?;
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                if let Err(err) =
                                    Self::handle_collection(&processor_id, &mut connectors, collection.as_ref()).await
                                {
                                    if let Some(output_sender) = &output {
                                        let error = StreamData::error(
                                            StreamError::new(err.to_string()).with_source(processor_id.clone()),
                                        );
                                        output_sender
                                            .send(error)
                                            .map_err(|_| ProcessorError::ChannelClosed)?;
                                    }
                                    return Err(err);
                                }

                                if let Some(output_sender) = &output {
                                    output_sender
                                        .send(StreamData::collection(collection))
                                        .map_err(|_| ProcessorError::ChannelClosed)?;
                                }
                            }
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                if let Some(output_sender) = &output {
                                    output_sender
                                        .send(data)
                                        .map_err(|_| ProcessorError::ChannelClosed)?;
                                }

                                if is_terminal {
                                    println!("[SinkProcessor:{processor_id}] received StreamEnd (data)");
                                    Self::handle_terminal(&mut connectors).await?;
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                return Err(ProcessorError::ProcessingError(format!(
                                    "SinkProcessor input lagged by {} messages",
                                    skipped
                                )))
                            }
                            None => {
                                Self::handle_terminal(&mut connectors).await?;
                                return Ok(());
                            }
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        if self.forward_to_result {
            Some(self.output.subscribe())
        } else {
            None
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.control_inputs.push(receiver);
    }
}
