//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::connector::{ConnectorEvent, SourceConnector};
use crate::processor::base::{fan_in_streams, DEFAULT_CHANNEL_CAPACITY};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier
    source_name: String,
    /// Input channels for receiving control signals
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for downstream consumers
    output: broadcast::Sender<StreamData>,
    /// External source connectors that feed this processor
    connectors: Vec<ConnectorBinding>,
}

struct ConnectorBinding {
    connector: Box<dyn SourceConnector>,
    decoder: Arc<dyn RecordDecoder>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(source_name: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            output,
            connectors: Vec::new(),
        }
    }

    /// Register an external source connector and its decoder.
    pub fn add_connector(
        &mut self,
        connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
    ) {
        self.connectors
            .push(ConnectorBinding { connector, decoder });
    }

    fn activate_connectors(&mut self) -> Vec<broadcast::Receiver<StreamData>> {
        let mut receivers = Vec::new();
        for binding in std::mem::take(&mut self.connectors) {
            let (sender, receiver) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
            Self::spawn_connector_task(
                binding.connector,
                binding.decoder,
                sender,
                self.source_name.clone(),
            );
            receivers.push(receiver);
        }
        receivers
    }

    fn spawn_connector_task(
        mut connector: Box<dyn SourceConnector>,
        decoder: Arc<dyn RecordDecoder>,
        sender: broadcast::Sender<StreamData>,
        processor_id: String,
    ) {
        tokio::spawn(async move {
            let mut stream = match connector.subscribe() {
                Ok(stream) => stream,
                Err(err) => {
                    let _ = sender.send(StreamData::error(
                        StreamError::new(format!("connector subscribe error: {}", err))
                            .with_source(processor_id.clone()),
                    ));
                    return;
                }
            };

            while let Some(event) = stream.next().await {
                match event {
                    Ok(ConnectorEvent::Payload(bytes)) => match decoder.decode(&bytes) {
                        Ok(batch) => {
                            println!(
                                "[DataSourceProcessor:{}] received collection with {} rows",
                                processor_id,
                                batch.num_rows()
                            );
                            if sender
                                .send(StreamData::collection(Box::new(batch)))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = sender.send(StreamData::error(
                                StreamError::new(format!("decode error: {}", err))
                                    .with_source(processor_id.clone()),
                            ));
                        }
                    },
                    Ok(ConnectorEvent::EndOfStream) => break,
                    Err(err) => {
                        let _ = sender.send(StreamData::error(
                            StreamError::new(format!("connector error: {}", err))
                                .with_source(processor_id.clone()),
                        ));
                    }
                }
            }
        });
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.source_name
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let output = self.output.clone();
        let mut base_inputs = std::mem::take(&mut self.inputs);
        base_inputs.extend(self.activate_connectors());
        let mut input_streams = fan_in_streams(base_inputs);

        tokio::spawn(async move {
            while let Some(item) = input_streams.next().await {
                let data = match item {
                    Ok(data) => data,
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        return Err(ProcessorError::ProcessingError(format!(
                            "DataSource input lagged by {} messages",
                            skipped
                        )))
                    }
                };
                output
                    .send(data.clone())
                    .map_err(|_| ProcessorError::ChannelClosed)?;

                if matches!(
                    data.as_control(),
                    Some(crate::processor::ControlSignal::StreamEnd)
                ) {
                    return Ok(());
                }
            }

            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
}
