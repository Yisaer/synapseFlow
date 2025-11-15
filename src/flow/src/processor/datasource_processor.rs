//! DataSourceProcessor - processes data from PhysicalDatasource
//!
//! This processor reads data from a PhysicalDatasource and sends it downstream
//! as StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::connector::{ConnectorEvent, SourceConnector};
use crate::model::{Collection, RecordBatch};
use crate::processor::base::{fan_in_streams, DEFAULT_CHANNEL_CAPACITY};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
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
    control_inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Broadcast channel for downstream consumers
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<StreamData>,
    /// External source connectors that feed this processor
    connectors: Vec<ConnectorBinding>,
}

static DATASOURCE_RECORDS_IN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "datasource_processor_records_in_total",
        "Rows received by datasource processors",
        &["processor"]
    )
    .expect("create datasource records_in counter vec")
});

static DATASOURCE_RECORDS_OUT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "datasource_processor_records_out_total",
        "Rows emitted by datasource processors",
        &["processor"]
    )
    .expect("create datasource records_out counter vec")
});

struct ConnectorBinding {
    connector: Box<dyn SourceConnector>,
    decoder: Arc<dyn RecordDecoder>,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(source_name: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            source_name: source_name.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
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

    fn rewrite_collection_sources(
        collection: Arc<dyn Collection>,
        source_name: &str,
    ) -> Result<Arc<dyn Collection>, ProcessorError> {
        let mut rows = collection.rows().to_vec();
        for tuple in rows.iter_mut() {
            tuple.source_name = source_name.to_string();
            for (src, _) in tuple.columns.iter_mut() {
                *src = source_name.to_string();
            }
        }
        Ok(Arc::new(RecordBatch::from_rows(rows)))
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.source_name
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.source_name.clone();
        let mut base_inputs = std::mem::take(&mut self.inputs);
        base_inputs.extend(self.activate_connectors());
        let mut input_streams = fan_in_streams(base_inputs);
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    control_item = control_streams.next(), if control_active => {
                        if let Some(result) = control_item {
                            let control_data = match result {
                                Ok(data) => data,
                                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                                    return Err(ProcessorError::ProcessingError(format!(
                                        "DataSource control input lagged by {} messages",
                                        skipped
                                    )))
                                }
                            };
                            let _ = control_output.send(control_data.clone());
                            if control_data.is_terminal() {
                                println!("[DataSourceProcessor:{}] received StreamEnd (control)", processor_id);
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(mut data)) => {
                                if let StreamData::Collection(collection) = data {
                                    let rows = collection.num_rows() as u64;
                                    DATASOURCE_RECORDS_IN
                                        .with_label_values(&[processor_id.as_str()])
                                        .inc_by(rows);
                                    let renamed =
                                        DataSourceProcessor::rewrite_collection_sources(collection, &processor_id)?;
                                    DATASOURCE_RECORDS_OUT
                                        .with_label_values(&[processor_id.as_str()])
                                        .inc_by(rows);
                                    data = StreamData::Collection(renamed);
                                }
                                output
                                    .send(data.clone())
                                    .map_err(|_| ProcessorError::ChannelClosed)?;

                                if data.is_control() && data.is_terminal() {
                                    println!("[DataSourceProcessor:{}] received StreamEnd (data)", processor_id);
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                return Err(ProcessorError::ProcessingError(format!(
                                    "DataSource input lagged by {} messages",
                                    skipped
                                )))
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
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
