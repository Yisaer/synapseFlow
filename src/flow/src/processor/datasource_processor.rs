//! DataSourceProcessor - reads raw payloads from connectors.
//!
//! This processor reads data from a data source and sends it downstream as
//! `StreamData::Bytes`. Decoding is handled by a dedicated decoder processor.

use crate::connector::{ConnectorError, ConnectorEvent, SourceConnector};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData, StreamError};
use datatypes::Schema;
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier (`datasource_{plan_index}` for plan-based processors)
    id: String,
    plan_index: Option<i64>,
    stream_name: String,
    schema: Arc<Schema>,
    /// Input channels for receiving control signals
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Broadcast channel for downstream consumers
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
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
    handle: Option<JoinHandle<()>>,
}

impl ConnectorBinding {
    fn activate(&mut self, processor_id: &str) -> broadcast::Receiver<StreamData> {
        let (sender, receiver) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let processor_id = processor_id.to_string();
        let connector_id = self.connector.id().to_string();
        let mut stream = match self.connector.subscribe() {
            Ok(stream) => stream,
            Err(err) => {
                let _ = sender.send(StreamData::error(
                    StreamError::new(format!("connector subscribe error: {}", err))
                        .with_source(processor_id.clone()),
                ));
                return receiver;
            }
        };

        tracing::info!(
            processor_id = %processor_id,
            connector_id = %connector_id,
            "data source connector starting"
        );
        let sender_clone = sender.clone();
        self.handle = Some(tokio::spawn(async move {
            let sender = sender_clone;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ConnectorEvent::Payload(bytes)) => {
                        if send_with_backpressure(&sender, StreamData::bytes(bytes))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(ConnectorEvent::EndOfStream) => break,
                    Err(err) => {
                        if send_with_backpressure(
                            &sender,
                            StreamData::error(
                                StreamError::new(format!("connector error: {}", err))
                                    .with_source(processor_id.clone()),
                            ),
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            tracing::info!(
                processor_id = %processor_id,
                connector_id = %connector_id,
                "data source connector stopped"
            );
        }));

        receiver
    }

    async fn shutdown(&mut self) -> Result<(), ProcessorError> {
        let connector_id = self.connector.id().to_string();
        tracing::info!(connector_id = %connector_id, "closing source connector");
        if let Err(err) = self.connector.close() {
            return Err(Self::connector_error(self.connector.id(), err));
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
            match handle.await {
                Ok(_) => {}
                Err(join_err) if join_err.is_cancelled() => {}
                Err(join_err) => {
                    return Err(ProcessorError::ProcessingError(format!(
                        "Connector join error: {}",
                        join_err
                    )));
                }
            };
        }
        tracing::info!(connector_id = %connector_id, "source connector closed");
        Ok(())
    }

    fn connector_error(id: &str, err: ConnectorError) -> ProcessorError {
        ProcessorError::ProcessingError(format!("connector `{}` error: {}", id, err))
    }
}
impl DataSourceProcessor {
    /// Create a new DataSourceProcessor from PhysicalDatasource
    pub fn new(plan_name: &str, source_name: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self::with_custom_id(
            None, // plan_index is no longer needed as we use plan_name for ID
            plan_name.to_string(),
            source_name,
            schema,
        )
    }

    pub fn with_custom_id(
        plan_index: Option<i64>,
        id: impl Into<String>,
        source_name: impl Into<String>,
        schema: Arc<Schema>,
    ) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let stream_name = source_name.into();
        Self {
            id: id.into(),
            plan_index,
            stream_name,
            schema,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            connectors: Vec::new(),
        }
    }

    /// Register an external source connector and its decoder.
    pub fn add_connector(&mut self, connector: Box<dyn SourceConnector>) {
        self.connectors.push(ConnectorBinding {
            connector,
            handle: None,
        });
    }

    fn activate_connectors(
        connectors: &mut [ConnectorBinding],
        processor_id: &str,
    ) -> Vec<broadcast::Receiver<StreamData>> {
        connectors
            .iter_mut()
            .map(|binding| binding.activate(processor_id))
            .collect()
    }

    async fn shutdown_connectors(
        connectors: &mut [ConnectorBinding],
    ) -> Result<(), ProcessorError> {
        for binding in connectors.iter_mut() {
            binding.shutdown().await?;
        }
        Ok(())
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let plan_label = self
            .plan_index
            .map(|idx| idx.to_string())
            .unwrap_or_else(|| "global".to_string());
        let stream_name = self.stream_name.clone();
        let mut base_inputs = std::mem::take(&mut self.inputs);
        let mut connectors = std::mem::take(&mut self.connectors);
        let connector_inputs = Self::activate_connectors(&mut connectors, &processor_id);
        base_inputs.extend(connector_inputs);
        let mut input_streams = fan_in_streams(base_inputs);
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        tracing::info!(
            processor_id = %processor_id,
            plan = %plan_label,
            stream = %stream_name,
            "data source starting"
        );
        tokio::spawn(async move {
            let mut connectors = connectors;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                tracing::info!(
                                    processor_id = %processor_id,
                                    plan = %plan_label,
                                    stream = %stream_name,
                                    "received StreamEnd (control)"
                                );
                                Self::shutdown_connectors(&mut connectors).await?;
                                tracing::info!(
                                    processor_id = %processor_id,
                                    plan = %plan_label,
                                    stream = %stream_name,
                                    "stopped"
                                );
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
                                if let Some(collection) = data.as_collection() {
                                    let rows = collection.num_rows() as u64;
                                    DATASOURCE_RECORDS_IN
                                        .with_label_values(&[processor_id.as_str()])
                                        .inc_by(rows);
                                    DATASOURCE_RECORDS_OUT
                                        .with_label_values(&[processor_id.as_str()])
                                        .inc_by(rows);
                                }
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;

                                if is_terminal {
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        plan = %plan_label,
                                        stream = %stream_name,
                                        "received StreamEnd (data)"
                                    );
                                    Self::shutdown_connectors(&mut connectors).await?;
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        plan = %plan_label,
                                        stream = %stream_name,
                                        "stopped"
                                    );
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "DataSource input lagged by {} messages",
                                    skipped
                                );
                                tracing::warn!(
                                    processor_id = %processor_id,
                                    plan = %plan_label,
                                    stream = %stream_name,
                                    skipped = skipped,
                                    "input lagged"
                                );
                                forward_error(&output, &processor_id, message).await?;
                                continue;
                            }
                            None => {
                                Self::shutdown_connectors(&mut connectors).await?;
                                tracing::info!(
                                    processor_id = %processor_id,
                                    plan = %plan_label,
                                    stream = %stream_name,
                                    "stopped"
                                );
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

impl DataSourceProcessor {
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }
}
