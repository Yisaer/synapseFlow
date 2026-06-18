//! DataSourceProcessor - reads raw payloads from connectors.
//!
//! This processor reads data from a data source and sends it downstream as
//! `StreamData::Bytes`. Decoding is handled by a dedicated decoder processor.

use crate::connector::{ConnectorError, ConnectorEvent, SourceConnector};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Schema;
use futures::{FutureExt, StreamExt};
use std::sync::Arc;
use tokio::sync::oneshot;
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
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    /// Broadcast channel for downstream consumers
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    /// External source connectors that feed this processor
    connectors: Vec<ConnectorBinding>,
    stats: Arc<ProcessorStats>,
}

struct ConnectorBinding {
    connector: Box<dyn SourceConnector>,
    handle: Option<JoinHandle<()>>,
}

impl ConnectorBinding {
    fn activate(
        &mut self,
        processor_id: &str,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
        spawner: &TaskSpawner,
    ) -> Result<LinkReceiver<StreamData>, ProcessorError> {
        let output = LinkOutput::broadcast(data_channel_capacity);
        let receiver = output.subscribe().ok_or_else(|| {
            ProcessorError::InvalidConfiguration("connector output unavailable".into())
        })?;
        let processor_id = processor_id.to_string();
        let connector_id = self.connector.id().to_string();
        let mut stream = match self.connector.subscribe() {
            Ok(stream) => stream,
            Err(err) => {
                let message = format!("connector subscribe error: {err}");
                tracing::error!(
                    processor_id = %processor_id,
                    connector_id = %connector_id,
                    error = %err,
                    "connector subscribe error"
                );
                stats.record_error(message);
                return Err(Self::connector_error(self.connector.id(), err));
            }
        };

        tracing::info!(
            processor_id = %processor_id,
            connector_id = %connector_id,
            "data source connector starting"
        );
        let sender_clone = output.clone();
        self.handle = Some(spawner.spawn(async move {
            let sender = sender_clone;
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ConnectorEvent::Payload(bytes)) => {
                        if send_with_backpressure(
                            &sender,
                            data_channel_capacity,
                            StreamData::bytes(bytes),
                            Some(stats.as_ref()),
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                    Ok(ConnectorEvent::Collection(collection)) => {
                        if send_with_backpressure(
                            &sender,
                            data_channel_capacity,
                            StreamData::collection(collection),
                            Some(stats.as_ref()),
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                    Ok(ConnectorEvent::EndOfStream) => break,
                    Err(err) => {
                        let message = format!("connector error: {err}");
                        tracing::error!(
                            processor_id = %processor_id,
                            connector_id = %connector_id,
                            error = %err,
                            "connector error"
                        );
                        stats.record_error(message);
                    }
                }
            }
            tracing::info!(
                processor_id = %processor_id,
                connector_id = %connector_id,
                "data source connector stopped"
            );
        }));

        Ok(receiver)
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
        Self::with_custom_id_and_channel_capacities(
            plan_index,
            id,
            source_name,
            schema,
            default_channel_capacities(),
        )
    }

    pub(crate) fn with_custom_id_and_channel_capacities(
        plan_index: Option<i64>,
        id: impl Into<String>,
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
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
            channel_capacities,
            connectors: Vec::new(),
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
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
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
        spawner: &TaskSpawner,
    ) -> Result<Vec<LinkReceiver<StreamData>>, ProcessorError> {
        connectors
            .iter_mut()
            .map(|binding| {
                binding.activate(
                    processor_id,
                    data_channel_capacity,
                    Arc::clone(stats),
                    spawner,
                )
            })
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

    async fn forward_data(
        processor_id: &str,
        output: &LinkOutput<StreamData>,
        channel_capacity: usize,
        stats: &ProcessorStats,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        log_received_data(processor_id, &data);
        if let Some(rows) = data.num_rows_hint() {
            stats.record_in(rows);
        }
        let out_rows = data.num_rows_hint();
        send_with_backpressure(output, channel_capacity, data, Some(stats)).await?;
        if let Some(rows) = out_rows {
            stats.record_out(rows);
        }
        Ok(())
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let plan_label = self
            .plan_index
            .map(|idx| idx.to_string())
            .unwrap_or_else(|| "global".to_string());
        let stream_name = self.stream_name.clone();
        let mut base_inputs = std::mem::take(&mut self.inputs);
        let mut connectors = std::mem::take(&mut self.connectors);
        let connector_inputs = match Self::activate_connectors(
            &mut connectors,
            &processor_id,
            channel_capacities.data,
            &stats,
            spawner,
        ) {
            Ok(inputs) => inputs,
            Err(err) => {
                return ProcessorStart::failed(spawner, err);
            }
        };
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
        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = spawner.spawn(async move {
            let _ = ready_tx.send(Ok(()));
            let mut connectors = connectors;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
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
                                if data.is_terminal() {
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        plan = %plan_label,
                                        stream = %stream_name,
                                        "received StreamEnd (data)"
                                    );
                                    Self::shutdown_connectors(&mut connectors).await?;
                                    while let Some(Some(Ok(pending))) =
                                        input_streams.next().now_or_never()
                                    {
                                        if pending.is_terminal() {
                                            continue;
                                        }
                                        Self::forward_data(
                                            &processor_id,
                                            &output,
                                            channel_capacities.data,
                                            stats.as_ref(),
                                            pending,
                                        )
                                        .await?;
                                    }
                                    Self::forward_data(
                                        &processor_id,
                                        &output,
                                        channel_capacities.data,
                                        stats.as_ref(),
                                        data,
                                    )
                                    .await?;
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        plan = %plan_label,
                                        stream = %stream_name,
                                        "stopped"
                                    );
                                    return Ok(());
                                }
                                Self::forward_data(
                                    &processor_id,
                                    &output,
                                    channel_capacities.data,
                                    stats.as_ref(),
                                    data,
                                )
                                .await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &processor_id,
                                    skipped,
                                    "datasource data input",
                                );
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

impl DataSourceProcessor {
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }
}
