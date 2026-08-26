//! DataSourceProcessor - reads raw payloads from connectors.
//!
//! This processor reads data from a data source and sends it downstream as
//! `StreamData::Bytes`. Decoding is handled by a dedicated decoder processor.

use crate::checkpoint::{CheckpointMode, CheckpointSnapshotCollector, OperatorSnapshot};
use crate::connector::{
    ConnectorError, ConnectorEvent, ConnectorStream, SourceCheckpointRequest, SourceConnector,
};
use crate::planner::physical::DataDomain;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::data_metrics::{DataMetricDomains, DataMetrics, DATASOURCE_METRICS};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Schema;
use futures::{stream::SelectAll, FutureExt, StreamExt};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

const DATASOURCE_CHECKPOINT_STATE_VERSION: u32 = 2;

/// DataSourceProcessor - reads data from PhysicalDatasource
///
/// This processor:
/// - Takes a PhysicalDatasource as input
/// - Reads data from the source when triggered by control signals
/// - Sends data downstream as StreamData::Collection
pub struct DataSourceProcessor {
    /// Processor identifier (`datasource_{plan_index}` for plan-based processors)
    id: String,
    checkpoint_key: String,
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
    checkpoint_snapshot_collector: Option<Arc<CheckpointSnapshotCollector>>,
    metric_domains: DataMetricDomains,
    stats: Arc<ProcessorStats>,
}

struct ConnectorBinding {
    connector: Box<dyn SourceConnector>,
}

struct CheckpointContext<'a> {
    processor_id: &'a str,
    checkpoint_key: &'a str,
    output: &'a LinkOutput<StreamData>,
    channel_capacity: usize,
    stats: &'a ProcessorStats,
    data_metrics: &'a DataMetrics,
    connectors: &'a mut [ConnectorBinding],
    connector_streams: &'a mut SelectAll<ConnectorStream>,
    collector: Option<Arc<CheckpointSnapshotCollector>>,
}

impl ConnectorBinding {
    fn activate(&mut self, processor_id: &str) -> Result<ConnectorStream, ProcessorError> {
        let processor_id = processor_id.to_string();
        let connector_id = self.connector.id().to_string();
        let stream = match self.connector.subscribe() {
            Ok(stream) => stream,
            Err(err) => {
                tracing::error!(
                    processor_id = %processor_id,
                    connector_id = %connector_id,
                    error = %err,
                    "connector subscribe error"
                );
                return Err(Self::connector_error(self.connector.id(), err));
            }
        };

        tracing::info!(
            processor_id = %processor_id,
            connector_id = %connector_id,
            "data source connector starting"
        );

        Ok(stream)
    }

    async fn shutdown(&mut self) -> Result<(), ProcessorError> {
        let connector_id = self.connector.id().to_string();
        tracing::info!(connector_id = %connector_id, "closing source connector");
        if let Err(err) = self.connector.close() {
            return Err(Self::connector_error(self.connector.id(), err));
        }
        tracing::info!(connector_id = %connector_id, "source connector closed");
        Ok(())
    }

    fn request_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<SourceCheckpointRequest, ProcessorError> {
        self.connector
            .request_checkpoint(checkpoint_id)
            .map_err(|err| Self::connector_error(self.connector.id(), err))
    }

    fn restore_checkpoint(
        &mut self,
        state: &crate::checkpoint::CheckpointState,
    ) -> Result<(), ProcessorError> {
        self.connector
            .restore_checkpoint(state)
            .map_err(|err| Self::connector_error(self.connector.id(), err))
    }

    fn validate_checkpoint(
        &self,
        state: &crate::checkpoint::CheckpointState,
    ) -> Result<(), ProcessorError> {
        self.connector
            .validate_checkpoint(state)
            .map_err(|err| Self::connector_error(self.connector.id(), err))
    }

    fn clear_checkpoint_restore(&mut self) {
        self.connector.clear_checkpoint_restore();
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
        let source_name = source_name.into();
        let checkpoint_key = format!("datasource:{source_name}:0");
        Self::with_custom_id_and_channel_capacities(
            plan_index,
            id,
            checkpoint_key,
            source_name,
            schema,
            default_channel_capacities(),
        )
    }

    pub(crate) fn with_custom_id_and_channel_capacities(
        plan_index: Option<i64>,
        id: impl Into<String>,
        checkpoint_key: impl Into<String>,
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
            checkpoint_key: checkpoint_key.into(),
            plan_index,
            stream_name,
            schema,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            connectors: Vec::new(),
            checkpoint_snapshot_collector: None,
            metric_domains: DataMetricDomains::NONE.passthrough(DataDomain::Message),
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    pub(crate) fn set_metric_domains(&mut self, metric_domains: DataMetricDomains) {
        self.metric_domains = metric_domains;
    }

    /// Register an external source connector and its decoder.
    pub fn add_connector(&mut self, connector: Box<dyn SourceConnector>) {
        self.connectors.push(ConnectorBinding { connector });
    }

    async fn finish_checkpoint(
        context: CheckpointContext<'_>,
        checkpoint_id: u64,
        checkpoint_mode: CheckpointMode,
        checkpoint_request: SourceCheckpointRequest,
        checkpoint_data: StreamData,
    ) -> Result<(), ProcessorError> {
        let CheckpointContext {
            processor_id,
            checkpoint_key,
            output,
            channel_capacity,
            stats,
            data_metrics,
            connectors,
            connector_streams,
            collector,
        } = context;
        let mut checkpoint_request = Box::pin(checkpoint_request);
        let mut snapshot = None;
        let mut snapshot_ready = false;
        let connector_active = true;

        loop {
            tokio::select! {
                biased;
                result = &mut checkpoint_request, if !snapshot_ready => {
                    snapshot = Some(
                        result.map_err(|err| {
                            ConnectorBinding::connector_error("checkpoint", err)
                        })?,
                    );
                    snapshot_ready = true;
                }
                event = connector_streams.next(), if connector_active => {
                    match event {
                        Some(Ok(ConnectorEvent::Payload(bytes))) => {
                            Self::forward_data(
                                processor_id,
                                output,
                                channel_capacity,
                                stats,
                                data_metrics,
                                StreamData::bytes(bytes),
                            )
                            .await?;
                        }
                        Some(Ok(ConnectorEvent::Collection(collection))) => {
                            Self::forward_data(
                                processor_id,
                                output,
                                channel_capacity,
                                stats,
                                data_metrics,
                                StreamData::collection(collection),
                            )
                            .await?;
                        }
                        Some(Ok(ConnectorEvent::CheckpointFence { checkpoint_id: fence_id })) => {
                            if fence_id != checkpoint_id {
                                return Err(ProcessorError::ProcessingError(format!(
                                    "source connector checkpoint fence mismatch: expected {}, got {}",
                                    checkpoint_id, fence_id
                                )));
                            }
                            let state = match snapshot.take() {
                                Some(state) => state,
                                None => checkpoint_request
                                    .await
                                    .map_err(|err| {
                                        ConnectorBinding::connector_error("checkpoint", err)
                                    })?,
                            };
                            if let Some(state) = state {
                                let collector = collector.as_ref().ok_or_else(|| {
                                    ProcessorError::InvalidConfiguration(
                                        "checkpoint snapshot collector is not configured".to_string(),
                                    )
                                })?;
                                collector
                                    .record(
                                        checkpoint_id,
                                        OperatorSnapshot::new(
                                            checkpoint_key,
                                            "datasource",
                                            DATASOURCE_CHECKPOINT_STATE_VERSION,
                                            state,
                                        ),
                                    )
                                    .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
                            }
                            Self::forward_data(
                                processor_id,
                                output,
                                channel_capacity,
                                stats,
                                data_metrics,
                                checkpoint_data,
                            )
                            .await?;
                            if checkpoint_mode == CheckpointMode::Final {
                                Self::shutdown_connectors(connectors).await?;
                            }
                            return Ok(());
                        }
                        Some(Ok(ConnectorEvent::EndOfStream)) | None => {
                            return Err(ProcessorError::ProcessingError(
                                "source connector ended before reaching the checkpoint fence"
                                    .to_string(),
                            ));
                        }
                        Some(Err(err)) => {
                            return Err(ConnectorBinding::connector_error("source", err));
                        }
                    }
                }
            }
        }
    }

    fn activate_connectors(
        connectors: &mut [ConnectorBinding],
        processor_id: &str,
    ) -> Result<Vec<ConnectorStream>, ProcessorError> {
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

    async fn forward_data(
        processor_id: &str,
        output: &LinkOutput<StreamData>,
        channel_capacity: usize,
        stats: &ProcessorStats,
        data_metrics: &DataMetrics,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        log_received_data(processor_id, &data);
        let measurement = data_metrics.record_input(stats, &data)?;
        send_with_backpressure(output, channel_capacity, data, Some(stats)).await?;
        data_metrics.record_output(stats, measurement)?;
        Ok(())
    }
}

impl Processor for DataSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn checkpoint_key(&self) -> &str {
        &self.checkpoint_key
    }

    fn set_checkpoint_snapshot_collector(
        &mut self,
        collector: Option<Arc<CheckpointSnapshotCollector>>,
    ) {
        self.checkpoint_snapshot_collector = collector;
    }

    fn validate_checkpoint(&self, snapshot: &OperatorSnapshot) -> Result<(), ProcessorError> {
        if snapshot.checkpoint_key != self.checkpoint_key {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "datasource checkpoint key mismatch: expected {}, got {}",
                self.checkpoint_key, snapshot.checkpoint_key
            )));
        }
        if snapshot.operator_kind != "datasource" {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "datasource checkpoint operator kind mismatch: expected datasource, got {}",
                snapshot.operator_kind
            )));
        }
        if snapshot.state_version != DATASOURCE_CHECKPOINT_STATE_VERSION {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "unsupported datasource checkpoint state version: {}",
                snapshot.state_version
            )));
        }
        if self.connectors.len() != 1 {
            return Err(ProcessorError::InvalidConfiguration(
                "datasource checkpoint restore requires exactly one connector".to_string(),
            ));
        }
        self.connectors[0].validate_checkpoint(&snapshot.state)
    }

    fn restore_checkpoint(&mut self, snapshot: &OperatorSnapshot) -> Result<(), ProcessorError> {
        self.validate_checkpoint(snapshot)?;
        self.connectors[0].restore_checkpoint(&snapshot.state)
    }

    fn clear_checkpoint_restore(&mut self) {
        for connector in &mut self.connectors {
            connector.clear_checkpoint_restore();
        }
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let checkpoint_key = self.checkpoint_key.clone();
        let channel_capacities = self.channel_capacities;
        let checkpoint_snapshot_collector = self.checkpoint_snapshot_collector.clone();
        let stats = Arc::clone(&self.stats);
        let data_metrics =
            DataMetrics::new(stats.as_ref(), DATASOURCE_METRICS, self.metric_domains);
        let plan_label = self
            .plan_index
            .map(|idx| idx.to_string())
            .unwrap_or_else(|| "global".to_string());
        let stream_name = self.stream_name.clone();
        let base_inputs = std::mem::take(&mut self.inputs);
        let mut connectors = std::mem::take(&mut self.connectors);
        let connector_streams = match Self::activate_connectors(&mut connectors, &processor_id) {
            Ok(inputs) => inputs,
            Err(err) => {
                return ProcessorStart::failed(spawner, err);
            }
        };
        let mut input_streams = fan_in_streams(base_inputs);
        let mut connector_streams = futures::stream::select_all(connector_streams);
        let mut connector_active = !connectors.is_empty();
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
                                if let Some(checkpoint_mode) =
                                    data.as_control().and_then(ControlSignal::checkpoint_mode)
                                {
                                    let checkpoint_id = data
                                        .as_control()
                                        .map(ControlSignal::id)
                                        .ok_or_else(|| {
                                            ProcessorError::ProcessingError(
                                                "checkpoint is missing its control signal"
                                                    .to_string(),
                                            )
                                        })?;
                                    if connectors.len() != 1 {
                                        return Err(ProcessorError::InvalidConfiguration(
                                            "data source checkpointing requires exactly one connector"
                                                .to_string(),
                                        ));
                                    }
                                    let checkpoint_request = connectors[0]
                                        .request_checkpoint(checkpoint_id)?;
                                    Self::finish_checkpoint(
                                        CheckpointContext {
                                            processor_id: &processor_id,
                                            checkpoint_key: &checkpoint_key,
                                            output: &output,
                                            channel_capacity: channel_capacities.data,
                                            stats: stats.as_ref(),
                                            data_metrics: &data_metrics,
                                            connectors: &mut connectors,
                                            connector_streams: &mut connector_streams,
                                            collector: checkpoint_snapshot_collector.clone(),
                                        },
                                        checkpoint_id,
                                        checkpoint_mode,
                                        checkpoint_request,
                                        data,
                                    )
                                    .await?;
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        plan = %plan_label,
                                        stream = %stream_name,
                                        checkpoint_id,
                                        mode = ?checkpoint_mode,
                                        "checkpoint completed"
                                    );
                                    if checkpoint_mode == CheckpointMode::Final {
                                        return Ok(());
                                    }
                                    continue;
                                }
                                if data.is_terminal() {
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        plan = %plan_label,
                                        stream = %stream_name,
                                        "received StreamEnd (data)"
                                    );
                                    Self::shutdown_connectors(&mut connectors).await?;
                                    while let Some(Some(connector_event)) =
                                        connector_streams.next().now_or_never()
                                    {
                                        match connector_event {
                                            Ok(ConnectorEvent::Payload(bytes)) => {
                                                Self::forward_data(
                                                    &processor_id,
                                                    &output,
                                                    channel_capacities.data,
                                                    stats.as_ref(),
                                                    &data_metrics,
                                                    StreamData::bytes(bytes),
                                                )
                                                .await?;
                                            }
                                            Ok(ConnectorEvent::Collection(collection)) => {
                                                Self::forward_data(
                                                    &processor_id,
                                                    &output,
                                                    channel_capacities.data,
                                                    stats.as_ref(),
                                                    &data_metrics,
                                                    StreamData::collection(collection),
                                                )
                                                .await?;
                                            }
                                            Ok(ConnectorEvent::EndOfStream) => break,
                                            Ok(ConnectorEvent::CheckpointFence {
                                                checkpoint_id,
                                            }) => {
                                                tracing::warn!(
                                                    processor_id = %processor_id,
                                                    checkpoint_id,
                                                    "source connector emitted an unexpected checkpoint fence while stopping"
                                                );
                                            }
                                            Err(err) => {
                                                let message =
                                                    format!("source connector error: {err}");
                                                tracing::error!(
                                                    processor_id = %processor_id,
                                                    error = %err,
                                                    "source connector error while stopping"
                                                );
                                                stats.record_error(message);
                                            }
                                        }
                                    }
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
                                            &data_metrics,
                                            pending,
                                        )
                                        .await?;
                                    }
                                    Self::forward_data(
                                        &processor_id,
                                        &output,
                                        channel_capacities.data,
                                        stats.as_ref(),
                                        &data_metrics,
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
                                    &data_metrics,
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
                    connector_event = connector_streams.next(), if connector_active => {
                        match connector_event {
                            Some(Ok(ConnectorEvent::Payload(bytes))) => {
                                Self::forward_data(
                                    &processor_id,
                                    &output,
                                    channel_capacities.data,
                                    stats.as_ref(),
                                    &data_metrics,
                                    StreamData::bytes(bytes),
                                )
                                .await?;
                            }
                            Some(Ok(ConnectorEvent::Collection(collection))) => {
                                Self::forward_data(
                                    &processor_id,
                                    &output,
                                    channel_capacities.data,
                                    stats.as_ref(),
                                    &data_metrics,
                                    StreamData::collection(collection),
                                )
                                .await?;
                            }
                            Some(Ok(ConnectorEvent::EndOfStream)) => {
                                connector_active = false;
                            }
                            Some(Ok(ConnectorEvent::CheckpointFence { checkpoint_id })) => {
                                tracing::warn!(
                                    processor_id = %processor_id,
                                    checkpoint_id,
                                    "source connector emitted an unexpected checkpoint fence"
                                );
                            }
                            Some(Err(err)) => {
                                let message = format!("source connector error: {err}");
                                tracing::error!(
                                    processor_id = %processor_id,
                                    error = %err,
                                    "source connector error"
                                );
                                stats.record_error(message);
                            }
                            None => {
                                connector_active = false;
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
