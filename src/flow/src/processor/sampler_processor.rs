//! SamplerProcessor - Samples a stream by emitting data based on configurable strategies.
//!
//! This processor is designed for downsampling high-frequency streams at the source level.
//! All pipelines using a stream with a sampler receive the same downsampled data.
//!
//! Strategies:
//! - Latest: Keep only the latest value, emit at fixed intervals (lossy downsampling)
//! - Future: Merge strategy for combining multiple records

use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::codec::{Merger, MergerRegistry};
use crate::shared_stream::AppliedDecodeState;
use datatypes::Schema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackerProps {
    pub merger: MergerConfig,
}

/// Sampling strategy for the SamplerProcessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SamplingStrategy {
    /// Keep only the latest value, discard intermediate values (lossy).
    #[default]
    Latest,
    /// Accumulate values and pack using a Merger.
    Packer {
        /// Packer properties.
        props: PackerProps,
    },
}

/// Configuration for a merger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergerConfig {
    /// Type of merger (e.g., "gbf").
    #[serde(rename = "type")]
    pub merger_type: String,
    /// Merger-specific properties.
    #[serde(default)]
    pub props: JsonMap<String, JsonValue>,
}

/// Configuration for the SamplerProcessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplerConfig {
    /// Interval between emissions (e.g., "100ms", "1s").
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Sampling strategy to use.
    #[serde(default)]
    pub strategy: SamplingStrategy,
}

impl SamplerConfig {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            strategy: SamplingStrategy::Latest,
        }
    }

    pub fn with_strategy(mut self, strategy: SamplingStrategy) -> Self {
        self.strategy = strategy;
        self
    }
}

/// Processor that samples a stream based on configurable strategies.
///
/// Currently supports:
/// - Latest: Emits the most recent value at fixed intervals
/// - Packer: Accumulates values and emits packed binary
pub struct SamplerProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    config: SamplerConfig,
    stats: Arc<ProcessorStats>,
    merger_registry: Option<Arc<MergerRegistry>>,
    /// Output schema used to build a fused (decode-capable) Packer merger.
    merger_schema: Option<Arc<Schema>>,
    /// Shared decode state for projection pushdown into the fused decode path.
    applied_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
}

impl SamplerProcessor {
    pub fn new(id: impl Into<String>, config: SamplerConfig) -> Self {
        Self::new_with_channel_capacities(id, config, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        config: SamplerConfig,
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
            config,
            stats: Arc::new(ProcessorStats::default()),
            merger_registry: None,
            merger_schema: None,
            applied_decode_state: None,
        }
    }

    /// Convenience constructor for Latest strategy with interval.
    pub fn latest(id: impl Into<String>, interval: Duration) -> Self {
        Self::new(id, SamplerConfig::new(interval))
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    pub fn set_merger_registry(&mut self, registry: Arc<MergerRegistry>) {
        self.merger_registry = Some(registry);
    }

    /// Provide the output schema used to construct a Packer merger (required for
    /// mergers that build an embedded decoder for the fused path).
    pub fn set_merger_schema(&mut self, schema: Arc<Schema>) {
        self.merger_schema = Some(schema);
    }

    /// Provide shared decode state so the fused decode path can apply the
    /// current column projection.
    pub(crate) fn set_applied_decode_state(
        &mut self,
        state: Arc<parking_lot::RwLock<AppliedDecodeState>>,
    ) {
        self.applied_decode_state = Some(state);
    }
}

impl Processor for SamplerProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let emit_interval = self.config.interval;
        let processor_id = self.id.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let strategy = self.config.strategy.clone();

        tracing::info!(
            processor_id = %processor_id,
            interval = ?emit_interval,
            strategy = ?strategy,
            "sampler processor starting"
        );

        let merger_registry = self.merger_registry.clone();
        let merger_schema = self.merger_schema.clone();
        let applied_decode_state = self.applied_decode_state.clone();

        ProcessorStart::ready(spawner.spawn(async move {
            let mut ticker = interval(emit_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut strategy_state = StrategyState::new(
                strategy,
                merger_registry,
                merger_schema,
                applied_decode_state,
            )?;
            let mut control_active = control_active;

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
                                if let Some(pending) = strategy_state.flush() {
                                    emit_data(&output, channel_capacities.data, &stats, pending)
                                        .await?;
                                }
                                tracing::info!(processor_id = %processor_id, "sampler received terminal signal, shutting down");
                                return Ok(());
                            }
                        } else if control_item.is_none() {
                            control_active = false;
                        } else if let Some(Err(BroadcastStreamRecvError::Lagged(n))) = control_item {
                            log_broadcast_lagged(&processor_id, n, "control");
                        }
                    }
                    _ = ticker.tick() => {
                        if let Some(data) = strategy_state.on_tick() {
                            emit_data(&output, channel_capacities.data, &stats, data).await?;
                            tracing::trace!(processor_id = %processor_id, "sampler emitted tick value");
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                stats.record_in(data.num_rows_hint().unwrap_or(0));

                                if !should_sample_data(&data) {
                                    // Do not throttle non-data items received on the data channel.
                                    // This keeps watermark/error/control events flowing promptly.
                                    let is_terminal = data.is_terminal();
                                    if is_terminal {
                                        if let Some(pending) = strategy_state.flush() {
                                            emit_data(
                                                &output,
                                                channel_capacities.data,
                                                &stats,
                                                pending,
                                            )
                                            .await?;
                                        }
                                        emit_data(&output, channel_capacities.data, &stats, data)
                                            .await?;
                                        tracing::info!(processor_id = %processor_id, "sampler received terminal item on data channel, shutting down");
                                        return Ok(());
                                    }
                                    emit_data(&output, channel_capacities.data, &stats, data)
                                        .await?;
                                    continue;
                                }

                                // Sample data based on selected strategy (lossy).
                                let handle_start = std::time::Instant::now();
                                if let Err(e) = strategy_state.on_sampled_data(data) {
                                    // For sampler (async emit), handle duration measures local state update time only.
                                    stats.record_handle_duration(handle_start.elapsed());
                                    tracing::error!(processor_id = %processor_id, error = ?e, "strategy sampling error");
                                    return Err(e);
                                }
                                // For sampler (async emit), handle duration measures local state update time only.
                                stats.record_handle_duration(handle_start.elapsed());
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&processor_id, n, "data");
                            }
                            None => {
                                if let Some(pending) = strategy_state.flush() {
                                    emit_data(&output, channel_capacities.data, &stats, pending)
                                        .await?;
                                }
                                tracing::info!(processor_id = %processor_id, "sampler input closed, shutting down");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }))
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

enum StrategyState {
    Latest {
        latest: Option<StreamData>,
    },
    Packer {
        merger: Box<dyn Merger>,
        /// Shared decode state for projection pushdown when the merger supports
        /// fused decode. `None` falls back to decoding all columns.
        applied_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    },
}

impl StrategyState {
    fn new(
        strategy: SamplingStrategy,
        registry: Option<Arc<MergerRegistry>>,
        schema: Option<Arc<Schema>>,
        applied_decode_state: Option<Arc<parking_lot::RwLock<AppliedDecodeState>>>,
    ) -> Result<Self, ProcessorError> {
        match strategy {
            SamplingStrategy::Latest => Ok(StrategyState::Latest { latest: None }),
            SamplingStrategy::Packer { props } => {
                let registry = registry.ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "Packer strategy requires MergerRegistry".to_string(),
                    )
                })?;
                let schema = schema.ok_or_else(|| {
                    ProcessorError::InvalidConfiguration(
                        "Packer strategy requires an output schema".to_string(),
                    )
                })?;
                let merger_instance = registry
                    .instantiate(&props.merger.merger_type, &props.merger.props, schema)
                    .map_err(|e| ProcessorError::InvalidConfiguration(e.to_string()))?;
                Ok(StrategyState::Packer {
                    merger: merger_instance,
                    applied_decode_state,
                })
            }
        }
    }

    fn on_sampled_data(&mut self, data: StreamData) -> Result<(), ProcessorError> {
        match self {
            StrategyState::Latest { latest } => {
                *latest = Some(data);
                Ok(())
            }
            StrategyState::Packer { merger, .. } => match data {
                StreamData::Bytes(bytes) => merger
                    .merge(&bytes)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string())),
                _ => {
                    tracing::warn!("Packer received non-bytes data: variant mismatch");
                    Ok(())
                }
            },
        }
    }

    fn on_tick(&mut self) -> Option<StreamData> {
        match self {
            StrategyState::Latest { latest } => latest.take(),
            StrategyState::Packer {
                merger,
                applied_decode_state,
            } => {
                if merger.supports_fused_decode() {
                    // Fused path: decode accumulated frames directly to a
                    // collection, skipping the binary re-encode + re-parse.
                    let projection = applied_decode_state
                        .as_ref()
                        .map(|state| Arc::clone(&state.read().decode_projection));
                    match merger.trigger_decoded(projection.as_deref()) {
                        Ok(Some(collection)) => Some(StreamData::collection(collection)),
                        Ok(None) => None,
                        Err(e) => {
                            tracing::error!(error = ?e, "merger trigger_decoded error");
                            None
                        }
                    }
                } else {
                    match merger.trigger() {
                        Ok(Some(bytes)) if !bytes.is_empty() => Some(StreamData::bytes(bytes)),
                        Ok(_) => None,
                        Err(e) => {
                            tracing::error!(error = ?e, "merger trigger error");
                            None
                        }
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> Option<StreamData> {
        self.on_tick()
    }
}

fn should_sample_data(item: &StreamData) -> bool {
    matches!(
        item,
        StreamData::Bytes(_) | StreamData::Collection(_) | StreamData::EncodedDelivery { .. }
    )
}

async fn emit_data(
    output: &LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
    data: StreamData,
) -> Result<(), ProcessorError> {
    let row_count = data.num_rows_hint().unwrap_or(0);
    send_with_backpressure(output, data_channel_capacity, data, Some(stats.as_ref())).await?;
    stats.record_out(row_count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Message, RecordBatch, Tuple};
    use crate::processor::StreamData;
    use crate::runtime::TaskSpawner;
    use datatypes::Value;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::sleep;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn create_test_data(value: i64) -> StreamData {
        let message = Arc::new(Message::new(
            "test",
            vec![Arc::from("value")],
            vec![Arc::new(Value::Int64(value))],
        ));
        let tuple = Tuple::new(vec![message]);
        let batch = RecordBatch::new(vec![tuple]).expect("create batch");
        StreamData::Collection(Box::new(batch))
    }

    #[tokio::test]
    async fn test_sampler_emits_latest_value() {
        let spawner = test_spawner();
        let mut sampler = SamplerProcessor::latest("latest_test", Duration::from_millis(200));

        let (input_tx, input_rx) = broadcast::channel::<StreamData>(16);
        sampler.add_input(input_rx);

        let mut output_rx = sampler.subscribe_output().unwrap();
        let _handle = sampler.start(&spawner);

        sleep(Duration::from_millis(10)).await;

        // Send values 1, 2, 3 rapidly
        let _ = input_tx.send(create_test_data(1));
        let _ = input_tx.send(create_test_data(2));
        let _ = input_tx.send(create_test_data(3));

        sleep(Duration::from_millis(250)).await;

        let received = output_rx.try_recv();
        assert!(received.is_ok(), "Expected to receive data");

        if let Ok(StreamData::Collection(collection)) = received {
            let rows = collection.rows();
            assert_eq!(rows.len(), 1);
            let message = &rows[0].messages()[0];
            let (_, value) = message.entry_by_index(0).expect("get value");
            assert_eq!(*value.as_ref(), Value::Int64(3));
        } else {
            panic!("Expected Collection data");
        }
    }

    #[test]
    fn test_sampler_config_deserialization_packer() {
        let json = serde_json::json!({
            "interval": "1s",
            "strategy": {
                "type": "packer",
                "props": {
                    "merger": {
                        "type": "can_merger",
                        "props": {
                            "schema": "/etc/can.dbc"
                        }
                    }
                }
            }
        });

        let config: SamplerConfig = serde_json::from_value(json).expect("deserialize config");
        match config.strategy {
            SamplingStrategy::Packer { props } => {
                assert_eq!(props.merger.merger_type, "can_merger");
                assert_eq!(props.merger.props["schema"], "/etc/can.dbc");
            }
            _ => panic!("Expected Packer strategy"),
        }
    }
}
