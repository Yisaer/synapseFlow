use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::processor::window_partition::{eval_partition_key, PartitionKey};

/// Tracks progress for a count-based window.
struct CountWindowState {
    target: u64,
    seen: u64,
    opened_at: Option<SystemTime>,
}

impl CountWindowState {
    fn new(target: u64) -> Self {
        Self {
            target,
            seen: 0,
            opened_at: None,
        }
    }

    fn register_row_and_check_finalize(&mut self, now: SystemTime) -> Option<SystemTime> {
        if self.seen == 0 {
            self.opened_at = Some(now);
        }
        self.seen += 1;
        if self.seen >= self.target {
            return Some(self.opened_at.take().unwrap_or(now));
        }
        None
    }

    fn reset(&mut self) {
        self.seen = 0;
        self.opened_at = None;
    }
}

/// Data-driven count window implementation.
pub struct StreamingCountAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    group_by_meta: Vec<GroupByMeta>,
    partition_by_scalars: Vec<crate::expr::ScalarExpr>,
    target: u64,
    stats: Arc<ProcessorStats>,
}

impl StreamingCountAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            physical,
            aggregate_registry,
            target,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let group_by_meta =
            build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let partition_by_scalars = match &physical.window {
            StreamingWindowSpec::Count {
                partition_by_scalars,
                ..
            } => partition_by_scalars.clone(),
            _ => Vec::new(),
        };
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            group_by_meta,
            partition_by_scalars,
            target,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        window_state: &mut CountWindowState,
        collection: &dyn Collection,
        stats: &ProcessorStats,
    ) -> Result<Vec<Box<dyn Collection>>, String> {
        let mut outputs = Vec::new();
        for row in collection.rows() {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged(
                        "streaming count aggregation processor error",
                        message,
                    );
                    continue;
                }
                Err(err) => return Err(err.to_string()),
            }
            let now = row.timestamp;
            worker.update_groups(row)?;

            if let Some(opened_at) = window_state.register_row_and_check_finalize(now) {
                if let Some(batch) = worker.finalize_current_window()? {
                    outputs.push(
                        window_metadata::attach_from_system_time(batch, opened_at, now)
                            .map_err(|err| err.to_string())?,
                    );
                }
                window_state.reset();
            }
        }
        Ok(outputs)
    }

    fn process_partitioned_collection(
        worker: &AggregationWorker,
        partition_by_scalars: &[crate::expr::ScalarExpr],
        target: u64,
        partitions: &mut PartitionedCountAggregationState,
        collection: &dyn Collection,
        stats: &ProcessorStats,
    ) -> Result<Vec<Box<dyn Collection>>, String> {
        let mut outputs = Vec::new();
        for row in collection.rows() {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged(
                        "streaming count aggregation processor error",
                        message,
                    );
                    continue;
                }
                Err(err) => return Err(err.to_string()),
            }
            let partition_key = match eval_partition_key(partition_by_scalars, row, "countwindow") {
                Ok(key) => key,
                Err(message) => {
                    stats.record_error_logged(
                        "streaming count aggregation processor error",
                        message,
                    );
                    continue;
                }
            };
            let state = partitions.get_or_insert(
                partition_key,
                target,
                Arc::clone(&worker.physical),
                Arc::clone(&worker.aggregate_registry),
                worker.group_by_meta.clone(),
            );
            let now = row.timestamp;
            state.worker.update_groups(row)?;

            if let Some(opened_at) = state.window.register_row_and_check_finalize(now) {
                if let Some(batch) = state.worker.finalize_current_window()? {
                    outputs.push(
                        window_metadata::attach_from_system_time(batch, opened_at, now)
                            .map_err(|err| err.to_string())?,
                    );
                }
                state.window.reset();
            }
        }
        Ok(outputs)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for StreamingCountAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_meta = self.group_by_meta.clone();
        let partition_by_scalars = self.partition_by_scalars.clone();
        let target = self.target;
        let stats = Arc::clone(&self.stats);

        ProcessorStart::ready(spawner.spawn(async move {
            let mut worker = AggregationWorker::new(physical, aggregate_registry, group_by_meta);
            let mut window_state = CountWindowState::new(target);
            let mut partitioned_state = PartitionedCountAggregationState::new();

            loop {
                tokio::select! {
                    // Handle control signals first if present
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        stats.record_in(collection.num_rows() as u64);
                                        let handle_start = std::time::Instant::now();
                                        let outputs_result = if partition_by_scalars.is_empty() {
                                            StreamingCountAggregationProcessor::process_collection(
                                                &mut worker,
                                                &mut window_state,
                                                collection.as_ref(),
                                                stats.as_ref(),
                                            )
                                        } else {
                                            StreamingCountAggregationProcessor::process_partitioned_collection(
                                                &worker,
                                                &partition_by_scalars,
                                                target,
                                                &mut partitioned_state,
                                                collection.as_ref(),
                                                stats.as_ref(),
                                            )
                                        };
                                        match outputs_result {
                                            Ok(outputs) => {
                                                for out in outputs {
                                                    stats.record_out(out.num_rows() as u64);
                                                    let data = StreamData::Collection(out);
                                                    let send_res = send_with_backpressure(
                                                        &output,
                                                        channel_capacities.data,
                                                        data,
                                                        Some(stats.as_ref()),
                                                    )
                                                    .await;
                                                    if let Err(err) = send_res {
                                                        stats.record_handle_duration(handle_start.elapsed());
                                                        return Err(err);
                                                    }
                                                }
                                                // Handle duration for collection processing includes downstream send/backpressure time.
                                                stats.record_handle_duration(handle_start.elapsed());
                                            }
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                return Err(ProcessorError::ProcessingError(
                                                    format!(
                                                        "Failed to process streaming count aggregation: {e}"
                                                    ),
                                                ));
                                            }
                                        }
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            StreamData::control(control_signal),
                                            Some(stats.as_ref()),
                                        )
                                    .await?;
                                        if is_terminal {
                                            break;
                                        }
                                    }
                                    other => {
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            other,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&id, n, "data input");
                            }
                            None => {
                                tracing::info!(processor_id = %id, "all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
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

struct PartitionCountAggregationState {
    window: CountWindowState,
    worker: AggregationWorker,
}

struct PartitionedCountAggregationState {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<PartitionCountAggregationState>,
}

impl PartitionedCountAggregationState {
    fn new() -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
        }
    }

    fn get_or_insert(
        &mut self,
        key: PartitionKey,
        target: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> &mut PartitionCountAggregationState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(PartitionCountAggregationState {
            window: CountWindowState::new(target),
            worker: AggregationWorker::new(physical, aggregate_registry, group_by_meta),
        });
        &mut self.states[id]
    }
}
