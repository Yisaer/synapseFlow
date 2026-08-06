use super::{build_group_by_meta, create_accumulators_static, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::processor::window_partition::{eval_partition_key, PartitionKey};

/// Streaming sliding aggregation processor (incremental).
///
/// - Maintain a list of active windows, each with its own incremental aggregation state.
/// - For every incoming tuple, append a new window starting at that tuple's timestamp.
/// - Update all active windows whose `[start, start + length + delay)` contains the tuple time.
/// - Emit the oldest active window:
///   - immediately when `delay == 0` (per-tuple trigger),
///   - or when receiving deadline watermarks from upstream (`delay > 0`).
///
/// Notes:
/// - This is processing-time only: tuple timestamps are assumed to be non-decreasing.
/// - For `slidingwindow('ss', length)`, we treat `length` as the window length and `delay = 0`.
/// - For `slidingwindow('ss', length, delay)`, we treat the third argument as the delay.
pub struct StreamingSlidingAggregationProcessor {
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
    length_secs: u64,
    delay_secs: u64,
    stats: Arc<ProcessorStats>,
}

struct WindowGroupState {
    accumulators: Vec<Box<dyn crate::aggregation::AggregateAccumulator>>,
    last_tuple: crate::model::Tuple,
    key_values: Vec<Value>,
}

struct IncAggWindow {
    start_secs: u64,
    groups: HashMap<String, WindowGroupState>,
}

#[derive(Clone, Copy)]
struct SlidingWindowBounds {
    length_secs: u64,
    delay_secs: u64,
}

impl SlidingWindowBounds {
    fn end_secs(self, start_secs: u64) -> u64 {
        start_secs
            .saturating_add(self.length_secs)
            .saturating_add(self.delay_secs)
    }
}

impl StreamingSlidingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(
            id,
            physical,
            aggregate_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        let group_by_meta =
            build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let partition_by_scalars = match &physical.window {
            StreamingWindowSpec::Sliding {
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

        let (length_secs, delay_secs) = Self::extract_window_spec(physical.as_ref())?;

        Ok(Self {
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
            length_secs,
            delay_secs,
            stats: Arc::new(ProcessorStats::default()),
        })
    }

    fn extract_window_spec(
        physical: &PhysicalStreamingAggregation,
    ) -> Result<(u64, u64), ProcessorError> {
        match &physical.window {
            StreamingWindowSpec::Sliding {
                time_unit: _,
                lookback,
                lookahead,
                ..
            } => Ok(((*lookback).max(1), lookahead.unwrap_or(0))),
            other => Err(ProcessorError::InvalidConfiguration(format!(
                "streaming sliding aggregation requires sliding window spec, got {other:?}",
            ))),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    fn validate_supported_aggregates(&self) -> Result<(), ProcessorError> {
        for call in &self.physical.aggregate_calls {
            if call.distinct {
                return Err(ProcessorError::InvalidConfiguration(
                    "DISTINCT aggregates are not supported in streaming sliding aggregation"
                        .to_string(),
                ));
            }
            if !self
                .aggregate_registry
                .supports_incremental(&call.func_name)
            {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "Aggregate function '{}' does not support incremental updates",
                    call.func_name
                )));
            }
        }
        Ok(())
    }
}

impl Processor for StreamingSlidingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        if let Err(e) = self.validate_supported_aggregates() {
            return ProcessorStart::failed(spawner, e);
        }

        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let physical = Arc::clone(&self.physical);
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let group_by_meta = self.group_by_meta.clone();
        let partition_by_scalars = self.partition_by_scalars.clone();
        let length_secs = self.length_secs;
        let delay_secs = self.delay_secs;
        let bounds = SlidingWindowBounds {
            length_secs,
            delay_secs,
        };
        let stats = Arc::clone(&self.stats);

        ProcessorStart::ready(spawner.spawn(async move {
            let mut partitioned_windows = PartitionedSlidingWindows::new(partition_by_scalars);

            fn to_epoch_secs(ts: SystemTime) -> Result<u64, ProcessorError> {
                Ok(ts
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| {
                        ProcessorError::ProcessingError(format!("invalid timestamp: {e}"))
                    })?
                    .as_secs())
            }

            fn gc_windows(
                windows: &mut VecDeque<IncAggWindow>,
                now_secs: u64,
                length_secs: u64,
                delay_secs: u64,
            ) {
                while let Some(front) = windows.front() {
                    if front
                        .start_secs
                        .saturating_add(length_secs)
                        .saturating_add(delay_secs)
                        < now_secs
                    {
                        windows.pop_front();
                    } else {
                        break;
                    }
                }
            }

            fn update_window_with_tuple(
                physical: &PhysicalStreamingAggregation,
                aggregate_registry: &AggregateFunctionRegistry,
                group_by_meta: &[GroupByMeta],
                window: &mut IncAggWindow,
                tuple: &crate::model::Tuple,
            ) -> Result<(), ProcessorError> {
                let mut key_values = Vec::with_capacity(group_by_meta.len());
                for meta in group_by_meta {
                    key_values.push(meta.scalar.eval_with_tuple(tuple).map_err(|e| {
                        ProcessorError::ProcessingError(format!(
                            "failed to evaluate group-by expression: {e}"
                        ))
                    })?);
                }
                let key_repr = format!("{:?}", key_values);

                let entry = match window.groups.entry(key_repr) {
                    Entry::Occupied(o) => o.into_mut(),
                    Entry::Vacant(v) => {
                        let accumulators = create_accumulators_static(
                            &physical.aggregate_calls,
                            aggregate_registry,
                        )
                        .map_err(ProcessorError::ProcessingError)?;
                        v.insert(WindowGroupState {
                            accumulators,
                            last_tuple: tuple.clone(),
                            key_values: key_values.clone(),
                        })
                    }
                };

                entry.last_tuple = tuple.clone();
                entry.key_values = key_values;

                for (idx, call) in physical.aggregate_calls.iter().enumerate() {
                    let mut args = Vec::with_capacity(call.args.len());
                    for arg_expr in &call.args {
                        args.push(arg_expr.eval_with_tuple(tuple).map_err(|e| {
                            ProcessorError::ProcessingError(format!(
                                "failed to evaluate aggregate argument: {e}"
                            ))
                        })?);
                    }
                    entry
                        .accumulators
                        .get_mut(idx)
                        .ok_or_else(|| {
                            ProcessorError::ProcessingError("accumulator missing".to_string())
                        })?
                        .update(&args)
                        .map_err(ProcessorError::ProcessingError)?;
                }

                Ok(())
            }

            async fn emit_oldest_window(
                output: &LinkOutput<StreamData>,
                data_channel_capacity: usize,
                physical: &PhysicalStreamingAggregation,
                group_by_meta: &[GroupByMeta],
                windows: &VecDeque<IncAggWindow>,
                bounds: SlidingWindowBounds,
                stats: &Arc<ProcessorStats>,
            ) -> Result<(), ProcessorError> {
                let Some(window) = windows.front() else {
                    return Ok(());
                };
                if window.groups.is_empty() {
                    return Ok(());
                }

                let mut out_rows = Vec::with_capacity(window.groups.len());
                for state in window.groups.values() {
                    let mut affiliate_entries = Vec::new();
                    for (call, accumulator) in physical
                        .aggregate_calls
                        .iter()
                        .zip(state.accumulators.iter())
                    {
                        affiliate_entries
                            .push((Arc::new(call.output_column.clone()), accumulator.finalize()));
                    }
                    for (idx, value) in state.key_values.iter().enumerate() {
                        if let Some(meta) = group_by_meta.get(idx) {
                            if !meta.is_simple {
                                affiliate_entries
                                    .push((Arc::new(meta.output_name.clone()), value.clone()));
                            }
                        }
                    }

                    let mut tuple = crate::model::Tuple::with_timestamp(
                        state.last_tuple.messages.clone(),
                        state.last_tuple.timestamp,
                    );
                    tuple.add_affiliate_columns(affiliate_entries);
                    out_rows.push(tuple);
                }

                if out_rows.is_empty() {
                    return Ok(());
                }
                stats.record_out(out_rows.len() as u64);
                let batch = window_metadata::record_batch_from_epoch_secs(
                    out_rows,
                    window.start_secs,
                    bounds.end_secs(window.start_secs),
                )?;
                send_with_backpressure(
                    output,
                    data_channel_capacity,
                    StreamData::collection(Box::new(batch)),
                    Some(stats.as_ref()),
                )
                .await?;
                Ok(())
            }

            async fn emit_all_windows(
                output: &LinkOutput<StreamData>,
                data_channel_capacity: usize,
                physical: &PhysicalStreamingAggregation,
                group_by_meta: &[GroupByMeta],
                partitioned_windows: &PartitionedSlidingWindows,
                bounds: SlidingWindowBounds,
                stats: &Arc<ProcessorStats>,
            ) -> Result<(), ProcessorError> {
                for windows in partitioned_windows.windows() {
                    for window in windows {
                        if window.groups.is_empty() {
                            continue;
                        }

                        let mut out_rows = Vec::with_capacity(window.groups.len());
                        for state in window.groups.values() {
                            let mut affiliate_entries = Vec::new();
                            for (call, accumulator) in physical
                                .aggregate_calls
                                .iter()
                                .zip(state.accumulators.iter())
                            {
                                affiliate_entries.push((
                                    Arc::new(call.output_column.clone()),
                                    accumulator.finalize(),
                                ));
                            }
                            for (idx, value) in state.key_values.iter().enumerate() {
                                if let Some(meta) = group_by_meta.get(idx) {
                                    if !meta.is_simple {
                                        affiliate_entries.push((
                                            Arc::new(meta.output_name.clone()),
                                            value.clone(),
                                        ));
                                    }
                                }
                            }

                            let mut tuple = crate::model::Tuple::with_timestamp(
                                state.last_tuple.messages.clone(),
                                state.last_tuple.timestamp,
                            );
                            tuple.add_affiliate_columns(affiliate_entries);
                            out_rows.push(tuple);
                        }

                        if out_rows.is_empty() {
                            continue;
                        }

                        stats.record_out(out_rows.len() as u64);
                        let batch = window_metadata::record_batch_from_epoch_secs(
                            out_rows,
                            window.start_secs,
                            bounds.end_secs(window.start_secs),
                        )?;
                        send_with_backpressure(
                            output,
                            data_channel_capacity,
                            StreamData::collection(Box::new(batch)),
                            Some(stats.as_ref()),
                        )
                        .await?;
                    }
                }
                Ok(())
            }

            loop {
                tokio::select! {
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
                            Some(Ok(StreamData::Collection(collection))) => {
                                stats.record_in(collection.num_rows() as u64);
                                let handle_start = std::time::Instant::now();
                                let res = async {
                                    let rows = collection.into_rows().map_err(|e| {
                                        ProcessorError::ProcessingError(format!(
                                            "failed to extract rows: {e}"
                                        ))
                                    })?;

                                    for tuple in rows {
                                        let now_secs = to_epoch_secs(tuple.timestamp)?;
                                        let windows =
                                            partitioned_windows.windows_for_tuple(&tuple)?;
                                        gc_windows(windows, now_secs, length_secs, delay_secs);

                                        windows.push_back(IncAggWindow {
                                            start_secs: now_secs,
                                            groups: HashMap::new(),
                                        });

                                        for window in windows.iter_mut() {
                                            if window.start_secs <= now_secs
                                                && window
                                                    .start_secs
                                                    .saturating_add(length_secs)
                                                    .saturating_add(delay_secs)
                                                    > now_secs
                                            {
                                                update_window_with_tuple(
                                                    &physical,
                                                    aggregate_registry.as_ref(),
                                                    &group_by_meta,
                                                    window,
                                                    &tuple,
                                                )?;
                                            }
                                        }

                                        if delay_secs == 0 {
                                            emit_oldest_window(
                                                &output,
                                                channel_capacities.data,
                                                &physical,
                                                &group_by_meta,
                                                windows,
                                                bounds,
                                                &stats,
                                            )
                                            .await?;
                                        }
                                    }

                                    Ok::<(), ProcessorError>(())
                                }
                                .await;
                                stats.record_handle_duration(handle_start.elapsed());
                                res?;
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                if delay_secs == 0 {
                                    continue;
                                }
                                let now_secs = to_epoch_secs(ts)?;
                                for windows in partitioned_windows.windows_mut() {
                                    gc_windows(windows, now_secs, length_secs, delay_secs);
                                    if let Some(front) = windows.front() {
                                        if front.start_secs.saturating_add(delay_secs) <= now_secs {
                                            emit_oldest_window(&output, channel_capacities.data, &physical, &group_by_meta, windows, bounds, &stats)
                                                .await?;
                                        }
                                    }
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                let is_graceful = control_signal.is_graceful_end();
                                if is_terminal {
                                    if is_graceful {
                                        emit_all_windows(
                                            &output,
                                            channel_capacities.data,
                                            &physical,
                                            &group_by_meta,
                                            &partitioned_windows,
                                            bounds,
                                            &stats,
                                        )
                                            .await?;
                                    }
                                    send_with_backpressure(
                                        &output,
                                        channel_capacities.data,
                                        StreamData::control(control_signal),
                                        Some(stats.as_ref()),
                                    )
                                    .await?;
                                    break;
                                }
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    StreamData::control(control_signal),
                                    Some(stats.as_ref()),
                                )
                                .await?;
                            }
                            Some(Ok(other)) => {
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    other,
                                    Some(stats.as_ref()),
                                )
                                    .await?;
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

struct PartitionedSlidingWindows {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<VecDeque<IncAggWindow>>,
    partition_by_scalars: Vec<crate::expr::ScalarExpr>,
}

impl PartitionedSlidingWindows {
    fn new(partition_by_scalars: Vec<crate::expr::ScalarExpr>) -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
            partition_by_scalars,
        }
    }

    fn windows_for_tuple(
        &mut self,
        tuple: &crate::model::Tuple,
    ) -> Result<&mut VecDeque<IncAggWindow>, ProcessorError> {
        let key = eval_partition_key(&self.partition_by_scalars, tuple, "slidingwindow")
            .map_err(ProcessorError::ProcessingError)?;
        if let Some(id) = self.state_ids.get(&key).copied() {
            return Ok(&mut self.states[id]);
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(VecDeque::new());
        Ok(&mut self.states[id])
    }

    fn windows_mut(&mut self) -> impl Iterator<Item = &mut VecDeque<IncAggWindow>> {
        self.states.iter_mut()
    }

    fn windows(&self) -> impl Iterator<Item = &VecDeque<IncAggWindow>> {
        self.states.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::AggregateFunctionRegistry;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ScalarExpr;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::AggregateCall;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::runtime::TaskSpawner;
    use datatypes::Value;
    use sqlparser::ast::{Expr, Ident};
    use std::collections::HashMap;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn col(name: &str) -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: name.to_string(),
        })
    }

    fn tuple_at(sec: u64, a: i64) -> crate::model::Tuple {
        let mut tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        );
        tuple.add_affiliate_column(Arc::new("a".to_string()), Value::Int64(a));
        tuple
    }

    fn make_physical() -> Arc<PhysicalStreamingAggregation> {
        make_physical_with_lookahead(None)
    }

    fn make_physical_with_lookahead(lookahead: Option<u64>) -> Arc<PhysicalStreamingAggregation> {
        let call = AggregateCall {
            output_column: "sum_a".to_string(),
            func_name: "sum".to_string(),
            args: vec![col("a")],
            distinct: false,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "sum_a".to_string(),
            Expr::Function(sqlparser::ast::Function {
                name: sqlparser::ast::ObjectName(vec![Ident::new("sum")]),
                args: vec![sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(Ident::new("a"))),
                )],
                over: None,
                distinct: false,
                order_by: vec![],
                filter: None,
                null_treatment: None,
                special: false,
            }),
        );

        Arc::new(PhysicalStreamingAggregation::new(
            StreamingWindowSpec::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 2,
                lookahead,
                partition_by_exprs: Vec::new(),
                partition_by_scalars: Vec::new(),
            },
            mappings,
            Vec::new(),
            vec![call],
            Vec::new(),
            Vec::new(),
            0,
        ))
    }

    fn extract_sum(collection: &dyn crate::model::Collection) -> i64 {
        assert_eq!(
            collection.rows().len(),
            1,
            "expected one row per sliding emit"
        );
        let value = collection.rows()[0]
            .value_by_name("", "sum_a")
            .cloned()
            .expect("aggregate value");
        let Value::Int64(sum) = value else {
            panic!("expected Int64 aggregate value, got {value:?}");
        };
        sum
    }

    #[tokio::test]
    async fn sliding_aggregation_emits_oldest_window_per_tuple_when_delay_is_zero() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical();

        let mut processor = StreamingSlidingAggregationProcessor::new(
            "sliding",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("sliding processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at(1, 1),
            tuple_at(2, 2),
            tuple_at(4, 4),
            tuple_at(5, 8),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut sums = Vec::new();
        for _ in 0..4 {
            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            let StreamData::Collection(collection) = item else {
                panic!("expected sliding aggregation collection");
            };
            sums.push(extract_sum(collection.as_ref()));
        }

        assert_eq!(sums, vec![1, 3, 2, 12]);
    }

    #[tokio::test]
    async fn sliding_aggregation_graceful_end_flushes_all_pending_windows_before_terminal_control()
    {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical_with_lookahead(Some(2));

        let mut processor = StreamingSlidingAggregationProcessor::new(
            "sliding",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("sliding processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(1, 1), tuple_at(2, 2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let mut sums = Vec::new();
        loop {
            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match item {
                StreamData::Collection(collection) => sums.push(extract_sum(collection.as_ref())),
                StreamData::Control(ControlSignal::Barrier(
                    crate::processor::BarrierControlSignal::StreamGracefulEnd { .. },
                )) => break,
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(sums, vec![3, 2]);
    }
}
