use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
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
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::processor::window_partition::{eval_partition_key, PartitionKey};

/// Time-driven tumbling window implementation.
pub struct StreamingTumblingAggregationProcessor {
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
    len_secs: u64,
    stats: Arc<ProcessorStats>,
}

impl StreamingTumblingAggregationProcessor {
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
            StreamingWindowSpec::Tumbling {
                partition_by_scalars,
                ..
            } => partition_by_scalars.clone(),
            _ => Vec::new(),
        };
        let len_secs = Self::extract_window_length(physical.as_ref())?;
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
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
            len_secs,
            stats: Arc::new(ProcessorStats::default()),
        })
    }

    fn extract_window_length(
        physical: &PhysicalStreamingAggregation,
    ) -> Result<u64, ProcessorError> {
        match &physical.window {
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length,
                ..
            } => Ok(*length),
            other => Err(ProcessorError::InvalidConfiguration(format!(
                "streaming tumbling aggregation requires tumbling window spec, got {other:?}",
            ))),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for StreamingTumblingAggregationProcessor {
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
        let stats = Arc::clone(&self.stats);
        let len_secs = self.len_secs;

        ProcessorStart::ready(spawner.spawn(async move {
            let mut window_state = PartitionedProcessingWindowState::new(
                len_secs,
                Arc::clone(&physical),
                Arc::clone(&aggregate_registry),
                group_by_meta.clone(),
                partition_by_scalars,
            );

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
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        stats.record_in(collection.num_rows() as u64);
                                        let handle_start = std::time::Instant::now();
                                        for row in collection.rows() {
                                            window_state.add_row(row).map_err(|e| {
                                                ProcessorError::ProcessingError(format!(
                                                    "Failed to update window state: {e}"
                                                ))
                                            })?;
                                        }
                                        stats.record_handle_duration(handle_start.elapsed());
                                    }
                                    StreamData::Watermark(ts) => {
                                        window_state
                                            .flush_until(
                                                ts,
                                                &output,
                                                channel_capacities.data,
                                                &stats,
                                            )
                                            .await?;
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        let is_graceful = control_signal.is_graceful_end();
                                        if is_terminal {
                                            if is_graceful {
                                                window_state
                                                    .flush_all(
                                                        &output,
                                                        channel_capacities.data,
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

/// Per-window aggregation state.
struct WindowAggState {
    start_secs: u64,
    end_secs: u64,
    worker: AggregationWorker,
}

impl WindowAggState {
    fn new(
        start_secs: u64,
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        Self {
            start_secs,
            end_secs: start_secs.saturating_add(len_secs),
            worker: AggregationWorker::new(
                Arc::clone(&physical),
                Arc::clone(&aggregate_registry),
                group_by_meta,
            ),
        }
    }
}

/// Processing-time windows assuming monotonically increasing timestamps.
struct ProcessingWindowState {
    windows: VecDeque<WindowAggState>,
    len_secs: u64,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_meta: Vec<GroupByMeta>,
}

impl ProcessingWindowState {
    fn new(
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        Self {
            windows: VecDeque::new(),
            len_secs,
            physical,
            aggregate_registry,
            group_by_meta,
        }
    }

    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        let start_secs = window_start_secs_str(row.timestamp, self.len_secs)?;
        if let Some(back) = self.windows.back_mut() {
            if back.start_secs == start_secs {
                return back.worker.update_groups(row);
            }
        }
        let new_state = WindowAggState::new(
            start_secs,
            self.len_secs,
            Arc::clone(&self.physical),
            Arc::clone(&self.aggregate_registry),
            self.group_by_meta.clone(),
        );
        self.windows.push_back(new_state);
        // SAFETY: push_back just succeeded; back_mut() is guaranteed Some
        #[allow(clippy::expect_used)]
        self.windows
            .back_mut()
            .expect("window deque must contain the just-pushed window state")
            .worker
            .update_groups(row)
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        let watermark_secs = to_secs(watermark, "watermark")?;
        while let Some(mut state) = self.windows.pop_front() {
            if state.end_secs > watermark_secs {
                self.windows.push_front(state);
                break;
            }
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(ProcessorError::ProcessingError)?
            {
                let batch = window_metadata::attach_from_epoch_secs(
                    batch,
                    state.start_secs,
                    state.end_secs,
                )?;
                stats.record_out(batch.num_rows() as u64);
                send_with_backpressure(
                    output,
                    data_channel_capacity,
                    StreamData::Collection(batch),
                    Some(stats.as_ref()),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        while let Some(mut state) = self.windows.pop_front() {
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(ProcessorError::ProcessingError)?
            {
                let batch = window_metadata::attach_from_epoch_secs(
                    batch,
                    state.start_secs,
                    state.end_secs,
                )?;
                stats.record_out(batch.num_rows() as u64);
                send_with_backpressure(
                    output,
                    data_channel_capacity,
                    StreamData::Collection(batch),
                    Some(stats.as_ref()),
                )
                .await?;
            }
        }
        Ok(())
    }
}

struct PartitionedProcessingWindowState {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<ProcessingWindowState>,
    len_secs: u64,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_meta: Vec<GroupByMeta>,
    partition_by_scalars: Vec<crate::expr::ScalarExpr>,
}

impl PartitionedProcessingWindowState {
    fn new(
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
        partition_by_scalars: Vec<crate::expr::ScalarExpr>,
    ) -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
            len_secs,
            physical,
            aggregate_registry,
            group_by_meta,
            partition_by_scalars,
        }
    }

    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        let partition_key = eval_partition_key(&self.partition_by_scalars, row, "tumblingwindow")?;
        self.get_or_insert(partition_key).add_row(row)
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        for state in &mut self.states {
            state
                .flush_until(watermark, output, data_channel_capacity, stats)
                .await?;
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        for state in &mut self.states {
            state
                .flush_all(output, data_channel_capacity, stats)
                .await?;
        }
        Ok(())
    }

    fn get_or_insert(&mut self, key: PartitionKey) -> &mut ProcessingWindowState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(ProcessingWindowState::new(
            self.len_secs,
            Arc::clone(&self.physical),
            Arc::clone(&self.aggregate_registry),
            self.group_by_meta.clone(),
        ));
        &mut self.states[id]
    }
}

fn window_start_secs_str(ts: SystemTime, len_secs: u64) -> Result<u64, String> {
    let secs = ts
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("invalid timestamp: {e}"))?
        .as_secs();
    let len = len_secs.max(1);
    Ok(secs / len * len)
}

fn to_secs(ts: SystemTime, label: &str) -> Result<u64, ProcessorError> {
    ts.duration_since(UNIX_EPOCH)
        .map_err(|e| ProcessorError::ProcessingError(format!("invalid {label}: {e}")))
        .map(|d| d.as_secs())
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

    fn tuple_at(sec: u64, cols: &[(&str, Value)]) -> crate::model::Tuple {
        let mut tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        );
        for (k, v) in cols {
            tuple.add_affiliate_column(Arc::new((*k).to_string()), v.clone());
        }
        tuple
    }

    fn make_physical() -> Arc<PhysicalStreamingAggregation> {
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
            StreamingWindowSpec::Tumbling {
                time_unit: TimeUnit::Seconds,
                length: 10,
                partition_by_exprs: Vec::new(),
                partition_by_scalars: Vec::new(),
            },
            mappings,
            vec![Expr::Nested(Box::new(Expr::Identifier(Ident::new("b"))))],
            vec![call],
            vec![col("b")],
            Vec::new(),
            0,
        ))
    }

    fn extract_grouped_rows(collection: &dyn crate::model::Collection) -> Vec<(i64, i64)> {
        let mut rows = collection
            .rows()
            .iter()
            .map(|tuple| {
                let group = tuple
                    .value_by_name("", "(b)")
                    .cloned()
                    .expect("group-by value");
                let sum = tuple
                    .value_by_name("", "sum_a")
                    .cloned()
                    .expect("aggregate value");
                let Value::Int64(group) = group else {
                    panic!("expected Int64 group value, got {group:?}");
                };
                let Value::Int64(sum) = sum else {
                    panic!("expected Int64 aggregate value, got {sum:?}");
                };
                (group, sum)
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|(group, _)| *group);
        rows
    }

    #[tokio::test]
    async fn tumbling_aggregation_flushes_grouped_rows_on_watermark() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical();

        let mut processor = StreamingTumblingAggregationProcessor::new(
            "tumbling",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("tumbling processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let first_window = crate::model::RecordBatch::new(vec![
            tuple_at(1, &[("a", Value::Int64(2)), ("b", Value::Int64(1))]),
            tuple_at(2, &[("a", Value::Int64(4)), ("b", Value::Int64(2))]),
            tuple_at(3, &[("a", Value::Int64(3)), ("b", Value::Int64(1))]),
        ])
        .expect("first batch");
        assert!(input
            .send(StreamData::collection(Box::new(first_window)))
            .is_ok());
        assert!(input
            .send(StreamData::Watermark(UNIX_EPOCH + Duration::from_secs(10)))
            .is_ok());

        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(first) = first else {
            panic!("expected first window collection");
        };
        assert_eq!(extract_grouped_rows(first.as_ref()), vec![(1, 5), (2, 4)]);

        let second_window = crate::model::RecordBatch::new(vec![tuple_at(
            11,
            &[("a", Value::Int64(7)), ("b", Value::Int64(2))],
        )])
        .expect("second batch");
        assert!(input
            .send(StreamData::collection(Box::new(second_window)))
            .is_ok());
        assert!(input
            .send(StreamData::Watermark(UNIX_EPOCH + Duration::from_secs(20)))
            .is_ok());

        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(second) = second else {
            panic!("expected second window collection");
        };
        assert_eq!(extract_grouped_rows(second.as_ref()), vec![(2, 7)]);
    }

    #[tokio::test]
    async fn tumbling_aggregation_graceful_end_flushes_open_window_before_terminal_control() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical();

        let mut processor = StreamingTumblingAggregationProcessor::new(
            "tumbling",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("tumbling processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at(1, &[("a", Value::Int64(2)), ("b", Value::Int64(1))]),
            tuple_at(2, &[("a", Value::Int64(3)), ("b", Value::Int64(1))]),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");

        let StreamData::Collection(collection) = first else {
            panic!("expected tumbling aggregation output before terminal control");
        };
        assert_eq!(extract_grouped_rows(collection.as_ref()), vec![(1, 5)]);
        assert!(matches!(
            second,
            StreamData::Control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { .. }
            ))
        ));
    }
}
