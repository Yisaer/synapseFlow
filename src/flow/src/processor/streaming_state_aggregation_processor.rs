use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::expr::ScalarExpr;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

struct PartitionAggState {
    active: bool,
    worker: AggregationWorker,
}

struct StateWindowConfig {
    open_expr: Expr,
    emit_expr: Expr,
    open_scalar: ScalarExpr,
    emit_scalar: ScalarExpr,
    partition_by_scalars: Vec<ScalarExpr>,
}

pub struct StreamingStateAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    group_by_meta: Vec<GroupByMeta>,
    state_window: Arc<StateWindowConfig>,
    stats: Arc<ProcessorStats>,
}

impl StreamingStateAggregationProcessor {
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
        let state_window = Arc::new(Self::extract_window_spec(physical.as_ref())?);
        let (output, _) = broadcast::channel(channel_capacities.data);
        let (control_output, _) = broadcast::channel(channel_capacities.control);
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
            state_window,
            stats: Arc::new(ProcessorStats::default()),
        })
    }

    fn extract_window_spec(
        physical: &PhysicalStreamingAggregation,
    ) -> Result<StateWindowConfig, ProcessorError> {
        match &physical.window {
            StreamingWindowSpec::State {
                open_expr,
                emit_expr,
                open_scalar,
                emit_scalar,
                partition_by_scalars,
                ..
            } => Ok(StateWindowConfig {
                open_expr: open_expr.clone(),
                emit_expr: emit_expr.clone(),
                open_scalar: open_scalar.clone(),
                emit_scalar: emit_scalar.clone(),
                partition_by_scalars: partition_by_scalars.clone(),
            }),
            other => Err(ProcessorError::InvalidConfiguration(format!(
                "streaming state aggregation requires state window spec, got {other:?}",
            ))),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    async fn flush_active_partitions(
        partitions: &mut HashMap<Option<String>, PartitionAggState>,
        output: &broadcast::Sender<StreamData>,
        channel_capacities: ProcessorChannelCapacities,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        for state in partitions.values_mut() {
            if !state.active {
                continue;
            }

            match state.worker.finalize_current_window() {
                Ok(Some(batch)) => {
                    stats.record_out(batch.num_rows() as u64);
                    send_with_backpressure(
                        output,
                        channel_capacities.data,
                        StreamData::Collection(batch),
                        Some(stats.as_ref()),
                    )
                    .await?;
                }
                Ok(None) => {}
                Err(e) => {
                    stats.record_error_logged(
                        "streaming state aggregation processor error",
                        e.to_string(),
                    );
                }
            }
            state.active = false;
        }

        Ok(())
    }
}

impl Processor for StreamingStateAggregationProcessor {
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
        let stats = Arc::clone(&self.stats);
        let state_window = Arc::clone(&self.state_window);

        ProcessorStart::ready(spawner.spawn(async move {
            let mut partitions: HashMap<Option<String>, PartitionAggState> = HashMap::new();
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
                                let tuples = match collection.into_rows() {
                                    Ok(rows) => rows,
                                    Err(e) => {
                                        stats.record_error_logged("streaming state aggregation processor error", format!("failed to extract rows: {e}"));
                                        stats.record_handle_duration(handle_start.elapsed());
                                        continue;
                                    }
                                };

                                for tuple in tuples {
                                    let partition_key = if state_window.partition_by_scalars.is_empty() {
                                        None
                                    } else {
                                        let mut key_values = Vec::with_capacity(state_window.partition_by_scalars.len());
                                        for expr in &state_window.partition_by_scalars {
                                            match expr.eval_with_tuple(&tuple) {
                                                Ok(v) => key_values.push(v),
                                                Err(e) => {
                                                    stats.record_error_logged("streaming state aggregation processor error", format!(
                                                        "failed to evaluate statewindow partition key: {e}"
                                                    ));
                                                    key_values.clear();
                                                    break;
                                                }
                                            }
                                        }
                                        if key_values.is_empty() {
                                            continue;
                                        }
                                        Some(format!("{:?}", key_values))
                                    };

                                    let entry = partitions.entry(partition_key).or_insert_with(|| {
                                        PartitionAggState {
                                            active: false,
                                            worker: AggregationWorker::new(
                                                Arc::clone(&physical),
                                                Arc::clone(&aggregate_registry),
                                                group_by_meta.clone(),
                                            ),
                                        }
                                    });

                                    let open = match state_window.open_scalar.eval_with_tuple(&tuple) {
                                        Ok(Value::Bool(v)) => v,
                                        Ok(other) => {
                                            stats.record_error_logged("streaming state aggregation processor error", format!(
                                                "statewindow open must be bool, got {other:?} (expr={})",
                                                state_window.open_expr
                                            ));
                                            continue;
                                        }
                                        Err(e) => {
                                            stats.record_error_logged("streaming state aggregation processor error", format!(
                                                "failed to evaluate statewindow open (expr={}): {e}",
                                                state_window.open_expr
                                            ));
                                            continue;
                                        }
                                    };

                                    let emit = match state_window.emit_scalar.eval_with_tuple(&tuple) {
                                        Ok(Value::Bool(v)) => v,
                                        Ok(other) => {
                                            stats.record_error_logged("streaming state aggregation processor error", format!(
                                                "statewindow emit must be bool, got {other:?} (expr={})",
                                                state_window.emit_expr
                                            ));
                                            continue;
                                        }
                                        Err(e) => {
                                            stats.record_error_logged("streaming state aggregation processor error", format!(
                                                "failed to evaluate statewindow emit (expr={}): {e}",
                                                state_window.emit_expr
                                            ));
                                            continue;
                                        }
                                    };

                                    if !entry.active {
                                        if open {
                                            entry.active = true;
                                            if let Err(e) = entry.worker.update_groups(&tuple) {
                                                stats.record_error_logged("streaming state aggregation processor error", e.to_string());
                                            }
                                        }
                                        continue;
                                    }

                                    if let Err(e) = entry.worker.update_groups(&tuple) {
                                        stats.record_error_logged("streaming state aggregation processor error", e.to_string());
                                        continue;
                                    }

                                    if emit {
                                        match entry.worker.finalize_current_window() {
                                            Ok(Some(batch)) => {
                                                stats.record_out(batch.num_rows() as u64);
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    StreamData::Collection(batch),
                                                    Some(stats.as_ref()),
                                                )
                                                .await;
                                                if let Err(err) = send_res {
                                                    stats.record_handle_duration(handle_start.elapsed());
                                                    return Err(err);
                                                }
                                            }
                                            Ok(None) => {}
                                            Err(e) => {
                                                stats.record_error_logged("streaming state aggregation processor error", e.to_string());
                                            }
                                        }
                                        entry.active = false;
                                    }
                                }
                                stats.record_handle_duration(handle_start.elapsed());
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    StreamData::watermark(ts),
                                    Some(stats.as_ref()),
                                )
                                .await?;
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                let is_graceful = control_signal.is_graceful_end();
                                if is_terminal {
                                    if is_graceful {
                                        Self::flush_active_partitions(
                                            &mut partitions,
                                            &output,
                                            channel_capacities,
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
                                let is_terminal = other.is_terminal();
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    other,
                                    Some(stats.as_ref()),
                                )
                                .await?;
                                if is_terminal {
                                    break;
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &id,
                                    skipped,
                                    "streaming state aggregation data input",
                                );
                                continue;
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ScalarExpr;
    use crate::planner::physical::AggregateCall;
    use crate::planner::physical::PhysicalStreamingAggregation;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::runtime::TaskSpawner;
    use datatypes::{BooleanType, ConcreteDatatype, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn lit_bool(v: bool) -> ScalarExpr {
        ScalarExpr::Literal(Value::Bool(v), ConcreteDatatype::Bool(BooleanType))
    }

    fn col(name: &str) -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: name.to_string(),
        })
    }

    fn tuple_with(cols: &[(&str, Value)]) -> crate::model::Tuple {
        let mut tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            std::time::UNIX_EPOCH,
        );
        for (k, v) in cols {
            tuple.add_affiliate_column(Arc::new((*k).to_string()), v.clone());
        }
        tuple
    }

    fn make_physical(
        open: ScalarExpr,
        emit: ScalarExpr,
        partition_by: Vec<ScalarExpr>,
    ) -> Arc<PhysicalStreamingAggregation> {
        let call = AggregateCall {
            output_column: "col_1".to_string(),
            func_name: "sum".to_string(),
            args: vec![col("a")],
            distinct: false,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "col_1".to_string(),
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

        let window = StreamingWindowSpec::State {
            open_expr: Expr::Identifier(Ident::new("open")),
            emit_expr: Expr::Identifier(Ident::new("emit")),
            partition_by_exprs: vec![],
            open_scalar: open,
            emit_scalar: emit,
            partition_by_scalars: partition_by,
        };

        Arc::new(PhysicalStreamingAggregation::new(
            window,
            mappings,
            Vec::new(),
            vec![call],
            Vec::new(),
            Vec::new(),
            0,
        ))
    }

    #[tokio::test]
    async fn streaming_state_agg_open_then_emit_outputs_sum() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical(lit_bool(true), lit_bool(true), Vec::new());

        let mut processor = StreamingStateAggregationProcessor::new(
            "s",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("state processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // First tuple opens (emit ignored because it was inactive).
        let batch = crate::model::RecordBatch::new(vec![tuple_with(&[("a", Value::Int64(1))])])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());

        // Second tuple updates and triggers emit -> sum includes both.
        let batch = crate::model::RecordBatch::new(vec![tuple_with(&[("a", Value::Int64(2))])])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        match output_rx.recv().await.unwrap() {
            StreamData::Collection(collection) => {
                assert_eq!(collection.rows().len(), 1);
                let out = collection.rows().first().unwrap();
                assert_eq!(out.value_by_name("", "col_1"), Some(&Value::Int64(3)));
            }
            other => panic!("unexpected output: {}", other.description()),
        }
    }

    #[tokio::test]
    async fn streaming_state_agg_partitioned_by_key_isolated() {
        let spawner = test_spawner();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let physical = make_physical(lit_bool(true), lit_bool(true), vec![col("k")]);

        let mut processor = StreamingStateAggregationProcessor::new(
            "s",
            Arc::clone(&physical),
            Arc::clone(&aggregate_registry),
        )
        .expect("state processor");
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // Interleaved keys; each needs its own open+emit cycle.
        let batch = crate::model::RecordBatch::new(vec![
            tuple_with(&[("k", Value::Int64(1)), ("a", Value::Int64(1))]),
            tuple_with(&[("k", Value::Int64(2)), ("a", Value::Int64(10))]),
            tuple_with(&[("k", Value::Int64(1)), ("a", Value::Int64(2))]),
            tuple_with(&[("k", Value::Int64(2)), ("a", Value::Int64(20))]),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let first = output_rx.recv().await.unwrap();
        let second = output_rx.recv().await.unwrap();

        let (b1, b2) = match (first, second) {
            (StreamData::Collection(c1), StreamData::Collection(c2)) => (c1, c2),
            (StreamData::Collection(c1), other) => {
                let next = output_rx.recv().await.unwrap();
                match next {
                    StreamData::Collection(c2) => (c1, c2),
                    other2 => panic!(
                        "expected second collection, got {} then {}",
                        other.description(),
                        other2.description()
                    ),
                }
            }
            (other1, other2) => panic!(
                "expected collections, got {} and {}",
                other1.description(),
                other2.description()
            ),
        };

        assert_eq!(b1.rows().len(), 1);
        assert_eq!(b2.rows().len(), 1);
        let v1 = b1
            .rows()
            .first()
            .unwrap()
            .value_by_name("", "col_1")
            .cloned()
            .unwrap_or(Value::Null);
        let v2 = b2
            .rows()
            .first()
            .unwrap()
            .value_by_name("", "col_1")
            .cloned()
            .unwrap_or(Value::Null);

        // key=1 sum is 1+2=3, key=2 sum is 10+20=30.
        let mut seen = vec![v1, v2];
        seen.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(seen, vec![Value::Int64(3), Value::Int64(30)]);
    }
}
