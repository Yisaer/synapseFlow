//! StateWindowProcessor - buffers rows between open/emit conditions.
//!
//! Semantics:
//! - When inactive and `open == true`, start buffering (do not emit even if `emit == true`).
//! - When active, buffer every incoming tuple. If `emit == true`, emit the buffered batch and close.
//! - When inactive and `emit == true`, ignore.

use crate::planner::physical::{PhysicalPlan, PhysicalStateWindow};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

struct PartitionState {
    active: bool,
    rows: VecDeque<crate::model::Tuple>,
}

pub struct StateWindowProcessor {
    id: String,
    physical: Arc<PhysicalStateWindow>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl StateWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalStateWindow>) -> Self {
        Self::new_with_channel_capacities(id, physical, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStateWindow>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::StateWindow(window) => {
                Some(Self::new(id, Arc::new(window.as_ref().clone())))
            }
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for StateWindowProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);

        let open_expr = self.physical.open_scalar.clone();
        let emit_expr = self.physical.emit_scalar.clone();
        let partition_by_exprs = self.physical.partition_by_scalars.clone();

        ProcessorStart::ready(spawner.spawn(async move {
            let mut partitions: HashMap<Option<String>, PartitionState> = HashMap::new();

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
                                tracing::info!(processor_id = %id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                stats.record_in(collection.num_rows() as u64);
                                let handle_start = std::time::Instant::now();
                                let tuples = match collection.into_rows() {
                                    Ok(rows) => rows,
                                    Err(e) => {
                                        stats.record_error_logged("state window processor error", format!("failed to extract rows: {e}"));
                                        stats.record_handle_duration(handle_start.elapsed());
                                        continue;
                                    }
                                };

                                for tuple in tuples {
                                    let partition_key = if partition_by_exprs.is_empty() {
                                        None
                                    } else {
                                        let mut key_values =
                                            Vec::with_capacity(partition_by_exprs.len());
                                        for expr in &partition_by_exprs {
                                            match expr.eval_with_tuple(&tuple) {
                                                Ok(v) => key_values.push(v),
                                                Err(e) => {
                                                    stats.record_error_logged("state window processor error", format!(
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

                                    let state =
                                        partitions.entry(partition_key).or_insert_with(|| {
                                            PartitionState {
                                                active: false,
                                                rows: VecDeque::new(),
                                            }
                                        });

                                    let open = match open_expr.eval_with_tuple(&tuple) {
                                        Ok(Value::Bool(v)) => v,
                                        Ok(other) => {
                                            stats.record_error_logged("state window processor error", format!(
                                                "statewindow open must be bool, got {other:?}"
                                            ));
                                            continue;
                                        }
                                        Err(e) => {
                                            stats.record_error_logged("state window processor error", format!(
                                                "failed to evaluate statewindow open: {e}"
                                            ));
                                            continue;
                                        }
                                    };

                                    let emit = match emit_expr.eval_with_tuple(&tuple) {
                                        Ok(Value::Bool(v)) => v,
                                        Ok(other) => {
                                            stats.record_error_logged("state window processor error", format!(
                                                "statewindow emit must be bool, got {other:?}"
                                            ));
                                            continue;
                                        }
                                        Err(e) => {
                                            stats.record_error_logged("state window processor error", format!(
                                                "failed to evaluate statewindow emit: {e}"
                                            ));
                                            continue;
                                        }
                                    };

                                    if !state.active {
                                        if open {
                                            state.active = true;
                                            state.rows.push_back(tuple);
                                        }
                                        continue;
                                    }

                                    state.rows.push_back(tuple);
                                    if emit {
                                        if state.rows.is_empty() {
                                            state.active = false;
                                            continue;
                                        }
                                        let batch_rows: Vec<_> = state.rows.drain(..).collect();
                                        stats.record_out(batch_rows.len() as u64);
                                        let batch = match crate::model::RecordBatch::new(batch_rows)
                                            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))
                                        {
                                            Ok(batch) => batch,
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                return Err(e);
                                            }
                                        };
                                        let send_res = send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            StreamData::collection(Box::new(batch)),
                                            Some(stats.as_ref()),
                                        )
                                        .await;
                                        if let Err(e) = send_res {
                                            stats.record_handle_duration(handle_start.elapsed());
                                            return Err(e);
                                        }
                                        state.active = false;
                                    }
                                }
                                // For synchronous processors, handle duration includes downstream send/backpressure time.
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
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                let is_graceful = signal.is_graceful_end();
                                if is_terminal {
                                    if is_graceful {
                                        for state in partitions.values_mut() {
                                            if state.active && !state.rows.is_empty() {
                                                let batch_rows: Vec<_> =
                                                    state.rows.drain(..).collect();
                                                stats.record_out(batch_rows.len() as u64);
                                                let batch = crate::model::RecordBatch::new(batch_rows)
                                                    .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
                                                send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    StreamData::collection(Box::new(batch)),
                                                    Some(stats.as_ref()),
                                                )
                                                .await?;
                                                state.active = false;
                                            }
                                        }
                                    }
                                    send_with_backpressure(
                                        &output,
                                        channel_capacities.data,
                                        StreamData::control(signal),
                                        Some(stats.as_ref()),
                                    )
                                    .await?;
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    StreamData::control(signal),
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
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "state window data input");
                                continue;
                            }
                            None => {
                                tracing::info!(processor_id = %id, "stopped");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ScalarExpr;
    use crate::processor::base::{Processor, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::processor::ProcessorStats;
    use crate::runtime::TaskSpawner;
    use datatypes::{BooleanType, ConcreteDatatype, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        )
    }

    fn tuple_with_key_at(sec: u64, key: i64) -> crate::model::Tuple {
        let mut tuple = tuple_at(sec);
        tuple.add_affiliate_column(std::sync::Arc::new("k".to_string()), Value::Int64(key));
        tuple
    }

    fn lit_bool(v: bool) -> ScalarExpr {
        ScalarExpr::Literal(Value::Bool(v), ConcreteDatatype::Bool(BooleanType))
    }

    // coverage-covers: stream.window.state
    #[tokio::test]
    async fn statewindow_open_then_emit_buffers_and_flushes() {
        let spawner = test_spawner();
        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            Vec::new(),
            lit_bool(true),
            lit_bool(true),
            Vec::new(),
            Vec::new(),
            0,
        );
        let mut processor = StateWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        // First tuple opens the window (emit ignored because it was inactive).
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());

        // Second tuple is buffered and triggers emit -> output includes both tuples.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        match output_rx.recv().await.unwrap() {
            StreamData::Collection(collection) => {
                assert_eq!(collection.rows().len(), 2);
            }
            other => panic!("unexpected output: {}", other.description()),
        }
    }

    #[tokio::test]
    async fn statewindow_emit_ignored_when_inactive() {
        let spawner = test_spawner();
        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            Vec::new(),
            lit_bool(false),
            lit_bool(true),
            Vec::new(),
            Vec::new(),
            0,
        );
        let mut processor = StateWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn statewindow_partition_by_independent_buffers() {
        let spawner = test_spawner();
        let key_expr = ScalarExpr::Column(ColumnRef::ByName {
            column_name: "k".to_string(),
        });

        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            vec![Expr::Identifier(Ident::new("k"))],
            lit_bool(true),
            lit_bool(true),
            vec![key_expr],
            Vec::new(),
            0,
        );

        let mut processor = StateWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_with_key_at(1, 1),
            tuple_with_key_at(2, 2),
            tuple_with_key_at(3, 1),
            tuple_with_key_at(4, 2),
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

        assert_eq!(b1.rows().len(), 2);
        assert_eq!(b2.rows().len(), 2);
        for t in b1.rows() {
            assert_eq!(t.value_by_name("", "k"), Some(&Value::Int64(1)));
        }
        for t in b2.rows() {
            assert_eq!(t.value_by_name("", "k"), Some(&Value::Int64(2)));
        }
    }

    #[tokio::test]
    async fn statewindow_partition_by_graceful_end_flushes_all_partitions() {
        let spawner = test_spawner();
        let key_expr = ScalarExpr::Column(ColumnRef::ByName {
            column_name: "k".to_string(),
        });

        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            vec![Expr::Identifier(Ident::new("k"))],
            lit_bool(true),
            lit_bool(false),
            vec![key_expr],
            Vec::new(),
            0,
        );

        let mut processor = StateWindowProcessor::new("sw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_with_key_at(1, 1), tuple_with_key_at(2, 2)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;

        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let mut collections = Vec::new();
        while collections.len() < 2 {
            match output_rx.recv().await.unwrap() {
                StreamData::Collection(c) => collections.push(c),
                StreamData::Control(_) => {}
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert!(collections.iter().all(|c| c.rows().len() == 1));
        let mut seen = collections
            .iter()
            .flat_map(|c| c.rows().iter())
            .filter_map(|t| t.value_by_name("", "k").cloned())
            .collect::<Vec<_>>();
        seen.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(seen, vec![Value::Int64(1), Value::Int64(2)]);
        assert_eq!(stats.snapshot().records_out, 2);
    }
}
