//! SlidingWindowProcessor - emits sliding windows triggered by incoming data.
//!
//! Processing-time mode assumes tuple timestamps are non-decreasing.
//! Window flushing for lookahead windows is driven by incoming watermarks.

use crate::expr::ScalarExpr;
use crate::planner::logical::TimeUnit;
use crate::planner::physical::{PhysicalPlan, PhysicalSlidingWindow, PipelineStateUsage};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::pipeline_state_runtime::update_row_hit_state;
use crate::processor::processor_state::ProcessorState;
use crate::processor::sliding_window_runtime::evaluate_trigger_condition;
use crate::processor::window_metadata;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

#[derive(Clone)]
struct SlidingTriggerRuntime {
    condition: Option<ScalarExpr>,
    state: Option<Arc<ProcessorState>>,
    state_usage: PipelineStateUsage,
}

impl SlidingTriggerRuntime {
    fn update_after_hit(&self, timestamp: SystemTime) -> Result<(), ProcessorError> {
        if let Some(state) = self.state.as_deref() {
            update_row_hit_state(state, self.state_usage, timestamp)?;
        }
        Ok(())
    }
}

pub struct SlidingWindowProcessor {
    id: String,
    lookback: Duration,
    lookahead: Option<Duration>,
    partition_by_scalars: Vec<ScalarExpr>,
    trigger_condition_scalar: Option<ScalarExpr>,
    trigger_processor_state: Option<Arc<ProcessorState>>,
    trigger_state_usage: PipelineStateUsage,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl SlidingWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalSlidingWindow>) -> Self {
        Self::new_with_channel_capacities(id, physical, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalSlidingWindow>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        let lookback = match physical.time_unit {
            TimeUnit::Seconds => Duration::from_secs(physical.lookback),
        };
        let lookahead = match physical.time_unit {
            TimeUnit::Seconds => physical.lookahead.map(Duration::from_secs),
        };
        let partition_by_scalars = physical.partition_by_scalars.clone();
        let trigger_condition_scalar = physical.trigger_condition_scalar.clone();
        let trigger_processor_state = physical.trigger_processor_state.clone();
        let trigger_state_usage = physical.trigger_state_usage;
        Self {
            id: id.into(),
            lookback,
            lookahead,
            partition_by_scalars,
            trigger_condition_scalar,
            trigger_processor_state,
            trigger_state_usage,
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
            PhysicalPlan::SlidingWindow(window) => Some(Self::new(id, Arc::new(window.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for SlidingWindowProcessor {
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

        let lookback = self.lookback;
        let lookahead = self.lookahead;
        let partition_by_scalars = self.partition_by_scalars.clone();
        let trigger = SlidingTriggerRuntime {
            condition: self.trigger_condition_scalar.clone(),
            state: self.trigger_processor_state.clone(),
            state_usage: self.trigger_state_usage,
        };

        let stats = Arc::clone(&self.stats);
        let mut state = PartitionedProcessingState::new(
            lookback,
            lookahead,
            partition_by_scalars,
            trigger,
            output.clone(),
            channel_capacities.data,
            Arc::clone(&stats),
        );

        ProcessorStart::ready(spawner.spawn(async move {
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
                                state.record_in(collection.num_rows() as u64);
                                let handle_start = std::time::Instant::now();
                                let res = state.add_collection(collection).await;
                                // For synchronous processors, handle duration includes downstream send/backpressure time.
                                stats.record_handle_duration(handle_start.elapsed());
                                if let Err(err) = res {
                                    if matches!(err, ProcessorError::ChannelClosed) {
                                        return Err(err);
                                    }
                                    stats.record_error_logged(
                                        "sliding window processor error",
                                        err.to_string(),
                                    );
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                state.flush_up_to(ts).await?;
                            }
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                let is_graceful = signal.is_graceful_end();
                                if is_terminal {
                                    if is_graceful {
                                        state.flush_all().await?;
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
                                log_broadcast_lagged(&id, skipped, "sliding window data input");
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

struct PartitionedProcessingState {
    state_ids: HashMap<PartitionKey, usize>,
    states: Vec<ProcessingState>,
    lookback: Duration,
    lookahead: Option<Duration>,
    partition_by_scalars: Vec<ScalarExpr>,
    trigger: SlidingTriggerRuntime,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
}

impl PartitionedProcessingState {
    fn new(
        lookback: Duration,
        lookahead: Option<Duration>,
        partition_by_scalars: Vec<ScalarExpr>,
        trigger: SlidingTriggerRuntime,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            state_ids: HashMap::new(),
            states: Vec::new(),
            lookback,
            lookahead,
            partition_by_scalars,
            trigger,
            output,
            data_channel_capacity,
            stats,
        }
    }

    fn record_in(&self, rows: u64) {
        self.stats.record_in(rows);
    }

    async fn add_collection(
        &mut self,
        collection: Box<dyn crate::model::Collection>,
    ) -> Result<(), ProcessorError> {
        let rows = match collection.into_rows() {
            Ok(rows) => rows,
            Err(e) => {
                self.stats.record_error_logged(
                    "sliding window processor error",
                    format!("failed to extract rows: {e}"),
                );
                return Ok(());
            }
        };
        for tuple in rows {
            let partition_key =
                match eval_partition_key(&self.partition_by_scalars, &tuple, "slidingwindow") {
                    Ok(key) => key,
                    Err(message) => {
                        self.stats
                            .record_error_logged("sliding window processor error", message);
                        continue;
                    }
                };
            let should_trigger = match evaluate_trigger_condition(
                self.trigger.condition.as_ref(),
                &tuple,
                "slidingwindow trigger condition",
            ) {
                Ok(value) => value,
                Err(err) => {
                    self.stats
                        .record_error_logged("sliding window processor error", err.to_string());
                    false
                }
            };
            let trigger_end = if should_trigger {
                let end = match self.lookahead {
                    Some(lookahead) => match tuple.timestamp.checked_add(lookahead) {
                        Some(end) => end,
                        None => {
                            self.stats.record_error_logged(
                                "sliding window processor error",
                                "slidingwindow end timestamp overflow",
                            );
                            continue;
                        }
                    },
                    None => tuple.timestamp,
                };
                match window_metadata::validate_system_time(end) {
                    Ok(()) => Some(end),
                    Err(err) => {
                        self.stats
                            .record_error_logged("sliding window processor error", err.to_string());
                        continue;
                    }
                }
            } else {
                None
            };
            if should_trigger {
                match self.trigger.update_after_hit(tuple.timestamp) {
                    Ok(()) => {}
                    Err(err) => {
                        self.stats
                            .record_error_logged("sliding window processor error", err.to_string());
                        continue;
                    }
                }
            }
            if let Err(err) = self
                .state_for(partition_key)
                .add_tuple(tuple, trigger_end)
                .await
            {
                if matches!(err, ProcessorError::ChannelClosed) {
                    return Err(err);
                }
                self.stats
                    .record_error_logged("sliding window processor error", err.to_string());
            }
        }
        Ok(())
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        for state in &mut self.states {
            state.flush_up_to(watermark).await?;
        }
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        for state in &mut self.states {
            state.flush_all().await?;
        }
        Ok(())
    }

    fn state_for(&mut self, key: PartitionKey) -> &mut ProcessingState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(ProcessingState::new(
            self.lookback,
            self.lookahead,
            self.output.clone(),
            self.data_channel_capacity,
            Arc::clone(&self.stats),
        ));
        &mut self.states[id]
    }
}

#[derive(Debug, Clone)]
struct WindowRequest {
    start: SystemTime,
    end: SystemTime,
}

/// Processing-time sliding window state (monotonic timestamps).
enum ProcessingState {
    WithLookahead(ProcessingWithLookaheadState),
    WithoutLookahead(ProcessingWithoutLookaheadState),
}

impl ProcessingState {
    fn new(
        lookback: Duration,
        lookahead: Option<Duration>,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        match lookahead {
            Some(_) => Self::WithLookahead(ProcessingWithLookaheadState::new(
                lookback,
                output,
                data_channel_capacity,
                Arc::clone(&stats),
            )),
            None => Self::WithoutLookahead(ProcessingWithoutLookaheadState::new(
                lookback,
                output,
                data_channel_capacity,
                stats,
            )),
        }
    }

    async fn add_tuple(
        &mut self,
        tuple: crate::model::Tuple,
        trigger_end: Option<SystemTime>,
    ) -> Result<(), ProcessorError> {
        match self {
            ProcessingState::WithLookahead(state) => state.add_tuple(tuple, trigger_end).await,
            ProcessingState::WithoutLookahead(state) => state.add_tuple(tuple, trigger_end).await,
        }
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        match self {
            ProcessingState::WithLookahead(state) => state.flush_up_to(watermark).await,
            ProcessingState::WithoutLookahead(state) => state.flush_up_to(watermark).await,
        }
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        match self {
            ProcessingState::WithLookahead(state) => state.flush_all().await,
            ProcessingState::WithoutLookahead(state) => state.flush_all().await,
        }
    }
}

struct ProcessingWithoutLookaheadState {
    rows: VecDeque<crate::model::Tuple>,
    lookback: Duration,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
}

impl ProcessingWithoutLookaheadState {
    fn new(
        lookback: Duration,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            lookback,
            output,
            data_channel_capacity,
            stats,
        }
    }

    async fn add_tuple(
        &mut self,
        tuple: crate::model::Tuple,
        trigger_end: Option<SystemTime>,
    ) -> Result<(), ProcessorError> {
        let t = tuple.timestamp;
        self.rows.push_back(tuple);
        let Some(end) = trigger_end else {
            return Ok(());
        };
        let start = t
            .checked_sub(self.lookback)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        self.emit_window(start, end).await
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        // Without lookahead, windows are emitted on data. Watermarks are still used to drive GC.
        self.trim(watermark);
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        Ok(())
    }

    fn trim(&mut self, watermark: SystemTime) {
        let min_start = watermark
            .checked_sub(self.lookback)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        while let Some(front) = self.rows.front() {
            if front.timestamp >= min_start {
                break;
            }
            self.rows.pop_front();
        }
    }

    async fn emit_window(&self, start: SystemTime, end: SystemTime) -> Result<(), ProcessorError> {
        let mut rows = Vec::new();
        for row in self.rows.iter() {
            if row.timestamp < start {
                continue;
            }
            if row.timestamp > end {
                break;
            }
            rows.push(row.clone());
        }
        let row_count = rows.len() as u64;
        let batch = match window_metadata::record_batch_from_system_time(rows, start, end) {
            Ok(batch) => batch,
            Err(err) => {
                self.stats
                    .record_error_logged("sliding window processor error", err.to_string());
                return Ok(());
            }
        };
        send_with_backpressure(
            &self.output,
            self.data_channel_capacity,
            StreamData::collection(Box::new(batch)),
            Some(self.stats.as_ref()),
        )
        .await?;
        self.stats.record_out(row_count);
        Ok(())
    }
}

struct ProcessingWithLookaheadState {
    rows: VecDeque<crate::model::Tuple>,
    pending: VecDeque<WindowRequest>,
    lookback: Duration,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
}

impl ProcessingWithLookaheadState {
    fn new(
        lookback: Duration,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            pending: VecDeque::new(),
            lookback,
            output,
            data_channel_capacity,
            stats,
        }
    }

    async fn add_tuple(
        &mut self,
        tuple: crate::model::Tuple,
        trigger_end: Option<SystemTime>,
    ) -> Result<(), ProcessorError> {
        let t = tuple.timestamp;
        self.rows.push_back(tuple);
        let Some(end) = trigger_end else {
            return Ok(());
        };
        let start = t
            .checked_sub(self.lookback)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        self.pending.push_back(WindowRequest { start, end });
        Ok(())
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        while let Some(request) = self.pending.pop_front() {
            if request.end > watermark {
                self.pending.push_front(request);
                break;
            }
            if let Err(err) = self.emit_window(request.start, request.end).await {
                if matches!(err, ProcessorError::ChannelClosed) {
                    return Err(err);
                }
                self.stats
                    .record_error_logged("sliding window processor error", err.to_string());
            }
        }
        self.trim(watermark);
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        while let Some(request) = self.pending.pop_front() {
            if let Err(err) = self.emit_window(request.start, request.end).await {
                if matches!(err, ProcessorError::ChannelClosed) {
                    return Err(err);
                }
                self.stats
                    .record_error_logged("sliding window processor error", err.to_string());
            }
        }
        Ok(())
    }

    fn trim(&mut self, watermark: SystemTime) {
        let min_start = if let Some(front) = self.pending.front() {
            front.start
        } else {
            watermark
                .checked_sub(self.lookback)
                .unwrap_or(SystemTime::UNIX_EPOCH)
        };
        while let Some(front) = self.rows.front() {
            if front.timestamp >= min_start {
                break;
            }
            self.rows.pop_front();
        }
    }

    async fn emit_window(&self, start: SystemTime, end: SystemTime) -> Result<(), ProcessorError> {
        let mut rows = Vec::new();
        for row in self.rows.iter() {
            if row.timestamp < start {
                continue;
            }
            if row.timestamp > end {
                break;
            }
            rows.push(row.clone());
        }
        let row_count = rows.len() as u64;
        let batch = match window_metadata::record_batch_from_system_time(rows, start, end) {
            Ok(batch) => batch,
            Err(err) => {
                self.stats
                    .record_error_logged("sliding window processor error", err.to_string());
                return Ok(());
            }
        };
        send_with_backpressure(
            &self.output,
            self.data_channel_capacity,
            StreamData::collection(Box::new(batch)),
            Some(self.stats.as_ref()),
        )
        .await?;
        self.stats.record_out(row_count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::func::BinaryFunc;
    use crate::expr::scalar::ColumnRef;
    use crate::expr::ProcStateField;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::processor::processor_state::ProcessorState;
    use crate::runtime::TaskSpawner;
    use datatypes::{ConcreteDatatype, Int64Type, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use tokio::time::timeout;

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

    fn tuple_with_a_at(timestamp: SystemTime, value: i64) -> crate::model::Tuple {
        let mut tuple =
            crate::model::Tuple::with_timestamp(crate::model::Tuple::empty_messages(), timestamp);
        tuple.add_affiliate_column(Arc::new("a".to_string()), Value::Int64(value));
        tuple
    }

    fn a_greater_than_five() -> ScalarExpr {
        ScalarExpr::Column(ColumnRef::ByName {
            column_name: "a".to_string(),
        })
        .call_binary(
            ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Gt,
        )
    }

    // coverage-covers: stream.window.sliding
    #[tokio::test]
    async fn sliding_window_without_lookahead_emits_on_data() {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, None, Vec::new(), 0);
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(105)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut seen = Vec::new();
        for _ in 0..2 {
            match output_rx.recv().await.unwrap() {
                StreamData::Collection(collection) => {
                    seen.push(collection.rows().len());
                }
                _ => panic!("unexpected output"),
            }
        }

        // For t=100, window [90,100] contains 1 row; for t=105, window [95,105] contains 2 rows.
        assert_eq!(seen, vec![1, 2]);
    }

    #[tokio::test]
    async fn sliding_window_updates_last_hit_time_after_trigger_hit() {
        let spawner = test_spawner();
        let state = Arc::new(ProcessorState::new());
        let mut physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, None, Vec::new(), 0);
        physical.trigger_condition_scalar = Some(ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(100_500),
                ConcreteDatatype::Int64(Int64Type),
            )),
        });
        physical.trigger_processor_state = Some(Arc::clone(&state));
        physical.trigger_state_usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(101), tuple_at(102)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut seen = Vec::new();
        for _ in 0..2 {
            match timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv")
            {
                StreamData::Collection(collection) => seen.push(collection.rows().len()),
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(seen, vec![1, 2]);
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 101_000);
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sliding_window_skips_trigger_hit_state_error_before_lookahead_mutation() {
        let spawner = test_spawner();
        let state = Arc::new(ProcessorState::new());
        let mut physical =
            PhysicalSlidingWindow::new(TimeUnit::Seconds, 2, Some(10), Vec::new(), 0);
        physical.trigger_condition_scalar = Some(ScalarExpr::CallBinary {
            func: BinaryFunc::Lt,
            expr1: Box::new(ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            }),
            expr2: Box::new(ScalarExpr::Literal(
                Value::Int64(100_000),
                ConcreteDatatype::Int64(Int64Type),
            )),
        });
        physical.trigger_processor_state = Some(Arc::clone(&state));
        physical.trigger_state_usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let bad_timestamp = UNIX_EPOCH
            .checked_sub(Duration::from_secs(5))
            .expect("pre-epoch timestamp");
        let good_timestamp = UNIX_EPOCH + Duration::from_secs(20);
        let batch = crate::model::RecordBatch::new(vec![
            tuple_with_a_at(bad_timestamp, 1),
            tuple_with_a_at(good_timestamp, 2),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(30)))
            .is_ok());

        let item = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(collection) = item else {
            panic!("expected sliding window collection");
        };
        assert_eq!(collection.rows().len(), 1);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.error_count, 1);
        assert!(snapshot
            .last_error
            .as_deref()
            .is_some_and(|err| err.contains("invalid timestamp")));
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 20_000);
        assert!(output_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sliding_window_with_lookahead_waits_for_watermark() {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, Some(15), Vec::new(), 0);
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(110), tuple_at(115)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        // No output until watermark reaches end of first window: 100 + 15 = 115.
        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(114)))
            .is_ok());

        assert!(output_rx.try_recv().is_err());

        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(115)))
            .is_ok());

        let out = output_rx.recv().await.unwrap();
        match out {
            StreamData::Collection(collection) => {
                // First trigger at 100: window [90,115] includes all 3 rows.
                assert_eq!(collection.rows().len(), 3);
            }
            _ => panic!("unexpected output"),
        }
    }

    #[tokio::test]
    async fn sliding_window_with_lookahead_graceful_end_flushes_pending_windows_before_terminal_control(
    ) {
        let spawner = test_spawner();
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, Some(15), Vec::new(), 0);
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch =
            crate::model::RecordBatch::new(vec![tuple_at(100), tuple_at(110), tuple_at(115)])
                .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                crate::processor::BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let mut window_sizes = Vec::new();
        loop {
            match timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv")
            {
                StreamData::Collection(collection) => window_sizes.push(collection.rows().len()),
                StreamData::Control(ControlSignal::Barrier(
                    crate::processor::BarrierControlSignal::StreamGracefulEnd { .. },
                )) => break,
                other => panic!("unexpected output: {}", other.description()),
            }
        }

        assert_eq!(window_sizes, vec![3, 3, 2]);
    }

    #[tokio::test]
    async fn sliding_window_validates_end_only_for_trigger_tuple() {
        let max_timestamp_secs = u64::try_from(i64::MAX).expect("i64 max fits u64") / 1_000_000;
        let lookahead_secs = 10;
        let first_timestamp =
            UNIX_EPOCH + Duration::from_secs(max_timestamp_secs.saturating_sub(lookahead_secs));
        let second_timestamp =
            UNIX_EPOCH + Duration::from_secs(max_timestamp_secs.saturating_sub(1));
        let watermark = UNIX_EPOCH + Duration::from_secs(max_timestamp_secs);

        for (second_value, expected_rows, expected_errors) in [(1, 2, 0), (10, 1, 1)] {
            let spawner = test_spawner();
            let physical = PhysicalSlidingWindow::new_with_trigger(
                TimeUnit::Seconds,
                2,
                Some(lookahead_secs),
                Vec::new(),
                Vec::new(),
                Some(Expr::Identifier(Ident::new("a_gt_5"))),
                Some(a_greater_than_five()),
                Vec::new(),
                0,
            );
            let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
            let stats = Arc::new(ProcessorStats::default());
            processor.set_stats(Arc::clone(&stats));
            let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
            processor.add_input(input.subscribe());
            let mut output_rx = processor.subscribe_output().unwrap();
            let _handle = processor.start(&spawner);

            let batch = crate::model::RecordBatch::new(vec![
                tuple_with_a_at(first_timestamp, 10),
                tuple_with_a_at(second_timestamp, second_value),
            ])
            .expect("batch");
            assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
            assert!(input.send(StreamData::watermark(watermark)).is_ok());

            let item = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            let StreamData::Collection(collection) = item else {
                panic!("expected sliding window collection");
            };
            assert_eq!(collection.rows().len(), expected_rows);

            let snapshot = stats.snapshot();
            assert_eq!(snapshot.error_count, expected_errors);
            assert_eq!(snapshot.last_error.is_some(), expected_errors > 0);
        }
    }
}
