//! SlidingWindowProcessor - emits sliding windows triggered by incoming data.
//!
//! Processing-time mode assumes tuple timestamps are non-decreasing.
//! Window flushing for lookahead windows is driven by incoming watermarks.

use crate::expr::ScalarExpr;
use crate::planner::logical::TimeUnit;
use crate::planner::physical::{PhysicalPlan, PhysicalSlidingWindow};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
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

pub struct SlidingWindowProcessor {
    id: String,
    lookback: Duration,
    lookahead: Option<Duration>,
    partition_by_scalars: Vec<ScalarExpr>,
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
        Self {
            id: id.into(),
            lookback,
            lookahead,
            partition_by_scalars,
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

        let stats = Arc::clone(&self.stats);
        let mut state = PartitionedProcessingState::new(
            lookback,
            lookahead,
            partition_by_scalars,
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
                                if let Err(e) = res {
                                    stats.record_error_logged("sliding window processor error", e.to_string());
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
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
}

impl PartitionedProcessingState {
    fn new(
        lookback: Duration,
        lookahead: Option<Duration>,
        partition_by_scalars: Vec<ScalarExpr>,
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
        let rows = collection
            .into_rows()
            .map_err(|e| ProcessorError::ProcessingError(format!("failed to extract rows: {e}")))?;
        for tuple in rows {
            let partition_key =
                eval_partition_key(&self.partition_by_scalars, &tuple, "slidingwindow")
                    .map_err(ProcessorError::ProcessingError)?;
            self.state_for(partition_key).add_tuple(tuple).await?;
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
            Some(lookahead) => Self::WithLookahead(ProcessingWithLookaheadState::new(
                lookback,
                lookahead,
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

    async fn add_tuple(&mut self, tuple: crate::model::Tuple) -> Result<(), ProcessorError> {
        match self {
            ProcessingState::WithLookahead(state) => state.add_tuple(tuple).await,
            ProcessingState::WithoutLookahead(state) => state.add_tuple(tuple).await,
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

    async fn add_tuple(&mut self, tuple: crate::model::Tuple) -> Result<(), ProcessorError> {
        let t = tuple.timestamp;
        self.rows.push_back(tuple);
        let start = t
            .checked_sub(self.lookback)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        self.emit_window(start, t).await
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
        self.stats.record_out(rows.len() as u64);
        let batch = window_metadata::record_batch_from_system_time(rows, start, end)?;
        send_with_backpressure(
            &self.output,
            self.data_channel_capacity,
            StreamData::collection(Box::new(batch)),
            Some(self.stats.as_ref()),
        )
        .await?;
        Ok(())
    }
}

struct ProcessingWithLookaheadState {
    rows: VecDeque<crate::model::Tuple>,
    pending: VecDeque<WindowRequest>,
    lookback: Duration,
    lookahead: Duration,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
}

impl ProcessingWithLookaheadState {
    fn new(
        lookback: Duration,
        lookahead: Duration,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            pending: VecDeque::new(),
            lookback,
            lookahead,
            output,
            data_channel_capacity,
            stats,
        }
    }

    async fn add_tuple(&mut self, tuple: crate::model::Tuple) -> Result<(), ProcessorError> {
        let t = tuple.timestamp;
        self.rows.push_back(tuple);
        let start = t
            .checked_sub(self.lookback)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let end = t + self.lookahead;
        self.pending.push_back(WindowRequest { start, end });
        Ok(())
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        while let Some(request) = self.pending.pop_front() {
            if request.end > watermark {
                self.pending.push_front(request);
                break;
            }
            self.emit_window(request.start, request.end).await?;
        }
        self.trim(watermark);
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        while let Some(request) = self.pending.pop_front() {
            self.emit_window(request.start, request.end).await?;
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
        self.stats.record_out(rows.len() as u64);
        let batch = window_metadata::record_batch_from_system_time(rows, start, end)?;
        send_with_backpressure(
            &self.output,
            self.data_channel_capacity,
            StreamData::collection(Box::new(batch)),
            Some(self.stats.as_ref()),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY;
    use crate::runtime::TaskSpawner;
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
}
