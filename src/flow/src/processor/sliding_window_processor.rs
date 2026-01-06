//! SlidingWindowProcessor - emits sliding windows triggered by incoming data.
//!
//! Processing-time mode assumes tuple timestamps are non-decreasing.
//! Window flushing for lookahead windows is driven by incoming watermarks.

use crate::planner::logical::TimeUnit;
use crate::planner::physical::{PhysicalPlan, PhysicalSlidingWindow};
use crate::processor::base::{
    attach_stats_to_collect_barrier, fan_in_control_streams, fan_in_streams, forward_error,
    log_broadcast_lagged, send_control_with_backpressure, send_with_backpressure,
    DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

pub struct SlidingWindowProcessor {
    id: String,
    lookback: Duration,
    lookahead: Option<Duration>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

impl SlidingWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalSlidingWindow>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let lookback = match physical.time_unit {
            TimeUnit::Seconds => Duration::from_secs(physical.lookback),
        };
        let lookahead = match physical.time_unit {
            TimeUnit::Seconds => physical.lookahead.map(Duration::from_secs),
        };
        Self {
            id: id.into(),
            lookback,
            lookahead,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
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

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();

        let lookback = self.lookback;
        let lookahead = self.lookahead;

        let stats = Arc::clone(&self.stats);
        let mut state =
            ProcessingState::new(lookback, lookahead, output.clone(), Arc::clone(&stats));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let control_signal =
                                attach_stats_to_collect_barrier(control_signal, &id, &stats);
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                                if let Err(e) = state.add_collection(collection).await {
                                    forward_error(&output, &id, e.to_string()).await?;
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                state.flush_up_to(ts).await?;
                            }
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                let is_graceful = signal.is_graceful_end();
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    if is_graceful {
                                        state.flush_all().await?;
                                    }
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(other)) => {
                                let is_terminal = other.is_terminal();
                                send_with_backpressure(&output, other).await?;
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
        })
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
        output: broadcast::Sender<StreamData>,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        match lookahead {
            Some(lookahead) => Self::WithLookahead(ProcessingWithLookaheadState::new(
                lookback,
                lookahead,
                output,
                Arc::clone(&stats),
            )),
            None => Self::WithoutLookahead(ProcessingWithoutLookaheadState::new(
                lookback, output, stats,
            )),
        }
    }

    fn record_in(&self, rows: u64) {
        match self {
            ProcessingState::WithLookahead(state) => state.stats.record_in(rows),
            ProcessingState::WithoutLookahead(state) => state.stats.record_in(rows),
        }
    }

    async fn add_collection(
        &mut self,
        collection: Box<dyn crate::model::Collection>,
    ) -> Result<(), ProcessorError> {
        match self {
            ProcessingState::WithLookahead(state) => state.add_collection(collection).await,
            ProcessingState::WithoutLookahead(state) => state.add_collection(collection).await,
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
    output: broadcast::Sender<StreamData>,
    stats: Arc<ProcessorStats>,
}

impl ProcessingWithoutLookaheadState {
    fn new(
        lookback: Duration,
        output: broadcast::Sender<StreamData>,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            lookback,
            output,
            stats,
        }
    }

    async fn add_collection(
        &mut self,
        collection: Box<dyn crate::model::Collection>,
    ) -> Result<(), ProcessorError> {
        let rows = collection
            .into_rows()
            .map_err(|e| ProcessorError::ProcessingError(format!("failed to extract rows: {e}")))?;
        for tuple in rows {
            let t = tuple.timestamp;
            self.rows.push_back(tuple);
            let start = t
                .checked_sub(self.lookback)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            self.emit_window(start, t).await?;
        }
        Ok(())
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
        let batch = crate::model::RecordBatch::new(rows)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        send_with_backpressure(&self.output, StreamData::collection(Box::new(batch))).await?;
        Ok(())
    }
}

struct ProcessingWithLookaheadState {
    rows: VecDeque<crate::model::Tuple>,
    pending: VecDeque<WindowRequest>,
    lookback: Duration,
    lookahead: Duration,
    output: broadcast::Sender<StreamData>,
    stats: Arc<ProcessorStats>,
}

impl ProcessingWithLookaheadState {
    fn new(
        lookback: Duration,
        lookahead: Duration,
        output: broadcast::Sender<StreamData>,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            pending: VecDeque::new(),
            lookback,
            lookahead,
            output,
            stats,
        }
    }

    async fn add_collection(
        &mut self,
        collection: Box<dyn crate::model::Collection>,
    ) -> Result<(), ProcessorError> {
        let rows = collection
            .into_rows()
            .map_err(|e| ProcessorError::ProcessingError(format!("failed to extract rows: {e}")))?;
        for tuple in rows {
            let t = tuple.timestamp;
            self.rows.push_back(tuple);
            let start = t
                .checked_sub(self.lookback)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let end = t + self.lookahead;
            self.pending.push_back(WindowRequest { start, end });
        }
        Ok(())
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        while let Some(front) = self.pending.front() {
            if front.end > watermark {
                break;
            }
            let request = self.pending.pop_front().unwrap();
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
        let batch = crate::model::RecordBatch::new(rows)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        send_with_backpressure(&self.output, StreamData::collection(Box::new(batch))).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        )
    }

    #[tokio::test]
    async fn sliding_window_without_lookahead_emits_on_data() {
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, None, Vec::new(), 0);
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

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
        let physical = PhysicalSlidingWindow::new(TimeUnit::Seconds, 10, Some(15), Vec::new(), 0);
        let mut processor = SlidingWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

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
}
