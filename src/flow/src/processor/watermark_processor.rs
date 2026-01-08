//! WatermarkProcessor - emits or forwards watermarks to drive time-related operators.

use crate::planner::physical::{
    PhysicalEventtimeWatermark, PhysicalPlan, PhysicalProcessTimeWatermark, WatermarkConfig,
    WatermarkStrategy,
};
use crate::processor::base::{
    attach_stats_to_collect_barrier, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, Interval, MissedTickBehavior, Sleep};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Watermark processor variants by window/operator kind.
pub enum WatermarkProcessor {
    ProcessTime(ProcessTimeWatermarkProcessor),
    Eventtime(EventtimeWatermarkProcessor),
}

/// Processing-time watermark processor variants by window/operator kind.
pub enum ProcessTimeWatermarkProcessor {
    Tumbling(TumblingWatermarkProcessor),
    Sliding(SlidingWatermarkProcessor),
}

impl WatermarkProcessor {
    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::ProcessTimeWatermark(watermark) => match &watermark.config {
                WatermarkConfig::Tumbling { .. } => Some(WatermarkProcessor::ProcessTime(
                    ProcessTimeWatermarkProcessor::Tumbling(TumblingWatermarkProcessor::new(
                        id,
                        Arc::new(watermark.clone()),
                    )),
                )),
                WatermarkConfig::Sliding { .. } => Some(WatermarkProcessor::ProcessTime(
                    ProcessTimeWatermarkProcessor::Sliding(SlidingWatermarkProcessor::new(
                        id,
                        Arc::new(watermark.clone()),
                    )),
                )),
            },
            PhysicalPlan::EventtimeWatermark(watermark) => Some(WatermarkProcessor::Eventtime(
                EventtimeWatermarkProcessor::new(id, Arc::new(watermark.clone())),
            )),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.set_stats(stats),
            WatermarkProcessor::Eventtime(p) => p.set_stats(stats),
        }
    }
}

impl ProcessTimeWatermarkProcessor {
    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.set_stats(stats),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.set_stats(stats),
        }
    }
}

impl Processor for WatermarkProcessor {
    fn id(&self) -> &str {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.id(),
            WatermarkProcessor::Eventtime(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.start(),
            WatermarkProcessor::Eventtime(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.subscribe_output(),
            WatermarkProcessor::Eventtime(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.subscribe_control_output(),
            WatermarkProcessor::Eventtime(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.add_input(receiver),
            WatermarkProcessor::Eventtime(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            WatermarkProcessor::ProcessTime(p) => p.add_control_input(receiver),
            WatermarkProcessor::Eventtime(p) => p.add_control_input(receiver),
        }
    }
}

impl Processor for ProcessTimeWatermarkProcessor {
    fn id(&self) -> &str {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.id(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.start(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.subscribe_output(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.subscribe_control_output(),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.add_input(receiver),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            ProcessTimeWatermarkProcessor::Tumbling(p) => p.add_control_input(receiver),
            ProcessTimeWatermarkProcessor::Sliding(p) => p.add_control_input(receiver),
        }
    }
}

/// Watermark operator processor.
pub struct TumblingWatermarkProcessor {
    id: String,
    physical: Arc<PhysicalProcessTimeWatermark>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

impl TumblingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalProcessTimeWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn build_interval(strategy: &WatermarkStrategy) -> Option<Interval> {
        strategy.interval_duration().map(|duration| {
            let mut ticker = interval(duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker
        })
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for TumblingWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mut ticker = Self::build_interval(self.physical.config.strategy());
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "watermark processor starting");

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
                                tracing::info!(processor_id = %id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    _tick = async {
                        if let Some(ticker) = ticker.as_mut() {
                            ticker.tick().await;
                            Some(())
                        } else {
                            None
                        }
                    }, if ticker.is_some() => {
                        let ts = SystemTime::now();
                        send_with_backpressure(&output, StreamData::watermark(ts)).await?;
                        continue;
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(data)) => {
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                let is_terminal = data.is_terminal();
                                let out_rows = data.num_rows_hint();
                                send_with_backpressure(&output, data).await?;
                                if let Some(rows) = out_rows {
                                    stats.record_out(rows);
                                }
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "watermark data input");
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

/// Sliding window watermark processor (processing-time only).
///
/// This processor emits periodic processing-time watermarks (ticker-driven) to advance time and
/// enable downstream GC in watermark-driven window processors.
///
/// When `WatermarkConfig::Sliding { lookahead: Some(L), .. }`, it also schedules and emits a
/// deadline watermark per trigger tuple at `deadline = tuple.timestamp + L`.
///
/// Implementation note (why there is a heap + a single `Sleep`):
/// - Each incoming tuple yields a deadline time `t + lookahead`; we push all deadlines into a
///   min-heap (`BinaryHeap<Reverse<_>>`) so we can always find the earliest pending deadline.
/// - We do NOT spawn one task per tuple. Instead, the processor's main loop keeps at most one
///   active timer (`next_sleep`) that sleeps until the current earliest deadline.
/// - When `next_sleep` fires, we pop that earliest deadline and emit a `StreamData::Watermark`
///   for it, then rebuild `next_sleep` for the next earliest deadline (if any).
///   This avoids unbounded numbers of concurrent timers while still emitting per-tuple deadlines
///   at precise times.
pub struct SlidingWatermarkProcessor {
    id: String,
    lookahead: Option<Duration>,
    ticker: Option<Interval>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

impl SlidingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalProcessTimeWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (lookahead, strategy) = match &physical.config {
            WatermarkConfig::Sliding {
                lookahead,
                strategy,
                ..
            } => (*lookahead, strategy),
            _ => panic!("SlidingWatermarkProcessor requires WatermarkConfig::Sliding"),
        };
        let lookahead = lookahead.map(Duration::from_secs);
        let ticker = strategy.interval_duration().map(|duration| {
            let mut ticker = interval(duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker
        });
        Self {
            id: id.into(),
            lookahead,
            ticker,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn to_nanos(ts: SystemTime) -> Result<u128, ProcessorError> {
        Ok(ts
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProcessorError::ProcessingError(format!("invalid timestamp: {e}")))?
            .as_nanos())
    }

    fn from_nanos(nanos: u128) -> Result<SystemTime, ProcessorError> {
        let nanos_u64 = u64::try_from(nanos).map_err(|_| {
            ProcessorError::ProcessingError("timestamp out of range for u64 nanos".to_string())
        })?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos_u64))
    }

    fn build_sleep(deadline_nanos: u128) -> Result<Pin<Box<Sleep>>, ProcessorError> {
        let deadline_ts = Self::from_nanos(deadline_nanos)?;
        let delay = match deadline_ts.duration_since(SystemTime::now()) {
            Ok(d) => d,
            Err(_) => Duration::from_secs(0),
        };
        Ok(Box::pin(sleep(delay)))
    }
}

impl Processor for SlidingWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let lookahead = self.lookahead;
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mut ticker = self.ticker.take();
        let stats = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut pending_deadlines: BinaryHeap<Reverse<u128>> = BinaryHeap::new();
            let mut next_sleep: Option<Pin<Box<Sleep>>> = None;
            let mut last_emitted_nanos: Option<u128> = None;
            tracing::info!(processor_id = %id, "watermark processor starting");

            async fn emit_monotonic_watermark(
                output: &broadcast::Sender<StreamData>,
                last_emitted_nanos: &mut Option<u128>,
                ts: SystemTime,
            ) -> Result<(), ProcessorError> {
                let nanos = SlidingWatermarkProcessor::to_nanos(ts)?;
                if last_emitted_nanos.is_some_and(|last| nanos <= last) {
                    return Ok(());
                }
                *last_emitted_nanos = Some(nanos);
                send_with_backpressure(output, StreamData::watermark(ts)).await?;
                Ok(())
            }

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
                                tracing::info!(processor_id = %id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    _tick = async {
                        if let Some(ticker) = ticker.as_mut() {
                            ticker.tick().await;
                            Some(())
                        } else {
                            None
                        }
                    }, if ticker.is_some() => {
                        emit_monotonic_watermark(&output, &mut last_emitted_nanos, SystemTime::now()).await?;
                    }
                    _deadline = async {
                        if let Some(sleep) = next_sleep.as_mut() {
                            sleep.as_mut().await;
                            Some(())
                        } else {
                            None
                        }
                    }, if next_sleep.is_some() => {
                        if let Some(Reverse(deadline_nanos)) = pending_deadlines.pop() {
                            let deadline_ts = SlidingWatermarkProcessor::from_nanos(deadline_nanos)?;
                            emit_monotonic_watermark(&output, &mut last_emitted_nanos, deadline_ts)
                                .await?;
                        }
                        next_sleep = if let Some(Reverse(nanos)) = pending_deadlines.peek() {
                            Some(SlidingWatermarkProcessor::build_sleep(*nanos)?)
                        } else {
                            None
                        };
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                match data {
                                    StreamData::Collection(collection) => {
                                        stats.record_in(collection.num_rows() as u64);
                                        if let Some(lookahead) = lookahead {
                                            for row in collection.rows() {
                                                let deadline = row.timestamp + lookahead;
                                                let deadline_nanos =
                                                    SlidingWatermarkProcessor::to_nanos(deadline)?;
                                                pending_deadlines.push(Reverse(deadline_nanos));
                                            }
                                            next_sleep = if let Some(Reverse(nanos)) = pending_deadlines.peek() {
                                                Some(SlidingWatermarkProcessor::build_sleep(*nanos)?)
                                            } else {
                                                None
                                            };
                                        }
                                        let out = StreamData::collection(collection);
                                        let out_rows = out.num_rows_hint();
                                        send_with_backpressure(&output, out).await?;
                                        if let Some(rows) = out_rows {
                                            stats.record_out(rows);
                                        }
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(&output, other).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "watermark data input");
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
struct HeapItem {
    ts_nanos: u128,
    seq: u64,
    tuple: crate::model::Tuple,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.ts_nanos == other.ts_nanos && self.seq == other.seq
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts_nanos
            .cmp(&other.ts_nanos)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

/// Event-time watermark processor (data-driven, no ticker).
///
/// This processor buffers out-of-order tuples and emits:
/// - ordered `StreamData::Collection(RecordBatch)` (non-decreasing tuple.timestamp)
/// - monotonic `StreamData::Watermark`
///   Late events (`ts <= current_watermark`) are dropped.
pub struct EventtimeWatermarkProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    state: EventtimeWatermarkState,
    stats: Arc<ProcessorStats>,
}

impl EventtimeWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalEventtimeWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);

        let late_tolerance = match physical.config.strategy() {
            WatermarkStrategy::EventTime { late_tolerance } => *late_tolerance,
            WatermarkStrategy::ProcessingTime { .. } => Duration::ZERO,
        };

        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            state: EventtimeWatermarkState::new(late_tolerance),
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn to_nanos(ts: SystemTime) -> Result<u128, ProcessorError> {
        Ok(ts
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProcessorError::ProcessingError(format!("invalid timestamp: {e}")))?
            .as_nanos())
    }

    fn from_nanos(nanos: u128) -> Result<SystemTime, ProcessorError> {
        let nanos_u64 = u64::try_from(nanos).map_err(|_| {
            ProcessorError::ProcessingError("timestamp out of range for u64 nanos".to_string())
        })?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos_u64))
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for EventtimeWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mut state = std::mem::take(&mut self.state);
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "eventtime watermark processor starting");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let control_signal =
                                attach_stats_to_collect_barrier(control_signal, &id, &stats);
                            let is_terminal = control_signal.is_terminal();
                            if control_signal.is_graceful_end() {
                                match state.on_graceful_end() {
                                    Ok(step) => {
                                        for err in step.errors {
                                            stats.record_error(err);
                                        }
                                        for item in step.outputs {
                                            send_with_backpressure(&output, item).await?;
                                        }
                                    }
                                    Err(err) => {
                                        stats.record_error(format!("eventtime flush error: {err}"));
                                    }
                                }
                            }
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                tracing::info!(processor_id = %id, "received StreamEnd (control)");
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
                                let rows = match collection.into_rows() {
                                    Ok(rows) => rows,
                                    Err(err) => {
                                        stats.record_error(format!(
                                            "eventtime collection error: {err}"
                                        ));
                                        continue;
                                    }
                                };
                                match state.on_rows(rows) {
                                    Ok(step) => {
                                        for err in step.errors {
                                            stats.record_error(err);
                                        }
                                        for item in step.outputs {
                                            let out_rows = item.num_rows_hint();
                                            send_with_backpressure(&output, item).await?;
                                            if let Some(rows) = out_rows {
                                                stats.record_out(rows);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        stats.record_error(format!("eventtime ingest error: {err}"));
                                    }
                                }
                            }
                            Some(Ok(StreamData::Watermark(_))) => {
                                stats.record_error(
                                    "unexpected StreamData::Watermark input in eventtime watermark processor"
                                        .to_string(),
                                );
                            }
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                if signal.is_graceful_end() {
                                    match state.on_graceful_end() {
                                        Ok(step) => {
                                            for err in step.errors {
                                                stats.record_error(err);
                                            }
                                            for item in step.outputs {
                                                send_with_backpressure(&output, item).await?;
                                            }
                                        }
                                        Err(err) => {
                                            stats.record_error(format!("eventtime flush error: {err}"));
                                        }
                                    }
                                }
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(other)) => {
                                let is_terminal = other.is_terminal();
                                send_with_backpressure(&output, other).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "eventtime watermark data input");
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

struct EventtimeStep {
    outputs: Vec<StreamData>,
    errors: Vec<String>,
}

#[derive(Debug)]
struct EventtimeWatermarkState {
    current_watermark_nanos: u128,
    max_timestamp_seen_nanos: u128,
    late_tolerance_nanos: u128,
    buffer: BinaryHeap<Reverse<HeapItem>>,
    seq: u64,
}

impl Default for EventtimeWatermarkState {
    fn default() -> Self {
        Self::new(Duration::ZERO)
    }
}

impl EventtimeWatermarkState {
    fn new(late_tolerance: Duration) -> Self {
        Self {
            current_watermark_nanos: 0,
            max_timestamp_seen_nanos: 0,
            late_tolerance_nanos: late_tolerance.as_nanos(),
            buffer: BinaryHeap::new(),
            seq: 0,
        }
    }

    fn compute_target(&self) -> u128 {
        let candidate = self
            .max_timestamp_seen_nanos
            .saturating_sub(self.late_tolerance_nanos);
        std::cmp::max(self.current_watermark_nanos, candidate)
    }

    fn on_rows(&mut self, rows: Vec<crate::model::Tuple>) -> Result<EventtimeStep, ProcessorError> {
        let mut errors = Vec::new();

        for tuple in rows {
            let ts_nanos = match EventtimeWatermarkProcessor::to_nanos(tuple.timestamp) {
                Ok(ts) => ts,
                Err(err) => {
                    errors.push(err.to_string());
                    continue;
                }
            };
            if ts_nanos <= self.current_watermark_nanos {
                continue;
            }
            self.max_timestamp_seen_nanos = std::cmp::max(self.max_timestamp_seen_nanos, ts_nanos);
            self.buffer.push(Reverse(HeapItem {
                ts_nanos,
                seq: self.seq,
                tuple,
            }));
            self.seq = self.seq.wrapping_add(1);
        }

        let target = self.compute_target();
        let outputs = self.flush_up_to(target)?;
        Ok(EventtimeStep { outputs, errors })
    }

    fn on_graceful_end(&mut self) -> Result<EventtimeStep, ProcessorError> {
        let outputs = self.flush_all()?;
        Ok(EventtimeStep {
            outputs,
            errors: Vec::new(),
        })
    }

    fn flush_up_to(&mut self, target: u128) -> Result<Vec<StreamData>, ProcessorError> {
        if target <= self.current_watermark_nanos {
            return Ok(Vec::new());
        }

        let mut out_rows = Vec::new();
        while let Some(Reverse(head)) = self.buffer.peek() {
            if head.ts_nanos > target {
                break;
            }
            let Reverse(item) = self
                .buffer
                .pop()
                .expect("peek returned Some, pop must succeed");
            out_rows.push(item.tuple);
        }

        let mut out = Vec::new();
        if !out_rows.is_empty() {
            let batch = crate::model::RecordBatch::new(out_rows)
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
            out.push(StreamData::collection(Box::new(batch)));
        }

        let watermark_ts = EventtimeWatermarkProcessor::from_nanos(target)?;
        out.push(StreamData::watermark(watermark_ts));
        self.current_watermark_nanos = target;
        Ok(out)
    }

    fn flush_all(&mut self) -> Result<Vec<StreamData>, ProcessorError> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }
        let mut out_rows = Vec::with_capacity(self.buffer.len());
        while let Some(Reverse(item)) = self.buffer.pop() {
            out_rows.push(item.tuple);
        }
        if out_rows.is_empty() {
            return Ok(Vec::new());
        }
        let batch = crate::model::RecordBatch::new(out_rows)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
        Ok(vec![StreamData::collection(Box::new(batch))])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::WatermarkConfig;
    use tokio::time::timeout;

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_secs(sec),
        )
    }

    #[tokio::test]
    async fn sliding_watermark_emits_deadline_per_tuple_in_processing_time() {
        let physical = PhysicalProcessTimeWatermark::new(
            WatermarkConfig::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: Some(1),
                strategy: WatermarkStrategy::ProcessingTime {
                    time_unit: TimeUnit::Seconds,
                    // Use a long tick interval so the deadline emission is observable and not
                    // dominated by periodic tick watermarks.
                    interval: 10,
                },
            },
            Vec::new(),
            0,
        );
        let mut processor = SlidingWatermarkProcessor::new("wm", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        // Use "now-ish" timestamps to match processing-time assumptions.
        let base = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("now")
            .as_secs();
        let batch = crate::model::RecordBatch::new(vec![tuple_at(base)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_collection = false;
        let mut saw_deadline_or_later = false;
        let expected_deadline = UNIX_EPOCH + Duration::from_secs(base + 1);
        for _ in 0..8 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(_) => saw_collection = true,
                StreamData::Watermark(ts) => {
                    if ts >= expected_deadline {
                        saw_deadline_or_later = true;
                    }
                }
                _ => {}
            }
            if saw_collection && saw_deadline_or_later {
                break;
            }
        }

        assert!(saw_collection);
        assert!(saw_deadline_or_later);
    }

    #[tokio::test]
    async fn sliding_watermark_without_lookahead_still_emits_tick_watermarks() {
        let physical = PhysicalProcessTimeWatermark::new(
            WatermarkConfig::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: None,
                strategy: WatermarkStrategy::ProcessingTime {
                    time_unit: TimeUnit::Seconds,
                    interval: 1,
                },
            },
            Vec::new(),
            0,
        );
        let mut processor = SlidingWatermarkProcessor::new("wm", Arc::new(physical));
        let (_input, _) = broadcast::channel::<StreamData>(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(_input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        // The ticker may fire immediately; just assert we see a watermark promptly.
        loop {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            if matches!(msg, StreamData::Watermark(_)) {
                break;
            }
        }
    }

    #[tokio::test]
    async fn eventtime_watermark_sorts_and_emits_monotonic_watermarks() {
        let physical = PhysicalEventtimeWatermark::new(
            WatermarkConfig::Tumbling {
                time_unit: TimeUnit::Seconds,
                length: 10,
                strategy: WatermarkStrategy::EventTime {
                    late_tolerance: Duration::from_secs(1),
                },
            },
            Vec::new(),
            0,
        );

        let mut processor = EventtimeWatermarkProcessor::new("ewm", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        // Out-of-order input: 3, 1, 2 (late_tolerance=1 => candidate=2)
        let batch = crate::model::RecordBatch::new(vec![tuple_at(3), tuple_at(1), tuple_at(2)])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        // Expect ordered output up to watermark=2, then watermark.
        let mut saw_rows = Vec::new();
        let mut saw_watermark = None;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(collection) => {
                    let rows = collection.into_rows().expect("rows");
                    for t in rows {
                        saw_rows.push(t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    }
                }
                StreamData::Watermark(ts) => {
                    saw_watermark = Some(ts.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    break;
                }
                _ => {}
            }
        }

        assert_eq!(saw_rows, vec![1, 2]);
        assert_eq!(saw_watermark, Some(2));

        // Next batch advances max to 4 => target becomes 3, flushing 3 and watermark 3.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(4)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_rows = Vec::new();
        let mut saw_watermark = None;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(collection) => {
                    let rows = collection.into_rows().expect("rows");
                    for t in rows {
                        saw_rows.push(t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    }
                }
                StreamData::Watermark(ts) => {
                    saw_watermark = Some(ts.duration_since(UNIX_EPOCH).unwrap().as_secs());
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(saw_rows, vec![3]);
        assert_eq!(saw_watermark, Some(3));
    }

    #[test]
    fn eventtime_state_computes_target_and_emits_watermarks() {
        /*
        Summary:
        - Validates state transitions for event-time watermark computation:
          - `max_seen` tracks the max tuple timestamp observed so far.
          - `target = max(current_watermark, max_seen - late_tolerance)` advances monotonically.
        - Validates output transitions when `target` advances:
          - First emits a `Collection` containing all buffered tuples with `ts <= target`,
            in non-decreasing `(ts, seq)` order.
          - Then emits `Watermark(target)`.
        */
        let mut state = EventtimeWatermarkState::new(Duration::from_secs(1));

        let step = state
            .on_rows(vec![tuple_at(3), tuple_at(1), tuple_at(2)])
            .expect("on_rows");
        assert_eq!(
            state.current_watermark_nanos,
            Duration::from_secs(2).as_nanos()
        );
        assert!(step.errors.is_empty());
        assert_eq!(step.outputs.len(), 2);
        match &step.outputs[0] {
            StreamData::Collection(collection) => {
                let rows = collection.clone().into_rows().expect("rows");
                let secs: Vec<u64> = rows
                    .into_iter()
                    .map(|t| t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs())
                    .collect();
                assert_eq!(secs, vec![1, 2]);
            }
            _ => panic!("expected collection"),
        }
        match &step.outputs[1] {
            StreamData::Watermark(ts) => {
                assert_eq!(ts.duration_since(UNIX_EPOCH).unwrap().as_secs(), 2);
            }
            _ => panic!("expected watermark"),
        }

        let step = state.on_rows(vec![tuple_at(4)]).expect("on_rows");
        assert_eq!(
            state.current_watermark_nanos,
            Duration::from_secs(3).as_nanos()
        );
        assert!(step.errors.is_empty());
        assert_eq!(step.outputs.len(), 2);
        match &step.outputs[0] {
            StreamData::Collection(collection) => {
                let rows = collection.clone().into_rows().expect("rows");
                let secs: Vec<u64> = rows
                    .into_iter()
                    .map(|t| t.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs())
                    .collect();
                assert_eq!(secs, vec![3]);
            }
            _ => panic!("expected collection"),
        }
        match &step.outputs[1] {
            StreamData::Watermark(ts) => {
                assert_eq!(ts.duration_since(UNIX_EPOCH).unwrap().as_secs(), 3);
            }
            _ => panic!("expected watermark"),
        }
    }

    #[test]
    fn eventtime_state_drops_late_events() {
        /*
        Summary:
        - Establishes a watermark (late_tolerance = 0) and then verifies late-drop semantics:
          - Any tuple with `ts <= current_watermark` is considered late and dropped.
          - Late drops do not advance watermark and do not emit outputs.
        */
        let mut state = EventtimeWatermarkState::new(Duration::from_secs(0));
        let step = state.on_rows(vec![tuple_at(2)]).expect("on_rows");
        assert_eq!(step.outputs.len(), 2);
        assert_eq!(
            state.current_watermark_nanos,
            Duration::from_secs(2).as_nanos()
        );

        let step = state
            .on_rows(vec![tuple_at(2), tuple_at(1)])
            .expect("on_rows");
        assert!(step.outputs.is_empty());
    }
}
