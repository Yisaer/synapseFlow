//! WatermarkProcessor - emits or forwards watermarks to drive time-related operators.

use crate::planner::physical::{
    PhysicalPlan, PhysicalWatermark, WatermarkConfig, WatermarkStrategy,
};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
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
    Tumbling(TumblingWatermarkProcessor),
    Sliding(SlidingWatermarkProcessor),
}

impl WatermarkProcessor {
    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Watermark(watermark) => match &watermark.config {
                WatermarkConfig::Tumbling { .. } => Some(WatermarkProcessor::Tumbling(
                    TumblingWatermarkProcessor::new(id, Arc::new(watermark.clone())),
                )),
                WatermarkConfig::Sliding { .. } => Some(WatermarkProcessor::Sliding(
                    SlidingWatermarkProcessor::new(id, Arc::new(watermark.clone())),
                )),
            },
            _ => None,
        }
    }
}

impl Processor for WatermarkProcessor {
    fn id(&self) -> &str {
        match self {
            WatermarkProcessor::Tumbling(p) => p.id(),
            WatermarkProcessor::Sliding(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            WatermarkProcessor::Tumbling(p) => p.start(),
            WatermarkProcessor::Sliding(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            WatermarkProcessor::Tumbling(p) => p.subscribe_output(),
            WatermarkProcessor::Sliding(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            WatermarkProcessor::Tumbling(p) => p.subscribe_control_output(),
            WatermarkProcessor::Sliding(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            WatermarkProcessor::Tumbling(p) => p.add_input(receiver),
            WatermarkProcessor::Sliding(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            WatermarkProcessor::Tumbling(p) => p.add_control_input(receiver),
            WatermarkProcessor::Sliding(p) => p.add_control_input(receiver),
        }
    }
}

/// Watermark operator processor.
pub struct TumblingWatermarkProcessor {
    id: String,
    physical: Arc<PhysicalWatermark>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl TumblingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
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
        println!("[WatermarkProcessor:{id}] starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                println!("[WatermarkProcessor:{id}] received StreamEnd (control)");
                                println!("[WatermarkProcessor:{id}] stopped");
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
                                    println!("[WatermarkProcessor:{id}] received StreamEnd (data)");
                                    println!("[WatermarkProcessor:{id}] stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    println!("[WatermarkProcessor:{id}] received StreamEnd (data)");
                                    println!("[WatermarkProcessor:{id}] stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "WatermarkProcessor input lagged by {} messages",
                                    skipped
                                );
                                println!("[WatermarkProcessor:{id}] input lagged by {skipped} messages");
                                forward_error(&output, &id, message).await?;
                            }
                            None => {
                                println!("[WatermarkProcessor:{id}] stopped");
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
/// When `WatermarkConfig::Sliding { lookahead: Some(L), .. }`, this processor schedules and emits
/// a deadline watermark per trigger tuple at `deadline = tuple.timestamp + L`.
///
/// Event-time watermark semantics are not implemented yet.
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
    lookahead: Duration,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl SlidingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let lookahead_secs = match &physical.config {
            WatermarkConfig::Sliding {
                lookahead: Some(lookahead),
                ..
            } => *lookahead,
            WatermarkConfig::Sliding {
                lookahead: None, ..
            } => {
                panic!("SlidingWatermarkProcessor requires sliding watermark lookahead")
            }
            _ => panic!("SlidingWatermarkProcessor requires WatermarkConfig::Sliding"),
        };
        let lookahead = Duration::from_secs(lookahead_secs);
        Self {
            id: id.into(),
            lookahead,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
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

        tokio::spawn(async move {
            let mut pending_deadlines: BinaryHeap<Reverse<u128>> = BinaryHeap::new();
            let mut next_sleep: Option<Pin<Box<Sleep>>> = None;
            let mut last_emitted_nanos: Option<u128> = None;
            println!("[WatermarkProcessor:{id}] starting");

            async fn emit_deadline_watermark(
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
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                println!("[WatermarkProcessor:{id}] received StreamEnd (control)");
                                println!("[WatermarkProcessor:{id}] stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
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
                            emit_deadline_watermark(&output, &mut last_emitted_nanos, deadline_ts)
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
                                        send_with_backpressure(&output, StreamData::collection(collection))
                                            .await?;
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(&output, other).await?;
                                        if is_terminal {
                                            println!("[WatermarkProcessor:{id}] received StreamEnd (data)");
                                            println!("[WatermarkProcessor:{id}] stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "WatermarkProcessor input lagged by {} messages",
                                    skipped
                                );
                                println!("[WatermarkProcessor:{id}] input lagged by {skipped} messages");
                                forward_error(&output, &id, message).await?;
                            }
                            None => {
                                println!("[WatermarkProcessor:{id}] stopped");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::{BasePhysicalPlan, WatermarkConfig};
    use tokio::time::timeout;

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(Vec::new(), UNIX_EPOCH + Duration::from_secs(sec))
    }

    #[tokio::test]
    async fn sliding_watermark_emits_deadline_per_tuple_in_processing_time() {
        let physical = PhysicalWatermark {
            base: BasePhysicalPlan::new(Vec::new(), 0),
            config: WatermarkConfig::Sliding {
                time_unit: TimeUnit::Seconds,
                lookback: 10,
                lookahead: Some(15),
                strategy: WatermarkStrategy::ProcessingTime {
                    time_unit: TimeUnit::Seconds,
                    interval: 1,
                },
            },
        };
        let mut processor = SlidingWatermarkProcessor::new("wm", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        // Use an old timestamp so deadline is already in the past and should be emitted immediately.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        let mut saw_collection = false;
        let mut saw_deadline = false;
        for _ in 0..4 {
            let msg = timeout(Duration::from_secs(2), output_rx.recv())
                .await
                .expect("timeout")
                .expect("recv");
            match msg {
                StreamData::Collection(_) => saw_collection = true,
                StreamData::Watermark(ts) => {
                    if ts == UNIX_EPOCH + Duration::from_secs(16) {
                        saw_deadline = true;
                    }
                }
                _ => {}
            }
            if saw_collection && saw_deadline {
                break;
            }
        }

        assert!(saw_collection);
        assert!(saw_deadline);
    }
}
