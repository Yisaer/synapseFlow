use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_received_data, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Time-driven tumbling window implementation.
pub struct StreamingTumblingAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_meta: Vec<GroupByMeta>,
    event_time: bool,
}

impl StreamingTumblingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let group_by_meta =
            build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            group_by_meta,
            event_time: false,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl Processor for StreamingTumblingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_meta = self.group_by_meta.clone();
        let len_secs = match physical.window {
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length,
            } => length,
            _ => unreachable!("tumbling processor requires tumbling window spec"),
        };
        let event_time = self.event_time;

        tokio::spawn(async move {
            let window_state = if event_time {
                WindowState::EventTime(EventWindowState::new(
                    len_secs,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    group_by_meta.clone(),
                ))
            } else {
                WindowState::ProcessingTime(ProcessingWindowState::new(
                    len_secs,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    group_by_meta.clone(),
                ))
            };
            let mut window_state = window_state;
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                stream_ended = true;
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                for row in collection.rows() {
                                    window_state.add_row(row).map_err(|e| ProcessorError::ProcessingError(format!("Failed to update window state: {e}")))?;
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                window_state.flush_until(ts, &output).await?;
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                let is_graceful = matches!(control_signal, ControlSignal::StreamGracefulEnd);
                                send_with_backpressure(&output, StreamData::control(control_signal)).await?;
                                if is_terminal {
                                    if is_graceful {
                                        window_state.flush_all(&output).await?;
                                    }
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                log_received_data(&id, &other);
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[StreamingTumblingAggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[StreamingTumblingAggregationProcessor:{id}] all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            if stream_ended {
                send_control_with_backpressure(&control_output, ControlSignal::StreamGracefulEnd)
                    .await?;
            }
            Ok(())
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

/// Window manager supporting event-time and processing-time modes.
enum WindowState {
    EventTime(EventWindowState),
    ProcessingTime(ProcessingWindowState),
}

impl WindowState {
    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        match self {
            WindowState::EventTime(state) => state.add_row(row),
            WindowState::ProcessingTime(state) => state.add_row(row),
        }
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        match self {
            WindowState::EventTime(state) => state.flush_until(watermark, output).await,
            WindowState::ProcessingTime(state) => state.flush_until(watermark, output).await,
        }
    }

    async fn flush_all(
        &mut self,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        match self {
            WindowState::EventTime(state) => state.flush_all(output).await,
            WindowState::ProcessingTime(state) => state.flush_all(output).await,
        }
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

/// Event-time windows keyed by start timestamp; watermark drives finalization.
struct EventWindowState {
    windows: BTreeMap<u64, WindowAggState>,
    len_secs: u64,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_meta: Vec<GroupByMeta>,
}

impl EventWindowState {
    fn new(
        len_secs: u64,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_meta: Vec<GroupByMeta>,
    ) -> Self {
        Self {
            windows: BTreeMap::new(),
            len_secs,
            physical,
            aggregate_registry,
            group_by_meta,
        }
    }

    fn add_row(&mut self, row: &crate::model::Tuple) -> Result<(), String> {
        let start_secs = window_start_secs_str(row.timestamp, self.len_secs)?;
        let entry = self.windows.entry(start_secs).or_insert_with(|| {
            WindowAggState::new(
                start_secs,
                self.len_secs,
                Arc::clone(&self.physical),
                Arc::clone(&self.aggregate_registry),
                self.group_by_meta.clone(),
            )
        });
        entry.worker.update_groups(row)
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let watermark_secs = to_secs(watermark, "watermark")?;
        let mut ready = Vec::new();
        for (&start, state) in self.windows.iter() {
            if state.end_secs <= watermark_secs {
                ready.push(start);
            }
        }

        for key in ready {
            if let Some(mut state) = self.windows.remove(&key) {
                if let Some(batch) = state
                    .worker
                    .finalize_current_window()
                    .map_err(ProcessorError::ProcessingError)?
                {
                    send_with_backpressure(output, StreamData::Collection(batch)).await?;
                }
            }
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let keys: Vec<u64> = self.windows.keys().copied().collect();
        for key in keys {
            if let Some(mut state) = self.windows.remove(&key) {
                if let Some(batch) = state
                    .worker
                    .finalize_current_window()
                    .map_err(ProcessorError::ProcessingError)?
                {
                    send_with_backpressure(output, StreamData::Collection(batch)).await?;
                }
            }
        }
        Ok(())
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
        self.windows
            .back_mut()
            .expect("just pushed")
            .worker
            .update_groups(row)
    }

    async fn flush_until(
        &mut self,
        watermark: SystemTime,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let watermark_secs = to_secs(watermark, "watermark")?;
        while let Some(front) = self.windows.front() {
            if front.end_secs > watermark_secs {
                break;
            }
            let mut state = self.windows.pop_front().expect("front exists");
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(ProcessorError::ProcessingError)?
            {
                send_with_backpressure(output, StreamData::Collection(batch)).await?;
            }
        }
        Ok(())
    }

    async fn flush_all(
        &mut self,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        while let Some(mut state) = self.windows.pop_front() {
            if let Some(batch) = state
                .worker
                .finalize_current_window()
                .map_err(ProcessorError::ProcessingError)?
            {
                send_with_backpressure(output, StreamData::Collection(batch)).await?;
            }
        }
        Ok(())
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
