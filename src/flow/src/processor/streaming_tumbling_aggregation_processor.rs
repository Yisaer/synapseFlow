use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_received_data, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::collections::VecDeque;
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

        tokio::spawn(async move {
            let mut window_state = ProcessingWindowState::new(
                len_secs,
                Arc::clone(&physical),
                Arc::clone(&aggregate_registry),
                group_by_meta.clone(),
            );

            loop {
                tokio::select! {
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                                        for row in collection.rows() {
                                            window_state.add_row(row).map_err(|e| {
                                                ProcessorError::ProcessingError(format!(
                                                    "Failed to update window state: {e}"
                                                ))
                                            })?;
                                        }
                                    }
                                    StreamData::Watermark(ts) => {
                                        window_state.flush_until(ts, &output).await?;
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        let is_graceful = control_signal.is_graceful_end();
                                        send_with_backpressure(
                                            &output,
                                            StreamData::control(control_signal),
                                        )
                                        .await?;
                                        if is_terminal {
                                            if is_graceful {
                                                window_state.flush_all(&output).await?;
                                            }
                                            break;
                                        }
                                    }
                                    other => {
                                        send_with_backpressure(&output, other).await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                tracing::warn!(processor_id = %id, skipped = n, "input lagged");
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
