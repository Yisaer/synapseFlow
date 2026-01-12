//! TumblingWindowProcessor - buffers rows by tumbling windows and flushes on watermarks.
//!

use crate::planner::logical::TimeUnit;
use crate::planner::physical::{PhysicalPlan, PhysicalTumblingWindow};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

pub struct TumblingWindowProcessor {
    id: String,
    window_length: Duration,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

impl TumblingWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalTumblingWindow>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let length = match physical.time_unit {
            TimeUnit::Seconds => Duration::from_secs(physical.length),
        };
        Self {
            id: id.into(),
            window_length: length,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::TumblingWindow(window) => Some(Self::new(id, Arc::new(window.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for TumblingWindowProcessor {
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
        let stats = Arc::clone(&self.stats);

        // Local state captured by the task.
        let len_secs = self.window_length.as_secs().max(1);
        let mut state = ProcessingState::new(len_secs, output.clone(), Arc::clone(&stats));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
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
                                stats.record_in(collection.num_rows() as u64);
                                if let Err(e) = state.add_collection(collection).await {
                                    stats.record_error(e.to_string());
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
                                    // Non-graceful end on data path: drop buffered rows.
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "tumbling window data input");
                                continue;
                            }
                            None => {
                                // Upstream ended without control signal: drop buffered rows.
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

/// Processing-time window state: assumes timestamps are non-decreasing, buffers rows in order.
struct ProcessingState {
    rows: VecDeque<crate::model::Tuple>,
    len_secs: u64,
    output: broadcast::Sender<StreamData>,
    stats: Arc<ProcessorStats>,
}

impl ProcessingState {
    fn new(
        len_secs: u64,
        output: broadcast::Sender<StreamData>,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            len_secs,
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
            self.rows.push_back(tuple);
        }
        Ok(())
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        // Flush whole windows whose end <= watermark.
        while let Some(front) = self.rows.front() {
            let window_start = window_start_secs(front.timestamp, self.len_secs)?;
            let window_end = SystemTime::UNIX_EPOCH
                + Duration::from_secs(window_start.saturating_add(self.len_secs));
            if window_end > watermark {
                break;
            }

            let mut current_rows = Vec::new();
            while let Some(row) = self.rows.front() {
                let row_start = window_start_secs(row.timestamp, self.len_secs)?;
                if row_start != window_start {
                    break;
                }
                current_rows.push(self.rows.pop_front().unwrap());
            }

            if current_rows.is_empty() {
                continue;
            }
            self.stats.record_out(current_rows.len() as u64);
            let batch = crate::model::RecordBatch::new(current_rows)
                .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
            send_with_backpressure(&self.output, StreamData::collection(Box::new(batch))).await?;
        }
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        while let Some(front) = self.rows.front() {
            let window_start = window_start_secs(front.timestamp, self.len_secs)?;
            let mut current_rows = Vec::new();
            while let Some(row) = self.rows.front() {
                let row_start = window_start_secs(row.timestamp, self.len_secs)?;
                if row_start != window_start {
                    break;
                }
                current_rows.push(self.rows.pop_front().unwrap());
            }
            if current_rows.is_empty() {
                continue;
            }
            let batch = crate::model::RecordBatch::new(current_rows)
                .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
            send_with_backpressure(&self.output, StreamData::collection(Box::new(batch))).await?;
        }
        Ok(())
    }
}

fn window_start_secs(ts: SystemTime, len_secs: u64) -> Result<u64, ProcessorError> {
    let epoch = ts
        .duration_since(UNIX_EPOCH)
        .map_err(|e| ProcessorError::ProcessingError(format!("invalid timestamp: {e}")))?;
    let secs = epoch.as_secs();
    let len = len_secs.max(1);
    Ok(secs / len * len)
}
