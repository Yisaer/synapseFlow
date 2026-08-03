//! TumblingWindowProcessor - buffers rows by tumbling windows and flushes on watermarks.
//!

use crate::planner::logical::TimeUnit;
use crate::planner::physical::{PhysicalPlan, PhysicalTumblingWindow};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, GaugeHandle, MetricKind, MetricSpec, Processor, ProcessorError, ProcessorStart,
    ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

pub struct TumblingWindowProcessor {
    id: String,
    window_length: Duration,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl TumblingWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalTumblingWindow>) -> Self {
        Self::new_with_channel_capacities(id, physical, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalTumblingWindow>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
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
            channel_capacities,
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

        // Local state captured by the task.
        let len_secs = self.window_length.as_secs().max(1);
        let mut state = ProcessingState::new(
            len_secs,
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
                                stats.record_in(collection.num_rows() as u64);
                                let handle_start = std::time::Instant::now();
                                let res = state.add_collection(collection).await;
                                // Tumbling window enqueue/buffer work is local-only.
                                stats.record_handle_duration(handle_start.elapsed());
                                if let Err(e) = res {
                                    stats.record_error_logged("tumbling window processor error", e.to_string());
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
                                    } else {
                                        state.rows_buffered.set(0);
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
                                    // Non-graceful end on data path: drop buffered rows.
                                    state.rows_buffered.set(0);
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
                                state.rows_buffered.set(0);
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

/// Processing-time window state: assumes timestamps are non-decreasing, buffers rows in order.
struct ProcessingState {
    rows: VecDeque<crate::model::Tuple>,
    len_secs: u64,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
    rows_buffered: GaugeHandle,
}

impl ProcessingState {
    fn new(
        len_secs: u64,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        let rows_buffered = stats.register_gauge(MetricSpec {
            id: "window.rows_buffered",
            flat_name: "rows_buffered",
            kind: MetricKind::Gauge,
        });
        rows_buffered.set(0);
        Self {
            rows: VecDeque::new(),
            len_secs,
            output,
            data_channel_capacity,
            stats,
            rows_buffered,
        }
    }

    fn update_rows_buffered(&self) {
        self.rows_buffered.set(self.rows.len() as u64);
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
        self.update_rows_buffered();
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
                let row = self.rows.pop_front().ok_or_else(|| {
                    ProcessorError::ProcessingError(
                        "tumbling window row buffer corrupted during flush".to_string(),
                    )
                })?;
                current_rows.push(row);
            }

            if current_rows.is_empty() {
                continue;
            }
            self.stats.record_out(current_rows.len() as u64);
            let batch = window_metadata::record_batch_from_epoch_secs(
                current_rows,
                window_start,
                window_start.saturating_add(self.len_secs),
            )?;
            send_with_backpressure(
                &self.output,
                self.data_channel_capacity,
                StreamData::collection(Box::new(batch)),
                Some(self.stats.as_ref()),
            )
            .await?;
            self.update_rows_buffered();
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
                let row = self.rows.pop_front().ok_or_else(|| {
                    ProcessorError::ProcessingError(
                        "tumbling window row buffer corrupted during flush_all".to_string(),
                    )
                })?;
                current_rows.push(row);
            }
            if current_rows.is_empty() {
                continue;
            }
            self.stats.record_out(current_rows.len() as u64);
            let batch = window_metadata::record_batch_from_epoch_secs(
                current_rows,
                window_start,
                window_start.saturating_add(self.len_secs),
            )?;
            send_with_backpressure(
                &self.output,
                self.data_channel_capacity,
                StreamData::collection(Box::new(batch)),
                Some(self.stats.as_ref()),
            )
            .await?;
            self.update_rows_buffered();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::PhysicalTumblingWindow;
    use crate::processor::base::{Processor, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::processor::{BarrierControlSignal, ProcessorStats};
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

    #[tokio::test]
    async fn tumbling_window_graceful_end_flushes_buffer_before_terminal_control() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Seconds, 10, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![tuple_at(1), tuple_at(2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::control(ControlSignal::Barrier(
                BarrierControlSignal::StreamGracefulEnd { barrier_id: 1 },
            )))
            .is_ok());

        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");

        let StreamData::Collection(collection) = first else {
            panic!("expected buffered collection before terminal control");
        };
        assert_eq!(collection.rows().len(), 2);
        assert!(matches!(
            second,
            StreamData::Control(ControlSignal::Barrier(
                BarrierControlSignal::StreamGracefulEnd { .. }
            ))
        ));
        assert_eq!(stats.snapshot().records_out, 2);
    }
}
