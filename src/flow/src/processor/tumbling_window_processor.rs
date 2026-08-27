//! TumblingWindowProcessor - buffers rows by tumbling windows and flushes on watermarks.
//!

use crate::expr::ScalarExpr;
use crate::planner::physical::{PhysicalPlan, PhysicalTumblingWindow};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::window_partition::{eval_partition_key, PartitionKey};
use crate::processor::{
    ControlSignal, GaugeHandle, MetricKind, MetricSpec, Processor, ProcessorError, ProcessorStart,
    ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

pub struct TumblingWindowProcessor {
    id: String,
    window_length: Duration,
    partition_by_scalars: Vec<ScalarExpr>,
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
        let length = physical.time_unit.duration(physical.length);
        let partition_by_scalars = physical.partition_by_scalars.clone();
        Self {
            id: id.into(),
            window_length: length,
            partition_by_scalars,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::TumblingWindow(window) => Some(Self::new(id, Arc::new(window.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
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
        let partition_by_scalars = self.partition_by_scalars.clone();

        // Local state captured by the task.
        let window_length = self.window_length;
        let mut state = PartitionedProcessingState::new(
            window_length,
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
                                stats.record_collection_in(collection.num_rows() as u64);
                                let handle_start = std::time::Instant::now();
                                let res = state.add_collection(collection).await;
                                // Tumbling window enqueue/buffer work is local-only.
                                stats.record_handle_duration(handle_start.elapsed());
                                if let Err(err) = res {
                                    stats.record_error_logged(
                                        "tumbling window processor error",
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
                                    } else {
                                        state.drop_all();
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
                                    state.drop_all();
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
                                state.drop_all();
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
    window_length: Duration,
    partition_by_scalars: Vec<ScalarExpr>,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
    rows_buffered: GaugeHandle,
}

impl PartitionedProcessingState {
    fn new(
        window_length: Duration,
        partition_by_scalars: Vec<ScalarExpr>,
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
            state_ids: HashMap::new(),
            states: Vec::new(),
            window_length,
            partition_by_scalars,
            output,
            data_channel_capacity,
            stats,
            rows_buffered,
        }
    }

    async fn add_collection(
        &mut self,
        collection: Box<dyn crate::model::Collection>,
    ) -> Result<(), ProcessorError> {
        let rows = match collection.into_rows() {
            Ok(rows) => rows,
            Err(e) => {
                self.stats.record_error_logged(
                    "tumbling window processor error",
                    format!("failed to extract rows: {e}"),
                );
                return Ok(());
            }
        };
        for tuple in rows {
            let window_start = match window_metadata::floor_to_window_start(
                tuple.timestamp,
                self.window_length,
                "timestamp",
            ) {
                Ok(start) => start,
                Err(err) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", err.to_string());
                    continue;
                }
            };
            let window_end = match window_start.checked_add(self.window_length) {
                Some(end) => end,
                None => {
                    self.stats.record_error_logged(
                        "tumbling window processor error",
                        "tumblingwindow end timestamp overflow".to_string(),
                    );
                    continue;
                }
            };
            if let Err(err) = window_metadata::validate_system_time(window_end) {
                self.stats
                    .record_error_logged("tumbling window processor error", err.to_string());
                continue;
            }
            let partition_key =
                match eval_partition_key(&self.partition_by_scalars, &tuple, "tumblingwindow") {
                    Ok(key) => key,
                    Err(message) => {
                        self.stats
                            .record_error_logged("tumbling window processor error", message);
                        continue;
                    }
                };
            self.state_for(partition_key).add_tuple(tuple);
        }
        self.update_rows_buffered();
        Ok(())
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        for state in &mut self.states {
            state.flush_up_to(watermark).await?;
        }
        self.update_rows_buffered();
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        for state in &mut self.states {
            state.flush_all().await?;
        }
        self.update_rows_buffered();
        Ok(())
    }

    fn drop_all(&mut self) {
        for state in &mut self.states {
            state.rows.clear();
        }
        self.update_rows_buffered();
    }

    fn update_rows_buffered(&self) {
        let total = self
            .states
            .iter()
            .map(|state| state.rows.len() as u64)
            .sum();
        self.rows_buffered.set(total);
    }

    fn state_for(&mut self, key: PartitionKey) -> &mut ProcessingState {
        if let Some(id) = self.state_ids.get(&key).copied() {
            return &mut self.states[id];
        }

        let id = self.states.len();
        self.state_ids.insert(key, id);
        self.states.push(ProcessingState::new(
            self.window_length,
            self.output.clone(),
            self.data_channel_capacity,
            Arc::clone(&self.stats),
        ));
        &mut self.states[id]
    }
}

/// Processing-time window state: assumes timestamps are non-decreasing, buffers rows in order.
struct ProcessingState {
    rows: VecDeque<crate::model::Tuple>,
    window_length: Duration,
    output: LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: Arc<ProcessorStats>,
}

impl ProcessingState {
    fn new(
        window_length: Duration,
        output: LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: Arc<ProcessorStats>,
    ) -> Self {
        Self {
            rows: VecDeque::new(),
            window_length,
            output,
            data_channel_capacity,
            stats,
        }
    }

    fn add_tuple(&mut self, tuple: crate::model::Tuple) {
        self.rows.push_back(tuple);
    }

    async fn flush_up_to(&mut self, watermark: SystemTime) -> Result<(), ProcessorError> {
        // Flush whole windows whose end <= watermark.
        while let Some(front) = self.rows.front() {
            let window_start = match window_metadata::floor_to_window_start(
                front.timestamp,
                self.window_length,
                "timestamp",
            ) {
                Ok(start) => start,
                Err(err) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", err.to_string());
                    self.rows.pop_front();
                    continue;
                }
            };
            let window_end = match window_start.checked_add(self.window_length) {
                Some(end) => end,
                None => {
                    self.stats.record_error_logged(
                        "tumbling window processor error",
                        "tumblingwindow end timestamp overflow".to_string(),
                    );
                    self.rows.pop_front();
                    continue;
                }
            };
            if window_end > watermark {
                break;
            }

            let mut current_rows = Vec::new();
            while let Some(row) = self.rows.front() {
                let row_start = match window_metadata::floor_to_window_start(
                    row.timestamp,
                    self.window_length,
                    "timestamp",
                ) {
                    Ok(start) => start,
                    Err(err) => {
                        self.stats.record_error_logged(
                            "tumbling window processor error",
                            err.to_string(),
                        );
                        self.rows.pop_front();
                        continue;
                    }
                };
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
            let row_count = current_rows.len() as u64;
            let batch = match window_metadata::record_batch_from_system_time(
                current_rows,
                window_start,
                window_end,
            ) {
                Ok(batch) => batch,
                Err(err) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", err.to_string());
                    continue;
                }
            };
            send_with_backpressure(
                &self.output,
                self.data_channel_capacity,
                StreamData::collection(Box::new(batch)),
                Some(self.stats.as_ref()),
            )
            .await?;
            self.stats.record_collection_out(row_count);
        }
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<(), ProcessorError> {
        while let Some(front) = self.rows.front() {
            let window_start = match window_metadata::floor_to_window_start(
                front.timestamp,
                self.window_length,
                "timestamp",
            ) {
                Ok(start) => start,
                Err(err) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", err.to_string());
                    self.rows.pop_front();
                    continue;
                }
            };
            let mut current_rows = Vec::new();
            while let Some(row) = self.rows.front() {
                let row_start = match window_metadata::floor_to_window_start(
                    row.timestamp,
                    self.window_length,
                    "timestamp",
                ) {
                    Ok(start) => start,
                    Err(err) => {
                        self.stats.record_error_logged(
                            "tumbling window processor error",
                            err.to_string(),
                        );
                        self.rows.pop_front();
                        continue;
                    }
                };
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
            let row_count = current_rows.len() as u64;
            let window_end = match window_start.checked_add(self.window_length) {
                Some(end) => end,
                None => {
                    self.stats.record_error_logged(
                        "tumbling window processor error",
                        "tumblingwindow end timestamp overflow".to_string(),
                    );
                    continue;
                }
            };
            let batch = match window_metadata::record_batch_from_system_time(
                current_rows,
                window_start,
                window_end,
            ) {
                Ok(batch) => batch,
                Err(err) => {
                    self.stats
                        .record_error_logged("tumbling window processor error", err.to_string());
                    continue;
                }
            };
            send_with_backpressure(
                &self.output,
                self.data_channel_capacity,
                StreamData::collection(Box::new(batch)),
                Some(self.stats.as_ref()),
            )
            .await?;
            self.stats.record_collection_out(row_count);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::TimeUnit;
    use crate::planner::physical::PhysicalTumblingWindow;
    use crate::processor::base::{Processor, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::processor::{BarrierControlSignal, ProcessorStats};
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

    fn tuple_at_millis(millis: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(millis),
        )
    }

    #[tokio::test]
    async fn tumbling_window_graceful_end_flushes_buffer_before_terminal_control() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Seconds, 10, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::collection_in_out());
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
        assert_eq!(stats.snapshot().records_out, Some(2));
    }

    #[tokio::test]
    async fn tumbling_window_skips_invalid_timestamp_and_preserves_buffered_rows() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Seconds, 10, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let stats = Arc::new(ProcessorStats::collection_in_out());
        processor.set_stats(Arc::clone(&stats));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let invalid_timestamp = UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .expect("one second before epoch is representable");
        let invalid_tuple = crate::model::Tuple::with_timestamp(
            crate::model::Tuple::empty_messages(),
            invalid_timestamp,
        );
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1), invalid_tuple, tuple_at(2)])
            .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(UNIX_EPOCH + Duration::from_secs(10)))
            .is_ok());

        let item = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(collection) = item else {
            panic!("expected tumbling window collection");
        };
        assert_eq!(collection.rows().len(), 2);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.error_count, 1);
        assert!(snapshot
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("invalid timestamp")));
    }

    #[tokio::test]
    async fn millisecond_tumbling_window_uses_exact_boundaries_and_metadata() {
        let spawner = test_spawner();
        let physical = PhysicalTumblingWindow::new(TimeUnit::Milliseconds, 100, Vec::new(), 0);
        let mut processor = TumblingWindowProcessor::new("tw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_DATA_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start(&spawner);

        let batch = crate::model::RecordBatch::new(vec![
            tuple_at_millis(1_050),
            tuple_at_millis(1_099),
            tuple_at_millis(1_100),
        ])
        .expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_099),
            ))
            .is_ok());
        assert!(
            timeout(Duration::from_millis(100), output_rx.recv())
                .await
                .is_err(),
            "window must not flush before its end boundary"
        );

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_100),
            ))
            .is_ok());
        let first = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(first) = first else {
            panic!("expected first millisecond tumbling window");
        };
        assert_eq!(first.num_rows(), 2);
        let metadata = first.metadata().window.as_deref().expect("window metadata");
        assert_eq!(metadata.start.epoch_micros(), 1_000_000);
        assert_eq!(metadata.end.epoch_micros(), 1_100_000);

        assert!(input
            .send(StreamData::watermark(
                UNIX_EPOCH + Duration::from_millis(1_200),
            ))
            .is_ok());
        let second = timeout(Duration::from_secs(2), output_rx.recv())
            .await
            .expect("timeout")
            .expect("recv");
        let StreamData::Collection(second) = second else {
            panic!("expected second millisecond tumbling window");
        };
        assert_eq!(second.num_rows(), 1);
    }
}
