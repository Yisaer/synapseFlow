use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::PhysicalStreamingAggregation;
use crate::processor::base::{
    attach_stats_to_collect_barrier, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Tracks progress for a count-based window.
struct CountWindowState {
    target: u64,
    seen: u64,
}

impl CountWindowState {
    fn new(target: u64) -> Self {
        Self { target, seen: 0 }
    }

    fn register_row_and_check_finalize(&mut self) -> bool {
        self.seen += 1;
        self.seen >= self.target
    }

    fn reset(&mut self) {
        self.seen = 0;
    }
}

/// Data-driven count window implementation.
pub struct StreamingCountAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_meta: Vec<GroupByMeta>,
    target: u64,
    stats: Arc<ProcessorStats>,
}

impl StreamingCountAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
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
            target,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        window_state: &mut CountWindowState,
        collection: &dyn Collection,
    ) -> Result<Vec<Box<dyn Collection>>, String> {
        let mut outputs = Vec::new();
        for row in collection.rows() {
            worker.update_groups(row)?;

            if window_state.register_row_and_check_finalize() {
                if let Some(batch) = worker.finalize_current_window()? {
                    outputs.push(batch);
                }
                window_state.reset();
            }
        }
        Ok(outputs)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for StreamingCountAggregationProcessor {
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
        let target = self.target;
        let stats = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut worker = AggregationWorker::new(physical, aggregate_registry, group_by_meta);
            let mut window_state = CountWindowState::new(target);

            loop {
                tokio::select! {
                    // Handle control signals first if present
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let control_signal =
                                attach_stats_to_collect_barrier(control_signal, &id, &stats);
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
                                        stats.record_in(collection.num_rows() as u64);
                                        match StreamingCountAggregationProcessor::process_collection(
                                            &mut worker,
                                            &mut window_state,
                                            collection.as_ref(),
                                        ) {
                                            Ok(outputs) => {
                                                for out in outputs {
                                                    stats.record_out(out.num_rows() as u64);
                                                    let data = StreamData::Collection(out);
                                                    send_with_backpressure(&output, data).await?
                                                }
                                            }
                                            Err(e) => {
                                                return Err(ProcessorError::ProcessingError(
                                                    format!(
                                                        "Failed to process streaming count aggregation: {e}"
                                                    ),
                                                ));
                                            }
                                        }
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        send_with_backpressure(
                                            &output,
                                            StreamData::control(control_signal),
                                        )
                                    .await?;
                                        if is_terminal {
                                            break;
                                        }
                                    }
                                    other => {
                                        send_with_backpressure(&output, other).await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&id, n, "data input");
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
