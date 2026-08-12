use super::{build_group_by_meta, AggregationWorker, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::Collection;
use crate::planner::physical::PhysicalStreamingAggregation;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::window_metadata;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// EOS-driven streaming aggregation implementation.
pub struct StreamingEosAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    group_by_meta: Vec<GroupByMeta>,
    stats: Arc<ProcessorStats>,
}

impl StreamingEosAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            physical,
            aggregate_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let group_by_meta =
            build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            group_by_meta,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        collection: &dyn Collection,
        opened_at: &mut Option<SystemTime>,
        last_seen_at: &mut Option<SystemTime>,
        stats: &ProcessorStats,
    ) -> Result<(), String> {
        for row in collection.rows() {
            match window_metadata::validate_system_time(row.timestamp) {
                Ok(()) => {}
                Err(ProcessorError::ProcessingError(message)) => {
                    stats.record_error_logged("streaming EOS aggregation processor error", message);
                    continue;
                }
                Err(err) => return Err(err.to_string()),
            }
            if let Err(message) = worker.update_groups(row) {
                stats.record_error_logged("streaming EOS aggregation processor error", message);
                continue;
            }
            if opened_at.is_none() {
                *opened_at = Some(row.timestamp);
            }
            *last_seen_at = Some(row.timestamp);
        }
        Ok(())
    }

    async fn flush_current_window(
        worker: &mut AggregationWorker,
        output: &LinkOutput<StreamData>,
        channel_capacity: usize,
        stats: &ProcessorStats,
        opened_at: Option<SystemTime>,
        last_seen_at: Option<SystemTime>,
    ) -> Result<(), ProcessorError> {
        if let Some(collection) = worker.finalize_current_window().map_err(|err| {
            ProcessorError::ProcessingError(format!("Failed to finalize EOS aggregation: {err}"))
        })? {
            let closed_at = last_seen_at.or(opened_at).unwrap_or(SystemTime::UNIX_EPOCH);
            let collection = window_metadata::attach_from_system_time(
                collection,
                opened_at.unwrap_or(closed_at),
                closed_at,
            )?;
            stats.record_out(collection.num_rows() as u64);
            send_with_backpressure(
                output,
                channel_capacity,
                StreamData::Collection(collection),
                Some(stats),
            )
            .await?;
        }
        Ok(())
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for StreamingEosAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_meta = self.group_by_meta.clone();
        let stats = Arc::clone(&self.stats);

        ProcessorStart::ready(spawner.spawn(async move {
            let mut worker = AggregationWorker::new(physical, aggregate_registry, group_by_meta);
            let mut opened_at: Option<SystemTime> = None;
            let mut last_seen_at: Option<SystemTime> = None;

            loop {
                tokio::select! {
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
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
                                        let handle_start = std::time::Instant::now();
                                        let result = Self::process_collection(
                                            &mut worker,
                                            collection.as_ref(),
                                            &mut opened_at,
                                            &mut last_seen_at,
                                            stats.as_ref(),
                                        );
                                        stats.record_handle_duration(handle_start.elapsed());
                                        if let Err(err) = result {
                                            return Err(ProcessorError::ProcessingError(format!(
                                                "Failed to process streaming EOS aggregation: {err}"
                                            )));
                                        }
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        let is_graceful = control_signal.is_graceful_end();
                                        if is_terminal {
                                            if is_graceful {
                                                Self::flush_current_window(
                                                    &mut worker,
                                                    &output,
                                                    channel_capacities.data,
                                                    stats.as_ref(),
                                                    opened_at.take(),
                                                    last_seen_at.take(),
                                                )
                                                .await?;
                                            }
                                            send_with_backpressure(
                                                &output,
                                                channel_capacities.data,
                                                StreamData::control(control_signal),
                                                Some(stats.as_ref()),
                                            )
                                            .await?;
                                            break;
                                        }
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            StreamData::control(control_signal),
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                    }
                                    other => {
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            other,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
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
