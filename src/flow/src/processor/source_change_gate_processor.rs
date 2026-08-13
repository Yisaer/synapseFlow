//! SourceChangeGateProcessor - suppresses source rows whose tracked columns did not change.

use crate::model::Tuple;
use crate::planner::physical::PhysicalSourceChangeGate;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Default)]
struct SourceChangeGateState {
    previous_tracked_row: Option<Vec<Option<Arc<Value>>>>,
}

pub struct SourceChangeGateProcessor {
    id: String,
    source_name: Arc<str>,
    tracked_column_indexes: Arc<[usize]>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl SourceChangeGateProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalSourceChangeGate>) -> Self {
        Self::new_with_channel_capacities(id, spec, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        spec: Arc<PhysicalSourceChangeGate>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            source_name: Arc::<str>::from(spec.source_name.as_str()),
            tracked_column_indexes: Arc::clone(&spec.tracked_column_indexes),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

fn extract_tracked_row(
    tuple: &Tuple,
    source_name: &str,
    tracked_column_indexes: &[usize],
) -> Result<Vec<Option<Arc<Value>>>, ProcessorError> {
    let message = tuple.message_by_source(source_name).ok_or_else(|| {
        ProcessorError::ProcessingError(format!(
            "source change gate processor failed to resolve source `{source_name}` from runtime tuple"
        ))
    })?;

    tracked_column_indexes
        .iter()
        .map(|&index| {
            message
                .entry_by_index(index)
                .map(|(_, value)| Some(Arc::clone(value)))
                .ok_or_else(|| {
                    ProcessorError::ProcessingError(format!(
                        "source change gate processor failed to resolve source `{source_name}` column index {index} from runtime tuple"
                    ))
                })
        })
        .collect()
}

fn gate_collection(
    collection: Box<dyn crate::model::Collection>,
    source_name: &str,
    tracked_column_indexes: &[usize],
    state: &mut SourceChangeGateState,
    stats: Option<&ProcessorStats>,
) -> Result<Option<Box<dyn crate::model::Collection>>, ProcessorError> {
    let num_rows = collection.num_rows();
    if num_rows == 0 {
        return Ok(None);
    }

    let mut selected_indices = Vec::with_capacity(num_rows);
    for (row_index, tuple) in collection.rows().iter().enumerate() {
        let tracked_row = match extract_tracked_row(tuple, source_name, tracked_column_indexes) {
            Ok(tracked_row) => tracked_row,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "source change gate processor error",
                        error.to_string(),
                    );
                }
                continue;
            }
        };
        let changed = match state.previous_tracked_row.as_ref() {
            Some(previous) => previous != &tracked_row,
            None => true,
        };

        if changed {
            state.previous_tracked_row = Some(tracked_row);
            selected_indices.push(row_index);
        }
    }

    if selected_indices.is_empty() {
        return Ok(None);
    }

    if selected_indices.len() == num_rows {
        return Ok(Some(collection));
    }

    collection
        .take(&selected_indices)
        .map(Some)
        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
}

impl Processor for SourceChangeGateProcessor {
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
        let source_name = Arc::clone(&self.source_name);
        let tracked_column_indexes = Arc::clone(&self.tracked_column_indexes);
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, source = %source_name, "source change gate processor starting");

        ProcessorStart::ready(spawner.spawn(async move {
            let mut state = SourceChangeGateState::default();
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
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                if let Some(rows) = data.row_count() {
                                    stats.record_collection_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match gate_collection(
                                            collection,
                                            source_name.as_ref(),
                                            tracked_column_indexes.as_ref(),
                                            &mut state,
                                            Some(stats.as_ref()),
                                        ) {
                                            Ok(Some(out_collection)) => {
                                                let out_data = StreamData::collection(out_collection);
                                                let out_rows = out_data.row_count();
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    out_data,
                                                    Some(stats.as_ref()),
                                                )
                                                .await;
                                                stats.record_handle_duration(handle_start.elapsed());
                                                send_res?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_collection_out(rows);
                                                }
                                            }
                                            Ok(None) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                            }
                                            Err(err) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged(
                                                    "source change gate processor error",
                                                    err.to_string(),
                                                );
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        let out_rows = data.row_count();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if let Some(rows) = out_rows {
                                            stats.record_collection_out(rows);
                                        }
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "source change gate data input");
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
