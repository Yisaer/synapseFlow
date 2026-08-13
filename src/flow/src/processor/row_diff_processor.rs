//! RowDiffProcessor - computes sink-side row diffs while preserving stable row schema.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::PhysicalRowDiff;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::output_row_accessor::OutputRowAccessor;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Default)]
struct RowDiffState {
    previous_output_row: Option<Vec<Option<Arc<Value>>>>,
}

type RowDiffOutput = (Vec<Arc<Value>>, Arc<[bool]>);

pub struct RowDiffProcessor {
    id: String,
    row_accessor: OutputRowAccessor,
    output_keys: Arc<[Arc<str>]>,
    tracked_flags: Arc<[bool]>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl RowDiffProcessor {
    pub fn new(
        id: impl Into<String>,
        physical_row_diff: Arc<PhysicalRowDiff>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(id, physical_row_diff, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_row_diff: Arc<PhysicalRowDiff>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        if physical_row_diff.base.children().len() != 1 {
            return Err(ProcessorError::InvalidConfiguration(
                "row diff processor requires exactly one input child".to_string(),
            ));
        }

        let row_accessor =
            OutputRowAccessor::from_output_layout(physical_row_diff.input_layout.as_ref());
        let output_width = row_accessor.width();
        let output_keys = physical_row_diff
            .output_layout
            .columns
            .iter()
            .map(|column| Arc::clone(&column.name))
            .collect::<Vec<_>>()
            .into();
        let tracked_flags = build_tracked_flags(
            output_width,
            physical_row_diff.tracked_column_indexes.as_ref(),
        )?;
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Ok(Self {
            id: id.into(),
            row_accessor,
            output_keys,
            tracked_flags,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        })
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

fn build_tracked_flags(
    column_count: usize,
    tracked_column_indexes: &[usize],
) -> Result<Arc<[bool]>, ProcessorError> {
    let mut flags = vec![false; column_count];
    for &index in tracked_column_indexes {
        if index >= column_count {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "row diff tracked column index {index} out of bounds for output width {column_count}"
            )));
        }
        flags[index] = true;
    }
    Ok(flags.into())
}

fn extract_output_row(
    row_accessor: &OutputRowAccessor,
    tuple: &Tuple,
) -> Result<Vec<Option<Arc<Value>>>, ProcessorError> {
    Ok(row_accessor.extract_row(tuple)?.into_optional_values())
}

fn build_diff_row(
    tracked_current_values: &[Option<Arc<Value>>],
    previous_tracked_row: Option<&[Option<Arc<Value>>]>,
    tracked_flags: &[bool],
    output_column_names: &[Arc<str>],
) -> Result<RowDiffOutput, ProcessorError> {
    let missing_tracked_columns = tracked_current_values
        .iter()
        .enumerate()
        .filter_map(|(idx, current_value)| {
            tracked_flags
                .get(idx)
                .copied()
                .unwrap_or(false)
                .then_some((idx, current_value))
        })
        .filter(|(_, current_value)| current_value.as_ref().is_none())
        .map(|(idx, _)| {
            output_column_names
                .get(idx)
                .map(|name| name.to_string())
                .unwrap_or_else(|| format!("#{idx}"))
        })
        .collect::<Vec<_>>();
    if !missing_tracked_columns.is_empty() {
        return Err(ProcessorError::ProcessingError(format!(
            "row diff processor failed to resolve tracked output columns [{}] from runtime tuple",
            missing_tracked_columns.join(", ")
        )));
    }

    let mut diff_values = Vec::with_capacity(tracked_current_values.len());
    let mut output_mask = Vec::with_capacity(tracked_current_values.len());

    for (idx, current_value) in tracked_current_values.iter().enumerate() {
        let is_tracked = tracked_flags.get(idx).copied().unwrap_or(false);
        if !is_tracked {
            diff_values.push(
                current_value
                    .as_ref()
                    .map(Arc::clone)
                    .unwrap_or_else(|| Arc::new(Value::Null)),
            );
            output_mask.push(true);
            continue;
        }

        let Some(current_value) = current_value.as_ref() else {
            return Err(ProcessorError::ProcessingError(format!(
                "row diff processor failed to resolve tracked output column {} from runtime tuple",
                output_column_names
                    .get(idx)
                    .map(|name| name.as_ref())
                    .unwrap_or("unknown")
            )));
        };

        let changed = previous_tracked_row
            .and_then(|previous| previous.get(idx))
            .and_then(|previous| previous.as_ref())
            .is_none_or(|previous| previous.as_ref() != current_value.as_ref());

        if changed {
            diff_values.push(Arc::clone(current_value));
            output_mask.push(true);
        } else {
            diff_values.push(Arc::new(Value::Null));
            output_mask.push(false);
        }
    }

    Ok((diff_values, output_mask.into()))
}

fn materialize_diff_tuple(
    base_tuple: &Tuple,
    output_keys: &Arc<[Arc<str>]>,
    diff_values: Vec<Arc<Value>>,
    output_mask: Arc<[bool]>,
) -> Tuple {
    let msg = Arc::new(Message::new_shared_keys(
        Arc::<str>::from(""),
        Arc::clone(output_keys),
        diff_values,
    ));
    let mut output_tuple = Tuple::with_timestamp(Arc::from(vec![msg]), base_tuple.timestamp);
    output_tuple.set_output_mask_shared(output_mask);
    output_tuple
}

fn apply_row_diff(
    input_collection: Box<dyn Collection>,
    row_accessor: &OutputRowAccessor,
    output_keys: &Arc<[Arc<str>]>,
    tracked_flags: &[bool],
    state: &mut RowDiffState,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let metadata = input_collection.metadata().clone();
    let input_rows = input_collection.into_rows().map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to materialize row diff input: {err}"))
    })?;
    let mut output_rows = Vec::with_capacity(input_rows.len());

    for tuple in input_rows {
        let current_values = match extract_output_row(row_accessor, &tuple) {
            Ok(current_values) => current_values,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged("row diff processor error", error.to_string());
                }
                continue;
            }
        };
        let (diff_values, output_mask) = match build_diff_row(
            current_values.as_slice(),
            state.previous_output_row.as_deref(),
            tracked_flags,
            output_keys.as_ref(),
        ) {
            Ok(diff) => diff,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged("row diff processor error", error.to_string());
                }
                continue;
            }
        };
        state.previous_output_row = Some(current_values);

        output_rows.push(materialize_diff_tuple(
            &tuple,
            output_keys,
            diff_values,
            output_mask,
        ));
    }

    let output = RecordBatch::new_with_metadata(output_rows, metadata).map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to build row diff output: {err}"))
    })?;
    Ok(Box::new(output))
}

impl Processor for RowDiffProcessor {
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
        let row_accessor = self.row_accessor.clone();
        let output_keys = Arc::clone(&self.output_keys);
        let tracked_flags = Arc::clone(&self.tracked_flags);
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "row diff processor starting");

        ProcessorStart::ready(spawner.spawn(async move {
            let mut state = RowDiffState::default();
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
                                        match apply_row_diff(
                                            collection,
                                            &row_accessor,
                                            &output_keys,
                                            tracked_flags.as_ref(),
                                            &mut state,
                                            Some(stats.as_ref()),
                                        ) {
                                            Ok(out_collection) => {
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
                                            Err(err) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged("row diff processor error", err.to_string());
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "row diff data input");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_diff_row_returns_processing_error_for_missing_tracked_columns() {
        let output_column_names = [Arc::<str>::from("x")];
        let err = build_diff_row(&[None], None, &[true], &output_column_names).unwrap_err();

        match err {
            ProcessorError::ProcessingError(message) => {
                assert_eq!(
                    message,
                    "row diff processor failed to resolve tracked output columns [x] from runtime tuple"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
