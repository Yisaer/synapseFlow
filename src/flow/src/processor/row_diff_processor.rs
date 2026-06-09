//! RowDiffProcessor - computes sink-side row diffs while preserving stable row schema.

use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{ByIndexProjection, PhysicalRowDiff};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    ProcessorChannelCapacities,
};
use crate::processor::output_row_accessor::OutputRowAccessor;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Default)]
struct RowDiffState {
    previous_tracked_row: Option<Vec<Option<Arc<Value>>>>,
}

type RowDiffOutput = (Vec<Arc<Value>>, Arc<[bool]>);

#[derive(Clone)]
enum RowDiffInputExtractor {
    Materialized(OutputRowAccessor),
    LateProjection {
        spec: Arc<ByIndexProjection>,
        output_width: usize,
    },
    Hybrid {
        materialized: OutputRowAccessor,
        late_projection: Arc<ByIndexProjection>,
    },
}

pub struct RowDiffProcessor {
    id: String,
    input_extractor: RowDiffInputExtractor,
    output_accessor: OutputRowAccessor,
    tracked_flags: Arc<[bool]>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
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

        let output_accessor =
            OutputRowAccessor::from_output_schema(physical_row_diff.output_schema.as_ref());
        let output_width = output_accessor.width();
        let input_extractor = match physical_row_diff.late_projection.as_ref() {
            Some(spec) => {
                validate_late_projection(spec.as_ref(), output_width)?;
                if spec.columns().len() == output_width {
                    RowDiffInputExtractor::LateProjection {
                        spec: Arc::clone(spec),
                        output_width,
                    }
                } else {
                    RowDiffInputExtractor::Hybrid {
                        materialized: OutputRowAccessor::from_output_schema(
                            physical_row_diff.output_schema.as_ref(),
                        ),
                        late_projection: Arc::clone(spec),
                    }
                }
            }
            None => RowDiffInputExtractor::Materialized(OutputRowAccessor::from_output_schema(
                physical_row_diff.output_schema.as_ref(),
            )),
        };
        let tracked_flags = build_tracked_flags(
            output_width,
            physical_row_diff.tracked_column_indexes.as_ref(),
        )?;
        let (output, _) = broadcast::channel(channel_capacities.data);
        let (control_output, _) = broadcast::channel(channel_capacities.control);
        Ok(Self {
            id: id.into(),
            input_extractor,
            output_accessor,
            tracked_flags,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        })
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
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

fn validate_late_projection(
    spec: &ByIndexProjection,
    output_width: usize,
) -> Result<(), ProcessorError> {
    if spec.is_empty() {
        return Err(ProcessorError::InvalidConfiguration(
            "row diff late projection cannot be empty".to_string(),
        ));
    }
    if spec.columns().len() > output_width {
        return Err(ProcessorError::InvalidConfiguration(format!(
            "row diff late projection width {} exceeds output width {}",
            spec.columns().len(),
            output_width
        )));
    }

    let mut seen_output_indexes = vec![false; output_width];
    for column in spec.columns() {
        if column.output_index >= output_width {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "row diff late projection output index {} out of bounds for output width {}",
                column.output_index, output_width
            )));
        }
        if std::mem::replace(&mut seen_output_indexes[column.output_index], true) {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "row diff late projection has duplicate output index {}",
                column.output_index
            )));
        }
    }

    Ok(())
}

fn extract_selected_row_from_materialized(
    row_accessor: &mut OutputRowAccessor,
    tuple: &Tuple,
    selected: &[bool],
) -> Result<Vec<Option<Arc<Value>>>, ProcessorError> {
    Ok(row_accessor
        .extract_selected_row(tuple, selected)?
        .into_optional_values())
}

fn extract_projected_values(
    spec: &ByIndexProjection,
    output_width: usize,
    tuple: &Tuple,
) -> Result<Vec<Option<Arc<Value>>>, ProcessorError> {
    let mut values = vec![None; output_width];
    let mut missing_columns = Vec::new();

    for column in spec.columns() {
        let value = tuple
            .message_by_source(column.source_name.as_ref())
            .and_then(|message| message.entry_by_index(column.column_index))
            .map(|(_, value)| Arc::clone(value));

        match value {
            Some(value) => values[column.output_index] = Some(value),
            None => missing_columns.push(format!(
                "{}#{}->{}",
                column.source_name.as_ref(),
                column.column_index,
                column.output_name.as_ref()
            )),
        }
    }

    if !missing_columns.is_empty() {
        return Err(ProcessorError::ProcessingError(format!(
            "row diff processor failed to resolve late projection columns [{}] from runtime tuple",
            missing_columns.join(", ")
        )));
    }

    Ok(values)
}

fn extract_tracked_row(
    input_extractor: &mut RowDiffInputExtractor,
    tuple: &Tuple,
    tracked_flags: &[bool],
) -> Result<Vec<Option<Arc<Value>>>, ProcessorError> {
    match input_extractor {
        RowDiffInputExtractor::Materialized(row_accessor) => {
            extract_selected_row_from_materialized(row_accessor, tuple, tracked_flags)
        }
        RowDiffInputExtractor::LateProjection { spec, output_width } => {
            Ok(extract_projected_values(spec, *output_width, tuple)?
                .into_iter()
                .enumerate()
                .map(|(idx, value)| {
                    if tracked_flags.get(idx).copied().unwrap_or(false) {
                        value
                    } else {
                        None
                    }
                })
                .collect())
        }
        RowDiffInputExtractor::Hybrid {
            materialized,
            late_projection,
        } => {
            let mut values = materialized
                .extract_selected_row(tuple, tracked_flags)?
                .into_optional_values();
            let projected_values =
                extract_projected_values(late_projection, tracked_flags.len(), tuple)?;
            for (index, value) in projected_values.into_iter().enumerate() {
                if let Some(value) = value {
                    values[index] = Some(value);
                }
            }
            Ok(values)
        }
    }
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
        if !tracked_flags.get(idx).copied().unwrap_or(false) {
            diff_values.push(Arc::new(Value::Null));
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

fn apply_row_diff(
    input_collection: Box<dyn Collection>,
    input_extractor: &mut RowDiffInputExtractor,
    output_accessor: &OutputRowAccessor,
    output_column_names: &[Arc<str>],
    tracked_flags: &[bool],
    state: &mut RowDiffState,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let input_rows = input_collection.into_rows().map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to materialize row diff input: {err}"))
    })?;
    let mut output_rows = Vec::with_capacity(input_rows.len());

    for tuple in input_rows {
        let tracked_current_values = extract_tracked_row(input_extractor, &tuple, tracked_flags)?;
        let (diff_values, output_mask) = build_diff_row(
            tracked_current_values.as_slice(),
            state.previous_tracked_row.as_deref(),
            tracked_flags,
            output_column_names,
        )?;
        state.previous_tracked_row = Some(tracked_current_values);

        let output_tuple =
            output_accessor.overlay_tuple(&tuple, &diff_values, tracked_flags, Some(output_mask));
        output_rows.push(output_tuple);
    }

    let output = RecordBatch::new(output_rows).map_err(|err| {
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
        let mut input_extractor = self.input_extractor.clone();
        let output_accessor = self.output_accessor.clone();
        let output_column_names = output_accessor.column_names();
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
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match apply_row_diff(
                                            collection,
                                            &mut input_extractor,
                                            &output_accessor,
                                            output_column_names.as_ref(),
                                            tracked_flags.as_ref(),
                                            &mut state,
                                        ) {
                                            Ok(out_collection) => {
                                                let out_data = StreamData::collection(out_collection);
                                                let out_rows = out_data.num_rows_hint();
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
                                                    stats.record_out(rows);
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
