//! ColumnFilterProcessor - applies per-sink column include/exclude filtering.

use crate::model::{Collection, Message, Tuple};
use crate::planner::physical::PhysicalColumnFilter;
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

/// Resolved index-level mapping: which position to read from the input tuple
/// and where to write in the output message.
#[derive(Clone)]
struct ResolvedKeep {
    /// Index into tuple.messages().
    msg_index: usize,
    /// Index into message.entries() for the key.
    key_index: usize,
    /// Output key name.
    output_key: Arc<str>,
}

/// Index-level description of a single output message.
#[derive(Clone)]
struct ResolvedMessage {
    source_name: Arc<str>,
    columns: Vec<ResolvedKeep>,
}

pub struct ColumnFilterProcessor {
    id: String,
    /// Per-column metadata from the physical plan.
    keep_specs: Vec<crate::planner::physical::ColumnFilterKeepSpec>,
    /// Lazily resolved index mapping, built on first tuple.
    resolved: Option<Vec<ResolvedMessage>>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl ColumnFilterProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalColumnFilter>) -> Self {
        Self::new_with_channel_capacities(id, spec, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        spec: Arc<PhysicalColumnFilter>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            keep_specs: spec.keep_specs.clone(),
            resolved: None,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

fn resolve_keep_specs(
    tuple: &Tuple,
    specs: &[crate::planner::physical::ColumnFilterKeepSpec],
) -> Result<Vec<ResolvedMessage>, ProcessorError> {
    let messages = tuple.messages();
    // Group specs by source_name to reconstruct output messages.
    let mut groups: Vec<(
        Arc<str>,
        Vec<&crate::planner::physical::ColumnFilterKeepSpec>,
    )> = Vec::new();
    for spec in specs {
        if let Some(group) = groups
            .iter_mut()
            .find(|g| g.0.as_ref() == spec.source_name.as_ref())
        {
            group.1.push(spec);
        } else {
            groups.push((Arc::clone(&spec.source_name), vec![spec]));
        }
    }

    let mut resolved_messages = Vec::with_capacity(groups.len());
    for (source_name, group_specs) in groups {
        // Empty source_name → affiliate columns, pass through.
        if source_name.is_empty() {
            // Affiliate columns are handled separately (pass-through).
            continue;
        }

        let msg_index = messages
            .iter()
            .position(|m| m.source() == source_name.as_ref())
            .ok_or_else(|| {
                ProcessorError::ProcessingError(format!(
                    "column filter: source `{source_name}` not found in tuple"
                ))
            })?;
        let msg = &messages[msg_index];

        let mut columns = Vec::with_capacity(group_specs.len());
        for spec in group_specs {
            let key_index = msg
                .entries()
                .position(|(key, _)| key == spec.column_name.as_ref())
                .ok_or_else(|| {
                    ProcessorError::ProcessingError(format!(
                        "column filter: column `{}` not found in source `{source_name}`",
                        spec.column_name
                    ))
                })?;
            columns.push(ResolvedKeep {
                msg_index,
                key_index,
                output_key: Arc::clone(&spec.output_name),
            });
        }
        resolved_messages.push(ResolvedMessage {
            source_name,
            columns,
        });
    }

    Ok(resolved_messages)
}

fn apply_column_filter(
    collection: Box<dyn Collection>,
    resolved: &mut Option<Vec<ResolvedMessage>>,
    specs: &[crate::planner::physical::ColumnFilterKeepSpec],
) -> Result<Box<dyn Collection>, ProcessorError> {
    let rows = collection
        .into_rows()
        .map_err(|e| ProcessorError::ProcessingError(format!("failed to materialize rows: {e}")))?;
    let mut output_rows = Vec::with_capacity(rows.len());

    for tuple in rows {
        // Lazily resolve on first tuple.
        if resolved.is_none() {
            *resolved = Some(resolve_keep_specs(&tuple, specs)?);
        }
        let filtered = build_filtered_tuple(
            &tuple,
            resolved.as_ref().expect("resolved should be set above"),
        );
        output_rows.push(filtered);
    }

    let output = crate::model::RecordBatch::new(output_rows).map_err(|e| {
        ProcessorError::ProcessingError(format!("failed to build filtered output: {e}"))
    })?;
    Ok(Box::new(output))
}

fn build_filtered_tuple(tuple: &Tuple, resolved: &[ResolvedMessage]) -> Tuple {
    let mut messages: Vec<Arc<Message>> = Vec::with_capacity(resolved.len());

    for rm in resolved {
        let mut keys = Vec::with_capacity(rm.columns.len());
        let mut values = Vec::with_capacity(rm.columns.len());
        for col in &rm.columns {
            let msg = &tuple.messages()[col.msg_index];
            keys.push(Arc::clone(&col.output_key));
            let value = msg
                .entry_by_index(col.key_index)
                .map(|(_, v)| Arc::clone(v))
                .unwrap_or_else(|| Arc::new(Value::Null));
            values.push(value);
        }
        messages.push(Arc::new(Message::new_shared_keys(
            Arc::clone(&rm.source_name),
            Arc::from(keys),
            values,
        )));
    }

    let mut output = Tuple::with_timestamp(messages.into(), tuple.timestamp);

    // Copy affiliate columns (pass-through).
    if let Some(affiliate) = tuple.affiliate() {
        for (key, value) in affiliate.entries() {
            output.add_affiliate_column(Arc::new(key.as_ref().to_string()), value.clone());
        }
    }

    // Carry over output mask if present.
    if let Some(mask) = tuple.output_mask_shared() {
        output.set_output_mask_shared(mask);
    }

    output
}

impl Processor for ColumnFilterProcessor {
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
        let specs = self.keep_specs.clone();
        let mut resolved: Option<Vec<ResolvedMessage>> = self.resolved.take();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);

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
                                        match apply_column_filter(
                                            collection,
                                            &mut resolved,
                                            &specs,
                                        ) {
                                            Ok(out_collection) => {
                                                let out_data =
                                                    StreamData::collection(out_collection);
                                                let out_rows = out_data.num_rows_hint();
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    out_data,
                                                    Some(stats.as_ref()),
                                                )
                                                .await;
                                                stats.record_handle_duration(
                                                    handle_start.elapsed(),
                                                );
                                                send_res?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_out(rows);
                                                }
                                            }
                                            Err(err) => {
                                                stats.record_handle_duration(
                                                    handle_start.elapsed(),
                                                );
                                                stats.record_error_logged(
                                                    "column filter error",
                                                    err.to_string(),
                                                );
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
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "column filter data input");
                                continue;
                            }
                            None => {
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
