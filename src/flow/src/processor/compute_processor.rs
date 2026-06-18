//! ComputeProcessor - materializes derived expressions into affiliate columns.
//!
//! Unlike Project, Compute preserves upstream messages and appends/overwrites derived columns
//! in the tuple affiliate row.

use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{PhysicalCompute, PhysicalPlan};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Clone)]
struct ComputeExpr {
    output_name: Arc<String>,
    field_name: String,
    expr: ScalarExpr,
}

/// ComputeProcessor - evaluates expressions and appends them as affiliate columns.
pub struct ComputeProcessor {
    id: String,
    fields: Vec<ComputeExpr>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl ComputeProcessor {
    pub fn new(id: impl Into<String>, physical_compute: Arc<PhysicalCompute>) -> Self {
        Self::new_with_channel_capacities(id, physical_compute, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_compute: Arc<PhysicalCompute>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        let fields = physical_compute
            .fields
            .iter()
            .map(|field| ComputeExpr {
                output_name: Arc::new(field.field_name.clone()),
                field_name: field.field_name.clone(),
                expr: field.compiled_expr.clone(),
            })
            .collect();
        Self {
            id: id.into(),
            fields,
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

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Compute(compute) => Some(Self::new(id, Arc::new(compute.clone()))),
            _ => None,
        }
    }
}

fn apply_compute(
    input_collection: &dyn Collection,
    fields: &[ComputeExpr],
) -> Result<Box<dyn Collection>, ProcessorError> {
    if fields.is_empty() {
        return Ok(input_collection.clone_box());
    }

    let mut out_rows = Vec::with_capacity(input_collection.num_rows());
    for tuple in input_collection.rows() {
        let mut out_tuple = tuple.clone();
        for field in fields {
            // Evaluate against the current tuple so compute fields can reference
            // earlier computed fields in the same node.
            let value = field
                .expr
                .eval_with_tuple(&out_tuple)
                .map_err(|eval_error| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to evaluate expression for field '{}': {}",
                        field.field_name, eval_error
                    ))
                })?;
            out_tuple.add_affiliate_column(Arc::clone(&field.output_name), value);
        }
        out_rows.push(out_tuple);
    }

    let out_batch = RecordBatch::new(out_rows).map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to build compute output: {err}"))
    })?;
    Ok(Box::new(out_batch))
}

impl Processor for ComputeProcessor {
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
        let fields = self.fields.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "compute processor starting");

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
                                        match apply_compute(collection.as_ref(), &fields) {
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
                                                // For synchronous processors, handle duration includes downstream send/backpressure time.
                                                stats.record_handle_duration(handle_start.elapsed());
                                                send_res?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_out(rows);
                                                }
                                            }
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged("compute processor error", e.to_string());
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
                                log_broadcast_lagged(&id, skipped, "compute data input");
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
