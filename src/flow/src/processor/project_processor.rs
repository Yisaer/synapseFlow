//! ProjectProcessor - processes projection operations
//!
//! This processor evaluates projection expressions and produces output with projected fields.

use crate::expr::{EvalRowContext, ScalarExpr};
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{
    PhysicalPlan, PhysicalProject, PhysicalProjectField, PipelineStateUsage,
};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::pipeline_state_runtime::update_row_hit_state;
use crate::processor::processor_state::ProcessorState;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// ProjectProcessor - evaluates projection expressions
///
/// This processor:
/// - Takes input data (Collection) and projection expressions
/// - Evaluates the expressions to create projected fields
/// - Sends the projected data downstream as StreamData::Collection
pub struct ProjectProcessor {
    /// Processor identifier
    id: String,
    fields: Arc<[PhysicalProjectField]>,
    /// Processor-local state for pipeline state functions (e.g. last_hit_count).
    pub(crate) processor_state: Option<Arc<ProcessorState>>,
    pub(crate) pipeline_state_usage: PipelineStateUsage,
    /// Input channels for receiving data
    inputs: Vec<LinkReceiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    /// Broadcast channel for downstream processors
    output: LinkOutput<StreamData>,
    /// Dedicated control output channel
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor from PhysicalProject
    pub fn new(id: impl Into<String>, physical_project: Arc<PhysicalProject>) -> Self {
        Self::new_with_channel_capacities(id, physical_project, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_project: Arc<PhysicalProject>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            fields: Arc::clone(&physical_project.fields),
            processor_state: physical_project.processor_state.clone(),
            pipeline_state_usage: physical_project.pipeline_state_usage,
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

    /// Create a ProjectProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalProject
    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Project(project) => Some(Self::new(id, Arc::new(project.clone()))),
            _ => None,
        }
    }
}

/// Apply projection to a collection
fn apply_projection(
    input_collection: &dyn Collection,
    fields: &[PhysicalProjectField],
    state: Option<&ProcessorState>,
    state_usage: PipelineStateUsage,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let mut projected_rows = Vec::with_capacity(input_collection.num_rows());
    'rows: for tuple in input_collection.rows() {
        let mut projected_tuple =
            Tuple::with_timestamp(Arc::clone(&tuple.messages), tuple.timestamp);
        for field in fields {
            if matches!(
                field.compiled_expr,
                ScalarExpr::Column(crate::expr::scalar::ColumnRef::ByIndex { .. })
                    | ScalarExpr::Wildcard { .. }
            ) {
                continue;
            }
            let context = EvalRowContext {
                tuple,
                collection_metadata: input_collection.metadata(),
            };
            let value = match field.compiled_expr.eval_with_context(&context) {
                Ok(value) => value,
                Err(error) => {
                    if let Some(stats) = stats {
                        stats.record_error_logged("project processor error", error.to_string());
                    }
                    continue 'rows;
                }
            };
            projected_tuple
                .add_affiliate_column(Arc::new(field.field_name.as_ref().to_string()), value);
        }
        if let Some(state) = state {
            if let Err(error) = update_row_hit_state(state, state_usage, tuple.timestamp) {
                if let Some(stats) = stats {
                    stats.record_error_logged("project processor error", error.to_string());
                }
                continue;
            }
        }
        projected_rows.push(projected_tuple);
    }

    RecordBatch::new_with_metadata_from(projected_rows, input_collection)
        .map(|batch| Box::new(batch) as Box<dyn Collection>)
        .map_err(|error| ProcessorError::ProcessingError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::ProcStateField;
    use crate::model::Message;
    use datatypes::Value;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn empty_tuple_at_ms(ms: u64) -> Tuple {
        Tuple::with_timestamp(
            Tuple::empty_messages(),
            UNIX_EPOCH + Duration::from_millis(ms),
        )
    }

    #[test]
    fn empty_projection_drops_upstream_affiliate() {
        let keys = vec![Arc::<str>::from("a")];
        let values = vec![Arc::new(Value::Int64(1))];
        let message = Arc::new(Message::new(Arc::<str>::from("stream"), keys, values));
        let timestamp = SystemTime::now();
        let mut input_tuple = Tuple::with_timestamp(Arc::from(vec![message]), timestamp);
        input_tuple.add_affiliate_column(Arc::new("tmp".to_string()), Value::Int64(999));
        assert!(input_tuple.affiliate().is_some(), "precondition");

        let input = RecordBatch::new(vec![input_tuple]).expect("record batch");
        let output = apply_projection(&input, &[], None, PipelineStateUsage::default(), None)
            .expect("projection succeeds");
        let out_rows = output.rows();
        assert_eq!(out_rows.len(), 1);
        assert!(
            out_rows[0].affiliate().is_none(),
            "upstream affiliate should not leak in passthrough mode"
        );
        assert_eq!(out_rows[0].messages().len(), 1);
        assert_eq!(out_rows[0].timestamp, timestamp);
    }

    #[test]
    fn projection_updates_last_hit_time_after_projected_row() {
        let state = Arc::new(ProcessorState::new());
        let input = RecordBatch::new(vec![empty_tuple_at_ms(1000), empty_tuple_at_ms(2000)])
            .expect("record batch");
        let field_name = "last_hit_time_unix_ms()";
        let fields = vec![PhysicalProjectField::new(
            field_name,
            sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(field_name)),
            ScalarExpr::ProcessorState {
                state: Arc::clone(&state),
                field: ProcStateField::LastHitTimeUnixMs,
            },
        )];
        let usage = PipelineStateUsage {
            last_hit_time_unix_ms: true,
            ..PipelineStateUsage::default()
        };

        let output = apply_projection(&input, &fields, Some(state.as_ref()), usage, None)
            .expect("projection should succeed");

        let rows = output.rows();
        assert_eq!(
            rows[0].affiliate().and_then(|row| row.value(field_name)),
            Some(&Value::Int64(0))
        );
        assert_eq!(
            rows[1].affiliate().and_then(|row| row.value(field_name)),
            Some(&Value::Int64(1000))
        );
        assert_eq!(state.last_hit_time_unix_ms.load(Ordering::Relaxed), 2000);
    }
}

impl Processor for ProjectProcessor {
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
        let fields = Arc::clone(&self.fields);
        let processor_state = self.processor_state.clone();
        let pipeline_state_usage = self.pipeline_state_usage;
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "project processor starting");

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
                                        let result = apply_projection(
                                            collection.as_ref(),
                                            fields.as_ref(),
                                            processor_state.as_deref(),
                                            pipeline_state_usage,
                                            Some(stats.as_ref()),
                                        );
                                        match result {
                                            Ok(projected_collection) => {
                                                let projected_data =
                                                    StreamData::collection(projected_collection);
                                                let out_rows = projected_data.num_rows_hint();
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    projected_data,
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
                                                stats.record_error_logged("project processor error", e.to_string());
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
                                log_broadcast_lagged(&id, skipped, "project data input");
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
