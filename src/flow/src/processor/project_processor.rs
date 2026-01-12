//! ProjectProcessor - processes projection operations
//!
//! This processor evaluates projection expressions and produces output with projected fields.

use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalPlan, PhysicalProject, PhysicalProjectField};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Clone)]
enum ProjectExecMode {
    Normal { fields: Vec<PhysicalProjectField> },
    Passthrough { fields: Vec<AffiliateExpr> },
}

impl ProjectExecMode {
    fn new(physical_project: &PhysicalProject) -> Self {
        if physical_project.passthrough_messages {
            Self::Passthrough {
                fields: physical_project
                    .fields
                    .iter()
                    .map(|field| AffiliateExpr {
                        output_name: Arc::new(field.field_name.clone()),
                        field_name: field.field_name.clone(),
                        expr: field.compiled_expr.clone(),
                    })
                    .collect(),
            }
        } else {
            Self::Normal {
                fields: physical_project.fields.clone(),
            }
        }
    }

    fn apply(
        &self,
        collection: Box<dyn Collection>,
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        match self {
            Self::Normal { fields } => apply_projection(collection.as_ref(), fields),
            Self::Passthrough { fields } => {
                apply_passthrough_projection(collection.as_ref(), fields)
            }
        }
    }
}

/// ProjectProcessor - evaluates projection expressions
///
/// This processor:
/// - Takes input data (Collection) and projection expressions
/// - Evaluates the expressions to create projected fields
/// - Sends the projected data downstream as StreamData::Collection
pub struct ProjectProcessor {
    /// Processor identifier
    id: String,
    exec_mode: ProjectExecMode,
    /// Input channels for receiving data
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Broadcast channel for downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control output channel
    control_output: broadcast::Sender<ControlSignal>,
    stats: Arc<ProcessorStats>,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor from PhysicalProject
    pub fn new(id: impl Into<String>, physical_project: Arc<PhysicalProject>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let exec_mode = ProjectExecMode::new(physical_project.as_ref());
        Self {
            id: id.into(),
            exec_mode,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
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
) -> Result<Box<dyn Collection>, ProcessorError> {
    // Use the collection's apply_projection method
    input_collection
        .apply_projection(fields)
        .map_err(|e| ProcessorError::ProcessingError(format!("Failed to apply projection: {}", e)))
}

#[derive(Clone)]
struct AffiliateExpr {
    output_name: Arc<String>,
    field_name: String,
    expr: ScalarExpr,
}

fn apply_passthrough_projection(
    input_collection: &dyn Collection,
    fields: &[AffiliateExpr],
) -> Result<Box<dyn Collection>, ProcessorError> {
    let mut projected_rows = Vec::with_capacity(input_collection.num_rows());
    for tuple in input_collection.rows() {
        let mut projected_tuple =
            Tuple::with_timestamp(Arc::clone(&tuple.messages), tuple.timestamp);
        for field in fields {
            let value = field.expr.eval_with_tuple(tuple).map_err(|eval_error| {
                ProcessorError::ProcessingError(format!(
                    "Failed to evaluate expression for field '{}': {}",
                    field.field_name, eval_error
                ))
            })?;
            projected_tuple.add_affiliate_column(Arc::clone(&field.output_name), value);
        }
        projected_rows.push(projected_tuple);
    }

    let projected = RecordBatch::new(projected_rows).map_err(|err| {
        ProcessorError::ProcessingError(format!("Failed to apply passthrough projection: {err}"))
    })?;
    Ok(Box::new(projected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Message;
    use datatypes::Value;
    use std::time::SystemTime;

    #[test]
    fn passthrough_empty_projection_drops_upstream_affiliate() {
        let keys = vec![Arc::<str>::from("a")];
        let values = vec![Arc::new(Value::Int64(1))];
        let message = Arc::new(Message::new(Arc::<str>::from("stream"), keys, values));
        let timestamp = SystemTime::now();
        let mut input_tuple = Tuple::with_timestamp(Arc::from(vec![message]), timestamp);
        input_tuple.add_affiliate_column(Arc::new("tmp".to_string()), Value::Int64(999));
        assert!(input_tuple.affiliate().is_some(), "precondition");

        let input = RecordBatch::new(vec![input_tuple]).expect("record batch");
        let output =
            apply_passthrough_projection(&input, &[]).expect("passthrough projection succeeds");
        let out_rows = output.rows();
        assert_eq!(out_rows.len(), 1);
        assert!(
            out_rows[0].affiliate().is_none(),
            "upstream affiliate should not leak in passthrough mode"
        );
        assert_eq!(out_rows[0].messages().len(), 1);
        assert_eq!(out_rows[0].timestamp, timestamp);
    }
}

impl Processor for ProjectProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let exec_mode = self.exec_mode.clone();
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %id, "project processor starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
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
                                        match exec_mode.apply(collection) {
                                            Ok(projected_collection) => {
                                                let projected_data =
                                                    StreamData::collection(projected_collection);
                                                let out_rows = projected_data.num_rows_hint();
                                                send_with_backpressure(&output, projected_data)
                                                    .await?;
                                                if let Some(rows) = out_rows {
                                                    stats.record_out(rows);
                                                }
                                            }
                                            Err(e) => {
                                                stats.record_error(e.to_string());
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        send_with_backpressure(&output, data).await?;
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
