//! StatefulFunctionProcessor - evaluates stateful scalar functions per row.

use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{PhysicalPlan, PhysicalStatefulFunction, StatefulCall};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData, StreamError};
use crate::stateful::{StatefulFunctionInstance, StatefulFunctionRegistry};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

struct StatefulProcessorCall {
    output_column: Arc<String>,
    arg_scalars: Vec<crate::expr::ScalarExpr>,
    instance: Box<dyn StatefulFunctionInstance>,
}

pub struct StatefulFunctionProcessor {
    id: String,
    physical_stateful: Arc<PhysicalStatefulFunction>,
    calls: Vec<StatefulProcessorCall>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl StatefulFunctionProcessor {
    pub fn new(
        id: impl Into<String>,
        physical_stateful: Arc<PhysicalStatefulFunction>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        let id = id.into();
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);

        let mut calls = Vec::with_capacity(physical_stateful.calls.len());
        for StatefulCall {
            output_column,
            func_name,
            arg_scalars,
            ..
        } in &physical_stateful.calls
        {
            let function = stateful_registry.get(func_name).ok_or_else(|| {
                ProcessorError::InvalidConfiguration(format!(
                    "unknown stateful function '{}'",
                    func_name
                ))
            })?;
            calls.push(StatefulProcessorCall {
                output_column: Arc::new(output_column.clone()),
                arg_scalars: arg_scalars.clone(),
                instance: function.create_instance(),
            });
        }

        Ok(Self {
            id,
            physical_stateful,
            calls,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        })
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
    ) -> Result<Option<Self>, ProcessorError> {
        match plan.as_ref() {
            PhysicalPlan::StatefulFunction(stateful) => Ok(Some(Self::new(
                id,
                Arc::new(stateful.clone()),
                stateful_registry,
            )?)),
            _ => Ok(None),
        }
    }

    fn apply_stateful(
        collection: Box<dyn Collection>,
        calls: &mut [StatefulProcessorCall],
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        let mut rows = collection.into_rows().map_err(|e| {
            ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
        })?;

        for tuple in rows.iter_mut() {
            for call in calls.iter_mut() {
                let mut args = Vec::with_capacity(call.arg_scalars.len());
                for scalar in &call.arg_scalars {
                    args.push(
                        scalar
                            .eval_with_tuple(tuple)
                            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?,
                    );
                }
                let out = call
                    .instance
                    .eval(&args)
                    .map_err(ProcessorError::ProcessingError)?;
                tuple.add_affiliate_column(Arc::clone(&call.output_column), out);
            }
        }

        let batch =
            RecordBatch::new(rows).map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        Ok(Box::new(batch))
    }
}

impl Processor for StatefulFunctionProcessor {
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
        let mut calls = std::mem::take(&mut self.calls);
        let _physical_stateful = Arc::clone(&self.physical_stateful);

        tracing::info!(processor_id = %id, "stateful function processor starting");
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
                            Some(Ok(StreamData::Collection(collection))) => {
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                match Self::apply_stateful(collection, &mut calls) {
                                    Ok(out_collection) => {
                                        send_with_backpressure(&output, StreamData::collection(out_collection)).await?;
                                    }
                                    Err(e) => {
                                        let error = StreamError::new(e.to_string()).with_source(id.clone());
                                        send_with_backpressure(&output, StreamData::error(error)).await?;
                                    }
                                }
                            }
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "StatefulFunctionProcessor input lagged by {} messages",
                                    skipped
                                );
                                tracing::warn!(processor_id = %id, skipped = skipped, "input lagged");
                                forward_error(&output, &id, message).await?;
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
