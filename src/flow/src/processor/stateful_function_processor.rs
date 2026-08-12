//! StatefulFunctionProcessor - evaluates stateful scalar functions per row.

use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{
    PartitionGroupKey, PhysicalPlan, PhysicalStatefulFunction, StatefulCall,
};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use crate::stateful::{
    StatefulEvalInput, StatefulEvalUpdate, StatefulFunction, StatefulFunctionInstance,
    StatefulFunctionRegistry,
};
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

struct StatefulPartitionState {
    instance: Box<dyn StatefulFunctionInstance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PartitionKey {
    Global,
    Values(Vec<Value>),
}

struct StatefulProcessorCall {
    output_column: Arc<String>,
    function: Arc<dyn StatefulFunction>,
    arg_scalars: Vec<ScalarExpr>,
    when_scalar: Option<ScalarExpr>,
    partition_group_id: usize,
    states: HashMap<PartitionKey, StatefulPartitionState>,
}

struct PartitionGroup {
    scalars: Vec<ScalarExpr>,
}

pub struct StatefulFunctionProcessor {
    id: String,
    physical_stateful: Arc<PhysicalStatefulFunction>,
    partition_groups: Vec<PartitionGroup>,
    calls: Vec<StatefulProcessorCall>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl StatefulFunctionProcessor {
    pub fn new(
        id: impl Into<String>,
        physical_stateful: Arc<PhysicalStatefulFunction>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(
            id,
            physical_stateful,
            stateful_registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_stateful: Arc<PhysicalStatefulFunction>,
        stateful_registry: Arc<StatefulFunctionRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        let id = id.into();
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );

        let mut partition_group_ids: HashMap<PartitionGroupKey, usize> = HashMap::new();
        let mut partition_groups = vec![PartitionGroup {
            scalars: Vec::new(),
        }];
        let mut calls = Vec::with_capacity(physical_stateful.calls.len());
        for StatefulCall {
            output_column,
            func_name,
            arg_scalars,
            when_scalar,
            partition_group_key,
            partition_by_scalars,
            ..
        } in &physical_stateful.calls
        {
            let function = stateful_registry.get(func_name).ok_or_else(|| {
                ProcessorError::InvalidConfiguration(format!(
                    "unknown stateful function '{}'",
                    func_name
                ))
            })?;
            let partition_group_id = if partition_by_scalars.is_empty() {
                0
            } else if let Some(existing) = partition_group_ids.get(partition_group_key) {
                *existing
            } else {
                let id = partition_groups.len();
                partition_groups.push(PartitionGroup {
                    scalars: partition_by_scalars.clone(),
                });
                partition_group_ids.insert(partition_group_key.clone(), id);
                id
            };
            calls.push(StatefulProcessorCall {
                output_column: Arc::new(output_column.clone()),
                function,
                arg_scalars: arg_scalars.clone(),
                when_scalar: when_scalar.clone(),
                partition_group_id,
                states: HashMap::new(),
            });
        }
        Ok(Self {
            id,
            physical_stateful,
            partition_groups,
            calls,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
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
        partition_groups: &[PartitionGroup],
        calls: &mut [StatefulProcessorCall],
        stats: Option<&ProcessorStats>,
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        let metadata = collection.metadata().clone();
        let rows = collection.into_rows().map_err(|e| {
            ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
        })?;
        let mut out_rows = Vec::with_capacity(rows.len());

        'rows: for mut tuple in rows {
            let mut partition_keys = Vec::with_capacity(partition_groups.len());
            for group in partition_groups {
                let key = if group.scalars.is_empty() {
                    PartitionKey::Global
                } else {
                    let mut key_values = Vec::with_capacity(group.scalars.len());
                    for scalar in &group.scalars {
                        let value = match scalar.eval_with_tuple(&tuple) {
                            Ok(value) => value,
                            Err(error) => {
                                if let Some(stats) = stats {
                                    stats.record_error_logged(
                                        "stateful function processor error",
                                        format!(
                                            "failed to evaluate stateful function partition key: {error}"
                                        ),
                                    );
                                }
                                continue 'rows;
                            }
                        };
                        key_values.push(value);
                    }
                    PartitionKey::Values(key_values)
                };
                partition_keys.push(key);
            }

            let mut prepared_updates: Vec<(usize, PartitionKey, Box<dyn StatefulEvalUpdate>)> =
                Vec::with_capacity(calls.len());
            for (idx, call) in calls.iter().enumerate() {
                let partition_key = &partition_keys[call.partition_group_id];
                let should_apply = match call.when_scalar.as_ref() {
                    Some(scalar) => match scalar.eval_with_tuple(&tuple) {
                        Ok(Value::Bool(v)) => v,
                        Ok(Value::Null) => false,
                        Ok(other) => {
                            if let Some(stats) = stats {
                                stats.record_error_logged(
                                    "stateful function processor error",
                                    format!("stateful function filter must be bool, got {other:?}"),
                                );
                            }
                            continue 'rows;
                        }
                        Err(error) => {
                            if let Some(stats) = stats {
                                stats.record_error_logged(
                                    "stateful function processor error",
                                    format!("failed to evaluate stateful function filter: {error}"),
                                );
                            }
                            continue 'rows;
                        }
                    },
                    None => true,
                };

                let mut args = Vec::with_capacity(call.arg_scalars.len());
                for scalar in &call.arg_scalars {
                    let value = match scalar.eval_with_tuple(&tuple) {
                        Ok(value) => value,
                        Err(error) => {
                            if let Some(stats) = stats {
                                stats.record_error_logged(
                                    "stateful function processor error",
                                    error.to_string(),
                                );
                            }
                            continue 'rows;
                        }
                    };
                    args.push(value);
                }

                let prepared_result = if let Some(state) = call.states.get(partition_key) {
                    state.instance.prepare_eval(StatefulEvalInput {
                        args: &args,
                        should_apply,
                    })
                } else {
                    let instance = call.function.create_instance();
                    instance.prepare_eval(StatefulEvalInput {
                        args: &args,
                        should_apply,
                    })
                };
                let prepared = match prepared_result {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        if let Some(stats) = stats {
                            stats.record_error_logged(
                                "stateful function processor error",
                                format!("failed to evaluate stateful function: {error}"),
                            );
                        }
                        continue 'rows;
                    }
                };
                tuple.add_affiliate_column(Arc::clone(&call.output_column), prepared.output);
                prepared_updates.push((idx, partition_key.clone(), prepared.update));
            }

            for (idx, partition_key, update) in prepared_updates {
                let call = &mut calls[idx];
                let state =
                    call.states
                        .entry(partition_key)
                        .or_insert_with(|| StatefulPartitionState {
                            instance: call.function.create_instance(),
                        });
                state.instance.commit_eval(update);
            }

            out_rows.push(tuple);
        }

        let batch = RecordBatch::new_with_metadata(out_rows, metadata)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        Ok(Box::new(batch))
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

impl Processor for StatefulFunctionProcessor {
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
        let channel_capacities = self.channel_capacities;
        let partition_groups = std::mem::take(&mut self.partition_groups);
        let mut calls = std::mem::take(&mut self.calls);
        let _physical_stateful = Arc::clone(&self.physical_stateful);
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "stateful function processor starting");
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
                                        match Self::apply_stateful(collection, &partition_groups, &mut calls, Some(stats.as_ref())) {
                                            Ok(out_collection) => {
                                                let out = StreamData::collection(out_collection);
                                                let out_rows = out.num_rows_hint();
                                                let send_res = send_with_backpressure(
                                                    &output,
                                                    channel_capacities.data,
                                                    out,
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
                                                stats.record_error_logged("stateful function processor error", e.to_string());
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
                                log_broadcast_lagged(
                                    &id,
                                    skipped,
                                    "stateful function data input",
                                );
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
