//! MemoryCollectionMaterializeProcessor - reshapes tuples for direct collection sinks.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalMemoryCollectionMaterialize, PhysicalPlan};
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
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct MemoryCollectionMaterializeProcessor {
    id: String,
    output_source_name: Arc<str>,
    row_accessor: OutputRowAccessor,
    keys: Arc<[Arc<str>]>,

    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl MemoryCollectionMaterializeProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalMemoryCollectionMaterialize>) -> Self {
        Self::new_with_channel_capacities(id, spec, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        spec: Arc<PhysicalMemoryCollectionMaterialize>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );

        let row_accessor = OutputRowAccessor::from_output_layout(&spec.output_layout);
        let keys = row_accessor.column_names();

        Self {
            id: id.into(),
            output_source_name: Arc::<str>::from(""),
            row_accessor,
            keys,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::MemoryCollectionMaterialize(spec) => {
                Some(Self::new(id, Arc::new(spec.clone())))
            }
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

fn materialize_collection(
    output_source_name: &Arc<str>,
    shared_keys: &Arc<[Arc<str>]>,
    row_accessor: &mut OutputRowAccessor,
    input: &dyn Collection,
    stats: Option<&ProcessorStats>,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let mut rows = Vec::with_capacity(input.num_rows());
    for tuple in input.rows() {
        let extracted = match row_accessor.extract_row(tuple) {
            Ok(extracted) => extracted,
            Err(error) => {
                if let Some(stats) = stats {
                    stats.record_error_logged(
                        "memory collection materialize processor error",
                        error.to_string(),
                    );
                }
                continue;
            }
        };
        let values = extracted.into_values_with_null_fill();

        let msg = Arc::new(Message::new_shared_keys(
            Arc::clone(output_source_name),
            Arc::clone(shared_keys),
            values,
        ));
        let mut output_tuple = Tuple::with_timestamp(Arc::from(vec![msg]), tuple.timestamp);
        if let Some(mask) = tuple.output_mask_shared() {
            output_tuple.set_output_mask_shared(mask);
        }
        rows.push(output_tuple);
    }

    let batch = RecordBatch::new_with_metadata_from(rows, input)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid record batch: {err}")))?;
    Ok(Box::new(batch))
}

impl Processor for MemoryCollectionMaterializeProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let data_receivers = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let output_source_name = Arc::clone(&self.output_source_name);
        let shared_keys = Arc::clone(&self.keys);
        let mut row_accessor = self.row_accessor.clone();
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "memory collection materialize starting");

        ProcessorStart::ready(spawner.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
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
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "memory materialize control input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
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
                                        match materialize_collection(
                                            &output_source_name,
                                            &shared_keys,
                                            &mut row_accessor,
                                            collection.as_ref(),
                                            Some(stats.as_ref()),
                                        ) {
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
                                                stats.record_error(e.to_string());
                                            }
                                        }
                                    }
                                    StreamData::Control(control_signal) => {
                                        let is_terminal = control_signal.is_terminal();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            StreamData::control(control_signal),
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            other,
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
                                log_broadcast_lagged(&id, skipped, "memory materialize data input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
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
    use crate::planner::physical::output_layout::{
        OutputColumnLayout, OutputLayout, OutputValueRef,
    };
    use datatypes::{ConcreteDatatype, Int64Type, Value};
    use std::time::{Duration, SystemTime};

    fn build_message(source: &str, keys: &[&str], values: Vec<Value>) -> Arc<Message> {
        let shared_keys: Arc<[Arc<str>]> = Arc::from(
            keys.iter()
                .map(|key| Arc::<str>::from(*key))
                .collect::<Vec<_>>(),
        );
        let shared_values = values.into_iter().map(Arc::new).collect();
        Arc::new(Message::new_shared_keys(source, shared_keys, shared_values))
    }

    #[test]
    fn materialize_collection_fills_planned_null_columns_and_preserves_mask() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(456);
        let mut tuple = Tuple::with_timestamp(
            Arc::from(vec![build_message("stream", &["a"], vec![Value::Int64(7)])]),
            timestamp,
        );
        tuple.add_affiliate_column(Arc::new("tmp".to_string()), Value::Int64(999));
        tuple.set_output_mask(vec![true, false, true]);

        let input = RecordBatch::new(vec![tuple]).expect("build input collection");
        let output_layout = OutputLayout::new(vec![
            OutputColumnLayout {
                name: Arc::from("a"),
                data_type: ConcreteDatatype::Int64(Int64Type),
                value_ref: OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            },
            OutputColumnLayout {
                name: Arc::from("missing_msg"),
                data_type: ConcreteDatatype::Int64(Int64Type),
                value_ref: OutputValueRef::Null,
            },
            OutputColumnLayout {
                name: Arc::from("missing_aff"),
                data_type: ConcreteDatatype::Int64(Int64Type),
                value_ref: OutputValueRef::Null,
            },
        ]);
        let mut row_accessor = OutputRowAccessor::from_output_layout(&output_layout);
        let shared_keys = row_accessor.column_names();

        let materialized = materialize_collection(
            &Arc::<str>::from(""),
            &shared_keys,
            &mut row_accessor,
            &input,
            None,
        )
        .expect("materialize collection");

        assert_eq!(
            materialized.num_rows(),
            1,
            "materialized row count mismatch"
        );

        let output_tuple = &materialized.rows()[0];
        assert!(
            output_tuple.affiliate().is_none(),
            "materialize should clear affiliate",
        );
        assert_eq!(
            output_tuple.output_mask(),
            Some([true, false, true].as_slice()),
            "output mask should be preserved",
        );
        assert_eq!(
            output_tuple.timestamp, timestamp,
            "timestamp should be preserved",
        );
        assert_eq!(
            output_tuple.messages().len(),
            1,
            "materialize should emit one message",
        );

        let msg = &output_tuple.messages()[0];
        assert_eq!(msg.source(), "", "message source should be empty");
        let entries: Vec<(&str, Value)> = msg
            .entries()
            .map(|(key, value)| (key, value.clone()))
            .collect();
        assert_eq!(
            entries,
            vec![
                ("a", Value::Int64(7)),
                ("missing_msg", Value::Null),
                ("missing_aff", Value::Null),
            ],
            "materialized values mismatch",
        );
    }
}
