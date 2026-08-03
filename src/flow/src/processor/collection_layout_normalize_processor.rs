//! CollectionLayoutNormalizeProcessor - normalizes tuple layout for collection sources.

use crate::model::{Collection, Message, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalCollectionLayoutNormalize, PhysicalPlan};
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
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct CollectionLayoutNormalizeProcessor {
    id: String,
    output_source_name: Arc<str>,
    keys: Arc<[Arc<str>]>,

    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl CollectionLayoutNormalizeProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalCollectionLayoutNormalize>) -> Self {
        Self::new_with_channel_capacities(id, spec, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        spec: Arc<PhysicalCollectionLayoutNormalize>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );

        let schema = spec.schema();
        let keys: Vec<Arc<str>> = schema
            .column_schemas()
            .iter()
            .map(|col| Arc::<str>::from(col.name.as_str()))
            .collect();

        Self {
            id: id.into(),
            output_source_name: Arc::<str>::from(spec.output_source_name()),
            keys: Arc::from(keys),
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
            PhysicalPlan::CollectionLayoutNormalize(spec) => {
                Some(Self::new(id, Arc::new(spec.clone())))
            }
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }
}

fn value_by_name_with_index(msg: &Message, expected: &str) -> Option<(usize, Arc<Value>)> {
    let mut idx = 0;
    while let Some((key, value)) = msg.entry_by_index(idx) {
        if key.as_ref() == expected {
            return Some((idx, Arc::clone(value)));
        }
        idx += 1;
    }
    None
}

fn normalize_collection(
    schema_keys: &[Arc<str>],
    output_source_name: &Arc<str>,
    shared_keys: &Arc<[Arc<str>]>,
    input: &dyn Collection,
    processor_id: &str,
) -> Result<Box<dyn Collection>, ProcessorError> {
    if input.is_empty() {
        let batch = RecordBatch::new_with_metadata_from(Vec::new(), input).map_err(|err| {
            ProcessorError::ProcessingError(format!("invalid record batch: {err}"))
        })?;
        return Ok(Box::new(batch));
    }

    let mut key_indices = vec![None; schema_keys.len()];
    let mut saw_non_single_message = false;
    let mut saw_empty_messages = false;

    let mut schema_pos = HashMap::<&str, usize>::with_capacity(schema_keys.len());
    for (idx, key) in schema_keys.iter().enumerate() {
        schema_pos.insert(key.as_ref(), idx);
    }

    if let Some(sample) = input.rows().first() {
        if sample.messages().len() != 1 {
            saw_non_single_message = true;
        }
        if let Some(msg) = sample.messages().first() {
            for (idx, (key, _)) in msg.entries().enumerate() {
                if let Some(pos) = schema_pos.get(key).copied() {
                    key_indices[pos] = Some(idx);
                }
            }
        } else {
            saw_empty_messages = true;
        }
    }

    let mut missing = BTreeSet::<String>::new();
    let mut rows = Vec::with_capacity(input.num_rows());
    let null = Arc::new(Value::Null);
    for tuple in input.rows() {
        if tuple.messages().len() != 1 {
            saw_non_single_message = true;
        }
        let input_msg = tuple.messages().first();
        if input_msg.is_none() {
            saw_empty_messages = true;
        }

        let mut values = Vec::with_capacity(schema_keys.len());
        for (idx, col_name) in schema_keys.iter().enumerate() {
            let expected = col_name.as_ref();

            let mut value = input_msg.and_then(|msg| {
                if let Some(key_idx) = key_indices.get(idx).and_then(|v| *v) {
                    msg.entry_by_index(key_idx).and_then(|(key, value)| {
                        if key.as_ref() == expected {
                            Some(Arc::clone(value))
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            });

            if value.is_none() {
                if let Some(msg) = input_msg {
                    if let Some((found_idx, found)) = value_by_name_with_index(msg, expected) {
                        value = Some(found);
                        if let Some(slot) = key_indices.get_mut(idx) {
                            *slot = Some(found_idx);
                        }
                    }
                }
            }

            if value.is_none() {
                missing.insert(expected.to_string());
            }
            values.push(value.unwrap_or_else(|| Arc::clone(&null)));
        }

        let msg = Arc::new(Message::new_shared_keys(
            Arc::clone(output_source_name),
            Arc::clone(shared_keys),
            values,
        ));
        rows.push(Tuple::with_timestamp(Arc::from(vec![msg]), tuple.timestamp));
    }

    if !missing.is_empty() {
        tracing::warn!(
            processor_id = %processor_id,
            missing_columns = ?missing,
            "collection layout normalize filled NULL for missing columns"
        );
    }
    if saw_non_single_message {
        tracing::warn!(
            processor_id = %processor_id,
            "collection layout normalize expected single-message tuples; extra messages are ignored"
        );
    }
    if saw_empty_messages {
        tracing::warn!(
            processor_id = %processor_id,
            "collection layout normalize received tuples without messages"
        );
    }

    let batch = RecordBatch::new_with_metadata_from(rows, input)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid record batch: {err}")))?;
    Ok(Box::new(batch))
}

impl Processor for CollectionLayoutNormalizeProcessor {
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
        let schema_keys: Vec<Arc<str>> = self.keys.iter().cloned().collect();
        let output_source_name = Arc::clone(&self.output_source_name);
        let shared_keys = Arc::clone(&self.keys);
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "collection layout normalize starting");

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
                                log_broadcast_lagged(&id, skipped, "collection normalize control input");
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
                                        match normalize_collection(
                                            &schema_keys,
                                            &output_source_name,
                                            &shared_keys,
                                            collection.as_ref(),
                                            &id,
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
                                        let out = StreamData::control(control_signal);
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            out,
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
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            other,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "collection normalize data input");
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
    fn normalize_collection_rewrites_source_and_stabilizes_schema_order() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(123);
        let input = RecordBatch::new(vec![
            Tuple::with_timestamp(
                Arc::from(vec![build_message(
                    "topic_binding",
                    &["b", "a"],
                    vec![Value::Int64(2), Value::Int64(1)],
                )]),
                timestamp,
            ),
            Tuple::with_timestamp(
                Arc::from(vec![build_message(
                    "topic_binding",
                    &["a"],
                    vec![Value::Int64(100)],
                )]),
                timestamp,
            ),
        ])
        .expect("build input collection");

        let schema_keys = vec![Arc::<str>::from("a"), Arc::<str>::from("b")];
        let shared_keys: Arc<[Arc<str>]> = Arc::from(schema_keys.clone());
        let output_source_name = Arc::<str>::from("stream");

        let normalized = normalize_collection(
            &schema_keys,
            &output_source_name,
            &shared_keys,
            &input,
            "normalize_test",
        )
        .expect("normalize collection");

        assert_eq!(normalized.num_rows(), 2, "normalized row count mismatch");

        let first = &normalized.rows()[0];
        assert!(
            first.affiliate().is_none(),
            "normalize should clear affiliate"
        );
        assert_eq!(first.timestamp, timestamp, "timestamp should be preserved");
        assert_eq!(
            first.messages().len(),
            1,
            "normalize should keep one message"
        );
        let first_msg = &first.messages()[0];
        assert_eq!(
            first_msg.source(),
            "stream",
            "message source should be rewritten to stream binding",
        );
        let first_entries: Vec<(&str, Value)> = first_msg
            .entries()
            .map(|(key, value)| (key, value.clone()))
            .collect();
        assert_eq!(
            first_entries,
            vec![("a", Value::Int64(1)), ("b", Value::Int64(2))],
            "first row should be reordered to schema order",
        );

        let second = &normalized.rows()[1];
        assert!(
            second.affiliate().is_none(),
            "normalize should clear affiliate"
        );
        assert_eq!(second.timestamp, timestamp, "timestamp should be preserved");
        assert_eq!(
            second.messages().len(),
            1,
            "normalize should keep one message"
        );
        let second_msg = &second.messages()[0];
        assert_eq!(
            second_msg.source(),
            "stream",
            "message source should be rewritten to stream binding",
        );
        let second_entries: Vec<(&str, Value)> = second_msg
            .entries()
            .map(|(key, value)| (key, value.clone()))
            .collect();
        assert_eq!(
            second_entries,
            vec![("a", Value::Int64(100)), ("b", Value::Null)],
            "missing columns should be filled with null in schema order",
        );
    }
}
