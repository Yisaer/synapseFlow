//! EmptySuppressProcessor - suppresses empty collections on sink branches.

use crate::model::Collection;
use crate::planner::physical::PhysicalEmptySuppress;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, MetricKind, MetricSpec, Processor, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct EmptySuppressProcessor {
    id: String,
    omit_if_empty: bool,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl EmptySuppressProcessor {
    pub fn new(id: impl Into<String>, spec: Arc<PhysicalEmptySuppress>) -> Self {
        Self::new_with_channel_capacities(id, spec, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        spec: Arc<PhysicalEmptySuppress>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let (output, _) = broadcast::channel(channel_capacities.data);
        let (control_output, _) = broadcast::channel(channel_capacities.control);
        Self {
            id: id.into(),
            omit_if_empty: spec.omit_if_empty,
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

impl Processor for EmptySuppressProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let channel_capacities = self.channel_capacities;
        let omit_if_empty = self.omit_if_empty;
        let stats = Arc::clone(&self.stats);
        let collections_in = stats.register_counter(MetricSpec {
            id: "empty_suppress.collections_in",
            flat_name: "collections_in",
            kind: MetricKind::Counter,
        });
        let collections_forwarded = stats.register_counter(MetricSpec {
            id: "empty_suppress.collections_forwarded",
            flat_name: "collections_forwarded",
            kind: MetricKind::Counter,
        });
        let collections_suppressed = stats.register_counter(MetricSpec {
            id: "empty_suppress.collections_suppressed",
            flat_name: "collections_suppressed",
            kind: MetricKind::Counter,
        });

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
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }

                                match data {
                                    StreamData::Collection(collection) => {
                                        collections_in.inc_by(1);
                                        if omit_if_empty
                                            && collection_is_effectively_empty(collection.as_ref())
                                        {
                                            collections_suppressed.inc_by(1);
                                            tracing::debug!(
                                                processor_id = %processor_id,
                                                "suppressed empty collection"
                                            );
                                            continue;
                                        }

                                        let out_rows = collection.num_rows() as u64;
                                        collections_forwarded.inc_by(1);
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            StreamData::collection(collection),
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        stats.record_out(out_rows);
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        let out_rows = data.num_rows_hint();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if let Some(rows) = out_rows {
                                            stats.record_out(rows);
                                        }
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %processor_id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &processor_id,
                                    skipped,
                                    "empty suppress data input",
                                );
                                continue;
                            }
                            None => {
                                tracing::info!(processor_id = %processor_id, "stopped");
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

fn collection_is_effectively_empty(collection: &dyn Collection) -> bool {
    if collection.num_rows() == 0 {
        return true;
    }

    collection.rows().iter().all(tuple_is_effectively_empty)
}

fn tuple_is_effectively_empty(tuple: &crate::model::Tuple) -> bool {
    match tuple.output_mask() {
        Some(mask) => mask.iter().all(|selected| !selected),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::collection_is_effectively_empty;
    use crate::model::{batch_from_columns_simple, Message, RecordBatch, Tuple};
    use datatypes::Value;
    use std::sync::Arc;

    #[test]
    fn zero_row_collection_is_empty() {
        let batch = RecordBatch::empty();
        assert!(collection_is_effectively_empty(&batch));
    }

    #[test]
    fn output_mask_only_makes_collection_empty() {
        let message = Arc::new(Message::new(
            Arc::<str>::from("stream"),
            vec![Arc::<str>::from("a")],
            vec![Arc::new(Value::Int64(1))],
        ));
        let mut tuple = Tuple::new(vec![message]);
        tuple.set_output_mask(vec![false]);

        let batch = RecordBatch::new(vec![tuple]).expect("batch");
        assert!(collection_is_effectively_empty(&batch));
    }

    #[test]
    fn output_mask_with_selected_field_keeps_collection_non_empty() {
        let message = Arc::new(Message::new(
            Arc::<str>::from("stream"),
            vec![Arc::<str>::from("a"), Arc::<str>::from("b")],
            vec![Arc::new(Value::Int64(1)), Arc::new(Value::Int64(2))],
        ));
        let mut tuple = Tuple::new(vec![message]);
        tuple.set_output_mask(vec![false, true]);

        let batch = RecordBatch::new(vec![tuple]).expect("batch");
        assert!(!collection_is_effectively_empty(&batch));
    }

    #[test]
    fn non_empty_collection_without_mask_is_not_empty() {
        let batch = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "a".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("batch");
        assert!(!collection_is_effectively_empty(&batch));
    }
}
