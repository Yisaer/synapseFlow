//! OrderProcessor - sorts each incoming Collection by ORDER BY keys.
//!
//! Semantics: sorting is applied within each incoming Collection (no global ordering across batches).

use crate::expr::value_compare;
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalOrder, PhysicalOrderKey, PhysicalPlan};
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
use std::cmp::Ordering;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct OrderProcessor {
    id: String,
    physical_order: Arc<PhysicalOrder>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl OrderProcessor {
    pub fn new(id: impl Into<String>, physical_order: Arc<PhysicalOrder>) -> Self {
        Self::new_with_channel_capacities(id, physical_order, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        physical_order: Arc<PhysicalOrder>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            physical_order,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Order(order) => Some(Self::new(id, Arc::new(order.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }

    fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
        value_compare::compare_values(left, right)
    }

    fn compare_key_values(
        key: &PhysicalOrderKey,
        left: &Value,
        right: &Value,
    ) -> Result<Ordering, String> {
        // Enforce NULLS LAST, regardless of ASC/DESC.
        if left.is_null() && right.is_null() {
            return Ok(Ordering::Equal);
        }
        if left.is_null() {
            return Ok(Ordering::Greater);
        }
        if right.is_null() {
            return Ok(Ordering::Less);
        }

        let ord = Self::compare_values(left, right).ok_or_else(|| {
            format!(
                "ORDER BY key '{}' is not comparable (left={:?}, right={:?})",
                key.original_expr, left, right
            )
        })?;

        Ok(if key.asc { ord } else { ord.reverse() })
    }

    fn apply_order(
        physical_order: &PhysicalOrder,
        collection: Box<dyn Collection>,
        stats: Option<&ProcessorStats>,
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        let metadata = collection.metadata().clone();
        let rows = collection.into_rows().map_err(|e| {
            ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
        })?;

        if rows.is_empty() || physical_order.keys.is_empty() {
            return Ok(Box::new(
                RecordBatch::new_with_metadata(rows, metadata).map_err(|e| {
                    ProcessorError::ProcessingError(format!("Failed to build record batch: {}", e))
                })?,
            ));
        }

        let mut sortable_rows: Vec<SortableRow> = Vec::with_capacity(rows.len());
        let mut key_representatives: Vec<Option<Value>> = vec![None; physical_order.keys.len()];
        'rows: for row in rows {
            let mut key_values = Vec::with_capacity(physical_order.keys.len());
            for key in &physical_order.keys {
                let value = match key.compiled_expr.eval_with_tuple(&row) {
                    Ok(value) => value,
                    Err(error) => {
                        if let Some(stats) = stats {
                            stats.record_error_logged(
                                "order processor error",
                                format!(
                                    "Failed to evaluate ORDER BY key '{}': {}",
                                    key.original_expr, error
                                ),
                            );
                        }
                        continue 'rows;
                    }
                };
                key_values.push(value);
            }

            for (idx, (key, value)) in physical_order
                .keys
                .iter()
                .zip(key_values.iter())
                .enumerate()
            {
                if value.is_null() {
                    continue;
                }
                if Self::compare_values(value, value).is_none() {
                    if let Some(stats) = stats {
                        stats.record_error_logged(
                            "order processor error",
                            format!(
                                "ORDER BY key '{}' is not comparable (value={:?})",
                                key.original_expr, value
                            ),
                        );
                    }
                    continue 'rows;
                }
                if let Some(representative) = &key_representatives[idx] {
                    if Self::compare_key_values(key, value, representative).is_ok() {
                        continue;
                    }
                    if let Some(stats) = stats {
                        stats.record_error_logged(
                            "order processor error",
                            format!(
                                "ORDER BY key '{}' is not comparable (left={:?}, right={:?})",
                                key.original_expr, value, representative
                            ),
                        );
                    }
                    continue 'rows;
                }
                key_representatives[idx] = Some(value.clone());
            }

            sortable_rows.push(SortableRow { row, key_values });
        }

        sortable_rows.sort_unstable_by(|left, right| {
            for (key, (left_value, right_value)) in physical_order
                .keys
                .iter()
                .zip(left.key_values.iter().zip(right.key_values.iter()))
            {
                let ord = Self::compare_key_values(key, left_value, right_value)
                    .unwrap_or(Ordering::Equal);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        let rows = sortable_rows.into_iter().map(|row| row.row).collect();

        let batch = RecordBatch::new_with_metadata(rows, metadata)
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        Ok(Box::new(batch))
    }
}

struct SortableRow {
    row: Tuple,
    key_values: Vec<Value>,
}

impl Processor for OrderProcessor {
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
        let physical_order = Arc::clone(&self.physical_order);
        let stats = Arc::clone(&self.stats);

        tracing::info!(processor_id = %id, "order processor starting");
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
                                if let Some(rows) = data.row_count() {
                                    stats.record_collection_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match Self::apply_order(physical_order.as_ref(), collection, Some(stats.as_ref())) {
                                            Ok(out_collection) => {
                                                let out = StreamData::collection(out_collection);
                                                let out_rows = out.row_count();
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
                                                    stats.record_collection_out(rows);
                                                }
                                            }
                                            Err(e) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                stats.record_error_logged("order processor error", e.to_string());
                                            }
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
                                log_broadcast_lagged(&id, skipped, "order data input");
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
