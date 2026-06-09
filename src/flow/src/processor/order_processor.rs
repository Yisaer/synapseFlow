//! OrderProcessor - sorts each incoming Collection by ORDER BY keys.
//!
//! Semantics: sorting is applied within each incoming Collection (no global ordering across batches).

use crate::expr::scalar::ColumnRef;
use crate::expr::value_compare;
use crate::expr::ScalarExpr;
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalOrder, PhysicalOrderKey, PhysicalPlan};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use datatypes::Value;
use futures::stream::StreamExt;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct OrderProcessor {
    id: String,
    physical_order: Arc<PhysicalOrder>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
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
        let (output, _) = broadcast::channel(channel_capacities.data);
        let (control_output, _) = broadcast::channel(channel_capacities.control);
        Self {
            id: id.into(),
            physical_order,
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
            PhysicalPlan::Order(order) => Some(Self::new(id, Arc::new(order.clone()))),
            _ => None,
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
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

    fn compare_rows(
        keys: &[PhysicalOrderKey],
        accessors: &[KeyAccessor],
        rows: &[Tuple],
        left_idx: usize,
        right_idx: usize,
    ) -> Result<Ordering, String> {
        for (key, accessor) in keys.iter().zip(accessors.iter()) {
            let left = accessor.value(rows, left_idx)?;
            let right = accessor.value(rows, right_idx)?;
            let ord = Self::compare_key_values(key, left, right)?;
            if ord != Ordering::Equal {
                return Ok(ord);
            }
        }
        Ok(Ordering::Equal)
    }

    fn compare_tuples(
        keys: &[PhysicalOrderKey],
        accessors: &[KeyAccessor],
        left: &Tuple,
        right: &Tuple,
    ) -> Result<Ordering, String> {
        for (key, accessor) in keys.iter().zip(accessors.iter()) {
            let left_val = accessor.value_from_tuple(left)?;
            let right_val = accessor.value_from_tuple(right)?;
            let ord = Self::compare_key_values(key, left_val, right_val)?;
            if ord != Ordering::Equal {
                return Ok(ord);
            }
        }
        Ok(Ordering::Equal)
    }

    fn reorder_rows_in_place(rows: &mut [Tuple], sorted_indices: &[usize]) {
        let n = rows.len();
        debug_assert_eq!(n, sorted_indices.len(), "row arity mismatch");

        let mut pos_to_source: Vec<usize> = (0..n).collect();
        let mut source_to_pos: Vec<usize> = (0..n).collect();

        for target_pos in 0..n {
            let desired_source = sorted_indices[target_pos];
            let current_pos = source_to_pos[desired_source];
            if current_pos == target_pos {
                continue;
            }

            rows.swap(target_pos, current_pos);

            let source_at_target = pos_to_source[target_pos];
            let source_at_current = pos_to_source[current_pos];
            pos_to_source.swap(target_pos, current_pos);

            source_to_pos[source_at_target] = current_pos;
            source_to_pos[source_at_current] = target_pos;
        }
    }

    fn apply_order(
        physical_order: &PhysicalOrder,
        collection: Box<dyn Collection>,
    ) -> Result<Box<dyn Collection>, ProcessorError> {
        let mut rows = collection.into_rows().map_err(|e| {
            ProcessorError::ProcessingError(format!("Failed to materialize rows: {}", e))
        })?;

        if rows.is_empty() || physical_order.keys.is_empty() {
            return Ok(Box::new(RecordBatch::new(rows).map_err(|e| {
                ProcessorError::ProcessingError(format!("Failed to build record batch: {}", e))
            })?));
        }

        let accessors = build_accessors(&physical_order.keys, &rows)?;

        // If all keys can be borrowed from tuples without pre-computation, sort rows in place.
        if accessors.iter().all(KeyAccessor::is_borrowed) {
            let err: RefCell<Option<String>> = RefCell::new(None);
            rows.sort_unstable_by(|left, right| {
                if err.borrow().is_some() {
                    return Ordering::Equal;
                }
                match Self::compare_tuples(&physical_order.keys, &accessors, left, right) {
                    Ok(ord) => ord,
                    Err(e) => {
                        *err.borrow_mut() = Some(e);
                        Ordering::Equal
                    }
                }
            });

            if let Some(e) = err.into_inner() {
                return Err(ProcessorError::ProcessingError(e));
            }

            let batch = RecordBatch::new(rows)
                .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
            return Ok(Box::new(batch));
        }

        // Otherwise, sort indices to keep computed key columns aligned with the original row order.
        let mut indices: Vec<usize> = (0..rows.len()).collect();
        let err: RefCell<Option<String>> = RefCell::new(None);
        indices.sort_unstable_by(|&left_idx, &right_idx| {
            if err.borrow().is_some() {
                return Ordering::Equal;
            }
            match Self::compare_rows(&physical_order.keys, &accessors, &rows, left_idx, right_idx) {
                Ok(ord) => ord,
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    Ordering::Equal
                }
            }
        });

        if let Some(e) = err.into_inner() {
            return Err(ProcessorError::ProcessingError(e));
        }

        Self::reorder_rows_in_place(rows.as_mut_slice(), &indices);

        let batch =
            RecordBatch::new(rows).map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        Ok(Box::new(batch))
    }
}

enum KeyAccessor {
    BorrowedByIndex {
        source_name: String,
        column_index: usize,
    },
    BorrowedByName {
        column_name: String,
    },
    Computed {
        values: Vec<Value>,
    },
}

impl KeyAccessor {
    fn is_borrowed(&self) -> bool {
        matches!(
            self,
            Self::BorrowedByIndex { .. } | Self::BorrowedByName { .. }
        )
    }

    fn value<'a>(&'a self, rows: &'a [Tuple], row_idx: usize) -> Result<&'a Value, String> {
        match self {
            Self::BorrowedByIndex {
                source_name,
                column_index,
            } => rows
                .get(row_idx)
                .and_then(|tuple| tuple.value_by_index(source_name, *column_index))
                .ok_or_else(|| {
                    format!(
                        "ORDER BY column not found: {}#{}",
                        source_name, column_index
                    )
                }),
            Self::BorrowedByName { column_name } => rows
                .get(row_idx)
                .and_then(|tuple| tuple.value_by_name("", column_name))
                .ok_or_else(|| format!("ORDER BY column not found: {}", column_name)),
            Self::Computed { values } => values
                .get(row_idx)
                .ok_or_else(|| "ORDER BY row index out of bounds".to_string()),
        }
    }

    fn value_from_tuple<'a>(&'a self, tuple: &'a Tuple) -> Result<&'a Value, String> {
        match self {
            Self::BorrowedByIndex {
                source_name,
                column_index,
            } => tuple
                .value_by_index(source_name, *column_index)
                .ok_or_else(|| {
                    format!(
                        "ORDER BY column not found: {}#{}",
                        source_name, column_index
                    )
                }),
            Self::BorrowedByName { column_name } => tuple
                .value_by_name("", column_name)
                .ok_or_else(|| format!("ORDER BY column not found: {}", column_name)),
            Self::Computed { .. } => Err("computed key accessor requires row index".to_string()),
        }
    }
}

fn build_accessors(
    keys: &[PhysicalOrderKey],
    rows: &[Tuple],
) -> Result<Vec<KeyAccessor>, ProcessorError> {
    let mut accessors = Vec::with_capacity(keys.len());
    for key in keys {
        match &key.compiled_expr {
            ScalarExpr::Column(ColumnRef::ByIndex {
                source_name,
                column_index,
            }) => accessors.push(KeyAccessor::BorrowedByIndex {
                source_name: source_name.clone(),
                column_index: *column_index,
            }),
            ScalarExpr::Column(ColumnRef::ByName { column_name }) => {
                accessors.push(KeyAccessor::BorrowedByName {
                    column_name: column_name.clone(),
                })
            }
            _ => {
                let mut values = Vec::with_capacity(rows.len());
                for tuple in rows {
                    let value = key.compiled_expr.eval_with_tuple(tuple).map_err(|e| {
                        ProcessorError::ProcessingError(format!(
                            "Failed to evaluate ORDER BY key '{}': {}",
                            key.original_expr, e
                        ))
                    })?;
                    values.push(value);
                }
                accessors.push(KeyAccessor::Computed { values });
            }
        }
    }
    Ok(accessors)
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
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        match Self::apply_order(physical_order.as_ref(), collection) {
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
