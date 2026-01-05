use super::{build_group_by_meta, create_accumulators_static, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::model::RecordBatch;
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Streaming sliding aggregation processor (incremental).
///
/// - Maintain a list of active windows, each with its own incremental aggregation state.
/// - For every incoming tuple, append a new window starting at that tuple's timestamp.
/// - Update all active windows whose `[start, start + length + delay)` contains the tuple time.
/// - Emit the oldest active window:
///   - immediately when `delay == 0` (per-tuple trigger),
///   - or when receiving deadline watermarks from upstream (`delay > 0`).
///
/// Notes:
/// - This is processing-time only: tuple timestamps are assumed to be non-decreasing.
/// - For `slidingwindow('ss', length)`, we treat `length` as the window length and `delay = 0`.
/// - For `slidingwindow('ss', length, delay)`, we treat the third argument as the delay.
pub struct StreamingSlidingAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_meta: Vec<GroupByMeta>,
    length_secs: u64,
    delay_secs: u64,
}

struct WindowGroupState {
    accumulators: Vec<Box<dyn crate::aggregation::AggregateAccumulator>>,
    last_tuple: crate::model::Tuple,
    key_values: Vec<Value>,
}

struct IncAggWindow {
    start_secs: u64,
    groups: HashMap<String, WindowGroupState>,
}

impl StreamingSlidingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let group_by_meta =
            build_group_by_meta(&physical.group_by_exprs, &physical.group_by_scalars);
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);

        let (length_secs, delay_secs) = match physical.window {
            StreamingWindowSpec::Sliding {
                time_unit: _,
                lookback,
                lookahead,
            } => (lookback.max(1), lookahead.unwrap_or(0)),
            _ => unreachable!("sliding processor requires sliding window spec"),
        };

        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            group_by_meta,
            length_secs,
            delay_secs,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn validate_supported_aggregates(&self) -> Result<(), ProcessorError> {
        for call in &self.physical.aggregate_calls {
            if call.distinct {
                return Err(ProcessorError::InvalidConfiguration(
                    "DISTINCT aggregates are not supported in streaming sliding aggregation"
                        .to_string(),
                ));
            }
            if !self
                .aggregate_registry
                .supports_incremental(&call.func_name)
            {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "Aggregate function '{}' does not support incremental updates",
                    call.func_name
                )));
            }
        }
        Ok(())
    }
}

impl Processor for StreamingSlidingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        if let Err(e) = self.validate_supported_aggregates() {
            return tokio::spawn(async move { Err(e) });
        }

        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let physical = Arc::clone(&self.physical);
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let group_by_meta = self.group_by_meta.clone();
        let length_secs = self.length_secs;
        let delay_secs = self.delay_secs;

        tokio::spawn(async move {
            let mut windows: VecDeque<IncAggWindow> = VecDeque::new();

            fn to_epoch_secs(ts: SystemTime) -> Result<u64, ProcessorError> {
                Ok(ts
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| {
                        ProcessorError::ProcessingError(format!("invalid timestamp: {e}"))
                    })?
                    .as_secs())
            }

            fn gc_windows(
                windows: &mut VecDeque<IncAggWindow>,
                now_secs: u64,
                length_secs: u64,
                delay_secs: u64,
            ) {
                while let Some(front) = windows.front() {
                    if front
                        .start_secs
                        .saturating_add(length_secs)
                        .saturating_add(delay_secs)
                        < now_secs
                    {
                        windows.pop_front();
                    } else {
                        break;
                    }
                }
            }

            fn update_window_with_tuple(
                physical: &PhysicalStreamingAggregation,
                aggregate_registry: &AggregateFunctionRegistry,
                group_by_meta: &[GroupByMeta],
                window: &mut IncAggWindow,
                tuple: &crate::model::Tuple,
            ) -> Result<(), ProcessorError> {
                let mut key_values = Vec::with_capacity(group_by_meta.len());
                for meta in group_by_meta {
                    key_values.push(meta.scalar.eval_with_tuple(tuple).map_err(|e| {
                        ProcessorError::ProcessingError(format!(
                            "failed to evaluate group-by expression: {e}"
                        ))
                    })?);
                }
                let key_repr = format!("{:?}", key_values);

                let entry = match window.groups.entry(key_repr) {
                    Entry::Occupied(o) => o.into_mut(),
                    Entry::Vacant(v) => {
                        let accumulators = create_accumulators_static(
                            &physical.aggregate_calls,
                            aggregate_registry,
                        )
                        .map_err(ProcessorError::ProcessingError)?;
                        v.insert(WindowGroupState {
                            accumulators,
                            last_tuple: tuple.clone(),
                            key_values: key_values.clone(),
                        })
                    }
                };

                entry.last_tuple = tuple.clone();
                entry.key_values = key_values;

                for (idx, call) in physical.aggregate_calls.iter().enumerate() {
                    let mut args = Vec::with_capacity(call.args.len());
                    for arg_expr in &call.args {
                        args.push(arg_expr.eval_with_tuple(tuple).map_err(|e| {
                            ProcessorError::ProcessingError(format!(
                                "failed to evaluate aggregate argument: {e}"
                            ))
                        })?);
                    }
                    entry
                        .accumulators
                        .get_mut(idx)
                        .ok_or_else(|| {
                            ProcessorError::ProcessingError("accumulator missing".to_string())
                        })?
                        .update(&args)
                        .map_err(ProcessorError::ProcessingError)?;
                }

                Ok(())
            }

            async fn emit_oldest_window(
                output: &broadcast::Sender<StreamData>,
                physical: &PhysicalStreamingAggregation,
                group_by_meta: &[GroupByMeta],
                windows: &VecDeque<IncAggWindow>,
            ) -> Result<(), ProcessorError> {
                let Some(window) = windows.front() else {
                    return Ok(());
                };
                if window.groups.is_empty() {
                    return Ok(());
                }

                let mut out_rows = Vec::with_capacity(window.groups.len());
                for state in window.groups.values() {
                    let mut affiliate_entries = Vec::new();
                    for (call, accumulator) in physical
                        .aggregate_calls
                        .iter()
                        .zip(state.accumulators.iter())
                    {
                        affiliate_entries
                            .push((Arc::new(call.output_column.clone()), accumulator.finalize()));
                    }
                    for (idx, value) in state.key_values.iter().enumerate() {
                        if let Some(meta) = group_by_meta.get(idx) {
                            if !meta.is_simple {
                                affiliate_entries
                                    .push((Arc::new(meta.output_name.clone()), value.clone()));
                            }
                        }
                    }

                    let mut tuple = crate::model::Tuple::with_timestamp(
                        state.last_tuple.messages.clone(),
                        state.last_tuple.timestamp,
                    );
                    tuple.add_affiliate_columns(affiliate_entries);
                    out_rows.push(tuple);
                }

                if out_rows.is_empty() {
                    return Ok(());
                }
                let batch = RecordBatch::new(out_rows)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
                send_with_backpressure(output, StreamData::collection(Box::new(batch))).await?;
                Ok(())
            }

            loop {
                tokio::select! {
                    biased;
                    Some(ctrl) = control_streams.next(), if control_active => {
                        if let Ok(control_signal) = ctrl {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                let rows = collection.into_rows().map_err(|e| {
                                    ProcessorError::ProcessingError(format!("failed to extract rows: {e}"))
                                })?;

                                for tuple in rows {
                                    let now_secs = to_epoch_secs(tuple.timestamp)?;
                                    gc_windows(&mut windows, now_secs, length_secs, delay_secs);

                                    windows.push_back(IncAggWindow {
                                        start_secs: now_secs,
                                        groups: HashMap::new(),
                                    });

                                    for window in windows.iter_mut() {
                                        if window.start_secs <= now_secs
                                            && window
                                                .start_secs
                                                .saturating_add(length_secs)
                                                .saturating_add(delay_secs)
                                                > now_secs
                                        {
                                            update_window_with_tuple(
                                                &physical,
                                                aggregate_registry.as_ref(),
                                                &group_by_meta,
                                                window,
                                                &tuple,
                                            )?;
                                        }
                                    }

                                    if delay_secs == 0 {
                                        emit_oldest_window(&output, &physical, &group_by_meta, &windows)
                                            .await?;
                                    }
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                if delay_secs == 0 {
                                    continue;
                                }
                                let now_secs = to_epoch_secs(ts)?;
                                gc_windows(&mut windows, now_secs, length_secs, delay_secs);
                                if let Some(front) = windows.front() {
                                    if front.start_secs.saturating_add(delay_secs) <= now_secs {
                                        emit_oldest_window(&output, &physical, &group_by_meta, &windows)
                                            .await?;
                                    }
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                let is_graceful = control_signal.is_graceful_end();
                                send_with_backpressure(&output, StreamData::control(control_signal)).await?;
                                if is_terminal {
                                    if is_graceful {
                                        emit_oldest_window(&output, &physical, &group_by_meta, &windows)
                                            .await?;
                                    }
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                log_broadcast_lagged(&id, n, "data input");
                            }
                            None => {
                                tracing::info!(processor_id = %id, "all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
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
