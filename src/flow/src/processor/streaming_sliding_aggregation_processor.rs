use super::{build_group_by_meta, GroupByMeta};
use crate::aggregation::AggregateFunctionRegistry;
use crate::expr::func::BinaryFunc;
use crate::model::{AffiliateRow, RecordBatch, Tuple};
use crate::planner::physical::{PhysicalStreamingAggregation, StreamingWindowSpec};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, send_control_with_backpressure, send_with_backpressure,
    DEFAULT_CHANNEL_CAPACITY,
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

/// Streaming sliding aggregation processor.
///
/// Implementation notes:
/// - This processor does not buffer raw tuples for window evaluation.
/// - Instead, it stores per-tuple "contributions" (the evaluated aggregate arguments) together
///   with an expiry timestamp, and recomputes the current window result by summing all active
///   contributions (add-only).
/// - For `slidingwindow('ss', lookback)` (no lookahead), the window is emitted on every tuple.
/// - For `slidingwindow('ss', lookback, lookahead)`, the window is emitted only when receiving
///   a deadline watermark from upstream (the watermark processor is responsible for scheduling
///   `t + lookahead`).
pub struct StreamingSlidingAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_meta: Vec<GroupByMeta>,
    mode: SlidingMode,
}

#[derive(Clone, Copy)]
enum SlidingMode {
    EmitOnTuple {
        lookback_secs: u64,
    },
    EmitOnWatermark {
        lookback_secs: u64,
        lookahead_secs: u64,
    },
}

struct Contribution {
    expiry_secs: u64,
    agg_values: Vec<Value>,
}

struct GroupState {
    contributions: VecDeque<Contribution>,
    last_tuple: Tuple,
    key_values: Vec<Value>,
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

        let mode = match physical.window {
            StreamingWindowSpec::Sliding {
                time_unit: _,
                lookback,
                lookahead: None,
            } => SlidingMode::EmitOnTuple {
                lookback_secs: lookback,
            },
            StreamingWindowSpec::Sliding {
                time_unit: _,
                lookback,
                lookahead: Some(lookahead),
            } => SlidingMode::EmitOnWatermark {
                lookback_secs: lookback,
                lookahead_secs: lookahead,
            },
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
            mode,
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
            if call.func_name.to_lowercase() != "sum" {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "Streaming sliding aggregation currently supports only SUM, got '{}'",
                    call.func_name
                )));
            }
            if call.args.len() != 1 {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "SUM expects exactly one argument, got {}",
                    call.args.len()
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
        let group_by_meta = self.group_by_meta.clone();
        let mode = self.mode;

        tokio::spawn(async move {
            let mut groups: HashMap<String, GroupState> = HashMap::new();
            let mut stream_ended = false;

            fn to_epoch_secs(ts: SystemTime) -> Result<u64, ProcessorError> {
                Ok(ts
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| {
                        ProcessorError::ProcessingError(format!("invalid timestamp: {e}"))
                    })?
                    .as_secs())
            }

            fn drop_expired(contributions: &mut VecDeque<Contribution>, now_secs: u64) {
                while let Some(front) = contributions.front() {
                    if front.expiry_secs < now_secs {
                        contributions.pop_front();
                    } else {
                        break;
                    }
                }
            }

            fn compute_totals(
                contributions: &VecDeque<Contribution>,
                call_count: usize,
            ) -> Result<(Vec<Value>, usize), ProcessorError> {
                let mut totals: Vec<Option<Value>> = vec![None; call_count];
                let mut row_count: usize = 0;
                for contrib in contributions.iter() {
                    row_count += 1;
                    for (idx, value) in contrib.agg_values.iter().enumerate() {
                        if value.is_null() {
                            continue;
                        }
                        totals[idx] = match totals[idx].take() {
                            None => Some(value.clone()),
                            Some(existing) => Some(
                                BinaryFunc::Add
                                    .eval_binary(existing, value.clone())
                                    .map_err(|e| {
                                        ProcessorError::ProcessingError(format!(
                                            "failed to add aggregate value: {e}"
                                        ))
                                    })?,
                            ),
                        };
                    }
                }
                Ok((
                    totals
                        .into_iter()
                        .map(|v| v.unwrap_or(Value::Null))
                        .collect(),
                    row_count,
                ))
            }

            fn build_output_tuple(
                physical: &PhysicalStreamingAggregation,
                group_by_meta: &[GroupByMeta],
                state: &GroupState,
                totals: &[Value],
            ) -> Tuple {
                let mut tuple = state.last_tuple.clone();
                let mut affiliate = tuple
                    .affiliate
                    .take()
                    .unwrap_or_else(|| AffiliateRow::new(Vec::new()));

                for (call, value) in physical.aggregate_calls.iter().zip(totals.iter()) {
                    affiliate.insert(Arc::new(call.output_column.clone()), value.clone());
                }

                for (idx, value) in state.key_values.iter().enumerate() {
                    if let Some(meta) = group_by_meta.get(idx) {
                        if !meta.is_simple {
                            affiliate.insert(Arc::new(meta.output_name.clone()), value.clone());
                        }
                    }
                }

                tuple.affiliate = Some(affiliate);
                tuple
            }

            async fn emit_window(
                output: &broadcast::Sender<StreamData>,
                physical: &PhysicalStreamingAggregation,
                group_by_meta: &[GroupByMeta],
                groups: &HashMap<String, GroupState>,
            ) -> Result<(), ProcessorError> {
                if groups.is_empty() {
                    return Ok(());
                }

                let mut out_rows = Vec::new();
                for state in groups.values() {
                    if state.contributions.is_empty() {
                        continue;
                    }
                    let (totals, row_count) =
                        compute_totals(&state.contributions, physical.aggregate_calls.len())?;
                    if row_count == 0 {
                        continue;
                    }
                    out_rows.push(build_output_tuple(physical, group_by_meta, state, &totals));
                }

                if out_rows.is_empty() {
                    return Ok(());
                }
                let batch = RecordBatch::new(out_rows)
                    .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
                send_with_backpressure(output, StreamData::collection(Box::new(batch))).await?;
                Ok(())
            }

            fn update_group(
                physical: &PhysicalStreamingAggregation,
                group_by_meta: &[GroupByMeta],
                groups: &mut HashMap<String, GroupState>,
                tuple: Tuple,
                expiry_secs: u64,
            ) -> Result<(), ProcessorError> {
                let mut key_values = Vec::with_capacity(group_by_meta.len());
                for meta in group_by_meta.iter() {
                    key_values.push(meta.scalar.eval_with_tuple(&tuple).map_err(|e| {
                        ProcessorError::ProcessingError(format!(
                            "failed to evaluate group-by expression: {e}"
                        ))
                    })?);
                }
                let key_repr = format!("{:?}", key_values);

                let mut agg_values = Vec::with_capacity(physical.aggregate_calls.len());
                for call in &physical.aggregate_calls {
                    let value = call
                        .args
                        .first()
                        .expect("validated SUM args")
                        .eval_with_tuple(&tuple)
                        .map_err(|e| {
                            ProcessorError::ProcessingError(format!(
                                "failed to evaluate aggregate argument: {e}"
                            ))
                        })?;
                    agg_values.push(value);
                }

                match groups.entry(key_repr) {
                    Entry::Vacant(v) => {
                        let mut contributions = VecDeque::new();
                        contributions.push_back(Contribution {
                            expiry_secs,
                            agg_values,
                        });
                        v.insert(GroupState {
                            contributions,
                            last_tuple: tuple,
                            key_values,
                        });
                    }
                    Entry::Occupied(mut o) => {
                        let entry = o.get_mut();
                        entry.last_tuple = tuple;
                        entry.key_values = key_values;
                        entry.contributions.push_back(Contribution {
                            expiry_secs,
                            agg_values,
                        });
                    }
                }
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
                                stream_ended = true;
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
                                    let expiry_secs = match mode {
                                        SlidingMode::EmitOnTuple { lookback_secs } => {
                                            now_secs.saturating_add(lookback_secs)
                                        }
                                        SlidingMode::EmitOnWatermark { lookback_secs, lookahead_secs } => {
                                            now_secs
                                                .saturating_add(lookback_secs)
                                                .saturating_add(lookahead_secs)
                                        }
                                    };

                                    update_group(&physical, &group_by_meta, &mut groups, tuple, expiry_secs)?;

                                    // Processing-time default: timestamps are non-decreasing, so we can
                                    // expire eagerly using the current tuple timestamp.
                                    for state in groups.values_mut() {
                                        drop_expired(&mut state.contributions, now_secs);
                                    }

                                    // Prune empty groups.
                                    groups.retain(|_, state| !state.contributions.is_empty());

                                    if let SlidingMode::EmitOnTuple { .. } = mode {
                                        emit_window(&output, &physical, &group_by_meta, &groups).await?;
                                    }
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                if let SlidingMode::EmitOnWatermark { .. } = mode {
                                    let now_secs = to_epoch_secs(ts)?;
                                    for state in groups.values_mut() {
                                        drop_expired(&mut state.contributions, now_secs);
                                    }
                                    groups.retain(|_, state| !state.contributions.is_empty());
                                    emit_window(&output, &physical, &group_by_meta, &groups).await?;
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                let is_graceful = matches!(control_signal, ControlSignal::StreamGracefulEnd);
                                send_with_backpressure(&output, StreamData::control(control_signal)).await?;
                                if is_terminal {
                                    if is_graceful {
                                        // Best-effort: emit the current window once.
                                        emit_window(&output, &physical, &group_by_meta, &groups).await?;
                                    }
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[StreamingSlidingAggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[StreamingSlidingAggregationProcessor:{id}] all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            if stream_ended {
                send_control_with_backpressure(&control_output, ControlSignal::StreamGracefulEnd)
                    .await?;
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
