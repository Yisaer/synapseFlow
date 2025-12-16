//! StreamingAggregationProcessor - incremental aggregation with windowing.

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry};
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{
    AggregateCall, PhysicalPlan, PhysicalStreamingAggregation, StreamingWindowSpec,
};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_received_data, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use sqlparser::ast::Expr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Tracks aggregation state for a single group key.
struct GroupState {
    accumulators: Vec<Box<dyn AggregateAccumulator>>,
    last_tuple: crate::model::Tuple,
    key_values: Vec<Value>,
}

/// Shared aggregation logic reused by count and tumbling windows.
struct AggregationWorker {
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    group_by_simple_flags: Vec<bool>,
    groups: HashMap<String, GroupState>,
}

impl AggregationWorker {
    fn new(
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        group_by_simple_flags: Vec<bool>,
    ) -> Self {
        Self {
            physical,
            aggregate_registry,
            group_by_simple_flags,
            groups: HashMap::new(),
        }
    }

    fn update_groups(&mut self, tuple: &crate::model::Tuple) -> Result<(), String> {
        let key_values = self.evaluate_group_by(tuple)?;
        let key_repr = format!("{:?}", key_values);

        let entry = match self.groups.entry(key_repr) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let accumulators = create_accumulators_static(
                    &self.physical.aggregate_calls,
                    self.aggregate_registry.as_ref(),
                )?;
                v.insert(GroupState {
                    accumulators,
                    last_tuple: tuple.clone(),
                    key_values: key_values.clone(),
                })
            }
        };

        entry.last_tuple = tuple.clone();
        entry.key_values = key_values;

        for (idx, call) in self.physical.aggregate_calls.iter().enumerate() {
            let mut args = Vec::new();
            for arg_expr in &call.args {
                args.push(
                    arg_expr
                        .eval_with_tuple(tuple)
                        .map_err(|e| format!("Failed to evaluate aggregate argument: {}", e))?,
                );
            }
            entry
                .accumulators
                .get_mut(idx)
                .ok_or_else(|| "accumulator missing".to_string())?
                .update(&args)?;
        }

        Ok(())
    }

    fn evaluate_group_by(&self, tuple: &crate::model::Tuple) -> Result<Vec<Value>, String> {
        let mut values = Vec::with_capacity(self.physical.group_by_scalars.len());
        for scalar in &self.physical.group_by_scalars {
            values.push(
                scalar
                    .eval_with_tuple(tuple)
                    .map_err(|e| format!("Failed to evaluate group-by expression: {}", e))?,
            );
        }
        Ok(values)
    }

    fn finalize_current_window(&mut self) -> Result<Option<Box<dyn Collection>>, String> {
        if self.groups.is_empty() {
            self.groups.clear();
            return Ok(None);
        }

        let mut output_tuples = Vec::with_capacity(self.groups.len());
        for (_key, mut state) in self.groups.drain() {
            let tuple = finalize_group(
                &self.physical.aggregate_calls,
                &self.physical.group_by_exprs,
                &self.group_by_simple_flags,
                &mut state.accumulators,
                &state.last_tuple,
                &state.key_values,
            )?;
            output_tuples.push(tuple);
        }

        let collection = RecordBatch::new(output_tuples)
            .map_err(|e| format!("Failed to build RecordBatch: {e}"))?;
        Ok(Some(Box::new(collection)))
    }
}

/// Tracks progress for a count-based window.
struct CountWindowState {
    target: u64,
    seen: u64,
}

impl CountWindowState {
    fn new(target: u64) -> Self {
        Self { target, seen: 0 }
    }

    fn register_row_and_check_finalize(&mut self) -> bool {
        self.seen += 1;
        self.seen >= self.target
    }

    fn reset(&mut self) {
        self.seen = 0;
    }
}

/// Tracks progress for a tumbling window.
struct TumblingWindowState {
    length: Duration,
    window_end: Option<Instant>,
}

impl TumblingWindowState {
    fn new(length: Duration) -> Self {
        Self {
            length,
            window_end: None,
        }
    }

    fn should_finalize(&self, now: Instant) -> bool {
        matches!(self.window_end, Some(end) if now >= end)
    }

    fn maybe_finalize(
        &mut self,
        now: Instant,
        worker: &mut AggregationWorker,
    ) -> Result<Option<Box<dyn Collection>>, String> {
        if self.should_finalize(now) {
            let result = worker.finalize_current_window()?;
            self.window_end = Some(now + self.length);
            return Ok(result);
        }
        Ok(None)
    }

    fn register_row(&mut self, now: Instant) {
        if self.window_end.is_none() {
            self.window_end = Some(now + self.length);
        }
    }

    fn handle_tick(
        &mut self,
        worker: &mut AggregationWorker,
    ) -> Result<Option<Box<dyn Collection>>, String> {
        let now = Instant::now();
        self.maybe_finalize(now, worker)
    }

    fn flush_on_end(
        &mut self,
        worker: &mut AggregationWorker,
    ) -> Result<Option<Box<dyn Collection>>, String> {
        let result = worker.finalize_current_window()?;
        self.window_end = None;
        Ok(result)
    }
}

/// StreamingAggregationProcessor - performs incremental aggregation with windowing.
pub enum StreamingAggregationProcessor {
    Count(StreamingCountAggregationProcessor),
    Tumbling(StreamingTumblingAggregationProcessor),
}

/// Data-driven count window implementation.
pub struct StreamingCountAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_simple_flags: Vec<bool>,
    target: u64,
}

/// Time-driven tumbling window implementation.
pub struct StreamingTumblingAggregationProcessor {
    id: String,
    physical: Arc<PhysicalStreamingAggregation>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    group_by_simple_flags: Vec<bool>,
}

impl StreamingAggregationProcessor {
    pub fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let window = physical.window.clone();
        match window {
            StreamingWindowSpec::Count { count } => {
                StreamingAggregationProcessor::Count(StreamingCountAggregationProcessor::new(
                    id,
                    Arc::clone(&physical),
                    Arc::clone(&aggregate_registry),
                    count,
                ))
            }
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length: _,
            } => {
                // Currently only seconds are supported at the logical level.
                StreamingAggregationProcessor::Tumbling(StreamingTumblingAggregationProcessor::new(
                    id,
                    Arc::clone(&physical),
                    aggregate_registry,
                ))
            }
        }
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::StreamingAggregation(aggregation) => Some(Self::new(
                id,
                Arc::new(aggregation.clone()),
                aggregate_registry,
            )),
            _ => None,
        }
    }
}

impl Processor for StreamingAggregationProcessor {
    fn id(&self) -> &str {
        match self {
            StreamingAggregationProcessor::Count(p) => p.id(),
            StreamingAggregationProcessor::Tumbling(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.start(),
            StreamingAggregationProcessor::Tumbling(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            StreamingAggregationProcessor::Count(p) => p.subscribe_control_output(),
            StreamingAggregationProcessor::Tumbling(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            StreamingAggregationProcessor::Count(p) => p.add_control_input(receiver),
            StreamingAggregationProcessor::Tumbling(p) => p.add_control_input(receiver),
        }
    }
}

impl StreamingCountAggregationProcessor {
    fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
        target: u64,
    ) -> Self {
        let group_by_simple_flags = group_by_flags(&physical.group_by_exprs);
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            group_by_simple_flags,
            target,
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        window_state: &mut CountWindowState,
        collection: &dyn Collection,
    ) -> Result<Vec<Box<dyn Collection>>, String> {
        let mut outputs = Vec::new();
        for row in collection.rows() {
            worker.update_groups(row)?;

            if window_state.register_row_and_check_finalize() {
                if let Some(batch) = worker.finalize_current_window()? {
                    outputs.push(batch);
                }
                window_state.reset();
            }
        }
        Ok(outputs)
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl Processor for StreamingCountAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_simple_flags = self.group_by_simple_flags.clone();
        let target = self.target;

        tokio::spawn(async move {
            let mut worker =
                AggregationWorker::new(physical, aggregate_registry, group_by_simple_flags);
            let mut window_state = CountWindowState::new(target);
            let mut stream_ended = false;

            loop {
                tokio::select! {
                    // Handle control signals first if present
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
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                match StreamingCountAggregationProcessor::process_collection(&mut worker, &mut window_state, collection.as_ref()) {
                                    Ok(outputs) => {
                                        for out in outputs {
                                            let data = StreamData::Collection(out);
                                            send_with_backpressure(&output, data).await?
                                        }
                                    }
                                    Err(e) => {
                                        return Err(ProcessorError::ProcessingError(format!("Failed to process streaming count aggregation: {e}")));
                                    }
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                send_control_with_backpressure(&control_output, control_signal).await?;
                                if is_terminal {
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                log_received_data(&id, &other);
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[StreamingCountAggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[StreamingCountAggregationProcessor:{id}] all input streams ended");
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

impl StreamingTumblingAggregationProcessor {
    fn new(
        id: impl Into<String>,
        physical: Arc<PhysicalStreamingAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let group_by_simple_flags = group_by_flags(&physical.group_by_exprs);
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            group_by_simple_flags,
        }
    }

    fn process_collection(
        worker: &mut AggregationWorker,
        window_state: &mut TumblingWindowState,
        collection: &dyn Collection,
    ) -> Result<Vec<Box<dyn Collection>>, String> {
        let mut outputs = Vec::new();
        for row in collection.rows() {
            let now = Instant::now();
            if let Some(batch) = window_state.maybe_finalize(now, worker)? {
                outputs.push(batch);
            }

            worker.update_groups(row)?;
            window_state.register_row(now);
        }

        Ok(outputs)
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl Processor for StreamingTumblingAggregationProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let aggregate_registry = Arc::clone(&self.aggregate_registry);
        let physical = Arc::clone(&self.physical);
        let group_by_simple_flags = self.group_by_simple_flags.clone();
        let window_length = match physical.window {
            StreamingWindowSpec::Tumbling {
                time_unit: _,
                length,
            } => Duration::from_secs(length),
            _ => unreachable!("tumbling processor requires tumbling window spec"),
        };

        tokio::spawn(async move {
            let mut worker =
                AggregationWorker::new(physical, aggregate_registry, group_by_simple_flags);
            let mut window_state = TumblingWindowState::new(window_length);
            let mut ticker = interval(window_length);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut stream_ended = false;

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
                    _ = ticker.tick() => {
                        match window_state.handle_tick(&mut worker) {
                            Ok(Some(batch)) => {
                                let data = StreamData::Collection(batch);
                                send_with_backpressure(&output, data).await?
                            }
                            Ok(None) => {}
                            Err(err) => {
                                return Err(ProcessorError::ProcessingError(format!("Failed to process tumbling window tick: {err}")));
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                match StreamingTumblingAggregationProcessor::process_collection(&mut worker, &mut window_state, collection.as_ref()) {
                                    Ok(outputs) => {
                                        for out in outputs {
                                            let data = StreamData::Collection(out);
                                            send_with_backpressure(&output, data).await?
                                        }
                                    }
                                    Err(e) => {
                                        return Err(ProcessorError::ProcessingError(format!("Failed to process streaming tumbling aggregation: {e}")));
                                    }
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                send_control_with_backpressure(&control_output, control_signal).await?;
                                if is_terminal {
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other)) => {
                                log_received_data(&id, &other);
                                send_with_backpressure(&output, other).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[StreamingTumblingAggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[StreamingTumblingAggregationProcessor:{id}] all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            if stream_ended {
                if let Some(batch) = window_state.flush_on_end(&mut worker).map_err(|err| {
                    ProcessorError::ProcessingError(format!(
                        "Failed to finalize last tumbling window: {err}"
                    ))
                })? {
                    send_with_backpressure(&output, StreamData::Collection(batch)).await?;
                }
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

fn finalize_group(
    aggregate_calls: &[AggregateCall],
    group_by_exprs: &[Expr],
    group_by_simple_flags: &[bool],
    accumulators: &mut [Box<dyn AggregateAccumulator>],
    last_tuple: &crate::model::Tuple,
    key_values: &[Value],
) -> Result<crate::model::Tuple, String> {
    use crate::model::AffiliateRow;
    use std::sync::Arc;

    let mut affiliate_entries = Vec::new();
    for (call, accumulator) in aggregate_calls.iter().zip(accumulators.iter_mut()) {
        affiliate_entries.push((Arc::new(call.output_column.clone()), accumulator.finalize()));
    }

    for (idx, value) in key_values.iter().enumerate() {
        if !group_by_simple_flags.get(idx).copied().unwrap_or(false) {
            let name = group_by_exprs[idx].to_string();
            affiliate_entries.push((Arc::new(name), value.clone()));
        }
    }

    let mut tuple = last_tuple.clone();
    let mut affiliate = tuple
        .affiliate
        .take()
        .unwrap_or_else(|| AffiliateRow::new(Vec::new()));
    for (k, v) in affiliate_entries {
        affiliate.insert(k, v);
    }
    tuple.affiliate = Some(affiliate);
    Ok(tuple)
}

fn create_accumulators_static(
    aggregate_calls: &[AggregateCall],
    registry: &AggregateFunctionRegistry,
) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
    let mut accumulators = Vec::new();
    for call in aggregate_calls {
        if call.distinct {
            return Err("DISTINCT aggregates are not supported yet".to_string());
        }
        let function = registry
            .get(&call.func_name)
            .ok_or_else(|| format!("Aggregate function '{}' not found", call.func_name))?;
        accumulators.push(function.create_accumulator());
    }
    Ok(accumulators)
}

fn is_simple_column_expr(expr: &sqlparser::ast::Expr) -> bool {
    matches!(expr, sqlparser::ast::Expr::Identifier(_))
}

fn group_by_flags(exprs: &[Expr]) -> Vec<bool> {
    exprs.iter().map(is_simple_column_expr).collect()
}
