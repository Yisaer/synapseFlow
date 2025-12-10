//! AggregationProcessor - processes aggregation operations
//!
//! This processor:
//! - Takes input data (Collection) and aggregation calls
//! - Creates accumulators for each aggregate function
//! - Updates accumulators with each incoming row
//! - Finalizes aggregates and outputs results when stream ends

use crate::aggregation::{AggregateAccumulator, AggregateFunctionRegistry};
use crate::model::Collection;
use crate::planner::physical::{AggregateCall, PhysicalAggregation, PhysicalPlan};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use datatypes::Value;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// AggregationProcessor - evaluates aggregation expressions
///
/// This processor:
/// - Takes input data (Collection) and aggregation configuration
/// - Creates accumulators for each aggregate call
/// - Updates accumulators with each incoming row
/// - Finalizes aggregates and outputs results when stream ends
pub struct AggregationProcessor {
    /// Processor identifier
    id: String,
    /// Physical aggregation configuration
    physical_aggregation: Arc<PhysicalAggregation>,
    /// Aggregate function registry for creating accumulators
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    /// Input channels for receiving data
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Broadcast channel for downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control output channel
    control_output: broadcast::Sender<ControlSignal>,
}

impl AggregationProcessor {
    /// Create a new AggregationProcessor from PhysicalAggregation
    pub fn new(
        id: impl Into<String>,
        physical_aggregation: Arc<PhysicalAggregation>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical_aggregation,
            aggregate_registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    /// Create an AggregationProcessor from a PhysicalPlan
    /// Returns None if the plan is not a PhysicalAggregation
    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
        aggregate_registry: Arc<AggregateFunctionRegistry>,
    ) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Aggregation(aggregation) => Some(Self::new(
                id,
                Arc::new(aggregation.clone()),
                aggregate_registry,
            )),
            _ => None,
        }
    }
}

impl Processor for AggregationProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let control_active = !control_receivers.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let physical_aggregation = self.physical_aggregation.clone();
        let aggregate_registry = self.aggregate_registry.clone();
        
        println!("[AggregationProcessor:{id}] starting with {} aggregate calls", 
                 physical_aggregation.aggregate_calls.len());

        tokio::spawn(async move {
            let mut control_streams = fan_in_control_streams(control_receivers);
            let mut stream_ended = false;
            
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                println!("[AggregationProcessor:{id}] received StreamEnd (control)");
                                stream_ended = true;
                                break;
                            }
                        }
                    }
                    data_item = input_streams.next() => {
                        match data_item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                log_received_data(&id, &StreamData::Collection(collection.clone()));
                                
                                // Create fresh accumulators for this batch of data
                                let mut batch_accumulators = match Self::create_accumulators_static(&physical_aggregation, &aggregate_registry) {
                                    Ok(acc) => acc,
                                    Err(e) => {
                                        return Err(ProcessorError::ProcessingError(format!("Failed to create accumulators: {}", e)));
                                    }
                                };
                                
                                // Process this collection: update accumulators and immediately finalize
                                if let Err(e) = Self::update_accumulators_static(&physical_aggregation, &mut batch_accumulators, collection.as_ref()) {
                                    return Err(ProcessorError::ProcessingError(format!("Failed to update accumulators: {}", e)));
                                }
                                
                                // Immediately finalize and output results for this batch
                                match Self::finalize_aggregates_static(&physical_aggregation, &batch_accumulators, collection.as_ref()) {
                                    Ok(result_collection) => {
                                        let result_data = StreamData::Collection(result_collection);
                                        if let Err(e) = send_with_backpressure(&output, result_data).await {
                                            println!("[AggregationProcessor:{id}] failed to send result: {}", e);
                                            return Err(e);
                                        }
                                        println!("[AggregationProcessor:{id}] processed batch and sent results");
                                    }
                                    Err(e) => {
                                        return Err(ProcessorError::ProcessingError(format!("Failed to finalize aggregates: {}", e)));
                                    }
                                }
                            }
                            Some(Ok(StreamData::Control(control_signal))) => {
                                let is_terminal = control_signal.is_terminal();
                                send_control_with_backpressure(&control_output, control_signal).await?;
                                if is_terminal {
                                    println!("[AggregationProcessor:{id}] received StreamEnd (data)");
                                    stream_ended = true;
                                    break;
                                }
                            }
                            Some(Ok(other_data)) => {
                                // Forward non-collection data (like Encoded, Bytes) as-is
                                log_received_data(&id, &other_data);
                                send_with_backpressure(&output, other_data).await?;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                                println!("[AggregationProcessor:{id}] lagged by {n} messages");
                            }
                            None => {
                                println!("[AggregationProcessor:{id}] all input streams ended");
                                break;
                            }
                        }
                    }
                }
            }

            // Stream has ended, just forward the end signal as results were already sent for each batch
            if stream_ended {
                println!("[AggregationProcessor:{id}] stream ended, forwarding end signal");
                
                // Send StreamGracefulEnd signal downstream
                send_control_with_backpressure(&control_output, ControlSignal::StreamGracefulEnd).await?;
                println!("[AggregationProcessor:{id}] aggregation processing completed");
            }

            println!("[AggregationProcessor:{id}] stopped");
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

impl AggregationProcessor {
    /// Static version of create_accumulators for use in async context
    fn create_accumulators_static(
        physical_aggregation: &PhysicalAggregation,
        aggregate_registry: &Arc<AggregateFunctionRegistry>,
    ) -> Result<Vec<Box<dyn AggregateAccumulator>>, String> {
        let mut accumulators = Vec::new();
        
        for call in &physical_aggregation.aggregate_calls {
            if call.distinct {
                return Err("DISTINCT aggregates are not supported yet".to_string());
            }
            
            let function = aggregate_registry
                .get(&call.func_name)
                .ok_or_else(|| format!("Aggregate function '{}' not found in registry", call.func_name))?;
            
            let accumulator = function.create_accumulator();
            accumulators.push(accumulator);
        }
        
        Ok(accumulators)
    }

    /// Static version of update_accumulators for use in async context
    fn update_accumulators_static(
        physical_aggregation: &PhysicalAggregation,
        accumulators: &mut [Box<dyn AggregateAccumulator>],
        collection: &dyn Collection,
    ) -> Result<(), String> {
        for (i, call) in physical_aggregation.aggregate_calls.iter().enumerate() {
            // Evaluate arguments for this aggregate call
            let args = Self::evaluate_arguments_static(call, collection)?;
            
            // Update accumulator for each row
            for row_idx in 0..collection.num_rows() {
                let mut row_args = Vec::new();
                for arg_values in &args {
                    row_args.push(arg_values[row_idx].clone());
                }
                accumulators[i].update(&row_args)?;
            }
        }
        
        Ok(())
    }

    /// Static version of evaluate_arguments for use in async context
    fn evaluate_arguments_static(
        call: &AggregateCall,
        collection: &dyn Collection,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut all_args = Vec::new();
        
        // Evaluate each argument expression for all rows
        for arg_expr in &call.args {
            let arg_values = arg_expr.eval_with_collection(collection)
                .map_err(|e| format!("Failed to evaluate argument: {}", e))?;
            all_args.push(arg_values);
        }
        
        Ok(all_args)
    }

    fn finalize_aggregates_static(
        physical_aggregation: &PhysicalAggregation,
        accumulators: &[Box<dyn AggregateAccumulator>],
        input_collection: &dyn Collection,
    ) -> Result<Box<dyn Collection>, String> {
        use crate::model::{RecordBatch, Tuple};
        use std::sync::Arc;

        // Create result values from finalized accumulators
        let mut affiliate_entries = Vec::new();
        
        for (call, accumulator) in physical_aggregation.aggregate_calls
            .iter()
            .zip(accumulators.iter())
        {
            let finalized_value = accumulator.finalize();
            affiliate_entries.push((
                Arc::new(call.output_column.clone()),
                finalized_value,
            ));
        }

        let mut tuples: Vec<Tuple> = input_collection.rows().to_vec();
        
        if tuples.is_empty() {
            let mut result_tuple = Tuple::new(vec![]);
            let affiliate_row = crate::model::AffiliateRow::new(affiliate_entries);
            result_tuple.affiliate = Some(affiliate_row);
            
            let collection = RecordBatch::new(vec![result_tuple])
                .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
            
            Ok(Box::new(collection))
        } else {
            let last_index = tuples.len() - 1;
            let last_tuple = &mut tuples[last_index];
            let affiliate_row = crate::model::AffiliateRow::new(affiliate_entries.clone());
            if let Some(existing_affiliate) = &mut last_tuple.affiliate {
                for (key, value) in affiliate_entries {
                    existing_affiliate.insert(key, value);
                }
            } else {
                last_tuple.affiliate = Some(affiliate_row);
            }
            let last_tuple_cloned = last_tuple.clone();
            let collection = RecordBatch::new(vec![last_tuple_cloned])
                .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;
            
            Ok(Box::new(collection))
        }
    }
}