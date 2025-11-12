//! Filter processor - corresponds to PhysicalFilter
//! 
//! This processor filters incoming data based on a predicate expression.
//! Currently just passes data through for pipeline establishment.
//! 
//! Design follows rstream's Executor pattern with tokio::spawn and dedicated filtering routine.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// Filter processor that corresponds to PhysicalFilter
/// 
/// This processor filters incoming data based on a predicate expression.
/// Currently just passes data through for pipeline establishment.
pub struct FilterProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channels from upstream processors
    input_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl FilterProcessor {
    /// Create a new FilterProcessor
    pub fn new(
        physical_plan: Arc<dyn PhysicalPlan>,
        input_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
        downstream_count: usize,
    ) -> Self {
        Self {
            physical_plan,
            input_receivers,
            downstream_count,
        }
    }
    
    /// Apply filter predicate to data (currently just passes through)
    fn should_include(&self, _data: &dyn crate::model::Collection) -> bool {
        // TODO: Implement actual filtering logic based on predicate expression
        // For now, include all data to establish pipeline
        true
    }
}

impl StreamProcessor for FilterProcessor {
    fn start(&self) -> ProcessorView {
        // Create channels using utils
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        let (stop_tx, stop_rx) = utils::create_stop_channel();
        
        // Spawn the filter routine - currently just passes data through
        let routine = self.create_filter_routine(result_tx, stop_rx);
        
        let join_handle = tokio::spawn(routine);
        
        ProcessorView::new(
            result_rx,
            stop_tx,
            ProcessorHandle::new(join_handle),
        )
    }
    
    fn get_physical_plan(&self) -> &Arc<dyn PhysicalPlan> {
        &self.physical_plan
    }
    
    fn downstream_count(&self) -> usize {
        self.downstream_count
    }
    
    fn input_receivers(&self) -> Vec<broadcast::Receiver<Result<StreamData, String>>> {
        self.input_receivers.iter()
            .map(|rx| rx.resubscribe())
            .collect()
    }
}

// Private helper methods
impl FilterProcessor {
    /// Create filter routine that runs in tokio task - currently just passes data through
    fn create_filter_routine(
        &self,
        result_tx: broadcast::Sender<Result<StreamData, String>>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut input_receivers = self.input_receivers();
        let downstream_count = self.downstream_count;
        
        async move {
            println!("FilterProcessor: Starting filter routine for {} downstream processors", downstream_count);
            
            // Send stream start signal
            if result_tx.send(Ok(StreamData::stream_start())).is_err() {
                println!("FilterProcessor: Failed to send start signal");
                return;
            }
            
            // Process incoming data (currently just passes through)
            // TODO: Implement actual filtering logic
            loop {
                tokio::select! {
                    // Check stop signal
                    _ = stop_rx.recv() => {
                        println!("FilterProcessor: Received stop signal, shutting down");
                        break;
                    }
                    
                    // Process from first input channel (simplified for now)
                    // In a real implementation, would need to handle multiple inputs properly
                    result = async {
                        if let Some(receiver) = input_receivers.get_mut(0) {
                            receiver.recv().await
                        } else {
                            // No input receivers, this might be an error case
                            Err(broadcast::error::RecvError::Closed)
                        }
                    } => {
                        match result {
                            Ok(Ok(stream_data)) => {
                                // Pass through data (currently no actual filtering)
                                if stream_data.is_data() {
                                    if let Some(collection) = stream_data.as_collection() {
                                        if Self::static_should_include_static(collection) {
                                            // Data passes filter, send it downstream
                                            if result_tx.send(Ok(stream_data)).is_err() {
                                                println!("FilterProcessor: All downstream receivers dropped, stopping");
                                                break;
                                            }
                                        } else {
                                            println!("FilterProcessor: Data filtered out");
                                        }
                                    }
                                } else {
                                    // Pass through control signals
                                    if result_tx.send(Ok(stream_data)).is_err() {
                                        println!("FilterProcessor: All downstream receivers dropped, stopping");
                                        break;
                                    }
                                }
                            }
                            Ok(Err(e)) => {
                                println!("FilterProcessor: Error from upstream: {}", e);
                                // Forward error downstream
                                if result_tx.send(Err(e)).is_err() {
                                    println!("FilterProcessor: All downstream receivers dropped, stopping");
                                    break;
                                }
                            }
                            Err(e) => {
                                // Handle broadcast errors
                                let error_data = utils::handle_receive_error(e);
                                if result_tx.send(error_data).is_err() {
                                    println!("FilterProcessor: All downstream receivers dropped, stopping");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            // Send stream end signal
            if result_tx.send(Ok(StreamData::stream_end())).is_err() {
                println!("FilterProcessor: Failed to send end signal");
            }
            
            println!("FilterProcessor: Filter routine completed");
        }
    }
    
    /// Static version of should_include for use in async routine
    fn static_should_include_static(_data: &dyn crate::model::Collection) -> bool {
        // TODO: Implement actual filtering logic
        true
    }
}