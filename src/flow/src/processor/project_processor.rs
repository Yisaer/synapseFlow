//! Project processor - corresponds to PhysicalProject
//! 
//! This processor projects columns from incoming data based on projection expressions.
//! Currently just passes data through for pipeline establishment.
//! 
//! Design follows rstream's Executor pattern with tokio::spawn and dedicated projection routine.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// Project processor that corresponds to PhysicalProject
/// 
/// This processor projects columns from incoming data based on projection expressions.
/// Currently just passes data through for pipeline establishment.
pub struct ProjectProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channels from upstream processors
    input_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl ProjectProcessor {
    /// Create a new ProjectProcessor
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
    
    /// Apply projection to data (currently just passes through)
    fn project_data(&self, data: &dyn crate::model::Collection) -> Result<Box<dyn crate::model::Collection>, crate::model::CollectionError> {
        // TODO: Implement actual projection logic based on projection expressions
        // For now, just return the original data to establish pipeline
        data.project(&[])
    }
}

impl StreamProcessor for ProjectProcessor {
    fn start(&self) -> ProcessorView {
        // Create channels using utils
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        let (stop_tx, stop_rx) = utils::create_stop_channel();
        
        // Spawn the projection routine - currently just passes data through
        let routine = self.create_project_routine(result_tx, stop_rx);
        
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
impl ProjectProcessor {
    /// Create project routine that runs in tokio task - currently just passes data through
    fn create_project_routine(
        &self,
        result_tx: broadcast::Sender<Result<StreamData, String>>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut input_receivers = self.input_receivers();
        let downstream_count = self.downstream_count;
        
        async move {
            println!("ProjectProcessor: Starting project routine for {} downstream processors", downstream_count);
            
            // Send stream start signal
            if result_tx.send(Ok(StreamData::stream_start())).is_err() {
                println!("ProjectProcessor: Failed to send start signal");
                return;
            }
            
            // Process incoming data (currently just passes through)
            // TODO: Implement actual projection logic
            loop {
                tokio::select! {
                    // Check stop signal
                    _ = stop_rx.recv() => {
                        println!("ProjectProcessor: Received stop signal, shutting down");
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
                                // Apply projection (currently just passes through)
                                if stream_data.is_data() {
                                    if let Some(collection) = stream_data.as_collection() {
                                        match Self::static_project_data_static(collection) {
                                            Ok(projected_data) => {
                                                let projected_stream_data = StreamData::collection(projected_data);
                                                if result_tx.send(Ok(projected_stream_data)).is_err() {
                                                    println!("ProjectProcessor: All downstream receivers dropped, stopping");
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                println!("ProjectProcessor: Error projecting data: {}", e);
                                                if result_tx.send(Err(e.to_string())).is_err() {
                                                    println!("ProjectProcessor: All downstream receivers dropped, stopping");
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    // Pass through control signals
                                    if result_tx.send(Ok(stream_data)).is_err() {
                                        println!("ProjectProcessor: All downstream receivers dropped, stopping");
                                        break;
                                    }
                                }
                            }
                            Ok(Err(e)) => {
                                println!("ProjectProcessor: Error from upstream: {}", e);
                                // Forward error downstream
                                if result_tx.send(Err(e)).is_err() {
                                    println!("ProjectProcessor: All downstream receivers dropped, stopping");
                                    break;
                                }
                            }
                            Err(e) => {
                                // Handle broadcast errors
                                let error_data = utils::handle_receive_error(e);
                                if result_tx.send(error_data).is_err() {
                                    println!("ProjectProcessor: All downstream receivers dropped, stopping");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            // Send stream end signal
            if result_tx.send(Ok(StreamData::stream_end())).is_err() {
                println!("ProjectProcessor: Failed to send end signal");
            }
            
            println!("ProjectProcessor: Project routine completed");
        }
    }
    
    /// Static version of project_data for use in async routine
    fn static_project_data_static(data: &dyn crate::model::Collection) -> Result<Box<dyn crate::model::Collection>, crate::model::CollectionError> {
        // TODO: Implement actual projection logic
        data.project(&[])
    }
}