//! Stream processor trait and common utilities
//! 
//! Defines the core StreamProcessor interface and shared utilities for all processors.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::stream_data::StreamData;
use crate::processor::ProcessorView;

/// Core trait for all stream processors
/// 
/// Each processor corresponds to a PhysicalPlan and runs in its own tokio task.
/// Processors handle data flow from multiple upstream sources and broadcast to multiple downstream sinks.
/// 
/// Design pattern:
/// 1. Constructor takes physical plan and input receivers
/// 2. `start()` method spawns tokio task and returns ProcessorView
/// 3. Each processor implements data processing routine with tokio::select!
/// 4. Supports graceful shutdown via broadcast channels
/// 5. Handles multiple input sources and output broadcasting
pub trait StreamProcessor: Send + Sync {
    /// Start the processor and return a view for control and output
    /// 
    /// This method should:
    /// 1. Create necessary channels (stop, result)
    /// 2. Spawn a tokio task for the processor routine
    /// 3. Return a ProcessorView for external control and data consumption
    fn start(&self) -> ProcessorView;
    
    /// Get the physical plan this processor corresponds to
    fn get_physical_plan(&self) -> &Arc<dyn PhysicalPlan>;
    
    /// Get the number of downstream processors this will broadcast to
    /// This is determined at construction time and used for channel capacity planning
    fn downstream_count(&self) -> usize;
    
    /// Get input channels from upstream processors
    fn input_receivers(&self) -> Vec<broadcast::Receiver<Result<StreamData, String>>>;
}

/// Common utilities for stream processors
pub mod utils {
    use super::*;
    
    /// Create a broadcast channel with appropriate capacity based on downstream count
    pub fn create_result_channel(downstream_count: usize) -> (broadcast::Sender<Result<StreamData, String>>, broadcast::Receiver<Result<StreamData, String>>) {
        // Base capacity + additional capacity per downstream
        let base_capacity = 1024;
        let additional_capacity = downstream_count * 256;
        let total_capacity = base_capacity + additional_capacity;
        
        broadcast::channel(total_capacity)
    }
    
    /// Create a stop channel for graceful shutdown
    pub fn create_stop_channel() -> (broadcast::Sender<()>, broadcast::Receiver<()>) {
        broadcast::channel(1)
    }
    
    /// Handle broadcast receive errors in a standardized way
    pub fn handle_receive_error(error: broadcast::error::RecvError) -> Result<StreamData, String> {
        match error {
            broadcast::error::RecvError::Lagged(_) => {
                Ok(StreamData::control(crate::processor::stream_data::ControlSignal::Backpressure))
            }
            broadcast::error::RecvError::Closed => {
                Ok(StreamData::control(crate::processor::stream_data::ControlSignal::StreamEnd))
            }
        }
    }
}