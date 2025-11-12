//! Processor view and handle for controlling stream processors
//! 
//! Provides control mechanisms for starting, stopping and monitoring stream processors.

use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use crate::processor::StreamData;

/// View for controlling and monitoring a stream processor
#[derive(Debug)]
pub struct ProcessorView {
    /// Channel for receiving processed results
    pub result_receiver: broadcast::Receiver<Result<StreamData, String>>,
    /// Channel for sending stop signal to the processor
    pub stop_sender: broadcast::Sender<()>,
    /// Handle to the processor task
    pub task_handle: ProcessorHandle,
}

/// Handle to a running processor task
#[derive(Debug)]
pub struct ProcessorHandle {
    /// The tokio task handle
    pub join_handle: JoinHandle<()>,
}

impl ProcessorHandle {
    /// Create a new processor handle
    pub fn new(join_handle: JoinHandle<()>) -> Self {
        Self { join_handle }
    }
    
    /// Wait for the processor task to complete
    pub async fn wait(self) -> Result<(), tokio::task::JoinError> {
        self.join_handle.await
    }
    
    /// Abort the processor task
    pub fn abort(&self) {
        self.join_handle.abort();
    }
}

impl ProcessorView {
    /// Create a new processor view
    pub fn new(
        result_receiver: broadcast::Receiver<Result<StreamData, String>>,
        stop_sender: broadcast::Sender<()>,
        task_handle: ProcessorHandle,
    ) -> Self {
        Self {
            result_receiver,
            stop_sender,
            task_handle,
        }
    }
    
    /// Send stop signal to the processor
    pub fn stop(&self) -> Result<usize, broadcast::error::SendError<()>> {
        self.stop_sender.send(())
    }
    
    /// Get a new receiver for the result channel
    pub fn result_resubscribe(&self) -> broadcast::Receiver<Result<StreamData, String>> {
        self.result_receiver.resubscribe()
    }
    
    /// Check if the processor is still running
    pub fn is_running(&self) -> bool {
        !self.task_handle.join_handle.is_finished()
    }
    
    /// Get the number of active downstream receivers
    pub fn active_receiver_count(&self) -> usize {
        self.result_receiver.len()
    }
}