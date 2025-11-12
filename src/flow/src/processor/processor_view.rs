//! ProcessorView - unified view for processor control and output access
//! 
//! This struct provides a consistent interface for accessing processor outputs and control.
//! 
//! Redesigned for single input, multiple output architecture:
//! - Single input channel receiver - what the processor listens to
//! - Multiple output channel senders - what downstream processors connect to
//! - Optional external control/result sender for special processors

use tokio::sync::broadcast;
use crate::processor::StreamData;
use tokio::task::JoinHandle;

/// Handle to a processor task
/// 
/// This provides a simple wrapper around tokio::task::JoinHandle
/// for better type safety and potential future extensions
#[derive(Debug)]
pub struct ProcessorHandle {
    join_handle: JoinHandle<()>,
}

impl ProcessorHandle {
    /// Create a new ProcessorHandle from a JoinHandle
    pub fn new(join_handle: JoinHandle<()>) -> Self {
        Self { join_handle }
    }
    
    /// Check if the task is finished
    pub fn is_finished(&self) -> bool {
        self.join_handle.is_finished()
    }
    
    /// Convert into the underlying JoinHandle
    pub fn into_join_handle(self) -> JoinHandle<()> {
        self.join_handle
    }
    
    /// Get a reference to the underlying JoinHandle
    pub fn join_handle(&self) -> &JoinHandle<()> {
        &self.join_handle
    }
}

/// Unified view for accessing processor outputs and control
/// 
/// Each processor creates:
/// - One input channel receiver (what it listens to)
/// - Multiple output channel senders (what it broadcasts to)
/// - Optional external control/result sender for special cases
#[derive(Debug)]
pub struct ProcessorView {
    /// Optional external sender for control signals or results
    /// 
    /// For ControlSourceProcessor: used for injecting external control signals
    /// For ResultSinkProcessor: used for forwarding results to external consumers
    /// For other processors: typically None
    pub external_sender: Option<broadcast::Sender<StreamData>>,
    
    /// Input channel receiver - this is what the processor listens to
    /// 
    /// Contains the data/control signals that this processor processes
    pub input_receiver: broadcast::Receiver<StreamData>,
    
    /// Output channel senders - these are what downstream processors connect to
    /// 
    /// Each processor broadcasts its processed results to these channels
    pub output_senders: Vec<broadcast::Sender<StreamData>>,
    
    /// Handle to the processor task
    pub task_handle: ProcessorHandle,
}

impl ProcessorView {
    /// Create a new ProcessorView
    pub fn new(
        external_sender: Option<broadcast::Sender<StreamData>>,
        input_receiver: broadcast::Receiver<StreamData>,
        output_senders: Vec<broadcast::Sender<StreamData>>,
        task_handle: ProcessorHandle,
    ) -> Self {
        Self {
            external_sender,
            input_receiver,
            output_senders,
            task_handle,
        }
    }
    
    /// Create a ProcessorView for processors that only have outputs (like ControlSourceProcessor)
    pub fn control_only(
        external_sender: Option<broadcast::Sender<StreamData>>,
        input_receiver: broadcast::Receiver<StreamData>,
        output_senders: Vec<broadcast::Sender<StreamData>>,
        task_handle: ProcessorHandle,
    ) -> Self {
        Self {
            external_sender,
            input_receiver,
            output_senders,
            task_handle,
        }
    }
    
    /// Create a ProcessorView for processors that only have inputs (like ResultSinkProcessor)
    pub fn sink_only(
        external_sender: Option<broadcast::Sender<StreamData>>,
        input_receiver: broadcast::Receiver<StreamData>,
        task_handle: ProcessorHandle,
    ) -> Self {
        Self {
            external_sender,
            input_receiver,
            output_senders: Vec::new(), // Sinks typically have no outputs
            task_handle,
        }
    }
    
    /// Get the number of output channels
    pub fn output_count(&self) -> usize {
        self.output_senders.len()
    }
    
    /// Check if this processor has external sender
    pub fn has_external_sender(&self) -> bool {
        self.external_sender.is_some()
    }
    
    /// Get the external sender if available
    pub fn external_sender(&self) -> Option<&broadcast::Sender<StreamData>> {
        self.external_sender.as_ref()
    }
    
    /// Get the input receiver
    pub fn input_receiver(&self) -> &broadcast::Receiver<StreamData> {
        &self.input_receiver
    }
    
    /// Get mutable input receiver (for consuming)
    pub fn input_receiver_mut(&mut self) -> &mut broadcast::Receiver<StreamData> {
        &mut self.input_receiver
    }
    
    /// Get the output senders
    pub fn output_senders(&self) -> &[broadcast::Sender<StreamData>] {
        &self.output_senders
    }
    
    /// Get a specific output sender by index
    pub fn output_sender(&self, index: usize) -> Option<&broadcast::Sender<StreamData>> {
        self.output_senders.get(index)
    }
    
    /// Get all output senders as a vector (for consuming)
    pub fn take_output_senders(self) -> Vec<broadcast::Sender<StreamData>> {
        self.output_senders
    }
    
    /// Get the task handle
    pub fn task_handle(&self) -> &ProcessorHandle {
        &self.task_handle
    }
    
    /// Take the task handle (for consuming)
    pub fn take_task_handle(self) -> ProcessorHandle {
        self.task_handle
    }
    
    /// Send a signal to all output channels
    pub fn broadcast_to_outputs(&self, signal: StreamData) -> Result<(), Vec<broadcast::error::SendError<StreamData>>> {
        let mut errors = Vec::new();
        
        for (_i, sender) in self.output_senders.iter().enumerate() {
            if let Err(e) = sender.send(signal.clone()) {
                errors.push(e);
            }
        }
        
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
    
    /// Send a signal to external channel if available
    pub fn send_to_external(&self, signal: StreamData) -> Result<usize, broadcast::error::SendError<StreamData>> {
        if let Some(sender) = &self.external_sender {
            sender.send(signal)
        } else {
            // No external sender, consider this a success with 0 receivers
            Ok(0)
        }
    }
    
    /// Check if the processor task is still running
    pub fn is_running(&self) -> bool {
        !self.task_handle.is_finished()
    }
    
    /// Wait for the processor task to complete
    pub async fn wait_for_completion(self) -> Result<(), tokio::task::JoinError> {
        self.task_handle.into_join_handle().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinHandle;
    
    fn create_test_processor_view() -> ProcessorView {
        let (external_tx, _) = broadcast::channel(10);
        let (input_tx, input_rx) = broadcast::channel(10);
        let (output_tx, _) = broadcast::channel(10);
        let output_senders = vec![output_tx];
        let handle = ProcessorHandle::new(tokio::spawn(async {}));
        
        ProcessorView::new(
            Some(external_tx),
            input_rx,
            output_senders,
            handle,
        )
    }
    
    #[test]
    fn test_processor_view_creation() {
        let view = create_test_processor_view();
        
        assert!(view.has_external_sender());
        assert_eq!(view.output_count(), 1);
        assert!(view.is_running());
    }
    
    #[test]
    fn test_control_only_view() {
        let (external_tx, _) = broadcast::channel(10);
        let (input_tx, input_rx) = broadcast::channel(10);
        let (output_tx, _) = broadcast::channel(10);
        let output_senders = vec![output_tx];
        let handle = ProcessorHandle::new(tokio::spawn(async {}));
        
        let view = ProcessorView::control_only(
            Some(external_tx),
            input_rx,
            output_senders,
            handle,
        );
        
        assert!(view.has_external_sender());
        assert_eq!(view.output_count(), 1);
    }
    
    #[test]
    fn test_sink_only_view() {
        let (external_tx, _) = broadcast::channel(10);
        let (input_tx, input_rx) = broadcast::channel(10);
        let handle = ProcessorHandle::new(tokio::spawn(async {}));
        
        let view = ProcessorView::sink_only(
            Some(external_tx),
            input_rx,
            handle,
        );
        
        assert!(view.has_external_sender());
        assert_eq!(view.output_count(), 0);
    }
}