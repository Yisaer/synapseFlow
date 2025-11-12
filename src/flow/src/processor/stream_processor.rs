//! Stream processor trait and utilities

use async_trait::async_trait;
use tokio::sync::mpsc;
use crate::processor::StreamData;

/// Stream processor trait - all processors implement this
#[async_trait]
pub trait StreamProcessor: Send + Sync {
    /// Get processor name for debugging
    fn name(&self) -> &str;
    
    /// Get number of input channels
    fn input_count(&self) -> usize;
    
    /// Get number of output channels
    fn output_count(&self) -> usize;
    
    /// Create input channels for this processor
    fn create_input_channels(&self, count: usize) -> Vec<mpsc::Sender<StreamData>> {
        let mut senders = Vec::new();
        for _ in 0..count {
            let (sender, _receiver) = mpsc::channel(1024);
            senders.push(sender);
        }
        senders
    }
    
    /// Create output channels for this processor
    fn create_output_channels(&self, count: usize) -> Vec<mpsc::Receiver<StreamData>> {
        let mut receivers = Vec::new();
        for _ in 0..count {
            let (_sender, receiver) = mpsc::channel(1024);
            receivers.push(receiver);
        }
        receivers
    }
    
    /// Start the processor task
    async fn start(&self, input_receivers: Vec<mpsc::Receiver<StreamData>>) -> ProcessorHandle;
}

/// Handle to a running processor
pub struct ProcessorHandle {
    /// Join handle for the processor task
    pub task_handle: tokio::task::JoinHandle<()>,
    /// Output channel receivers
    pub output_receivers: Vec<mpsc::Receiver<StreamData>>,
}

/// Processor utilities
pub mod utils {
    use super::*;
    
    /// Create connected channel pair
    pub fn create_channel() -> (mpsc::Sender<StreamData>, mpsc::Receiver<StreamData>) {
        mpsc::channel(1024)
    }
    
    /// Create multiple connected channel pairs
    pub fn create_channels(count: usize) -> (Vec<mpsc::Sender<StreamData>>, Vec<mpsc::Receiver<StreamData>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        
        for _ in 0..count {
            let (sender, receiver) = mpsc::channel(1024);
            senders.push(sender);
            receivers.push(receiver);
        }
        
        (senders, receivers)
    }
}