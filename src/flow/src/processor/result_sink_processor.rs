//! ResultSinkProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and prints it.

use tokio::sync::mpsc;
use crate::processor::{Processor, ProcessorError, StreamData};

/// ResultSinkProcessor - prints received data
///
/// This processor acts as the final destination in the data flow. It:
/// - Receives StreamData from upstream processors
/// - Prints Collection data to stdout
/// - Handles control signals and errors appropriately
pub struct ResultSinkProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving data
    inputs: Vec<mpsc::Receiver<StreamData>>,
    /// Output channels (typically empty for sink, but kept for consistency)
    outputs: Vec<mpsc::Sender<StreamData>>,
}

impl ResultSinkProcessor {
    /// Create a new ResultSinkProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }
    
    /// Print Collection data
    fn print_collection(&self, collection: &dyn crate::model::Collection) {
        println!("[{}] Received Collection: {} rows, {} columns", 
                 self.id, 
                 collection.num_rows(), 
                 collection.num_columns());
        
        // Print column information
        for (idx, column) in collection.columns().iter().enumerate() {
            println!("  Column {}: {} (source: {})", 
                     idx, 
                     column.name, 
                     column.source_name);
        }
        
        // Print sample data (first few rows)
        let num_rows_to_print = std::cmp::min(5, collection.num_rows());
        if num_rows_to_print > 0 {
            println!("  Sample data (first {} rows):", num_rows_to_print);
            for row_idx in 0..num_rows_to_print {
                let mut row_data = Vec::new();
                for column in collection.columns() {
                    if let Some(value) = column.get(row_idx) {
                        row_data.push(format!("{}={:?}", column.name, value));
                    }
                }
                println!("    Row {}: {}", row_idx, row_data.join(", "));
            }
            if collection.num_rows() > num_rows_to_print {
                println!("  ... ({} more rows)", collection.num_rows() - num_rows_to_print);
            }
        }
    }
}

impl Processor for ResultSinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }
    
    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut inputs = std::mem::take(&mut self.inputs);
        
        tokio::spawn(async move {
            loop {
                let mut all_closed = true;
                let mut received_any = false;
                
                // Check all input channels
                for (idx, input) in inputs.iter_mut().enumerate() {
                    match input.try_recv() {
                        Ok(data) => {
                            all_closed = false;
                            received_any = true;
                            
                            match &data {
                                StreamData::Collection(collection) => {
                                    println!("[{}] Input {}: {}", id, idx, data.description());
                                    // Print collection details
                                    println!("[{}] Collection details:", id);
                                    println!("  Rows: {}", collection.num_rows());
                                    println!("  Columns: {}", collection.num_columns());
                                    for (col_idx, column) in collection.columns().iter().enumerate() {
                                        println!("    Column {}: {} from {}", 
                                                 col_idx, 
                                                 column.name, 
                                                 column.source_name);
                                    }
                                }
                                StreamData::Control(control) => {
                                    println!("[{}] Input {}: Control signal: {:?}", id, idx, control);
                                    if matches!(control, crate::processor::ControlSignal::StreamEnd) {
                                        // Stream ended, but continue to check other inputs
                                        println!("[{}] StreamEnd received from input {}", id, idx);
                                    }
                                }
                                StreamData::Error(error) => {
                                    eprintln!("[{}] Input {}: Error: {}", id, idx, error);
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            all_closed = false;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            // Channel disconnected
                            println!("[{}] Input {} disconnected", id, idx);
                        }
                    }
                }
                
                // If all channels are closed and we haven't received anything, exit
                if all_closed && !received_any {
                    println!("[{}] All inputs closed, exiting", id);
                    return Ok(());
                }
                
                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        })
    }
    
    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }
    
    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }
    
    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}
