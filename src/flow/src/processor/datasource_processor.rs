//! DataSource processor - corresponds to PhysicalDataSource
//! 
//! This processor acts as a source of data in the stream processing pipeline.
//! Currently generates empty data to establish the data flow pipeline.
//! 
//! Design follows rstream's Executor pattern with tokio::spawn and dedicated data generation routine.

use tokio::sync::broadcast;
use std::sync::Arc;
use crate::planner::physical::PhysicalPlan;
use crate::processor::{StreamProcessor, ProcessorView, ProcessorHandle, utils, StreamData};

/// DataSource processor that corresponds to PhysicalDataSource
/// 
/// This is typically the starting point of a stream processing pipeline.
/// Currently just generates empty data to establish data flow.
pub struct DataSourceProcessor {
    /// The physical plan this processor corresponds to
    physical_plan: Arc<dyn PhysicalPlan>,
    /// Input channels (should be empty for data source)
    input_receivers: Vec<broadcast::Receiver<Result<StreamData, String>>>,
    /// Number of downstream processors this will broadcast to
    downstream_count: usize,
}

impl DataSourceProcessor {
    /// Create a new DataSourceProcessor with specified downstream count
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
    
    /// Generate initial data (currently empty for pipeline establishment)
    async fn generate_initial_data(&self) -> Result<Vec<Box<dyn crate::model::Collection>>, String> {
        // For now, return empty data to establish pipeline
        // In a real implementation, this would:
        // 1. Connect to the actual data source (Kafka, file, etc.)
        // 2. Read and parse data
        // 3. Return StreamData items
        Ok(vec![])
    }
}

impl StreamProcessor for DataSourceProcessor {
    fn start(&self) -> ProcessorView {
        // Create channels using utils
        let (result_tx, result_rx) = utils::create_result_channel(self.downstream_count);
        let (stop_tx, stop_rx) = utils::create_stop_channel();
        
        // Spawn the data source routine - currently just generates empty data
        let routine = self.create_datasource_routine(result_tx, stop_rx);
        
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
impl DataSourceProcessor {
    /// Create data source routine that runs in tokio task - currently just generates empty data
    fn create_datasource_routine(
        &self,
        result_tx: broadcast::Sender<Result<StreamData, String>>,
        mut stop_rx: broadcast::Receiver<()>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let downstream_count = self.downstream_count;
        
        async move {
            println!("DataSourceProcessor: Starting data source routine for {} downstream processors", downstream_count);
            
            // Send stream start signal
            if result_tx.send(Ok(StreamData::stream_start())).is_err() {
                println!("DataSourceProcessor: Failed to send start signal");
                return;
            }
            
            // Generate initial data (currently empty)
            match Self::static_generate_initial_data().await {
                Ok(data_items) => {
                    for data in data_items {
                        // Check stop signal before sending each item
                        if stop_rx.try_recv().is_ok() {
                            println!("DataSourceProcessor: Received stop signal, shutting down");
                            break;
                        }
                        
                        if result_tx.send(Ok(StreamData::collection(data))).is_err() {
                            // All downstream receivers dropped, stop processing
                            println!("DataSourceProcessor: All downstream receivers dropped, stopping");
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("DataSourceProcessor: Error generating data: {}", e);
                    // Send error to downstream
                    let _ = result_tx.send(Err(e));
                }
            }
            
            // Send stream end signal
            if result_tx.send(Ok(StreamData::stream_end())).is_err() {
                println!("DataSourceProcessor: Failed to send end signal");
            }
            
            println!("DataSourceProcessor: Data source routine completed");
        }
    }
    
    /// Static version of generate_initial_data for use in async routine
    async fn static_generate_initial_data() -> Result<Vec<Box<dyn crate::model::Collection>>, String> {
        // For now, return empty data to establish pipeline
        // In a real implementation, this would generate actual data
        Ok(vec![])
    }
}