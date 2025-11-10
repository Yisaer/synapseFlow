use datatypes::Value;
use std::any::Any;

/// Row trait defines the interface for row-like data access
/// 
/// This trait provides methods to access values by different indexing strategies:
/// - Name-based access  
/// - Source+column name access
pub trait Row: Send + Sync + Any {
    /// Get value by column name
    fn get_by_name(&self, name: &str) -> Option<&Value>;
    
    /// Get value by source name and column name
    fn get_by_source_column(&self, source_name: &str, column_name: &str) -> Option<&Value>;
    
    /// Get the number of elements in the row
    fn len(&self) -> usize;
    
    /// Check if the row is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}