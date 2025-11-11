use std::collections::HashMap;
use datatypes::Value;
use crate::model::{CollectionError, Column};

/// RecordBatch represents a collection of rows stored in columnar format
/// 
/// This is a columnar implementation where data is organized by columns
/// rather than by rows, enabling efficient columnar operations and better
/// cache locality for analytical workloads.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    /// Columns of data stored in columnar format
    columns: Vec<Column>,
    /// Mapping from (source_name, column_name) to column index for O(1) lookup
    column_index: HashMap<(String, String), usize>,
    /// Number of rows in this batch
    num_rows: usize,
}

impl RecordBatch {
    /// Create a new RecordBatch from columns
    pub fn new(columns: Vec<Column>) -> Result<Self, CollectionError> {
        let num_rows = if columns.is_empty() {
            0
        } else {
            columns[0].len()
        };
        
        // Validate that all columns have the same number of rows
        for (i, column) in columns.iter().enumerate() {
            if column.len() != num_rows {
                return Err(CollectionError::Other(format!(
                    "Column {} has {} rows, expected {}",
                    i, column.len(), num_rows
                )));
            }
        }
        
        // Build column index map: (source_name, name) -> index
        let mut column_index = HashMap::new();
        for (i, column) in columns.iter().enumerate() {
            let key = (column.source_name.clone(), column.name.clone());
            if column_index.insert(key, i).is_some() {
                return Err(CollectionError::Other(format!(
                    "Duplicate column: {}.{}",
                    column.source_name, column.name
                )));
            }
        }
        
        Ok(Self {
            columns,
            column_index,
            num_rows,
        })
    }
    
    /// Create an empty RecordBatch
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            column_index: HashMap::new(),
            num_rows: 0,
        }
    }
    
    
    /// Get the number of rows
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
    
    /// Get the number of columns
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
    
    /// Get a column by index
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }
    
    /// Get a column by source name and column name (O(1) lookup)
    pub fn column_by_name(&self, source_name: &str, name: &str) -> Option<&Column> {
        let key = (source_name.to_string(), name.to_string());
        self.column_index.get(&key).and_then(|&idx| self.columns.get(idx))
    }
    
    /// Get all columns
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
    
    /// Get a value at the given row and column index
    pub fn get_value(&self, row_index: usize, col_index: usize) -> Option<&Value> {
        if row_index >= self.num_rows {
            return None;
        }
        self.columns.get(col_index)?.get(row_index)
    }
    
    /// Get a value at the given row index and column name
    pub fn get_value_by_name(&self, row_index: usize, source_name: &str, col_name: &str) -> Option<&Value> {
        if row_index >= self.num_rows {
            return None;
        }
        let col = self.column_by_name(source_name, col_name)?;
        col.get(row_index)
    }
    
    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }
    
    /// Create a new RecordBatch with only the specified column indices
    pub fn project(&self, column_indices: &[usize]) -> Result<Self, CollectionError> {
        if column_indices.is_empty() {
            return Err(CollectionError::Other("Cannot project to zero columns".to_string()));
        }
        
        let mut new_columns = Vec::with_capacity(column_indices.len());
        
        for &idx in column_indices {
            if idx >= self.columns.len() {
                return Err(CollectionError::InvalidColumnIndex {
                    index: idx,
                    num_columns: self.columns.len(),
                });
            }
            new_columns.push(self.columns[idx].clone());
        }
        
        Self::new(new_columns)
    }
}
