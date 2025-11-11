use crate::model::{Collection, CollectionError, Column};
use super::RecordBatch;
use datatypes::Value;

impl Collection for RecordBatch {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
    
    fn num_columns(&self) -> usize {
        self.num_columns()
    }
    
    fn column(&self, index: usize) -> Option<&Column> {
        self.column(index)
    }
    
    fn column_by_name(&self, source_name: &str, name: &str) -> Option<&Column> {
        self.column_by_name(source_name, name)
    }
    
    fn slice(&self, start: usize, end: usize) -> Result<Box<dyn Collection>, CollectionError> {
        if start > end || end > self.num_rows() {
            return Err(CollectionError::InvalidSliceRange {
                start,
                end,
                len: self.num_rows(),
            });
        }
        
        let mut new_columns = Vec::with_capacity(self.columns().len());
        
        for column in self.columns() {
            let new_data = column.values()[start..end].to_vec();
            new_columns.push(Column::new(
                column.name.clone(),
                column.source_name.clone(),
                new_data
            ));
        }
        
        let new_batch = RecordBatch::new(new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if indices.is_empty() {
            return Ok(Box::new(RecordBatch::empty()));
        }
        
        // Validate all indices
        for &idx in indices {
            if idx >= self.num_rows() {
                return Err(CollectionError::IndexOutOfBounds {
                    index: idx,
                    len: self.num_rows(),
                });
            }
        }
        
        let mut new_columns = Vec::with_capacity(self.columns().len());
        
        for column in self.columns() {
            let mut new_data = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(value) = column.get(idx) {
                    new_data.push(value.clone());
                } else {
                    new_data.push(Value::Null);
                }
            }
            new_columns.push(Column::new(
                column.name.clone(),
                column.source_name.clone(),
                new_data
            ));
        }
        
        let new_batch = RecordBatch::new(new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn columns(&self) -> &[Column] {
        self.columns()
    }
    
    fn project(&self, column_indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if column_indices.is_empty() {
            return Ok(Box::new(RecordBatch::empty()));
        }
        
        // Validate all indices
        for &idx in column_indices {
            if idx >= self.num_columns() {
                return Err(CollectionError::IndexOutOfBounds {
                    index: idx,
                    len: self.num_columns(),
                });
            }
        }
        
        let mut new_columns = Vec::with_capacity(column_indices.len());
        for &idx in column_indices {
            if let Some(col) = self.column(idx) {
                new_columns.push(col.clone());
            }
        }
        
        let new_batch = RecordBatch::new(new_columns)?;
        Ok(Box::new(new_batch))
    }
}