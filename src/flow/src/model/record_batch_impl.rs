use crate::model::{Collection, CollectionError, Column};
use super::RecordBatch;

impl Collection for RecordBatch {
    fn schema(&self) -> &datatypes::Schema {
        self.schema()
    }
    
    fn num_rows(&self) -> usize {
        self.num_rows()
    }
    
    fn column(&self, index: usize) -> Option<&Column> {
        self.column(index)
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
                column.name().to_string(),
                column.data_type().clone(),
                new_data
            ));
        }
        
        let new_batch = RecordBatch::new(self.schema().clone(), new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if indices.is_empty() {
            return Ok(Box::new(RecordBatch::empty(self.schema().clone())));
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
                    new_data.push(datatypes::Value::Null);
                }
            }
            
            new_columns.push(Column::new(
                column.name().to_string(),
                column.data_type().clone(),
                new_data
            ));
        }
        
        let new_batch = RecordBatch::new(self.schema().clone(), new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn row(&self, index: usize) -> Option<Box<dyn crate::model::Row>> {
        if index >= self.num_rows() {
            return None;
        }
        
        // Create a temporary row from column data
        let mut row_data = std::collections::HashMap::new();
        for (i, col_schema) in self.schema().column_schemas().iter().enumerate() {
            if let Some(value) = self.get_value(index, i) {
                row_data.insert(
                    (col_schema.source_name().to_string(), col_schema.name.clone()),
                    value.clone()
                );
            }
        }
        
        Some(Box::new(crate::model::tuple::Tuple::new(row_data)))
    }
    
    fn filter_boxed(&self, predicate: Box<dyn Fn(&dyn crate::model::Row) -> bool + Send + Sync>) -> Result<Box<dyn Collection>, CollectionError> {
        let mut selected_indices = Vec::new();
        
        for i in 0..self.num_rows() {
            if let Some(row) = self.row(i) {
                if predicate(row.as_ref()) {
                    selected_indices.push(i);
                }
            }
        }
        
        self.take(&selected_indices)
    }
    
    fn project(&self, column_indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if column_indices.is_empty() {
            return Err(CollectionError::Other("Cannot project to zero columns".to_string()));
        }
        
        let mut new_columns = Vec::with_capacity(column_indices.len());
        let mut new_schema_columns = Vec::with_capacity(column_indices.len());
        
        for &idx in column_indices {
            if idx >= self.columns().len() {
                return Err(CollectionError::InvalidColumnIndex {
                    index: idx,
                    num_columns: self.columns().len(),
                });
            }
            new_columns.push(self.columns()[idx].clone());
            new_schema_columns.push(self.schema().column_schemas()[idx].clone());
        }
        
        let new_schema = datatypes::Schema::new(new_schema_columns);
        let new_batch = RecordBatch::new(new_schema, new_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn columns(&self) -> &[Column] {
        self.columns()
    }
}