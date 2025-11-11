use datatypes::{Schema, Value};
use crate::model::{CollectionError, Column, Row};
use crate::model::tuple::Tuple;

/// RecordBatch represents a collection of rows stored in columnar format
/// 
/// This is a columnar implementation where data is organized by columns
/// rather than by rows, enabling efficient columnar operations and better
/// cache locality for analytical workloads.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    /// Schema describing the structure of the data
    schema: Schema,
    /// Columns of data stored in columnar format
    columns: Vec<Column>,
    /// Number of rows in this batch
    num_rows: usize,
}

impl RecordBatch {
    /// Create a new RecordBatch from schema and columns
    pub fn new(schema: Schema, columns: Vec<Column>) -> Result<Self, CollectionError> {
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
        
        // Validate that column count matches schema
        if columns.len() != schema.column_schemas().len() {
            return Err(CollectionError::Other(format!(
                "Schema has {} columns, but {} columns provided",
                schema.column_schemas().len(),
                columns.len()
            )));
        }
        
        // Validate column names match schema
        for (i, (col, col_schema)) in columns.iter().zip(schema.column_schemas().iter()).enumerate() {
            if col.name != col_schema.name {
                return Err(CollectionError::Other(format!(
                    "Column {} name mismatch: '{}' vs '{}'",
                    i, col.name, col_schema.name
                )));
            }
            // TODO: Validate data types match schema
        }
        
        Ok(Self {
            schema,
            columns,
            num_rows,
        })
    }
    
    /// Create an empty RecordBatch with the given schema
    pub fn empty(schema: Schema) -> Self {
        let columns = schema.column_schemas()
            .iter()
            .map(|col_schema| Column::new(col_schema.name.clone(), col_schema.data_type.clone(), Vec::new()))
            .collect();
            
        Self {
            schema,
            columns,
            num_rows: 0,
        }
    }
    
    /// Create a RecordBatch from rows (convert from row-based to columnar)
    pub fn from_rows(schema: Schema, rows: Vec<Tuple>) -> Result<Self, CollectionError> {
        if rows.is_empty() {
            return Ok(Self::empty(schema));
        }
        
        let num_rows = rows.len();
        let mut columns_data: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); schema.column_schemas().len()];
        
        // Convert each row and collect column data
        for row in rows {
            for (i, col_schema) in schema.column_schemas().iter().enumerate() {
                let value = row.get_by_source_column(col_schema.source_name(), &col_schema.name)
                    .cloned()
                    .unwrap_or(Value::Null);
                columns_data[i].push(value);
            }
        }
        
        // Create columns
        let columns = schema.column_schemas()
            .iter()
            .zip(columns_data)
            .map(|(col_schema, data)| Column::new(col_schema.name.clone(), col_schema.data_type.clone(), data))
            .collect();
            
        Self::new(schema, columns)
    }
    
    /// Get the schema
    pub fn schema(&self) -> &Schema {
        &self.schema
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
    
    /// Get a column by name
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|col| col.name == name)
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
    pub fn get_value_by_name(&self, row_index: usize, col_name: &str) -> Option<&Value> {
        if row_index >= self.num_rows {
            return None;
        }
        let col_index = self.schema.column_schemas()
            .iter()
            .position(|col_schema| col_schema.name == col_name)?;
        self.get_value(row_index, col_index)
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
        let mut new_schema_columns = Vec::with_capacity(column_indices.len());
        
        for &idx in column_indices {
            if idx >= self.columns.len() {
                return Err(CollectionError::InvalidColumnIndex {
                    index: idx,
                    num_columns: self.columns.len(),
                });
            }
            new_columns.push(self.columns[idx].clone());
            new_schema_columns.push(self.schema.column_schemas()[idx].clone());
        }
        
        let new_schema = Schema::new(new_schema_columns);
        Self::new(new_schema, new_columns)
    }
    
    /// Create a new RecordBatch with only the specified column names
    pub fn select_columns(&self, column_names: &[&str]) -> Result<Self, CollectionError> {
        let mut column_indices = Vec::with_capacity(column_names.len());
        
        for &name in column_names {
            let pos = self.schema.column_schemas()
                .iter()
                .position(|col_schema| col_schema.name == name)
                .ok_or_else(|| CollectionError::ColumnNotFound { name: name.to_string() })?;
            column_indices.push(pos);
        }
        
        self.project(&column_indices)
    }
}

/// Row view for a specific row in the RecordBatch
#[derive(Debug)]
pub struct RecordBatchRow {
    // Store the actual row data instead of references to avoid lifetime issues
    row_data: std::collections::HashMap<(String, String), Value>,
    num_columns: usize,
}

impl RecordBatchRow {
    pub fn new(batch: &RecordBatch, row_index: usize) -> Option<Self> {
        if row_index >= batch.num_rows() {
            return None;
        }
        
        let mut row_data = std::collections::HashMap::new();
        
        for (i, col_schema) in batch.schema().column_schemas().iter().enumerate() {
            if let Some(value) = batch.get_value(row_index, i) {
                row_data.insert(
                    (col_schema.source_name().to_string(), col_schema.name.clone()),
                    value.clone()
                );
            }
        }
        
        Some(Self {
            row_data,
            num_columns: batch.num_columns(),
        })
    }
}

impl Row for RecordBatchRow {
    fn get_by_name(&self, name: &str) -> Option<&Value> {
        // Try to find by column name alone (assuming any source)
        for ((_source_name, column_name), value) in &self.row_data {
            if column_name == name {
                return Some(value);
            }
        }
        None
    }
    
    fn get_by_source_column(&self, _source_name: &str, column_name: &str) -> Option<&Value> {
        // For now, ignore source_name and just use column_name
        // This can be enhanced later to handle source-specific lookups
        self.get_by_name(column_name)
    }
    
    fn len(&self) -> usize {
        self.num_columns
    }
}