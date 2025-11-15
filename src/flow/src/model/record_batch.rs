use crate::model::{CollectionError, Column, Tuple};
use datatypes::Value;
use std::collections::HashSet;

/// RecordBatch represents a collection of rows stored purely as tuples.
#[derive(Debug)]
pub struct RecordBatch {
    rows: Vec<Tuple>,
}

impl RecordBatch {
    /// Create a new RecordBatch from columnar data.
    pub fn new(columns: Vec<Column>) -> Result<Self, CollectionError> {
        let rows = columns_to_rows(&columns)?;
        Ok(Self { rows })
    }

    /// Create a RecordBatch from already materialized rows.
    pub fn from_rows(rows: Vec<Tuple>) -> Self {
        Self { rows }
    }

    /// Create an empty RecordBatch.
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    /// Access the stored rows.
    pub fn rows(&self) -> &[Tuple] {
        &self.rows
    }

    /// Number of rows.
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Number of logical columns derived from the rows.
    pub fn num_columns(&self) -> usize {
        self.column_pairs().len()
    }

    /// Return a snapshot of the logical columns.
    pub fn columns(&self) -> Vec<Column> {
        let pairs = self.column_pairs();
        build_columns_from_rows(&self.rows, &pairs)
    }

    /// Get a column by index as a snapshot.
    pub fn column(&self, index: usize) -> Option<Column> {
        self.columns().into_iter().nth(index)
    }

    /// Get a column by source/column names.
    pub fn column_by_name(&self, source_name: &str, column_name: &str) -> Option<Column> {
        self.columns()
            .into_iter()
            .find(|col| col.source_name() == source_name && col.name() == column_name)
    }

    /// Get a single value by row/column index.
    pub fn get_value(&self, row_index: usize, col_index: usize) -> Option<&Value> {
        if row_index >= self.rows.len() {
            return None;
        }
        let pairs = self.column_pairs();
        let (source, name) = pairs.get(col_index)?;
        self.rows[row_index].value_by_name(source, name)
    }

    /// Rename all tuple source identifiers.
    pub fn with_source_name(&self, source_name: &str) -> Self {
        let mut rows = self.rows.clone();
        for tuple in rows.iter_mut() {
            tuple.source_name = source_name.to_string();
            for (src, _) in tuple.columns.iter_mut() {
                *src = source_name.to_string();
            }
        }
        Self { rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn column_pairs(&self) -> Vec<(String, String)> {
        collect_column_pairs(&self.rows)
    }
}

impl Clone for RecordBatch {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
        }
    }
}

impl PartialEq for RecordBatch {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

fn columns_to_rows(columns: &[Column]) -> Result<Vec<Tuple>, CollectionError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen = HashSet::new();
    for column in columns {
        let key = (column.source_name.clone(), column.name.clone());
        if !seen.insert(key.clone()) {
            return Err(CollectionError::Other(format!(
                "Duplicate column: {}.{}",
                key.0, key.1
            )));
        }
    }

    let num_rows = columns[0].len();
    for (i, column) in columns.iter().enumerate() {
        if column.len() != num_rows {
            return Err(CollectionError::Other(format!(
                "Column {} has {} rows, expected {}",
                i,
                column.len(),
                num_rows
            )));
        }
    }

    let column_pairs: Vec<(String, String)> = columns
        .iter()
        .map(|column| (column.source_name.clone(), column.name.clone()))
        .collect();

    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(columns.len());
        for column in columns {
            values.push(column.get(row_idx).cloned().unwrap_or(Value::Null));
        }
        rows.push(Tuple::new(String::new(), column_pairs.clone(), values));
    }
    Ok(rows)
}

pub fn collect_column_pairs(rows: &[Tuple]) -> Vec<(String, String)> {
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        for pair in &row.columns {
            if seen.insert(pair.clone()) {
                order.push(pair.clone());
            }
        }
    }
    order
}

pub fn build_columns_from_rows(rows: &[Tuple], column_pairs: &[(String, String)]) -> Vec<Column> {
    let effective_pairs = if column_pairs.is_empty() {
        collect_column_pairs(rows)
    } else {
        column_pairs.to_vec()
    };

    let mut column_values: Vec<Vec<Value>> = effective_pairs
        .iter()
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();

    for row in rows {
        for (idx, (source, name)) in effective_pairs.iter().enumerate() {
            if let Some(value) = row.value_by_name(source, name) {
                column_values[idx].push(value.clone());
            } else {
                column_values[idx].push(Value::Null);
            }
        }
    }

    effective_pairs
        .into_iter()
        .zip(column_values.into_iter())
        .map(|((source, name), values)| Column::new(source, name, values))
        .collect()
}
