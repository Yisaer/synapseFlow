use crate::model::{CollectionError, Column, Message, Tuple};
use datatypes::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// RecordBatch represents a collection of rows stored purely as tuples.
#[derive(Debug)]
pub struct RecordBatch {
    rows: Vec<Tuple>,
}

impl RecordBatch {
    pub fn new(rows: Vec<Tuple>) -> Result<Self, CollectionError> {
        Ok(Self { rows })
    }

    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn rows(&self) -> &[Tuple] {
        &self.rows
    }

    pub fn rows_mut(&mut self) -> &mut [Tuple] {
        &mut self.rows
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn into_rows(self) -> Vec<Tuple> {
        self.rows
    }
}

impl Clone for RecordBatch {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
        }
    }
}

type ColumnValues = Vec<Value>;
type ColumnEntry = (String, ColumnValues);

/// Build rows from simple column tuples `(source, column, values)`.
pub fn rows_from_columns_simple(
    columns: Vec<(String, String, Vec<Value>)>,
) -> Result<Vec<Tuple>, CollectionError> {
    if columns.is_empty() {
        return Ok(Vec::new());
    }

    let expected_len = columns[0].2.len();
    let mut grouped: HashMap<String, Vec<ColumnEntry>> = HashMap::new();

    for (source, name, values) in columns {
        if values.len() != expected_len {
            return Err(CollectionError::Other(format!(
                "Column {} has {} rows, expected {}",
                name,
                values.len(),
                expected_len
            )));
        }
        grouped
            .entry(Arc::<str>::from(source).to_string())
            .or_default()
            .push((name, values));
    }

    let mut rows = Vec::with_capacity(expected_len);
    for row_idx in 0..expected_len {
        let messages = grouped
            .iter()
            .map(|(source, cols)| {
                let mut keys = Vec::with_capacity(cols.len());
                let mut values_vec = Vec::with_capacity(cols.len());
                for (col_name, values) in cols {
                    let value = values.get(row_idx).cloned().unwrap_or(Value::Null);
                    keys.push(Arc::<str>::from(col_name.as_str()));
                    values_vec.push(Arc::new(value));
                }
                Arc::new(Message::new(
                    Arc::<str>::from(source.as_str()),
                    keys,
                    values_vec,
                ))
            })
            .collect();
        rows.push(Tuple::new(messages));
    }
    Ok(rows)
}

pub fn batch_from_columns_simple(
    columns: Vec<(String, String, Vec<Value>)>,
) -> Result<RecordBatch, CollectionError> {
    let rows = rows_from_columns_simple(columns)?;
    RecordBatch::new(rows)
}

/// Legacy helper that accepts Column structs. Will be removed once all call sites migrate.
pub fn rows_from_columns(columns: Vec<Column>) -> Result<Vec<Tuple>, CollectionError> {
    let simple = columns
        .into_iter()
        .map(|column| (column.source_name, column.name, column.data))
        .collect();
    rows_from_columns_simple(simple)
}

pub fn batch_from_columns(columns: Vec<Column>) -> Result<RecordBatch, CollectionError> {
    let rows = rows_from_columns(columns)?;
    RecordBatch::new(rows)
}
