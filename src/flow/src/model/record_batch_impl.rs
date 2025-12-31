use super::RecordBatch;
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::model::{Collection, CollectionError, Tuple};
use crate::planner::physical::PhysicalProjectField;
use datatypes::Value;
use std::sync::Arc;

#[allow(clippy::type_complexity)]
enum PartialMessages {
    Single {
        keys: Vec<Arc<str>>,
        values: Vec<Arc<Value>>,
    },
    Buckets {
        buckets: Vec<(Vec<Arc<str>>, Vec<Arc<Value>>)>,
    },
}

impl PartialMessages {
    fn push(
        &mut self,
        message_index: usize,
        key: Arc<str>,
        value: Arc<Value>,
    ) -> Result<(), CollectionError> {
        match self {
            PartialMessages::Single { keys, values } => {
                let _ = message_index;
                keys.push(key);
                values.push(value);
                Ok(())
            }
            PartialMessages::Buckets { buckets } => {
                if message_index >= buckets.len() {
                    return Err(CollectionError::Other(format!(
                        "Failed to apply projection: message index {} out of bounds (len={})",
                        message_index,
                        buckets.len()
                    )));
                }
                let entry = &mut buckets[message_index];
                entry.0.push(key);
                entry.1.push(value);
                Ok(())
            }
        }
    }

    fn append_to_messages(
        self,
        tuple: &Tuple,
        projected_messages: &mut Vec<Arc<crate::model::Message>>,
    ) {
        match self {
            PartialMessages::Single { keys, values } => {
                if !keys.is_empty() {
                    let source = tuple
                        .messages()
                        .first()
                        .map(|message| message.source())
                        .unwrap_or_default();
                    let msg = Arc::new(crate::model::Message::new(source, keys, values));
                    projected_messages.push(msg);
                }
            }
            PartialMessages::Buckets { buckets } => {
                for (idx, (keys, values)) in buckets.into_iter().enumerate() {
                    if keys.is_empty() {
                        continue;
                    }
                    let source = tuple
                        .messages()
                        .get(idx)
                        .map(|message| message.source())
                        .unwrap_or_default();
                    let msg = Arc::new(crate::model::Message::new(source, keys, values));
                    projected_messages.push(msg);
                }
            }
        }
    }
}

fn message_index_for_source(tuple: &Tuple, source: &str) -> Option<usize> {
    if source.is_empty() && tuple.messages().len() == 1 {
        return Some(0);
    }

    tuple
        .messages()
        .iter()
        .position(|message| message.source() == source)
}

fn apply_projection_for_tuple(
    tuple: &Tuple,
    fields: &[PhysicalProjectField],
    mut partial_messages: PartialMessages,
) -> Result<Tuple, CollectionError> {
    let mut projected_tuple = Tuple::with_timestamp(Tuple::empty_messages(), tuple.timestamp);
    let mut projected_messages = Vec::with_capacity(tuple.messages().len().saturating_add(1));

    for field in fields {
        match &field.compiled_expr {
            ScalarExpr::Wildcard { source_name } => match source_name {
                Some(prefix) => {
                    if let Some(message) = tuple.message_by_source(prefix) {
                        projected_messages.push(message.clone());
                    } else {
                        let qualifier = format!("{}.*", prefix);
                        return Err(CollectionError::Other(format!(
                            "Failed to evaluate expression for field '{}': Column not found: {}",
                            field.field_name, qualifier
                        )));
                    }
                }
                None => {
                    if tuple.messages().is_empty() {
                        continue;
                    }
                    for message in tuple.messages() {
                        projected_messages.push(message.clone());
                    }
                }
            },
            ScalarExpr::Column(ColumnRef::ByIndex {
                source_name,
                column_index,
            }) => {
                let message_index =
                    message_index_for_source(tuple, source_name).ok_or_else(|| {
                        CollectionError::Other(format!(
                            "Failed to evaluate expression for field '{}': Column not found: {}",
                            field.field_name, source_name
                        ))
                    })?;
                let message = &tuple.messages()[message_index];
                let (col_name, value) = message.entry_by_index(*column_index).ok_or_else(|| {
                    CollectionError::Other(format!(
                        "Failed to evaluate expression for field '{}': Column not found: {}#{}",
                        field.field_name, source_name, column_index
                    ))
                })?;

                partial_messages.push(message_index, col_name.clone(), value.clone())?;
            }
            _ => {
                let value = field
                    .compiled_expr
                    .eval_with_tuple(tuple)
                    .map_err(|eval_error| {
                        CollectionError::Other(format!(
                            "Failed to evaluate expression for field '{}': {}",
                            field.field_name, eval_error
                        ))
                    })?;

                projected_tuple.add_affiliate_column(Arc::new(field.field_name.clone()), value);
            }
        }
    }

    partial_messages.append_to_messages(tuple, &mut projected_messages);
    projected_tuple.messages = Arc::from(projected_messages);

    Ok(projected_tuple)
}

impl Collection for RecordBatch {
    fn num_rows(&self) -> usize {
        self.num_rows()
    }

    fn rows(&self) -> &[Tuple] {
        self.rows()
    }

    fn slice(&self, start: usize, end: usize) -> Result<Box<dyn Collection>, CollectionError> {
        if start > end || end > self.num_rows() {
            return Err(CollectionError::InvalidSliceRange {
                start,
                end,
                len: self.num_rows(),
            });
        }
        let new_rows = self.rows()[start..end].to_vec();
        Ok(Box::new(RecordBatch::new(new_rows)?))
    }

    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if indices.is_empty() {
            let new_batch = RecordBatch::new(Vec::new())?;
            return Ok(Box::new(new_batch));
        }

        for &idx in indices {
            if idx >= self.num_rows() {
                return Err(CollectionError::IndexOutOfBounds {
                    index: idx,
                    len: self.num_rows(),
                });
            }
        }

        let mut new_rows = Vec::with_capacity(indices.len());
        for &idx in indices {
            new_rows.push(self.rows()[idx].clone());
        }
        Ok(Box::new(RecordBatch::new(new_rows)?))
    }

    fn apply_projection(
        &self,
        fields: &[PhysicalProjectField],
    ) -> Result<Box<dyn Collection>, CollectionError> {
        let by_index_count = fields
            .iter()
            .filter(|field| {
                matches!(
                    &field.compiled_expr,
                    ScalarExpr::Column(ColumnRef::ByIndex { .. })
                )
            })
            .count();

        let mut projected_rows = Vec::with_capacity(self.num_rows());
        for tuple in self.rows() {
            let partial_messages = if tuple.messages().len() == 1 {
                PartialMessages::Single {
                    keys: Vec::with_capacity(by_index_count),
                    values: Vec::with_capacity(by_index_count),
                }
            } else {
                PartialMessages::Buckets {
                    buckets: vec![(Vec::new(), Vec::new()); tuple.messages().len()],
                }
            };

            projected_rows.push(apply_projection_for_tuple(tuple, fields, partial_messages)?);
        }

        Ok(Box::new(RecordBatch::new(projected_rows)?))
    }

    fn apply_filter(
        &self,
        filter_expr: &ScalarExpr,
    ) -> Result<Box<dyn Collection>, CollectionError> {
        let filter_results = filter_expr
            .eval_with_collection(self)
            .map_err(|eval_error| CollectionError::FilterError {
                message: format!("Failed to evaluate filter expression: {}", eval_error),
            })?;

        let mut selected_rows = Vec::new();
        for (row, result) in self.rows().iter().zip(filter_results.iter()) {
            match result {
                Value::Bool(true) => selected_rows.push(row.clone()),
                Value::Bool(false) => {}
                _ => {
                    return Err(CollectionError::FilterError {
                        message: format!(
                            "Filter expression must return boolean values, got {:?}",
                            result
                        ),
                    })
                }
            }
        }

        if selected_rows.is_empty() {
            let empty_batch = RecordBatch::new(Vec::new())?;
            return Ok(Box::new(empty_batch));
        }

        Ok(Box::new(RecordBatch::new(selected_rows)?))
    }

    fn clone_box(&self) -> Box<dyn Collection> {
        Box::new(self.clone())
    }

    fn into_rows(self: Box<Self>) -> Result<Vec<Tuple>, CollectionError> {
        let batch: RecordBatch = *self;
        Ok(batch.into_rows())
    }
}
