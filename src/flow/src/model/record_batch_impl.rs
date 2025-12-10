use super::RecordBatch;
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::model::{Collection, CollectionError, Tuple};
use crate::planner::physical::PhysicalProjectField;
use datatypes::Value;
use std::collections::HashMap;
use std::sync::Arc;

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
        let mut projected_rows = Vec::with_capacity(self.num_rows());
        for tuple in self.rows() {
            let mut projected_tuple = Tuple::new(Vec::new());
            #[allow(clippy::type_complexity)]
            let mut partial_messages: HashMap<&str, (Vec<Arc<str>>, Vec<Arc<Value>>)> =
                HashMap::new();
            let mut projected_messages = Vec::new();

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
                        let message = tuple.message_by_source(source_name).ok_or_else(|| {
                            CollectionError::Other(format!(
                                "Failed to evaluate expression for field '{}': Column not found: {}",
                                field.field_name, source_name
                            ))
                        })?;
                        let (col_name, value) = message.entry_by_index(*column_index).ok_or_else(|| {
                            CollectionError::Other(format!(
                                "Failed to evaluate expression for field '{}': Column not found: {}#{}",
                                field.field_name, source_name, column_index
                            ))
                        })?;

                        let entry = if let Some(existing) =
                            partial_messages.get_mut(source_name.as_str())
                        {
                            existing
                        } else {
                            partial_messages
                                .entry(source_name.as_str())
                                .or_insert_with(|| (Vec::new(), Vec::new()))
                        };
                        entry.0.push(col_name.clone());
                        entry.1.push(value.clone());
                    }
                    _ => {
                        let value =
                            field
                                .compiled_expr
                                .eval_with_tuple(tuple)
                                .map_err(|eval_error| {
                                    CollectionError::Other(format!(
                                        "Failed to evaluate expression for field '{}': {}",
                                        field.field_name, eval_error
                                    ))
                                })?;

                        projected_tuple
                            .add_affiliate_column(Arc::new(field.field_name.clone()), value);
                    }
                }
            }

            for (source, (keys, values)) in partial_messages {
                let msg = Arc::new(crate::model::Message::new(source, keys, values));
                projected_messages.push(msg);
            }

            projected_tuple.messages = projected_messages;

            projected_rows.push(projected_tuple);
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
}
