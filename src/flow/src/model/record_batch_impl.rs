use super::RecordBatch;
use crate::expr::ScalarExpr;
use crate::model::{Collection, CollectionError, Column, Tuple};
use crate::planner::physical::PhysicalProjectField;
use datatypes::Value;

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
        Ok(Box::new(RecordBatch::from_rows(new_rows)))
    }

    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError> {
        if indices.is_empty() {
            let empty_columns: Vec<Column> = self
                .column_pairs()
                .into_iter()
                .map(|(source, name)| Column::new(source, name, Vec::new()))
                .collect();
            let new_batch = RecordBatch::new(empty_columns)?;
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
        Ok(Box::new(RecordBatch::from_rows(new_rows)))
    }

    fn apply_projection(
        &self,
        fields: &[PhysicalProjectField],
    ) -> Result<Box<dyn Collection>, CollectionError> {
        let mut projected_rows = Vec::with_capacity(self.num_rows());
        for tuple in self.rows() {
            let mut projected_columns = Vec::new();
            let mut projected_values = Vec::new();
            for field in fields {
                if let ScalarExpr::Wildcard { source_name } = &field.compiled_expr {
                    let selected: Vec<_> = tuple
                        .columns
                        .iter()
                        .zip(tuple.values.iter())
                        .filter(|((src, _), _)| match source_name {
                            Some(prefix) => src == prefix,
                            None => true,
                        })
                        .collect();

                    if selected.is_empty() && source_name.is_some() {
                        let qualifier = source_name
                            .as_ref()
                            .map(|prefix| format!("{}.*", prefix))
                            .unwrap_or_else(|| "*".to_string());
                        return Err(CollectionError::Other(format!(
                            "Failed to evaluate expression for field '{}': Column not found: {}",
                            field.field_name, qualifier
                        )));
                    }

                    for ((src, name), value) in selected {
                        projected_columns.push((src.clone(), name.clone()));
                        projected_values.push(value.clone());
                    }
                    continue;
                }

                let value = field
                    .compiled_expr
                    .eval_with_tuple(tuple)
                    .map_err(|eval_error| {
                        CollectionError::Other(format!(
                            "Failed to evaluate expression for field '{}': {}",
                            field.field_name, eval_error
                        ))
                    })?;

                projected_columns.push(("".to_string(), field.field_name.clone()));
                projected_values.push(value);
            }

            projected_rows.push(Tuple::new(
                tuple.source_name.clone(),
                projected_columns,
                projected_values,
            ));
        }

        Ok(Box::new(RecordBatch::from_rows(projected_rows)))
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
            let empty_columns: Vec<Column> = self
                .column_pairs()
                .into_iter()
                .map(|(source, name)| Column::new(source, name, Vec::new()))
                .collect();
            let empty_batch = RecordBatch::new(empty_columns)?;
            return Ok(Box::new(empty_batch));
        }

        Ok(Box::new(RecordBatch::from_rows(selected_rows)))
    }

    fn clone_box(&self) -> Box<dyn Collection> {
        Box::new(self.clone())
    }
}
