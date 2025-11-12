use crate::model::{Collection, CollectionError, Column};
use super::RecordBatch;
use datatypes::Value;
use crate::planner::physical::PhysicalProjectField;
use crate::expr::datafusion_func::DataFusionEvaluator;

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
    
    fn apply_projection(&self, fields: &[PhysicalProjectField]) -> Result<Box<dyn Collection>, CollectionError> {
        let num_rows = self.num_rows();
        let mut projected_columns = Vec::with_capacity(fields.len());
        
        // Create DataFusion evaluator for expression evaluation
        let evaluator = DataFusionEvaluator::new();
        
        for field in fields {
            // Evaluate the compiled expression using vectorized evaluation
            // If evaluation fails, return the error immediately
            let evaluated_values = field.compiled_expr.eval_vectorized(&evaluator, self)
                .map_err(|eval_error| {
                    CollectionError::Other(format!(
                        "Failed to evaluate expression for field '{}': {}",
                        field.field_name, eval_error
                    ))
                })?;
            
            // Ensure we have the right number of values
            if evaluated_values.len() != num_rows {
                return Err(CollectionError::Other(format!(
                    "Expression evaluation for field '{}' returned {} values, expected {}",
                    field.field_name, evaluated_values.len(), num_rows
                )));
            }
            
            // Create new column with evaluated values
            let column = Column::new(
                field.field_name.clone(),
                "".to_string(),
                evaluated_values,
            );
            projected_columns.push(column);
        }
        
        // Create new RecordBatch with projected columns
        // This leverages the existing columnar structure efficiently
        let new_batch = RecordBatch::new(projected_columns)?;
        Ok(Box::new(new_batch))
    }
    
    fn clone_box(&self) -> Box<dyn Collection> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{Collection, Column, RecordBatch};
    use crate::planner::physical::PhysicalProjectField;
    use sqlparser::ast::{Expr, Value as SqlValue};
    use datatypes::Value;
    #[test]
    fn test_recordbatch_apply_projection_expression_calculation() {
        let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
        let col_b_values = vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)];
        let col_c_values = vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)];
        
        // 使用空字符串作为 source_name，与 convert_identifier_to_column 中的用法一致
        let column_a = Column::new("a".to_string(), "".to_string(), col_a_values);
        let column_b = Column::new("b".to_string(), "".to_string(), col_b_values);
        let column_c = Column::new("c".to_string(), "".to_string(), col_c_values);
        
        let batch = RecordBatch::new(vec![column_a, column_b, column_c]).expect("Failed to create RecordBatch");

        let project_field_a_plus_1 = PhysicalProjectField::from_logical(
            "a_plus_1".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("a"))),
                op: sqlparser::ast::BinaryOperator::Plus,
                right: Box::new(Expr::Value(SqlValue::Number("1".to_string(), false))),
            },
        ).expect("Failed to compile a+1 expression");
        
        let project_field_b_plus_2 = PhysicalProjectField::from_logical(
            "b_plus_2".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("b"))),
                op: sqlparser::ast::BinaryOperator::Plus,
                right: Box::new(Expr::Value(SqlValue::Number("2".to_string(), false))),
            },
        ).expect("Failed to compile b+2 expression");
        

        
        let fields = vec![project_field_a_plus_1, project_field_b_plus_2];

        let result = batch.apply_projection(&fields);
        if let Err(ref e) = result {
            println!("apply_projection failed: {:?}", e);
        }
        assert!(result.is_ok(), "apply_projection should succeed");
        
        let projected_collection = result.unwrap();

        assert_eq!(projected_collection.num_rows(), 3, "Should maintain row count");
        assert_eq!(projected_collection.num_columns(), 2, "Should have 2 projected columns");

        let col1 = projected_collection.column(0).expect("Should have first column");
        let col2 = projected_collection.column(1).expect("Should have second column");

        assert_eq!(col1.name, "a_plus_1");
        assert_eq!(col2.name, "b_plus_2");

        assert_eq!(col1.values(),vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],"" );
        assert_eq!(col2.values(),vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],"" );
    }
}