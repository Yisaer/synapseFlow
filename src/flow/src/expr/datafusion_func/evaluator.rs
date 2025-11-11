//! DataFusion-based expression evaluator for flow tuples

use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue, ToDFSchema};
use datafusion_expr::{Expr, execution_props::ExecutionProps, lit};
use datatypes::{Value, ListValue, ConcreteDatatype};
use std::any::Any;
use std::sync::Arc;

use crate::expr::datafusion_func::adapter::*;
use crate::expr::scalar::ScalarExpr;
use crate::model::{Row, Collection};
use crate::tuple::Tuple;

/// DataFusion-based expression evaluator
pub struct DataFusionEvaluator {
    session_ctx: SessionContext,
    #[allow(dead_code)]
    execution_props: ExecutionProps,
}

impl DataFusionEvaluator {
    /// Create a new DataFusion evaluator
    pub fn new() -> Self {
        Self {
            session_ctx: SessionContext::new(),
            execution_props: ExecutionProps::new(),
        }
    }

    /// Evaluate a ScalarExpr against a Collection using DataFusion (vectorized)
    /// This method handles CallDf expressions with true vectorized evaluation
    pub fn evaluate_expr_vectorized(&self, expr: &ScalarExpr, collection: &dyn Collection) -> DataFusionResult<Vec<Value>> {
        match expr {
            ScalarExpr::CallDf { function_name, args } => {
                // Only handle CallDf expressions - this is the main purpose of DataFusionEvaluator
                self.evaluate_df_function_vectorized(function_name, args, collection)
            }
            _ => {
                // For non-CallDf expressions, we should not handle them here
                Err(DataFusionError::Plan(format!(
                    "DataFusionEvaluator should only handle CallDf expressions, got {:?}", 
                    std::mem::discriminant(expr)
                )))
            }
        }
    }

    /// Legacy method: Evaluate a ScalarExpr against a Tuple using DataFusion (single row)
    /// This method should only handle CallDf expressions, as per greptimedb design
    pub fn evaluate_expr(&self, expr: &ScalarExpr, row: &dyn Row) -> DataFusionResult<Value> {
        match expr {
            ScalarExpr::CallDf { function_name, args } => {
                // Only handle CallDf expressions - this is the main purpose of DataFusionEvaluator
                self.evaluate_df_function_legacy(function_name, args, row)
            }
            _ => {
                // For non-CallDf expressions, we should not handle them here
                // This should ideally be an error, but for compatibility we'll delegate to regular eval
                // In a strict greptimedb design, this would return an error
                Err(DataFusionError::Plan(format!(
                    "DataFusionEvaluator should only handle CallDf expressions, got {:?}", 
                    std::mem::discriminant(expr)
                )))
            }
        }
    }

    /// Evaluate a DataFusion function by name with vectorized input (true vectorized evaluation)
    pub fn evaluate_df_function_vectorized(&self, function_name: &str, args: &[ScalarExpr], collection: &dyn Collection) -> DataFusionResult<Vec<Value>> {
        let num_rows = collection.num_rows();
        
        // If collection is empty, return empty result
        if num_rows == 0 {
            return Ok(vec![]);
        }
        
        // Convert collection to RecordBatch for DataFusion
        let record_batch = self.collection_to_record_batch(collection)?;
        
        // Evaluate all argument expressions to get their values for each row
        let mut arg_columns = Vec::new();
        for arg_expr in args {
            let arg_values = arg_expr.eval_vectorized(&DataFusionEvaluator::new(), collection)
                .map_err(|e| DataFusionError::Execution(format!("Failed to evaluate argument: {}", e)))?;
            arg_columns.push(arg_values);
        }
        
        // Convert argument values to DataFusion literals (one per row)
        let mut df_args = Vec::new();
        for row_idx in 0..num_rows {
            let mut row_args = Vec::new();
            for arg_column in arg_columns.iter() {
                if let Some(value) = arg_column.get(row_idx) {
                    let scalar_value = value_to_scalar_value(value)?;
                    row_args.push(lit(scalar_value));
                } else {
                    // Use null for missing values
                    row_args.push(lit(ScalarValue::Null));
                }
            }
            df_args.push(row_args);
        }
        
        // For now, we'll evaluate row by row, but this could be optimized to batch evaluation
        // In the future, we could create a single DataFusion expression that works on the entire batch
        let mut results = Vec::with_capacity(num_rows);
        
        for (row_idx, row_args) in df_args.iter().enumerate().take(num_rows) {
            // Create a single-row RecordBatch for this evaluation
            let single_row_batch = self.create_single_row_batch(&record_batch, row_idx)?;
            
            // Create the DataFusion function call
            let df_expr = crate::expr::datafusion_func::adapter::create_df_function_call(
                function_name.to_string(), 
                row_args.clone()
            )?;
            
            // Create a physical expression and evaluate
            let df_schema = single_row_batch.schema();
            let df_schema_ref = df_schema.to_dfschema_ref()?;
            let physical_expr = self.session_ctx.create_physical_expr(df_expr, &df_schema_ref)?;
            
            // Evaluate the expression
            let result = physical_expr.evaluate(&single_row_batch)?;
            
            // Convert the result back to flow Value
            let value = self.convert_columnar_value_to_flow_value(result)?;
            results.push(value);
        }
        
        Ok(results)
    }

    /// Helper method to evaluate a single row using vectorized evaluation
    fn eval_single_row_vectorized(&self, expr: &ScalarExpr, _row: &dyn Row) -> DataFusionResult<Value> {
        // Create a minimal single-row collection for vectorized evaluation
        // Since Row doesn't provide schema, we'll create a dummy schema
        let dummy_schema = datatypes::Schema::new(vec![]);
        let single_row_collection = crate::model::record_batch::RecordBatch::new(dummy_schema, vec![])
            .map_err(|e| DataFusionError::Execution(format!("Failed to create single row collection: {}", e)))?;
        
        // Use vectorized evaluation - this works for expressions that don't need specific column data
        let results = expr.eval_vectorized(self, &single_row_collection)
            .map_err(|e| DataFusionError::Execution(format!("Vectorized evaluation failed: {}", e)))?;
        
        // Extract single result
        if let Some(result) = results.into_iter().next() {
            Ok(result)
        } else {
            // For expressions that need actual data, we need to handle differently
            // This is a limitation - we need schema information from the row
            Err(DataFusionError::NotImplemented(
                "Single row evaluation requires schema information from Row trait".to_string()
            ))
        }
    }

    /// Legacy method: Evaluate a DataFusion function by name (single row)
    pub fn evaluate_df_function_legacy(&self, function_name: &str, args: &[ScalarExpr], row: &dyn Row) -> DataFusionResult<Value> {
        // First, evaluate all arguments using vectorized evaluation for single row
        let mut arg_values = Vec::new();
        for arg in args {
            match self.eval_single_row_vectorized(arg, row) {
                Ok(value) => arg_values.push(value),
                Err(eval_error) => return Err(DataFusionError::Execution(format!("Failed to evaluate argument: {}", eval_error))),
            }
        }
        
        // Convert row to RecordBatch - for now we assume it's a Tuple
        let record_batch = if let Some(tuple) = (row as &dyn Any).downcast_ref::<Tuple>() {
            tuple_to_record_batch(tuple)?
        } else {
            return Err(DataFusionError::NotImplemented(
                "evaluate_df_function_legacy currently only supports Tuple rows".to_string()
            ));
        };
        
        // Convert the evaluated argument values to DataFusion literals
        let df_args: DataFusionResult<Vec<Expr>> = arg_values
            .iter()
            .map(|value| {
                let scalar_value = value_to_scalar_value(value)?;
                Ok(lit(scalar_value))
            })
            .collect();
        let df_args = df_args?;
        
        // Create the DataFusion function call
        let df_expr = crate::expr::datafusion_func::adapter::create_df_function_call(function_name.to_string(), df_args)?;
        
        // Create a physical expression and evaluate
        let df_schema = record_batch.schema();
        let df_schema_ref = df_schema.to_dfschema_ref()?;
        let physical_expr = self.session_ctx.create_physical_expr(df_expr, &df_schema_ref)?;
        
        // Evaluate the expression
        let result = physical_expr.evaluate(&record_batch)?;
        
        // Convert the result back to flow Value
        self.convert_columnar_value_to_flow_value(result)
    }

    /// Evaluate multiple expressions against a row using vectorized evaluation
    pub fn evaluate_exprs_vectorized(&self, exprs: &[ScalarExpr], row: &dyn Row) -> DataFusionResult<Vec<Value>> {
        exprs.iter()
            .map(|expr| self.eval_single_row_vectorized(expr, row))
            .collect()
    }

    /// Evaluate a DataFusion expression directly against a row using vectorized evaluation
    pub fn evaluate_df_expr_vectorized(&self, expr: Expr, _row: &dyn Row) -> DataFusionResult<Value> {
        // Create a minimal single-row collection for vectorized evaluation
        let dummy_schema = datatypes::Schema::new(vec![]);
        let single_row_collection = crate::model::record_batch::RecordBatch::new(dummy_schema, vec![])
            .map_err(|e| DataFusionError::Execution(format!("Failed to create single row collection: {}", e)))?;
        
        // Convert to RecordBatch for DataFusion
        let record_batch = self.collection_to_record_batch(&single_row_collection)?;
        
        // Create a physical expression from the logical expression
        let df_schema = record_batch.schema();
        let df_schema_ref = df_schema.to_dfschema_ref()?;
        let physical_expr = self.session_ctx.create_physical_expr(expr, &df_schema_ref)?;
        
        // Evaluate the expression
        let result = physical_expr.evaluate(&record_batch)?;
        
        // Convert the result back to flow Value
        self.convert_columnar_value_to_flow_value(result)
    }

    /// Convert DataFusion ColumnarValue to flow Value
    fn convert_columnar_value_to_flow_value(&self, result: datafusion_expr::ColumnarValue) -> DataFusionResult<Value> {
        match result {
            datafusion_expr::ColumnarValue::Array(array) => {
                match array.len() {
                    0 => {
                        // Empty array should return Null
                        Ok(Value::Null)
                    }
                    1 => {
                        // Single element - extract the first element
                        self.array_element_to_value(&array, 0)
                    }
                    _ => {
                        // Multiple elements - convert to ListValue
                        let mut values = Vec::new();
                        for i in 0..array.len() {
                            values.push(self.array_element_to_value(&array, i)?);
                        }
                        
                        // Determine the datatype from the first element, or use a default
                        let datatype = if let Some(first_value) = values.first() {
                            Arc::new(Self::concrete_datatype_from_value(first_value))
                        } else {
                            // For empty arrays, we could try to infer from Arrow type, but for now use Int64 as default
                            Arc::new(ConcreteDatatype::Int64(::datatypes::Int64Type))
                        };
                        
                        Ok(Value::List(ListValue::new(values, datatype)))
                    }
                }
            }
            datafusion_expr::ColumnarValue::Scalar(scalar) => {
                scalar_value_to_value(&scalar)
            }
        }
    }
    
    /// Infer ConcreteDatatype from a Value
    fn concrete_datatype_from_value(value: &Value) -> ConcreteDatatype {
        match value {
            Value::Int8(_) => ConcreteDatatype::Int8(::datatypes::Int8Type),
            Value::Int16(_) => ConcreteDatatype::Int16(::datatypes::Int16Type),
            Value::Int32(_) => ConcreteDatatype::Int32(::datatypes::Int32Type),
            Value::Int64(_) => ConcreteDatatype::Int64(::datatypes::Int64Type),
            Value::Uint8(_) => ConcreteDatatype::Uint8(::datatypes::Uint8Type),
            Value::Uint16(_) => ConcreteDatatype::Uint16(::datatypes::Uint16Type),
            Value::Uint32(_) => ConcreteDatatype::Uint32(::datatypes::Uint32Type),
            Value::Uint64(_) => ConcreteDatatype::Uint64(::datatypes::Uint64Type),
            Value::Float32(_) => ConcreteDatatype::Float32(::datatypes::Float32Type),
            Value::Float64(_) => ConcreteDatatype::Float64(::datatypes::Float64Type),
            Value::String(_) => ConcreteDatatype::String(::datatypes::StringType),
            Value::Bool(_) => ConcreteDatatype::Bool(::datatypes::BooleanType),
            Value::Null => {
                // For null values, default to Int64
                ConcreteDatatype::Int64(::datatypes::Int64Type)
            }
            Value::Struct(_) => {
                // For struct, we would need more context, default to empty struct
                ConcreteDatatype::Struct(::datatypes::StructType::new(Arc::new(vec![])))
            }
            Value::List(list_val) => {
                // For list, use the existing datatype
                list_val.datatype().clone()
            }
        }
    }
    
    /// Convert a single array element to flow Value
    fn array_element_to_value(&self, array: &arrow::array::ArrayRef, index: usize) -> DataFusionResult<Value> {
        let scalar_value = match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let int_array = arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&array);
                ScalarValue::Int64(Some(int_array.value(index)))
            }
            arrow::datatypes::DataType::Int32 => {
                let int_array = arrow::array::as_primitive_array::<arrow::datatypes::Int32Type>(&array);
                ScalarValue::Int32(Some(int_array.value(index)))
            }
            arrow::datatypes::DataType::Int16 => {
                let int_array = arrow::array::as_primitive_array::<arrow::datatypes::Int16Type>(&array);
                ScalarValue::Int16(Some(int_array.value(index)))
            }
            arrow::datatypes::DataType::Int8 => {
                let int_array = arrow::array::as_primitive_array::<arrow::datatypes::Int8Type>(&array);
                ScalarValue::Int8(Some(int_array.value(index)))
            }
            arrow::datatypes::DataType::UInt64 => {
                let uint_array = arrow::array::as_primitive_array::<arrow::datatypes::UInt64Type>(&array);
                ScalarValue::UInt64(Some(uint_array.value(index)))
            }
            arrow::datatypes::DataType::UInt32 => {
                let uint_array = arrow::array::as_primitive_array::<arrow::datatypes::UInt32Type>(&array);
                ScalarValue::UInt32(Some(uint_array.value(index)))
            }
            arrow::datatypes::DataType::UInt16 => {
                let uint_array = arrow::array::as_primitive_array::<arrow::datatypes::UInt16Type>(&array);
                ScalarValue::UInt16(Some(uint_array.value(index)))
            }
            arrow::datatypes::DataType::UInt8 => {
                let uint_array = arrow::array::as_primitive_array::<arrow::datatypes::UInt8Type>(&array);
                ScalarValue::UInt8(Some(uint_array.value(index)))
            }
            arrow::datatypes::DataType::Float64 => {
                let float_array = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(&array);
                ScalarValue::Float64(Some(float_array.value(index)))
            }
            arrow::datatypes::DataType::Float32 => {
                let float_array = arrow::array::as_primitive_array::<arrow::datatypes::Float32Type>(&array);
                ScalarValue::Float32(Some(float_array.value(index)))
            }
            arrow::datatypes::DataType::Utf8 => {
                let string_array = arrow::array::as_string_array(&array);
                ScalarValue::Utf8(Some(string_array.value(index).to_string()))
            }
            arrow::datatypes::DataType::Boolean => {
                let bool_array = arrow::array::as_boolean_array(&array);
                ScalarValue::Boolean(Some(bool_array.value(index)))
            }
            _ => return Err(DataFusionError::NotImplemented(
                format!("Array element type {:?} conversion not implemented", array.data_type())
            )),
        };
        scalar_value_to_value(&scalar_value)
    }

    /// Convert Collection to RecordBatch for DataFusion evaluation
    fn collection_to_record_batch(&self, collection: &dyn Collection) -> DataFusionResult<RecordBatch> {
        // For now, we'll create a simple RecordBatch from the collection
        // This is a simplified implementation - in production you'd want more sophisticated conversion
        
        let num_rows = collection.num_rows();
        if num_rows == 0 {
            // Return empty RecordBatch with appropriate schema
            let schema = arrow::datatypes::Schema::empty();
            return Ok(RecordBatch::try_new(
                std::sync::Arc::new(schema),
                vec![]
            )?);
        }
        
        // For now, create a simple single-column RecordBatch as placeholder
        // In a real implementation, this would convert the full collection schema
        let int_array = arrow::array::Int64Array::from(vec![0i64; num_rows]);
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("dummy", arrow::datatypes::DataType::Int64, true)
        ]);
        
        Ok(RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![std::sync::Arc::new(int_array)]
        )?)
    }
    
    /// Create a single-row RecordBatch from a multi-row batch
    fn create_single_row_batch(&self, original_batch: &RecordBatch, row_idx: usize) -> DataFusionResult<RecordBatch> {
        let num_rows = original_batch.num_rows();
        if row_idx >= num_rows {
            return Err(DataFusionError::Execution(format!("Row index {} out of bounds for batch with {} rows", row_idx, num_rows)));
        }
        
        // Create single-row arrays from the original batch
        let mut single_row_arrays = Vec::new();
        for i in 0..original_batch.num_columns() {
            let array = original_batch.column(i);
            let single_row_array = array.slice(row_idx, 1);
            single_row_arrays.push(single_row_array);
        }
        
        Ok(RecordBatch::try_new(
            original_batch.schema(),
            single_row_arrays
        )?)
    }

    /// Create a RecordBatch from multiple tuples for batch evaluation
    pub fn tuples_to_record_batch(_tuples: &[Tuple]) -> DataFusionResult<RecordBatch> {
        // For now, this is not implemented with the new Tuple design
        Err(DataFusionError::NotImplemented(
            "Batch evaluation not yet implemented for the new Tuple design".to_string()
        ))
    }
}

impl Default for DataFusionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{ColumnSchema, Int64Type, StringType, Float64Type, BooleanType, ConcreteDatatype};
    use crate::model::Column;

    fn create_test_tuple() -> Tuple {
        let schema = datatypes::Schema::new(vec![
            ColumnSchema::new("id".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("name".to_string(), "test_table".to_string(), ConcreteDatatype::String(StringType)),
            ColumnSchema::new("age".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("score".to_string(), "test_table".to_string(), ConcreteDatatype::Float64(Float64Type)),
            ColumnSchema::new("active".to_string(), "test_table".to_string(), ConcreteDatatype::Bool(BooleanType)),
        ]);

        let _values = vec![
            Value::Int64(1),
            Value::String("Alice".to_string()),
            Value::Int64(25),
            Value::Float64(98.5),
            Value::Bool(true),
        ];

        Tuple::from_values(schema, _values)
    }

    #[test]
    fn test_basic_evaluation_vectorized() {
        // Note: DataFusionEvaluator should only handle CallDf expressions
        // Basic column references and literals should be handled by ScalarExpr::eval_vectorized directly
        let evaluator = DataFusionEvaluator::new();
        let _tuple = create_test_tuple();
        
        // Create single-row collection for vectorized testing
        let schema = datatypes::Schema::new(vec![
            ColumnSchema::new("id".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("name".to_string(), "test_table".to_string(), ConcreteDatatype::String(StringType)),
            ColumnSchema::new("age".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("score".to_string(), "test_table".to_string(), ConcreteDatatype::Float64(Float64Type)),
            ColumnSchema::new("active".to_string(), "test_table".to_string(), ConcreteDatatype::Bool(BooleanType)),
        ]);
        
        let collection = crate::model::RecordBatch::new(schema, vec![
            Column::new("id".to_string(), ConcreteDatatype::Int64(Int64Type), vec![Value::Int64(1)]),
            Column::new("name".to_string(), ConcreteDatatype::String(StringType), vec![Value::String("Alice".to_string())]),
            Column::new("age".to_string(), ConcreteDatatype::Int64(Int64Type), vec![Value::Int64(25)]),
            Column::new("score".to_string(), ConcreteDatatype::Float64(Float64Type), vec![Value::Float64(98.5)]),
            Column::new("active".to_string(), ConcreteDatatype::Bool(BooleanType), vec![Value::Bool(true)]),
        ]).unwrap();

        // Test column reference using vectorized evaluation
        let col_expr = ScalarExpr::column("test_table", "id");
        let results = col_expr.eval_with_collection(&evaluator, &collection).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Value::Int64(1));

        // Test literal using vectorized evaluation
        let lit_expr = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let results = lit_expr.eval_with_collection(&evaluator, &collection).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Value::Int64(42));
    }

    #[test]
    fn test_concat_function_vectorized() {
        let evaluator = DataFusionEvaluator::new();
        let _tuple = create_test_tuple();
        
        // Create single-row collection for vectorized testing
        let schema = datatypes::Schema::new(vec![
            ColumnSchema::new("id".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("name".to_string(), "test_table".to_string(), ConcreteDatatype::String(StringType)),
            ColumnSchema::new("age".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("score".to_string(), "test_table".to_string(), ConcreteDatatype::Float64(Float64Type)),
            ColumnSchema::new("active".to_string(), "test_table".to_string(), ConcreteDatatype::Bool(BooleanType)),
        ]);
        
        let collection = crate::model::RecordBatch::new(schema, vec![
            Column::new("id".to_string(), ConcreteDatatype::Int64(Int64Type), vec![Value::Int64(1)]),
            Column::new("name".to_string(), ConcreteDatatype::String(StringType), vec![Value::String("Alice".to_string())]),
            Column::new("age".to_string(), ConcreteDatatype::Int64(Int64Type), vec![Value::Int64(25)]),
            Column::new("score".to_string(), ConcreteDatatype::Float64(Float64Type), vec![Value::Float64(98.5)]),
            Column::new("active".to_string(), ConcreteDatatype::Bool(BooleanType), vec![Value::Bool(true)]),
        ]).unwrap();

        // Test concat function via CallDf using vectorized evaluation
        let name_col = ScalarExpr::column("test_table", "name"); // name column
        let lit_expr = ScalarExpr::literal(Value::String(" Smith".to_string()), ConcreteDatatype::String(StringType));
        let concat_expr = ScalarExpr::CallDf {
            function_name: "concat".to_string(),
            args: vec![name_col, lit_expr],
        };
        
        let results = evaluator.evaluate_expr_vectorized(&concat_expr, &collection).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Value::String("Alice Smith".to_string()));
    }
}