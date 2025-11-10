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
use crate::model::Row;
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

    /// Evaluate a ScalarExpr against a Tuple using DataFusion
    /// This method should only handle CallDf expressions, as per greptimedb design
    pub fn evaluate_expr(&self, expr: &ScalarExpr, row: &dyn Row) -> DataFusionResult<Value> {
        match expr {
            ScalarExpr::CallDf { function_name, args } => {
                // Only handle CallDf expressions - this is the main purpose of DataFusionEvaluator
                self.evaluate_df_function(function_name, args, row)
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

    /// Evaluate a DataFusion function by name
    pub fn evaluate_df_function(&self, function_name: &str, args: &[ScalarExpr], row: &dyn Row) -> DataFusionResult<Value> {
        // First, evaluate all arguments using regular ScalarExpr::eval to get their values
        let mut arg_values = Vec::new();
        for arg in args {
            // Use regular ScalarExpr evaluation for arguments
            match arg.eval(self, row) {
                Ok(value) => arg_values.push(value),
                Err(eval_error) => return Err(DataFusionError::Execution(format!("Failed to evaluate argument: {}", eval_error))),
            }
        }
        
        // Convert row to RecordBatch - for now we assume it's a Tuple
        let record_batch = if let Some(tuple) = (row as &dyn Any).downcast_ref::<Tuple>() {
            tuple_to_record_batch(tuple)?
        } else {
            return Err(DataFusionError::NotImplemented(
                "evaluate_df_function currently only supports Tuple rows".to_string()
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

    /// Evaluate multiple expressions against a row
    pub fn evaluate_exprs(&self, exprs: &[ScalarExpr], row: &dyn Row) -> DataFusionResult<Vec<Value>> {
        exprs.iter()
            .map(|expr| self.evaluate_expr(expr, row))
            .collect()
    }

    /// Evaluate a DataFusion expression directly against a row
    pub fn evaluate_df_expr(&self, expr: Expr, row: &dyn Row) -> DataFusionResult<Value> {
        // For now, we need to convert the row to a RecordBatch for DataFusion evaluation
        // This requires specific knowledge of the row implementation
        // In the future, we could add a method to Row trait to convert to RecordBatch
        
        // Try to downcast to Tuple for now
        use std::any::Any;
        if let Some(tuple) = (row as &dyn Any).downcast_ref::<Tuple>() {
            // Convert tuple to RecordBatch
            let record_batch = tuple_to_record_batch(tuple)?;
            
            // Create a physical expression from the logical expression
            let df_schema = record_batch.schema();
            let df_schema_ref = df_schema.to_dfschema_ref()?;
            let physical_expr = self.session_ctx.create_physical_expr(expr, &df_schema_ref)?;
            
            // Evaluate the expression
            let result = physical_expr.evaluate(&record_batch)?;
            
            // Convert the result back to flow Value
            self.convert_columnar_value_to_flow_value(result)
        } else {
            Err(DataFusionError::NotImplemented(
                "evaluate_df_expr currently only supports Tuple rows. Consider adding a to_record_batch() method to Row trait".to_string()
            ))
        }
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

    fn create_test_tuple() -> Tuple {
        let schema = datatypes::Schema::new(vec![
            ColumnSchema::new("id".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("name".to_string(), "test_table".to_string(), ConcreteDatatype::String(StringType)),
            ColumnSchema::new("age".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("score".to_string(), "test_table".to_string(), ConcreteDatatype::Float64(Float64Type)),
            ColumnSchema::new("active".to_string(), "test_table".to_string(), ConcreteDatatype::Bool(BooleanType)),
        ]);

        let values = vec![
            Value::Int64(1),
            Value::String("Alice".to_string()),
            Value::Int64(25),
            Value::Float64(98.5),
            Value::Bool(true),
        ];

        Tuple::from_values(schema, values)
    }

    #[test]
    fn test_basic_evaluation() {
        // Note: DataFusionEvaluator should only handle CallDf expressions
        // Basic column references and literals should be handled by ScalarExpr::eval directly
        let evaluator = DataFusionEvaluator::new();
        let tuple = create_test_tuple();

        // Test column reference using direct eval (with DataFusionEvaluator)
        let col_expr = ScalarExpr::column("test_table", "id");
        let result = col_expr.eval(&evaluator, &tuple).unwrap();
        assert_eq!(result, Value::Int64(1));

        // Test literal using direct eval (with DataFusionEvaluator)
        let lit_expr = ScalarExpr::literal(Value::Int64(42), ConcreteDatatype::Int64(Int64Type));
        let result = lit_expr.eval(&evaluator, &tuple).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_concat_function() {
        let evaluator = DataFusionEvaluator::new();
        let tuple = create_test_tuple();

        // Test concat function via CallDf
        let name_col = ScalarExpr::column("test_table", "name"); // name column
        let lit_expr = ScalarExpr::literal(Value::String(" Smith".to_string()), ConcreteDatatype::String(StringType));
        let concat_expr = ScalarExpr::CallDf {
            function_name: "concat".to_string(),
            args: vec![name_col, lit_expr],
        };
        
        let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
        assert_eq!(result, Value::String("Alice Smith".to_string()));
    }
}