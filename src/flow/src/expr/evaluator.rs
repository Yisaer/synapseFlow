//! DataFusion-based expression evaluator for flow tuples

use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, Int64Array, Float64Array, StringArray, BooleanArray};
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue, ToDFSchema};
use datafusion_expr::{Expr, execution_props::ExecutionProps, lit};
use datatypes::{Value, ConcreteDatatype};
use std::sync::Arc;

use crate::expr::datafusion_adapter::*;
use crate::expr::ScalarExpr;
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
    pub fn evaluate_expr(&self, expr: &ScalarExpr, tuple: &Tuple) -> DataFusionResult<Value> {
        match expr {
            ScalarExpr::CallDf { function_name, args } => {
                // Only handle CallDf expressions - this is the main purpose of DataFusionEvaluator
                self.evaluate_df_function(function_name, args, tuple)
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
    pub fn evaluate_df_function(&self, function_name: &str, args: &[ScalarExpr], tuple: &Tuple) -> DataFusionResult<Value> {
        // First, evaluate all arguments using regular ScalarExpr::eval to get their values
        let mut arg_values = Vec::new();
        for arg in args {
            // Use regular ScalarExpr evaluation for arguments
            match arg.eval(self, tuple) {
                Ok(value) => arg_values.push(value),
                Err(eval_error) => return Err(DataFusionError::Execution(format!("Failed to evaluate argument: {}", eval_error))),
            }
        }
        
        // Convert tuple to RecordBatch
        let record_batch = tuple_to_record_batch(tuple)?;
        
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
        let df_expr = crate::expr::datafusion_adapter::create_df_function_call(function_name.to_string(), df_args)?;
        
        // Create a physical expression and evaluate
        let df_schema = record_batch.schema();
        let df_schema_ref = df_schema.to_dfschema_ref()?;
        let physical_expr = self.session_ctx.create_physical_expr(df_expr, &df_schema_ref)?;
        
        // Evaluate the expression
        let result = physical_expr.evaluate(&record_batch)?;
        
        // Convert the result back to flow Value
        self.convert_columnar_value_to_flow_value(result)
    }

    /// Evaluate multiple expressions against a tuple
    pub fn evaluate_exprs(&self, exprs: &[ScalarExpr], tuple: &Tuple) -> DataFusionResult<Vec<Value>> {
        exprs.iter()
            .map(|expr| self.evaluate_expr(expr, tuple))
            .collect()
    }

    /// Evaluate a DataFusion expression directly against a tuple
    pub fn evaluate_df_expr(&self, expr: Expr, tuple: &Tuple) -> DataFusionResult<Value> {
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
    }

    /// Convert DataFusion ColumnarValue to flow Value
    fn convert_columnar_value_to_flow_value(&self, result: datafusion_expr::ColumnarValue) -> DataFusionResult<Value> {
        match result {
            datafusion_expr::ColumnarValue::Array(array) => {
                // For single-row batches, extract the first element
                if array.len() == 1 {
                    // Convert array element to ScalarValue first
                    let scalar_value = match array.data_type() {
                        arrow::datatypes::DataType::Int64 => {
                            let int_array = arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&array);
                            ScalarValue::Int64(Some(int_array.value(0)))
                        }
                        arrow::datatypes::DataType::Int32 => {
                            let int_array = arrow::array::as_primitive_array::<arrow::datatypes::Int32Type>(&array);
                            ScalarValue::Int64(Some(int_array.value(0) as i64))
                        }
                        arrow::datatypes::DataType::Float64 => {
                            let float_array = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(&array);
                            ScalarValue::Float64(Some(float_array.value(0)))
                        }
                        arrow::datatypes::DataType::Utf8 => {
                            let string_array = arrow::array::as_string_array(&array);
                            ScalarValue::Utf8(Some(string_array.value(0).to_string()))
                        }
                        arrow::datatypes::DataType::Boolean => {
                            let bool_array = arrow::array::as_boolean_array(&array);
                            ScalarValue::Boolean(Some(bool_array.value(0)))
                        }
                        arrow::datatypes::DataType::UInt8 => {
                            let uint8_array = arrow::array::as_primitive_array::<arrow::datatypes::UInt8Type>(&array);
                            ScalarValue::UInt8(Some(uint8_array.value(0)))
                        }
                        _ => return Err(DataFusionError::NotImplemented(
                            format!("Array type {:?} conversion not implemented", array.data_type())
                        )),
                    };
                    scalar_value_to_value(&scalar_value)
                } else {
                    Err(DataFusionError::Execution(
                        format!("Expected single-row result, got {} rows", array.len())
                    ))
                }
            }
            datafusion_expr::ColumnarValue::Scalar(scalar) => {
                scalar_value_to_value(&scalar)
            }
        }
    }

    /// Create a RecordBatch from multiple tuples for batch evaluation
    pub fn tuples_to_record_batch(tuples: &[Tuple]) -> DataFusionResult<RecordBatch> {
        if tuples.is_empty() {
            return Err(DataFusionError::Execution("Cannot create RecordBatch from empty tuples".to_string()));
        }

        // Use the schema from the first tuple
        let schema = tuples[0].schema();
        let arrow_schema = flow_schema_to_arrow_schema(schema)?;
        
        // Collect all values for each column
        let mut columns: Vec<Vec<Value>> = vec![Vec::new(); schema.column_schemas().len()];
        
        for tuple in tuples {
            for (i, value) in tuple.row().iter().enumerate() {
                columns[i].push(value.clone());
            }
        }
        
        // Convert each column to Arrow array
        let arrow_columns: DataFusionResult<Vec<ArrayRef>> = columns
            .into_iter()
            .zip(schema.column_schemas().iter())
            .map(|(values, col_schema)| values_to_array(values, &col_schema.data_type))
            .collect();
        
        RecordBatch::try_new(Arc::new(arrow_schema), arrow_columns?)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Default for DataFusionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert multiple flow Values to Arrow Array
fn values_to_array(values: Vec<Value>, datatype: &ConcreteDatatype) -> DataFusionResult<ArrayRef> {
    match datatype {
        ConcreteDatatype::Int64(_) => {
            let int_values: Vec<i64> = values
                .into_iter()
                .map(|v| match v {
                    Value::Int64(i) => Ok(i),
                    _ => Err(DataFusionError::Internal(
                        format!("Expected Int64 value, got {:?}", v)
                    )),
                })
                .collect::<DataFusionResult<_>>()?;
            Ok(Arc::new(Int64Array::from(int_values)) as ArrayRef)
        }
        ConcreteDatatype::Float64(_) => {
            let float_values: Vec<f64> = values
                .into_iter()
                .map(|v| match v {
                    Value::Float64(f) => Ok(f),
                    _ => Err(DataFusionError::Internal(
                        format!("Expected Float64 value, got {:?}", v)
                    )),
                })
                .collect::<DataFusionResult<_>>()?;
            Ok(Arc::new(Float64Array::from(float_values)) as ArrayRef)
        }
        ConcreteDatatype::String(_) => {
            let string_values: Vec<String> = values
                .into_iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s),
                    _ => Err(DataFusionError::Internal(
                        format!("Expected String value, got {:?}", v)
                    )),
                })
                .collect::<DataFusionResult<_>>()?;
            Ok(Arc::new(StringArray::from(string_values)) as ArrayRef)
        }
        ConcreteDatatype::Bool(_) => {
            let bool_values: Vec<bool> = values
                .into_iter()
                .map(|v| match v {
                    Value::Bool(b) => Ok(b),
                    _ => Err(DataFusionError::Internal(
                        format!("Expected Bool value, got {:?}", v)
                    )),
                })
                .collect::<DataFusionResult<_>>()?;
            Ok(Arc::new(BooleanArray::from(bool_values)) as ArrayRef)
        }
        ConcreteDatatype::Uint8(_) => {
            let uint8_values: Vec<u8> = values
                .into_iter()
                .map(|v| match v {
                    Value::Uint8(u) => Ok(u),
                    _ => Err(DataFusionError::Internal(
                        format!("Expected Uint8 value, got {:?}", v)
                    )),
                })
                .collect::<DataFusionResult<_>>()?;
            // Use UInt8Array from arrow
            use arrow::array::UInt8Array;
            Ok(Arc::new(UInt8Array::from(uint8_values)) as ArrayRef)
        }
        _ => Err(DataFusionError::NotImplemented(
            format!("Array conversion for type {:?} not implemented", datatype)
        )),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{ColumnSchema, Int64Type, StringType, Float64Type, BooleanType};

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
        let col_expr = ScalarExpr::column(0);
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
        let name_col = ScalarExpr::column(1); // name column
        let lit_expr = ScalarExpr::literal(Value::String(" Smith".to_string()), ConcreteDatatype::String(StringType));
        let concat_expr = ScalarExpr::CallDf {
            function_name: "concat".to_string(),
            args: vec![name_col, lit_expr],
        };
        
        let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
        assert_eq!(result, Value::String("Alice Smith".to_string()));
    }
}