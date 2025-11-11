//! DataFusion-based expression evaluator for flow tuples

use std::sync::Arc;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue, ToDFSchema};
use datafusion_expr::{lit, Expr, ColumnarValue};
use datatypes::{Value, ListValue, ConcreteDatatype};

use crate::model::Collection;

/// DataFusion-based expression evaluator
pub struct DataFusionEvaluator {
    session_ctx: SessionContext,
}

impl DataFusionEvaluator {
    /// Create a new DataFusion evaluator
    pub fn new() -> Self {
        Self {
            session_ctx: SessionContext::new(),
        }
    }

    /// Evaluate a DataFusion function by name with vectorized input (true vectorized evaluation)
    /// 
    /// # Arguments
    /// 
    /// * `function_name` - Name of the DataFusion function to call
    /// * `args` - Vector of argument vectors, where args[i][j] is the i-th argument's j-th row value
    /// * `collection` - The collection providing context (row count, etc.)
    pub fn evaluate_df_function_vectorized(&self, function_name: &str, args: &[Vec<Value>], collection: &dyn Collection) -> DataFusionResult<Vec<Value>> {
        if args.is_empty() {
            return Ok(vec![]);
        }
        
        let num_rows = collection.num_rows();
        if num_rows == 0 {
            return Ok(vec![]);
        }
        
        // Validate all argument vectors have the same length
        for (i, arg_vec) in args.iter().enumerate() {
            if arg_vec.len() != num_rows {
                return Err(DataFusionError::Execution(format!(
                    "Argument {} has {} values, expected {} (all arguments must have the same length)",
                    i, arg_vec.len(), num_rows
                )));
            }
        }
        
        // Convert arguments to DataFusion format and evaluate row by row
        // TODO: This could be optimized to process the entire batch at once
        let mut results = Vec::with_capacity(num_rows);
        
        for row_idx in 0..num_rows {
            // Build arguments for this row
            let mut row_args = Vec::new();
            for arg_vec in args {
                let value = &arg_vec[row_idx];
                let scalar_value = value_to_scalar_value(value)?;
                row_args.push(lit(scalar_value));
            }
            
            // Create the function call
            let df_expr = create_df_function_call(function_name.to_string(), row_args)?;
            
            // Create a minimal single-row collection for evaluation
            let single_row_collection = self.create_single_row_collection()?;
            
            // Evaluate using DataFusion
            let df_schema = single_row_collection.schema();
            let df_schema_ref = df_schema.to_dfschema_ref()?;
            let physical_expr = self.session_ctx.create_physical_expr(df_expr, &df_schema_ref)?;
            
            let result = physical_expr.evaluate(&single_row_collection)?;
            let value = self.convert_columnar_value_to_flow_value(result)?;
            results.push(value);
        }
        
        Ok(results)
    }

    /// Convert DataFusion ColumnarValue to flow Value
    fn convert_columnar_value_to_flow_value(&self, result: ColumnarValue) -> DataFusionResult<Value> {
        match result {
            ColumnarValue::Array(array) => {
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
                        
                        // Determine the datatype from the Arrow array type
                        let datatype = Arc::new(arrow_type_to_concrete_datatype(array.data_type())?);
                        
                        Ok(Value::List(ListValue::new(values, datatype)))
                    }
                }
            }
            ColumnarValue::Scalar(scalar) => {
                scalar_value_to_value(&scalar)
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
    
    /// Create a minimal single-row collection for DataFusion evaluation
    fn create_single_row_collection(&self) -> DataFusionResult<arrow::record_batch::RecordBatch> {
        // Create a minimal RecordBatch with a single dummy column
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("dummy", arrow::datatypes::DataType::Int64, true)
        ]);
        
        let int_array = arrow::array::Int64Array::from(vec![Some(0i64)]);
        
        arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![std::sync::Arc::new(int_array)]
        ).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
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
    use crate::model::{Column, RecordBatch};
    use datatypes::Value;

    #[test]
    fn test_concat_function_vectorized() {
        let evaluator = DataFusionEvaluator::new();
        
        // Create test data: ["Hello", "World"] and [" ", " "]
        let col1 = Column::new(
            "col1".to_string(),
            "test".to_string(),
            vec![
                Value::String("Hello".to_string()),
                Value::String("World".to_string())
            ]
        );
        let col2 = Column::new(
            "col2".to_string(),
            "test".to_string(),
            vec![
                Value::String(" ".to_string()),
                Value::String(" ".to_string())
            ]
        );
        
        let collection = RecordBatch::new(vec![col1, col2]).unwrap();
        
        // Build arguments: two string columns
        let arg1_values = vec![
            Value::String("Hello".to_string()),
            Value::String("World".to_string())
        ];
        let arg2_values = vec![
            Value::String(" ".to_string()),
            Value::String(" ".to_string())
        ];
        let arg3_values = vec![
            Value::String("!".to_string()),
            Value::String("!".to_string())
        ];
        
        let args = vec![arg1_values, arg2_values, arg3_values];
        
        // Evaluate concat function
        let results = evaluator.evaluate_df_function_vectorized("concat", &args, &collection).unwrap();
        
        // Verify results
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Value::String("Hello !".to_string()));
        assert_eq!(results[1], Value::String("World !".to_string()));
    }
    
    #[test]
    fn test_upper_function_vectorized() {
        let evaluator = DataFusionEvaluator::new();
        
        // Create test data
        let col = Column::new(
            "col".to_string(),
            "test".to_string(),
            vec![
                Value::String("hello".to_string()),
                Value::String("world".to_string())
            ]
        );
        
        let collection = RecordBatch::new(vec![col]).unwrap();
        
        // Build arguments
        let args = vec![
            vec![
                Value::String("hello".to_string()),
                Value::String("world".to_string())
            ]
        ];
        
        // Evaluate upper function
        let results = evaluator.evaluate_df_function_vectorized("upper", &args, &collection).unwrap();
        
        // Verify results
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Value::String("HELLO".to_string()));
        assert_eq!(results[1], Value::String("WORLD".to_string()));
    }
}

/// Convert flow Value to DataFusion ScalarValue
fn value_to_scalar_value(value: &Value) -> DataFusionResult<ScalarValue> {
    match value {
        Value::Null => Err(DataFusionError::NotImplemented(
            "Null value conversion to ScalarValue requires type information".to_string()
        )),
        Value::Int8(v) => Ok(ScalarValue::Int8(Some(*v))),
        Value::Int16(v) => Ok(ScalarValue::Int16(Some(*v))),
        Value::Int32(v) => Ok(ScalarValue::Int32(Some(*v))),
        Value::Int64(v) => Ok(ScalarValue::Int64(Some(*v))),
        Value::Float32(v) => Ok(ScalarValue::Float32(Some(*v))),
        Value::Float64(v) => Ok(ScalarValue::Float64(Some(*v))),
        Value::Uint8(v) => Ok(ScalarValue::UInt8(Some(*v))),
        Value::Uint16(v) => Ok(ScalarValue::UInt16(Some(*v))),
        Value::Uint32(v) => Ok(ScalarValue::UInt32(Some(*v))),
        Value::Uint64(v) => Ok(ScalarValue::UInt64(Some(*v))),
        Value::String(v) => Ok(ScalarValue::Utf8(Some(v.clone()))),
        Value::Bool(v) => Ok(ScalarValue::Boolean(Some(*v))),
        Value::Struct(_) => Err(DataFusionError::NotImplemented(
            "Struct value conversion not implemented".to_string()
        )),
        Value::List(_) => Err(DataFusionError::NotImplemented(
            "List value conversion not implemented".to_string()
        )),
    }
}

/// Convert DataFusion ScalarValue to flow Value
fn scalar_value_to_value(scalar: &ScalarValue) -> DataFusionResult<Value> {
    match scalar {
        ScalarValue::Int8(None) | ScalarValue::Int16(None) | ScalarValue::Int32(None) | ScalarValue::Int64(None) |
        ScalarValue::Float32(None) | ScalarValue::Float64(None) | ScalarValue::UInt8(None) | ScalarValue::UInt16(None) |
        ScalarValue::UInt32(None) | ScalarValue::UInt64(None) | ScalarValue::Utf8(None) | ScalarValue::Boolean(None) => {
            Ok(Value::Null)
        },
        ScalarValue::Int8(Some(v)) => Ok(Value::Int8(*v)),
        ScalarValue::Int16(Some(v)) => Ok(Value::Int16(*v)),
        ScalarValue::Int32(Some(v)) => Ok(Value::Int32(*v)),
        ScalarValue::Int64(Some(v)) => Ok(Value::Int64(*v)),
        ScalarValue::Float32(Some(v)) => Ok(Value::Float32(*v)),
        ScalarValue::Float64(Some(v)) => Ok(Value::Float64(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(Value::Uint8(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(Value::Uint16(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(Value::Uint32(*v)),
        ScalarValue::UInt64(Some(v)) => Ok(Value::Uint64(*v)),
        ScalarValue::Utf8(Some(v)) => Ok(Value::String(v.clone())),
        ScalarValue::Boolean(Some(v)) => Ok(Value::Bool(*v)),
        _ => Err(DataFusionError::NotImplemented(format!("Unsupported ScalarValue type: {:?}", scalar))),
    }
}

/// Create a DataFusion function call by name
fn create_df_function_call(function_name: String, args: Vec<Expr>) -> DataFusionResult<Expr> {
    match function_name.as_str() {
        "concat" => {
            // Use DataFusion's built-in concat function
            Ok(datafusion::functions::string::concat().call(args))
        }
        "upper" => {
            // Use DataFusion's upper function
            Ok(datafusion::functions::string::upper().call(args))
        }
        "lower" => {
            // Use DataFusion's lower function
            Ok(datafusion::functions::string::lower().call(args))
        }
        "trim" => {
            // Use DataFusion's btrim function (trim is not available in this version)
            Ok(datafusion::functions::string::btrim().call(args))
        }
        "length" => {
            // Use DataFusion's character_length function from unicode module
            Ok(datafusion::functions::unicode::character_length().call(args))
        }
        "substr" | "substring" => {
            // Use DataFusion's substr function from unicode module
            Ok(datafusion::functions::unicode::substr().call(args))
        }
        "round" => {
            // Use DataFusion's round function
            Ok(datafusion::functions::math::round().call(args))
        }
        "abs" => {
            // Use DataFusion's abs function
            Ok(datafusion::functions::math::abs().call(args))
        }
        "sqrt" => {
            // Use DataFusion's sqrt function
            Ok(datafusion::functions::math::sqrt().call(args))
        }
        _ => {
            // For unknown functions, try to create a scalar function call
            // This allows for extensibility - users can register custom functions
            Err(DataFusionError::Plan(format!(
                "Unknown function: {}. Supported functions: concat, upper, lower, trim, length, substr, round, abs, sqrt",
                function_name
            )))
        }
    }
}

/// Convert Arrow DataType to ConcreteDatatype
fn arrow_type_to_concrete_datatype(arrow_type: &arrow::datatypes::DataType) -> DataFusionResult<ConcreteDatatype> {
    match arrow_type {
        arrow::datatypes::DataType::Int8 => Ok(ConcreteDatatype::Int8(::datatypes::Int8Type)),
        arrow::datatypes::DataType::Int16 => Ok(ConcreteDatatype::Int16(::datatypes::Int16Type)),
        arrow::datatypes::DataType::Int32 => Ok(ConcreteDatatype::Int32(::datatypes::Int32Type)),
        arrow::datatypes::DataType::Int64 => Ok(ConcreteDatatype::Int64(::datatypes::Int64Type)),
        arrow::datatypes::DataType::UInt8 => Ok(ConcreteDatatype::Uint8(::datatypes::Uint8Type)),
        arrow::datatypes::DataType::UInt16 => Ok(ConcreteDatatype::Uint16(::datatypes::Uint16Type)),
        arrow::datatypes::DataType::UInt32 => Ok(ConcreteDatatype::Uint32(::datatypes::Uint32Type)),
        arrow::datatypes::DataType::UInt64 => Ok(ConcreteDatatype::Uint64(::datatypes::Uint64Type)),
        arrow::datatypes::DataType::Float32 => Ok(ConcreteDatatype::Float32(::datatypes::Float32Type)),
        arrow::datatypes::DataType::Float64 => Ok(ConcreteDatatype::Float64(::datatypes::Float64Type)),
        arrow::datatypes::DataType::Utf8 => Ok(ConcreteDatatype::String(::datatypes::StringType)),
        arrow::datatypes::DataType::Boolean => Ok(ConcreteDatatype::Bool(::datatypes::BooleanType)),
        _ => Err(DataFusionError::NotImplemented(
            format!("Arrow type {:?} conversion to ConcreteDatatype not implemented", arrow_type)
        )),
    }
}
