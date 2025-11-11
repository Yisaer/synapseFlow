//! DataFusion adapter for converting between flow types and DataFusion types

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::Expr;
use datatypes::{ConcreteDatatype, Value, Schema as FlowSchema};

/// List of functions that can be called through CallDf (DataFusion functions)
pub const DATAFUSION_FUNCTIONS: &[&str] = &[
    "concat", "upper", "lower", "trim", "length", "substr", "substring", 
    "round", "abs", "sqrt"
];

/// List of functions that can be called through CallFunc (custom functions)
pub const CUSTOM_FUNCTIONS: &[&str] = &[
    "concat",  // Note: concat is supported both as DataFusion and custom function
];

/// Convert flow Value to DataFusion ScalarValue
pub fn value_to_scalar_value(value: &Value) -> DataFusionResult<ScalarValue> {
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
pub fn scalar_value_to_value(scalar: &ScalarValue) -> DataFusionResult<Value> {
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

/// Convert ConcreteDatatype to Arrow DataType
pub fn concrete_datatype_to_arrow_type(dt: &ConcreteDatatype) -> DataFusionResult<DataType> {
    match dt {
        ConcreteDatatype::Null => Ok(DataType::Null),
        ConcreteDatatype::Float32(_) => Ok(DataType::Float32),
        ConcreteDatatype::Float64(_) => Ok(DataType::Float64),
        ConcreteDatatype::Int8(_) => Ok(DataType::Int8),
        ConcreteDatatype::Int16(_) => Ok(DataType::Int16),
        ConcreteDatatype::Int32(_) => Ok(DataType::Int32),
        ConcreteDatatype::Int64(_) => Ok(DataType::Int64),
        ConcreteDatatype::Uint8(_) => Ok(DataType::UInt8),
        ConcreteDatatype::Uint16(_) => Ok(DataType::UInt16),
        ConcreteDatatype::Uint32(_) => Ok(DataType::UInt32),
        ConcreteDatatype::Uint64(_) => Ok(DataType::UInt64),
        ConcreteDatatype::String(_) => Ok(DataType::Utf8),
        ConcreteDatatype::Bool(_) => Ok(DataType::Boolean),
        ConcreteDatatype::Struct(_) => Err(DataFusionError::NotImplemented("Struct type conversion not implemented".to_string())),
        ConcreteDatatype::List(_) => Err(DataFusionError::NotImplemented("List type conversion not implemented".to_string())),
    }
}

/// Convert flow Schema to Arrow Schema
pub fn flow_schema_to_arrow_schema(flow_schema: &FlowSchema) -> DataFusionResult<ArrowSchema> {
    let fields: DataFusionResult<Vec<Field>> = flow_schema
        .column_schemas()
        .iter()
        .map(|col_schema| {
            let data_type = concrete_datatype_to_arrow_type(&col_schema.data_type)?;
            Ok(Field::new(&col_schema.name, data_type, true)) // Assuming nullable for now
        })
        .collect();
    
    Ok(ArrowSchema::new(fields?))
}

/// Create a DataFusion function call by name
pub fn create_df_function_call(function_name: String, args: Vec<Expr>) -> DataFusionResult<Expr> {
    // Check if the function is in the allowed DataFusion functions list
    if !DATAFUSION_FUNCTIONS.contains(&function_name.as_str()) {
        return Err(DataFusionError::Plan(format!(
            "Function '{}' is not available for CallDf. Available DataFusion functions: {:?}",
            function_name, DATAFUSION_FUNCTIONS
        )));
    }
    
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
            // This should not happen due to the check above, but keep for safety
            Err(DataFusionError::Internal(format!(
                "Function '{}' is in DATAFUSION_FUNCTIONS but not handled in match expression",
                function_name
            )))
        }
    }
}

/// Error types for DataFusion adapter
#[derive(Debug, Clone, PartialEq)]
pub enum AdapterError {
    DataFusionError(String),
    TypeConversionError(String),
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdapterError::DataFusionError(msg) => write!(f, "DataFusion error: {}", msg),
            AdapterError::TypeConversionError(msg) => write!(f, "Type conversion error: {}", msg),
        }
    }
}

impl std::error::Error for AdapterError {}

impl From<DataFusionError> for AdapterError {
    fn from(error: DataFusionError) -> Self {
        AdapterError::DataFusionError(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datafusion_function_validation() {
        // Test that all DATAFUSION_FUNCTIONS are supported
        for &func_name in DATAFUSION_FUNCTIONS {
            let result = create_df_function_call(func_name.to_string(), vec![]);
            assert!(result.is_ok(), "Function '{}' should be supported", func_name);
        }
    }
    
    #[test]
    fn test_unknown_function_error() {
        let result = create_df_function_call("unknown_function".to_string(), vec![]);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("not available for CallDf"));
        assert!(error_msg.contains("concat")); // Should list available functions
    }
    
    #[test]
    fn test_custom_function_list() {
        // Verify concat is in both lists
        assert!(DATAFUSION_FUNCTIONS.contains(&"concat"));
        assert!(CUSTOM_FUNCTIONS.contains(&"concat"));
        
        // Verify other functions are only in DATAFUSION_FUNCTIONS
        assert!(DATAFUSION_FUNCTIONS.contains(&"upper"));
        assert!(!CUSTOM_FUNCTIONS.contains(&"upper"));
    }
}
