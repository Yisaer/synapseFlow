//! DataFusion adapter for converting between flow types and DataFusion types

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::Expr;
use datatypes::{ConcreteDatatype, Value, Schema as FlowSchema};

use crate::tuple::Tuple;
use std::sync::Arc;

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
        _ => Err(DataFusionError::NotImplemented(
            format!("Conversion from ScalarValue {:?} not implemented", scalar)
        )),
    }
}

/// Convert flow ConcreteDatatype to DataFusion DataType
pub fn concrete_datatype_to_arrow_type(datatype: &ConcreteDatatype) -> DataFusionResult<DataType> {
    match datatype {
        ConcreteDatatype::Int8(_) => Ok(DataType::Int8),
        ConcreteDatatype::Int16(_) => Ok(DataType::Int16),
        ConcreteDatatype::Int32(_) => Ok(DataType::Int32),
        ConcreteDatatype::Int64(_) => Ok(DataType::Int64),
        ConcreteDatatype::Float32(_) => Ok(DataType::Float32),
        ConcreteDatatype::Float64(_) => Ok(DataType::Float64),
        ConcreteDatatype::Uint8(_) => Ok(DataType::UInt8),
        ConcreteDatatype::Uint16(_) => Ok(DataType::UInt16),
        ConcreteDatatype::Uint32(_) => Ok(DataType::UInt32),
        ConcreteDatatype::Uint64(_) => Ok(DataType::UInt64),
        ConcreteDatatype::String(_) => Ok(DataType::Utf8),
        ConcreteDatatype::Bool(_) => Ok(DataType::Boolean),
        ConcreteDatatype::Struct(_) => Err(DataFusionError::NotImplemented(
            "Struct type conversion not implemented".to_string()
        )),
        ConcreteDatatype::List(_) => Err(DataFusionError::NotImplemented(
            "List type conversion not implemented".to_string()
        )),
    }
}

/// Convert DataFusion DataType to flow ConcreteDatatype
pub fn arrow_type_to_concrete_datatype(data_type: &DataType) -> DataFusionResult<ConcreteDatatype> {
    match data_type {
        DataType::Int64 => Ok(ConcreteDatatype::Int64(datatypes::Int64Type)),
        DataType::Float64 => Ok(ConcreteDatatype::Float64(datatypes::Float64Type)),
        DataType::UInt8 => Ok(ConcreteDatatype::Uint8(datatypes::Uint8Type)),
        DataType::Utf8 => Ok(ConcreteDatatype::String(datatypes::StringType)),
        DataType::Boolean => Ok(ConcreteDatatype::Bool(datatypes::BooleanType)),
        _ => Err(DataFusionError::NotImplemented(
            format!("Conversion from DataType {:?} not implemented", data_type)
        )),
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

/// Convert a single Tuple to a RecordBatch
pub fn tuple_to_record_batch(tuple: &Tuple) -> DataFusionResult<RecordBatch> {
    // Extract all values from the tuple's HashMap
    let data = tuple.data();
    
    if data.is_empty() {
        return Err(DataFusionError::Execution("Cannot create RecordBatch from empty tuple".to_string()));
    }
    
    // Collect all values and sort them for consistent ordering
    let mut values: Vec<((String, String), Value)> = data.iter()
        .map(|((source, col), value)| ((source.clone(), col.clone()), value.clone()))
        .collect();
    
    // Sort by source name, then column name for consistent ordering
    values.sort_by(|a, b| {
        a.0.0.cmp(&b.0.0).then_with(|| a.0.1.cmp(&b.0.1))
    });
    
    // Create fields and arrays - one per value
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    
    for ((source_name, column_name), value) in values {
        let field_name = format!("{}.{}", source_name, column_name);
        
        match value {
            Value::Int64(v) => {
                fields.push(Field::new(&field_name, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Float64(v) => {
                fields.push(Field::new(&field_name, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::String(v) => {
                fields.push(Field::new(&field_name, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Bool(v) => {
                fields.push(Field::new(&field_name, DataType::Boolean, true));
                arrays.push(Arc::new(BooleanArray::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Int32(v) => {
                // Keep original Int32 type for type consistency
                fields.push(Field::new(&field_name, DataType::Int32, true));
                arrays.push(Arc::new(arrow::array::Int32Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Uint8(v) => {
                // Keep original UInt8 type for type consistency
                fields.push(Field::new(&field_name, DataType::UInt8, true));
                arrays.push(Arc::new(arrow::array::UInt8Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Int8(v) => {
                // Keep original Int8 type for type consistency
                fields.push(Field::new(&field_name, DataType::Int8, true));
                arrays.push(Arc::new(arrow::array::Int8Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Int16(v) => {
                // Keep original Int16 type for type consistency
                fields.push(Field::new(&field_name, DataType::Int16, true));
                arrays.push(Arc::new(arrow::array::Int16Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Float32(v) => {
                // Keep original Float32 type for type consistency
                fields.push(Field::new(&field_name, DataType::Float32, true));
                arrays.push(Arc::new(arrow::array::Float32Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Uint16(v) => {
                // Keep original UInt16 type for type consistency
                fields.push(Field::new(&field_name, DataType::UInt16, true));
                arrays.push(Arc::new(arrow::array::UInt16Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Uint32(v) => {
                // Keep original UInt32 type for type consistency
                fields.push(Field::new(&field_name, DataType::UInt32, true));
                arrays.push(Arc::new(arrow::array::UInt32Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Uint64(v) => {
                // Keep original UInt64 type for type consistency
                fields.push(Field::new(&field_name, DataType::UInt64, true));
                arrays.push(Arc::new(arrow::array::UInt64Array::from(vec![Some(v)])) as ArrayRef);
            }
            Value::Null => {
                // For null values, we need to infer type from column name or skip
                // For now, let's skip null values in RecordBatch creation
                continue;
            }
            _ => {
                return Err(DataFusionError::NotImplemented(
                    format!("Value type {:?} not supported for RecordBatch conversion", value)
                ));
            }
        }
    }
    
    if fields.is_empty() {
        return Err(DataFusionError::Execution("No valid columns found for RecordBatch creation".to_string()));
    }
    
    let arrow_schema = ArrowSchema::new(fields);
    
    RecordBatch::try_new(Arc::new(arrow_schema), arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Create a DataFusion function call by name
pub fn create_df_function_call(function_name: String, args: Vec<Expr>) -> DataFusionResult<Expr> {
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