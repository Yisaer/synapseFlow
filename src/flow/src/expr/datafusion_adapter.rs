//! DataFusion adapter for converting between flow types and DataFusion types

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::{prelude::*};
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{Expr, col, lit, ExprSchemable};
use datatypes::{ConcreteDatatype, Value, Schema as FlowSchema};
use crate::expr::{ScalarExpr, BinaryFunc, UnaryFunc};
use crate::tuple::Tuple;
use std::sync::Arc;

/// Convert flow Value to DataFusion ScalarValue
pub fn value_to_scalar_value(value: &Value) -> DataFusionResult<ScalarValue> {
    match value {
        Value::Int64(v) => Ok(ScalarValue::Int64(Some(*v))),
        Value::Float64(v) => Ok(ScalarValue::Float64(Some(*v))),
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
        ScalarValue::Int64(Some(v)) => Ok(Value::Int64(*v)),
        ScalarValue::Float64(Some(v)) => Ok(Value::Float64(*v)),
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
        ConcreteDatatype::Int64(_) => Ok(DataType::Int64),
        ConcreteDatatype::Float64(_) => Ok(DataType::Float64),
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
    let arrow_schema = flow_schema_to_arrow_schema(tuple.schema())?;
    let columns: DataFusionResult<Vec<ArrayRef>> = tuple
        .row()
        .iter()
        .zip(tuple.schema().column_schemas().iter())
        .map(|(value, col_schema)| value_to_array(value, &col_schema.data_type))
        .collect();
    
    RecordBatch::try_new(Arc::new(arrow_schema), columns?)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Convert flow Value to Arrow Array (single element array)
fn value_to_array(value: &Value, datatype: &ConcreteDatatype) -> DataFusionResult<ArrayRef> {
    match (value, datatype) {
        (Value::Int64(v), ConcreteDatatype::Int64(_)) => {
            Ok(Arc::new(Int64Array::from(vec![*v])) as ArrayRef)
        }
        (Value::Float64(v), ConcreteDatatype::Float64(_)) => {
            Ok(Arc::new(Float64Array::from(vec![*v])) as ArrayRef)
        }
        (Value::String(v), ConcreteDatatype::String(_)) => {
            Ok(Arc::new(StringArray::from(vec![v.as_str()])) as ArrayRef)
        }
        (Value::Bool(v), ConcreteDatatype::Bool(_)) => {
            Ok(Arc::new(BooleanArray::from(vec![*v])) as ArrayRef)
        }
        _ => Err(DataFusionError::NotImplemented(
            format!("Array conversion for value {:?} with type {:?} not implemented", value, datatype)
        )),
    }
}

/// Convert ScalarExpr to DataFusion Expr with schema context
pub fn scalar_expr_to_datafusion_expr(expr: &ScalarExpr, schema: &FlowSchema) -> DataFusionResult<Expr> {
    match expr {
        ScalarExpr::Column(index) => {
            // Get the actual column name from schema
            let column_schemas = schema.column_schemas();
            if *index < column_schemas.len() {
                let col_name = &column_schemas[*index].name;
                Ok(col(col_name))
            } else {
                Err(DataFusionError::Plan(format!("Column index {} out of bounds for schema with {} columns", index, column_schemas.len())))
            }
        },
        ScalarExpr::Literal(value, _datatype) => {
            let scalar_value = value_to_scalar_value(value)?;
            Ok(lit(scalar_value))
        }
        ScalarExpr::CallUnary { func, expr } => {
            let inner_expr = scalar_expr_to_datafusion_expr(expr, schema)?;
            match func {
                UnaryFunc::Not => Ok(inner_expr.not()),
                UnaryFunc::IsNull => Ok(inner_expr.is_null()),
                UnaryFunc::IsTrue => Ok(inner_expr.eq(lit(true))),
                UnaryFunc::IsFalse => Ok(inner_expr.eq(lit(false))),
                UnaryFunc::Cast(target_type) => {
                    let arrow_type = concrete_datatype_to_arrow_type(target_type)?;
                    Ok(inner_expr.cast_to(&arrow_type, &datafusion_common::DFSchema::empty())?)
                }
            }
        }
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            let left = scalar_expr_to_datafusion_expr(expr1, schema)?;
            let right = scalar_expr_to_datafusion_expr(expr2, schema)?;
            

            match func {
                BinaryFunc::Eq => Ok(left.eq(right)),
                BinaryFunc::NotEq => Ok(left.not_eq(right)),
                BinaryFunc::Lt => Ok(left.lt(right)),
                BinaryFunc::Lte => Ok(left.lt_eq(right)),
                BinaryFunc::Gt => Ok(left.gt(right)),
                BinaryFunc::Gte => Ok(left.gt_eq(right)),
                BinaryFunc::Add => Ok(left + right),
                BinaryFunc::Sub => Ok(left - right),
                BinaryFunc::Mul => Ok(left * right),
                BinaryFunc::Div => Ok(left / right),
                BinaryFunc::Mod => Ok(left % right),
            }
        }
        ScalarExpr::CallDf { function_name, args } => {
            // Convert arguments to DataFusion expressions
            let df_args: DataFusionResult<Vec<Expr>> = args
                .iter()
                .map(|arg| scalar_expr_to_datafusion_expr(arg, schema))
                .collect();
            let df_args = df_args?;
            
            // Create a DataFusion function call by name
            create_df_function_call(function_name.clone(), df_args)
        }
    }
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