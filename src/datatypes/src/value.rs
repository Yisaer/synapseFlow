use std::sync::Arc;

use crate::datatypes::ConcreteDatatype;
use crate::types::StructType;

/// List value containing items and their datatype
#[derive(Debug, Clone, PartialEq)]
pub struct ListValue {
    items: Vec<Value>,
    /// Inner values datatype, to distinguish empty lists of different datatypes
    datatype: Arc<ConcreteDatatype>,
}

impl ListValue {
    pub fn new(items: Vec<Value>, datatype: Arc<ConcreteDatatype>) -> Self {
        Self { items, datatype }
    }

    pub fn items(&self) -> &[Value] {
        &self.items
    }

    pub fn datatype(&self) -> &ConcreteDatatype {
        &self.datatype
    }

    /// Get element at the given index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.items.get(index)
    }

    /// Get the length of the list
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if the list is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Struct value containing items and field definitions
#[derive(Debug, Clone, PartialEq)]
pub struct StructValue {
    items: Vec<Value>,
    fields: StructType,
}

impl StructValue {
    pub fn new(items: Vec<Value>, fields: StructType) -> Self {
        Self { items, fields }
    }

    pub fn items(&self) -> &[Value] {
        &self.items
    }

    pub fn fields(&self) -> &StructType {
        &self.fields
    }

    /// Get field value by field name
    pub fn get_field(&self, field_name: &str) -> Option<&Value> {
        self.fields
            .fields()
            .iter()
            .position(|field| field.name() == field_name)
            .and_then(|index| self.items.get(index))
    }
}

/// Value type for type casting
/// Should be synchronized with ConcreteDatatype variants
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null value representing missing/unknown data
    Null,
    /// 32-bit floating point number
    Float32(f32),
    /// 64-bit floating point number
    Float64(f64),
    /// 8-bit signed integer
    Int8(i8),
    /// 16-bit signed integer
    Int16(i16),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 8-bit unsigned integer
    Uint8(u8),
    /// 16-bit unsigned integer
    Uint16(u16),
    /// 32-bit unsigned integer
    Uint32(u32),
    /// 64-bit unsigned integer
    Uint64(u64),
    /// String type
    String(String),
    /// Boolean type
    Bool(bool),
    /// Struct value
    Struct(StructValue),
    /// List value
    List(ListValue),
}

impl Value {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    
    /// Get the datatype of this value
    pub fn datatype(&self) -> ConcreteDatatype {
        match self {
            Value::Null => ConcreteDatatype::Null,
            Value::Float32(_) => ConcreteDatatype::Float32(crate::types::Float32Type),
            Value::Float64(_) => ConcreteDatatype::Float64(crate::types::Float64Type),
            Value::Int8(_) => ConcreteDatatype::Int8(crate::types::Int8Type),
            Value::Int16(_) => ConcreteDatatype::Int16(crate::types::Int16Type),
            Value::Int32(_) => ConcreteDatatype::Int32(crate::types::Int32Type),
            Value::Int64(_) => ConcreteDatatype::Int64(crate::types::Int64Type),
            Value::Uint8(_) => ConcreteDatatype::Uint8(crate::types::Uint8Type),
            Value::Uint16(_) => ConcreteDatatype::Uint16(crate::types::Uint16Type),
            Value::Uint32(_) => ConcreteDatatype::Uint32(crate::types::Uint32Type),
            Value::Uint64(_) => ConcreteDatatype::Uint64(crate::types::Uint64Type),
            Value::String(_) => ConcreteDatatype::String(crate::types::StringType),
            Value::Bool(_) => ConcreteDatatype::Bool(crate::types::BooleanType),
            Value::Struct(s) => ConcreteDatatype::Struct(s.fields().clone()),
            Value::List(l) => ConcreteDatatype::List(crate::types::ListType::new(Arc::new(l.datatype().clone()))),
        }
    }
}
