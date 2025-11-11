use std::sync::Arc;

use crate::datatypes::DataType;
use crate::datatypes::ConcreteDatatype;
use crate::value::{StructValue, Value};

/// Struct field definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructField {
    /// Field name
    name: String,
    /// Field data type
    data_type: ConcreteDatatype,
    /// Whether the field is nullable
    nullable: bool,
}

impl StructField {
    /// Create a new struct field
    pub fn new(name: String, data_type: ConcreteDatatype, nullable: bool) -> Self {
        StructField {
            name,
            data_type,
            nullable,
        }
    }

    /// Returns the field name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field data type
    pub fn data_type(&self) -> &ConcreteDatatype {
        &self.data_type
    }

    /// Returns whether the field is nullable
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// Struct type, containing field definitions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructType {
    /// Struct fields
    fields: Arc<Vec<StructField>>,
}

impl StructType {
    /// Create a new struct type with fields
    pub fn new(fields: Arc<Vec<StructField>>) -> Self {
        StructType { fields }
    }

    /// Returns the struct fields
    pub fn fields(&self) -> Arc<Vec<StructField>> {
        self.fields.clone()
    }
}

impl DataType for StructType {
    fn name(&self) -> String {
        let field_names: Vec<String> = self
            .fields
            .iter()
            .map(|f| format!("{}: {}", f.name(), get_type_name(f.data_type())))
            .collect();
        format!("Struct<{}>", field_names.join(", "))
    }

    fn default_value(&self) -> Value {
        let items: Vec<Value> = self
            .fields
            .iter()
            .map(|f| get_default_value(f.data_type()))
            .collect();
        Value::Struct(StructValue::new(items, self.clone()))
    }

    fn try_cast(&self, _from: Value) -> Option<Value> {
        // Struct casting is not supported
        None
    }
}

fn get_type_name(dt: &ConcreteDatatype) -> String {
    match dt {
        ConcreteDatatype::Null => "Null",
        ConcreteDatatype::Float32(_) => "Float32",
        ConcreteDatatype::Float64(_) => "Float64",
        ConcreteDatatype::Int8(_) => "Int8",
        ConcreteDatatype::Int16(_) => "Int16",
        ConcreteDatatype::Int32(_) => "Int32",
        ConcreteDatatype::Int64(_) => "Int64",
        ConcreteDatatype::Uint8(_) => "Uint8",
        ConcreteDatatype::Uint16(_) => "Uint16",
        ConcreteDatatype::Uint32(_) => "Uint32",
        ConcreteDatatype::Uint64(_) => "Uint64",
        ConcreteDatatype::String(_) => "String",
        ConcreteDatatype::Bool(_) => "Boolean",
        ConcreteDatatype::Struct(_) => "Struct",
        ConcreteDatatype::List(_) => "List",
    }
    .to_string()
}

fn get_default_value(dt: &ConcreteDatatype) -> Value {
    match dt {
        ConcreteDatatype::Null => Value::Null,
        ConcreteDatatype::Int8(_) => Value::Int8(0),
        ConcreteDatatype::Int16(_) => Value::Int16(0),
        ConcreteDatatype::Int32(_) => Value::Int32(0),
        ConcreteDatatype::Int64(_) => Value::Int64(0),
        ConcreteDatatype::Float32(_) => Value::Float32(0.0),
        ConcreteDatatype::Float64(_) => Value::Float64(0.0),
        ConcreteDatatype::Uint8(_) => Value::Uint8(0),
        ConcreteDatatype::Uint16(_) => Value::Uint16(0),
        ConcreteDatatype::Uint32(_) => Value::Uint32(0),
        ConcreteDatatype::Uint64(_) => Value::Uint64(0),
        ConcreteDatatype::String(_) => Value::String(String::new()),
        ConcreteDatatype::Bool(_) => Value::Bool(false),
        ConcreteDatatype::Struct(_) => {
            // For nested struct, return empty struct value
            // This is a simplified implementation
            Value::Struct(StructValue::new(Vec::new(), super::StructType::new(Arc::new(Vec::new()))))
        }
        ConcreteDatatype::List(_) => {
            use crate::value::ListValue;
            Value::List(ListValue::new(Vec::new(), Arc::new(dt.clone())))
        }
    }
}
