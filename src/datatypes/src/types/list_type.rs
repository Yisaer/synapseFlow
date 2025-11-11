use std::sync::Arc;

use crate::datatypes::DataType;
use crate::datatypes::ConcreteDatatype;
use crate::value::{ListValue, Value};

/// List type, containing element type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListType {
    /// The type of List's item
    item_type: Arc<ConcreteDatatype>,
}

impl ListType {
    /// Create a new `ListType` whose item's data type is `item_type`
    pub fn new(item_type: Arc<ConcreteDatatype>) -> Self {
        ListType { item_type }
    }

    /// Returns the item data type
    pub fn item_type(&self) -> &ConcreteDatatype {
        &self.item_type
    }
}

impl DataType for ListType {
    fn name(&self) -> String {
        format!("List<{}>", get_type_name(&self.item_type))
    }

    fn default_value(&self) -> Value {
        Value::List(ListValue::new(Vec::new(), self.item_type.clone()))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::List(v) => {
                // Check if the datatype matches
                if v.datatype() == self.item_type.as_ref() {
                    Some(Value::List(v))
                } else {
                    None
                }
            }
            _ => None,
        }
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
