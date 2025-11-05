use datatypes::{ConcreteDatatype, DataType, ListValue, Schema, StructValue, Value};
use std::sync::Arc;
use serde_json::Value as JsonValue;

use crate::row::Row;

/// Tuple represents a row of data with its associated schema.
///
/// A Tuple combines a Schema (which defines the structure and types of columns)
/// with a Row (which contains the actual data values).
#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    /// The schema defining the structure of this tuple
    schema: Schema,
    /// The row containing the actual data values
    row: Row,
}

impl Tuple {
    /// Create a new tuple with the given schema and row
    ///
    /// # Panics
    ///
    /// Panics if the number of values in the row does not match the number of columns in the schema
    pub fn new(schema: Schema, row: Row) -> Self {
        assert_eq!(
            schema.column_schemas().len(),
            row.len(),
            "Row length must match schema column count"
        );
        Self { schema, row }
    }

    /// Create a new tuple from a schema and a vector of values
    pub fn from_values(schema: Schema, values: Vec<Value>) -> Self {
        let row = Row::new(values);
        Self::new(schema, row)
    }

    /// Get a reference to the schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get a reference to the row
    pub fn row(&self) -> &Row {
        &self.row
    }

    /// Get a mutable reference to the row
    pub fn row_mut(&mut self) -> &mut Row {
        &mut self.row
    }

    /// Get the value at the given column index
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.row.get(idx)
    }

    /// Get the value for a column by name
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.schema
            .column_schema_by_name(name)
            .and_then(|col_schema| {
                self.schema
                    .column_schemas()
                    .iter()
                    .position(|cs| cs.name == col_schema.name)
                    .and_then(|idx| self.row.get(idx))
            })
    }

    /// Get the number of columns in this tuple
    pub fn len(&self) -> usize {
        self.row.len()
    }

    /// Returns true if the tuple contains no columns
    pub fn is_empty(&self) -> bool {
        self.row.is_empty()
    }

    /// Consume the tuple and return the schema and row
    pub fn into_parts(self) -> (Schema, Row) {
        (self.schema, self.row)
    }

    /// Create a new tuple from a schema and JSON bytes
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema defining the structure of the tuple
    /// * `json_bytes` - A byte slice containing a JSON object
    ///
    /// # Returns
    ///
    /// Returns a `Result` with the created `Tuple` or an error if JSON parsing fails
    /// or if the JSON doesn't match the schema.
    pub fn new_from_json(schema: Schema, json_bytes: &[u8]) -> Result<Self, JsonError> {
        let json_value: JsonValue = serde_json::from_slice(json_bytes)
            .map_err(|e| JsonError::ParseError(e.to_string()))?;

        let json_obj = json_value
            .as_object()
            .ok_or_else(|| JsonError::InvalidFormat("Expected JSON object".to_string()))?;

        let mut values = Vec::new();
        for col_schema in schema.column_schemas() {
            let json_val = json_obj.get(&col_schema.name);
            let value = match json_val {
                Some(val) => json_value_to_value(val, &col_schema.data_type)?,
                None => get_default_value(&col_schema.data_type),
            };
            values.push(value);
        }

        let row = Row::new(values);
        Ok(Self::new(schema, row))
    }
}

/// Convert a JSON value to a Value based on the expected data type
/// For basic types, only supports: string, bool, int64, float64
/// For complex types, supports: struct (from JSON object), list (from JSON array)
fn json_value_to_value(json_val: &JsonValue, data_type: &ConcreteDatatype) -> Result<Value, JsonError> {
    match (json_val, data_type) {
        // Null values use default
        (JsonValue::Null, _) => Ok(get_default_value(data_type)),
        
        // Basic types: String
        (JsonValue::String(s), ConcreteDatatype::String(_)) => Ok(Value::String(s.clone())),
        
        // Basic types: Bool
        (JsonValue::Bool(b), ConcreteDatatype::Bool(_)) => Ok(Value::Bool(*b)),
        
        // Basic types: Int64 (from JSON Number)
        (JsonValue::Number(n), ConcreteDatatype::Int64(_)) => {
            n.as_i64()
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Int64)
        }
        
        // Basic types: Float64 (from JSON Number)
        (JsonValue::Number(n), ConcreteDatatype::Float64(_)) => {
            n.as_f64()
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Float64".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Float64)
        }
        
        // Complex types: Struct (from JSON object)
        (JsonValue::Object(obj), ConcreteDatatype::Struct(struct_type)) => {
            let fields = struct_type.fields();
            let mut values = Vec::new();
            
            for field in fields.iter() {
                let json_val = obj.get(field.name());
                let value = match json_val {
                    Some(val) => json_value_to_value(val, field.data_type())?,
                    None => {
                        if field.is_nullable() {
                            get_default_value(field.data_type())
                        } else {
                            return Err(JsonError::TypeMismatch {
                                expected: format!("field {} in struct", field.name()),
                                actual: "null".to_string(),
                            });
                        }
                    }
                };
                values.push(value);
            }
            
            Ok(Value::Struct(StructValue::new(values, struct_type.clone())))
        }
        
        // Complex types: List (from JSON array)
        (JsonValue::Array(arr), ConcreteDatatype::List(list_type)) => {
            let item_type = list_type.item_type();
            let items: Result<Vec<Value>, JsonError> = arr
                .iter()
                .map(|json_item| json_value_to_value(json_item, item_type))
                .collect();
            
            Ok(Value::List(ListValue::new(
                items?,
                Arc::new(item_type.clone()),
            )))
        }
        
        // Type mismatch
        _ => Err(JsonError::TypeMismatch {
            expected: format!("{:?}", data_type),
            actual: format!("{:?}", json_val),
        }),
    }
}

/// Get default value for a ConcreteDatatype
fn get_default_value(data_type: &ConcreteDatatype) -> Value {
    match data_type {
        ConcreteDatatype::Int8(t) => t.default_value(),
        ConcreteDatatype::Int16(t) => t.default_value(),
        ConcreteDatatype::Int32(t) => t.default_value(),
        ConcreteDatatype::Int64(t) => t.default_value(),
        ConcreteDatatype::Float32(t) => t.default_value(),
        ConcreteDatatype::Float64(t) => t.default_value(),
        ConcreteDatatype::Uint8(t) => t.default_value(),
        ConcreteDatatype::Uint16(t) => t.default_value(),
        ConcreteDatatype::Uint32(t) => t.default_value(),
        ConcreteDatatype::Uint64(t) => t.default_value(),
        ConcreteDatatype::String(t) => t.default_value(),
        ConcreteDatatype::Bool(t) => t.default_value(),
        ConcreteDatatype::Struct(t) => t.default_value(),
        ConcreteDatatype::List(t) => t.default_value(),
    }
}

/// Error type for JSON parsing and conversion
#[derive(Debug, Clone, PartialEq)]
pub enum JsonError {
    /// JSON parsing error
    ParseError(String),
    /// Invalid JSON format
    InvalidFormat(String),
    /// Type mismatch error
    TypeMismatch {
        expected: String,
        actual: String,
    },
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonError::ParseError(msg) => write!(f, "JSON parse error: {}", msg),
            JsonError::InvalidFormat(msg) => write!(f, "Invalid JSON format: {}", msg),
            JsonError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
        }
    }
}

impl std::error::Error for JsonError {}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{ColumnSchema, Int64Type, StringType, Float64Type, BooleanType};

    #[test]
    fn test_new_from_json() {
        let schema = Schema::new(vec![
            ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
            ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("score".to_string(), ConcreteDatatype::Float64(Float64Type)),
            ColumnSchema::new("active".to_string(), ConcreteDatatype::Bool(BooleanType)),
        ]);

        let json = br#"{"id": 1, "name": "Alice", "age": 25, "score": 98.5, "active": true}"#;
        let tuple = Tuple::new_from_json(schema.clone(), json).unwrap();

        assert_eq!(tuple.len(), 5);
        assert_eq!(tuple.get(0), Some(&Value::Int64(1)));
        assert_eq!(tuple.get(1), Some(&Value::String("Alice".to_string())));
        assert_eq!(tuple.get(2), Some(&Value::Int64(25)));
        assert_eq!(tuple.get(3), Some(&Value::Float64(98.5)));
        assert_eq!(tuple.get(4), Some(&Value::Bool(true)));

        // Test with missing fields (should use default values)
        let json2 = br#"{"id": 2, "name": "Bob"}"#;
        let tuple2 = Tuple::new_from_json(schema.clone(), json2).unwrap();
        
        assert_eq!(tuple2.get(0), Some(&Value::Int64(2)));
        assert_eq!(tuple2.get(1), Some(&Value::String("Bob".to_string())));
        assert_eq!(tuple2.get(2), Some(&Value::Int64(0))); // default for Int64
        assert_eq!(tuple2.get(3), Some(&Value::Float64(0.0))); // default for Float64
        assert_eq!(tuple2.get(4), Some(&Value::Bool(false))); // default for Bool

        // Test with null values (should use default values)
        let json3 = br#"{"id": 3, "name": null, "age": 30, "score": null, "active": null}"#;
        let tuple3 = Tuple::new_from_json(schema, json3).unwrap();
        
        assert_eq!(tuple3.get(0), Some(&Value::Int64(3)));
        assert_eq!(tuple3.get(1), Some(&Value::String(String::new()))); // default for String
        assert_eq!(tuple3.get(2), Some(&Value::Int64(30)));
        assert_eq!(tuple3.get(3), Some(&Value::Float64(0.0))); // default for Float64
        assert_eq!(tuple3.get(4), Some(&Value::Bool(false))); // default for Bool
    }
}
