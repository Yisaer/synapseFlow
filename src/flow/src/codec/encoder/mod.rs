//! Encoder abstractions for turning in-memory [`Collection`]s into outbound payloads.

use crate::model::Collection;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Errors that can occur during encoding.
#[derive(thiserror::Error, Debug)]
pub enum EncodeError {
    /// Failed to serialize into the requested format.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Custom error.
    #[error("{0}")]
    Other(String),
}

/// Trait implemented by every sink encoder.
pub trait CollectionEncoder: Send + Sync + 'static {
    /// Identifier for metrics/logging.
    fn id(&self) -> &str;
    /// Convert a collection into a single payload.
    fn encode(&self, collection: &dyn Collection) -> Result<Vec<u8>, EncodeError>;
}

/// Encoder that emits the entire collection as a JSON array of row objects.
pub struct JsonEncoder {
    id: String,
}

impl JsonEncoder {
    /// Create a new JSON encoder with the provided identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

impl CollectionEncoder for JsonEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn encode(&self, collection: &dyn Collection) -> Result<Vec<u8>, EncodeError> {
        let num_rows = collection.num_rows();
        let payload = if num_rows == 0 || collection.num_columns() == 0 {
            serde_json::to_vec(&JsonValue::Array(Vec::new()))
        } else {
            let columns = collection.columns();
            let mut rows = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let mut json_row = JsonMap::with_capacity(columns.len());
                for column in columns {
                    let key = column_identifier(column);
                    let value = column
                        .get(row_idx)
                        .map(value_to_json)
                        .unwrap_or(JsonValue::Null);
                    json_row.insert(key, value);
                }
                rows.push(JsonValue::Object(json_row));
            }
            serde_json::to_vec(&JsonValue::Array(rows))
        }
        .map_err(EncodeError::Serialization)?;
        Ok(payload)
    }
}

use crate::model::Column;
use datatypes::Value;

fn column_identifier(column: &Column) -> String {
    let name = column.name();
    name.to_string()
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Float32(v) => number_from_f64(*v as f64),
        Value::Float64(v) => number_from_f64(*v),
        Value::Int8(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int16(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int32(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint8(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint16(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint32(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Struct(struct_value) => {
            let mut map = JsonMap::new();
            let fields = struct_value.fields().fields();
            for (field, item) in fields.iter().zip(struct_value.items().iter()) {
                map.insert(field.name().to_string(), value_to_json(item));
            }
            JsonValue::Object(map)
        }
        Value::List(list) => {
            let values = list.items().iter().map(value_to_json).collect::<Vec<_>>();
            JsonValue::Array(values)
        }
    }
}

fn number_from_f64(value: f64) -> JsonValue {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, RecordBatch};
    use datatypes::Value;

    #[test]
    fn json_encoder_emits_single_payload() {
        let column_a = Column::new(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(10), Value::Int64(20)],
        );
        let column_b = Column::new(
            "orders".to_string(),
            "status".to_string(),
            vec![
                Value::String("ok".to_string()),
                Value::String("fail".to_string()),
            ],
        );
        let batch = RecordBatch::new(vec![column_a, column_b]).expect("valid batch");

        let encoder = JsonEncoder::new("json");
        let payload = encoder.encode(&batch).expect("encode collection");

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"amount":10, "status":"ok"},
                {"amount":20, "status":"fail"}
            ])
        );
    }
}
