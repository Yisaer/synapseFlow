//! Encoder abstractions for turning in-memory [`Collection`]s into outbound payloads.
//!
//! Sink processors rely on encoders to transform arbitrary collections into the
//! format required by a downstream connector (e.g. JSON documents).

use crate::model::{Collection, Column};
use datatypes::Value;
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
    /// Convert a collection into one or more payloads.
    ///
    /// Each entry in the returned vector represents a logical message that
    /// should be sent downstream. Returning an empty vector means nothing
    /// should be emitted for the provided collection.
    fn encode(&self, collection: &dyn Collection) -> Result<Vec<Vec<u8>>, EncodeError>;
}

/// Encoder that emits each row as a standalone JSON object.
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

    fn encode(&self, collection: &dyn Collection) -> Result<Vec<Vec<u8>>, EncodeError> {
        let num_rows = collection.num_rows();
        if num_rows == 0 || collection.num_columns() == 0 {
            return Ok(Vec::new());
        }

        let mut payloads = Vec::with_capacity(num_rows);
        let columns = collection.columns();

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
            let serialized = serde_json::to_vec(&JsonValue::Object(json_row))?;
            payloads.push(serialized);
        }

        Ok(payloads)
    }
}

fn column_identifier(column: &Column) -> String {
    if column.source_name().is_empty() {
        column.name().to_string()
    } else {
        format!("{}.{}", column.source_name(), column.name())
    }
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
    fn json_encoder_emits_one_payload_per_row() {
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
        let payloads = encoder.encode(&batch).expect("encode collection");

        assert_eq!(payloads.len(), 2);
        let first = serde_json::from_slice::<serde_json::Value>(&payloads[0]).unwrap();
        assert_eq!(
            first,
            serde_json::json!({"orders.amount":10, "orders.status":"ok"})
        );
    }
}
