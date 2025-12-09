//! Decoder abstractions for turning raw bytes into RecordBatch collections.

use crate::model::{CollectionError, Message, RecordBatch, Tuple};
use datatypes::{ConcreteDatatype, ListValue, Schema, StructField, StructType, StructValue, Value};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;

/// Errors that can occur while decoding payloads.
#[derive(thiserror::Error, Debug)]
pub enum CodecError {
    /// Payload was not valid UTF-8 (used by the simple string decoder).
    #[error("invalid utf8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// Payload was not valid JSON.
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    /// RecordBatch construction failed.
    #[error("collection error: {0}")]
    Collection(#[from] CollectionError),
    /// Custom decoder-specific failure.
    #[error("{0}")]
    Other(String),
}

/// Trait implemented by all record decoders.
pub trait RecordDecoder: Send + Sync + 'static {
    /// Convert raw bytes into a RecordBatch.
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError>;

    /// Convert raw bytes into a single tuple.
    fn decode_tuple(&self, payload: &[u8]) -> Result<Tuple, CodecError>;
}

/// Decoder that converts JSON documents (object or array) into a RecordBatch.
pub struct JsonDecoder {
    stream_name: String,
    schema: Arc<Schema>,
    schema_keys: Vec<Arc<str>>,
}

impl JsonDecoder {
    pub fn new(stream_name: impl Into<String>, schema: Arc<Schema>) -> Self {
        let schema_keys = schema
            .column_schemas()
            .iter()
            .map(|col| Arc::<str>::from(col.name.as_str()))
            .collect();
        Self {
            stream_name: stream_name.into(),
            schema,
            schema_keys,
        }
    }

    fn decode_value(&self, json: JsonValue) -> Result<RecordBatch, CodecError> {
        match json {
            JsonValue::Object(map) => self.build_from_object_rows(vec![map]),
            JsonValue::Array(items) => self.decode_array(items),
            other => Err(CodecError::Other(format!(
                "JSON root must be object or array, got {other:?}"
            ))),
        }
    }

    fn decode_array(&self, items: Vec<JsonValue>) -> Result<RecordBatch, CodecError> {
        if items.is_empty() {
            return Ok(RecordBatch::empty());
        }

        if !items.iter().all(|v| v.is_object()) {
            return Err(CodecError::Other(
                "JSON array must contain only objects".to_string(),
            ));
        }

        let rows: Vec<JsonMap<String, JsonValue>> = items
            .into_iter()
            .map(|v| match v {
                JsonValue::Object(map) => map,
                _ => unreachable!("validated object rows"),
            })
            .collect();
        self.build_from_object_rows(rows)
    }

    pub fn decode_tuple(&self, payload: &[u8]) -> Result<Tuple, CodecError> {
        let json = serde_json::from_slice(payload)?;
        let tuples = self.decode_value_to_tuples(json)?;
        match tuples.as_slice() {
            [] => Err(CodecError::Other(
                "JSON payload did not contain any object rows".to_string(),
            )),
            [tuple] => Ok(tuple.clone()),
            _ => Err(CodecError::Other(
                "JSON payload contained multiple rows; expected a single object".to_string(),
            )),
        }
    }

    fn decode_value_to_tuples(&self, json: JsonValue) -> Result<Vec<Tuple>, CodecError> {
        match json {
            JsonValue::Object(map) => self.build_tuples_from_object_rows(vec![map]),
            JsonValue::Array(items) => self.decode_array_to_tuples(items),
            other => Err(CodecError::Other(format!(
                "JSON root must be object or array, got {other:?}"
            ))),
        }
    }

    fn decode_array_to_tuples(&self, items: Vec<JsonValue>) -> Result<Vec<Tuple>, CodecError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        if !items.iter().all(|v| v.is_object()) {
            return Err(CodecError::Other(
                "JSON array must contain only objects".to_string(),
            ));
        }

        let rows: Vec<JsonMap<String, JsonValue>> = items
            .into_iter()
            .map(|v| match v {
                JsonValue::Object(map) => map,
                _ => unreachable!("validated object rows"),
            })
            .collect();
        self.build_tuples_from_object_rows(rows)
    }

    fn build_from_object_rows(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
    ) -> Result<RecordBatch, CodecError> {
        if rows.is_empty() {
            return Ok(RecordBatch::empty());
        }

        let tuples = self.build_tuples_from_object_rows(rows)?;
        Ok(RecordBatch::new(tuples)?)
    }

    fn build_tuples_from_object_rows(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
    ) -> Result<Vec<Tuple>, CodecError> {
        let mut tuples = Vec::with_capacity(rows.len());
        for mut row in rows {
            let mut keys = Vec::with_capacity(self.schema_keys.len() + row.len());
            let mut values = Vec::with_capacity(keys.capacity());
            for (idx, column) in self.schema.column_schemas().iter().enumerate() {
                let value = row
                    .remove(&column.name)
                    .map(|json| json_to_value(&json))
                    .unwrap_or(Value::Null);
                keys.push(self.schema_keys[idx].clone());
                values.push(Arc::new(value));
            }
            for (key, value) in row {
                keys.push(Arc::<str>::from(key.as_str()));
                values.push(Arc::new(json_to_value(&value)));
            }
            let message = Arc::new(Message::new(
                Arc::<str>::from(self.stream_name.as_str()),
                keys,
                values,
            ));
            tuples.push(Tuple::new(vec![message]));
        }
        Ok(tuples)
    }
}

impl RecordDecoder for JsonDecoder {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        let json = serde_json::from_slice(payload)?;
        let batch = self.decode_value(json)?;
        Ok(batch)
    }

    fn decode_tuple(&self, payload: &[u8]) -> Result<Tuple, CodecError> {
        JsonDecoder::decode_tuple(self, payload)
    }
}

fn json_to_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(u) = n.as_u64() {
                Value::Uint64(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        JsonValue::String(s) => Value::String(s.clone()),
        JsonValue::Array(items) => {
            let converted: Vec<Value> = items.iter().map(json_to_value).collect();
            let element_type = converted
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            Value::List(ListValue::new(converted, Arc::new(element_type)))
        }
        JsonValue::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            let mut values = Vec::with_capacity(map.len());

            for (key, val) in map {
                let converted = json_to_value(val);
                let datatype = converted.datatype();
                fields.push(StructField::new(key.clone(), datatype, true));
                values.push(converted);
            }

            Value::Struct(StructValue::new(values, StructType::new(Arc::new(fields))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, Value};

    #[test]
    fn json_decoder_decodes_single_tuple() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "orders".to_string(),
                "amount".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "orders".to_string(),
                "status".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]));
        let decoder = JsonDecoder::new("orders", schema);
        let payload = br#"{"amount":10,"status":"ok"}"#.as_ref();
        let tuple = decoder.decode_tuple(payload).expect("decode tuple");

        let mut columns: Vec<_> = tuple
            .entries()
            .into_iter()
            .map(|((src, col), _)| (src.to_string(), col.to_string()))
            .collect();
        columns.sort();
        assert_eq!(
            columns,
            vec![
                ("orders".to_string(), "amount".to_string()),
                ("orders".to_string(), "status".to_string())
            ]
        );
        assert_eq!(
            tuple.value_by_name("orders", "amount"),
            Some(&Value::Int64(10))
        );
        assert_eq!(
            tuple.value_by_name("orders", "status"),
            Some(&Value::String("ok".to_string()))
        );
    }

    #[test]
    fn json_decoder_rejects_multiple_rows_for_tuple() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "amount".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));
        let decoder = JsonDecoder::new("orders", schema);
        let payload = br#"[{"amount":10},{"amount":20}]"#.as_ref();
        let err = decoder
            .decode_tuple(payload)
            .expect_err("multiple rows fail");
        assert!(format!("{err}").contains("multiple rows"));
    }
}
