//! Encoder abstractions for turning in-memory [`Collection`]s into outbound payloads.

use crate::model::{Collection, Tuple};
use crate::planner::physical::ByIndexProjection;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::sync::Arc;

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
    /// Convert a tuple into a single payload.
    fn encode_tuple(&self, tuple: &Tuple) -> Result<Vec<u8>, EncodeError>;
    /// Whether this encoder supports streaming aggregation.
    fn supports_streaming(&self) -> bool {
        false
    }
    /// Whether this encoder supports index-based lazy materialization (`ByIndexProjection`).
    fn supports_index_lazy_materialization(&self) -> bool {
        false
    }
    /// Attach a by-index projection spec to enable index-based lazy materialization.
    fn with_by_index_projection(
        self: Arc<Self>,
        _spec: Arc<ByIndexProjection>,
    ) -> Result<Arc<dyn CollectionEncoder>, EncodeError> {
        Err(EncodeError::Other(
            "index lazy materialization is not supported for this encoder".to_string(),
        ))
    }
    /// Start a streaming session if supported.
    fn start_stream(&self) -> Option<Box<dyn CollectionEncoderStream>> {
        None
    }
}

/// Stateful encoder stream used for incremental encoding.
pub trait CollectionEncoderStream: Send {
    /// Append a tuple into the stream buffer.
    fn append(&mut self, tuple: &Tuple) -> Result<(), EncodeError>;
    /// Append an entire collection by iterating its rows.
    fn append_collection(&mut self, collection: &dyn Collection) -> Result<(), EncodeError> {
        for tuple in collection.rows() {
            self.append(tuple)?;
        }
        Ok(())
    }
    /// Finalize the stream and emit the payload.
    fn finish(self: Box<Self>) -> Result<Vec<u8>, EncodeError>;
}

/// Encoder that emits the entire collection as a JSON array of row objects.
pub struct JsonEncoder {
    id: String,
    props: JsonMap<String, JsonValue>,
    by_index_projection: Option<Arc<ByIndexProjection>>,
}

impl JsonEncoder {
    /// Create a new JSON encoder with the provided identifier.
    pub fn new(id: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        let id = id.into();
        Self {
            id,
            props,
            by_index_projection: None,
        }
    }

    /// Access encoder props (currently unused by JSON encoder).
    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }

    /// Encode a tuple as a JSON object payload.
    pub fn encode_tuple(&self, tuple: &Tuple) -> Result<Vec<u8>, EncodeError> {
        self.encode_tuple_impl(tuple)
    }

    fn encode_tuple_impl(&self, tuple: &Tuple) -> Result<Vec<u8>, EncodeError> {
        serde_json::to_vec(&tuple_to_json_maybe_by_index_projection(
            tuple,
            self.by_index_projection.as_deref(),
        )?)
        .map_err(EncodeError::Serialization)
    }
}

impl CollectionEncoder for JsonEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn encode(&self, collection: &dyn Collection) -> Result<Vec<u8>, EncodeError> {
        let rows = collection.rows();
        let payload = if rows.is_empty() {
            serde_json::to_vec(&JsonValue::Array(Vec::new()))
        } else {
            let mut json_rows = Vec::with_capacity(rows.len());
            for tuple in rows {
                json_rows.push(tuple_to_json_maybe_by_index_projection(
                    tuple,
                    self.by_index_projection.as_deref(),
                )?);
            }
            serde_json::to_vec(&JsonValue::Array(json_rows))
        }
        .map_err(EncodeError::Serialization)?;
        Ok(payload)
    }

    fn encode_tuple(&self, tuple: &Tuple) -> Result<Vec<u8>, EncodeError> {
        self.encode_tuple_impl(tuple)
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_index_lazy_materialization(&self) -> bool {
        true
    }

    fn with_by_index_projection(
        self: Arc<Self>,
        spec: Arc<ByIndexProjection>,
    ) -> Result<Arc<dyn CollectionEncoder>, EncodeError> {
        Ok(Arc::new(Self {
            id: self.id.clone(),
            props: self.props.clone(),
            by_index_projection: Some(spec),
        }))
    }

    fn start_stream(&self) -> Option<Box<dyn CollectionEncoderStream>> {
        Some(Box::new(JsonStreamingEncoder::new(
            self.by_index_projection.clone(),
        )))
    }
}

struct JsonStreamingEncoder {
    payload: Vec<u8>,
    is_first_row: bool,
    by_index_projection: Option<Arc<ByIndexProjection>>,
}

impl JsonStreamingEncoder {
    fn new(by_index_projection: Option<Arc<ByIndexProjection>>) -> Self {
        Self {
            payload: vec![b'['],
            is_first_row: true,
            by_index_projection,
        }
    }
}

impl CollectionEncoderStream for JsonStreamingEncoder {
    fn append(&mut self, tuple: &Tuple) -> Result<(), EncodeError> {
        if self.is_first_row {
            self.is_first_row = false;
        } else {
            self.payload.push(b',');
        }

        serde_json::to_writer(
            &mut self.payload,
            &tuple_to_json_maybe_by_index_projection(tuple, self.by_index_projection.as_deref())?,
        )
        .map_err(EncodeError::Serialization)?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>, EncodeError> {
        let mut encoder = *self;
        encoder.payload.push(b']');
        Ok(encoder.payload)
    }
}

use datatypes::Value;

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

fn tuple_to_json(tuple: &Tuple) -> JsonValue {
    let mut json_row = JsonMap::with_capacity(tuple.len());
    for ((_, column_name), value) in tuple.entries() {
        json_row.insert(column_name.to_string(), value_to_json(value));
    }
    JsonValue::Object(json_row)
}

fn tuple_to_json_maybe_by_index_projection(
    tuple: &Tuple,
    by_index_projection: Option<&ByIndexProjection>,
) -> Result<JsonValue, EncodeError> {
    let Some(by_index_projection) = by_index_projection else {
        return Ok(tuple_to_json(tuple));
    };

    let mut json_row = JsonMap::with_capacity(by_index_projection.columns().len());
    if let Some(affiliate) = tuple.affiliate() {
        for (key, value) in affiliate.entries() {
            json_row.insert(key.as_str().to_string(), value_to_json(value));
        }
    }

    for column in by_index_projection.columns().iter() {
        let value = tuple
            .value_by_index(column.source_name.as_ref(), column.column_index)
            .ok_or_else(|| {
                EncodeError::Other(format!(
                    "by_index_projection column not found: {}#{}",
                    column.source_name.as_ref(),
                    column.column_index
                ))
            })?;
        json_row.insert(
            column.output_name.as_ref().to_string(),
            value_to_json(value),
        );
    }

    Ok(JsonValue::Object(json_row))
}

fn number_from_f64(value: f64) -> JsonValue {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{batch_from_columns_simple, Message, Tuple};
    use datatypes::Value;
    use std::sync::Arc;

    #[test]
    fn json_encoder_emits_single_payload() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10), Value::Int64(20)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![
                    Value::String("ok".to_string()),
                    Value::String("fail".to_string()),
                ],
            ),
        ])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", JsonMap::new());
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

    #[test]
    fn json_encoder_encodes_tuple() {
        let keys = vec![Arc::<str>::from("amount"), Arc::<str>::from("status")];
        let values = vec![
            Arc::new(Value::Int64(5)),
            Arc::new(Value::String("ok".to_string())),
        ];
        let message = Arc::new(Message::new(Arc::<str>::from("orders"), keys, values));
        let tuple = Tuple::new(vec![message]);
        let encoder = JsonEncoder::new("json", JsonMap::new());
        let payload = encoder.encode_tuple(&tuple).expect("encode tuple");

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!({"amount":5, "status":"ok"}));
    }

    #[test]
    fn json_encoder_streaming() {
        let encoder = JsonEncoder::new("json", JsonMap::new());
        assert!(
            encoder.supports_streaming(),
            "json encoder should be streaming"
        );
        let mut stream = encoder.start_stream().expect("stream");

        let batch1 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(1)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("ok".to_string())],
            ),
        ])
        .expect("batch1");
        stream.append_collection(&batch1).expect("append batch1");

        let batch2 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(2)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("fail".to_string())],
            ),
        ])
        .expect("batch2");
        for tuple in batch2.rows() {
            stream.append(tuple).expect("stream append");
        }

        let payload = stream.finish().expect("stream finish");
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"amount":1, "status":"ok"},
                {"amount":2, "status":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_streaming_empty_payload_is_array() {
        let encoder = JsonEncoder::new("json", JsonMap::new());
        let stream = encoder.start_stream().expect("stream");
        let payload = stream.finish().expect("stream finish");

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([]));
    }
}
