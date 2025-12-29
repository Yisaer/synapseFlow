use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::processor::StreamData;
use flow::FlowInstance;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

pub async fn install_stream_schema(instance: &FlowInstance, columns: &[(String, Vec<Value>)]) {
    let schema_columns = columns
        .iter()
        .map(|(name, values)| {
            let datatype = values
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            ColumnSchema::new("stream".to_string(), name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        "stream".to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}

#[derive(Clone)]
pub struct ColumnCheck {
    pub expected_name: String,
    pub expected_values: Vec<Value>,
}

pub async fn recv_next_json(
    output: &mut tokio::sync::mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::EncodedBytes { payload, .. } => {
                return serde_json::from_slice(&payload).expect("invalid JSON payload")
            }
            StreamData::Control(_) => continue,
            StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            other => panic!("unexpected stream data: {}", other.description()),
        }
    }
}

pub fn build_expected_json(expected_rows: usize, column_checks: &[ColumnCheck]) -> JsonValue {
    let mut rows = Vec::with_capacity(expected_rows);
    for row_idx in 0..expected_rows {
        let mut obj = serde_json::Map::with_capacity(column_checks.len());
        for check in column_checks {
            let Some(value) = check.expected_values.get(row_idx) else {
                panic!(
                    "expected_values length mismatch for column {}, expected at least {} items",
                    check.expected_name, expected_rows
                );
            };
            obj.insert(check.expected_name.clone(), datatype_value_to_json(value));
        }
        rows.push(JsonValue::Object(obj));
    }
    JsonValue::Array(rows)
}

pub fn normalize_json(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(normalize_json).collect())
        }
        JsonValue::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut out = serde_json::Map::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(k, normalize_json(v));
            }
            JsonValue::Object(out)
        }
        other => other,
    }
}

fn datatype_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Float64(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Int8(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Int16(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Int32(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Int64(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Uint8(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Uint16(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Uint32(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Uint64(v) => JsonValue::Number(serde_json::Number::from(*v)),
        Value::Struct(struct_value) => {
            let fields = struct_value.fields().fields();
            let mut map = serde_json::Map::with_capacity(fields.len());
            for (field, item) in fields.iter().zip(struct_value.items().iter()) {
                map.insert(field.name().to_string(), datatype_value_to_json(item));
            }
            JsonValue::Object(map)
        }
        Value::List(list) => {
            JsonValue::Array(list.items().iter().map(datatype_value_to_json).collect())
        }
    }
}
