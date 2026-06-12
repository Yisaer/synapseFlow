use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{
    MemoryData, MemoryTopicKind, SharedCollection, DEFAULT_MEMORY_PUBSUB_CAPACITY,
};
use flow::model::Collection;
use flow::FlowInstance;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

static TOPIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn sanitize_topic_fragment(fragment: &str) -> String {
    let mut out = String::with_capacity(fragment.len());
    for ch in fragment.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    let out = out.trim_matches('_');
    if out.is_empty() {
        "case".to_string()
    } else {
        out.to_string()
    }
}

pub fn make_memory_topics(test_suite: &str, case_name: &str) -> (String, String) {
    let counter = TOPIC_COUNTER.fetch_add(1, Ordering::Relaxed);
    let suite = sanitize_topic_fragment(test_suite);
    let case_name = sanitize_topic_fragment(case_name);
    let base = format!("tests.{suite}.{case_name}.{counter}");
    (format!("{base}.input"), format!("{base}.output"))
}

pub fn declare_memory_input_output_topics(
    instance: &FlowInstance,
    input_topic: &str,
    output_topic: &str,
) {
    instance
        .declare_memory_topic(
            input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input memory topic");
    instance
        .declare_memory_topic(
            output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output memory topic");
}

pub fn declare_memory_input_output_topics_with_output_kind(
    instance: &FlowInstance,
    input_topic: &str,
    output_topic: &str,
    output_kind: MemoryTopicKind,
) {
    instance
        .declare_memory_topic(
            input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input memory topic");
    instance
        .declare_memory_topic(output_topic, output_kind, DEFAULT_MEMORY_PUBSUB_CAPACITY)
        .expect("declare output memory topic");
}

pub async fn install_memory_stream_schema(
    instance: &FlowInstance,
    input_topic: &str,
    columns: &[(String, Vec<Value>)],
) {
    install_memory_stream_schema_with_name(instance, input_topic, "stream", columns).await;
}

pub async fn install_memory_stream_schema_with_name(
    instance: &FlowInstance,
    input_topic: &str,
    source_name: &str,
    columns: &[(String, Vec<Value>)],
) {
    let schema_columns = columns
        .iter()
        .map(|(name, values)| {
            let datatype = values
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            ColumnSchema::new(source_name.to_string(), name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        source_name.to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::new("none".to_string(), JsonMap::new()),
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
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) -> JsonValue {
    use tokio::sync::broadcast::error::RecvError;

    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output");

        match item {
            Ok(MemoryData::Bytes(payload)) => {
                return serde_json::from_slice(payload.as_ref()).expect("invalid JSON payload")
            }
            Ok(MemoryData::Collection(_)) => {
                panic!("unexpected collection payload on bytes topic")
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }
}

/// Receive the next JSON output, or return `fallback` if nothing arrives within
/// `timeout_duration`. Used for empty results: a predicate that matches nothing now
/// emits no output at all (the empty collection is dropped at the filter), so "no
/// output" and "an explicit empty collection" are both valid representations of an
/// empty result.
pub async fn recv_next_json_or(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
    fallback: JsonValue,
) -> JsonValue {
    use tokio::sync::broadcast::error::RecvError;

    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, output.recv()).await {
            Err(_) => return fallback,
            Ok(Ok(MemoryData::Bytes(payload))) => {
                return serde_json::from_slice(payload.as_ref()).expect("invalid JSON payload")
            }
            Ok(Ok(MemoryData::Collection(_))) => {
                panic!("unexpected collection payload on bytes topic")
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => return fallback,
        }
    }
}

pub async fn assert_no_json_output(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) {
    use tokio::sync::broadcast::error::RecvError;

    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, output.recv()).await {
            Err(_) => return,
            Ok(Ok(MemoryData::Bytes(payload))) => {
                panic!(
                    "unexpected JSON payload when no output expected: {}",
                    String::from_utf8_lossy(payload.as_ref())
                );
            }
            Ok(Ok(MemoryData::Collection(_))) => {
                panic!("unexpected collection payload on bytes topic")
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => panic!("pipeline output topic closed"),
        }
    }
}

pub async fn recv_next_collection(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) -> SharedCollection {
    use tokio::sync::broadcast::error::RecvError;

    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output");

        match item {
            Ok(MemoryData::Collection(payload)) => return payload,
            Ok(MemoryData::Bytes(_)) => panic!("unexpected bytes payload on collection topic"),
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }
}

pub async fn publish_input_collection(
    instance: &FlowInstance,
    input_topic: &str,
    collection: Box<dyn Collection>,
    timeout_duration: Duration,
) {
    instance
        .wait_for_memory_subscribers(
            input_topic,
            MemoryTopicKind::Collection,
            1,
            timeout_duration,
        )
        .await
        .expect("wait for memory source subscriber");

    let publisher = instance
        .open_memory_publisher_collection(input_topic)
        .expect("open memory publisher");
    publisher
        .publish_collection(SharedCollection::from_box(collection))
        .expect("publish collection");
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
            insert_expected_json_field(&mut obj, check.expected_name.clone(), value);
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
        Value::Bytes(v) => {
            use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
            use base64::Engine;

            JsonValue::String(BASE64_STANDARD.encode(v.as_ref()))
        }
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
        Value::Timestamp(v) => v
            .to_rfc3339_utc()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::Struct(struct_value) => {
            let fields = struct_value.fields().fields();
            let mut map = serde_json::Map::with_capacity(fields.len());
            for (field, item) in fields.iter().zip(struct_value.items().iter()) {
                insert_expected_json_field(&mut map, field.name().to_string(), item);
            }
            JsonValue::Object(map)
        }
        Value::List(list) => {
            JsonValue::Array(list.items().iter().map(datatype_value_to_json).collect())
        }
    }
}

fn insert_expected_json_field(
    json_object: &mut serde_json::Map<String, JsonValue>,
    key: String,
    value: &Value,
) {
    if matches!(value, Value::Null) {
        return;
    }

    json_object.insert(key, datatype_value_to_json(value));
}
