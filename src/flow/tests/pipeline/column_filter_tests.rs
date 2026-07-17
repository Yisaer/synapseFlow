//! Integration tests for per-sink column filtering (include_columns / exclude_columns).

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryData, MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::model::batch_from_columns_simple;
use flow::pipeline::{MemorySinkProps, PipelineDefinition, SinkDefinition, SinkProps, SinkType};
use flow::planner::sink::SinkOutputConfig;
use flow::FlowInstance;
use flow::{CreatePipelineRequest, PipelineStopMode};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

use super::common::publish_input_collection;

static TOPIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_topic(prefix: &str) -> String {
    let counter = TOPIC_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("tests.column_filter.{prefix}.{counter}")
}

fn make_int64_schema(stream_name: &str, columns: &[&str]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|col| {
                ColumnSchema::new(
                    stream_name.to_string(),
                    col.to_string(),
                    ConcreteDatatype::Int64(Int64Type),
                )
            })
            .collect(),
    ))
}

async fn recv_json(
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
                return serde_json::from_slice(payload.as_ref()).expect("invalid JSON payload");
            }
            Ok(MemoryData::Collection(_)) => {
                panic!("unexpected collection payload on bytes topic");
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }
}

async fn install_stream(
    instance: &FlowInstance,
    stream_name: &str,
    columns: &[&str],
    input_topic: &str,
) {
    let schema = make_int64_schema(stream_name, columns);
    let def = StreamDefinition::new(
        stream_name.to_string(),
        schema,
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::new("none".to_string(), JsonMap::new()),
    );
    instance
        .create_stream(def, false)
        .await
        .expect("create stream");
}

fn build_memory_json_sink(output_topic: &str) -> SinkDefinition {
    SinkDefinition::new(
        format!("sink_{output_topic}"),
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.to_string())),
    )
}

fn publish_two_rows(
    stream_name: &str,
    col_a: &str,
    vals_a: Vec<Value>,
    col_b: &str,
    vals_b: Vec<Value>,
    extra_cols: Vec<(&str, Vec<Value>)>,
) -> Box<dyn flow::Collection> {
    let mut columns: Vec<(String, String, Vec<Value>)> = vec![
        (stream_name.to_string(), col_a.to_string(), vals_a),
        (stream_name.to_string(), col_b.to_string(), vals_b),
    ];
    for (name, vals) in extra_cols {
        columns.push((stream_name.to_string(), name.to_string(), vals));
    }
    Box::new(batch_from_columns_simple(columns).expect("build batch"))
}

#[tokio::test]
async fn column_filter_single_sink_include() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let input_topic = unique_topic("single_include.input");
    let output_topic = unique_topic("single_include.output");

    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output");
    install_stream(&instance, "stream", &["a", "b", "c"], &input_topic).await;

    let sink = build_memory_json_sink(&output_topic).with_output(
        SinkOutputConfig::default().with_include_columns(["a".to_string(), "c".to_string()]),
    );

    let pipeline = PipelineDefinition::new(
        "pipe_single_include".to_string(),
        "SELECT a, b, c FROM stream",
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline("pipe_single_include")
        .await
        .expect("start");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe");

    let batch = publish_two_rows(
        "stream",
        "a",
        vec![Value::Int64(1)],
        "b",
        vec![Value::Int64(2)],
        vec![("c", vec![Value::Int64(3)])],
    );
    publish_input_collection(&instance, &input_topic, batch, Duration::from_secs(5)).await;

    let result = recv_json(&mut output, Duration::from_secs(5)).await;
    let row = &result.as_array().unwrap()[0];
    assert_eq!(row["a"], JsonValue::Number(serde_json::Number::from(1)));
    assert_eq!(row["c"], JsonValue::Number(serde_json::Number::from(3)));
    assert!(row.get("b").is_none(), "b should be excluded");

    instance
        .stop_pipeline(
            "pipe_single_include",
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop");
    instance
        .delete_pipeline("pipe_single_include")
        .await
        .expect("delete");
}

#[tokio::test]
async fn column_filter_two_sinks_different_include() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let input_topic = unique_topic("two_sinks.input");
    let output_a = unique_topic("two_sinks.output_a");
    let output_b = unique_topic("two_sinks.output_b");

    for topic in [&input_topic, &output_a, &output_b] {
        let kind = if topic == &input_topic {
            MemoryTopicKind::Collection
        } else {
            MemoryTopicKind::Bytes
        };
        instance
            .declare_memory_topic(topic, kind, DEFAULT_MEMORY_PUBSUB_CAPACITY)
            .expect("declare");
    }
    install_stream(&instance, "stream", &["a", "b", "c"], &input_topic).await;

    let sink_a = build_memory_json_sink(&output_a).with_output(
        SinkOutputConfig::default().with_include_columns(["a".to_string(), "b".to_string()]),
    );
    let sink_b = build_memory_json_sink(&output_b)
        .with_output(SinkOutputConfig::default().with_include_columns(["c".to_string()]));

    let pipeline = PipelineDefinition::new(
        "pipe_two_sinks_include".to_string(),
        "SELECT a, b, c FROM stream",
        vec![sink_a, sink_b],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create");
    instance
        .start_pipeline("pipe_two_sinks_include")
        .await
        .expect("start");

    let mut out_a = instance
        .open_memory_subscribe_bytes(&output_a)
        .expect("subscribe a");
    let mut out_b = instance
        .open_memory_subscribe_bytes(&output_b)
        .expect("subscribe b");

    let batch = publish_two_rows(
        "stream",
        "a",
        vec![Value::Int64(1)],
        "b",
        vec![Value::Int64(2)],
        vec![("c", vec![Value::Int64(3)])],
    );
    publish_input_collection(&instance, &input_topic, batch, Duration::from_secs(5)).await;

    let r_a = recv_json(&mut out_a, Duration::from_secs(5)).await;
    let r_b = recv_json(&mut out_b, Duration::from_secs(5)).await;
    let row_a = &r_a.as_array().unwrap()[0];
    let row_b = &r_b.as_array().unwrap()[0];
    assert_eq!(row_a["a"], JsonValue::Number(serde_json::Number::from(1)));
    assert_eq!(row_a["b"], JsonValue::Number(serde_json::Number::from(2)));
    assert!(row_a.get("c").is_none());
    assert_eq!(row_b["c"], JsonValue::Number(serde_json::Number::from(3)));
    assert!(row_b.get("a").is_none());
    assert!(row_b.get("b").is_none());

    instance
        .stop_pipeline(
            "pipe_two_sinks_include",
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop");
    instance
        .delete_pipeline("pipe_two_sinks_include")
        .await
        .expect("delete");
}

#[tokio::test]
async fn column_filter_single_sink_exclude() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let input_topic = unique_topic("exclude.input");
    let output_topic = unique_topic("exclude.output");
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output");
    install_stream(&instance, "stream", &["a", "b", "c"], &input_topic).await;

    let sink = build_memory_json_sink(&output_topic)
        .with_output(SinkOutputConfig::default().with_exclude_columns(["b".to_string()]));
    let pipeline = PipelineDefinition::new(
        "pipe_exclude".to_string(),
        "SELECT a, b, c FROM stream",
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create");
    instance
        .start_pipeline("pipe_exclude")
        .await
        .expect("start");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe");
    let batch = publish_two_rows(
        "stream",
        "a",
        vec![Value::Int64(1)],
        "b",
        vec![Value::Int64(2)],
        vec![("c", vec![Value::Int64(3)])],
    );
    publish_input_collection(&instance, &input_topic, batch, Duration::from_secs(5)).await;

    let result = recv_json(&mut output, Duration::from_secs(5)).await;
    let row = &result.as_array().unwrap()[0];
    assert_eq!(row["a"], JsonValue::Number(serde_json::Number::from(1)));
    assert_eq!(row["c"], JsonValue::Number(serde_json::Number::from(3)));
    assert!(row.get("b").is_none());

    instance
        .stop_pipeline(
            "pipe_exclude",
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop");
    instance
        .delete_pipeline("pipe_exclude")
        .await
        .expect("delete");
}

#[tokio::test]
async fn column_filter_delta_include() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let input_topic = unique_topic("delta_include.input");
    let output_topic = unique_topic("delta_include.output");
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output");
    install_stream(&instance, "stream", &["a", "b"], &input_topic).await;

    let sink = build_memory_json_sink(&output_topic)
        .with_output(SinkOutputConfig::delta().with_include_columns(["a".to_string()]));
    let pipeline = PipelineDefinition::new(
        "pipe_delta_include".to_string(),
        "SELECT a, b FROM stream",
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create");
    instance
        .start_pipeline("pipe_delta_include")
        .await
        .expect("start");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe");

    // Two rows: {a:1,b:2}, {a:1,b:3} — a unchanged, only b changes
    let batch = publish_two_rows(
        "stream",
        "a",
        vec![Value::Int64(1), Value::Int64(1)],
        "b",
        vec![Value::Int64(2), Value::Int64(3)],
        vec![],
    );
    publish_input_collection(&instance, &input_topic, batch, Duration::from_secs(5)).await;

    let result = recv_json(&mut output, Duration::from_secs(5)).await;
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 2);

    // Row 1: a=1 (new row, column emitted)
    let row1 = &rows[0];
    assert_eq!(row1["a"], JsonValue::Number(serde_json::Number::from(1)));
    assert!(row1.get("b").is_none(), "b excluded");

    // Row 2: a unchanged, so the delta object is empty.
    let row2 = &rows[1];
    assert!(row2.get("a").is_none());
    assert!(row2.get("b").is_none());

    instance
        .stop_pipeline(
            "pipe_delta_include",
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop");
    instance
        .delete_pipeline("pipe_delta_include")
        .await
        .expect("delete");
}

#[tokio::test]
async fn column_filter_delta_exclude() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let input_topic = unique_topic("delta_exclude.input");
    let output_topic = unique_topic("delta_exclude.output");
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output");
    install_stream(&instance, "stream", &["a", "b", "c"], &input_topic).await;

    let sink = build_memory_json_sink(&output_topic)
        .with_output(SinkOutputConfig::delta().with_exclude_columns(["c".to_string()]));
    let pipeline = PipelineDefinition::new(
        "pipe_delta_exclude".to_string(),
        "SELECT a, b, c FROM stream",
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create");
    instance
        .start_pipeline("pipe_delta_exclude")
        .await
        .expect("start");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe");

    // Two rows: {a:1,b:2,c:3}, {a:4,b:2,c:3} — a changes, b unchanged, c excluded
    let batch = publish_two_rows(
        "stream",
        "a",
        vec![Value::Int64(1), Value::Int64(4)],
        "b",
        vec![Value::Int64(2), Value::Int64(2)],
        vec![("c", vec![Value::Int64(3), Value::Int64(3)])],
    );
    publish_input_collection(&instance, &input_topic, batch, Duration::from_secs(5)).await;

    let result = recv_json(&mut output, Duration::from_secs(5)).await;
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 2);

    // Row 1: {a:1,b:2}
    let row1 = &rows[0];
    assert_eq!(row1["a"], JsonValue::Number(serde_json::Number::from(1)));
    assert_eq!(row1["b"], JsonValue::Number(serde_json::Number::from(2)));
    assert!(row1.get("c").is_none());

    // Row 2: a changed, b unchanged
    let row2 = &rows[1];
    assert_eq!(row2["a"], JsonValue::Number(serde_json::Number::from(4)));
    assert!(row2.get("b").is_none());
    assert!(row2.get("c").is_none());

    instance
        .stop_pipeline(
            "pipe_delta_exclude",
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop");
    instance
        .delete_pipeline("pipe_delta_exclude")
        .await
        .expect("delete");
}

#[tokio::test]
async fn column_filter_include_computed_column() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    let input_topic = unique_topic("computed_include.input");
    let output_topic = unique_topic("computed_include.output");
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output");
    install_stream(&instance, "stream", &["a", "b"], &input_topic).await;

    let sink = build_memory_json_sink(&output_topic).with_output(
        SinkOutputConfig::default().with_include_columns(["a".to_string(), "x".to_string()]),
    );
    let pipeline = PipelineDefinition::new(
        "pipe_computed_include".to_string(),
        "SELECT a, b, a + 1 AS x FROM stream",
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create");
    instance
        .start_pipeline("pipe_computed_include")
        .await
        .expect("start");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe");
    let batch = publish_two_rows(
        "stream",
        "a",
        vec![Value::Int64(5)],
        "b",
        vec![Value::Int64(10)],
        vec![],
    );
    publish_input_collection(&instance, &input_topic, batch, Duration::from_secs(5)).await;

    let result = recv_json(&mut output, Duration::from_secs(5)).await;
    let row = &result.as_array().unwrap()[0];
    assert_eq!(row["a"], JsonValue::Number(serde_json::Number::from(5)));
    assert_eq!(row["x"], JsonValue::Number(serde_json::Number::from(6)));
    assert!(row.get("b").is_none());

    instance
        .stop_pipeline(
            "pipe_computed_include",
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop");
    instance
        .delete_pipeline("pipe_computed_include")
        .await
        .expect("delete");
}
