//! Tests for the public pipeline creation helpers.
//!
//! This module exercises both `create_pipeline_with_log_sink` (default/testing)
//! and the customizable `create_pipeline` API that accepts user-defined sinks.

use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::model::batch_from_columns_simple;
use flow::planner::sink::{
    CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig,
};
use flow::processor::StreamData;
use flow::FlowInstance;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn recv_next_json(
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

fn normalize_json(value: JsonValue) -> JsonValue {
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

fn sort_json_array_by_key(mut value: JsonValue, key: &str) -> JsonValue {
    let JsonValue::Array(items) = &mut value else {
        panic!("expected JSON array");
    };
    items.sort_by(|a, b| {
        let JsonValue::Object(a_obj) = a else {
            return std::cmp::Ordering::Equal;
        };
        let JsonValue::Object(b_obj) = b else {
            return std::cmp::Ordering::Equal;
        };
        let a_val = a_obj.get(key);
        let b_val = b_obj.get(key);
        match (a_val, b_val) {
            (Some(JsonValue::Number(a_num)), Some(JsonValue::Number(b_num))) => {
                a_num.as_i64().cmp(&b_num.as_i64())
            }
            (Some(a), Some(b)) => format!("{a:?}").cmp(&format!("{b:?}")),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    value
}

#[tokio::test]
async fn test_create_pipeline_aggregation_with_group_by_window_and_expr() {
    let test_name = "aggregation_with_group_by_window_and_expr";
    let sql = "SELECT sum(a) + 1, b + 1 FROM stream GROUP BY countwindow(4), b + 1";
    let input_data = vec![
        (
            "a".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
            ],
        ),
        (
            "b".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(2),
            ],
        ),
        (
            "c".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(1),
                Value::Int64(2),
            ],
        ),
    ];

    let instance = FlowInstance::new();
    install_stream_schema(&instance, &input_data).await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink(sql, true)
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_name));

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_name));

    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(test_batch)))
        .await
        .unwrap_or_else(|_| panic!("Failed to send test data for: {}", test_name));

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    let expected = serde_json::json!([
        {"sum(a) + 1": 3, "b + 1": 2},
        {"sum(a) + 1": 3, "b + 1": 3}
    ]);
    let actual_sorted = sort_json_array_by_key(normalize_json(actual), "b + 1");
    let expected_sorted = sort_json_array_by_key(normalize_json(expected), "b + 1");
    assert_eq!(
        actual_sorted, expected_sorted,
        "Wrong output JSON for test: {}",
        test_name
    );

    pipeline
        .close()
        .await
        .unwrap_or_else(|_| panic!("Failed to close pipeline for test: {}", test_name));
}

#[tokio::test]
async fn test_create_pipeline_with_custom_sink_connectors() {
    let instance = FlowInstance::new();
    install_stream_schema(&instance, &[("a".to_string(), vec![Value::Int64(10)])]).await;

    let connector = PipelineSinkConnector::new(
        "custom_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("custom_sink", connector).with_forward_to_result(true);

    let mut pipeline = instance
        .build_pipeline("SELECT a FROM stream", vec![sink])
        .expect("pipeline with custom sink should succeed");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = br#"[{"a":10}]"#.to_vec();
    pipeline
        .send_stream_data("stream", StreamData::bytes(payload))
        .await
        .expect("send data");

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let actual = recv_next_json(&mut output, Duration::from_secs(1)).await;
    assert_eq!(
        normalize_json(actual),
        normalize_json(serde_json::json!([{"a": 10}]))
    );

    pipeline.close().await.expect("close pipeline");
}

#[tokio::test]
async fn test_batch_processor_flushes_on_count() {
    let instance = FlowInstance::new();
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
    )
    .await;

    let connector = PipelineSinkConnector::new(
        "batch_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("batch_sink", connector)
        .with_forward_to_result(true)
        .with_common_props(CommonSinkProps {
            batch_count: Some(2),
            batch_duration: None,
        });

    let mut pipeline = instance
        .build_pipeline("SELECT a FROM stream", vec![sink])
        .expect("failed to create batched pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let columns = vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )];
    let batch =
        batch_from_columns_simple(columns).expect("failed to create batch for batching test");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let first = timeout(Duration::from_secs(1), output.recv())
        .await
        .expect("first batch timeout")
        .expect("first batch missing");
    match first {
        StreamData::EncodedBytes { num_rows, .. } => {
            assert_eq!(num_rows, 2, "first batch should contain 2 rows");
        }
        other => panic!(
            "expected first batch collection, got {}",
            other.description()
        ),
    }

    pipeline.close().await.expect("close pipeline");

    let second = timeout(Duration::from_secs(1), output.recv())
        .await
        .expect("second batch timeout")
        .expect("second batch missing");
    match second {
        StreamData::EncodedBytes { num_rows, .. } => {
            assert_eq!(num_rows, 1, "leftover batch should contain 1 row");
        }
        other => panic!("expected leftover collection, got {}", other.description()),
    }
}

async fn install_stream_schema(instance: &FlowInstance, columns: &[(String, Vec<Value>)]) {
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

#[tokio::test]
async fn test_aggregation_with_countwindow() {
    let instance = FlowInstance::new();

    // Install stream schema with column 'a'
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![
                Value::Int64(10),
                Value::Int64(20),
                Value::Int64(30),
                Value::Int64(40),
                Value::Int64(50),
            ],
        )],
    )
    .await;

    let connector = PipelineSinkConnector::new(
        "aggregation_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("aggregation_sink", connector).with_forward_to_result(true);

    // Create pipeline with countwindow and sum aggregation
    let mut pipeline = instance
        .build_pipeline(
            "SELECT sum(a) FROM stream GROUP BY countwindow(2)",
            vec![sink],
        )
        .expect("failed to create aggregation pipeline with countwindow");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send data - should be divided into 3 windows: first 2, middle 2, last 1
    let columns = vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
            Value::Int64(40),
            Value::Int64(50),
        ],
    )];
    let batch = batch_from_columns_simple(columns).expect("create batch");
    let stream_data = StreamData::collection(Box::new(batch));
    pipeline
        .send_stream_data("stream", stream_data)
        .await
        .expect("send data");

    // Wait for aggregation results
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Close pipeline to trigger final aggregation
    pipeline.close().await.expect("close pipeline");

    // Collect output results
    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    // Expect to get 2 complete windows (countwindow(2) with 5 rows = 2 full windows + 1 remainder)
    // Note: The current implementation may not output the final partial window
    let mut results = Vec::new();

    // Read first window result (sum of 10, 20 = 30)
    let first = timeout(Duration::from_secs(2), output.recv())
        .await
        .expect("first window timeout")
        .expect("first window missing");
    results.push(first);

    // Read second window result (sum of 30, 40 = 70)
    let second = timeout(Duration::from_secs(2), output.recv())
        .await
        .expect("second window timeout")
        .expect("second window missing");
    results.push(second);

    // Note: We expect only 2 complete windows, the final single row (50) may not be processed
    // in the current window implementation as it might wait for more data to complete the window

    // Verify results
    assert_eq!(results.len(), 2, "Expected 2 complete window results");

    let first = match &results[0] {
        StreamData::EncodedBytes { payload, .. } => serde_json::from_slice(payload).expect("json"),
        other => panic!(
            "Expected EncodedBytes for first window, got {}",
            other.description()
        ),
    };
    let second = match &results[1] {
        StreamData::EncodedBytes { payload, .. } => serde_json::from_slice(payload).expect("json"),
        other => panic!(
            "Expected EncodedBytes for second window, got {}",
            other.description()
        ),
    };
    assert_eq!(
        normalize_json(first),
        normalize_json(serde_json::json!([{"sum(a)": 30}]))
    );
    assert_eq!(
        normalize_json(second),
        normalize_json(serde_json::json!([{"sum(a)": 70}]))
    );

    println!("✓ All aggregation with countwindow tests passed!");
}

#[tokio::test]
async fn test_last_row_with_countwindow() {
    let instance = FlowInstance::new();

    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![
                Value::Int64(10),
                Value::Int64(20),
                Value::Int64(30),
                Value::Int64(40),
            ],
        )],
    )
    .await;

    let connector = PipelineSinkConnector::new(
        "last_row_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("last_row_sink", connector).with_forward_to_result(true);

    let mut pipeline = instance
        .build_pipeline(
            "SELECT last_row(a) FROM stream GROUP BY countwindow(2)",
            vec![sink],
        )
        .expect("failed to create last_row pipeline with countwindow");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let columns = vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
            Value::Int64(40),
        ],
    )];
    let batch = batch_from_columns_simple(columns).expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    tokio::time::sleep(Duration::from_millis(200)).await;
    pipeline.close().await.expect("close pipeline");

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    let first = timeout(Duration::from_secs(2), output.recv())
        .await
        .expect("first window timeout")
        .expect("first window missing");
    let second = timeout(Duration::from_secs(2), output.recv())
        .await
        .expect("second window timeout")
        .expect("second window missing");

    let first = match first {
        StreamData::EncodedBytes { payload, .. } => serde_json::from_slice(&payload).expect("json"),
        other => panic!("expected EncodedBytes, got {}", other.description()),
    };
    let second = match second {
        StreamData::EncodedBytes { payload, .. } => serde_json::from_slice(&payload).expect("json"),
        other => panic!("expected EncodedBytes, got {}", other.description()),
    };
    assert_eq!(
        normalize_json(first),
        normalize_json(serde_json::json!([{"last_row(a)": 20}]))
    );
    assert_eq!(
        normalize_json(second),
        normalize_json(serde_json::json!([{"last_row(a)": 40}]))
    );
}
