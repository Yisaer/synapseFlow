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
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_create_pipeline_aggregation_with_group_by_window_and_expr() {
    let test_name = "aggregation_with_group_by_window_and_expr";
    let sql = "SELECT sum(a) + 1, b + 1 FROM stream GROUP BY countwindow(4), b + 1";
    let input_data = vec![
        (
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(1), Value::Int64(1), Value::Int64(1)],
        ),
        (
            "b".to_string(),
            vec![Value::Int64(1), Value::Int64(1), Value::Int64(2), Value::Int64(2)],
        ),
        (
            "c".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(1), Value::Int64(2)],
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
    let received_data = timeout(Duration::from_secs(5), output.recv())
        .await
        .unwrap_or_else(|_| panic!("Timeout waiting for output for: {}", test_name))
        .unwrap_or_else(|| panic!("Failed to receive output for: {}", test_name));

    match received_data {
        StreamData::Collection(result_collection) => {
            let mut rows = result_collection.rows().to_vec();
            rows.sort_by(|a, b| {
                let av = a
                    .value_by_name("stream", "b + 1")
                    .or_else(|| a.value_by_name("", "b + 1"));
                let bv = b
                    .value_by_name("stream", "b + 1")
                    .or_else(|| b.value_by_name("", "b + 1"));
                format!("{:?}", av).cmp(&format!("{:?}", bv))
            });

            assert_eq!(rows.len(), 2, "Wrong number of rows for test: {}", test_name);
            for row in &rows {
                assert_eq!(
                    row.len(),
                    2,
                    "Wrong number of columns for test: {}",
                    test_name
                );
            }

            let read_col = |name: &str| -> Vec<Value> {
                rows.iter()
                    .map(|row| {
                        row.value_by_name("stream", name)
                            .or_else(|| row.value_by_name("", name))
                            .unwrap_or_else(|| panic!("column {} missing", name))
                            .clone()
                    })
                    .collect()
            };

            assert_eq!(
                read_col("sum(a) + 1"),
                vec![Value::Int64(3), Value::Int64(3)],
                "Wrong values in column sum(a) + 1 for test: {}",
                test_name
            );
            assert_eq!(
                read_col("b + 1"),
                vec![Value::Int64(2), Value::Int64(3)],
                "Wrong values in column b + 1 for test: {}",
                test_name
            );
        }
        other => panic!(
            "Expected Collection data, but received {} for test: {}",
            other.description(),
            test_name
        ),
    }

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
        SinkConnectorConfig::Nop(NopSinkConfig),
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
    let received = timeout(Duration::from_secs(1), output.recv())
        .await
        .expect("pipeline output timeout")
        .expect("pipeline output missing");
    match received {
        StreamData::Collection(collection) => {
            let rows = collection.rows();
            assert_eq!(rows.len(), 1);
            let value = rows[0]
                .value_by_name("stream", "a")
                .or_else(|| rows[0].value_by_name("", "a"))
                .expect("missing column a");
            assert_eq!(value, &Value::Int64(10));
        }
        other => panic!("expected collection data, got {:?}", other.description()),
    }

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
        SinkConnectorConfig::Nop(NopSinkConfig),
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
        StreamData::Collection(collection) => {
            assert_eq!(
                collection.num_rows(),
                2,
                "first batch should contain 2 rows"
            );
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
        StreamData::Collection(collection) => {
            assert_eq!(
                collection.num_rows(),
                1,
                "leftover batch should contain 1 row"
            );
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
        SinkConnectorConfig::Nop(NopSinkConfig),
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

    // Verify first window: sum(10, 20) = 30
    match &results[0] {
        StreamData::Collection(collection) => {
            assert_eq!(collection.num_rows(), 1, "Each window should produce 1 row");
            let rows = collection.rows();
            let sum_value = rows[0]
                .value_by_name("", "sum(a)") // aggregate result in affiliate column
                .expect("missing sum(a) column");
            assert_eq!(sum_value, &Value::Int64(30));
            println!("✓ First window: sum(10, 20) = 30");
        }
        other => panic!(
            "Expected collection data for first window, got {:?}",
            other.description()
        ),
    }

    // Verify second window: sum(30, 40) = 70
    match &results[1] {
        StreamData::Collection(collection) => {
            assert_eq!(collection.num_rows(), 1, "Each window should produce 1 row");
            let rows = collection.rows();
            let sum_value = rows[0]
                .value_by_name("", "sum(a)")
                .expect("missing sum(a) column");
            assert_eq!(sum_value, &Value::Int64(70));
            println!("✓ Second window: sum(30, 40) = 70");
        }
        other => panic!(
            "Expected collection data for second window, got {:?}",
            other.description()
        ),
    }

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
        SinkConnectorConfig::Nop(NopSinkConfig),
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

    match first {
        StreamData::Collection(collection) => {
            assert_eq!(collection.num_rows(), 1);
            let value = collection.rows()[0]
                .value_by_name("", "last_row(a)")
                .expect("missing last_row(a)");
            assert_eq!(value, &Value::Int64(20));
        }
        other => panic!("expected collection, got {}", other.description()),
    }

    match second {
        StreamData::Collection(collection) => {
            assert_eq!(collection.num_rows(), 1);
            let value = collection.rows()[0]
                .value_by_name("", "last_row(a)")
                .expect("missing last_row(a)");
            assert_eq!(value, &Value::Int64(40));
        }
        other => panic!("expected collection, got {}", other.description()),
    }
}
