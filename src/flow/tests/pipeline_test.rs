//! Tests for the public pipeline creation helpers.
//!
//! This module exercises both `create_pipeline_with_log_sink` (default/testing)
//! and the customizable `create_pipeline` API that accepts user-defined sinks.

use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::model::batch_from_columns_simple;
use flow::planner::sink::{
    CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig,
};
use flow::processor::StreamData;
use flow::FlowInstance;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// Test case structure for table-driven tests
struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns by name
}

/// Column-specific checks
struct ColumnCheck {
    expected_name: String,
    expected_values: Vec<Value>,
}

/// Run a single test case
async fn run_test_case(test_case: TestCase) {
    println!("Running test: {}", test_case.name);

    let instance = FlowInstance::new();
    install_stream_schema(&instance, &test_case.input_data).await;

    // Create pipeline from SQL
    let mut pipeline = instance
        .build_pipeline_with_log_sink(test_case.sql, true)
        .expect(&format!(
            "Failed to create pipeline for: {}",
            test_case.name
        ));

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create test data (row-based)
    let columns = test_case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();

    let test_batch = batch_from_columns_simple(columns).expect(&format!(
        "Failed to create test RecordBatch for: {}",
        test_case.name
    ));

    let stream_data = StreamData::collection(Box::new(test_batch));
    pipeline
        .send_stream_data("stream", stream_data)
        .await
        .expect(&format!("Failed to send test data for: {}", test_case.name));

    // Receive and verify results
    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let timeout_duration = Duration::from_secs(5);
    let received_data = timeout(timeout_duration, output.recv())
        .await
        .unwrap_or_else(|_| panic!("Timeout waiting for output for: {}", test_case.name))
        .unwrap_or_else(|| panic!("Failed to receive output for: {}", test_case.name));

    match received_data {
        StreamData::Collection(result_collection) => {
            let rows = result_collection.rows();

            // Check basic properties
            assert_eq!(
                rows.len(),
                test_case.expected_rows,
                "Wrong number of rows for test: {}",
                test_case.name
            );
            if test_case.expected_rows == 0 {
                assert!(
                    rows.is_empty(),
                    "Row-less batches should have no rows for test: {}",
                    test_case.name
                );
            } else {
                for row in rows {
                    assert_eq!(
                        row.len(),
                        test_case.expected_columns,
                        "Wrong number of columns for test: {}",
                        test_case.name
                    );
                }

                for check in test_case.column_checks {
                    let mut values = Vec::with_capacity(rows.len());
                    for row in rows {
                        let value = row
                            .value_by_name("stream", &check.expected_name)
                            .or_else(|| row.value_by_name("", &check.expected_name))
                            .unwrap_or_else(|| panic!("column {} missing", check.expected_name));
                        values.push(value.clone());
                    }
                    assert_eq!(
                        values, check.expected_values,
                        "Wrong values in column {} for test: {}",
                        check.expected_name, test_case.name
                    );
                }
            }
        }
        StreamData::Encoded { .. } => {
            panic!(
                "Expected Collection data, but received encoded payload for test: {}",
                test_case.name
            );
        }
        StreamData::Control(_) => {
            panic!(
                "Expected Collection data, but received control signal for test: {}",
                test_case.name
            );
        }
        StreamData::Error(e) => {
            panic!(
                "Expected Collection data, but received error for test: {}: {}",
                test_case.name, e.message
            );
        }
        StreamData::Bytes(_) => {
            panic!(
                "Expected Collection data, but received undecoded bytes for test: {}",
                test_case.name
            );
        }
    }

    pipeline.close().await.expect(&format!(
        "Failed to close pipeline for test: {}",
        test_case.name
    ));
}

/// Test create_pipeline with various SQL queries using table-driven approach
#[tokio::test]
async fn test_create_pipeline_various_queries() {
    let test_cases = vec![
        TestCase {
            name: "wildcard",
            sql: "SELECT * FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 3,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                },
            ],
        },
        TestCase {
            name: "simple_projection",
            sql: "SELECT a + 1, b + 2 FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 1".to_string(),
                    expected_values: vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],
                },
                ColumnCheck {
                    expected_name: "b + 2".to_string(),
                    expected_values: vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],
                },
            ],
        },
        TestCase {
            name: "simple_filter",
            sql: "SELECT a, b FROM stream WHERE a > 15",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 2, // Only rows where a > 15
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(20), Value::Int64(30)], // Filtered values
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(200), Value::Int64(300)], // Corresponding b values
                },
            ],
        },
        TestCase {
            name: "filter_with_projection",
            sql: "SELECT a + 5, b * 2 FROM stream WHERE a > 15",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 2, // Only rows where a > 15
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 5".to_string(),
                    expected_values: vec![Value::Int64(25), Value::Int64(35)], // (20+5), (30+5)
                },
                ColumnCheck {
                    expected_name: "b * 2".to_string(),
                    expected_values: vec![Value::Int64(400), Value::Int64(600)], // (200*2), (300*2)
                },
            ],
        },
        TestCase {
            name: "filter_no_matches",
            sql: "SELECT a, b FROM stream WHERE a > 100",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 0, // No rows match the filter
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![], // Empty
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![], // Empty
                },
            ],
        },
        TestCase {
            name: "filter_all_match",
            sql: "SELECT a FROM stream WHERE a > 5",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            )],
            expected_rows: 3, // All rows match the filter
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            }],
        },
    ];

    // Run all test cases
    for test_case in test_cases {
        run_test_case(test_case).await;
    }
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
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json_default("stream"),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}
