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

/// Test case structure for table-driven tests
struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns by name
    sort_by_fields: Option<Vec<&'static str>>,
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
            let mut rows = result_collection.rows().to_vec();

            if let Some(sort_fields) = &test_case.sort_by_fields {
                use std::cmp::Ordering;
                rows.sort_by(|a, b| {
                    for field in sort_fields {
                        let av = a
                            .value_by_name("stream", field)
                            .or_else(|| a.value_by_name("", field));
                        let bv = b
                            .value_by_name("stream", field)
                            .or_else(|| b.value_by_name("", field));
                        let ord = format!("{:?}", av).cmp(&format!("{:?}", bv));
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    Ordering::Equal
                });
            }

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
                for row in &rows {
                    assert_eq!(
                        row.len(),
                        test_case.expected_columns,
                        "Wrong number of columns for test: {}",
                        test_case.name
                    );
                }

                for check in test_case.column_checks {
                    let mut values = Vec::with_capacity(rows.len());
                    for row in &rows {
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
        StreamData::Watermark(ts) => {
            panic!(
                "Expected Collection data, but received watermark {:?} for test: {}",
                ts, test_case.name
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
            sort_by_fields: None,
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
            sort_by_fields: None,
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
            sort_by_fields: None,
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
            sort_by_fields: None,
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
            sort_by_fields: None,
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
            sort_by_fields: None,
        },
        TestCase {
            name: "aggregation_with_group_by_window_and_expr",
            sql: "SELECT sum(a) + 1, b + 1 FROM stream GROUP BY countwindow(4), b + 1",
            input_data: vec![
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
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "sum(a) + 1".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(3)],
                },
                ColumnCheck {
                    expected_name: "b + 1".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(3)],
                },
            ],
            sort_by_fields: Some(vec!["b + 1"]),
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
