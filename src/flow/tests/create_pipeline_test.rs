//! Tests for the public pipeline creation helpers.
//!
//! This module exercises both `create_pipeline_with_log_sink` (default/testing)
//! and the customizable `create_pipeline` API that accepts user-defined sinks.

use datatypes::Value;
use flow::connector::MockSinkConnector;
use flow::create_pipeline;
use flow::create_pipeline_with_log_sink;
use flow::model::{Column, RecordBatch as FlowRecordBatch};
use flow::processor::{SinkProcessor, StreamData};
use flow::JsonEncoder;
use serde_json::json;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// Test case structure for table-driven tests
struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns
}

/// Column-specific checks
struct ColumnCheck {
    column_index: usize,
    expected_name: String,
    expected_values: Vec<Value>,
}

/// Run a single test case
async fn run_test_case(test_case: TestCase) {
    println!("Running test: {}", test_case.name);

    // Create pipeline from SQL
    let mut pipeline = create_pipeline_with_log_sink(test_case.sql, true).expect(&format!(
        "Failed to create pipeline for: {}",
        test_case.name
    ));

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create test data
    let mut columns = Vec::new();
    for (col_name, values) in test_case.input_data {
        let column = Column::new("".to_string(), col_name, values);
        columns.push(column);
    }

    let test_batch = FlowRecordBatch::new(columns).expect(&format!(
        "Failed to create test RecordBatch for: {}",
        test_case.name
    ));

    let stream_data = StreamData::collection(Box::new(test_batch));
    pipeline
        .input
        .send(stream_data)
        .await
        .expect(&format!("Failed to send test data for: {}", test_case.name));

    // Receive and verify results
    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let timeout_duration = Duration::from_secs(5);
    let received_data = timeout(timeout_duration, output.recv())
        .await
        .expect(&format!(
            "Timeout waiting for output for: {}",
            test_case.name
        ))
        .expect(&format!("Failed to receive output for: {}", test_case.name));

    match received_data {
        StreamData::Collection(result_collection) => {
            let batch = FlowRecordBatch::from_rows(result_collection.rows().to_vec());

            // Check basic properties
            assert_eq!(
                batch.num_rows(),
                test_case.expected_rows,
                "Wrong number of rows for test: {}",
                test_case.name
            );
            let all_columns = batch.columns();
            if test_case.expected_rows == 0 {
                assert!(
                    all_columns.is_empty(),
                    "Row-less batches have no materialized columns for test: {}",
                    test_case.name
                );
            } else {
                assert_eq!(
                    all_columns.len(),
                    test_case.expected_columns,
                    "Wrong number of columns for test: {}",
                    test_case.name
                );

                for check in test_case.column_checks {
                    let column = all_columns
                        .get(check.column_index)
                        .expect("column snapshot");
                    assert_eq!(
                        column.name, check.expected_name,
                        "Wrong column name at index {} for test: {}",
                        check.column_index, test_case.name
                    );
                    assert_eq!(
                        column.values(),
                        &check.expected_values,
                        "Wrong values in column {} for test: {}",
                        check.column_index,
                        test_case.name
                    );
                }
            }
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
                    column_index: 0,
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    column_index: 1,
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
                    column_index: 0,
                    expected_name: "a + 1".to_string(),
                    expected_values: vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],
                },
                ColumnCheck {
                    column_index: 1,
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
                    column_index: 0,
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(20), Value::Int64(30)], // Filtered values
                },
                ColumnCheck {
                    column_index: 1,
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
                    column_index: 0,
                    expected_name: "a + 5".to_string(),
                    expected_values: vec![Value::Int64(25), Value::Int64(35)], // (20+5), (30+5)
                },
                ColumnCheck {
                    column_index: 1,
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
                    column_index: 0,
                    expected_name: "a".to_string(),
                    expected_values: vec![], // Empty
                },
                ColumnCheck {
                    column_index: 1,
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
                column_index: 0,
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
    let mut sink = SinkProcessor::new("custom_sink");
    let (connector, mut handle) = MockSinkConnector::new("custom_sink_connector");
    sink.add_connector(Box::new(connector), Arc::new(JsonEncoder::new("json")));

    let mut pipeline = create_pipeline("SELECT a FROM stream", vec![sink])
        .expect("pipeline with custom sink should succeed");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let column = Column::new("".to_string(), "a".to_string(), vec![Value::Int64(10)]);
    let batch = FlowRecordBatch::new(vec![column]).expect("record batch");
    pipeline
        .input
        .send(StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let payload = timeout(Duration::from_secs(1), handle.recv())
        .await
        .expect("sink payload timeout")
        .expect("sink payload missing");
    let json_payload: serde_json::Value =
        serde_json::from_slice(&payload).expect("valid json payload");
    assert_eq!(json_payload, json!([{"a":10}]));

    pipeline.close().await.expect("close pipeline");
}

/// Test create_pipeline with invalid SQL
#[tokio::test]
async fn test_create_pipeline_invalid_sql() {
    let invalid_sql_cases = vec![
        "INVALID SQL SYNTAX",
        "SELECT * FROM",                // incomplete query
        "INSERT INTO table VALUES (1)", // unsupported statement type
        "",
    ];

    for sql in invalid_sql_cases {
        let result = create_pipeline_with_log_sink(sql, false);
        assert!(result.is_err(), "Should fail with invalid SQL: {}", sql);
    }
}
