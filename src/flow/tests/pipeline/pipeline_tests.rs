//! Table-driven tests for pipeline creation helpers.

use datatypes::Value;
use flow::model::batch_from_columns_simple;
use flow::processor::StreamData;
use flow::FlowInstance;
use serde_json::Value as JsonValue;
use tokio::time::Duration;

use super::common::ColumnCheck;
use super::common::{build_expected_json, install_stream_schema, normalize_json, recv_next_json};

struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns by name
}

async fn run_test_case(test_case: TestCase) {
    println!("Running test: {}", test_case.name);

    let instance = FlowInstance::new();
    install_stream_schema(&instance, &test_case.input_data).await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink(test_case.sql, true)
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let columns = test_case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();

    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(test_batch)))
        .await
        .unwrap_or_else(|_| panic!("Failed to send test data for: {}", test_case.name));

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let timeout_duration = Duration::from_secs(5);
    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        test_case.expected_columns,
        test_case.column_checks.len(),
        "expected_columns should match column_checks for JSON comparison (test: {})",
        test_case.name
    );
    let expected: JsonValue =
        build_expected_json(test_case.expected_rows, &test_case.column_checks);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong output JSON for test: {}",
        test_case.name
    );

    pipeline
        .close()
        .await
        .unwrap_or_else(|_| panic!("Failed to close pipeline for test: {}", test_case.name));
}

#[tokio::test]
async fn pipeline_table_driven_queries() {
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
                ColumnCheck {
                    expected_name: "c".to_string(),
                    expected_values: vec![
                        Value::Int64(1000),
                        Value::Int64(2000),
                        Value::Int64(3000),
                    ],
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
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(200), Value::Int64(300)],
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
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 5".to_string(),
                    expected_values: vec![Value::Int64(25), Value::Int64(35)],
                },
                ColumnCheck {
                    expected_name: "b * 2".to_string(),
                    expected_values: vec![Value::Int64(400), Value::Int64(600)],
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
            expected_rows: 0,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![],
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
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            }],
        },
    ];

    for test_case in test_cases {
        run_test_case(test_case).await;
    }
}
