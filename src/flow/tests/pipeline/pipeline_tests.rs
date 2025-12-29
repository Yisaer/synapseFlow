//! Table-driven tests for pipeline creation helpers.

use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::model::batch_from_columns_simple;
use flow::processor::StreamData;
use flow::FlowInstance;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns by name
}

struct ColumnCheck {
    expected_name: String,
    expected_values: Vec<Value>,
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
    let received_data = timeout(timeout_duration, output.recv())
        .await
        .unwrap_or_else(|_| panic!("Timeout waiting for output for: {}", test_case.name))
        .unwrap_or_else(|| panic!("Failed to receive output for: {}", test_case.name));

    match received_data {
        StreamData::Collection(result_collection) => {
            let rows = result_collection.rows();

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
        StreamData::Watermark(ts) => {
            panic!(
                "Expected Collection data, but received watermark {:?} for test: {}",
                ts, test_case.name
            );
        }
    }

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

