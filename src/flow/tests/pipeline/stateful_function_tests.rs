//! Table-driven tests for built-in stateful functions.

use datatypes::Value;
use flow::model::batch_from_columns_simple;
use flow::processor::StreamData;
use flow::FlowInstance;
use tokio::time::Duration;

use super::common::{install_stream_schema, recv_next_collection};

struct ColumnCheck {
    expected_name: String,
    expected_values: Vec<Value>,
}

struct ExpectedCollection {
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>,
}

struct StatefulCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    expected_outputs: Vec<ExpectedCollection>,
    wait_after_send: Duration,
    close_before_read: bool,
}

async fn run_stateful_case(case: StatefulCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new();
    install_stream_schema(&instance, &case.input_data).await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink(case.sql, true)
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(test_batch)))
        .await
        .unwrap_or_else(|_| panic!("Failed to send test data for: {}", case.name));

    tokio::time::sleep(case.wait_after_send).await;

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    if case.close_before_read {
        pipeline
            .close()
            .await
            .unwrap_or_else(|_| panic!("Failed to close pipeline for test: {}", case.name));
    }

    let timeout_duration = Duration::from_secs(5);
    for expected in case.expected_outputs {
        let collection = recv_next_collection(&mut output, timeout_duration).await;
        let rows = collection.rows();

        assert_eq!(
            rows.len(),
            expected.expected_rows,
            "Wrong number of rows for test: {}",
            case.name
        );
        if expected.expected_rows > 0 {
            for row in rows {
                assert_eq!(
                    row.len(),
                    expected.expected_columns,
                    "Wrong number of columns for test: {}",
                    case.name
                );
            }

            for check in expected.column_checks {
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
                    check.expected_name, case.name
                );
            }
        }
    }

    if !case.close_before_read {
        pipeline
            .close()
            .await
            .unwrap_or_else(|_| panic!("Failed to close pipeline for test: {}", case.name));
    }
}

#[tokio::test]
async fn stateful_function_table_driven() {
    let cases = vec![
        StatefulCase {
            name: "lag_basic",
            sql: "SELECT lag(a) AS prev FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![Value::Null, Value::Int64(1), Value::Int64(2)],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_dedup_two_columns",
            sql: "SELECT lag(a) AS p1, lag(a) AS p2 FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "p1".to_string(),
                        expected_values: vec![Value::Null, Value::Int64(1), Value::Int64(2)],
                    },
                    ColumnCheck {
                        expected_name: "p2".to_string(),
                        expected_values: vec![Value::Null, Value::Int64(1), Value::Int64(2)],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_in_where_filter",
            sql: "SELECT a FROM stream WHERE lag(a) > 1",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Int64(4),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 2,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(4)],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "nested_inside_aggregate_countwindow",
            sql: "SELECT sum(a), last_row(lag(a)) FROM stream GROUP BY countwindow(4)",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(40),
                    Value::Int64(50),
                    Value::Int64(60),
                    Value::Int64(70),
                    Value::Int64(80),
                ],
            )],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 2,
                    column_checks: vec![
                        ColumnCheck {
                            expected_name: "sum(a)".to_string(),
                            expected_values: vec![Value::Int64(100)],
                        },
                        ColumnCheck {
                            expected_name: "last_row(lag(a))".to_string(),
                            expected_values: vec![Value::Int64(30)],
                        },
                    ],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 2,
                    column_checks: vec![
                        ColumnCheck {
                            expected_name: "sum(a)".to_string(),
                            expected_values: vec![Value::Int64(260)],
                        },
                        ColumnCheck {
                            expected_name: "last_row(lag(a))".to_string(),
                            expected_values: vec![Value::Int64(70)],
                        },
                    ],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: true,
        },
    ];

    for case in cases {
        run_stateful_case(case).await;
    }
}
