//! Table-driven tests for built-in stateful functions.

use datatypes::Value;
use flow::model::batch_from_columns_simple;
use flow::pipeline::MemorySinkProps;
use flow::pipeline::PipelineDefinition;
use flow::planner::sink::{CommonSinkProps, SinkOutputConfig};
use flow::FlowInstance;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::Value as JsonValue;
use tokio::time::Duration;

use super::common::ColumnCheck;
use super::common::{
    assert_no_json_output, build_expected_json, declare_memory_input_output_topics,
    install_memory_stream_schema, install_memory_stream_schema_with_name, make_memory_topics,
    normalize_json, publish_input_collection, recv_next_json,
};

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

fn source_name_for_case(case: &StatefulCase) -> &'static str {
    if case.sql.contains("FROM stream_ab") {
        "stream_ab"
    } else {
        "stream"
    }
}

async fn run_stateful_case(case: StatefulCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("stateful_function_table_driven", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    let source_name = source_name_for_case(&case);
    install_memory_stream_schema_with_name(&instance, &input_topic, source_name, &case.input_data)
        .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| (source_name.to_string(), col_name, values))
        .collect();
    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        Duration::from_secs(5),
    )
    .await;

    tokio::time::sleep(case.wait_after_send).await;

    if case.close_before_read {
        instance
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Quick,
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    }

    let timeout_duration = Duration::from_secs(5);
    for expected in case.expected_outputs {
        let actual: JsonValue = recv_next_json(&mut output, timeout_duration).await;
        assert_eq!(
            expected.expected_columns,
            expected.column_checks.len(),
            "expected_columns should match column_checks for JSON comparison (test: {})",
            case.name
        );
        let expected_json: JsonValue =
            build_expected_json(expected.expected_rows, &expected.column_checks);
        assert_eq!(
            normalize_json(actual),
            normalize_json(expected_json),
            "Wrong output JSON for test: {}",
            case.name
        );
    }

    if !case.close_before_read {
        instance
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Quick,
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    }

    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

// coverage-covers: stream.window.count, planner.logical.common_subexpression_elimination, parser.function.stateful_functions
#[tokio::test]
async fn stateful_function_table_driven() {
    let cases = vec![
        StatefulCase {
            name: "aggregation_sum_countwindow",
            sql: "SELECT sum(a) FROM stream GROUP BY countwindow(2)",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(40),
                    Value::Int64(50),
                ],
            )],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "sum(a)".to_string(),
                        expected_values: vec![Value::Int64(30)],
                    }],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "sum(a)".to_string(),
                        expected_values: vec![Value::Int64(70)],
                    }],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: false,
        },
        StatefulCase {
            name: "aggregation_sum_countwindow_with_window_filter",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY countwindow(2) FILTER (WHERE b > 0)",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(50),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "sum(a)".to_string(),
                        expected_values: vec![Value::Int64(40)],
                    }],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "sum(a)".to_string(),
                        expected_values: vec![Value::Int64(90)],
                    }],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: false,
        },
        StatefulCase {
            name: "last_row_countwindow",
            sql: "SELECT last_row(a) FROM stream GROUP BY countwindow(2)",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(40),
                ],
            )],
            expected_outputs: vec![
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "last_row(a)".to_string(),
                        expected_values: vec![Value::Int64(20)],
                    }],
                },
                ExpectedCollection {
                    expected_rows: 1,
                    expected_columns: 1,
                    column_checks: vec![ColumnCheck {
                        expected_name: "last_row(a)".to_string(),
                        expected_values: vec![Value::Int64(40)],
                    }],
                },
            ],
            wait_after_send: Duration::from_millis(200),
            close_before_read: false,
        },
        StatefulCase {
            name: "aggregation_sum_statewindow_grouped_ordered",
            sql: "SELECT b, sum(a) AS s FROM stream GROUP BY statewindow(a = 1, a = 4), b ORDER BY b",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(4)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(1)],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 2,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "b".to_string(),
                        expected_values: vec![Value::Int64(1), Value::Int64(2)],
                    },
                    ColumnCheck {
                        expected_name: "s".to_string(),
                        expected_values: vec![Value::Int64(5), Value::Int64(2)],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(200),
            close_before_read: false,
        },
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
            name: "acc_sum_running_total",
            sql: "SELECT acc_sum(a) AS total FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Float64(1.0),
                    Value::Float64(2.5),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "total".to_string(),
                    expected_values: vec![
                        Value::Float64(1.0),
                        Value::Float64(3.5),
                        Value::Float64(3.5),
                        Value::Float64(6.5),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_max_running_max",
            sql: "SELECT acc_max(a) AS max_a FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "max_a".to_string(),
                    expected_values: vec![
                        Value::Float64(2.0),
                        Value::Float64(2.0),
                        Value::Float64(2.0),
                        Value::Float64(3.0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_min_running_min",
            sql: "SELECT acc_min(a) AS min_a FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "min_a".to_string(),
                    expected_values: vec![
                        Value::Float64(2.0),
                        Value::Float64(1.0),
                        Value::Float64(1.0),
                        Value::Float64(1.0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_count_running_non_null_count",
            sql: "SELECT acc_count(a) AS n FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::String("x".to_string()),
                    Value::Null,
                    Value::String("y".to_string()),
                    Value::String("z".to_string()),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "n".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_avg_running_average",
            sql: "SELECT acc_avg(a) AS avg_a FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Float64(1.0),
                    Value::Float64(2.0),
                    Value::Null,
                    Value::Float64(3.0),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "avg_a".to_string(),
                    expected_values: vec![
                        Value::Float64(1.0),
                        Value::Float64(1.5),
                        Value::Float64(1.5),
                        Value::Float64(2.0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_sum_filter_where_updates_only_matching_rows",
            sql: "SELECT acc_sum(a) FILTER (WHERE flag = 1) AS total FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                        Value::Float64(3.0),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(1), Value::Int64(0), Value::Int64(1)],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "total".to_string(),
                    expected_values: vec![
                        Value::Float64(1.0),
                        Value::Float64(1.0),
                        Value::Float64(4.0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_count_over_partition_by_keeps_independent_counts",
            sql: "SELECT k, acc_count(a) OVER (PARTITION BY k) AS n FROM stream",
            input_data: vec![
                (
                    "k".to_string(),
                    vec![
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                    ],
                ),
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(10),
                        Value::Int64(2),
                        Value::Int64(20),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "k".to_string(),
                        expected_values: vec![
                            Value::String("A".to_string()),
                            Value::String("B".to_string()),
                            Value::String("A".to_string()),
                            Value::String("B".to_string()),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "n".to_string(),
                        expected_values: vec![
                            Value::Int64(1),
                            Value::Int64(1),
                            Value::Int64(2),
                            Value::Int64(2),
                        ],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_sum_filter_where_over_partition_by_combines_clauses",
            sql:
                "SELECT k, acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k) AS total FROM stream",
            input_data: vec![
                (
                    "k".to_string(),
                    vec![
                        Value::String("A".to_string()),
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                    ],
                ),
                (
                    "a".to_string(),
                    vec![
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                        Value::Float64(10.0),
                        Value::Float64(3.0),
                        Value::Float64(20.0),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "k".to_string(),
                        expected_values: vec![
                            Value::String("A".to_string()),
                            Value::String("A".to_string()),
                            Value::String("B".to_string()),
                            Value::String("A".to_string()),
                            Value::String("B".to_string()),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "total".to_string(),
                        expected_values: vec![
                            Value::Float64(1.0),
                            Value::Float64(1.0),
                            Value::Float64(10.0),
                            Value::Float64(4.0),
                            Value::Float64(10.0),
                        ],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_sum_begin_reset_lifecycle",
            sql: "SELECT acc_sum(a, start, stop) AS total FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Float64(10.0),
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                        Value::Float64(3.0),
                        Value::Float64(4.0),
                        Value::Float64(5.0),
                    ],
                ),
                (
                    "start".to_string(),
                    vec![
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                    ],
                ),
                (
                    "stop".to_string(),
                    vec![
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "total".to_string(),
                    expected_values: vec![
                        Value::Float64(0.0),
                        Value::Float64(1.0),
                        Value::Float64(3.0),
                        Value::Float64(6.0),
                        Value::Float64(0.0),
                        Value::Float64(5.0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_sum_filter_does_not_block_begin_reset",
            sql: "SELECT acc_sum(a, start, stop) FILTER (WHERE valid) AS total FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Float64(10.0),
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                        Value::Float64(3.0),
                    ],
                ),
                (
                    "start".to_string(),
                    vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                    ],
                ),
                (
                    "stop".to_string(),
                    vec![
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                    ],
                ),
                (
                    "valid".to_string(),
                    vec![
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "total".to_string(),
                    expected_values: vec![
                        Value::Float64(0.0),
                        Value::Float64(1.0),
                        Value::Float64(1.0),
                        Value::Float64(0.0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_count_begin_without_reset",
            sql: "SELECT acc_count(a, start, false) AS n FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Null,
                        Value::Int64(3),
                    ],
                ),
                (
                    "start".to_string(),
                    vec![
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "n".to_string(),
                    expected_values: vec![
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "acc_sum_begin_reset_partition_by_keeps_independent_state",
            sql: "SELECT k, acc_sum(a, start, stop) OVER (PARTITION BY k) AS total FROM stream",
            input_data: vec![
                (
                    "k".to_string(),
                    vec![
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                        Value::String("B".to_string()),
                        Value::String("A".to_string()),
                        Value::String("A".to_string()),
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                    ],
                ),
                (
                    "a".to_string(),
                    vec![
                        Value::Float64(1.0),
                        Value::Float64(10.0),
                        Value::Float64(2.0),
                        Value::Float64(3.0),
                        Value::Float64(4.0),
                        Value::Float64(5.0),
                        Value::Float64(1.0),
                    ],
                ),
                (
                    "start".to_string(),
                    vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                    ],
                ),
                (
                    "stop".to_string(),
                    vec![
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 7,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "k".to_string(),
                        expected_values: vec![
                            Value::String("A".to_string()),
                            Value::String("B".to_string()),
                            Value::String("B".to_string()),
                            Value::String("A".to_string()),
                            Value::String("A".to_string()),
                            Value::String("A".to_string()),
                            Value::String("B".to_string()),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "total".to_string(),
                        expected_values: vec![
                            Value::Float64(1.0),
                            Value::Float64(0.0),
                            Value::Float64(2.0),
                            Value::Float64(4.0),
                            Value::Float64(8.0),
                            Value::Float64(0.0),
                            Value::Float64(3.0),
                        ],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "multiple_acc_filter_where_over_partition_by_calls_share_partition_key",
            sql:
                "SELECT acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k) AS total, acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k) AS n FROM stream",
            input_data: vec![
                (
                    "k".to_string(),
                    vec![
                        Value::String("A".to_string()),
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                        Value::String("A".to_string()),
                        Value::String("B".to_string()),
                    ],
                ),
                (
                    "a".to_string(),
                    vec![
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                        Value::Float64(10.0),
                        Value::Float64(3.0),
                        Value::Float64(20.0),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "total".to_string(),
                        expected_values: vec![
                            Value::Float64(1.0),
                            Value::Float64(1.0),
                            Value::Float64(10.0),
                            Value::Float64(4.0),
                            Value::Float64(10.0),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "n".to_string(),
                        expected_values: vec![
                            Value::Int64(1),
                            Value::Int64(1),
                            Value::Int64(1),
                            Value::Int64(2),
                            Value::Int64(1),
                        ],
                    },
                ],
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
            name: "lag_filter_false_returns_visible_lag_value",
            sql: "SELECT lag(a) FILTER (WHERE flag = 1) AS prev FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(30),
                    ],
                }],
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
            name: "lag_filter_partition_by_uses_visible_lag_per_partition",
            sql: "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) AS prev FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(50),
                        Value::Int64(60),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
                (
                    "k1".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Null,
                        Value::Int64(40),
                        Value::Int64(40),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_filter_depends_on_prior_stateful_bool_output",
            sql: "SELECT lag(flag) AS prev_flag, lag(a) FILTER (WHERE lag(flag)) AS prev_a FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "prev_flag".to_string(),
                        expected_values: vec![
                            Value::Null,
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "prev_a".to_string(),
                        expected_values: vec![
                            Value::Null,
                            Value::Null,
                            Value::Int64(20),
                            Value::Int64(20),
                        ],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "project_expression_uses_visible_lag_value",
            sql: "SELECT a + lag(b) FILTER (WHERE flag = 1) AS v FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(4),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "v".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(12),
                        Value::Int64(13),
                        Value::Int64(34),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "stateful_projection_with_repeated_visible_expr_keeps_runtime_semantics",
            sql: "SELECT lag(a) + 1 AS x, x + 1 AS y FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "x".to_string(),
                        expected_values: vec![Value::Null, Value::Int64(2), Value::Int64(3)],
                    },
                    ColumnCheck {
                        expected_name: "y".to_string(),
                        expected_values: vec![Value::Null, Value::Int64(3), Value::Int64(4)],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "lag_offset_ignore_null",
            sql: "SELECT lag(a, 2, true) AS prev FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Null, Value::Int64(30), Value::Int64(40)],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "prev".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int64(10),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "latest_tracks_last_non_null_with_filter",
            sql: "SELECT latest(a) FILTER (WHERE flag = 1) AS latest_a FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Null,
                        Value::Int64(20),
                        Value::Int64(30),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "latest_a".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(20),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "latest_partition_by_keeps_state_per_partition",
            sql: "SELECT latest(a) OVER (PARTITION BY k1) AS latest_a FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Null,
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Null,
                    ],
                ),
                (
                    "k1".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "latest_a".to_string(),
                    expected_values: vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(10),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(40),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "changed_col_emits_only_on_changes",
            sql: "SELECT changed_col(true, a) FILTER (WHERE flag = 1) AS delta FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Null,
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "delta".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Null,
                        Value::Int64(2),
                        Value::Null,
                        Value::Null,
                        Value::Int64(3),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "changed_col_ignore_null_false_treats_null_as_change",
            sql: "SELECT changed_col(false, a) AS delta FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Null,
                    Value::Null,
                    Value::Int64(2),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "delta".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int64(2),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "changed_col_filter_false_always_returns_null",
            sql: "SELECT changed_col(true, a) FILTER (WHERE flag = 1) AS delta FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(4),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "delta".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Null,
                        Value::Null,
                        Value::Int64(4),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "had_changed_detects_any_input_change",
            sql: "SELECT had_changed(true, a, b) FILTER (WHERE flag = 1) AS changed FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::String("x".to_string()),
                        Value::String("x".to_string()),
                        Value::String("y".to_string()),
                        Value::String("y".to_string()),
                        Value::Null,
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "changed".to_string(),
                    expected_values: vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(true),
                        Value::Bool(false),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "had_changed_ignore_null_false_null_affects_state",
            sql: "SELECT had_changed(false, a, b) AS changed FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Null,
                        Value::Null,
                        Value::Int64(1),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::String("x".to_string()),
                        Value::String("x".to_string()),
                        Value::String("x".to_string()),
                        Value::Null,
                        Value::Null,
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "changed".to_string(),
                    expected_values: vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(true),
                        Value::Bool(true),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "had_changed_filter_false_returns_false_and_does_not_advance",
            sql: "SELECT had_changed(true, a, b) FILTER (WHERE flag = 1) AS changed FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::String("x".to_string()),
                        Value::String("y".to_string()),
                        Value::String("y".to_string()),
                        Value::String("y".to_string()),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "changed".to_string(),
                    expected_values: vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(true),
                    ],
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
        StatefulCase {
            name: "change_to_emits_on_transition_to_target",
            sql: "SELECT change_to(true, a, 2) AS hit FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Int64(2),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "hit".to_string(),
                    expected_values: vec![
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "change_to_filter_freezes_state",
            sql: "SELECT change_to(true, a, 2) FILTER (WHERE flag = 1) AS hit FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(2),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "hit".to_string(),
                    expected_values: vec![
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "change_capture_holds_value_on_any_monitor_change",
            sql: "SELECT change_capture(true, a, b) AS cap FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(50),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "cap".to_string(),
                    expected_values: vec![
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(30),
                        Value::Int64(30),
                        Value::Int64(50),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "change_capture_captures_only_on_change_to_target",
            sql: "SELECT change_capture(true, a, b, 2) AS cap FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                        Value::Int64(50),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "cap".to_string(),
                    expected_values: vec![
                        Value::Null,
                        Value::Int64(20),
                        Value::Int64(20),
                        Value::Int64(20),
                        Value::Int64(50),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "change_capture_filter_holds_and_freezes_state",
            sql: "SELECT change_capture(true, a, b) FILTER (WHERE flag = 1) AS cap FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(30),
                        Value::Int64(40),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Int64(3),
                    ],
                ),
                (
                    "flag".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(0),
                        Value::Int64(0),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 4,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "cap".to_string(),
                    expected_values: vec![
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int64(40),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "consecutive_count_counts_true_run_and_resets",
            sql: "SELECT consecutive_count(a > 5) AS c FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(10),
                    Value::Int64(20),
                    Value::Int64(3),
                    Value::Int64(20),
                    Value::Int64(30),
                    Value::Int64(1),
                ],
            )],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 6,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "c".to_string(),
                    expected_values: vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(0),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(0),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            name: "consecutive_start_holds_run_start_and_clears_on_false",
            sql: "SELECT consecutive_start(a > 5, ts) AS s FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(20),
                        Value::Int64(3),
                        Value::Int64(40),
                        Value::Int64(50),
                    ],
                ),
                (
                    "ts".to_string(),
                    vec![
                        Value::Int64(100),
                        Value::Int64(200),
                        Value::Int64(300),
                        Value::Int64(400),
                        Value::Int64(500),
                    ],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 5,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![
                        Value::Int64(100),
                        Value::Int64(100),
                        Value::Null,
                        Value::Int64(400),
                        Value::Int64(400),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            // A filtered-out row whose condition is false must NOT reset the run:
            // if it did, the final row would recapture ts=300 instead of holding 100.
            name: "consecutive_start_filter_freezes_state",
            sql: "SELECT consecutive_start(a > 5, ts) FILTER (WHERE flag = 1) AS s FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(3), Value::Int64(40)],
                ),
                (
                    "ts".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(1), Value::Int64(0), Value::Int64(1)],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 1,
                column_checks: vec![ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![
                        Value::Int64(100),
                        Value::Int64(100),
                        Value::Int64(100),
                    ],
                }],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
        StatefulCase {
            // Regression for the eKuiper empty-key collision: two consecutive_start
            // calls in one statement must keep independent state.
            name: "consecutive_start_multiple_calls_are_independent",
            sql: "SELECT consecutive_start(a > 5, ts) AS s1, consecutive_start(b > 5, ts) AS s2 FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(3), Value::Int64(10)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(3), Value::Int64(10), Value::Int64(10)],
                ),
                (
                    "ts".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_outputs: vec![ExpectedCollection {
                expected_rows: 3,
                expected_columns: 2,
                column_checks: vec![
                    ColumnCheck {
                        expected_name: "s1".to_string(),
                        expected_values: vec![
                            Value::Int64(100),
                            Value::Null,
                            Value::Int64(300),
                        ],
                    },
                    ColumnCheck {
                        expected_name: "s2".to_string(),
                        expected_values: vec![
                            Value::Null,
                            Value::Int64(200),
                            Value::Int64(200),
                        ],
                    },
                ],
            }],
            wait_after_send: Duration::from_millis(0),
            close_before_read: false,
        },
    ];

    for case in cases {
        run_stateful_case(case).await;
    }
}

// coverage-covers: parser.function.stateful_functions, sink.output.row_diff
#[tokio::test]
async fn stateful_projection_followed_by_row_diff_delta_output() {
    let case_name = "stateful_projection_followed_by_row_diff_delta_output";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("stateful_function_delta", case_name);
    let input_data = vec![(
        "a".to_string(),
        vec![
            Value::Int64(1),
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3),
        ],
    )];
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT lag(a) AS prev FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_output(SinkOutputConfig::delta())],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case_name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {"prev": null},
        {"prev": 1},
        {},
        {"prev": 2}
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong delta JSON for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for test {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}

// coverage-covers: parser.function.stateful_functions, sink.output.row_diff, sink.output.omit_if_empty
#[tokio::test]
async fn acc_lifecycle_delta_omit_if_empty_suppresses_unchanged_inactive_state() {
    let case_name = "acc_lifecycle_delta_omit_if_empty_suppresses_unchanged_inactive_state";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("stateful_function_delta_omit", case_name);
    let schema_hint = vec![
        (
            "a".to_string(),
            vec![Value::Float64(10.0), Value::Float64(20.0)],
        ),
        (
            "start".to_string(),
            vec![Value::Bool(false), Value::Bool(true)],
        ),
        (
            "stop".to_string(),
            vec![Value::Bool(false), Value::Bool(true)],
        ),
    ];
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &schema_hint).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT acc_sum(a, start, stop) AS total FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_output(SinkOutputConfig::delta().with_omit_if_empty(true))],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    let inactive_batch = batch_from_columns_simple(vec![
        (
            "stream".to_string(),
            "a".to_string(),
            vec![Value::Float64(10.0), Value::Float64(20.0)],
        ),
        (
            "stream".to_string(),
            "start".to_string(),
            vec![Value::Bool(false), Value::Bool(false)],
        ),
        (
            "stream".to_string(),
            "stop".to_string(),
            vec![Value::Bool(false), Value::Bool(false)],
        ),
    ])
    .expect("create inactive acc batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(inactive_batch),
        timeout_duration,
    )
    .await;

    let first_actual: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let first_expected = serde_json::json!([
        {"total": 0.0},
        {}
    ]);
    assert_eq!(
        normalize_json(first_actual),
        normalize_json(first_expected),
        "Wrong initial inactive delta JSON for test: {}",
        case_name
    );

    let repeated_inactive_batch = batch_from_columns_simple(vec![
        (
            "stream".to_string(),
            "a".to_string(),
            vec![Value::Float64(10.0), Value::Float64(20.0)],
        ),
        (
            "stream".to_string(),
            "start".to_string(),
            vec![Value::Bool(false), Value::Bool(false)],
        ),
        (
            "stream".to_string(),
            "stop".to_string(),
            vec![Value::Bool(false), Value::Bool(false)],
        ),
    ])
    .expect("create repeated inactive acc batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(repeated_inactive_batch),
        timeout_duration,
    )
    .await;
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    let active_batch = batch_from_columns_simple(vec![
        (
            "stream".to_string(),
            "a".to_string(),
            vec![
                Value::Float64(1.0),
                Value::Float64(2.0),
                Value::Float64(3.0),
                Value::Float64(4.0),
            ],
        ),
        (
            "stream".to_string(),
            "start".to_string(),
            vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
            ],
        ),
        (
            "stream".to_string(),
            "stop".to_string(),
            vec![
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
            ],
        ),
    ])
    .expect("create active acc batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(active_batch),
        timeout_duration,
    )
    .await;

    let active_actual: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let active_expected = serde_json::json!([
        {"total": 1.0},
        {"total": 3.0},
        {"total": 0.0},
        {"total": 4.0}
    ]);
    assert_eq!(
        normalize_json(active_actual),
        normalize_json(active_expected),
        "Wrong active lifecycle delta JSON for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for test {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}

// coverage-covers: parser.function.stateful_functions, sink.output.batching, sink.connector.memory_output
#[tokio::test]
async fn stateful_projection_with_batched_sink_encoder() {
    let case_name = "stateful_projection_with_batched_sink_encoder";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("stateful_function_batching", case_name);
    let input_data = vec![(
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )];
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT lag(a) AS prev FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_common_props(CommonSinkProps {
            batch_count: Some(2),
            batch_duration: Some(Duration::from_millis(50)),
        })],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case_name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual_first: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let expected_first = serde_json::json!([
        {},
        {"prev": 1}
    ]);
    assert_eq!(
        normalize_json(actual_first),
        normalize_json(expected_first),
        "Wrong first batched JSON for test: {}",
        case_name
    );

    let actual_second: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let expected_second = serde_json::json!([
        {"prev": 2}
    ]);
    assert_eq!(
        normalize_json(actual_second),
        normalize_json(expected_second),
        "Wrong second batched JSON for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for test {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}

// coverage-covers: parser.function.stateful_functions, sink.output.batching, sink.connector.memory_output
#[tokio::test]
async fn acc_lifecycle_with_batched_sink_encoder() {
    let case_name = "acc_lifecycle_with_batched_sink_encoder";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("stateful_function_batching", case_name);
    let input_data = vec![
        (
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        ),
        (
            "start".to_string(),
            vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
        ),
        (
            "stop".to_string(),
            vec![Value::Bool(false), Value::Bool(true), Value::Bool(false)],
        ),
    ];
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT acc_count(a, start, stop) AS n FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_common_props(CommonSinkProps {
            batch_count: Some(2),
            batch_duration: Some(Duration::from_millis(50)),
        })],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case_name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual_first: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let expected_first = serde_json::json!([
        {"n": 1},
        {"n": 2}
    ]);
    assert_eq!(
        normalize_json(actual_first),
        normalize_json(expected_first),
        "Wrong first acc batched JSON for test: {}",
        case_name
    );

    let actual_second: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let expected_second = serde_json::json!([
        {"n": 1}
    ]);
    assert_eq!(
        normalize_json(actual_second),
        normalize_json(expected_second),
        "Wrong second acc batched JSON for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for test {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}

#[tokio::test]
async fn streaming_state_window_graceful_close_flushes_active_window() {
    let case_name = "streaming_state_window_graceful_close_flushes_active_window";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("stateful_function_graceful_stop", case_name);
    let input_data = vec![(
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )];
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT sum(a) AS s FROM stream GROUP BY statewindow(a = 1, a = 9)",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case_name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Graceful, timeout_duration)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "Failed to gracefully stop pipeline for test {}: {err}",
                case_name
            )
        });

    let actual: JsonValue = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([{ "s": 6 }]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong JSON flushed by graceful stop for test: {}",
        case_name
    );

    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}
