//! Pipeline tests for window `GROUP BY` keys and `OVER (PARTITION BY ...)`.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::catalog::{
    EventtimeDefinition, MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::model::batch_from_columns_simple;
use flow::pipeline::{EventtimeOptions, MemorySinkProps, PipelineDefinition, PipelineOptions};
use flow::FlowInstance;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::Duration;

use super::common::{
    assert_no_json_output, declare_memory_input_output_topics, install_memory_stream_schema,
    make_memory_topics, normalize_json, publish_input_collection, recv_next_json,
};

async fn install_window_partition_stream(instance: &FlowInstance, input_topic: &str) {
    let stream_name = "stream_eventtime";
    let schema = Schema::new(
        ["a", "k1", "k2", "m", "event_ts"]
            .into_iter()
            .map(|name| {
                ColumnSchema::new(
                    stream_name.to_string(),
                    name.to_string(),
                    ConcreteDatatype::Int64(Int64Type),
                )
            })
            .collect(),
    );
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::json(),
    )
    .with_eventtime(EventtimeDefinition::new("event_ts", "unixtimestamp_ms"));
    instance
        .create_stream(definition, false)
        .await
        .expect("create window partition stream");
}

fn eventtime_pipeline_options() -> PipelineOptions {
    PipelineOptions {
        eventtime: EventtimeOptions {
            enabled: true,
            late_tolerance: Duration::from_millis(7_000),
        },
        ..PipelineOptions::default()
    }
}

async fn publish_json_row(instance: &FlowInstance, input_topic: &str, row: JsonValue) {
    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(input_topic)
        .expect("open bytes publisher");
    publisher
        .publish_bytes(serde_json::to_vec(&row).expect("encode input row"))
        .expect("publish bytes row");
}

struct WindowPartitionCase {
    name: &'static str,
    sql: &'static str,
    input_rows: Vec<JsonValue>,
    graceful_stop_before_expect: bool,
    expected_outputs: Vec<JsonValue>,
    assert_no_extra_output: bool,
}

struct CollectionWindowPartitionCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    expected_outputs: Vec<JsonValue>,
    assert_no_extra_output: bool,
}

async fn run_collection_window_partition_case(case: CollectionWindowPartitionCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_window_partition", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create window partition pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start window partition pipeline");

    let columns = case
        .input_data
        .into_iter()
        .map(|(name, values)| ("stream".to_string(), name, values))
        .collect();
    let batch = batch_from_columns_simple(columns).expect("build input batch");
    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let mut actual_outputs = Vec::with_capacity(case.expected_outputs.len());
    for _ in 0..case.expected_outputs.len() {
        actual_outputs.push(recv_next_json(&mut output, timeout_duration).await);
    }

    assert_json_output_multiset_eq(actual_outputs, case.expected_outputs, case.name);

    if case.assert_no_extra_output {
        assert_no_json_output(&mut output, Duration::from_millis(300)).await;
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop window partition pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete window partition pipeline");
}

async fn run_window_partition_case(case: WindowPartitionCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_window_partition", case.name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
    install_window_partition_stream(&instance, &input_topic).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink])
        .with_options(eventtime_pipeline_options());
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create window partition pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start window partition pipeline");

    for row in case.input_rows {
        publish_json_row(&instance, &input_topic, row).await;
    }

    let timeout_duration = Duration::from_secs(5);
    if case.graceful_stop_before_expect {
        instance
            .stop_pipeline(&pipeline_id, PipelineStopMode::Graceful, timeout_duration)
            .await
            .expect("gracefully stop window partition pipeline");
    }

    let mut actual_outputs = Vec::with_capacity(case.expected_outputs.len());
    for _ in 0..case.expected_outputs.len() {
        actual_outputs.push(recv_next_json(&mut output, timeout_duration).await);
    }

    assert_json_output_multiset_eq(actual_outputs, case.expected_outputs, case.name);

    if case.assert_no_extra_output {
        assert_no_json_output(&mut output, Duration::from_millis(300)).await;
    }

    if !case.graceful_stop_before_expect {
        instance
            .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
            .await
            .expect("stop window partition pipeline");
    }
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete window partition pipeline");
}

fn assert_json_output_multiset_eq(
    actual: Vec<JsonValue>,
    expected: Vec<JsonValue>,
    case_name: &str,
) {
    let mut actual = actual
        .into_iter()
        .map(normalized_json_string)
        .collect::<Vec<_>>();
    let mut expected = expected
        .into_iter()
        .map(normalized_json_string)
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected, "wrong output JSON for test: {case_name}");
}

fn normalized_json_string(value: JsonValue) -> String {
    serde_json::to_string(&normalize_json(value)).expect("serialize normalized json")
}

fn base_rows() -> Vec<JsonValue> {
    vec![
        serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 1000}),
        serde_json::json!({"a": 10, "k1": 2, "k2": 10, "m": 0, "event_ts": 2000}),
        serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 3000}),
        serde_json::json!({"a": 20, "k1": 2, "k2": 20, "m": 0, "event_ts": 4000}),
    ]
}

fn count_partition_group_rows() -> Vec<JsonValue> {
    vec![
        serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 1000}),
        serde_json::json!({"a": 100, "k1": 2, "k2": 10, "m": 0, "event_ts": 2000}),
        serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 3000}),
        serde_json::json!({"a": 200, "k1": 2, "k2": 20, "m": 0, "event_ts": 4000}),
        serde_json::json!({"a": 3, "k1": 1, "k2": 10, "m": 0, "event_ts": 5000}),
        serde_json::json!({"a": 4, "k1": 1, "k2": 20, "m": 0, "event_ts": 6000}),
    ]
}

fn tumbling_flush_row() -> JsonValue {
    serde_json::json!({"a": 99, "k1": 9, "k2": 99, "m": 0, "event_ts": 17000})
}

fn columns_from_rows(rows: Vec<JsonValue>) -> Vec<(String, Vec<Value>)> {
    ["a", "k1", "k2", "m"]
        .into_iter()
        .map(|name| {
            let values = rows
                .iter()
                .map(|row| {
                    Value::Int64(
                        row.get(name)
                            .and_then(JsonValue::as_i64)
                            .unwrap_or_else(|| panic!("missing int64 field `{name}` in {row}")),
                    )
                })
                .collect();
            (name.to_string(), values)
        })
        .collect()
}

// coverage-covers: parser.window.count.over_partition_by, pipeline.window.partition_by, stream.window.count
#[tokio::test]
async fn count_window_group_and_partition_pipeline_table_driven() {
    let cases = vec![
        CollectionWindowPartitionCase {
            name: "count_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream GROUP BY countwindow(4), k2 ORDER BY k2",
            input_data: columns_from_rows(base_rows()),
            expected_outputs: vec![serde_json::json!([
                {"k2": 10, "s": 11},
                {"k2": 20, "s": 22},
            ])],
            assert_no_extra_output: true,
        },
        CollectionWindowPartitionCase {
            name: "count_partition_by_key",
            sql: "SELECT sum(a) AS s FROM stream GROUP BY countwindow(2) OVER (PARTITION BY k1)",
            input_data: columns_from_rows(base_rows()),
            expected_outputs: vec![
                serde_json::json!([{"s": 3}]),
                serde_json::json!([{"s": 30}]),
            ],
            assert_no_extra_output: true,
        },
        CollectionWindowPartitionCase {
            name: "count_partition_by_and_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream GROUP BY countwindow(4) OVER (PARTITION BY k1), k2 ORDER BY k2",
            input_data: columns_from_rows(count_partition_group_rows()),
            expected_outputs: vec![serde_json::json!([
                {"k2": 10, "s": 4},
                {"k2": 20, "s": 6},
            ])],
            assert_no_extra_output: true,
        },
        CollectionWindowPartitionCase {
            name: "count_partition_by_and_group_by_key_non_streaming",
            sql: "SELECT k2, ndv(a) AS n FROM stream GROUP BY countwindow(4) OVER (PARTITION BY k1), k2 ORDER BY k2",
            input_data: columns_from_rows(vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 1000}),
                serde_json::json!({"a": 9, "k1": 2, "k2": 10, "m": 0, "event_ts": 2000}),
                serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 3000}),
                serde_json::json!({"a": 9, "k1": 2, "k2": 20, "m": 0, "event_ts": 4000}),
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 5000}),
                serde_json::json!({"a": 4, "k1": 1, "k2": 20, "m": 0, "event_ts": 6000}),
            ]),
            expected_outputs: vec![serde_json::json!([
                {"k2": 10, "n": 1},
                {"k2": 20, "n": 2},
            ])],
            assert_no_extra_output: true,
        },
    ];

    for case in cases {
        run_collection_window_partition_case(case).await;
    }
}

// coverage-covers: parser.window.tumbling.over_partition_by, pipeline.runtime.eventtime, pipeline.window.partition_by, stream.window.tumbling
#[tokio::test]
async fn tumbling_window_group_and_partition_pipeline_table_driven() {
    let mut group_rows = base_rows();
    group_rows.push(tumbling_flush_row());

    let mut partition_group_rows = count_partition_group_rows();
    partition_group_rows.push(tumbling_flush_row());

    let cases = vec![
        WindowPartitionCase {
            name: "tumbling_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10), k2 ORDER BY k2",
            input_rows: group_rows,
            graceful_stop_before_expect: false,
            expected_outputs: vec![serde_json::json!([
                {"k2": 10, "s": 11},
                {"k2": 20, "s": 22},
            ])],
            assert_no_extra_output: true,
        },
        WindowPartitionCase {
            name: "tumbling_partition_by_key",
            sql: "SELECT sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10) OVER (PARTITION BY k1)",
            input_rows: {
                let mut rows = base_rows();
                rows.push(tumbling_flush_row());
                rows
            },
            graceful_stop_before_expect: false,
            expected_outputs: vec![
                serde_json::json!([{"s": 3}]),
                serde_json::json!([{"s": 30}]),
            ],
            assert_no_extra_output: true,
        },
        WindowPartitionCase {
            name: "tumbling_partition_by_and_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10) OVER (PARTITION BY k1), k2 ORDER BY k2",
            input_rows: partition_group_rows,
            graceful_stop_before_expect: false,
            expected_outputs: vec![
                serde_json::json!([
                    {"k2": 10, "s": 4},
                    {"k2": 20, "s": 6},
                ]),
                serde_json::json!([
                    {"k2": 10, "s": 100},
                    {"k2": 20, "s": 200},
                ]),
            ],
            assert_no_extra_output: true,
        },
    ];

    for case in cases {
        run_window_partition_case(case).await;
    }
}

// coverage-covers: parser.window.sliding.over_partition_by, pipeline.runtime.eventtime, pipeline.window.partition_by, stream.window.sliding
#[tokio::test]
async fn sliding_window_group_and_partition_pipeline_table_driven() {
    let cases = vec![
        WindowPartitionCase {
            name: "sliding_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream_eventtime GROUP BY slidingwindow('ss', 10, 5), k2 ORDER BY k2",
            input_rows: vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 1000}),
                serde_json::json!({"a": 10, "k1": 2, "k2": 20, "m": 0, "event_ts": 2000}),
            ],
            graceful_stop_before_expect: true,
            expected_outputs: vec![
                serde_json::json!([
                    {"k2": 10, "s": 1},
                    {"k2": 20, "s": 10},
                ]),
                serde_json::json!([{"k2": 20, "s": 10}]),
            ],
            assert_no_extra_output: true,
        },
        WindowPartitionCase {
            name: "sliding_partition_by_key",
            sql: "SELECT sum(a) AS s FROM stream_eventtime GROUP BY slidingwindow('ss', 10, 5) OVER (PARTITION BY k1)",
            input_rows: vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 1000}),
                serde_json::json!({"a": 10, "k1": 2, "k2": 20, "m": 0, "event_ts": 2000}),
            ],
            graceful_stop_before_expect: true,
            expected_outputs: vec![
                serde_json::json!([{"s": 1}]),
                serde_json::json!([{"s": 10}]),
            ],
            assert_no_extra_output: true,
        },
        WindowPartitionCase {
            name: "sliding_partition_by_and_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream_eventtime GROUP BY slidingwindow('ss', 10, 5) OVER (PARTITION BY k1), k2 ORDER BY k2",
            input_rows: vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 0, "event_ts": 1000}),
                serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 2000}),
                serde_json::json!({"a": 100, "k1": 2, "k2": 10, "m": 0, "event_ts": 3000}),
            ],
            graceful_stop_before_expect: true,
            expected_outputs: vec![
                serde_json::json!([
                    {"k2": 10, "s": 1},
                    {"k2": 20, "s": 2},
                ]),
                serde_json::json!([{"k2": 20, "s": 2}]),
                serde_json::json!([{"k2": 10, "s": 100}]),
            ],
            assert_no_extra_output: true,
        },
    ];

    for case in cases {
        run_window_partition_case(case).await;
    }
}

// coverage-covers: parser.window.state.over_partition_by, pipeline.window.partition_by, stream.window.state
#[tokio::test]
async fn state_window_group_and_partition_pipeline_table_driven() {
    let cases = vec![
        CollectionWindowPartitionCase {
            name: "state_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream GROUP BY statewindow(m = 1, m = 9), k2 ORDER BY k2",
            input_data: columns_from_rows(vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 1, "event_ts": 1000}),
                serde_json::json!({"a": 10, "k1": 2, "k2": 10, "m": 0, "event_ts": 2000}),
                serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 3000}),
                serde_json::json!({"a": 20, "k1": 2, "k2": 20, "m": 9, "event_ts": 4000}),
            ]),
            expected_outputs: vec![serde_json::json!([
                {"k2": 10, "s": 11},
                {"k2": 20, "s": 22},
            ])],
            assert_no_extra_output: true,
        },
        CollectionWindowPartitionCase {
            name: "state_partition_by_key",
            sql: "SELECT sum(a) AS s FROM stream GROUP BY statewindow(m = 1, m = 9) OVER (PARTITION BY k1)",
            input_data: columns_from_rows(vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 1, "event_ts": 1000}),
                serde_json::json!({"a": 10, "k1": 2, "k2": 10, "m": 1, "event_ts": 2000}),
                serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 3000}),
                serde_json::json!({"a": 20, "k1": 2, "k2": 20, "m": 0, "event_ts": 4000}),
                serde_json::json!({"a": 3, "k1": 1, "k2": 10, "m": 9, "event_ts": 5000}),
                serde_json::json!({"a": 30, "k1": 2, "k2": 20, "m": 9, "event_ts": 6000}),
            ]),
            expected_outputs: vec![
                serde_json::json!([{"s": 6}]),
                serde_json::json!([{"s": 60}]),
            ],
            assert_no_extra_output: true,
        },
        CollectionWindowPartitionCase {
            name: "state_partition_by_and_group_by_key",
            sql: "SELECT k2, sum(a) AS s FROM stream GROUP BY statewindow(m = 1, m = 9) OVER (PARTITION BY k1), k2 ORDER BY k2",
            input_data: columns_from_rows(vec![
                serde_json::json!({"a": 1, "k1": 1, "k2": 10, "m": 1, "event_ts": 1000}),
                serde_json::json!({"a": 100, "k1": 2, "k2": 10, "m": 1, "event_ts": 2000}),
                serde_json::json!({"a": 2, "k1": 1, "k2": 20, "m": 0, "event_ts": 3000}),
                serde_json::json!({"a": 3, "k1": 1, "k2": 10, "m": 0, "event_ts": 4000}),
                serde_json::json!({"a": 4, "k1": 1, "k2": 20, "m": 9, "event_ts": 5000}),
            ]),
            expected_outputs: vec![serde_json::json!([
                {"k2": 10, "s": 4},
                {"k2": 20, "s": 6},
            ])],
            assert_no_extra_output: true,
        },
    ];

    for case in cases {
        run_collection_window_partition_case(case).await;
    }
}
