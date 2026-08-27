//! Integration tests for eventtime-enabled pipeline behavior.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{
    EventtimeDefinition, MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{
    EventtimeOptions, MemorySinkProps, PipelineDefinition, PipelineOptions, SourceDefinition,
    SourceInputConfig,
};
use flow::FlowInstance;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use std::sync::Arc;
use tokio::time::Duration;

use super::common::{assert_no_json_output, make_memory_topics, recv_next_json};

async fn install_memory_json_eventtime_stream(
    instance: &FlowInstance,
    input_topic: &str,
    stream_name: &str,
) {
    let schema = Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "event_ts".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]);
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
        .expect("create eventtime stream");
}

async fn install_memory_json_eventtime_stream_with_schema(
    instance: &FlowInstance,
    input_topic: &str,
    stream_name: &str,
    columns: &[&str],
    eventtime_column: &str,
) {
    let schema = Schema::new(
        columns
            .iter()
            .map(|name| {
                ColumnSchema::new(
                    stream_name.to_string(),
                    (*name).to_string(),
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
    .with_eventtime(EventtimeDefinition::new(
        eventtime_column,
        "unixtimestamp_ms",
    ));
    instance
        .create_stream(definition, false)
        .await
        .expect("create eventtime stream");
}

fn eventtime_pipeline_options(late_tolerance_ms: u64) -> PipelineOptions {
    PipelineOptions {
        eventtime: EventtimeOptions {
            enabled: true,
            late_tolerance: Duration::from_millis(late_tolerance_ms),
        },
        ..PipelineOptions::default()
    }
}

async fn publish_json_row(instance: &FlowInstance, input_topic: &str, row: serde_json::Value) {
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

struct WindowMetadataCase {
    name: &'static str,
    sql: &'static str,
    input_rows: Vec<serde_json::Value>,
    stop_before_expect: bool,
    expected: serde_json::Value,
}

struct LastHitTimeCase {
    name: &'static str,
    sql: &'static str,
    input_rows: Vec<serde_json::Value>,
    expected_outputs: Vec<serde_json::Value>,
    assert_no_extra_output: bool,
}

async fn run_window_metadata_case(case: WindowMetadataCase) {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_eventtime", case.name);
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
    install_memory_json_eventtime_stream(&instance, &input_topic, "stream_eventtime").await;

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
        .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    for row in case.input_rows {
        publish_json_row(&instance, &input_topic, row).await;
    }

    if case.stop_before_expect {
        instance
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Graceful,
                Duration::from_secs(5),
            )
            .await
            .expect("gracefully stop eventtime pipeline");
    }

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(actual, case.expected, "wrong output for {}", case.name);

    if !case.stop_before_expect {
        instance
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Quick,
                Duration::from_secs(5),
            )
            .await
            .expect("stop eventtime pipeline");
    }
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}

async fn run_last_hit_time_case(case: LastHitTimeCase) {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_eventtime", case.name);
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
    install_memory_json_eventtime_stream(&instance, &input_topic, "stream_eventtime").await;

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
        .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    for row in case.input_rows {
        publish_json_row(&instance, &input_topic, row).await;
    }

    let timeout_duration = Duration::from_secs(5);
    let mut actual_outputs = Vec::with_capacity(case.expected_outputs.len());
    for _ in 0..case.expected_outputs.len() {
        actual_outputs.push(recv_next_json(&mut output, timeout_duration).await);
    }
    assert_eq!(
        actual_outputs, case.expected_outputs,
        "wrong outputs for {}",
        case.name
    );

    if case.assert_no_extra_output {
        assert_no_json_output(&mut output, Duration::from_millis(300)).await;
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop eventtime pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}

// coverage-covers: expr.window_metadata, pipeline.runtime.eventtime, stream.watermark.propagation, stream.window.tumbling, stream.window.count, stream.window.state
#[tokio::test]
async fn eventtime_window_metadata_table_driven() {
    let cases = vec![
        WindowMetadataCase {
            name: "eventtime_tumbling_window_projects_window_metadata",
            sql: "SELECT window_start() AS ws, window_end() AS we, sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
            input_rows: vec![
                serde_json::json!({"a": 1, "event_ts": 1000}),
                serde_json::json!({"a": 4, "event_ts": 4000}),
                serde_json::json!({"a": 7, "event_ts": 7000}),
                serde_json::json!({"a": 17, "event_ts": 17000}),
            ],
            stop_before_expect: false,
            expected: serde_json::json!([{
                "ws": "1970-01-01T00:00:00.000000Z",
                "we": "1970-01-01T00:00:10.000000Z",
                "s": 12,
            }]),
        },
        WindowMetadataCase {
            name: "eventtime_millisecond_tumbling_window_projects_window_metadata",
            sql: "SELECT window_start() AS ws, window_end() AS we, sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ms', 100)",
            input_rows: vec![
                serde_json::json!({"a": 2, "event_ts": 1050}),
                serde_json::json!({"a": 3, "event_ts": 1099}),
                serde_json::json!({"a": 7, "event_ts": 1100}),
                serde_json::json!({"a": 17, "event_ts": 8100}),
            ],
            stop_before_expect: false,
            expected: serde_json::json!([{
                "ws": "1970-01-01T00:00:01.000000Z",
                "we": "1970-01-01T00:00:01.100000Z",
                "s": 5,
            }]),
        },
        WindowMetadataCase {
            name: "eventtime_count_window_projects_lifecycle_metadata",
            sql: "SELECT window_start() AS ws, window_end() AS we, sum(a) AS s FROM stream_eventtime GROUP BY countwindow(3)",
            input_rows: vec![
                serde_json::json!({"a": 7, "event_ts": 7000}),
                serde_json::json!({"a": 1, "event_ts": 1000}),
                serde_json::json!({"a": 4, "event_ts": 4000}),
                serde_json::json!({"a": 17, "event_ts": 17000}),
            ],
            stop_before_expect: false,
            expected: serde_json::json!([{
                "ws": "1970-01-01T00:00:01.000000Z",
                "we": "1970-01-01T00:00:07.000000Z",
                "s": 12,
            }]),
        },
        WindowMetadataCase {
            name: "eventtime_state_window_projects_lifecycle_metadata",
            sql: "SELECT window_start() AS ws, window_end() AS we, sum(a) AS s FROM stream_eventtime GROUP BY statewindow(a = 1, a = 3)",
            input_rows: vec![
                serde_json::json!({"a": 3, "event_ts": 3000}),
                serde_json::json!({"a": 1, "event_ts": 1000}),
                serde_json::json!({"a": 2, "event_ts": 2000}),
                serde_json::json!({"a": 12, "event_ts": 12000}),
            ],
            stop_before_expect: false,
            expected: serde_json::json!([{
                "ws": "1970-01-01T00:00:01.000000Z",
                "we": "1970-01-01T00:00:03.000000Z",
                "s": 6,
            }]),
        },
    ];

    for case in cases {
        run_window_metadata_case(case).await;
    }
}

// coverage-covers: expr.pipeline_state, pipeline.runtime.eventtime
#[tokio::test]
async fn eventtime_last_hit_time_unix_ms_table_driven() {
    let cases = vec![
        LastHitTimeCase {
            name: "last_hit_time_select_reads_previous_project_hit",
            sql: "SELECT last_hit_time_unix_ms() AS prev_ts, a FROM stream_eventtime",
            input_rows: vec![
                serde_json::json!({"a": 1, "event_ts": 1000}),
                serde_json::json!({"a": 2, "event_ts": 2000}),
                serde_json::json!({"a": 3, "event_ts": 3000}),
            ],
            expected_outputs: vec![
                serde_json::json!([{"prev_ts": 0, "a": 1}]),
                serde_json::json!([{"prev_ts": 1000, "a": 2}]),
                serde_json::json!([{"prev_ts": 2000, "a": 3}]),
            ],
            assert_no_extra_output: true,
        },
        LastHitTimeCase {
            name: "last_hit_time_where_updates_after_accepted_rows",
            sql: "SELECT a FROM stream_eventtime WHERE last_hit_time_unix_ms() < 2500",
            input_rows: vec![
                serde_json::json!({"a": 1, "event_ts": 1000}),
                serde_json::json!({"a": 2, "event_ts": 2000}),
                serde_json::json!({"a": 3, "event_ts": 3000}),
                serde_json::json!({"a": 4, "event_ts": 4000}),
            ],
            expected_outputs: vec![
                serde_json::json!([{"a": 1}]),
                serde_json::json!([{"a": 2}]),
                serde_json::json!([{"a": 3}]),
            ],
            assert_no_extra_output: true,
        },
    ];

    for case in cases {
        run_last_hit_time_case(case).await;
    }
}

// coverage-covers: pipeline.runtime.eventtime, stream.watermark.propagation, stream.window.tumbling
#[tokio::test]
async fn eventtime_tumbling_window_orders_out_of_order_input_before_flush() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_eventtime",
        "eventtime_tumbling_window_orders_out_of_order_input_before_flush",
    );
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
    install_memory_json_eventtime_stream(&instance, &input_topic, "stream_eventtime").await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
        vec![sink],
    )
    .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 7, "event_ts": 7000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 1, "event_ts": 1000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 4, "event_ts": 4000}),
    )
    .await;

    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 17, "event_ts": 17000}),
    )
    .await;

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        actual,
        serde_json::json!([{"s": 12}]),
        "out-of-order tuples within late_tolerance should aggregate correctly once watermark advances",
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop eventtime pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}

// coverage-covers: planner.eventtime.hidden_column_preservation, planner.physical.streaming_aggregation_rewrite, pipeline.runtime.eventtime, stream.watermark.propagation, stream.window.tumbling
#[tokio::test]
async fn eventtime_tumbling_window_drops_tuple_older_than_current_watermark() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_eventtime",
        "eventtime_tumbling_window_drops_tuple_older_than_current_watermark",
    );
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
    install_memory_json_eventtime_stream(&instance, &input_topic, "stream_eventtime").await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
        vec![sink],
    )
    .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 7, "event_ts": 7000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 1, "event_ts": 1000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 4, "event_ts": 4000}),
    )
    .await;

    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 17, "event_ts": 17000}),
    )
    .await;

    let first_window = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        first_window,
        serde_json::json!([{"s": 12}]),
        "watermark advancement should flush the first eventtime window",
    );

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 100, "event_ts": 2000}),
    )
    .await;
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 27, "event_ts": 27000}),
    )
    .await;

    let final_window = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        final_window,
        serde_json::json!([{"s": 17}]),
        "late tuple should be dropped and must not alter the next flushed eventtime window",
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop eventtime pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}

// coverage-covers: pipeline.runtime.eventtime, source.on_change.gating, stream.watermark.propagation, stream.window.tumbling
#[tokio::test]
async fn eventtime_tumbling_window_with_on_change_gate_suppresses_unchanged_rows() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_eventtime",
        "eventtime_tumbling_window_with_on_change_gate_suppresses_unchanged_rows",
    );
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
    install_memory_json_eventtime_stream_with_schema(
        &instance,
        &input_topic,
        "stream_eventtime",
        &["speed", "rpm", "event_ts"],
        "event_ts",
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT sum(speed) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
        vec![sink],
    )
    .with_sources(vec![SourceDefinition::new("stream_eventtime")
        .with_input(SourceInputConfig::on_change_with_columns(["rpm"]))])
    .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 10, "rpm": 1000, "event_ts": 1000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 20, "rpm": 1000, "event_ts": 2000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 30, "rpm": 2000, "event_ts": 3000}),
    )
    .await;

    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 1, "rpm": 3000, "event_ts": 17000}),
    )
    .await;

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        actual,
        serde_json::json!([{"s": 40}]),
        "on_change gating should suppress unchanged rpm rows before eventtime aggregation",
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop eventtime pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}

// coverage-covers: pipeline.runtime.eventtime, source.on_change.gating, stream.watermark.propagation, stream.window.tumbling
#[tokio::test]
async fn eventtime_tumbling_window_with_on_change_gate_drops_late_rows_after_watermark() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_eventtime",
        "eventtime_tumbling_window_with_on_change_gate_drops_late_rows_after_watermark",
    );
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
    install_memory_json_eventtime_stream_with_schema(
        &instance,
        &input_topic,
        "stream_eventtime",
        &["speed", "rpm", "event_ts"],
        "event_ts",
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT sum(speed) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
        vec![sink],
    )
    .with_sources(vec![SourceDefinition::new("stream_eventtime")
        .with_input(SourceInputConfig::on_change_with_columns(["rpm"]))])
    .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 10, "rpm": 1000, "event_ts": 1000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 20, "rpm": 1000, "event_ts": 2000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 30, "rpm": 2000, "event_ts": 3000}),
    )
    .await;

    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 1, "rpm": 3000, "event_ts": 17000}),
    )
    .await;

    let first_window = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        first_window,
        serde_json::json!([{"s": 40}]),
        "on_change gating should suppress unchanged rows before the first eventtime window flush",
    );

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 100, "rpm": 4000, "event_ts": 2000}),
    )
    .await;
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"speed": 2, "rpm": 5000, "event_ts": 27000}),
    )
    .await;

    let second_window = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        second_window,
        serde_json::json!([{"s": 1}]),
        "late rows older than the watermark must be dropped even when source on-change admits them",
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop eventtime pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}

// coverage-covers: pipeline.runtime.eventtime, stream.watermark.propagation, stream.window.tumbling
#[tokio::test]
async fn eventtime_tumbling_window_graceful_stop_flushes_final_window() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_eventtime",
        "eventtime_tumbling_window_graceful_stop_flushes_final_window",
    );
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
    install_memory_json_eventtime_stream(&instance, &input_topic, "stream_eventtime").await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT sum(a) AS s FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
        vec![sink],
    )
    .with_options(eventtime_pipeline_options(7_000));
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create eventtime pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start eventtime pipeline");

    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 7, "event_ts": 7000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 1, "event_ts": 1000}),
    )
    .await;
    publish_json_row(
        &instance,
        &input_topic,
        serde_json::json!({"a": 4, "event_ts": 4000}),
    )
    .await;

    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Graceful,
            Duration::from_secs(5),
        )
        .await
        .expect("gracefully stop eventtime pipeline");

    let final_window = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        final_window,
        serde_json::json!([{"s": 12}]),
        "graceful stop should flush the final buffered eventtime tumbling window",
    );

    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete eventtime pipeline");
}
