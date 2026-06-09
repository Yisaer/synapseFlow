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
