//! Integration tests for pipeline stats behavior.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::catalog::{
    MemoryStreamProps, MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::model::batch_from_columns_simple;
use flow::pipeline::MemorySinkProps;
use flow::planner::sink::SinkOutputConfig;
use flow::FlowInstance;
use flow::{
    CreatePipelineRequest, NopSinkProps, PipelineDefinition, PipelineError, PipelineStopMode,
    SinkDefinition, SinkEncoderConfig, SinkProps, SinkType,
};
use serde_json::json;
use std::sync::Arc;
use tokio::time::Duration;

use super::common::{
    assert_no_json_output, declare_memory_input_output_topics, install_memory_stream_schema,
    make_memory_topics, publish_input_collection, recv_next_json,
};

async fn create_mock_json_stream(instance: &FlowInstance, stream_name: &str) {
    let schema = Schema::new(vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create mock stream");
}

fn build_nop_pipeline(pipeline_id: &str, sql: &str) -> PipelineDefinition {
    PipelineDefinition::new(
        pipeline_id.to_string(),
        sql,
        vec![SinkDefinition::new(
            "nop_sink",
            SinkType::Nop,
            SinkProps::Nop(NopSinkProps::default()),
        )],
    )
}

async fn create_memory_json_stream(instance: &FlowInstance, topic: &str, stream_name: &str) {
    let schema = Schema::new(vec![ColumnSchema::new(
        stream_name.to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(topic.to_string())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create memory json stream");
}

fn find_processor_stats<'a>(
    stats: &'a [flow::processor::ProcessorStatsEntry],
    processor_fragment: &str,
) -> &'a flow::processor::ProcessorStatsEntry {
    stats
        .iter()
        .find(|entry| entry.processor_id.contains(processor_fragment))
        .unwrap_or_else(|| panic!("missing processor stats for fragment {processor_fragment}"))
}

// coverage-covers: pipeline.runtime.stats
#[tokio::test]
async fn collect_pipeline_stats_returns_base_fields_for_running_pipeline() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    create_mock_json_stream(&instance, "stats_stream").await;

    let pipeline_id = "stats_running_pipeline";
    let pipeline = build_nop_pipeline(pipeline_id, "SELECT x FROM stats_stream");
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline");

    let stats = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats for running pipeline");
    assert!(
        !stats.is_empty(),
        "running pipeline should expose at least one processor stats entry"
    );
    assert!(
        stats
            .iter()
            .all(|entry| entry.stats.error_count == 0 && entry.stats.last_error.is_none()),
        "fresh running pipeline should not report processor errors"
    );
    assert!(
        stats.iter().all(|entry| {
            entry.stats.records_in == 0
                && entry.stats.records_out == 0
                && entry.stats.custom.is_empty()
        }),
        "idle pipeline should expose zeroed base stats without custom metrics"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: pipeline.runtime.stats
#[tokio::test]
async fn collect_pipeline_stats_returns_error_for_stopped_pipeline() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    create_mock_json_stream(&instance, "stats_stopped_stream").await;

    let pipeline_id = "stats_stopped_pipeline";
    let pipeline = build_nop_pipeline(pipeline_id, "SELECT x FROM stats_stopped_stream");
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline");
    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");

    let err = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect_err("stopped pipeline should not return stats");
    match err {
        PipelineError::Runtime(message) => {
            assert!(
                message.contains("is not running"),
                "unexpected stopped-pipeline stats error: {message}"
            );
        }
        other => panic!("unexpected stats error for stopped pipeline: {other:?}"),
    }

    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: pipeline.runtime.stats
#[tokio::test]
async fn collect_pipeline_stats_reset_after_runtime_rebuild() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_stats",
        "collect_pipeline_stats_reset_after_runtime_rebuild",
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);

    let schema_hint = vec![(
        "x".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )];
    install_memory_stream_schema(&instance, &input_topic, &schema_hint).await;

    let pipeline_id = "stats_rebuild_pipeline";
    let pipeline = PipelineDefinition::new(
        pipeline_id.to_string(),
        "SELECT x FROM stream",
        vec![SinkDefinition::new(
            "memory_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline before stats collection");

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "x".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )])
    .expect("build input batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(batch),
        Duration::from_secs(5),
    )
    .await;

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(actual, json!([{"x": 1}, {"x": 2}, {"x": 3}]));

    let stats_before = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats before rebuild");
    assert!(
        stats_before
            .iter()
            .any(|entry| entry.stats.records_in > 0 || entry.stats.records_out > 0),
        "processed pipeline should expose non-zero counters before rebuild: {stats_before:#?}"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline before rebuild");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("restart pipeline to rebuild runtime");

    let stats_after = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats after rebuild");
    assert!(
        !stats_after.is_empty(),
        "rebuilt pipeline should still expose processor stats entries"
    );
    assert!(
        stats_after.iter().all(|entry| {
            entry.stats.records_in == 0
                && entry.stats.records_out == 0
                && entry.stats.error_count == 0
                && entry.stats.last_error.is_none()
                && entry.stats.custom.is_empty()
        }),
        "runtime rebuild should reset in-memory stats counters: {stats_after:#?}"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop rebuilt pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: pipeline.runtime.stats
#[tokio::test]
async fn collect_pipeline_stats_exposes_empty_suppress_custom_metrics() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_stats",
        "collect_pipeline_stats_exposes_empty_suppress_custom_metrics",
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);

    let schema_hint = vec![("a".to_string(), vec![Value::Int64(1)])];
    install_memory_stream_schema(&instance, &input_topic, &schema_hint).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = "stats_empty_suppress_pipeline";
    let pipeline = PipelineDefinition::new(
        pipeline_id.to_string(),
        "SELECT a FROM stream",
        vec![SinkDefinition::new(
            "memory_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_encoder(SinkEncoderConfig::json())
        .with_output(SinkOutputConfig::delta().with_omit_if_empty(true))],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline before stats collection");

    let first_batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1)],
    )])
    .expect("build first input batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(first_batch),
        Duration::from_secs(5),
    )
    .await;
    let first_output = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(first_output, json!([{"a": 1}]));

    let repeated_batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1)],
    )])
    .expect("build repeated input batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(repeated_batch),
        Duration::from_secs(5),
    )
    .await;
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    let stats = instance
        .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
        .await
        .expect("collect stats for omit_if_empty pipeline");
    let empty_suppress = find_processor_stats(&stats, "PhysicalEmptySuppress");
    assert_eq!(
        empty_suppress.stats.custom.get("collections_in"),
        Some(&2),
        "empty suppress should count both input collections: {empty_suppress:#?}"
    );
    assert_eq!(
        empty_suppress.stats.custom.get("collections_forwarded"),
        Some(&1),
        "empty suppress should forward only the first collection: {empty_suppress:#?}"
    );
    assert_eq!(
        empty_suppress.stats.custom.get("collections_suppressed"),
        Some(&1),
        "empty suppress should report one suppressed collection: {empty_suppress:#?}"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: pipeline.runtime.stats
#[tokio::test]
async fn collect_pipeline_stats_reports_decoder_errors_without_failing_collection() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_stats",
        "collect_pipeline_stats_reports_decoder_errors_without_failing_collection",
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
    create_memory_json_stream(&instance, &input_topic, "stream_stats_json").await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = "stats_decoder_error_pipeline";
    let pipeline = PipelineDefinition::new(
        pipeline_id.to_string(),
        "SELECT a FROM stream_stats_json",
        vec![SinkDefinition::new(
            "memory_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline before stats collection");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            Duration::from_secs(5),
        )
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open bytes publisher");
    publisher
        .publish_bytes(b"{invalid-json".as_ref())
        .expect("publish invalid json");
    publisher
        .publish_bytes(br#"{"a":42}"#.as_ref())
        .expect("publish valid json");

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        actual,
        json!([{"a": 42}]),
        "decoder error should not stop later valid rows from reaching the sink",
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let stats = loop {
        let stats = instance
            .collect_pipeline_stats(pipeline_id, Duration::from_secs(1))
            .await
            .expect("collect stats after decoder error");
        let decoder = find_processor_stats(&stats, "PhysicalDecoder");
        if decoder.stats.error_count > 0 {
            break stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "decoder stats did not report the recorded error before timeout: {stats:#?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let decoder = find_processor_stats(&stats, "PhysicalDecoder");
    assert!(
        decoder.stats.error_count > 0,
        "decoder stats should expose processor-local errors: {decoder:#?}"
    );
    assert!(
        decoder
            .stats
            .last_error
            .as_deref()
            .is_some_and(|message| !message.is_empty()),
        "decoder stats should preserve the last error message: {decoder:#?}"
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}
