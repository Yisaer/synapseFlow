//! Integration tests for shared stream dynamic decode lifecycle behavior.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryData, MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{MemorySinkProps, PipelineDefinition, PipelineOptions};
use flow::processor::SamplerConfig;
use flow::FlowInstance;
use flow::SharedStreamStatus;
use flow::{
    CreatePipelineRequest, ExplainPipelineTarget, PipelineStopMode, SinkDefinition, SinkProps,
    SinkType,
};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use super::common::{make_memory_topics, recv_next_json};

async fn install_shared_mock_json_stream(instance: &FlowInstance, stream_name: &str) {
    let schema = Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "c".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, true)
        .await
        .expect("create shared mock stream");
}

async fn install_shared_mock_json_stream_with_sampler(
    instance: &FlowInstance,
    stream_name: &str,
    interval: Duration,
) {
    let schema = Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "c".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    )
    .with_sampler(SamplerConfig::new(interval));
    instance
        .create_stream(definition, true)
        .await
        .expect("create shared mock stream with sampler");
}

fn declare_output_topic(instance: &FlowInstance, output_topic: &str) {
    instance
        .declare_memory_topic(
            output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
}

fn create_memory_sink_pipeline(
    instance: &FlowInstance,
    pipeline_id: &str,
    sql: &str,
    output_topic: &str,
) {
    create_memory_sink_pipeline_with_options(
        instance,
        pipeline_id,
        sql,
        output_topic,
        PipelineOptions::default(),
    );
}

fn create_memory_sink_pipeline_with_options(
    instance: &FlowInstance,
    pipeline_id: &str,
    sql: &str,
    output_topic: &str,
    options: PipelineOptions,
) {
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.to_string())),
    );
    let pipeline =
        PipelineDefinition::new(pipeline_id.to_string(), sql, vec![sink]).with_options(options);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
}

fn shared_slice_pipeline_options() -> PipelineOptions {
    PipelineOptions {
        shared_slice_projection_enabled: Some(true),
        ..PipelineOptions::default()
    }
}

async fn recv_until_json(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    expected: &JsonValue,
    timeout_duration: Duration,
    label: &str,
) {
    let deadline = Instant::now() + timeout_duration;
    let mut last_seen = None;

    loop {
        let now = Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for {label}; expected={expected} last_seen={last_seen:?}"
        );

        let actual = recv_next_json(output, deadline.saturating_duration_since(now)).await;
        if &actual == expected {
            return;
        }
        last_seen = Some(actual);
    }
}

async fn wait_for_shared_stream_subscriber_count(
    instance: &FlowInstance,
    stream_name: &str,
    expected_subscriber_count: usize,
    timeout_duration: Duration,
) {
    let deadline = Instant::now() + timeout_duration;
    loop {
        let info = instance
            .get_stream(stream_name)
            .await
            .expect("get shared stream")
            .shared_info
            .expect("shared stream info");
        if info.subscriber_count == expected_subscriber_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "shared stream subscriber_count did not converge before timeout: expected={} actual={} decoding_columns={:?}",
            expected_subscriber_count,
            info.subscriber_count,
            info.decoding_columns
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn shared_stream_status_name(status: &SharedStreamStatus) -> &'static str {
    match status {
        SharedStreamStatus::Starting => "starting",
        SharedStreamStatus::Running => "running",
        SharedStreamStatus::Stopped => "stopped",
        SharedStreamStatus::Failed(_) => "failed",
    }
}

async fn wait_for_shared_stream_status(
    instance: &FlowInstance,
    stream_name: &str,
    expected_status: &str,
    timeout_duration: Duration,
) {
    let deadline = Instant::now() + timeout_duration;
    loop {
        let info = instance
            .get_stream(stream_name)
            .await
            .expect("get shared stream")
            .shared_info
            .expect("shared stream info");
        let actual_status = shared_stream_status_name(&info.status);
        if actual_status == expected_status {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "shared stream status did not converge before timeout: expected={} actual={} subscribers={} decoding_columns={:?}",
            expected_status,
            actual_status,
            info.subscriber_count,
            info.decoding_columns
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_shared_stream_decoding_columns(
    instance: &FlowInstance,
    stream_name: &str,
    expected_subscriber_count: usize,
    expected_decoding_columns: &[&str],
    timeout_duration: Duration,
) {
    let expected_decoding_columns = expected_decoding_columns
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    let deadline = Instant::now() + timeout_duration;
    loop {
        let info = instance
            .get_stream(stream_name)
            .await
            .expect("get shared stream")
            .shared_info
            .expect("shared stream info");
        let actual_decoding_columns = info
            .decoding_columns
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if info.subscriber_count == expected_subscriber_count
            && actual_decoding_columns == expected_decoding_columns
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "shared stream decode state did not converge before timeout: expected_subscribers={} actual_subscribers={} expected_decoding_columns={expected_decoding_columns:?} actual_decoding_columns={:?}",
            expected_subscriber_count,
            info.subscriber_count,
            info.decoding_columns
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn stop_and_delete_pipeline(instance: &FlowInstance, pipeline_id: &str) {
    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(5))
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: source.shared.dynamic_decode
#[tokio::test]
async fn shared_stream_new_consumer_waits_for_required_columns_before_first_tuple() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_readiness";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_new_consumer_waits_for_required_columns_before_first_tuple_a",
    );
    let (_, output_topic_c) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_new_consumer_waits_for_required_columns_before_first_tuple_c",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_c);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_c = instance
        .open_memory_subscribe_bytes(&output_topic_c)
        .expect("subscribe pipeline c output");

    let pipeline_a_id = "shared_stream_readiness_pipeline_a";
    let pipeline_c_id = "shared_stream_readiness_pipeline_c";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_c_id,
        &format!("SELECT c FROM {stream_name}"),
        &output_topic_c,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .await
        .expect("start pipeline a");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .start_pipeline(pipeline_c_id)
        .await
        .expect("start pipeline c");
    wait_for_shared_stream_subscriber_count(&instance, stream_name, 2, Duration::from_secs(5))
        .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":2,"c":3}"#.as_ref())
        .await
        .expect("send shared mock payload");

    let actual_a = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    let actual_c = recv_next_json(&mut output_c, Duration::from_secs(5)).await;
    assert_eq!(actual_a, json!([{"a": 1}]));
    assert_eq!(
        actual_c,
        json!([{"c": 3}]),
        "new consumer should not observe initial NULL values before required columns are applied",
    );

    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "c"],
        Duration::from_secs(5),
    )
    .await;

    stop_and_delete_pipeline(&instance, pipeline_c_id).await;
    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: source.shared.dynamic_decode
#[tokio::test]
async fn shared_stream_required_columns_shrink_after_consumer_stop() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_shrink";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_required_columns_shrink_after_consumer_stop_a",
    );
    let (_, output_topic_b) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_required_columns_shrink_after_consumer_stop_b",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_b);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_b = instance
        .open_memory_subscribe_bytes(&output_topic_b)
        .expect("subscribe pipeline b output");

    let pipeline_a_id = "shared_stream_shrink_pipeline_a";
    let pipeline_b_id = "shared_stream_shrink_pipeline_b";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_b_id,
        &format!("SELECT b FROM {stream_name}"),
        &output_topic_b,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .await
        .expect("start pipeline a");
    instance
        .start_pipeline(pipeline_b_id)
        .await
        .expect("start pipeline b");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "b"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":10,"b":20,"c":30}"#.as_ref())
        .await
        .expect("send shared mock payload");

    let actual_a = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    let actual_b = recv_next_json(&mut output_b, Duration::from_secs(5)).await;
    assert_eq!(actual_a, json!([{"a": 10}]));
    assert_eq!(actual_b, json!([{"b": 20}]));

    instance
        .stop_pipeline(
            pipeline_b_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop pipeline b");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;
    instance
        .delete_pipeline(pipeline_b_id)
        .await
        .expect("delete pipeline b");

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":11,"b":21,"c":31}"#.as_ref())
        .await
        .expect("send post-shrink shared mock payload");

    let actual_a_after_shrink = recv_next_json(&mut output_a, Duration::from_secs(5)).await;
    assert_eq!(actual_a_after_shrink, json!([{"a": 11}]));

    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: source.shared.dynamic_decode, source.shared.lifecycle
#[tokio::test]
async fn shared_stream_decode_projection_survives_consumer_lifecycle_changes() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_decode_lifecycle";
    install_shared_mock_json_stream(&instance, stream_name).await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_decode_projection_survives_consumer_lifecycle_changes_a",
    );
    let (_, output_topic_b) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_decode_projection_survives_consumer_lifecycle_changes_b",
    );
    let (_, output_topic_c) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_decode_projection_survives_consumer_lifecycle_changes_c",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_b);
    declare_output_topic(&instance, &output_topic_c);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_b = instance
        .open_memory_subscribe_bytes(&output_topic_b)
        .expect("subscribe pipeline b output");

    let pipeline_a_id = "shared_stream_decode_lifecycle_pipeline_a";
    let pipeline_b_id = "shared_stream_decode_lifecycle_pipeline_b";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_b_id,
        &format!("SELECT b FROM {stream_name}"),
        &output_topic_b,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .await
        .expect("start pipeline a");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .start_pipeline(pipeline_b_id)
        .await
        .expect("start pipeline b");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "b"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":2,"c":3}"#.as_ref())
        .await
        .expect("send shared mock payload");
    assert_eq!(
        recv_next_json(&mut output_a, Duration::from_secs(5)).await,
        json!([{"a": 1}])
    );
    assert_eq!(
        recv_next_json(&mut output_b, Duration::from_secs(5)).await,
        json!([{"b": 2}])
    );

    stop_and_delete_pipeline(&instance, pipeline_b_id).await;
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;
    wait_for_shared_stream_subscriber_count(&instance, stream_name, 0, Duration::from_secs(5))
        .await;

    let mut output_c = instance
        .open_memory_subscribe_bytes(&output_topic_c)
        .expect("subscribe pipeline c output");
    let pipeline_c_id = "shared_stream_decode_lifecycle_pipeline_c";
    create_memory_sink_pipeline(
        &instance,
        pipeline_c_id,
        &format!("SELECT c FROM {stream_name}"),
        &output_topic_c,
    );
    instance
        .start_pipeline(pipeline_c_id)
        .await
        .expect("start pipeline c");
    wait_for_shared_stream_status(&instance, stream_name, "running", Duration::from_secs(5)).await;
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["c"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":4,"b":5,"c":6}"#.as_ref())
        .await
        .expect("send shared mock payload after restart");
    assert_eq!(
        recv_next_json(&mut output_c, Duration::from_secs(5)).await,
        json!([{"c": 6}])
    );

    stop_and_delete_pipeline(&instance, pipeline_c_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: source.shared.dynamic_decode
#[tokio::test]
async fn shared_stream_slot_schema_wildcard_expands_and_detach_reclaims_tail() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_slot_schema_wildcard";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_c) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_slot_schema_wildcard_expands_and_detach_keeps_slots_c",
    );
    let (_, output_topic_all) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_slot_schema_wildcard_expands_and_detach_keeps_slots_all",
    );
    declare_output_topic(&instance, &output_topic_c);
    declare_output_topic(&instance, &output_topic_all);

    let mut output_c = instance
        .open_memory_subscribe_bytes(&output_topic_c)
        .expect("subscribe pipeline c output");
    let mut output_all = instance
        .open_memory_subscribe_bytes(&output_topic_all)
        .expect("subscribe wildcard output");

    let pipeline_c_id = "shared_stream_slot_schema_wildcard_pipeline_c";
    let pipeline_all_id = "shared_stream_slot_schema_wildcard_pipeline_all";
    create_memory_sink_pipeline_with_options(
        &instance,
        pipeline_c_id,
        &format!("SELECT c FROM {stream_name}"),
        &output_topic_c,
        shared_slice_pipeline_options(),
    );

    instance
        .start_pipeline(pipeline_c_id)
        .await
        .expect("start pipeline c");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["c"],
        Duration::from_secs(5),
    )
    .await;

    create_memory_sink_pipeline_with_options(
        &instance,
        pipeline_all_id,
        &format!("SELECT * FROM {stream_name}"),
        &output_topic_all,
        shared_slice_pipeline_options(),
    );
    let explain = instance
        .explain_pipeline(ExplainPipelineTarget::Id(pipeline_all_id))
        .expect("explain wildcard pipeline")
        .to_pretty_string();
    assert!(
        explain.contains("schema=[c, a, b]"),
        "SELECT * should plan against slot order after a prior narrow slot:\n{explain}"
    );
    instance
        .start_pipeline(pipeline_all_id)
        .await
        .expect("start wildcard pipeline");
    // The parse whitelist (decoding_columns) is the union of consumers' columns, in
    // schema order — independent of the slot schema, which is slot-ordered [c,a,b].
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "b", "c"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":2,"c":3}"#.as_ref())
        .await
        .expect("send wildcard-phase shared mock payload");

    let actual_c = recv_next_json(&mut output_c, Duration::from_secs(5)).await;
    let actual_all = recv_next_json(&mut output_all, Duration::from_secs(5)).await;
    assert_eq!(actual_c, json!([{"c": 3}]));
    assert_eq!(actual_all, json!([{"c": 3, "a": 1, "b": 2}]));

    instance
        .stop_pipeline(
            pipeline_all_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop wildcard pipeline");
    // Detaching the wildcard consumer releases a and b. Both are unused now and sit
    // at the tail (slots c@0, a@1, b@2), so they are reclaimed; the whitelist
    // narrows back to {c}. The surviving narrow consumer's c@0 slot is untouched.
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["c"],
        Duration::from_secs(5),
    )
    .await;
    instance
        .delete_pipeline(pipeline_all_id)
        .await
        .expect("delete wildcard pipeline");

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":4,"b":5,"c":6}"#.as_ref())
        .await
        .expect("send post-wildcard shared mock payload");

    let actual_c_after_detach = recv_next_json(&mut output_c, Duration::from_secs(5)).await;
    assert_eq!(actual_c_after_detach, json!([{"c": 6}]));

    stop_and_delete_pipeline(&instance, pipeline_c_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: source.shared.dynamic_decode
#[tokio::test]
async fn shared_stream_slot_schema_full_consumer_first_keeps_later_narrow_reads_correct() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_slot_schema_full_first";
    install_shared_mock_json_stream(&instance, stream_name).await;

    let (_, output_topic_all) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_slot_schema_full_consumer_first_keeps_later_narrow_reads_correct_all",
    );
    let (_, output_topic_b) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_slot_schema_full_consumer_first_keeps_later_narrow_reads_correct_b",
    );
    declare_output_topic(&instance, &output_topic_all);
    declare_output_topic(&instance, &output_topic_b);

    let mut output_all = instance
        .open_memory_subscribe_bytes(&output_topic_all)
        .expect("subscribe wildcard output");
    let mut output_b = instance
        .open_memory_subscribe_bytes(&output_topic_b)
        .expect("subscribe pipeline b output");

    let pipeline_all_id = "shared_stream_slot_schema_full_first_pipeline_all";
    let pipeline_b_id = "shared_stream_slot_schema_full_first_pipeline_b";
    create_memory_sink_pipeline_with_options(
        &instance,
        pipeline_all_id,
        &format!("SELECT * FROM {stream_name}"),
        &output_topic_all,
        shared_slice_pipeline_options(),
    );
    create_memory_sink_pipeline_with_options(
        &instance,
        pipeline_b_id,
        &format!("SELECT b FROM {stream_name}"),
        &output_topic_b,
        shared_slice_pipeline_options(),
    );

    instance
        .start_pipeline(pipeline_all_id)
        .await
        .expect("start wildcard pipeline");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a", "b", "c"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .start_pipeline(pipeline_b_id)
        .await
        .expect("start pipeline b");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "b", "c"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":7,"b":8,"c":9}"#.as_ref())
        .await
        .expect("send full-first shared mock payload");

    assert_eq!(
        recv_next_json(&mut output_all, Duration::from_secs(5)).await,
        json!([{"a": 7, "b": 8, "c": 9}])
    );
    assert_eq!(
        recv_next_json(&mut output_b, Duration::from_secs(5)).await,
        json!([{"b": 8}]),
        "later narrow consumer should read from the already-expanded slot schema",
    );

    stop_and_delete_pipeline(&instance, pipeline_b_id).await;
    stop_and_delete_pipeline(&instance, pipeline_all_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: source.shared.lifecycle
#[tokio::test]
async fn shared_stream_runtime_is_lazy_started_and_reclaimed_when_last_consumer_leaves() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_lifecycle";
    install_shared_mock_json_stream(&instance, stream_name).await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;

    let (_, output_topic) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_runtime_is_lazy_started_and_reclaimed_when_last_consumer_leaves",
    );
    declare_output_topic(&instance, &output_topic);

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe shared stream output");
    let pipeline_id = "shared_stream_lifecycle_pipeline";
    create_memory_sink_pipeline(
        &instance,
        pipeline_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic,
    );

    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline");
    wait_for_shared_stream_status(&instance, stream_name, "running", Duration::from_secs(5)).await;
    wait_for_shared_stream_subscriber_count(&instance, stream_name, 1, Duration::from_secs(5))
        .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":7,"b":8,"c":9}"#.as_ref())
        .await
        .expect("send running shared stream payload");
    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(actual, json!([{"a": 7}]));

    stop_and_delete_pipeline(&instance, pipeline_id).await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;
    wait_for_shared_stream_subscriber_count(&instance, stream_name, 0, Duration::from_secs(5))
        .await;

    let err = instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":10,"b":11,"c":12}"#.as_ref())
        .await
        .expect_err("stopped shared stream runtime should reject mock payload injection");
    let message = err.to_string();
    assert!(
        message.contains("not ready to receive payloads") || message.contains("mock source closed"),
        "unexpected stopped shared stream send error: {message}"
    );

    let info = instance
        .get_stream(stream_name)
        .await
        .expect("get shared stream after runtime reclaim");
    assert!(
        info.shared_info.is_some(),
        "shared stream definition should still exist after runtime reclaim"
    );

    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: source.shared.lifecycle
#[tokio::test]
async fn shared_stream_runtime_can_restart_after_all_consumers_released() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_restart";
    install_shared_mock_json_stream(&instance, stream_name).await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;

    let (_, output_topic_first) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_runtime_can_restart_after_all_consumers_released_first",
    );
    let (_, output_topic_second) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_runtime_can_restart_after_all_consumers_released_second",
    );
    declare_output_topic(&instance, &output_topic_first);
    declare_output_topic(&instance, &output_topic_second);

    let pipeline_first = "shared_stream_restart_pipeline_first";
    create_memory_sink_pipeline(
        &instance,
        pipeline_first,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_first,
    );
    instance
        .start_pipeline(pipeline_first)
        .await
        .expect("start first pipeline");
    wait_for_shared_stream_status(&instance, stream_name, "running", Duration::from_secs(5)).await;

    stop_and_delete_pipeline(&instance, pipeline_first).await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;

    let mut output_second = instance
        .open_memory_subscribe_bytes(&output_topic_second)
        .expect("subscribe second output");
    let pipeline_second = "shared_stream_restart_pipeline_second";
    create_memory_sink_pipeline(
        &instance,
        pipeline_second,
        &format!("SELECT c FROM {stream_name}"),
        &output_topic_second,
    );
    instance
        .start_pipeline(pipeline_second)
        .await
        .expect("start second pipeline");
    wait_for_shared_stream_status(&instance, stream_name, "running", Duration::from_secs(5)).await;
    wait_for_shared_stream_subscriber_count(&instance, stream_name, 1, Duration::from_secs(5))
        .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":2,"c":13}"#.as_ref())
        .await
        .expect("send payload after shared stream restart");
    let actual = recv_next_json(&mut output_second, Duration::from_secs(5)).await;
    assert_eq!(actual, json!([{"c": 13}]));

    stop_and_delete_pipeline(&instance, pipeline_second).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}

// coverage-covers: processor.sampler.strategy, source.shared.dynamic_decode, source.shared.lifecycle
#[tokio::test]
async fn shared_stream_sampler_dynamic_decode_lifecycle_keeps_outputs_in_sync() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let stream_name = "shared_stream_sampler_dynamic_decode";
    install_shared_mock_json_stream_with_sampler(
        &instance,
        stream_name,
        Duration::from_millis(200),
    )
    .await;
    wait_for_shared_stream_status(&instance, stream_name, "stopped", Duration::from_secs(5)).await;

    let (_, output_topic_a) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_sampler_dynamic_decode_lifecycle_keeps_outputs_in_sync_a",
    );
    let (_, output_topic_c) = make_memory_topics(
        "pipeline_shared_stream",
        "shared_stream_sampler_dynamic_decode_lifecycle_keeps_outputs_in_sync_c",
    );
    declare_output_topic(&instance, &output_topic_a);
    declare_output_topic(&instance, &output_topic_c);

    let mut output_a = instance
        .open_memory_subscribe_bytes(&output_topic_a)
        .expect("subscribe pipeline a output");
    let mut output_c = instance
        .open_memory_subscribe_bytes(&output_topic_c)
        .expect("subscribe pipeline c output");

    let pipeline_a_id = "shared_stream_sampler_pipeline_a";
    let pipeline_c_id = "shared_stream_sampler_pipeline_c";
    create_memory_sink_pipeline(
        &instance,
        pipeline_a_id,
        &format!("SELECT a FROM {stream_name}"),
        &output_topic_a,
    );
    create_memory_sink_pipeline(
        &instance,
        pipeline_c_id,
        &format!("SELECT c FROM {stream_name}"),
        &output_topic_c,
    );

    instance
        .start_pipeline(pipeline_a_id)
        .await
        .expect("start pipeline a");
    wait_for_shared_stream_status(&instance, stream_name, "running", Duration::from_secs(5)).await;
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":1,"b":10,"c":100}"#.as_ref())
        .await
        .expect("send first sampler payload");
    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":2,"b":20,"c":200}"#.as_ref())
        .await
        .expect("send second sampler payload");
    recv_until_json(
        &mut output_a,
        &json!([{"a": 2}]),
        Duration::from_secs(5),
        "latest sampled a output before dynamic decode expands",
    )
    .await;

    instance
        .start_pipeline(pipeline_c_id)
        .await
        .expect("start pipeline c");
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        2,
        &["a", "c"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":3,"b":30,"c":300}"#.as_ref())
        .await
        .expect("send third sampler payload");
    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":4,"b":40,"c":400}"#.as_ref())
        .await
        .expect("send fourth sampler payload");
    recv_until_json(
        &mut output_a,
        &json!([{"a": 4}]),
        Duration::from_secs(5),
        "latest sampled a output after dynamic decode expands",
    )
    .await;
    recv_until_json(
        &mut output_c,
        &json!([{"c": 400}]),
        Duration::from_secs(5),
        "latest sampled c output after dynamic decode expands",
    )
    .await;

    stop_and_delete_pipeline(&instance, pipeline_c_id).await;
    wait_for_shared_stream_decoding_columns(
        &instance,
        stream_name,
        1,
        &["a"],
        Duration::from_secs(5),
    )
    .await;

    instance
        .send_shared_mock_stream_payload(stream_name, br#"{"a":5,"b":50,"c":500}"#.as_ref())
        .await
        .expect("send post-detach sampler payload");
    recv_until_json(
        &mut output_a,
        &json!([{"a": 5}]),
        Duration::from_secs(5),
        "latest sampled a output after pipeline c detaches",
    )
    .await;

    stop_and_delete_pipeline(&instance, pipeline_a_id).await;
    instance
        .delete_stream(stream_name)
        .await
        .expect("delete shared stream");
}
