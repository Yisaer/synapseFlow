use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryData, MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{MemorySinkProps, PipelineDefinition};
use flow::{
    CreatePipelineRequest, FlowInstance, FlowInstanceDedicatedRuntimeOptions, PipelineStopMode,
    SinkDefinition, SinkProps, SinkType,
};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn create_memory_json_pipeline(
    instance: &FlowInstance,
    stream_name: &str,
    input_topic: &str,
    output_topic: &str,
    pipeline_id: &str,
) {
    instance
        .declare_memory_topic(
            input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    instance
        .declare_memory_topic(
            output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output topic");

    let schema = Schema::new(vec![ColumnSchema::new(
        stream_name.to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create memory json stream");

    let sql = format!("SELECT a FROM {stream_name}");
    let pipeline = PipelineDefinition::new(
        pipeline_id.to_string(),
        &sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.to_string())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create memory json pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start memory json pipeline");
}

async fn recv_json(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) -> serde_json::Value {
    let got = timeout(timeout_duration, output.recv())
        .await
        .expect("receive output timeout")
        .expect("receive output failed");
    match got {
        MemoryData::Bytes(bytes) => {
            serde_json::from_slice(bytes.as_ref()).expect("decode output bytes as json")
        }
        MemoryData::Collection(_) => panic!("expected bytes payload"),
    }
}

async fn assert_no_output(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) {
    assert!(
        timeout(timeout_duration, output.recv()).await.is_err(),
        "unexpected cross-instance output observed"
    );
}

async fn stop_memory_pipeline(instance: &FlowInstance, pipeline_id: &str) {
    let timeout_duration = Duration::from_secs(5);
    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_pubsub_isolated_across_instances() {
    let instance_a = FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        "instance_a",
        None,
        FlowInstanceDedicatedRuntimeOptions::default(),
    ))
    .expect("create flow instance");
    let shared_registries = instance_a.shared_registries();
    let instance_b = FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        "instance_b",
        Some(shared_registries),
        FlowInstanceDedicatedRuntimeOptions::default(),
    ))
    .expect("create flow instance");

    let topic = "tests.memory_pubsub.isolation.bytes";
    instance_a
        .declare_memory_topic(
            topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare topic in instance_a");
    instance_b
        .declare_memory_topic(
            topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare topic in instance_b");

    let mut sub_a = instance_a
        .open_memory_subscribe_bytes(topic)
        .expect("subscribe in instance_a");
    let mut sub_b = instance_b
        .open_memory_subscribe_bytes(topic)
        .expect("subscribe in instance_b");

    let pub_a = instance_a
        .open_memory_publisher_bytes(topic)
        .expect("open publisher in instance_a");
    pub_a
        .publish_bytes("from_instance_a")
        .expect("publish in a");

    let got_a = timeout(Duration::from_secs(2), sub_a.recv())
        .await
        .expect("receive in a timeout")
        .expect("receive in a failed");
    match got_a {
        MemoryData::Bytes(bytes) => assert_eq!(bytes.as_ref(), b"from_instance_a"),
        MemoryData::Collection(_) => panic!("expected bytes payload in instance_a"),
    }

    assert!(
        timeout(Duration::from_millis(200), sub_b.recv())
            .await
            .is_err(),
        "instance_b unexpectedly received message from instance_a"
    );

    let pub_b = instance_b
        .open_memory_publisher_bytes(topic)
        .expect("open publisher in instance_b");
    pub_b
        .publish_bytes("from_instance_b")
        .expect("publish in b");

    let got_b = timeout(Duration::from_secs(2), sub_b.recv())
        .await
        .expect("receive in b timeout")
        .expect("receive in b failed");
    match got_b {
        MemoryData::Bytes(bytes) => assert_eq!(bytes.as_ref(), b"from_instance_b"),
        MemoryData::Collection(_) => panic!("expected bytes payload in instance_b"),
    }

    assert!(
        timeout(Duration::from_millis(200), sub_a.recv())
            .await
            .is_err(),
        "instance_a unexpectedly received message from instance_b"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_source_and_sink_topics_are_isolated_across_instances() {
    let instance_a = FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        "instance_a",
        None,
        FlowInstanceDedicatedRuntimeOptions::default(),
    ))
    .expect("create flow instance");
    let shared_registries = instance_a.shared_registries();
    let instance_b = FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        "instance_b",
        Some(shared_registries),
        FlowInstanceDedicatedRuntimeOptions::default(),
    ))
    .expect("create flow instance");

    let input_topic = "tests.memory_pipeline.isolation.input";
    let output_topic = "tests.memory_pipeline.isolation.output";
    let stream_name = "memory_stream";
    let pipeline_a_id = "memory_pipe_a";
    let pipeline_b_id = "memory_pipe_b";

    create_memory_json_pipeline(
        &instance_a,
        stream_name,
        input_topic,
        output_topic,
        pipeline_a_id,
    )
    .await;
    create_memory_json_pipeline(
        &instance_b,
        stream_name,
        input_topic,
        output_topic,
        pipeline_b_id,
    )
    .await;

    let mut output_a = instance_a
        .open_memory_subscribe_bytes(output_topic)
        .expect("subscribe output in instance_a");
    let mut output_b = instance_b
        .open_memory_subscribe_bytes(output_topic)
        .expect("subscribe output in instance_b");

    let timeout_duration = Duration::from_secs(5);
    instance_a
        .wait_for_memory_subscribers(input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for instance_a source subscriber");
    instance_b
        .wait_for_memory_subscribers(input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for instance_b source subscriber");

    let publisher_a = instance_a
        .open_memory_publisher_bytes(input_topic)
        .expect("open input publisher in instance_a");
    publisher_a
        .publish_bytes(
            serde_json::to_vec(&serde_json::json!([{ "a": 11 }])).expect("encode instance_a input"),
        )
        .expect("publish instance_a input");

    let got_a = recv_json(&mut output_a, timeout_duration).await;
    assert_eq!(got_a, serde_json::json!([{ "a": 11 }]));
    assert_no_output(&mut output_b, Duration::from_millis(200)).await;

    let publisher_b = instance_b
        .open_memory_publisher_bytes(input_topic)
        .expect("open input publisher in instance_b");
    publisher_b
        .publish_bytes(
            serde_json::to_vec(&serde_json::json!([{ "a": 22 }])).expect("encode instance_b input"),
        )
        .expect("publish instance_b input");

    let got_b = recv_json(&mut output_b, timeout_duration).await;
    assert_eq!(got_b, serde_json::json!([{ "a": 22 }]));
    assert_no_output(&mut output_a, Duration::from_millis(200)).await;

    stop_memory_pipeline(&instance_a, pipeline_a_id).await;
    stop_memory_pipeline(&instance_b, pipeline_b_id).await;
}
