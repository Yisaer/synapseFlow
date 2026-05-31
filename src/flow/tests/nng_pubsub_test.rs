#![cfg(feature = "nng_pubsub")]

use anng::{protocols::pubsub0, Message};
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{NngPubSubStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{MemorySinkProps, NngPubSubSinkProps, PipelineDefinition};
use flow::{
    CreatePipelineRequest, FlowInstance, PipelineStopMode, SinkDefinition, SinkProps, SinkType,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

fn test_instance() -> FlowInstance {
    FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance")
}

fn json_schema(source_name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![ColumnSchema::new(
        source_name.to_string(),
        "v".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]))
}

// coverage-covers: source.nng_pubsub.bytes_input
#[tokio::test]
async fn nng_pubsub_source_feeds_memory_sink_pipeline() {
    let instance = test_instance();
    let source_name = "nng_stream";
    let output_topic = format!("tests.nng.source.output.{}", uuid::Uuid::new_v4());
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output topic");

    let url = format!("inproc://vf-nng-pipeline-source-{}", uuid::Uuid::new_v4());
    let c_url = std::ffi::CString::new(url.clone()).expect("valid url");
    let mut publisher = pubsub0::Pub0::listen(c_url.as_c_str())
        .await
        .expect("start nng publisher");

    let stream = StreamDefinition::new(
        source_name,
        json_schema(source_name),
        StreamProps::NngPubSub(NngPubSubStreamProps::new(url, "topic/can")),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create nng stream");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output");
    let pipeline_id = "nng_source_to_memory";
    let pipeline = PipelineDefinition::new(
        pipeline_id,
        format!("SELECT v FROM {source_name}"),
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .expect("start pipeline");

    sleep(Duration::from_millis(150)).await;
    publisher
        .publish(Message::from(&b"topic/can:{\"v\":42}"[..]))
        .await
        .expect("publish nng frame");

    let got = timeout(Duration::from_secs(3), output.recv())
        .await
        .expect("output timeout")
        .expect("receive output");
    let flow::connector::MemoryData::Bytes(bytes) = got else {
        panic!("expected bytes output");
    };
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(bytes.as_ref()).expect("decode output json"),
        json!([{ "v": 42 }])
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(3))
        .await
        .expect("stop pipeline");
}

// coverage-covers: sink.connector.nng_pubsub_output
#[tokio::test]
async fn memory_source_feeds_nng_pubsub_sink_pipeline() {
    let instance = test_instance();
    let source_name = "memory_stream";
    let input_topic = format!("tests.nng.sink.input.{}", uuid::Uuid::new_v4());
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    let stream = StreamDefinition::new(
        source_name,
        json_schema(source_name),
        StreamProps::Memory(flow::catalog::MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create memory stream");

    let url = format!("inproc://vf-nng-pipeline-sink-{}", uuid::Uuid::new_v4());
    let c_url = std::ffi::CString::new(url.clone()).expect("valid url");
    let subscriber = pubsub0::Sub0::listen(c_url.as_c_str())
        .await
        .expect("start nng subscriber");
    let mut sub = subscriber.context();
    sub.subscribe_to(b"topic/can");

    let pipeline_id = "memory_to_nng_sink";
    let pipeline = PipelineDefinition::new(
        pipeline_id,
        format!("SELECT v FROM {source_name}"),
        vec![SinkDefinition::new(
            "nng_sink",
            SinkType::NngPubSub,
            SinkProps::NngPubSub(NngPubSubSinkProps::new(url, "topic/can")),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            Duration::from_secs(3),
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    publisher
        .publish_bytes(bytes::Bytes::from_static(br#"[{"v":7}]"#))
        .expect("publish memory input");

    let message = timeout(Duration::from_secs(3), sub.next())
        .await
        .expect("nng output timeout")
        .expect("receive nng output");
    assert_eq!(message.as_slice(), b"topic/can:[{\"v\":7}]");

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, Duration::from_secs(3))
        .await
        .expect("stop pipeline");
}
