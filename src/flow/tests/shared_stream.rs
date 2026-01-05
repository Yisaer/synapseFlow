use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{Catalog, MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::JsonDecoder;
use flow::connector::{MockSourceConnector, MqttClientManager};
use flow::processor::StreamData;
use flow::{shared_stream_registry, PipelineRegistries, SharedStreamConfig};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

async fn recv_next_json(
    output: &mut tokio::sync::mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::EncodedBytes { payload, .. } => {
                return serde_json::from_slice(&payload).expect("invalid JSON payload")
            }
            StreamData::Control(_) => continue,
            StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            other => panic!("unexpected stream data: {}", other.description()),
        }
    }
}

#[tokio::test]
async fn shared_stream_two_pipelines_project_different_columns() {
    let stream_name = format!("shared_stream_it_{}", Uuid::new_v4().simple());
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            stream_name.clone(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.clone(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.clone(),
            "c".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));

    let catalog = Arc::new(Catalog::new());
    catalog.upsert(StreamDefinition::new(
        stream_name.clone(),
        Arc::clone(&schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    ));

    let registry = shared_stream_registry();
    if registry.is_registered(&stream_name).await {
        registry
            .drop_stream(&stream_name)
            .await
            .expect("drop existing shared stream");
    }

    let (connector, handle) = MockSourceConnector::new(format!("{stream_name}_connector"));
    let decoder = Arc::new(JsonDecoder::new(
        stream_name.clone(),
        Arc::clone(&schema),
        JsonMap::new(),
    ));
    let config = SharedStreamConfig::new(stream_name.clone(), Arc::clone(&schema))
        .with_connector(Box::new(connector), decoder);
    registry
        .create_stream(config)
        .await
        .expect("create shared stream");

    let mqtt_manager = MqttClientManager::new();
    let registries = PipelineRegistries::new_with_builtin();

    let mut pipeline_ab = flow::create_pipeline_with_log_sink(
        &format!("SELECT a, b FROM {stream_name}"),
        true,
        &catalog,
        registry,
        mqtt_manager.clone(),
        &registries,
    )
    .expect("create pipeline_ab");
    pipeline_ab.set_pipeline_id(format!("pipeline_ab_{}", Uuid::new_v4().simple()));

    let mut pipeline_bc = flow::create_pipeline_with_log_sink(
        &format!("SELECT b, c FROM {stream_name}"),
        true,
        &catalog,
        registry,
        mqtt_manager.clone(),
        &registries,
    )
    .expect("create pipeline_bc");
    pipeline_bc.set_pipeline_id(format!("pipeline_bc_{}", Uuid::new_v4().simple()));

    let mut out_ab = pipeline_ab
        .take_output()
        .expect("pipeline_ab should expose output receiver");
    let mut out_bc = pipeline_bc
        .take_output()
        .expect("pipeline_bc should expose output receiver");

    pipeline_ab.start();
    pipeline_bc.start();
    tokio::time::sleep(Duration::from_millis(200)).await;

    handle
        .send(r#"{"a": 1, "b": 2, "c": 3}"#)
        .await
        .expect("send mock payload");

    let timeout_duration = Duration::from_secs(5);
    let json_ab = recv_next_json(&mut out_ab, timeout_duration).await;
    let json_bc = recv_next_json(&mut out_bc, timeout_duration).await;

    assert_eq!(json_ab, serde_json::json!([{"a": 1, "b": 2}]));
    assert_eq!(json_bc, serde_json::json!([{"b": 2, "c": 3}]));

    let info = registry
        .get_stream(&stream_name)
        .await
        .expect("shared stream info");
    assert_eq!(info.decoding_columns, vec!["a", "b", "c"]);

    pipeline_ab
        .close(Duration::from_secs(5))
        .await
        .expect("close pipeline_ab");
    pipeline_bc
        .close(Duration::from_secs(5))
        .await
        .expect("close pipeline_bc");

    registry
        .drop_stream(&stream_name)
        .await
        .expect("drop shared stream");
}
