use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::aggregation::AggregateFunctionRegistry;
use flow::catalog::{Catalog, MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::{DecoderRegistry, EncoderRegistry, JsonDecoder};
use flow::connector::{ConnectorRegistry, MockSourceConnector, MqttClientManager};
use flow::processor::StreamData;
use flow::{shared_stream_registry, PipelineRegistries, SharedStreamConfig};
use serde_json::Map as JsonMap;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

async fn recv_next_collection(
    output: &mut tokio::sync::mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> Box<dyn flow::model::Collection> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::Collection(collection) => return collection,
            StreamData::Control(_) => continue,
            StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            StreamData::Encoded { .. } => panic!("unexpected stream data: Encoded"),
            StreamData::Bytes(_) => panic!("unexpected stream data: Bytes"),
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
    let connector_registry = ConnectorRegistry::with_builtin_sinks();
    let encoder_registry = EncoderRegistry::with_builtin_encoders();
    let decoder_registry = DecoderRegistry::with_builtin_decoders();
    let aggregate_registry = AggregateFunctionRegistry::with_builtins();
    let registries = PipelineRegistries::new(
        connector_registry,
        encoder_registry,
        decoder_registry,
        aggregate_registry,
    );

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
    let col_ab = recv_next_collection(&mut out_ab, timeout_duration).await;
    let col_bc = recv_next_collection(&mut out_bc, timeout_duration).await;

    assert_eq!(col_ab.num_rows(), 1);
    assert_eq!(col_bc.num_rows(), 1);

    let row_ab = &col_ab.rows()[0];
    let row_bc = &col_bc.rows()[0];

    assert_eq!(
        row_ab.value_by_name(&stream_name, "a"),
        Some(&Value::Int64(1))
    );
    assert_eq!(
        row_ab.value_by_name(&stream_name, "b"),
        Some(&Value::Int64(2))
    );
    assert_eq!(row_ab.value_by_name(&stream_name, "c"), None);

    assert_eq!(
        row_bc.value_by_name(&stream_name, "b"),
        Some(&Value::Int64(2))
    );
    assert_eq!(
        row_bc.value_by_name(&stream_name, "c"),
        Some(&Value::Int64(3))
    );
    assert_eq!(row_bc.value_by_name(&stream_name, "a"), None);

    let info = registry
        .get_stream(&stream_name)
        .await
        .expect("shared stream info");
    assert_eq!(info.decoding_columns, vec!["a", "b", "c"]);

    pipeline_ab.close().await.expect("close pipeline_ab");
    pipeline_bc.close().await.expect("close pipeline_bc");

    registry
        .drop_stream(&stream_name)
        .await
        .expect("drop shared stream");
}
