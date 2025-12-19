use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Value};
use flow::aggregation::AggregateFunctionRegistry;
use flow::catalog::{Catalog, MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{
    get_mock_source_handle, take_mock_source_handle, ConnectorRegistry, MqttClientManager,
};
use flow::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
use flow::processor::StreamData;
use flow::{create_pipeline_with_attached_sources, shared_stream_registry, DecoderRegistry};
use flow::{EncoderRegistry, PipelineRegistries};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn mock_source_attaches_and_can_send_payloads() {
    let catalog = Arc::new(Catalog::new());
    let registry = shared_stream_registry();
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

    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]));
    catalog.upsert(StreamDefinition::new(
        "stream".to_string(),
        Arc::clone(&schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    ));

    let connector = PipelineSinkConnector::new(
        "test_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("test_sink", connector).with_forward_to_result(true);

    let mut pipeline = create_pipeline_with_attached_sources(
        "SELECT a + 1 FROM stream",
        vec![sink],
        &catalog,
        registry,
        mqtt_manager.clone(),
        &registries,
    )
    .expect("create pipeline with attached sources");

    let pipeline_id = pipeline.pipeline_id().to_string();
    let processor_id = "PhysicalDataSource_0";
    let key = format!("{pipeline_id}:stream:{processor_id}");

    assert!(
        get_mock_source_handle(&key).is_some(),
        "mock handle should be registered"
    );
    let handle = take_mock_source_handle(&key).expect("take mock handle");

    pipeline.start();

    handle.send(r#"{"a": 41}"#).await.expect("send payload");

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");
    let item = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timeout waiting output")
        .expect("missing output");
    let StreamData::Collection(collection) = item else {
        panic!("expected collection");
    };
    assert_eq!(collection.num_rows(), 1);
    let row = &collection.rows()[0];
    assert_eq!(row.value_by_name("", "a + 1"), Some(&Value::Int64(42)));

    pipeline.close().await.expect("close pipeline");
}
