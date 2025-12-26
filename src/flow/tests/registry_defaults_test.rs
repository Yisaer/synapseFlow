use flow::connector::{ConnectorRegistry, MqttClientManager};
use flow::{
    AggregateFunctionRegistry, BuiltinEventtimeType, CustomFuncRegistry, DecoderRegistry,
    EncoderRegistry, EventtimeTypeRegistry, NopSinkConfig, PipelineRegistries, SinkConnectorConfig,
    StatefulFunctionRegistry,
};

#[test]
fn individual_registry_defaults_include_builtins() {
    let mqtt_clients = MqttClientManager::new();
    let connector_registry = ConnectorRegistry::default();
    connector_registry
        .instantiate_sink(
            "nop",
            "sink_1",
            &SinkConnectorConfig::Nop(NopSinkConfig),
            &mqtt_clients,
        )
        .expect("nop sink should be registered by default");

    let encoder_registry = EncoderRegistry::default();
    assert!(encoder_registry.is_registered("json"));

    let decoder_registry = DecoderRegistry::default();
    assert!(decoder_registry.is_registered("json"));

    let aggregate_registry = AggregateFunctionRegistry::default();
    assert!(aggregate_registry.is_registered("sum"));
    assert!(aggregate_registry.is_registered("last_row"));

    let stateful_registry = StatefulFunctionRegistry::default();
    assert!(stateful_registry.is_registered("lag"));

    let custom_func_registry = CustomFuncRegistry::default();
    assert!(custom_func_registry.is_registered("concat"));

    let eventtime_registry = EventtimeTypeRegistry::default();
    assert!(eventtime_registry.is_registered(BuiltinEventtimeType::UnixtimestampSeconds.key()));
    assert!(eventtime_registry.is_registered(BuiltinEventtimeType::UnixtimestampMillis.key()));
}

#[test]
fn pipeline_registries_default_uses_builtins() {
    let mqtt_clients = MqttClientManager::new();
    let registries = PipelineRegistries::default();
    registries
        .connector_registry()
        .instantiate_sink(
            "nop",
            "sink_1",
            &SinkConnectorConfig::Nop(NopSinkConfig),
            &mqtt_clients,
        )
        .expect("nop sink should be registered by default");

    assert!(registries.encoder_registry().is_registered("json"));
    assert!(registries.decoder_registry().is_registered("json"));
    assert!(registries.aggregate_registry().is_registered("sum"));
    assert!(registries.stateful_registry().is_registered("lag"));
    assert!(registries.custom_func_registry().is_registered("concat"));
    assert!(registries
        .eventtime_type_registry()
        .is_registered(BuiltinEventtimeType::UnixtimestampSeconds.key()));
}
