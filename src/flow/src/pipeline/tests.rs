use super::*;
use crate::catalog::{
    Catalog, MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use crate::codec::JsonDecoder;
use crate::connector::{MockSourceConnector, MqttClientManager};
use crate::shared_stream::SharedStreamConfig;
use crate::shared_stream_registry;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use serde_json::Map as JsonMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn install_stream(catalog: &Arc<Catalog>, name: &str) {
    let schema = Schema::new(vec![ColumnSchema::new(
        name.to_string(),
        "value".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let definition = StreamDefinition::new(
        name.to_string(),
        Arc::new(schema),
        StreamProps::Mqtt(MqttStreamProps::new(
            "mqtt://localhost:1883",
            format!("{name}/in"),
            0,
        )),
        StreamDecoderConfig::json(),
    );
    catalog.upsert(definition);
}

fn sample_pipeline(id: &str, stream: &str) -> PipelineDefinition {
    let sink = SinkDefinition::new(
        format!("{id}_sink"),
        SinkType::Mqtt,
        SinkProps::Mqtt(MqttSinkProps::new(
            "mqtt://localhost:1883",
            format!("{id}/out"),
            0,
        )),
    );
    PipelineDefinition::new(
        id.to_string(),
        format!("SELECT * FROM {stream}"),
        vec![sink],
    )
}

#[test]
fn create_and_list_pipeline() {
    let catalog = Arc::new(Catalog::new());
    let registry = shared_stream_registry();
    let mqtt_manager = MqttClientManager::new();
    install_stream(&catalog, "test_stream");
    let registries = PipelineRegistries::new_with_builtin();
    let manager = PipelineManager::new(
        Arc::clone(&catalog),
        registry,
        mqtt_manager.clone(),
        registries,
    );
    let snapshot = manager
        .create_pipeline(sample_pipeline("pipe_a", "test_stream"))
        .expect("create pipeline");
    assert_eq!(snapshot.status, PipelineStatus::Stopped);
    let list = manager.list();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].definition.id(), "pipe_a");
    Runtime::new()
        .unwrap()
        .block_on(manager.delete_pipeline("pipe_a"))
        .expect("delete pipeline");
}

#[test]
fn prevent_duplicate_pipeline() {
    let catalog = Arc::new(Catalog::new());
    let registry = shared_stream_registry();
    let mqtt_manager = MqttClientManager::new();
    install_stream(&catalog, "dup_stream");
    let registries = PipelineRegistries::new_with_builtin();
    let manager = PipelineManager::new(
        Arc::clone(&catalog),
        registry,
        mqtt_manager.clone(),
        registries,
    );
    manager
        .create_pipeline(sample_pipeline("dup_pipe", "dup_stream"))
        .expect("first creation");
    let result = manager.create_pipeline(sample_pipeline("dup_pipe", "dup_stream"));
    assert!(matches!(result, Err(PipelineError::AlreadyExists(_))));
    Runtime::new()
        .unwrap()
        .block_on(manager.delete_pipeline("dup_pipe"))
        .ok();
}

#[test]
fn attach_sources_accepts_shared_stream_only_pipeline() {
    let runtime = Runtime::new().expect("runtime");
    runtime.block_on(async move {
        let stream_name = format!("shared_stream_attach_test_{}", Uuid::new_v4().simple());
        let catalog = Arc::new(Catalog::new());
        let registry = shared_stream_registry();
        let mqtt_manager = MqttClientManager::new();
        let registries = PipelineRegistries::new_with_builtin();

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            stream_name.clone(),
            "value".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));
        let definition = StreamDefinition::new(
            stream_name.clone(),
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::new(
                "mqtt://localhost:1883",
                format!("{stream_name}/in"),
                0,
            )),
            StreamDecoderConfig::json(),
        );
        catalog.upsert(definition);

        let (connector, _handle) = MockSourceConnector::new(format!("{stream_name}_connector"));
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

        let mut pipeline = crate::create_pipeline_with_log_sink(
            &format!("SELECT sum(value) FROM {stream_name} GROUP BY slidingwindow('ss',10)"),
            false,
            &catalog,
            registry,
            mqtt_manager.clone(),
            &registries,
        )
        .expect("create pipeline");

        let stream_defs = HashMap::new();
        super::internal::attach_sources_from_catalog(&mut pipeline, &stream_defs, &mqtt_manager)
            .expect("shared stream should not require datasource connectors");
    });
}

#[test]
fn shared_stream_pipeline_uses_full_schema_for_column_indices() {
    let runtime = Runtime::new().expect("runtime");
    runtime.block_on(async move {
        let stream_name = format!("shared_stream_schema_test_{}", Uuid::new_v4().simple());
        let catalog = Arc::new(Catalog::new());
        let registry = shared_stream_registry();
        let mqtt_manager = MqttClientManager::new();
        let registries = PipelineRegistries::new_with_builtin();

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
        ]));

        let definition = StreamDefinition::new(
            stream_name.clone(),
            Arc::clone(&schema),
            StreamProps::Mqtt(MqttStreamProps::new(
                "mqtt://localhost:1883",
                format!("{stream_name}/in"),
                0,
            )),
            StreamDecoderConfig::json(),
        );
        catalog.upsert(definition);

        let (connector, _handle) = MockSourceConnector::new(format!("{stream_name}_connector"));
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

        let mut pipeline = crate::create_pipeline_with_log_sink(
            &format!("SELECT sum(b) FROM {stream_name} GROUP BY slidingwindow('ss',10)"),
            false,
            &catalog,
            registry,
            mqtt_manager.clone(),
            &registries,
        )
        .expect("create pipeline");

        let stream_defs = HashMap::new();
        super::internal::attach_sources_from_catalog(&mut pipeline, &stream_defs, &mqtt_manager)
            .expect("shared stream should not require datasource connectors");
    });
}
