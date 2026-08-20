use super::*;
use crate::catalog::{
    Catalog, MockStreamProps, MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use crate::codec::JsonDecoder;
use crate::connector::{MemoryPubSubRegistry, MockSourceConnector, MqttClientManager};
use crate::planner::sink::{
    NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
};
use crate::processor::{EncodedDeliveryFlags, StreamData};
use crate::runtime::TaskSpawner;
use crate::shared_stream::{SharedStreamConfig, SharedStreamRegistry};
use crate::PipelineRegistries;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};
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

fn test_spawner() -> TaskSpawner {
    TaskSpawner::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build test tokio runtime"),
    )
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

fn create_pipeline_with_log_sink(
    sql: &str,
    forward_to_result: bool,
    catalog: &Catalog,
    shared_stream_registry: Arc<SharedStreamRegistry>,
    mqtt_client_manager: MqttClientManager,
    spawner: TaskSpawner,
    registries: &PipelineRegistries,
) -> Result<crate::processor::ProcessorPipeline, Box<dyn std::error::Error>> {
    let connector = PipelineSinkConnector::new(
        "log_sink_connector",
        SinkConnectorConfig::Nop(NopSinkConfig { log: true }),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("log_sink", connector).with_forward_to_result(forward_to_result);
    crate::create_pipeline(
        sql,
        vec![sink],
        catalog,
        shared_stream_registry,
        mqtt_client_manager,
        spawner,
        registries,
    )
}

async fn recv_next_json(
    output: &mut tokio::sync::mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut delivery: Vec<u8> = Vec::new();
    let mut active_delivery = false;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::EncodedDelivery { flags, bytes } => {
                if flags.contains(EncodedDeliveryFlags::ABORT) {
                    panic!("encoded delivery aborted");
                }
                if flags.contains(EncodedDeliveryFlags::START) {
                    delivery.clear();
                    active_delivery = true;
                }
                if !active_delivery {
                    panic!("encoded delivery chunk without START");
                }
                delivery.extend_from_slice(&bytes);
                if flags.contains(EncodedDeliveryFlags::END) {
                    return serde_json::from_slice(&delivery).expect("invalid JSON payload");
                }
            }
            StreamData::Control(_) => continue,
            StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            other => panic!("unexpected stream data: {}", other.description()),
        }
    }
}

#[test]
fn create_and_list_pipeline() {
    let catalog = Arc::new(Catalog::new());
    let spawner = test_spawner();
    let mqtt_manager = MqttClientManager::new("default", spawner.clone());
    let memory_pubsub_registry = MemoryPubSubRegistry::new();
    let registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
    install_stream(&catalog, "test_stream");
    let registries = PipelineRegistries::new_with_builtin();
    let context = crate::pipeline::PipelineContext::new(
        "default",
        Arc::clone(&registry),
        mqtt_manager.clone(),
        memory_pubsub_registry.clone(),
        spawner.clone(),
    );
    let manager = PipelineManager::new(Arc::clone(&catalog), context, registries);
    let snapshot = manager
        .create_pipeline(CreatePipelineRequest::new(sample_pipeline(
            "pipe_a",
            "test_stream",
        )))
        .expect("create pipeline")
        .snapshot;
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
fn shared_stream_two_pipelines_project_different_columns() {
    let runtime = Runtime::new().expect("runtime");
    runtime.block_on(async move {
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

        let spawner = test_spawner();
        let shared_stream_registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
        let (connector, handle) = MockSourceConnector::new(format!("{stream_name}_connector"));
        let decoder = Arc::new(JsonDecoder::new(
            stream_name.clone(),
            Arc::clone(&schema),
            JsonMap::new(),
        ));
        let config = SharedStreamConfig::new(stream_name.clone(), Arc::clone(&schema))
            .with_connector(Box::new(connector), decoder);
        shared_stream_registry
            .create_stream(config)
            .await
            .expect("create shared stream");

        let mqtt_manager = MqttClientManager::new("default", spawner.clone());
        let registries = PipelineRegistries::new_with_builtin();

        let mut pipeline_ab = create_pipeline_with_log_sink(
            &format!("SELECT a, b FROM {stream_name}"),
            true,
            catalog.as_ref(),
            Arc::clone(&shared_stream_registry),
            mqtt_manager.clone(),
            spawner.clone(),
            &registries,
        )
        .expect("create pipeline_ab");
        pipeline_ab.set_pipeline_id(format!("pipeline_ab_{}", Uuid::new_v4().simple()));

        let mut pipeline_bc = create_pipeline_with_log_sink(
            &format!("SELECT b, c FROM {stream_name}"),
            true,
            catalog.as_ref(),
            Arc::clone(&shared_stream_registry),
            mqtt_manager.clone(),
            spawner.clone(),
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

        pipeline_ab.start().await.expect("start pipeline_ab");
        pipeline_bc.start().await.expect("start pipeline_bc");
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

        let info = shared_stream_registry
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

        shared_stream_registry
            .drop_stream(&stream_name)
            .await
            .expect("drop shared stream");
    });
}

#[test]
fn prevent_duplicate_pipeline() {
    let catalog = Arc::new(Catalog::new());
    let spawner = test_spawner();
    let mqtt_manager = MqttClientManager::new("default", spawner.clone());
    let memory_pubsub_registry = MemoryPubSubRegistry::new();
    let registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
    install_stream(&catalog, "dup_stream");
    let registries = PipelineRegistries::new_with_builtin();
    let context = crate::pipeline::PipelineContext::new(
        "default",
        Arc::clone(&registry),
        mqtt_manager.clone(),
        memory_pubsub_registry.clone(),
        spawner.clone(),
    );
    let manager = PipelineManager::new(Arc::clone(&catalog), context, registries);
    manager
        .create_pipeline(CreatePipelineRequest::new(sample_pipeline(
            "dup_pipe",
            "dup_stream",
        )))
        .expect("first creation");
    let result = manager.create_pipeline(CreatePipelineRequest::new(sample_pipeline(
        "dup_pipe",
        "dup_stream",
    )));
    assert!(matches!(result, Err(PipelineError::AlreadyExists(_))));
    Runtime::new()
        .unwrap()
        .block_on(manager.delete_pipeline("dup_pipe"))
        .ok();
}

#[test]
fn final_checkpoint_failure_leaves_pipeline_stopped() {
    let runtime = Runtime::new().expect("runtime");
    runtime.block_on(async move {
        let stream_name = format!("checkpoint_failure_{}", Uuid::new_v4().simple());
        let pipeline_id = format!("checkpoint_failure_pipe_{}", Uuid::new_v4().simple());
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            stream_name.clone(),
            "value".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));
        let catalog = Arc::new(Catalog::new());
        catalog.upsert(StreamDefinition::new(
            stream_name.clone(),
            schema,
            StreamProps::Mock(MockStreamProps::default()),
            StreamDecoderConfig::json(),
        ));

        let spawner = test_spawner();
        let shared_stream_registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
        let context = crate::pipeline::PipelineContext::new(
            "default",
            shared_stream_registry,
            MqttClientManager::new("default", spawner.clone()),
            MemoryPubSubRegistry::new(),
            spawner,
        );
        let manager = PipelineManager::new(
            Arc::clone(&catalog),
            context,
            PipelineRegistries::new_with_builtin(),
        );
        manager.set_checkpoint_store(Arc::new(crate::checkpoint::InMemoryCheckpointStore::new()));
        let definition = PipelineDefinition::new(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
            Vec::new(),
        )
        .with_options(PipelineOptions {
            checkpoint_enabled: true,
            ..PipelineOptions::default()
        });
        manager
            .create_pipeline(CreatePipelineRequest::new(definition))
            .expect("create checkpoint pipeline");
        manager
            .start_pipeline(&pipeline_id)
            .await
            .expect("start checkpoint pipeline");

        manager
            .stop_pipeline(
                &pipeline_id,
                PipelineStopMode::Graceful,
                Duration::from_millis(20),
            )
            .await
            .expect_err("mock source should reject the final checkpoint");

        let snapshot = manager
            .list()
            .into_iter()
            .find(|snapshot| snapshot.definition.id() == pipeline_id)
            .expect("pipeline snapshot");
        assert_eq!(snapshot.status, PipelineStatus::Stopped);
        let stats_err = manager
            .collect_stats(&pipeline_id, Duration::from_millis(20))
            .await
            .expect_err("stopped pipeline should not expose running stats");
        assert!(stats_err.to_string().contains("is not running"));
    });
}

#[test]
fn attach_sources_accepts_shared_stream_only_pipeline() {
    let runtime = Runtime::new().expect("runtime");
    runtime.block_on(async move {
        let stream_name = format!("shared_stream_attach_test_{}", Uuid::new_v4().simple());
        let catalog = Arc::new(Catalog::new());
        let spawner = test_spawner();
        let mqtt_manager = MqttClientManager::new("default", spawner.clone());
        let registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
        let memory_pubsub_registry = MemoryPubSubRegistry::new();
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

        let mut pipeline = create_pipeline_with_log_sink(
            &format!("SELECT sum(value) FROM {stream_name} GROUP BY slidingwindow('ss',10)"),
            false,
            &catalog,
            Arc::clone(&registry),
            mqtt_manager.clone(),
            spawner.clone(),
            &registries,
        )
        .expect("create pipeline");

        let context = crate::pipeline::PipelineContext::new(
            "default",
            Arc::clone(&registry),
            mqtt_manager.clone(),
            memory_pubsub_registry.clone(),
            spawner.clone(),
        );
        let stream_defs = HashMap::new();
        super::internal::attach_sources_from_catalog(&mut pipeline, &stream_defs, &context)
            .expect("shared stream should not require datasource connectors");
    });
}

#[test]
fn shared_stream_pipeline_uses_full_schema_for_column_indices() {
    let runtime = Runtime::new().expect("runtime");
    runtime.block_on(async move {
        let stream_name = format!("shared_stream_schema_test_{}", Uuid::new_v4().simple());
        let catalog = Arc::new(Catalog::new());
        let spawner = test_spawner();
        let mqtt_manager = MqttClientManager::new("default", spawner.clone());
        let registry = Arc::new(SharedStreamRegistry::new(spawner.clone()));
        let memory_pubsub_registry = MemoryPubSubRegistry::new();
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

        let mut pipeline = create_pipeline_with_log_sink(
            &format!("SELECT sum(b) FROM {stream_name} GROUP BY slidingwindow('ss',10)"),
            false,
            &catalog,
            Arc::clone(&registry),
            mqtt_manager.clone(),
            spawner.clone(),
            &registries,
        )
        .expect("create pipeline");

        let context = crate::pipeline::PipelineContext::new(
            "default",
            Arc::clone(&registry),
            mqtt_manager.clone(),
            memory_pubsub_registry.clone(),
            spawner.clone(),
        );
        let stream_defs = HashMap::new();
        super::internal::attach_sources_from_catalog(&mut pipeline, &stream_defs, &context)
            .expect("shared stream should not require datasource connectors");
    });
}
