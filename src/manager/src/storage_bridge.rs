use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstances};
use crate::pipeline::{CreatePipelineRequest, build_pipeline_definition};
use crate::startup::StartupPhase;
use crate::stream::{
    CreateStreamRequest, build_schema_from_request, build_stream_decoder, build_stream_props,
    named_schema_store, schema_registry, validate_memory_stream_binding,
    validate_memory_stream_topic, validate_stream_decoder_config,
};
use flow::catalog::EventtimeDefinition;
use flow::catalog::StreamDefinition;
use flow::connector::SharedMqttClientConfig;
#[cfg(feature = "wasm_udf")]
use flow::expr::custom_func::CustomFuncRegistry;
use flow::pipeline::PipelineDefinition;
use flow::{DecoderRegistry, EncoderRegistry};
use std::sync::Arc;
use storage::{
    StorageManager, StoredMemoryTopicKind, StoredMqttClientConfig, StoredPipeline, StoredStream,
};
#[cfg(feature = "wasm_udf")]
use udf::WasmEngine;

/// Serialize a create-stream request for storage.
pub fn stored_stream_from_request(req: &CreateStreamRequest) -> Result<StoredStream, String> {
    let mut req = req.clone();
    req.normalize();
    let raw_json =
        serde_json::to_string(&req).map_err(|err| format!("serialize stream request: {err}"))?;
    Ok(StoredStream {
        id: req.name.clone(),
        raw_json,
    })
}

/// Rebuild a StreamDefinition from stored raw JSON.
pub fn stream_definition_from_stored(
    stored: &StoredStream,
    decoder_registry: &DecoderRegistry,
) -> Result<StreamDefinition, String> {
    let req: CreateStreamRequest = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored stream {}: {err}", stored.id))?;
    let schema = build_schema_from_request(&req)?;
    let props = build_stream_props(&req.stream_type, &req.props)?;
    let decoder = build_stream_decoder(&req, decoder_registry)?;
    let mut definition = StreamDefinition::new(req.name.clone(), Arc::new(schema), props, decoder);
    if let Some(cfg) = &req.eventtime {
        definition = definition.with_eventtime(EventtimeDefinition::new(
            cfg.column.clone(),
            cfg.eventtime_type.clone(),
        ));
    }
    if let Some(sampler) = &req.sampler {
        definition = definition.with_sampler(sampler.clone());
    }
    Ok(definition)
}

/// Serialize a create-pipeline request for storage.
pub fn stored_pipeline_from_request(req: &CreatePipelineRequest) -> Result<StoredPipeline, String> {
    let raw_json =
        serde_json::to_string(req).map_err(|err| format!("serialize pipeline request: {err}"))?;
    Ok(StoredPipeline {
        id: req.id.clone(),
        raw_json,
    })
}

/// Rebuild a PipelineDefinition from stored raw JSON.
pub fn pipeline_definition_from_stored(
    stored: &StoredPipeline,
    encoder_registry: &EncoderRegistry,
    instance: &flow::FlowInstance,
) -> Result<PipelineDefinition, String> {
    let req = pipeline_request_from_stored(stored)?;
    build_pipeline_definition(&req, encoder_registry, instance)
}

pub fn pipeline_request_from_stored(
    stored: &StoredPipeline,
) -> Result<CreatePipelineRequest, String> {
    let mut req: CreatePipelineRequest = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored pipeline {}: {err}", stored.id))?;
    req.normalize();
    let instance_id = req
        .flow_instance_id
        .as_deref()
        .map(|val| val.trim().to_string())
        .filter(|val| !val.is_empty())
        .unwrap_or_else(|| DEFAULT_FLOW_INSTANCE_ID.to_string());
    req.flow_instance_id = Some(instance_id);
    Ok(req)
}

pub fn mqtt_config_from_stored(stored: &StoredMqttClientConfig) -> SharedMqttClientConfig {
    serde_json::from_str(&stored.raw_json).unwrap_or_else(|_| SharedMqttClientConfig {
        key: stored.key.clone(),
        broker_url: String::new(),
        topic: String::new(),
        client_id: String::new(),
        qos: 0,
        max_packet_size: None,
    })
}

pub fn stored_mqtt_from_config(cfg: &SharedMqttClientConfig) -> StoredMqttClientConfig {
    let raw_json = serde_json::to_string(cfg).unwrap_or_default();
    StoredMqttClientConfig {
        key: cfg.key.clone(),
        raw_json,
    }
}

fn hydrate_schemas_from_storage(storage: &StorageManager) -> Result<usize, String> {
    let stored_schemas = storage.list_schemas().map_err(|e| e.to_string())?;
    let count = stored_schemas.len();
    for stored in &stored_schemas {
        let props: serde_json::Map<String, serde_json::Value> =
            match serde_json::from_str(&stored.props_json) {
                Ok(p) => p,
                Err(err) => {
                    tracing::error!(
                        schema = %stored.name,
                        error = %err,
                        "failed to deserialize stored schema props"
                    );
                    continue;
                }
            };
        match schema_registry().parse(&stored.schema_type, &stored.name, &props) {
            Ok(schema) => {
                named_schema_store().insert(stored.name.clone(), schema);
            }
            Err(err) => {
                tracing::error!(
                    schema = %stored.name,
                    schema_type = %stored.schema_type,
                    error = %err,
                    "failed to parse stored schema; skipping"
                );
            }
        }
    }
    Ok(count)
}

struct InstanceGlobalsHydrationSummary {
    memory_topic_failures: usize,
    shared_mqtt_failures: usize,
    stream_failures: usize,
}

/// Load persisted resources into the running FlowInstance.
async fn hydrate_instance_globals_from_storage(
    storage: &StorageManager,
    instance: &flow::FlowInstance,
) -> Result<InstanceGlobalsHydrationSummary, String> {
    let mut summary = InstanceGlobalsHydrationSummary {
        memory_topic_failures: 0,
        shared_mqtt_failures: 0,
        stream_failures: 0,
    };
    for topic in storage.list_memory_topics().map_err(|e| e.to_string())? {
        let kind = match topic.kind {
            StoredMemoryTopicKind::Bytes => flow::connector::MemoryTopicKind::Bytes,
            StoredMemoryTopicKind::Collection => flow::connector::MemoryTopicKind::Collection,
        };
        if let Err(err) = instance.declare_memory_topic(&topic.topic, kind, topic.capacity) {
            summary.memory_topic_failures += 1;
            tracing::error!(topic = %topic.topic, error = %err, "failed to restore memory topic");
        }
    }

    for cfg in storage.list_mqtt_configs().map_err(|e| e.to_string())? {
        if let Err(err) = instance
            .create_shared_mqtt_client(mqtt_config_from_stored(&cfg))
            .await
        {
            summary.shared_mqtt_failures += 1;
            tracing::error!(key = %cfg.key, error = %err, "failed to restore shared mqtt client");
        }
    }

    for stream in storage.list_streams().map_err(|e| e.to_string())? {
        if let Err(err) = restore_stream(stream.clone(), storage, instance).await {
            summary.stream_failures += 1;
            tracing::error!(stream_id = %stream.id, error = %err, "failed to restore stream");
        }
    }
    Ok(summary)
}

async fn hydrate_pipelines_into_instances_from_storage(
    storage: &StorageManager,
    instances: &FlowInstances,
) -> Result<usize, String> {
    let mut failures = 0usize;
    for pipeline in storage.list_pipelines().map_err(|e| e.to_string())? {
        if let Err(err) = restore_pipeline(pipeline.clone(), storage, instances).await {
            failures += 1;
            tracing::error!(pipeline_id = %pipeline.id, error = %err, "failed to restore pipeline");
        }
    }
    Ok(failures)
}

async fn restore_stream(
    stream: StoredStream,
    storage: &StorageManager,
    instance: &flow::FlowInstance,
) -> Result<(), String> {
    let decoder_registry = instance.decoder_registry();
    let def = stream_definition_from_stored(&stream, decoder_registry.as_ref())?;
    let req = serde_json::from_str::<CreateStreamRequest>(&stream.raw_json)
        .map_err(|err| format!("decode stored stream {}: {err}", stream.id))?;
    let shared = req.shared;
    validate_stream_decoder_config(&req, def.decoder())?;
    if let flow::catalog::StreamProps::Memory(memory_props) = def.props() {
        validate_memory_stream_topic(&req, memory_props)?;
        validate_memory_stream_binding(&req, memory_props, def.decoder(), storage)?;
    }
    instance
        .create_stream(def, shared)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn restore_pipeline(
    pipeline: StoredPipeline,
    storage: &StorageManager,
    instances: &FlowInstances,
) -> Result<(), String> {
    let req = pipeline_request_from_stored(&pipeline)?;
    let flow_instance_id = req
        .flow_instance_id
        .as_deref()
        .unwrap_or(DEFAULT_FLOW_INSTANCE_ID);

    let Some(instance) = instances.get(flow_instance_id) else {
        tracing::warn!(
            pipeline_id = %pipeline.id,
            flow_instance_id = %flow_instance_id,
            "skipping pipeline restore: flow instance not available in this process"
        );
        return Ok(());
    };

    let encoder_registry = instance.encoder_registry();
    let def =
        pipeline_definition_from_stored(&pipeline, encoder_registry.as_ref(), instance.as_ref())?;
    instance
        .create_pipeline(flow::CreatePipelineRequest::new(def))
        .map_err(|e| e.to_string())?;

    match storage
        .get_pipeline_run_state(&pipeline.id)
        .map_err(|e| e.to_string())?
    {
        Some(state)
            if matches!(
                state.desired_state,
                storage::StoredPipelineDesiredState::Running
            ) =>
        {
            if let Err(err) = instance.start_pipeline(&pipeline.id) {
                tracing::error!(
                    pipeline_id = %pipeline.id,
                    error = %err,
                    "failed to auto-start pipeline"
                );
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(feature = "wasm_udf")]
fn load_wasm_udfs(
    storage: &StorageManager,
    instance: &flow::FlowInstance,
) -> Result<usize, String> {
    let udfs = storage.list_udfs().map_err(|e| e.to_string())?;
    if udfs.is_empty() {
        return Ok(0);
    }

    let engine = WasmEngine::new().map_err(|e| format!("failed to create WASM engine: {e}"))?;
    let wasm_dir = storage.wasm_files_dir();
    let mut loaded: Vec<Arc<dyn flow::CustomFunc>> = Vec::with_capacity(udfs.len());

    for udf in &udfs {
        let wasm_path = wasm_dir.join(format!("{}.wasm", udf.wasm_sha256));
        let wasm_bytes =
            std::fs::read(&wasm_path).map_err(|e| format!("read {}: {e}", wasm_path.display()))?;

        let wasm_udf = engine
            .instantiate(&udf.name, &wasm_bytes)
            .map_err(|e| format!("instantiate UDF '{}': {e}", udf.name))?;

        loaded.push(Arc::new(wasm_udf));
    }

    let registry = CustomFuncRegistry::with_builtins_and_wasm(loaded)
        .map_err(|e| format!("failed to build registry with WASM UDFs: {e}"))?;
    instance.set_custom_func_registry(registry);

    Ok(udfs.len())
}

pub(crate) async fn hydrate_runtime_from_storage(
    storage: &StorageManager,
    instances: &FlowInstances,
) -> Result<(), String> {
    let phase = StartupPhase::new("manager", "default", "runtime_storage_hydrate");
    let memory_topics = storage.list_memory_topics().map_err(|e| e.to_string())?;
    let mqtt_configs = storage.list_mqtt_configs().map_err(|e| e.to_string())?;
    let streams = storage.list_streams().map_err(|e| e.to_string())?;
    let pipelines = storage.list_pipelines().map_err(|e| e.to_string())?;

    // Hydrate named schemas first — streams may reference them.
    let schema_count = hydrate_schemas_from_storage(storage)?;

    let instances_snapshot = instances.instances_snapshot();

    tracing::info!(
        mode = "manager",
        flow_instance_id = "default",
        phase = "runtime_storage_hydrate",
        result = "discovered",
        persisted_memory_topic_count = memory_topics.len(),
        persisted_shared_mqtt_client_count = mqtt_configs.len(),
        persisted_schema_count = schema_count,
        persisted_stream_count = streams.len(),
        persisted_pipeline_count = pipelines.len(),
        instance_count = instances_snapshot.len(),
        "storage hydrate discovered persisted resources"
    );

    let mut memory_topic_restore_failures = 0usize;
    let mut shared_mqtt_restore_failures = 0usize;
    let mut stream_restore_failures = 0usize;
    let udf_count = storage.list_udfs().map_err(|e| e.to_string())?.len();

    for (_, instance) in instances_snapshot {
        let summary = hydrate_instance_globals_from_storage(storage, instance.as_ref()).await?;
        memory_topic_restore_failures += summary.memory_topic_failures;
        shared_mqtt_restore_failures += summary.shared_mqtt_failures;
        stream_restore_failures += summary.stream_failures;
    }

    #[cfg(feature = "wasm_udf")]
    let mut udf_failures = 0usize;
    #[cfg(not(feature = "wasm_udf"))]
    let udf_failures = 0usize;

    #[cfg(feature = "wasm_udf")]
    {
        // Load WASM UDFs before restoring pipelines (pipelines may reference UDFs in SQL).
        for (_, instance) in instances.instances_snapshot() {
            match load_wasm_udfs(storage, instance.as_ref()) {
                Ok(loaded) => {
                    if loaded > 0 {
                        tracing::info!(
                            flow_instance_id = %instance.id(),
                            wasm_udf_count = loaded,
                            "loaded WASM UDFs"
                        );
                    }
                }
                Err(err) => {
                    udf_failures += 1;
                    tracing::error!(
                        flow_instance_id = %instance.id(),
                        error = %err,
                        "failed to load WASM UDFs"
                    );
                }
            }
        }
    }

    let pipeline_restore_failures =
        hydrate_pipelines_into_instances_from_storage(storage, instances).await?;
    tracing::info!(
        mode = "manager",
        flow_instance_id = "default",
        phase = "runtime_storage_hydrate",
        result = "succeeded",
        elapsed_ms = phase.elapsed_ms(),
        persisted_memory_topic_count = memory_topics.len(),
        persisted_shared_mqtt_client_count = mqtt_configs.len(),
        persisted_schema_count = schema_count,
        persisted_stream_count = streams.len(),
        persisted_pipeline_count = pipelines.len(),
        memory_topic_restore_failures,
        shared_mqtt_restore_failures,
        stream_restore_failures,
        pipeline_restore_failures,
        wasm_udf_restore_failures = udf_failures,
        persisted_udf_count = udf_count,
        "storage hydrate completed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::FlowInstances;
    use crate::pipeline::CreatePipelineRequest;
    use crate::stream::CreateStreamRequest;
    use flow::FlowInstance;
    use serde_json::{Map as JsonMap, Value as JsonValue, json};
    use tempfile::tempdir;

    fn sample_stream_request(name: &str) -> CreateStreamRequest {
        let schema_props: JsonMap<String, JsonValue> = json!({
            "columns": [
                {"name":"value","data_type":"int64"}
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        let props_fields: JsonMap<String, JsonValue> = json!({
            "broker_url": "mqtt://localhost:1883",
            "topic": "in",
            "qos": 0
        })
        .as_object()
        .unwrap()
        .clone();

        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: crate::stream::SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: schema_props,
                r#ref: None,
            },
            props: crate::stream::StreamPropsRequest {
                fields: props_fields,
            },
            shared: false,
            decoder: crate::stream::DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn sample_pipeline_request(id: &str, stream_name: &str) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": id,
            "flow_instance_id": "default",
            "sql": format!("SELECT * FROM {stream_name}"),
            "sinks": [
                {
                    "id": format!("{id}_sink_0"),
                    "type": "nop",
                    "props": { "log": false },
                    "common_sink_props": {},
                    "encoder": { "type": "json", "props": {} }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request")
    }

    // Plan-cache behavior is intentionally tested in the flow crate. The manager's storage bridge
    // only restores pipelines from their persisted SQL specification.
    #[tokio::test]
    async fn load_storage_skips_invalid_streams() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let instance = FlowInstance::new(
            flow::instance::FlowInstanceOptions::shared_current_runtime("default", None),
        )
        .expect("create flow instance");

        // 1. Create a GOOD stream
        let good_req = sample_stream_request("good_stream");
        let good_stored = stored_stream_from_request(&good_req).unwrap();
        storage.create_stream(good_stored.clone()).unwrap();

        // 2. Create a BAD stream (manually insert invalid JSON)
        let bad_stored = StoredStream {
            id: "bad_stream".to_string(),
            raw_json: "{ invalid json".to_string(),
        };
        storage.create_stream(bad_stored).unwrap();

        // 3. Load. This should NOT return Err because stream restore errors are logged and skipped.
        hydrate_instance_globals_from_storage(&storage, &instance)
            .await
            .expect("hydrate instance globals from storage");

        // 4. Verify GOOD stream exists, BAD stream does not
        let streams = instance.list_streams().await.unwrap();
        assert!(streams.iter().any(|s| s.definition.id() == "good_stream"));
        assert!(!streams.iter().any(|s| s.definition.id() == "bad_stream"));
    }

    #[tokio::test]
    async fn load_storage_skips_invalid_pipelines_and_restores_valid_neighbors() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let instance = FlowInstance::new(
            flow::instance::FlowInstanceOptions::shared_current_runtime("default", None),
        )
        .expect("create flow instance");
        let instances = FlowInstances::new(instance);

        let stream_req = sample_stream_request("good_stream");
        storage
            .create_stream(stored_stream_from_request(&stream_req).expect("serialize stream"))
            .expect("store good stream");

        let good_pipeline = sample_pipeline_request("good_pipe", "good_stream");
        storage
            .create_pipeline(
                stored_pipeline_from_request(&good_pipeline).expect("serialize good pipeline"),
            )
            .expect("store good pipeline");
        storage
            .create_pipeline(StoredPipeline {
                id: "bad_pipe".to_string(),
                raw_json: "{ invalid json".to_string(),
            })
            .expect("store bad pipeline");

        hydrate_runtime_from_storage(&storage, &instances)
            .await
            .expect("hydrate runtime from storage");

        let snapshots = instances.default_instance().list_pipelines();
        assert!(
            snapshots
                .iter()
                .any(|snapshot| snapshot.definition.id() == "good_pipe"),
            "valid pipeline should still be restored",
        );
        assert!(
            !snapshots
                .iter()
                .any(|snapshot| snapshot.definition.id() == "bad_pipe"),
            "invalid pipeline should be skipped during restore",
        );
    }
}
