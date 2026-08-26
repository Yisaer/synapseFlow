use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstances};
use crate::pipeline::{CreatePipelineRequest, build_pipeline_definition};
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use crate::schema::source::{reconcile_installed_sources, resolve_installed_props};
use crate::startup::StartupPhase;
use crate::stream::{
    CreateStreamRequest, build_stream_decoder, build_stream_props, named_schema_store,
    resolve_schema_from_request, schema_registry, validate_memory_stream_binding,
    validate_memory_stream_topic, validate_stream_connector_key, validate_stream_decoder_config,
};
use crate::table::CreateTableRequest;
use flow::catalog::EventtimeDefinition;
use flow::catalog::StreamDefinition;
use flow::catalog::TableDefinition;
use flow::connector::SharedMqttClientConfig;
#[cfg(feature = "wasm_udf")]
use flow::expr::custom_func::CustomFuncRegistry;
use flow::pipeline::PipelineDefinition;
use flow::{DecoderRegistry, EncoderRegistry};
use std::sync::Arc;
use storage::{
    StorageManager, StoredMemoryTopicKind, StoredMqttClientConfig, StoredPipeline, StoredStream,
    StoredTable,
};
#[cfg(feature = "wasm_udf")]
use udf::WasmEngine;

/// Serialize a create-stream request for storage.
pub fn stored_stream_from_request(req: &CreateStreamRequest) -> Result<StoredStream, String> {
    let mut req = req.clone();
    req.normalize();
    let revision = req.revision;
    let mut value =
        serde_json::to_value(&req).map_err(|err| format!("serialize stream request: {err}"))?;
    value
        .as_object_mut()
        .ok_or_else(|| "stream request must serialize as an object".to_string())?
        .remove("revision");
    let raw_json =
        serde_json::to_string(&value).map_err(|err| format!("serialize stream request: {err}"))?;
    Ok(StoredStream {
        id: req.name.clone(),
        revision,
        raw_json,
    })
}

/// Serialize a create-table request for storage.
pub fn stored_table_from_request(req: &CreateTableRequest) -> Result<StoredTable, String> {
    let mut value =
        serde_json::to_value(req).map_err(|err| format!("serialize table request: {err}"))?;
    value
        .as_object_mut()
        .ok_or_else(|| "table request must serialize as an object".to_string())?
        .remove("revision");
    let raw_json =
        serde_json::to_string(&value).map_err(|err| format!("serialize table request: {err}"))?;
    Ok(StoredTable {
        id: req.name.clone(),
        revision: req.revision,
        raw_json,
    })
}

/// Rebuild a StreamDefinition from stored raw JSON.
pub fn stream_definition_from_stored(
    stored: &StoredStream,
    decoder_registry: &DecoderRegistry,
) -> Result<StreamDefinition, String> {
    let req = stream_request_from_stored(stored)?;
    let resolved_schema = resolve_schema_from_request(&req)?;
    let props = build_stream_props(&req.stream_type, &req.props)?;
    let decoder = build_stream_decoder(&req, decoder_registry, &resolved_schema)?;
    let mut definition = StreamDefinition::new(
        req.name.clone(),
        Arc::clone(&resolved_schema.logical_schema),
        props,
        decoder,
    );
    if let Some(cfg) = &req.eventtime {
        definition = definition.with_eventtime(EventtimeDefinition::new(
            cfg.column.clone(),
            cfg.eventtime_type.clone(),
        ));
    }
    if let Some(sampler) = req.sampler.clone() {
        definition = definition.with_sampler(sampler);
    }
    Ok(definition)
}

pub fn stream_request_from_stored(stored: &StoredStream) -> Result<CreateStreamRequest, String> {
    let mut value: serde_json::Value = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored stream {}: {err}", stored.id))?;
    value
        .as_object_mut()
        .ok_or_else(|| format!("stored stream {} must be a JSON object", stored.id))?
        .insert("revision".to_string(), stored.revision.into());
    serde_json::from_value(value)
        .map_err(|err| format!("decode stored stream {}: {err}", stored.id))
}

pub fn table_request_from_stored(stored: &StoredTable) -> Result<CreateTableRequest, String> {
    let mut value: serde_json::Value = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored table {}: {err}", stored.id))?;
    value
        .as_object_mut()
        .ok_or_else(|| format!("stored table {} must be a JSON object", stored.id))?
        .insert("revision".to_string(), stored.revision.into());
    serde_json::from_value(value).map_err(|err| format!("decode stored table {}: {err}", stored.id))
}

/// Rebuild a TableDefinition from stored raw JSON.
pub fn table_definition_from_stored(
    stored: &StoredTable,
    decoder_registry: &DecoderRegistry,
) -> Result<TableDefinition, String> {
    use crate::table::{build_table_decoder, build_table_props};

    let req = table_request_from_stored(stored)?;
    let resolved_schema = {
        if let Some(ref_name) = &req.schema.r#ref {
            validate_resource_id(ResourceIdKind::SchemaName, ref_name)
                .map_err(|err| format!("invalid schema ref: {err}"))?;
            named_schema_store()
                .get_resolved(ref_name)
                .ok_or_else(|| format!("referenced schema '{}' not found", ref_name))?
        } else {
            let (schema, bundle, artifact) = schema_registry()
                .parse(&req.schema.schema_type, &req.name, &req.schema.props)
                .map_err(|err| format!("parse schema for table {}: {err}", stored.id))?;
            crate::stream::ResolvedSchema::new(schema, bundle, artifact)
        }
    };
    let props = build_table_props(&req.table_type, &req.props)?;
    let decoder = build_table_decoder(&req, decoder_registry, &resolved_schema)?;
    Ok(TableDefinition::new(
        req.name.clone(),
        Arc::clone(&resolved_schema.logical_schema),
        props,
        decoder,
    ))
}

/// Serialize a create-pipeline request for storage.
pub fn stored_pipeline_from_request(req: &CreatePipelineRequest) -> Result<StoredPipeline, String> {
    let mut value =
        serde_json::to_value(req).map_err(|err| format!("serialize pipeline request: {err}"))?;
    value
        .as_object_mut()
        .ok_or_else(|| "pipeline request must serialize as an object".to_string())?
        .remove("revision");
    let raw_json = serde_json::to_string(&value)
        .map_err(|err| format!("serialize pipeline request: {err}"))?;
    Ok(StoredPipeline {
        id: req.id.clone(),
        revision: req.revision,
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
    let mut value: serde_json::Value = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored pipeline {}: {err}", stored.id))?;
    value
        .as_object_mut()
        .ok_or_else(|| format!("stored pipeline {} must be a JSON object", stored.id))?
        .insert("revision".to_string(), stored.revision.into());
    let mut req: CreatePipelineRequest = serde_json::from_value(value)
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
        protocol_version: Default::default(),
        username: None,
        password: None,
        resolved_password: None,
    })
}

pub fn stored_mqtt_from_config(
    cfg: &SharedMqttClientConfig,
    revision: u64,
) -> StoredMqttClientConfig {
    let raw_json = serde_json::to_string(cfg).unwrap_or_default();
    StoredMqttClientConfig {
        key: cfg.key.clone(),
        revision,
        raw_json,
    }
}

pub(crate) fn hydrate_schemas_from_storage(storage: &StorageManager) -> Result<usize, String> {
    let stored_schemas = storage.list_schemas().map_err(|e| e.to_string())?;
    reconcile_installed_sources(storage, &stored_schemas)?;
    let count = stored_schemas.len();
    for stored in &stored_schemas {
        // Defensive: skip historically-invalid schema names (VF-51 §5.2).
        if let Err(err) = validate_resource_id(ResourceIdKind::SchemaName, &stored.name) {
            tracing::error!(
                schema = %stored.name.escape_debug(),
                error = %err,
                "skipping stored schema with invalid name"
            );
            continue;
        }
        let mut props: serde_json::Map<String, serde_json::Value> =
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
        if let Err(err) =
            resolve_installed_props(storage, &stored.name, &stored.schema_type, &mut props)
        {
            tracing::error!(
                schema = %stored.name,
                schema_type = %stored.schema_type,
                error = %err,
                "failed to resolve installed schema source; skipping"
            );
            continue;
        }
        match schema_registry().parse(&stored.schema_type, &stored.name, &props) {
            Ok((schema, proto_bundle, artifact)) => {
                named_schema_store().insert_resolved(
                    stored.name.clone(),
                    schema,
                    proto_bundle,
                    artifact,
                );
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
    table_failures: usize,
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
        table_failures: 0,
    };
    for topic in storage.list_memory_topics().map_err(|e| e.to_string())? {
        // Defensive: skip historically-invalid topic ids (VF-51 §5.2).
        if let Err(err) = validate_resource_id(ResourceIdKind::MemoryTopic, &topic.topic) {
            summary.memory_topic_failures += 1;
            tracing::error!(topic = %topic.topic.escape_debug(), error = %err, "skipping memory topic with invalid id");
            continue;
        }
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
        // Defensive: skip historically-invalid shared mqtt keys (VF-51 §5.2).
        if let Err(err) = validate_resource_id(ResourceIdKind::SharedMqttClientKey, &cfg.key) {
            summary.shared_mqtt_failures += 1;
            tracing::error!(key = %cfg.key.escape_debug(), error = %err, "skipping shared mqtt client with invalid key");
            continue;
        }
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

    for table in storage.list_tables().map_err(|e| e.to_string())? {
        if let Err(err) = restore_table(table.clone(), instance).await {
            summary.table_failures += 1;
            tracing::error!(table_id = %table.id, error = %err, "failed to restore table");
        }
    }
    Ok(summary)
}

async fn restore_table(stored: StoredTable, instance: &flow::FlowInstance) -> Result<(), String> {
    // Defensive: skip historically-invalid ids (VF-51 §5.2).
    validate_resource_id(ResourceIdKind::StreamName, &stored.id)?;
    let decoder_registry = instance.decoder_registry();
    let def = table_definition_from_stored(&stored, decoder_registry.as_ref())?;
    instance
        .create_table(def)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
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
    let req = stream_request_from_stored(&stream)?;
    // Defensive: reject historically-invalid resource ids so bad data cannot
    // re-enter the runtime on restart (VF-51 §5.2). The error is propagated to
    // the caller, which logs and skips this stream.
    validate_resource_id(ResourceIdKind::StreamName, &req.name)?;
    validate_stream_connector_key(&req)?;
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
    // Defensive: reject historically-invalid pipeline ids (VF-51 §5.2). The
    // caller logs and skips on error.
    validate_resource_id(ResourceIdKind::PipelineId, &req.id)?;
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

    let is_scheduled = req.options.schedule.is_some();
    if is_scheduled {
        storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: pipeline.id.clone(),
                desired_state: storage::StoredPipelineDesiredState::ScheduledStopped,
            })
            .map_err(|e| e.to_string())?;
    }

    if let Some(failure) = storage
        .get_pipeline_runtime_failure(&pipeline.id)
        .map_err(|e| e.to_string())?
        .filter(|failure| failure.revision == pipeline.revision)
    {
        instance
            .mark_pipeline_failed(&pipeline.id)
            .map_err(|e| e.to_string())?;
        tracing::warn!(
            pipeline_id = %pipeline.id,
            revision = pipeline.revision,
            processor_id = %failure.processor_id,
            processor_kind = %failure.processor_kind,
            reason = %failure.reason,
            "skipping pipeline auto-start because it has a persisted runtime failure"
        );
        return Ok(());
    }

    if is_scheduled {
        return Ok(());
    }

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
            if let Err(err) = instance.start_pipeline(&pipeline.id).await {
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
    let tables = storage.list_tables().map_err(|e| e.to_string())?;

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
        persisted_table_count = tables.len(),
        instance_count = instances_snapshot.len(),
        "storage hydrate discovered persisted resources"
    );

    let mut memory_topic_restore_failures = 0usize;
    let mut shared_mqtt_restore_failures = 0usize;
    let mut stream_restore_failures = 0usize;
    let mut table_restore_failures = 0usize;
    let udf_count = storage.list_udfs().map_err(|e| e.to_string())?.len();

    for (_, instance) in instances_snapshot {
        let summary = hydrate_instance_globals_from_storage(storage, instance.as_ref()).await?;
        memory_topic_restore_failures += summary.memory_topic_failures;
        shared_mqtt_restore_failures += summary.shared_mqtt_failures;
        stream_restore_failures += summary.stream_failures;
        table_restore_failures += summary.table_failures;
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
        persisted_table_count = tables.len(),
        memory_topic_restore_failures,
        shared_mqtt_restore_failures,
        stream_restore_failures,
        table_restore_failures,
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

    #[test]
    fn stored_shared_mqtt_config_without_protocol_defaults_to_v3() {
        let stored = StoredMqttClientConfig {
            key: "legacy".to_string(),
            revision: 1,
            raw_json: serde_json::json!({
                "key": "legacy",
                "broker_url": "tcp://127.0.0.1:1883",
                "topic": "in",
                "client_id": "legacy-client",
                "qos": 0,
                "max_packet_size": null
            })
            .to_string(),
        };

        let config = mqtt_config_from_stored(&stored);
        assert_eq!(config.protocol_version, flow::MqttProtocolVersion::V3);
    }

    #[test]
    fn stored_shared_mqtt_config_roundtrip_preserves_v5() {
        let config = SharedMqttClientConfig {
            key: "mqtt_v5".to_string(),
            broker_url: "tcp://127.0.0.1:1883".to_string(),
            topic: "in".to_string(),
            client_id: "mqtt-v5-client".to_string(),
            qos: 1,
            max_packet_size: None,
            protocol_version: flow::MqttProtocolVersion::V5,
            username: None,
            password: None,
            resolved_password: None,
        };

        let stored = stored_mqtt_from_config(&config, 7);
        let decoded = mqtt_config_from_stored(&stored);
        assert_eq!(decoded.protocol_version, flow::MqttProtocolVersion::V5);
        assert_eq!(stored.revision, 7);
    }

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
            revision: 1,
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
            "revision": 1,
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

    #[test]
    fn stored_pipeline_roundtrip_preserves_datetime_range_only_schedule() {
        let mut request = sample_pipeline_request("range_pipe", "range_stream");
        request.options.schedule = Some(
            serde_json::from_value(json!({
                "datetime_ranges": [{
                    "begin_timestamp_ms": 1_000,
                    "end_timestamp_ms": 2_000
                }]
            }))
            .expect("deserialize range-only schedule"),
        );

        let stored = stored_pipeline_from_request(&request).expect("serialize pipeline");
        let decoded = pipeline_request_from_stored(&stored).expect("deserialize pipeline");
        let schedule = decoded.options.schedule.expect("schedule exists");

        assert_eq!(schedule.cron, None);
        assert_eq!(schedule.duration_secs, None);
        assert_eq!(schedule.datetime_ranges.len(), 1);
        assert_eq!(schedule.datetime_ranges[0].begin_timestamp_ms, 1_000);
        assert_eq!(schedule.datetime_ranges[0].end_timestamp_ms, 2_000);
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
            revision: 1,
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
                revision: 1,
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

    #[tokio::test]
    async fn hydrate_running_pipeline_with_failure_marker_restores_failed_without_starting() {
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
            .expect("store stream");
        let pipeline_req = sample_pipeline_request("failed_pipe", "good_stream");
        let stored_pipeline =
            stored_pipeline_from_request(&pipeline_req).expect("serialize pipeline");
        storage
            .create_pipeline(stored_pipeline.clone())
            .expect("store pipeline");
        storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: stored_pipeline.id.clone(),
                desired_state: storage::StoredPipelineDesiredState::Running,
            })
            .expect("store run state");
        storage
            .put_pipeline_runtime_failure(storage::StoredPipelineRuntimeFailure {
                pipeline_id: stored_pipeline.id.clone(),
                revision: stored_pipeline.revision,
                failed_at_ms: 1234,
                processor_id: "PhysicalFilter_1".to_string(),
                processor_kind: "filter".to_string(),
                reason: "processor task failed".to_string(),
            })
            .expect("store failure marker");

        hydrate_runtime_from_storage(&storage, &instances)
            .await
            .expect("hydrate runtime from storage");

        let snapshots = instances.default_instance().list_pipelines();
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.definition.id() == "failed_pipe")
            .expect("failed pipeline restored");
        assert_eq!(snapshot.status, flow::pipeline::PipelineStatus::Failed);
    }

    #[tokio::test]
    async fn hydrate_scheduled_pipeline_resets_previous_running_state() {
        let dir = tempdir().unwrap();
        let storage = StorageManager::new(dir.path()).unwrap();
        let instance = FlowInstance::new(
            flow::instance::FlowInstanceOptions::shared_current_runtime("default", None),
        )
        .expect("create flow instance");
        let instances = FlowInstances::new(instance);

        let stream_req = sample_stream_request("scheduled_stream");
        storage
            .create_stream(stored_stream_from_request(&stream_req).expect("serialize stream"))
            .expect("store stream");
        let mut pipeline_req = sample_pipeline_request("scheduled_pipe", "scheduled_stream");
        pipeline_req.options.schedule = Some(
            serde_json::from_value(json!({
                "datetime_ranges": [{
                    "begin_timestamp_ms": 1_000,
                    "end_timestamp_ms": 2_000
                }]
            }))
            .expect("deserialize schedule"),
        );
        let stored_pipeline =
            stored_pipeline_from_request(&pipeline_req).expect("serialize pipeline");
        storage
            .create_pipeline(stored_pipeline.clone())
            .expect("store pipeline");
        storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: stored_pipeline.id.clone(),
                desired_state: storage::StoredPipelineDesiredState::ScheduledRunning,
            })
            .expect("store scheduled running state");

        hydrate_runtime_from_storage(&storage, &instances)
            .await
            .expect("hydrate runtime from storage");

        let run_state = storage
            .get_pipeline_run_state("scheduled_pipe")
            .expect("read run state")
            .expect("run state exists");
        assert_eq!(
            run_state.desired_state,
            storage::StoredPipelineDesiredState::ScheduledStopped
        );
        let snapshot = instances
            .default_instance()
            .list_pipelines()
            .into_iter()
            .find(|snapshot| snapshot.definition.id() == "scheduled_pipe")
            .expect("scheduled pipeline restored");
        assert_eq!(snapshot.status, flow::pipeline::PipelineStatus::Stopped);
    }
}
