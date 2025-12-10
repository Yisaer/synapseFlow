use crate::pipeline::{CreatePipelineRequest, build_pipeline_definition};
use crate::stream::{
    CreateStreamRequest, build_schema_from_request, build_stream_decoder, build_stream_props,
};
use flow::catalog::StreamDefinition;
use flow::connector::SharedMqttClientConfig;
use flow::pipeline::PipelineDefinition;
use flow::{DecoderRegistry, EncoderRegistry};
use std::sync::Arc;
use storage::{StorageManager, StoredMqttClientConfig, StoredPipeline, StoredStream};

/// Serialize a create-stream request for storage.
pub fn stored_stream_from_request(req: &CreateStreamRequest) -> Result<StoredStream, String> {
    let raw_json =
        serde_json::to_string(req).map_err(|err| format!("serialize stream request: {err}"))?;
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
    Ok(StreamDefinition::new(
        req.name.clone(),
        Arc::new(schema),
        props,
        decoder,
    ))
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
) -> Result<PipelineDefinition, String> {
    let req: CreatePipelineRequest = serde_json::from_str(&stored.raw_json)
        .map_err(|err| format!("decode stored pipeline {}: {err}", stored.id))?;
    build_pipeline_definition(&req, encoder_registry)
}

pub fn mqtt_config_from_stored(stored: &StoredMqttClientConfig) -> SharedMqttClientConfig {
    serde_json::from_str(&stored.raw_json).unwrap_or_else(|_| SharedMqttClientConfig {
        key: stored.key.clone(),
        broker_url: String::new(),
        topic: String::new(),
        client_id: String::new(),
        qos: 0,
    })
}

pub fn stored_mqtt_from_config(cfg: &SharedMqttClientConfig) -> StoredMqttClientConfig {
    let raw_json = serde_json::to_string(cfg).unwrap_or_default();
    StoredMqttClientConfig {
        key: cfg.key.clone(),
        raw_json,
    }
}

/// Load persisted resources into the running FlowInstance.
pub async fn load_from_storage(
    storage: &StorageManager,
    instance: &flow::FlowInstance,
) -> Result<(), String> {
    let encoder_registry = instance.encoder_registry();
    let decoder_registry = instance.decoder_registry();
    for cfg in storage.list_mqtt_configs().map_err(|e| e.to_string())? {
        instance
            .create_shared_mqtt_client(mqtt_config_from_stored(&cfg))
            .await
            .map_err(|e| e.to_string())?;
    }

    for stream in storage.list_streams().map_err(|e| e.to_string())? {
        let def = stream_definition_from_stored(&stream, decoder_registry.as_ref())?;
        let shared = serde_json::from_str::<CreateStreamRequest>(&stream.raw_json)
            .map_err(|err| format!("decode stored stream {}: {err}", stream.id))?
            .shared;
        instance
            .create_stream(def, shared)
            .await
            .map_err(|e| e.to_string())?;
    }

    for pipeline in storage.list_pipelines().map_err(|e| e.to_string())? {
        let def = pipeline_definition_from_stored(&pipeline, encoder_registry.as_ref())?;
        instance.create_pipeline(def).map_err(|e| e.to_string())?;
    }
    Ok(())
}
