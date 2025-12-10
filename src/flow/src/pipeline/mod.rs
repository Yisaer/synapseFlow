use crate::catalog::{Catalog, StreamDefinition, StreamProps};
use crate::codec::{DecoderRegistry, EncoderRegistry};
use crate::connector::{
    ConnectorRegistry, MqttClientManager, MqttSinkConfig, MqttSourceConfig, MqttSourceConnector,
};
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use crate::processor::processor_builder::{PlanProcessor, ProcessorPipeline};
use crate::processor::Processor;
use crate::shared_stream::SharedStreamRegistry;
use crate::{
    create_pipeline, PipelineRegistries, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
};
use parser::parse_sql;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const DEFAULT_SINK_TOPIC: &str = "/yisa/data2";

/// Errors that can occur when mutating pipeline definitions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PipelineError {
    #[error("pipeline already exists: {0}")]
    AlreadyExists(String),
    #[error("pipeline not found: {0}")]
    NotFound(String),
    #[error("pipeline build error: {0}")]
    BuildFailure(String),
    #[error("pipeline runtime error: {0}")]
    Runtime(String),
}

/// Supported sink types for pipeline outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    /// MQTT sink.
    Mqtt,
}

/// Sink configuration payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkProps {
    /// MQTT sink configuration.
    Mqtt(MqttSinkProps),
}

/// Runtime state for pipeline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Created,
    Running,
}

/// Concrete MQTT sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSinkProps {
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

impl MqttSinkProps {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>, qos: u8) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
            connector_key: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    pub fn with_connector_key(mut self, connector_key: impl Into<String>) -> Self {
        self.connector_key = Some(connector_key.into());
        self
    }
}

/// Sink definition for a pipeline.
#[derive(Debug, Clone)]
pub struct SinkDefinition {
    pub sink_id: String,
    pub sink_type: SinkType,
    pub props: SinkProps,
    pub common: CommonSinkProps,
    pub encoder: SinkEncoderConfig,
}

impl SinkDefinition {
    pub fn new(sink_id: impl Into<String>, sink_type: SinkType, props: SinkProps) -> Self {
        let sink_id_str = sink_id.into();
        Self {
            sink_id: sink_id_str.clone(),
            sink_type,
            props,
            common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
        }
    }

    pub fn with_common_props(mut self, common: CommonSinkProps) -> Self {
        self.common = common;
        self
    }

    pub fn with_encoder(mut self, encoder: SinkEncoderConfig) -> Self {
        self.encoder = encoder;
        self
    }
}

/// Pipeline definition referencing SQL + sinks.
#[derive(Debug, Clone)]
pub struct PipelineDefinition {
    id: String,
    sql: String,
    sinks: Vec<SinkDefinition>,
}

impl PipelineDefinition {
    pub fn new(id: impl Into<String>, sql: impl Into<String>, sinks: Vec<SinkDefinition>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            sinks,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn sinks(&self) -> &[SinkDefinition] {
        &self.sinks
    }
}

struct ManagedPipeline {
    definition: Arc<PipelineDefinition>,
    pipeline: ProcessorPipeline,
    streams: Vec<String>,
    status: PipelineStatus,
}

impl ManagedPipeline {
    fn snapshot(&self) -> PipelineSnapshot {
        PipelineSnapshot {
            definition: Arc::clone(&self.definition),
            streams: self.streams.clone(),
            status: self.status,
        }
    }
}

/// User-facing view of a pipeline entry.
#[derive(Clone)]
pub struct PipelineSnapshot {
    pub definition: Arc<PipelineDefinition>,
    pub streams: Vec<String>,
    pub status: PipelineStatus,
}

/// Stores all registered pipelines and manages their lifecycle.
pub struct PipelineManager {
    pipelines: RwLock<HashMap<String, ManagedPipeline>>,
    catalog: Arc<Catalog>,
    shared_stream_registry: &'static SharedStreamRegistry,
    mqtt_client_manager: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
}

impl PipelineManager {
    pub fn new(
        catalog: Arc<Catalog>,
        shared_stream_registry: &'static SharedStreamRegistry,
        mqtt_client_manager: MqttClientManager,
        connector_registry: Arc<ConnectorRegistry>,
        decoder_registry: Arc<DecoderRegistry>,
        encoder_registry: Arc<EncoderRegistry>,
    ) -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
            catalog,
            shared_stream_registry,
            mqtt_client_manager,
            connector_registry,
            encoder_registry,
            decoder_registry,
        }
    }

    /// Create a pipeline runtime from definition and store it.
    pub fn create_pipeline(
        &self,
        definition: PipelineDefinition,
    ) -> Result<PipelineSnapshot, PipelineError> {
        let pipeline_id = definition.id().to_string();
        let registries = PipelineRegistries::new(
            Arc::clone(&self.connector_registry),
            Arc::clone(&self.encoder_registry),
            Arc::clone(&self.decoder_registry),
        );
        let (pipeline, streams) = build_pipeline_runtime(
            &definition,
            &self.catalog,
            self.shared_stream_registry,
            &self.mqtt_client_manager,
            &registries,
        )
        .map_err(PipelineError::BuildFailure)?;
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
        if guard.contains_key(&pipeline_id) {
            return Err(PipelineError::AlreadyExists(pipeline_id));
        }
        let entry = ManagedPipeline {
            definition: Arc::new(definition),
            pipeline,
            streams,
            status: PipelineStatus::Created,
        };
        let snapshot = entry.snapshot();
        guard.insert(pipeline_id, entry);
        Ok(snapshot)
    }

    /// Retrieve a snapshot for a specific pipeline.
    pub fn get(&self, pipeline_id: &str) -> Option<PipelineSnapshot> {
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
        guard.get(pipeline_id).map(|entry| entry.snapshot())
    }

    /// List all managed pipelines.
    pub fn list(&self) -> Vec<PipelineSnapshot> {
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
        guard.values().map(|entry| entry.snapshot()).collect()
    }

    /// Start the pipeline runtime if not already running.
    pub fn start_pipeline(&self, pipeline_id: &str) -> Result<(), PipelineError> {
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
        let entry = guard
            .get_mut(pipeline_id)
            .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
        if matches!(entry.status, PipelineStatus::Running) {
            return Ok(());
        }
        entry.pipeline.start();
        entry.status = PipelineStatus::Running;
        Ok(())
    }

    /// Remove a pipeline runtime and close it if running.
    pub async fn delete_pipeline(&self, pipeline_id: &str) -> Result<(), PipelineError> {
        let maybe_entry = {
            let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
            guard.remove(pipeline_id)
        };
        let entry = maybe_entry.ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))?;
        if matches!(entry.status, PipelineStatus::Running) {
            let pipeline_id = entry.definition.id().to_string();
            tokio::spawn(async move {
                if let Err(err) = close_pipeline(entry.pipeline).await {
                    eprintln!(
                        "[PipelineManager] failed to close pipeline {}: {err}",
                        pipeline_id
                    );
                }
            });
        }
        Ok(())
    }
}

async fn close_pipeline(mut pipeline: ProcessorPipeline) -> Result<(), PipelineError> {
    pipeline
        .graceful_close()
        .await
        .map_err(|err| PipelineError::Runtime(err.to_string()))
}

fn build_pipeline_runtime(
    definition: &PipelineDefinition,
    catalog: &Catalog,
    shared_stream_registry: &SharedStreamRegistry,
    mqtt_client_manager: &MqttClientManager,
    registries: &PipelineRegistries,
) -> Result<(ProcessorPipeline, Vec<String>), String> {
    let select_stmt = parse_sql(definition.sql()).map_err(|err| err.to_string())?;
    let streams: Vec<String> = select_stmt
        .source_infos
        .iter()
        .map(|info| info.name.clone())
        .collect();
    let mut stream_definitions = HashMap::new();
    for stream in &streams {
        let definition = catalog
            .get(stream)
            .ok_or_else(|| format!("stream {stream} not found in catalog"))?;
        stream_definitions.insert(stream.clone(), definition);
    }

    let sinks = build_sinks_from_definition(definition)?;
    let mut pipeline = create_pipeline(
        definition.sql(),
        sinks,
        catalog,
        shared_stream_registry,
        mqtt_client_manager.clone(),
        registries,
    )
    .map_err(|err| err.to_string())?;
    attach_sources_from_catalog(&mut pipeline, &stream_definitions, mqtt_client_manager)?;
    pipeline.set_pipeline_id(definition.id().to_string());
    Ok((pipeline, streams))
}

fn build_sinks_from_definition(
    definition: &PipelineDefinition,
) -> Result<Vec<PipelineSink>, String> {
    let mut sinks = Vec::with_capacity(definition.sinks().len());
    for sink in definition.sinks() {
        match sink.sink_type {
            SinkType::Mqtt => {
                let SinkProps::Mqtt(props) = &sink.props;
                let mut config = MqttSinkConfig::new(
                    sink.sink_id.clone(),
                    props.broker_url.clone(),
                    if props.topic.is_empty() {
                        DEFAULT_SINK_TOPIC.to_string()
                    } else {
                        props.topic.clone()
                    },
                    props.qos,
                );
                config = config.with_retain(props.retain);
                if let Some(client_id) = &props.client_id {
                    config = config.with_client_id(client_id.clone());
                }
                if let Some(conn_key) = &props.connector_key {
                    config = config.with_connector_key(conn_key.clone());
                }
                let connector = PipelineSinkConnector::new(
                    sink.sink_id.clone(),
                    SinkConnectorConfig::Mqtt(config),
                    sink.encoder.clone(),
                );
                let pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
                    .with_common_props(sink.common.clone());
                sinks.push(pipeline_sink);
            }
        }
    }
    Ok(sinks)
}

fn attach_sources_from_catalog(
    pipeline: &mut ProcessorPipeline,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    mqtt_client_manager: &MqttClientManager,
) -> Result<(), String> {
    let mut attached = false;
    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let stream_name = ds.stream_name().to_string();
            let definition = stream_defs.get(&stream_name).ok_or_else(|| {
                format!("stream {stream_name} missing definition when attaching sources")
            })?;
            let StreamProps::Mqtt(stream_props) = definition.props();
            let processor_id = ds.id().to_string();
            let mut config = MqttSourceConfig::new(
                processor_id.clone(),
                stream_props.broker_url.clone(),
                stream_props.topic.clone(),
                stream_props.qos,
            );
            if let Some(client_id) = &stream_props.client_id {
                config = config.with_client_id(client_id.clone());
            }
            if let Some(connector_key) = &stream_props.connector_key {
                config = config.with_connector_key(connector_key.clone());
            }
            let connector = MqttSourceConnector::new(
                format!("{processor_id}_source_connector"),
                config,
                mqtt_client_manager.clone(),
            );
            ds.add_connector(Box::new(connector));
            attached = true;
        }
    }

    if attached {
        Ok(())
    } else {
        Err("no datasource processors available to attach connectors".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        Catalog, MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
    };
    use crate::codec::EncoderRegistry;
    use crate::connector::{ConnectorRegistry, MqttClientManager};
    use crate::shared_stream_registry;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use std::sync::Arc;
    use tokio::runtime::Runtime;

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
        let connector_registry = ConnectorRegistry::with_builtin_sinks();
        let encoder_registry = EncoderRegistry::with_builtin_encoders();
        let decoder_registry = DecoderRegistry::with_builtin_decoders();
        install_stream(&catalog, "test_stream");
        let manager = PipelineManager::new(
            Arc::clone(&catalog),
            registry,
            mqtt_manager.clone(),
            Arc::clone(&connector_registry),
            Arc::clone(&decoder_registry),
            Arc::clone(&encoder_registry),
        );
        let snapshot = manager
            .create_pipeline(sample_pipeline("pipe_a", "test_stream"))
            .expect("create pipeline");
        assert_eq!(snapshot.status, PipelineStatus::Created);
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
        let connector_registry = ConnectorRegistry::with_builtin_sinks();
        let encoder_registry = EncoderRegistry::with_builtin_encoders();
        let decoder_registry = DecoderRegistry::with_builtin_decoders();
        install_stream(&catalog, "dup_stream");
        let manager = PipelineManager::new(
            Arc::clone(&catalog),
            registry,
            mqtt_manager.clone(),
            connector_registry,
            decoder_registry,
            encoder_registry,
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
}
