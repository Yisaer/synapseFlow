use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::aggregation::AggregateFunctionRegistry;
use crate::catalog::{Catalog, CatalogError, StreamDefinition, StreamProps};
use crate::codec::{CodecError, DecoderRegistry, EncoderRegistry};
use crate::connector::{
    ConnectorError, ConnectorRegistry, MqttClientManager, SharedMqttClientConfig,
};
use crate::pipeline::{PipelineDefinition, PipelineError, PipelineManager, PipelineSnapshot};
use crate::processor::ProcessorPipeline;
use crate::shared_stream::{
    registry as shared_stream_registry, SharedStreamConfig, SharedStreamError, SharedStreamInfo,
    SharedStreamRegistry,
};
use crate::{create_pipeline, create_pipeline_with_log_sink};
use crate::{PipelineExplain, PipelineRegistries, PipelineSink};

/// Runtime container that manages all Flow resources (streams, pipelines, shared clients).
#[derive(Clone)]
pub struct FlowInstance {
    catalog: Arc<Catalog>,
    shared_stream_registry: &'static SharedStreamRegistry,
    pipeline_manager: Arc<PipelineManager>,
    // In-memory registry of shared MQTT client configs for discovery/listing.
    shared_mqtt_client_configs: Arc<Mutex<HashMap<String, SharedMqttClientConfig>>>,
    mqtt_client_manager: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
}

impl FlowInstance {
    /// Create a new Flow instance backed by global registries.
    pub fn new() -> Self {
        let catalog = Arc::new(Catalog::new());
        let shared_stream_registry = shared_stream_registry();
        let mqtt_client_manager = MqttClientManager::new();
        let connector_registry = ConnectorRegistry::with_builtin_sinks();
        let encoder_registry = EncoderRegistry::with_builtin_encoders();
        let decoder_registry = DecoderRegistry::with_builtin_decoders();
        let aggregate_registry = AggregateFunctionRegistry::with_builtins();
        let pipeline_manager = Arc::new(PipelineManager::new(
            Arc::clone(&catalog),
            shared_stream_registry,
            mqtt_client_manager.clone(),
            Arc::clone(&connector_registry),
            Arc::clone(&decoder_registry),
            Arc::clone(&encoder_registry),
            Arc::clone(&aggregate_registry),
        ));
        Self {
            catalog,
            shared_stream_registry,
            pipeline_manager,
            shared_mqtt_client_configs: Arc::new(Mutex::new(HashMap::new())),
            mqtt_client_manager,
            connector_registry,
            encoder_registry,
            decoder_registry,
            aggregate_registry,
        }
    }

    /// Create a stream definition and optionally attach a shared stream runtime.
    pub async fn create_stream(
        &self,
        definition: StreamDefinition,
        shared: bool,
    ) -> Result<StreamRuntimeInfo, FlowInstanceError> {
        let stored = self.catalog.insert(definition)?;
        let shared_info = if shared {
            match self.ensure_shared_stream(stored.clone()).await {
                Ok(info) => Some(info),
                Err(err) => {
                    let _ = self.catalog.remove(stored.id());
                    return Err(err);
                }
            }
        } else {
            None
        };
        Ok(StreamRuntimeInfo {
            definition: stored,
            shared_info,
        })
    }

    /// Retrieve a stream definition and its shared runtime (if any).
    pub async fn get_stream(&self, name: &str) -> Result<StreamRuntimeInfo, FlowInstanceError> {
        let definition = self
            .catalog
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        let shared_info = match self.shared_stream_registry.get_stream(name).await {
            Ok(info) => Some(info),
            Err(SharedStreamError::NotFound(_)) => None,
            Err(err) => return Err(err.into()),
        };
        Ok(StreamRuntimeInfo {
            definition,
            shared_info,
        })
    }

    /// List all streams with their shared runtime metadata.
    pub async fn list_streams(&self) -> Result<Vec<StreamRuntimeInfo>, FlowInstanceError> {
        let shared_infos = self.shared_stream_registry.list_streams().await;
        let shared_map: HashMap<String, SharedStreamInfo> = shared_infos
            .into_iter()
            .map(|info| (info.name.clone(), info))
            .collect();

        let mut payload = Vec::new();
        for definition in self.catalog.list() {
            let shared_info = shared_map.get(definition.id()).cloned();
            payload.push(StreamRuntimeInfo {
                definition,
                shared_info,
            });
        }
        Ok(payload)
    }

    /// Delete a stream definition and its shared runtime (if registered).
    pub async fn delete_stream(&self, name: &str) -> Result<(), FlowInstanceError> {
        if self.shared_stream_registry.is_registered(name).await {
            self.shared_stream_registry.drop_stream(name).await?;
        }
        self.catalog.remove(name)?;
        Ok(())
    }

    /// Register a shared MQTT client that can be referenced by connector keys.
    pub async fn create_shared_mqtt_client(
        &self,
        config: SharedMqttClientConfig,
    ) -> Result<(), FlowInstanceError> {
        self.mqtt_client_manager
            .create_client(config.clone())
            .await?;
        self.shared_mqtt_client_configs
            .lock()
            .expect("shared mqtt map poisoned")
            .insert(config.key.clone(), config);
        Ok(())
    }

    /// Drop a shared MQTT client identified by key.
    pub fn drop_shared_mqtt_client(&self, key: &str) -> Result<(), FlowInstanceError> {
        self.mqtt_client_manager.drop_client(key)?;
        self.shared_mqtt_client_configs
            .lock()
            .expect("shared mqtt map poisoned")
            .remove(key);
        Ok(())
    }

    /// List metadata for registered shared MQTT clients.
    pub fn list_shared_mqtt_clients(&self) -> Vec<SharedMqttClientConfig> {
        self.shared_mqtt_client_configs
            .lock()
            .expect("shared mqtt map poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Fetch metadata for a single shared MQTT client.
    pub fn get_shared_mqtt_client(&self, key: &str) -> Option<SharedMqttClientConfig> {
        self.shared_mqtt_client_configs
            .lock()
            .expect("shared mqtt map poisoned")
            .get(key)
            .cloned()
    }

    /// Create a pipeline runtime from definition.
    pub fn create_pipeline(
        &self,
        definition: PipelineDefinition,
    ) -> Result<PipelineSnapshot, PipelineError> {
        self.pipeline_manager.create_pipeline(definition)
    }

    /// Start a pipeline by identifier.
    pub fn start_pipeline(&self, id: &str) -> Result<(), PipelineError> {
        self.pipeline_manager.start_pipeline(id)
    }

    /// Stop and delete a pipeline.
    pub async fn delete_pipeline(&self, id: &str) -> Result<(), PipelineError> {
        self.pipeline_manager.delete_pipeline(id).await
    }

    /// Retrieve pipeline snapshots.
    pub fn list_pipelines(&self) -> Vec<PipelineSnapshot> {
        self.pipeline_manager.list()
    }

    /// Explain an existing pipeline by id (logical + physical plans).
    pub fn explain_pipeline(&self, id: &str) -> Result<PipelineExplain, PipelineError> {
        self.pipeline_manager.explain_pipeline(id)
    }

    /// Build a processor pipeline directly without registering it.
    pub fn build_pipeline(
        &self,
        sql: &str,
        sinks: Vec<PipelineSink>,
    ) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
        let registries = self.pipeline_registries();
        create_pipeline(
            sql,
            sinks,
            &self.catalog,
            self.shared_stream_registry,
            self.mqtt_client_manager.clone(),
            &registries,
        )
    }

    /// Build a processor pipeline wired to a default logging sink.
    pub fn build_pipeline_with_log_sink(
        &self,
        sql: &str,
        forward_to_result: bool,
    ) -> Result<ProcessorPipeline, Box<dyn std::error::Error>> {
        let registries = self.pipeline_registries();
        create_pipeline_with_log_sink(
            sql,
            forward_to_result,
            &self.catalog,
            self.shared_stream_registry,
            self.mqtt_client_manager.clone(),
            &registries,
        )
    }

    pub fn connector_registry(&self) -> Arc<ConnectorRegistry> {
        Arc::clone(&self.connector_registry)
    }

    pub fn encoder_registry(&self) -> Arc<EncoderRegistry> {
        Arc::clone(&self.encoder_registry)
    }

    pub fn decoder_registry(&self) -> Arc<DecoderRegistry> {
        Arc::clone(&self.decoder_registry)
    }

    pub fn aggregate_registry(&self) -> Arc<AggregateFunctionRegistry> {
        Arc::clone(&self.aggregate_registry)
    }

    fn pipeline_registries(&self) -> PipelineRegistries {
        PipelineRegistries::new(
            Arc::clone(&self.connector_registry),
            Arc::clone(&self.encoder_registry),
            Arc::clone(&self.decoder_registry),
            Arc::clone(&self.aggregate_registry),
        )
    }

    async fn ensure_shared_stream(
        &self,
        definition: Arc<StreamDefinition>,
    ) -> Result<SharedStreamInfo, FlowInstanceError> {
        match definition.props() {
            StreamProps::Mqtt(props) => {
                let mut config = SharedStreamConfig::new(definition.id(), definition.schema());
                struct MqttSharedStreamConnectorFactory {
                    stream_id: String,
                    schema: Arc<datatypes::Schema>,
                    decoder: crate::catalog::StreamDecoderConfig,
                    broker_url: String,
                    topic: String,
                    qos: u8,
                    client_id: Option<String>,
                    connector_key: Option<String>,
                    mqtt_client_manager: crate::connector::MqttClientManager,
                    decoder_registry: Arc<crate::codec::DecoderRegistry>,
                }

                impl crate::shared_stream::SharedStreamConnectorFactory
                    for MqttSharedStreamConnectorFactory
                {
                    fn connector_id(&self) -> String {
                        format!("{}_shared_source_connector", self.stream_id)
                    }

                    fn build(
                        &self,
                    ) -> Result<
                        (Box<dyn crate::connector::SourceConnector>, Arc<dyn crate::codec::RecordDecoder>),
                        crate::shared_stream::SharedStreamError,
                    > {
                        let mut source_config = crate::connector::MqttSourceConfig::new(
                            format!("{}_shared_source", self.stream_id),
                            self.broker_url.clone(),
                            self.topic.clone(),
                            self.qos,
                        );
                        if let Some(client_id) = &self.client_id {
                            source_config = source_config.with_client_id(client_id.clone());
                        }
                        if let Some(connector_key) = &self.connector_key {
                            source_config = source_config.with_connector_key(connector_key.clone());
                        }
                        let connector = crate::connector::MqttSourceConnector::new(
                            self.connector_id(),
                            source_config,
                            self.mqtt_client_manager.clone(),
                        );
                        let decoder = self.decoder_registry.instantiate(
                            &self.decoder,
                            &self.stream_id,
                            Arc::clone(&self.schema),
                        );
                        let decoder = decoder.map_err(|err| {
                            crate::shared_stream::SharedStreamError::Internal(err.to_string())
                        })?;
                        Ok((Box::new(connector), decoder))
                    }
                }

                let factory = Arc::new(MqttSharedStreamConnectorFactory {
                    stream_id: definition.id().to_string(),
                    schema: definition.schema(),
                    decoder: definition.decoder().clone(),
                    broker_url: props.broker_url.clone(),
                    topic: props.topic.clone(),
                    qos: props.qos,
                    client_id: props.client_id.clone(),
                    connector_key: props.connector_key.clone(),
                    mqtt_client_manager: self.mqtt_client_manager.clone(),
                    decoder_registry: Arc::clone(&self.decoder_registry),
                });

                config.set_connector_factory(factory);
                self.shared_stream_registry
                    .create_stream(config)
                    .await
                    .map_err(FlowInstanceError::from)
            }
            StreamProps::Mock(_) => Err(FlowInstanceError::Invalid(
                "mock stream props cannot be used to create shared streams".to_string(),
            )),
        }
    }
}

impl Default for FlowInstance {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined runtime view for a catalog stream and its shared stream state.
#[derive(Clone)]
pub struct StreamRuntimeInfo {
    pub definition: Arc<StreamDefinition>,
    pub shared_info: Option<SharedStreamInfo>,
}

/// Errors surfaced by FlowInstance APIs.
#[derive(thiserror::Error, Debug)]
pub enum FlowInstanceError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    SharedStream(#[from] SharedStreamError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("{0}")]
    Invalid(String),
}
