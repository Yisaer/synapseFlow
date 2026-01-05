use crate::aggregation::AggregateFunctionRegistry;
use crate::catalog::{Catalog, CatalogError, StreamDefinition};
use crate::codec::{CodecError, DecoderRegistry, EncoderRegistry};
use crate::connector::{
    ConnectorError, ConnectorRegistry, MqttClientManager, SharedMqttClientConfig,
};
use crate::eventtime::EventtimeTypeRegistry;
use crate::expr::custom_func::CustomFuncRegistry;
use crate::pipeline::PipelineManager;
use crate::shared_stream::{
    registry as shared_stream_registry, SharedStreamError, SharedStreamInfo, SharedStreamRegistry,
};
use crate::stateful::StatefulFunctionRegistry;
use crate::PipelineRegistries;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod mqtt;
mod pipelines;
mod registries;
mod streams;

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
    stateful_registry: Arc<StatefulFunctionRegistry>,
    custom_func_registry: Arc<CustomFuncRegistry>,
    eventtime_type_registry: Arc<EventtimeTypeRegistry>,
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
        let stateful_registry = StatefulFunctionRegistry::with_builtins();
        let custom_func_registry = CustomFuncRegistry::with_builtins();
        let eventtime_type_registry = EventtimeTypeRegistry::with_builtin_types();
        let registries = PipelineRegistries::new(
            Arc::clone(&connector_registry),
            Arc::clone(&encoder_registry),
            Arc::clone(&decoder_registry),
            Arc::clone(&aggregate_registry),
            Arc::clone(&stateful_registry),
            Arc::clone(&custom_func_registry),
            Arc::clone(&eventtime_type_registry),
        );
        let pipeline_manager = Arc::new(PipelineManager::new(
            Arc::clone(&catalog),
            shared_stream_registry,
            mqtt_client_manager.clone(),
            registries,
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
            stateful_registry,
            custom_func_registry,
            eventtime_type_registry,
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
