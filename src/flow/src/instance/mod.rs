use crate::aggregation::AggregateFunctionRegistry;
use crate::catalog::{Catalog, CatalogError, StreamDefinition};
use crate::codec::{CodecError, DecoderRegistry, EncoderRegistry, MergerRegistry};
use crate::connector::{
    ConnectorError, ConnectorRegistry, MemoryData, MemoryPubSubError, MemoryPubSubRegistry,
    MemoryPublisher, MemoryTopicKind, MockSourceHandle, MqttClientManager, SharedMqttClientConfig,
    SinkConnector,
};
use crate::eventtime::EventtimeTypeRegistry;
use crate::expr::custom_func::CustomFuncRegistry;
use crate::pipeline::PipelineManager;
use crate::planner::sink::SinkConnectorConfig;
use crate::runtime::TaskSpawner;
use crate::secret::SecretContext;
use crate::shared_stream::{SharedStreamError, SharedStreamInfo, SharedStreamRegistry};
use crate::stateful::StatefulFunctionRegistry;
use crate::PipelineRegistries;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

use parking_lot::Mutex;

mod cpu_metrics;
mod mqtt;
mod pipelines;
mod registries;
mod streams;

use cpu_metrics::{
    build_flow_instance_cpu_metrics_state, register_flow_instance_cpu_metrics_state,
    sample_flow_instance_cpu_usage_percent, FlowInstanceCpuMetricsState, RuntimeThreadRegistry,
};

/// Registries that can be shared across multiple [`FlowInstance`]s.
///
/// Instance-scoped runtime resources (shared stream runtime, MQTT clients, memory pub/sub) remain
/// per-instance. These registries only hold definitions/factories and are safe to share to keep
/// custom registrations consistent across instances.
#[derive(Clone)]
pub struct FlowInstanceSharedRegistries {
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    stateful_registry: Arc<StatefulFunctionRegistry>,
    custom_func_registry: Arc<CustomFuncRegistry>,
    eventtime_type_registry: Arc<EventtimeTypeRegistry>,
    merger_registry: Arc<MergerRegistry>,
}

#[derive(Clone)]
pub enum FlowInstanceRuntimeOptions {
    SharedCurrent,
    Dedicated(FlowInstanceDedicatedRuntimeOptions),
}

#[derive(Clone, Default)]
pub struct FlowInstanceDedicatedRuntimeOptions {
    pub worker_threads: Option<usize>,
    pub thread_name_prefix: Option<String>,
    pub thread_cgroup_path: Option<String>,
}

#[derive(Clone)]
pub struct FlowInstanceOptions {
    pub id: String,
    pub shared_registries: Option<FlowInstanceSharedRegistries>,
    pub runtime: FlowInstanceRuntimeOptions,
}

impl FlowInstanceOptions {
    pub fn shared_current_runtime(
        id: impl Into<String>,
        shared_registries: Option<FlowInstanceSharedRegistries>,
    ) -> Self {
        Self {
            id: id.into(),
            shared_registries,
            runtime: FlowInstanceRuntimeOptions::SharedCurrent,
        }
    }

    pub fn dedicated_runtime(
        id: impl Into<String>,
        shared_registries: Option<FlowInstanceSharedRegistries>,
        runtime: FlowInstanceDedicatedRuntimeOptions,
    ) -> Self {
        Self {
            id: id.into(),
            shared_registries,
            runtime: FlowInstanceRuntimeOptions::Dedicated(runtime),
        }
    }
}

/// Runtime container that manages all Flow resources (streams, pipelines, shared clients).
#[derive(Clone)]
pub struct FlowInstance {
    id: String,
    catalog: Arc<Catalog>,
    spawner: crate::runtime::TaskSpawner,
    shared_stream_registry: Arc<SharedStreamRegistry>,
    pipeline_manager: Arc<PipelineManager>,
    memory_pubsub_registry: MemoryPubSubRegistry,
    // In-memory registry of shared MQTT client configs for discovery/listing.
    shared_mqtt_client_configs: Arc<Mutex<HashMap<String, SharedMqttClientConfig>>>,
    shared_mock_source_handles: Arc<Mutex<HashMap<String, MockSourceHandle>>>,
    mqtt_client_manager: MqttClientManager,
    connector_registry: Arc<ConnectorRegistry>,
    encoder_registry: Arc<EncoderRegistry>,
    decoder_registry: Arc<DecoderRegistry>,
    aggregate_registry: Arc<AggregateFunctionRegistry>,
    stateful_registry: Arc<StatefulFunctionRegistry>,
    custom_func_registry: Arc<parking_lot::RwLock<Arc<CustomFuncRegistry>>>,
    eventtime_type_registry: Arc<EventtimeTypeRegistry>,
    merger_registry: Arc<MergerRegistry>,
    // Secret store + policy for resolving `SecretRef` config (VF-51). Installed
    // at startup; defaults to an empty store so inline configs keep working.
    secret_ctx: Arc<parking_lot::RwLock<SecretContext>>,
    cpu_metrics_state: Option<FlowInstanceCpuMetricsState>,
}

impl FlowInstance {
    fn dedicated_spawner(
        instance_id: &str,
        options: &FlowInstanceDedicatedRuntimeOptions,
        thread_registry: RuntimeThreadRegistry,
    ) -> Result<crate::runtime::TaskSpawner, FlowInstanceError> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = options.worker_threads {
            builder.worker_threads(worker_threads);
        }
        let thread_name = options
            .thread_name_prefix
            .clone()
            .unwrap_or_else(|| format!("flow-instance-{instance_id}"));
        let thread_cgroup_path = options.thread_cgroup_path.clone();
        let runtime_id = instance_id.to_string();
        // Captures the first cgroup-bind error from any worker thread.
        // Worker threads call on_thread_start synchronously during build(), so this cell
        // is populated before build() returns.
        let cgroup_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        #[cfg(target_os = "linux")]
        let cgroup_error_capture = Arc::clone(&cgroup_error);
        builder.on_thread_start(move || {
            #[cfg(not(target_os = "linux"))]
            {
                let _ = &runtime_id;
                let _ = &thread_cgroup_path;
                let _ = &thread_registry;
            }

            #[cfg(target_os = "linux")]
            {
                if let Some(thread_cgroup_path) = &thread_cgroup_path {
                    match crate::runtime::bind_current_thread_to_cgroup(thread_cgroup_path) {
                        Ok(tid) => {
                            thread_registry.insert(tid);
                            tracing::info!(
                                flow_instance_id = %runtime_id,
                                tid,
                                thread_cgroup_path = %thread_cgroup_path,
                                "flow instance runtime thread joined thread cgroup"
                            );
                        }
                        Err(err) => {
                            tracing::error!(
                                flow_instance_id = %runtime_id,
                                thread_cgroup_path = %thread_cgroup_path,
                                error = %err,
                                "failed to bind flow instance runtime thread to cgroup"
                            );
                            *cgroup_error_capture.lock() = Some(err.to_string());
                        }
                    }
                    return;
                }

                match crate::runtime::current_thread_tid() {
                    Ok(tid) => {
                        thread_registry.insert(tid);
                    }
                    Err(err) => {
                        tracing::warn!(
                            flow_instance_id = %runtime_id,
                            error = %err,
                            "failed to register flow instance runtime thread tid"
                        );
                    }
                }
            }
        });
        let runtime = builder
            .thread_name(thread_name)
            .enable_all()
            .build()
            .map_err(|e| {
                FlowInstanceError::Invalid(format!("failed to build tokio runtime: {e}"))
            })?;
        if let Some(err) = cgroup_error.lock().take() {
            return Err(FlowInstanceError::CgroupBind(err));
        }
        Ok(crate::runtime::TaskSpawner::new(runtime))
    }

    fn build(
        id: String,
        catalog: Arc<Catalog>,
        spawner: crate::runtime::TaskSpawner,
        shared_registries: Option<FlowInstanceSharedRegistries>,
    ) -> Self {
        let mqtt_client_manager = MqttClientManager::new(id.clone(), spawner.clone());
        let memory_pubsub_registry = MemoryPubSubRegistry::new();
        let connector_registry =
            ConnectorRegistry::with_builtin_sinks(memory_pubsub_registry.clone());

        let registries = shared_registries.unwrap_or_else(|| FlowInstanceSharedRegistries {
            encoder_registry: EncoderRegistry::with_builtin_encoders(),
            decoder_registry: DecoderRegistry::with_builtin_decoders(),
            aggregate_registry: AggregateFunctionRegistry::with_builtins(),
            stateful_registry: StatefulFunctionRegistry::with_builtins(),
            custom_func_registry: CustomFuncRegistry::with_builtins(),
            eventtime_type_registry: EventtimeTypeRegistry::with_builtin_types(),
            merger_registry: Arc::new(MergerRegistry::new()),
        });
        let shared_stream_registry = Arc::new(SharedStreamRegistry::new_with_merger_registry(
            spawner.clone(),
            Arc::clone(&registries.merger_registry),
        ));

        let registries_bundle = PipelineRegistries::new(
            Arc::clone(&connector_registry),
            Arc::clone(&registries.encoder_registry),
            Arc::clone(&registries.decoder_registry),
            Arc::clone(&registries.aggregate_registry),
            Arc::clone(&registries.stateful_registry),
            Arc::clone(&registries.custom_func_registry),
            Arc::clone(&registries.eventtime_type_registry),
            Arc::clone(&registries.merger_registry),
        );
        let context = crate::pipeline::PipelineContext::new(
            id.to_string(),
            Arc::clone(&shared_stream_registry),
            mqtt_client_manager.clone(),
            memory_pubsub_registry.clone(),
            spawner.clone(),
        );
        let pipeline_manager = Arc::new(PipelineManager::new(
            Arc::clone(&catalog),
            context,
            registries_bundle,
        ));

        Self {
            id,
            catalog,
            spawner,
            shared_stream_registry,
            pipeline_manager,
            memory_pubsub_registry,
            shared_mqtt_client_configs: Arc::new(Mutex::new(HashMap::new())),
            shared_mock_source_handles: Arc::new(Mutex::new(HashMap::new())),
            mqtt_client_manager,
            connector_registry,
            encoder_registry: registries.encoder_registry,
            decoder_registry: registries.decoder_registry,
            aggregate_registry: registries.aggregate_registry,
            stateful_registry: registries.stateful_registry,
            custom_func_registry: Arc::new(parking_lot::RwLock::new(
                registries.custom_func_registry,
            )),
            eventtime_type_registry: registries.eventtime_type_registry,
            merger_registry: registries.merger_registry,
            secret_ctx: Arc::new(parking_lot::RwLock::new(SecretContext::empty())),
            cpu_metrics_state: None,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Snapshot of the secret resolution context (store + policy). Used during
    /// config application to resolve `SecretRef` fields.
    pub fn secret_context(&self) -> SecretContext {
        self.secret_ctx.read().clone()
    }

    /// Install the secret context (called once at startup after loading the
    /// encrypted store with the configured root key provider).
    pub fn set_secret_context(&self, ctx: SecretContext) {
        *self.secret_ctx.write() = ctx;
    }

    pub fn shared_registries(&self) -> FlowInstanceSharedRegistries {
        FlowInstanceSharedRegistries {
            encoder_registry: Arc::clone(&self.encoder_registry),
            decoder_registry: Arc::clone(&self.decoder_registry),
            aggregate_registry: Arc::clone(&self.aggregate_registry),
            stateful_registry: Arc::clone(&self.stateful_registry),
            custom_func_registry: Arc::clone(&self.custom_func_registry.read()),
            eventtime_type_registry: Arc::clone(&self.eventtime_type_registry),
            merger_registry: Arc::clone(&self.merger_registry),
        }
    }

    /// Create a Flow instance from explicit options.
    pub fn new(options: FlowInstanceOptions) -> Result<Self, FlowInstanceError> {
        let id = options.id.trim();
        assert!(!id.is_empty(), "flow instance id must not be empty");
        let mut cpu_metrics_state = None;
        let spawner = match &options.runtime {
            FlowInstanceRuntimeOptions::SharedCurrent => {
                let handle = tokio::runtime::Handle::current();
                crate::runtime::TaskSpawner::from_handle(handle)
            }
            FlowInstanceRuntimeOptions::Dedicated(runtime_options) => {
                let thread_registry = RuntimeThreadRegistry::default();
                cpu_metrics_state = build_flow_instance_cpu_metrics_state(thread_registry.clone());
                Self::dedicated_spawner(id, runtime_options, thread_registry)?
            }
        };
        let mut instance = Self::build(
            id.to_string(),
            Arc::new(Catalog::new()),
            spawner,
            options.shared_registries,
        );
        instance.cpu_metrics_state = cpu_metrics_state;
        register_flow_instance_cpu_metrics_state(&instance.id, instance.cpu_metrics_state.clone());
        Ok(instance)
    }

    pub fn merger_registry(&self) -> Arc<MergerRegistry> {
        Arc::clone(&self.merger_registry)
    }

    /// Sample CPU usage of this FlowInstance as percent of one CPU core.
    ///
    /// A return value of `20.0` means this instance consumed 20% of one core
    /// during the most recent sampling window.
    pub fn sample_cpu_usage_percent(&self) -> Result<Option<f64>, FlowInstanceCpuMetricsError> {
        let state = self
            .cpu_metrics_state
            .as_ref()
            .ok_or(FlowInstanceCpuMetricsError::Unsupported)?;
        sample_flow_instance_cpu_usage_percent(state)
    }

    pub fn declare_memory_topic(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
        capacity: usize,
    ) -> Result<(), MemoryPubSubError> {
        self.memory_pubsub_registry
            .declare_topic(topic, kind, capacity)
    }

    pub fn open_memory_publisher_bytes(
        &self,
        topic: &str,
    ) -> Result<MemoryPublisher, MemoryPubSubError> {
        self.memory_pubsub_registry.open_publisher_bytes(topic)
    }

    pub fn open_memory_publisher_collection(
        &self,
        topic: &str,
    ) -> Result<MemoryPublisher, MemoryPubSubError> {
        self.memory_pubsub_registry.open_publisher_collection(topic)
    }

    pub fn memory_topic_kind(&self, topic: &str) -> Option<MemoryTopicKind> {
        self.memory_pubsub_registry.topic_kind(topic)
    }

    pub fn memory_topic_capacity(&self, topic: &str) -> Option<usize> {
        self.memory_pubsub_registry.topic_capacity(topic)
    }

    pub fn open_memory_subscribe_bytes(
        &self,
        topic: &str,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        self.memory_pubsub_registry.open_subscribe_bytes(topic)
    }

    pub fn open_memory_subscribe_collection(
        &self,
        topic: &str,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        self.memory_pubsub_registry.open_subscribe_collection(topic)
    }

    pub async fn wait_for_memory_subscribers(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
        min: usize,
        timeout: std::time::Duration,
    ) -> Result<(), MemoryPubSubError> {
        self.memory_pubsub_registry
            .wait_for_subscribers(topic, kind, min, timeout)
            .await
    }

    /// Register a custom sink connector factory under the given kind name.
    #[allow(dead_code)]
    #[allow(clippy::type_complexity)]
    pub(crate) fn register_sink_connector(
        &self,
        kind: impl Into<String>,
        factory: Arc<
            dyn Fn(
                    &str,
                    &SinkConnectorConfig,
                    &str,
                    &MqttClientManager,
                    &TaskSpawner,
                ) -> Result<Box<dyn SinkConnector>, ConnectorError>
                + Send
                + Sync,
        >,
    ) {
        self.connector_registry.register_sink_factory(kind, factory);
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
    #[error("stream {stream} still referenced by pipelines: {pipelines}")]
    StreamInUse { stream: String, pipelines: String },
    #[error("{0}")]
    Invalid(String),
    #[error("failed to bind runtime thread to cgroup: {0}")]
    CgroupBind(String),
}

/// Errors surfaced by FlowInstance CPU metric sampling.
#[derive(thiserror::Error, Debug)]
pub enum FlowInstanceCpuMetricsError {
    #[error("flow instance cpu metric sampling is unsupported")]
    Unsupported,
    #[error("runtime worker thread not found: {0}")]
    ThreadNotFound(u32),
    #[error("{0}")]
    Other(String),
}

pub(crate) use cpu_metrics::collect_registered_flow_instance_cpu_metrics;
