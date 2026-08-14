use crate::connector::{MemoryPubSubRegistry, MockSourceHandle, MqttClientManager};
use crate::pipeline::PipelineRuntimeFailure;
use crate::shared_stream::SharedStreamRegistry;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) type PipelineFailureReporter = Arc<dyn Fn(PipelineRuntimeFailure) + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct MockSourceHandleRegistry {
    handles: Arc<RwLock<HashMap<String, MockSourceHandle>>>,
}

impl MockSourceHandleRegistry {
    pub(crate) fn register(&self, key: impl Into<String>, handle: MockSourceHandle) {
        self.handles.write().insert(key.into(), handle);
    }
}

#[derive(Clone)]
pub(crate) struct PipelineContext {
    flow_instance_id: Arc<str>,
    shared_stream_registry: Arc<SharedStreamRegistry>,
    mqtt_client_manager: MqttClientManager,
    memory_pubsub_registry: MemoryPubSubRegistry,
    mock_source_handle_registry: MockSourceHandleRegistry,
    spawner: crate::runtime::TaskSpawner,
    property_context: Arc<RwLock<crate::PropertyContext>>,
    pipeline_failure_reporter: Arc<RwLock<Option<PipelineFailureReporter>>>,
}

impl PipelineContext {
    pub(crate) fn new(
        flow_instance_id: impl Into<Arc<str>>,
        shared_stream_registry: Arc<SharedStreamRegistry>,
        mqtt_client_manager: MqttClientManager,
        memory_pubsub_registry: MemoryPubSubRegistry,
        spawner: crate::runtime::TaskSpawner,
    ) -> Self {
        Self {
            flow_instance_id: flow_instance_id.into(),
            shared_stream_registry,
            mqtt_client_manager,
            memory_pubsub_registry,
            mock_source_handle_registry: MockSourceHandleRegistry::default(),
            spawner,
            property_context: Arc::new(RwLock::new(crate::PropertyContext::default())),
            pipeline_failure_reporter: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) fn flow_instance_id(&self) -> &str {
        self.flow_instance_id.as_ref()
    }

    pub(crate) fn shared_stream_registry(&self) -> Arc<SharedStreamRegistry> {
        Arc::clone(&self.shared_stream_registry)
    }

    pub(crate) fn mqtt_client_manager(&self) -> &MqttClientManager {
        &self.mqtt_client_manager
    }

    pub(crate) fn memory_pubsub_registry(&self) -> &MemoryPubSubRegistry {
        &self.memory_pubsub_registry
    }

    pub(crate) fn mock_source_handle_registry(&self) -> &MockSourceHandleRegistry {
        &self.mock_source_handle_registry
    }

    pub(crate) fn spawner(&self) -> &crate::runtime::TaskSpawner {
        &self.spawner
    }

    pub(crate) fn property_context(&self) -> crate::PropertyContext {
        self.property_context.read().clone()
    }

    pub(crate) fn property_context_handle(&self) -> Arc<RwLock<crate::PropertyContext>> {
        Arc::clone(&self.property_context)
    }

    pub(crate) fn set_pipeline_failure_reporter(&self, reporter: PipelineFailureReporter) {
        *self.pipeline_failure_reporter.write() = Some(reporter);
    }

    pub(crate) fn pipeline_failure_reporter(&self) -> Option<PipelineFailureReporter> {
        self.pipeline_failure_reporter.read().clone()
    }
}
