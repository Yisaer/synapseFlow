use super::sink::file::FileSinkConnector;
use super::sink::http::HttpSinkConnector;
use super::sink::kuksa::KuksaSinkConnector;
use super::sink::kura::KuraSinkConnector;
use super::sink::memory::MemorySinkConnector;
use super::sink::mqtt::MqttSinkConnector;
#[cfg(feature = "nng_pubsub")]
use super::sink::nng_pubsub::NngPubSubSinkConnector;
use super::sink::nop::NopSinkConnector;
use super::sink::video::VideoSinkConnector;
use super::sink::SinkConnector;
use super::ConnectorError;
use crate::connector::MemoryPubSubRegistry;
use crate::connector::MqttClientManager;
use crate::planner::sink::SinkConnectorConfig;
use crate::runtime::TaskSpawner;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

type SinkConnectorFactory = Arc<
    dyn Fn(
            &str,
            &SinkConnectorConfig,
            &str,
            &MqttClientManager,
            &TaskSpawner,
        ) -> Result<Box<dyn SinkConnector>, ConnectorError>
        + Send
        + Sync,
>;

/// Registry that resolves sink connector IDs to factory functions.
pub struct ConnectorRegistry {
    sink_factories: RwLock<HashMap<String, SinkConnectorFactory>>,
    memory_pubsub_registry: MemoryPubSubRegistry,
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        let registry = Self::new(MemoryPubSubRegistry::new());
        registry.register_builtin_sinks();
        registry
    }
}

impl ConnectorRegistry {
    pub fn new(memory_pubsub_registry: MemoryPubSubRegistry) -> Self {
        Self {
            sink_factories: RwLock::new(HashMap::new()),
            memory_pubsub_registry,
        }
    }

    pub fn with_builtin_sinks(memory_pubsub_registry: MemoryPubSubRegistry) -> Arc<Self> {
        let registry = Arc::new(Self::new(memory_pubsub_registry));
        registry.register_builtin_sinks();
        registry
    }

    pub(crate) fn register_sink_factory(
        &self,
        kind: impl Into<String>,
        factory: SinkConnectorFactory,
    ) {
        self.sink_factories.write().insert(kind.into(), factory);
    }

    pub fn is_registered(&self, kind: &str) -> bool {
        let guard = self.sink_factories.read();
        guard.contains_key(kind)
    }

    pub(crate) fn instantiate_sink(
        &self,
        kind: &str,
        sink_id: &str,
        config: &SinkConnectorConfig,
        flow_instance_id: &str,
        mqtt_clients: &MqttClientManager,
        spawner: &TaskSpawner,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let guard = self.sink_factories.read();
        let factory = guard.get(kind).ok_or_else(|| {
            ConnectorError::NotFound(format!("sink connector kind `{kind}` not registered"))
        })?;
        factory(sink_id, config, flow_instance_id, mqtt_clients, spawner)
    }

    fn register_builtin_sinks(&self) {
        let memory_pubsub_registry = self.memory_pubsub_registry.clone();

        self.register_sink_factory(
            "mqtt",
            Arc::new(
                |sink_id, config, flow_instance_id, mqtt_clients, spawner| match config {
                    SinkConnectorConfig::Mqtt(mqtt_cfg) => Ok(Box::new(MqttSinkConnector::new(
                        sink_id.to_string(),
                        mqtt_cfg.clone(),
                        flow_instance_id,
                        mqtt_clients.clone(),
                        spawner.clone(),
                    ))),
                    other => Err(ConnectorError::Other(format!(
                        "connector `{sink_id}` expected MQTT config but received {:?}",
                        other.kind()
                    ))),
                },
            ),
        );

        self.register_sink_factory(
            "nop",
            Arc::new(|sink_id, config, _, _, _| match config {
                SinkConnectorConfig::Nop(cfg) => Ok(Box::new(NopSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected Nop config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "kuksa",
            Arc::new(|sink_id, config, _, _, spawner| match config {
                SinkConnectorConfig::Kuksa(cfg) => Ok(Box::new(KuksaSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                    spawner.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected Kuksa config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "kura",
            Arc::new(|sink_id, config, _, _, spawner| match config {
                SinkConnectorConfig::Kura(cfg) => Ok(Box::new(KuraSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                    spawner.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected Kura config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "memory",
            Arc::new(move |sink_id, config, _, _, _| match config {
                SinkConnectorConfig::Memory(cfg) => Ok(Box::new(MemorySinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                    memory_pubsub_registry.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected Memory config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "file",
            Arc::new(|sink_id, config, _, _, _| match config {
                SinkConnectorConfig::File(cfg) => Ok(Box::new(FileSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected File config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "video",
            Arc::new(|sink_id, config, _, _, _| match config {
                SinkConnectorConfig::Video(cfg) => Ok(Box::new(VideoSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected Video config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        #[cfg(feature = "nng_pubsub")]
        self.register_sink_factory(
            "nng_pubsub",
            Arc::new(|sink_id, config, flow_instance_id, _, _| match config {
                SinkConnectorConfig::NngPubSub(cfg) => Ok(Box::new(NngPubSubSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                    flow_instance_id.to_string(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected NNG pubsub config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "http",
            Arc::new(|sink_id, config, _, _, _| match config {
                SinkConnectorConfig::Http(cfg) => Ok(Box::new(HttpSinkConnector::new(
                    sink_id.to_string(),
                    cfg.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected HTTP config but received {:?}",
                    other.kind()
                ))),
            }),
        );
    }
}
