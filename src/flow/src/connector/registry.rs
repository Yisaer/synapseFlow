use super::sink::mqtt::MqttSinkConnector;
use super::sink::nop::NopSinkConnector;
use super::sink::SinkConnector;
use super::ConnectorError;
use crate::connector::MqttClientManager;
use crate::planner::sink::SinkConnectorConfig;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type SinkConnectorFactory = Arc<
    dyn Fn(
            &str,
            &SinkConnectorConfig,
            &MqttClientManager,
        ) -> Result<Box<dyn SinkConnector>, ConnectorError>
        + Send
        + Sync,
>;

/// Registry that resolves sink connector IDs to factory functions.
pub struct ConnectorRegistry {
    sink_factories: RwLock<HashMap<String, SinkConnectorFactory>>,
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            sink_factories: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtin_sinks() -> Arc<Self> {
        let registry = Arc::new(Self::new());
        registry.register_builtin_sinks();
        registry
    }

    pub fn register_sink_factory(&self, kind: impl Into<String>, factory: SinkConnectorFactory) {
        self.sink_factories
            .write()
            .expect("connector registry poisoned")
            .insert(kind.into(), factory);
    }

    pub fn instantiate_sink(
        &self,
        kind: &str,
        sink_id: &str,
        config: &SinkConnectorConfig,
        mqtt_clients: &MqttClientManager,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let guard = self
            .sink_factories
            .read()
            .expect("connector registry poisoned");
        let factory = guard.get(kind).ok_or_else(|| {
            ConnectorError::NotFound(format!("sink connector kind `{kind}` not registered"))
        })?;
        factory(sink_id, config, mqtt_clients)
    }

    fn register_builtin_sinks(&self) {
        self.register_sink_factory(
            "mqtt",
            Arc::new(|sink_id, config, mqtt_clients| match config {
                SinkConnectorConfig::Mqtt(mqtt_cfg) => Ok(Box::new(MqttSinkConnector::new(
                    sink_id.to_string(),
                    mqtt_cfg.clone(),
                    mqtt_clients.clone(),
                ))),
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected MQTT config but received {:?}",
                    other.kind()
                ))),
            }),
        );

        self.register_sink_factory(
            "nop",
            Arc::new(|sink_id, config, _| match config {
                SinkConnectorConfig::Nop(_) => {
                    Ok(Box::new(NopSinkConnector::new(sink_id.to_string())))
                }
                other => Err(ConnectorError::Other(format!(
                    "connector `{sink_id}` expected Nop config but received {:?}",
                    other.kind()
                ))),
            }),
        );
    }
}
