use crate::connector::SharedMqttClientConfig;

use super::{FlowInstance, FlowInstanceError};

impl FlowInstance {
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
}
