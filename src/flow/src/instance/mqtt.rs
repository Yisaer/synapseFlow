use crate::connector::SharedMqttClientConfig;

use super::{FlowInstance, FlowInstanceError};

impl FlowInstance {
    /// Register a shared MQTT client that can be referenced by connector keys.
    pub async fn create_shared_mqtt_client(
        &self,
        config: SharedMqttClientConfig,
    ) -> Result<(), FlowInstanceError> {
        // Resolve the password against the secret store for the runtime copy; the
        // registry/persisted copy keeps the `SecretRef` pointer (never the value).
        let mut runtime_config = config.clone();
        if let Some(warning) = runtime_config
            .resolve_secrets(&self.secret_context())
            .map_err(|err| FlowInstanceError::Invalid(err.to_string()))?
        {
            tracing::warn!(target: "veloflux::secret", "{warning}");
        }
        self.mqtt_client_manager
            .create_client(runtime_config)
            .await?;
        self.shared_mqtt_client_configs
            .lock()
            .insert(config.key.clone(), config);
        Ok(())
    }

    /// Drop a shared MQTT client identified by key.
    pub fn drop_shared_mqtt_client(&self, key: &str) -> Result<(), FlowInstanceError> {
        self.mqtt_client_manager.drop_client(key)?;
        self.shared_mqtt_client_configs.lock().remove(key);
        Ok(())
    }

    /// List metadata for registered shared MQTT clients.
    pub fn list_shared_mqtt_clients(&self) -> Vec<SharedMqttClientConfig> {
        self.shared_mqtt_client_configs
            .lock()
            .values()
            .cloned()
            .collect()
    }

    /// Fetch metadata for a single shared MQTT client.
    pub fn get_shared_mqtt_client(&self, key: &str) -> Option<SharedMqttClientConfig> {
        self.shared_mqtt_client_configs.lock().get(key).cloned()
    }
}
