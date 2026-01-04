//! Kuksa sink connector (kuksa.val.v2) - updates VSS paths from decoded collections.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use datatypes::Value;
use std::collections::HashMap;
use std::fs;

use crate::model::Collection;

/// Kuksa sink configuration.
#[derive(Debug, Clone)]
pub struct KuksaSinkConfig {
    pub sink_name: String,
    /// Kuksa broker address, e.g. `http://127.0.0.1:55555` (exact format depends on SDK).
    pub addr: String,
    /// Path to a mapping file that maps input field keys -> VSS paths.
    pub vss_path: String,
}

pub struct KuksaSinkConnector {
    id: String,
    config: KuksaSinkConfig,
    mapping: Option<HashMap<String, String>>,
}

impl KuksaSinkConnector {
    pub fn new(id: impl Into<String>, config: KuksaSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
            mapping: None,
        }
    }

    fn load_mapping(&self) -> Result<HashMap<String, String>, SinkConnectorError> {
        let raw = fs::read_to_string(&self.config.vss_path).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kuksa sink failed to read vss mapping file {}: {err}",
                self.config.vss_path
            ))
        })?;
        serde_json::from_str::<HashMap<String, String>>(&raw).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kuksa sink failed to parse vss mapping file {} as JSON object: {err}",
                self.config.vss_path
            ))
        })
    }

    fn resolve_vss_path<'a>(
        mapping: &'a HashMap<String, String>,
        key: &str,
    ) -> Option<&'a str> {
        mapping.get(key).map(|s| s.as_str())
    }

    fn iter_updates_for_row(
        mapping: &HashMap<String, String>,
        tuple: &crate::model::Tuple,
    ) -> HashMap<String, Value> {
        let mut out: HashMap<String, Value> = HashMap::new();
        for ((_, column_name), value) in tuple.entries() {
            if value.is_null() {
                continue;
            }
            let Some(vss_path) = Self::resolve_vss_path(mapping, column_name) else {
                continue;
            };
            out.insert(vss_path.to_string(), value.clone());
        }
        out
    }

    fn ensure_mapping_loaded(&mut self) -> Result<(), SinkConnectorError> {
        if self.mapping.is_some() {
            return Ok(());
        }
        self.mapping = Some(self.load_mapping()?);
        Ok(())
    }

    async fn update_vss_paths(
        &self,
        _updates: &HashMap<String, Value>,
    ) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Other(
            "kuksa sink client not wired yet: add kuksa-rust-sdk dependency and implement kuksa.val.v2 update"
                .to_string(),
        ))
    }
}

#[async_trait]
impl SinkConnector for KuksaSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        tracing::info!(
            connector_id = %self.id,
            addr = %self.config.addr,
            vss_path = %self.config.vss_path,
            "kuksa sink ready"
        );
        Ok(())
    }

    async fn send(&mut self, _payload: &[u8]) -> Result<(), SinkConnectorError> {
        Err(SinkConnectorError::Other(
            "kuksa sink expects collection payloads (encoder must be none)".to_string(),
        ))
    }

    async fn send_collection(&mut self, collection: &dyn Collection) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| SinkConnectorError::Other("kuksa sink mapping missing".to_string()))?;

        for tuple in collection.rows() {
            let updates = Self::iter_updates_for_row(mapping, tuple);
            if updates.is_empty() {
                continue;
            }
            self.update_vss_paths(&updates).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        tracing::info!(connector_id = %self.id, "kuksa sink closed");
        Ok(())
    }
}

