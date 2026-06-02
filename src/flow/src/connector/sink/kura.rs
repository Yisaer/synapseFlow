//! Kura sink connector — writes VSS signal values to a kura server via gRPC (yoriito VISS producer).

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use datatypes::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use crate::model::Collection;
use crate::runtime::TaskSpawner;

// ── generated protobuf types ────────────────────────────────────────────────
#[allow(clippy::enum_variant_names)]
mod kura_proto {
    tonic::include_proto!("yoriito.viss.v1");

    pub mod producer {
        tonic::include_proto!("yoriito.viss.v1.producer");
    }
}

use kura_proto::producer::viss_client::VissClient;
use kura_proto::producer::{set_current_response, SetCurrentRequest};
use kura_proto::{value_type, DataPackageCurrent, DataPackagesCurrent, DataPointCurrent};

/// Kura sink configuration.
#[derive(Debug, Clone)]
pub struct KuraSinkConfig {
    pub sink_name: String,
    /// Kura gRPC endpoint, e.g. `http://127.0.0.1:50051`
    pub addr: String,
    /// Path to a JSON mapping file: `{ "<field_name>": "<VSS path>" }`
    pub mapping_path: String,
}

pub(crate) struct KuraSinkConnector {
    id: String,
    config: KuraSinkConfig,
    mapping: Option<Arc<HashMap<String, String>>>,
    client: Option<VissClient<tonic::transport::Channel>>,
}

impl KuraSinkConnector {
    pub fn new(id: impl Into<String>, config: KuraSinkConfig, _spawner: TaskSpawner) -> Self {
        Self {
            id: id.into(),
            config,
            mapping: None,
            client: None,
        }
    }

    // ── mapping file ────────────────────────────────────────────────────────

    fn load_mapping(&self) -> Result<HashMap<String, String>, SinkConnectorError> {
        let raw = fs::read_to_string(&self.config.mapping_path).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kura sink failed to read mapping file {}: {err}",
                self.config.mapping_path
            ))
        })?;
        let mapping: HashMap<String, String> = serde_json::from_str(&raw).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kura sink failed to parse mapping JSON {}: {err}",
                self.config.mapping_path
            ))
        })?;
        Ok(mapping)
    }

    fn ensure_mapping_loaded(&mut self) -> Result<(), SinkConnectorError> {
        if self.mapping.is_some() {
            return Ok(());
        }
        self.mapping = Some(Arc::new(self.load_mapping()?));
        Ok(())
    }

    // ── gRPC client ─────────────────────────────────────────────────────────

    async fn ensure_client(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }

        let addr = self.config.addr.trim();
        let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };

        let client = VissClient::connect(endpoint.clone()).await.map_err(|err| {
            SinkConnectorError::Other(format!(
                "kura sink failed to connect to `{}`: {err}",
                self.config.addr
            ))
        })?;

        self.client = Some(client);
        Ok(())
    }

    // ── value conversion ────────────────────────────────────────────────────

    /// Convert a `datatypes::Value` into the proto `ValueType` oneof.
    fn to_value_type(value: &Value) -> Result<kura_proto::ValueType, SinkConnectorError> {
        let vt = match value {
            Value::Null => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support null values".to_string(),
                ));
            }
            Value::Bool(b) => value_type::ValueType::Bool(*b),
            Value::Int8(v) => value_type::ValueType::Int8(*v as i32),
            Value::Int16(v) => value_type::ValueType::Int16(*v as i32),
            Value::Int32(v) => value_type::ValueType::Int32(*v),
            Value::Int64(v) => value_type::ValueType::Int64(*v),
            Value::Uint8(v) => value_type::ValueType::Uint8(*v as u32),
            Value::Uint16(v) => value_type::ValueType::Uint16(*v as u32),
            Value::Uint32(v) => value_type::ValueType::Uint32(*v),
            Value::Uint64(v) => value_type::ValueType::Uint64(*v),
            Value::Float32(v) => value_type::ValueType::Float(*v),
            Value::Float64(v) => value_type::ValueType::Double(*v),
            Value::String(s) => value_type::ValueType::String(s.clone()),
            // Struct, list, bytes, and timestamp values have no standard VSS
            // wire representation; reject them explicitly.
            Value::Struct(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support struct values".to_string(),
                ));
            }
            Value::List(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support list values".to_string(),
                ));
            }
            Value::Bytes(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support bytes values".to_string(),
                ));
            }
            Value::Timestamp(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support timestamp values".to_string(),
                ));
            }
        };
        Ok(kura_proto::ValueType {
            value_type: Some(vt),
        })
    }

    /// Walk a collection row and resolve every non-null column that appears in
    /// the mapping file.  Returns `(path, DataPointCurrent)` pairs.
    fn iter_updates_for_row(
        mapping: &HashMap<String, String>,
        tuple: &crate::model::Tuple,
    ) -> Result<Vec<(String, DataPointCurrent)>, SinkConnectorError> {
        let mut out: Vec<(String, DataPointCurrent)> = Vec::new();
        for ((_, column_name), value) in tuple.entries() {
            if value.is_null() {
                continue;
            }
            let Some(vss_path) = mapping.get(column_name) else {
                continue;
            };
            let vt = Self::to_value_type(value)?;
            let dp = DataPointCurrent {
                stored_timestamp: 0,
                value: Some(vt),
                produced_timestamp: None,
                is_available_sensor: None,
                is_available_actuator: None,
            };
            out.push((vss_path.clone(), dp));
        }
        Ok(out)
    }
}

// ── SinkConnector impl ──────────────────────────────────────────────────────

#[async_trait]
impl SinkConnector for KuraSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        self.ensure_client().await?;
        tracing::info!(
            connector_id = %self.id,
            addr = %self.config.addr,
            mapping_path = %self.config.mapping_path,
            "kura sink ready"
        );
        Ok(())
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        let mapping = self
            .mapping
            .clone()
            .ok_or_else(|| SinkConnectorError::Other("kura sink mapping missing".to_string()))?;

        let mut packages: Vec<DataPackageCurrent> = Vec::new();

        // Collect all column → VSS-path mappings per row into a single batch.
        for tuple in collection.rows() {
            let entries = Self::iter_updates_for_row(mapping.as_ref(), tuple)?;
            for (path, dp) in entries {
                packages.push(DataPackageCurrent { path, dp: vec![dp] });
            }
        }

        if packages.is_empty() {
            return Ok(());
        }

        let request = tonic::Request::new(SetCurrentRequest {
            data: Some(DataPackagesCurrent { data: packages }),
        });

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| SinkConnectorError::Other("kura client missing".to_string()))?;

        let response = client
            .set_current(request)
            .await
            .map_err(|err| SinkConnectorError::Other(format!("kura set_current: {err}")))?;

        let resp = response.into_inner();
        if let Some(result) = resp.result {
            match result {
                set_current_response::Result::Success(_) => {}
                set_current_response::Result::Error(err) => {
                    return Err(SinkConnectorError::Other(format!(
                        "kura set_current error: {:?} {:?} {}",
                        err.number(),
                        err.reason(),
                        err.description.as_deref().unwrap_or("")
                    )));
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.client = None;
        tracing::info!(connector_id = %self.id, "kura sink closed");
        Ok(())
    }
}
