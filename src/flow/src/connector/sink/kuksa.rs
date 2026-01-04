//! Kuksa sink connector (kuksa.val.v2) - updates VSS paths from decoded collections.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use datatypes::Value;
use http::Uri;
use kuksa_rust_sdk::kuksa::common::types::OpenProviderStream;
use kuksa_rust_sdk::kuksa::common::ClientError as KuksaClientError;
use kuksa_rust_sdk::kuksa::common::ClientTraitV2;
use kuksa_rust_sdk::kuksa::val::v2::KuksaClientV2;
use kuksa_rust_sdk::proto::kuksa::val::v2::{
    open_provider_stream_request, Datapoint, Metadata, OpenProviderStreamRequest, SampleInterval,
    Value as KuksaValue,
};
use kuksa_rust_sdk::proto::kuksa::val::v2::{value as kuksa_value, DataType};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

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
    mapping: Option<Arc<HashMap<String, String>>>,
    client: Option<KuksaClientV2>,
    provider_sender: Option<tokio::sync::mpsc::Sender<OpenProviderStreamRequest>>,
    response_task: Option<tokio::task::JoinHandle<()>>,
    path_cache: HashMap<String, PathInfo>,
    provided_signal_ids: HashSet<i32>,
    request_id_counter: u32,
}

impl KuksaSinkConnector {
    pub fn new(id: impl Into<String>, config: KuksaSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
            mapping: None,
            client: None,
            provider_sender: None,
            response_task: None,
            path_cache: HashMap::new(),
            provided_signal_ids: HashSet::new(),
            request_id_counter: 0,
        }
    }

    fn load_mapping(&self) -> Result<HashMap<String, String>, SinkConnectorError> {
        let raw = fs::read_to_string(&self.config.vss_path).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kuksa sink failed to read vss mapping file {}: {err}",
                self.config.vss_path
            ))
        })?;
        let root: serde_json::Value = serde_json::from_str(&raw).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kuksa sink failed to unmarshal vss json file {}: {err}",
                self.config.vss_path
            ))
        })?;
        Ok(parse_vss_json(&root))
    }

    fn resolve_vss_path<'a>(mapping: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
        mapping.get(key).map(|s| s.as_str())
    }

    fn iter_updates_for_row<'a>(
        mapping: &HashMap<String, String>,
        tuple: &'a crate::model::Tuple,
    ) -> HashMap<String, &'a Value> {
        let mut out: HashMap<String, &'a Value> = HashMap::new();
        for ((_, column_name), value) in tuple.entries() {
            if value.is_null() {
                continue;
            }
            let Some(vss_path) = Self::resolve_vss_path(mapping, column_name) else {
                continue;
            };
            out.insert(vss_path.to_string(), value);
        }
        out
    }

    fn ensure_mapping_loaded(&mut self) -> Result<(), SinkConnectorError> {
        if self.mapping.is_some() {
            return Ok(());
        }
        self.mapping = Some(Arc::new(self.load_mapping()?));
        Ok(())
    }

    fn ensure_kuksa_client(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }
        let addr = self.config.addr.trim();
        let normalized = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let uri: Uri = normalized.parse().map_err(|err| {
            SinkConnectorError::Other(format!("invalid kuksa addr `{}`: {err}", self.config.addr))
        })?;
        self.client = Some(KuksaClientV2::new(uri));
        Ok(())
    }

    async fn ensure_provider_stream(&mut self) -> Result<(), SinkConnectorError> {
        if self.provider_sender.is_some() {
            return Ok(());
        }
        self.ensure_kuksa_client()?;
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| SinkConnectorError::Other("kuksa client missing".to_string()))?;
        let OpenProviderStream {
            sender,
            mut receiver_stream,
        } = client.open_provider_stream(Some(32)).await.map_err(|err| {
            SinkConnectorError::Other(format!("kuksa open_provider_stream: {err:?}"))
        })?;

        let connector_id = self.id.clone();
        let response_task = tokio::spawn(async move {
            loop {
                match receiver_stream.message().await {
                    Ok(Some(response)) => {
                        tracing::debug!(connector_id = %connector_id, "kuksa provider response: {:?}", response);
                    }
                    Ok(None) => {
                        tracing::info!(connector_id = %connector_id, "kuksa provider stream closed");
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(connector_id = %connector_id, error = %err, "kuksa provider stream recv error");
                        break;
                    }
                }
            }
        });

        self.provider_sender = Some(sender);
        self.response_task = Some(response_task);
        Ok(())
    }

    async fn ensure_path_info(&mut self, path: &str) -> Result<PathInfo, SinkConnectorError> {
        if let Some(info) = self.path_cache.get(path) {
            return Ok(info.clone());
        }
        self.ensure_kuksa_client()?;
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| SinkConnectorError::Other("kuksa client missing".to_string()))?;

        let metadata_list = client
            .list_metadata((path.to_string(), "*".to_string()))
            .await
            .map_err(map_kuksa_error)?;
        let metadata = metadata_list
            .into_iter()
            .find(|m: &Metadata| m.path == path)
            .ok_or_else(|| {
                SinkConnectorError::Other(format!("kuksa metadata not found for path {path}"))
            })?;

        let info = PathInfo {
            id: metadata.id,
            data_type: DataType::try_from(metadata.data_type).unwrap_or(DataType::Unspecified),
        };
        self.path_cache.insert(path.to_string(), info.clone());
        Ok(info)
    }

    async fn ensure_provided_ids(&mut self, ids: &[i32]) -> Result<(), SinkConnectorError> {
        self.ensure_provider_stream().await?;
        let mut signals_sample_intervals: HashMap<i32, SampleInterval> = HashMap::new();
        for id in ids {
            if self.provided_signal_ids.contains(id) {
                continue;
            }
            signals_sample_intervals.insert(*id, SampleInterval { interval_ms: 0 });
        }
        if signals_sample_intervals.is_empty() {
            return Ok(());
        }

        let request = OpenProviderStreamRequest {
            action: Some(open_provider_stream_request::Action::ProvideSignalRequest(
                kuksa_rust_sdk::proto::kuksa::val::v2::ProvideSignalRequest {
                    signals_sample_intervals,
                },
            )),
        };
        let sender = self.provider_sender.as_mut().ok_or_else(|| {
            SinkConnectorError::Other("kuksa provider sender missing".to_string())
        })?;
        sender.send(request).await.map_err(|err| {
            SinkConnectorError::Other(format!("kuksa provide_signal send: {err}"))
        })?;

        for id in ids {
            self.provided_signal_ids.insert(*id);
        }
        Ok(())
    }

    async fn publish_values_by_id(
        &mut self,
        datapoints: HashMap<i32, Datapoint>,
    ) -> Result<(), SinkConnectorError> {
        self.ensure_provider_stream().await?;
        if datapoints.is_empty() {
            return Ok(());
        }
        self.request_id_counter = self.request_id_counter.wrapping_add(1);
        let request = OpenProviderStreamRequest {
            action: Some(open_provider_stream_request::Action::PublishValuesRequest(
                kuksa_rust_sdk::proto::kuksa::val::v2::PublishValuesRequest {
                    request_id: self.request_id_counter,
                    data_points: datapoints,
                },
            )),
        };
        let sender = self.provider_sender.as_mut().ok_or_else(|| {
            SinkConnectorError::Other("kuksa provider sender missing".to_string())
        })?;
        sender.send(request).await.map_err(|err| {
            SinkConnectorError::Other(format!("kuksa publish_values send: {err}"))
        })?;
        Ok(())
    }

    async fn update_vss_paths(
        &mut self,
        updates: &HashMap<String, &Value>,
    ) -> Result<(), SinkConnectorError> {
        let mut datapoints: HashMap<i32, Datapoint> = HashMap::with_capacity(updates.len());
        let mut ids: Vec<i32> = Vec::with_capacity(updates.len());
        for (path, value) in updates {
            let info = self.ensure_path_info(path).await?;
            ids.push(info.id);
            let typed_value = convert_value(value, info.data_type)?;
            datapoints.insert(
                info.id,
                Datapoint {
                    timestamp: None,
                    value: Some(typed_value),
                },
            );
        }

        self.ensure_provided_ids(&ids).await?;
        self.publish_values_by_id(datapoints).await
    }
}

#[derive(Debug, Clone)]
struct PathInfo {
    id: i32,
    data_type: DataType,
}

fn map_kuksa_error(err: KuksaClientError) -> SinkConnectorError {
    SinkConnectorError::Other(format!("kuksa client error: {err:?}"))
}

fn convert_value(value: &Value, data_type: DataType) -> Result<KuksaValue, SinkConnectorError> {
    let typed_value = match data_type {
        DataType::String => kuksa_value::TypedValue::String(match value {
            Value::String(s) => s.clone(),
            other => {
                return Err(SinkConnectorError::Other(format!(
                    "expected string, got {other:?}"
                )))
            }
        }),
        DataType::Boolean => kuksa_value::TypedValue::Bool(match value {
            Value::Bool(b) => *b,
            other => {
                return Err(SinkConnectorError::Other(format!(
                    "expected bool, got {other:?}"
                )))
            }
        }),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            kuksa_value::TypedValue::Int32(match value {
                Value::Int8(v) => *v as i32,
                Value::Int16(v) => *v as i32,
                Value::Int32(v) => *v,
                other => {
                    return Err(SinkConnectorError::Other(format!(
                        "expected int32-compatible, got {other:?}"
                    )))
                }
            })
        }
        DataType::Int64 => kuksa_value::TypedValue::Int64(match value {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            Value::Int16(v) => *v as i64,
            Value::Int8(v) => *v as i64,
            other => {
                return Err(SinkConnectorError::Other(format!(
                    "expected int64-compatible, got {other:?}"
                )))
            }
        }),
        DataType::Uint8 | DataType::Uint16 | DataType::Uint32 => {
            kuksa_value::TypedValue::Uint32(match value {
                Value::Uint8(v) => *v as u32,
                Value::Uint16(v) => *v as u32,
                Value::Uint32(v) => *v,
                other => {
                    return Err(SinkConnectorError::Other(format!(
                        "expected uint32-compatible, got {other:?}"
                    )))
                }
            })
        }
        DataType::Uint64 => kuksa_value::TypedValue::Uint64(match value {
            Value::Uint64(v) => *v,
            Value::Uint32(v) => *v as u64,
            Value::Uint16(v) => *v as u64,
            Value::Uint8(v) => *v as u64,
            other => {
                return Err(SinkConnectorError::Other(format!(
                    "expected uint64-compatible, got {other:?}"
                )))
            }
        }),
        DataType::Float => kuksa_value::TypedValue::Float(match value {
            Value::Float32(v) => *v,
            Value::Float64(v) => *v as f32,
            other => {
                return Err(SinkConnectorError::Other(format!(
                    "expected float-compatible, got {other:?}"
                )))
            }
        }),
        DataType::Double => kuksa_value::TypedValue::Double(match value {
            Value::Float64(v) => *v,
            Value::Float32(v) => *v as f64,
            other => {
                return Err(SinkConnectorError::Other(format!(
                    "expected double-compatible, got {other:?}"
                )))
            }
        }),
        DataType::Unspecified => {
            return Err(SinkConnectorError::Other(
                "unsupported kuksa data type: unspecified".to_string(),
            ))
        }
        other => {
            return Err(SinkConnectorError::Other(format!(
                "unsupported kuksa data type: {other:?}"
            )))
        }
    };
    Ok(KuksaValue {
        typed_value: Some(typed_value),
    })
}

fn parse_vss_json(root: &serde_json::Value) -> HashMap<String, String> {
    let mut result = HashMap::new();
    fn maybe_record_sig2vss(
        node: &serde_json::Value,
        vss_path: &str,
        out: &mut HashMap<String, String>,
    ) {
        let serde_json::Value::Object(map) = node else {
            return;
        };
        let Some(serde_json::Value::Object(sig2vss)) = map.get("sig2vss") else {
            return;
        };
        let Some(serde_json::Value::String(qualified_name)) = sig2vss.get("qualifiedName") else {
            return;
        };
        out.insert(qualified_name.clone(), vss_path.to_string());
    }

    fn walk_named_node(
        node_name: &str,
        node: &serde_json::Value,
        path_segments: &mut Vec<String>,
        out: &mut HashMap<String, String>,
    ) {
        path_segments.push(node_name.to_string());
        let vss_path = path_segments.join(".");
        maybe_record_sig2vss(node, &vss_path, out);

        let serde_json::Value::Object(map) = node else {
            path_segments.pop();
            return;
        };

        if let Some(children) = map.get("children") {
            match children {
                serde_json::Value::Object(children_map) => {
                    for (child_name, child_node) in children_map {
                        walk_named_node(child_name, child_node, path_segments, out);
                    }
                }
                serde_json::Value::Array(children_arr) => {
                    for child in children_arr {
                        if let serde_json::Value::Object(child_obj) = child {
                            if let Some(serde_json::Value::String(name)) = child_obj.get("name") {
                                walk_named_node(name, child, path_segments, out);
                                continue;
                            }
                            if child_obj.len() == 1 {
                                if let Some((child_name, child_node)) = child_obj.iter().next() {
                                    walk_named_node(child_name, child_node, path_segments, out);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        path_segments.pop();
    }

    match root {
        serde_json::Value::Object(map) => {
            if map.contains_key("children") || map.contains_key("type") {
                if let Some(children) = map.get("children") {
                    match children {
                        serde_json::Value::Object(children_map) => {
                            for (child_name, child_node) in children_map {
                                walk_named_node(
                                    child_name,
                                    child_node,
                                    &mut Vec::new(),
                                    &mut result,
                                );
                            }
                        }
                        serde_json::Value::Array(children_arr) => {
                            for child in children_arr {
                                if let serde_json::Value::Object(child_obj) = child {
                                    if let Some(serde_json::Value::String(name)) =
                                        child_obj.get("name")
                                    {
                                        walk_named_node(name, child, &mut Vec::new(), &mut result);
                                        continue;
                                    }
                                    if child_obj.len() == 1 {
                                        if let Some((child_name, child_node)) =
                                            child_obj.iter().next()
                                        {
                                            walk_named_node(
                                                child_name,
                                                child_node,
                                                &mut Vec::new(),
                                                &mut result,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                for (node_name, node) in map {
                    walk_named_node(node_name, node, &mut Vec::new(), &mut result);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for node in arr {
                if let serde_json::Value::Object(obj) = node {
                    if obj.len() == 1 {
                        if let Some((node_name, node_value)) = obj.iter().next() {
                            walk_named_node(node_name, node_value, &mut Vec::new(), &mut result);
                        }
                    }
                }
            }
        }
        _ => {}
    }
    result
}

#[async_trait]
impl SinkConnector for KuksaSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        self.ensure_kuksa_client()?;
        self.ensure_provider_stream().await?;
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

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        let mapping = self
            .mapping
            .clone()
            .ok_or_else(|| SinkConnectorError::Other("kuksa sink mapping missing".to_string()))?;

        for tuple in collection.rows() {
            let updates = Self::iter_updates_for_row(mapping.as_ref(), tuple);
            if updates.is_empty() {
                continue;
            }
            self.update_vss_paths(&updates).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if let Some(handle) = self.response_task.take() {
            handle.abort();
        }
        self.provider_sender = None;
        self.client = None;
        tracing::info!(connector_id = %self.id, "kuksa sink closed");
        Ok(())
    }
}
