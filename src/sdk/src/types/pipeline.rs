use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineCreateRequest {
    pub id: String,
    pub revision: u64,
    pub sql: String,
    pub sinks: Vec<serde_json::Value>,
    #[serde(default)]
    pub flow_instance_id: Option<String>,
}

impl PipelineCreateRequest {
    pub fn nop(id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            revision: 1,
            sql: sql.into(),
            sinks: vec![serde_json::json!({ "type": "nop" })],
            flow_instance_id: None,
        }
    }

    pub fn with_flow_instance_id(mut self, id: impl Into<String>) -> Self {
        self.flow_instance_id = Some(id.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineUpsertRequest {
    pub revision: u64,
    pub sql: String,
    pub sinks: Vec<JsonValue>,

    #[serde(default)]
    pub options: JsonValue,
}

impl PipelineUpsertRequest {
    pub fn nop(sql: impl Into<String>) -> Self {
        Self {
            revision: 2,
            sql: sql.into(),
            sinks: vec![serde_json::json!({ "type": "nop" })],
            options: serde_json::json!({}),
        }
    }

    pub fn with_options(mut self, options: JsonValue) -> Self {
        self.options = options;
        self
    }
}

#[derive(Clone, Debug)]
pub enum StopMode {
    Graceful,
}

impl StopMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            StopMode::Graceful => "graceful",
        }
    }
}

#[derive(Clone, Debug)]
pub struct StopOptions {
    pub mode: StopMode,
    pub timeout_ms: u64,
}

impl StopOptions {
    pub fn graceful(timeout_ms: u64) -> Self {
        Self {
            mode: StopMode::Graceful,
            timeout_ms,
        }
    }

    pub fn to_query_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("mode", self.mode.as_str().to_string()),
            ("timeout_ms", self.timeout_ms.to_string()),
        ]
    }
}
