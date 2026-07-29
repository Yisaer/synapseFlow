use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamCreateRequest {
    pub name: String,
    pub revision: u64,
    #[serde(rename = "type")]
    pub stream_type: String,
    pub schema: JsonValue,
    pub props: JsonValue,
    pub shared: bool,
    pub decoder: JsonValue,
}

impl StreamCreateRequest {
    pub fn mock_shared_i64_value(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name,
            revision: 1,
            stream_type: "mock".to_string(),
            schema: serde_json::json!({
                "type": "json",
                "props": {
                    "columns": [
                        { "name": "value", "data_type": "int64" }
                    ]
                }
            }),
            props: serde_json::json!({}),
            shared: true,
            decoder: serde_json::json!({ "type": "json", "props": {} }),
        }
    }

    pub fn mock_non_shared_i64_value(name: impl Into<String>) -> Self {
        let mut req = Self::mock_shared_i64_value(name);
        req.shared = false;
        req
    }
}

/// Request body for `PUT /streams/:name`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamUpsertRequest {
    pub revision: u64,
    pub schema: JsonValue,
    pub props: JsonValue,
    pub decoder: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared: Option<bool>,
}

impl StreamUpsertRequest {
    pub fn from_create(req: &StreamCreateRequest) -> Self {
        Self {
            revision: req.revision.saturating_add(1),
            schema: req.schema.clone(),
            props: req.props.clone(),
            decoder: req.decoder.clone(),
            shared: Some(req.shared),
        }
    }

    pub fn with_shared(mut self, shared: bool) -> Self {
        self.shared = Some(shared);
        self
    }
}
