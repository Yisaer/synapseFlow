use base64::Engine;
use flow::codec::{
    CompressionCodec, EncryptionAlgorithm, InlineEncryptionKey, SecretEncoding,
    SinkEncryptionConfig,
};
use flow::pipeline::{
    HttpBodyConfig, HttpMultipartConfig, SinkRetryConfig, SourceDefinition, SourceInputConfig,
    SourceInputMode, SourceOnChangeConfig,
};
use flow::planner::sink::{
    CommonSinkProps, SinkDeltaOutputConfig, SinkOutputConfig, SinkOutputMode,
};
use flow::secret::{SecretContext, SecretRef};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineRequest {
    pub id: String,
    #[serde(deserialize_with = "crate::revision::deserialize_revision")]
    pub revision: u64,
    #[serde(default)]
    pub flow_instance_id: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub sources: Vec<CreatePipelineSourceRequest>,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
    #[serde(default)]
    pub options: PipelineOptionsRequest,
}

impl CreatePipelineRequest {
    pub(crate) fn normalize(&mut self) {
        for source in &mut self.sources {
            source.normalize();
        }
        for sink in &mut self.sinks {
            sink.normalize();
        }
        self.options.normalize();
    }
}

#[derive(Deserialize, Serialize)]
pub struct UpsertPipelineRequest {
    #[serde(deserialize_with = "crate::revision::deserialize_revision")]
    pub revision: u64,
    pub sql: String,
    #[serde(default)]
    pub sources: Vec<CreatePipelineSourceRequest>,
    #[serde(default)]
    pub sinks: Vec<CreatePipelineSinkRequest>,
    #[serde(default)]
    pub options: PipelineOptionsRequest,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PipelineScheduleRequest {
    /// 5-field cron expression: "min hour dom month dow".
    pub cron: String,
    /// How long each scheduled run lasts, in seconds.
    /// Must be greater than 0.
    pub duration_secs: u64,
    /// Absolute UTC timestamp ranges in which the cron windows are effective.
    /// Empty means no datetime restriction.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datetime_ranges: Vec<PipelineDatetimeRangeRequest>,
}

impl PipelineScheduleRequest {
    fn normalize(&mut self) {
        normalize_datetime_ranges(&mut self.datetime_ranges);
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct PipelineDatetimeRangeRequest {
    pub begin_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct PipelineOptionsRequest {
    #[serde(rename = "data_channel_capacity")]
    pub data_channel_capacity: usize,
    #[serde(default)]
    pub eventtime: EventtimeOptionsRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<PipelineScheduleRequest>,
}

impl Default for PipelineOptionsRequest {
    fn default() -> Self {
        Self {
            data_channel_capacity: 16,
            eventtime: EventtimeOptionsRequest::default(),
            schedule: None,
        }
    }
}

impl PipelineOptionsRequest {
    fn normalize(&mut self) {
        if let Some(schedule) = &mut self.schedule {
            schedule.normalize();
        }
    }
}

fn normalize_datetime_ranges(ranges: &mut Vec<PipelineDatetimeRangeRequest>) {
    if ranges.iter().any(|range| {
        range.begin_timestamp_ms < 0
            || range.end_timestamp_ms < 0
            || range.begin_timestamp_ms >= range.end_timestamp_ms
    }) {
        return;
    }

    ranges.sort_by_key(|range| (range.begin_timestamp_ms, range.end_timestamp_ms));
    let mut merged: Vec<PipelineDatetimeRangeRequest> = Vec::with_capacity(ranges.len());

    for range in ranges.drain(..) {
        if let Some(last) = merged.last_mut()
            && range.begin_timestamp_ms <= last.end_timestamp_ms
        {
            last.end_timestamp_ms = last.end_timestamp_ms.max(range.end_timestamp_ms);
            continue;
        }
        merged.push(range);
    }

    *ranges = merged;
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct EventtimeOptionsRequest {
    pub enabled: bool,
    pub late_tolerance_ms: u64,
}

#[derive(Serialize)]
pub struct CreatePipelineResponse {
    pub id: String,
    pub revision: u64,
    pub status: String,
}

#[derive(Serialize)]
pub struct ListPipelineItem {
    pub id: String,
    pub revision: u64,
    pub status: String,
    pub flow_instance_id: String,
}

#[derive(Serialize)]
pub struct GetPipelineResponse {
    pub id: String,
    pub revision: u64,
    pub status: String,
    pub spec: CreatePipelineRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_status: Option<ScheduleStatus>,
}

/// Scheduling status for a pipeline (returned in GET response).
#[derive(Serialize)]
pub struct ScheduleStatus {
    pub cron: String,
    pub duration_secs: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub datetime_ranges: Vec<PipelineDatetimeRangeRequest>,
    /// Whether current time falls within an active scheduling window.
    pub in_window: bool,
    /// Timestamp of the last (or current) cron fire, in RFC 3339 UTC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_fire_at: Option<String>,
    /// Timestamp of the next cron fire, in RFC 3339 UTC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<String>,
    /// Timestamp when the current scheduled run will be auto-stopped, in RFC 3339 UTC.
    /// Only present when in_window is true and the run was started by the scheduler.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_stop_at: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct VideoSinkPropsRequest {
    pub path: Option<String>,
    pub filename_prefix: Option<String>,
    pub codec: String,
    pub container: String,
    pub rolling: VideoRollingRequest,
}

impl Default for VideoSinkPropsRequest {
    fn default() -> Self {
        Self {
            path: None,
            filename_prefix: None,
            codec: "h264".to_string(),
            container: "mp4".to_string(),
            rolling: VideoRollingRequest::default(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct VideoRollingRequest {
    #[serde(rename = "type")]
    pub rolling_type: String,
    pub seconds: u64,
}

impl Default for VideoRollingRequest {
    fn default() -> Self {
        Self {
            rolling_type: "duration".to_string(),
            seconds: 60,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub(crate) struct CreatePipelineQuery {
    /// When true, the pipeline is started immediately after creation.
    /// Must not be combined with a schedule.
    pub(crate) start: bool,
}

#[derive(Deserialize)]
#[serde(default)]
pub(crate) struct CollectStatsQuery {
    pub(crate) timeout_ms: u64,
}

impl Default for CollectStatsQuery {
    fn default() -> Self {
        Self { timeout_ms: 5_000 }
    }
}

#[derive(Deserialize)]
#[serde(default)]
pub(crate) struct StopPipelineQuery {
    pub(crate) mode: String,
    pub(crate) timeout_ms: u64,
}

impl Default for StopPipelineQuery {
    fn default() -> Self {
        Self {
            mode: "quick".to_string(),
            timeout_ms: 5_000,
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineSourceRequest {
    pub stream: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<SourceInputConfigRequest>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceInputConfigRequest {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_change: Option<SourceOnChangeConfigRequest>,
}

impl SourceInputConfigRequest {
    pub(super) fn to_input_config(&self) -> Result<SourceInputConfig, String> {
        match self.mode.trim().to_ascii_lowercase().as_str() {
            "full" => {
                if self.on_change.is_some() {
                    return Err(
                        "source input.on_change is only supported when input.mode=on_change"
                            .to_string(),
                    );
                }
                Ok(SourceInputConfig::new(SourceInputMode::Full))
            }
            "on_change" => Ok(SourceInputConfig {
                mode: SourceInputMode::OnChange,
                on_change: self.on_change.as_ref().map(|cfg| SourceOnChangeConfig {
                    columns: cfg.columns.clone(),
                }),
            }),
            other => Err(format!(
                "invalid source input.mode `{other}` (expected full|on_change)"
            )),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceOnChangeConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

impl CreatePipelineSourceRequest {
    pub(crate) fn normalized_stream(&self) -> &str {
        self.stream.trim()
    }

    pub(crate) fn normalize(&mut self) {
        self.stream = self.normalized_stream().to_string();
    }

    pub(super) fn to_source_definition(&self) -> Result<SourceDefinition, String> {
        let input = self
            .input
            .as_ref()
            .map(SourceInputConfigRequest::to_input_config)
            .transpose()?
            .unwrap_or_default();
        Ok(SourceDefinition::new(self.normalized_stream().to_string()).with_input(input))
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineSinkRequest {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub sink_type: String,
    #[serde(default)]
    pub props: SinkPropsRequest,
    #[serde(rename = "common_sink_props", default)]
    pub common: CommonSinkPropsRequest,
    #[serde(default)]
    pub encoder: EncoderConfigRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<SinkOutputConfigRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<SinkDeliveryConfigRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<SinkRetryConfigRequest>,
}

impl CreatePipelineSinkRequest {
    pub(crate) fn normalize(&mut self) {
        if !self.sink_type.eq_ignore_ascii_case("mqtt") {
            return;
        }

        let normalized = match self.props.fields.get("connector_key") {
            Some(JsonValue::String(value)) => {
                let trimmed = value.trim();
                Some((!trimmed.is_empty()).then(|| JsonValue::String(trimmed.to_string())))
            }
            _ => None,
        };

        match normalized {
            Some(Some(value)) => {
                self.props.fields.insert("connector_key".to_string(), value);
            }
            Some(None) => {
                self.props.fields.remove("connector_key");
            }
            None => {}
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct EncoderConfigRequest {
    #[serde(rename = "type")]
    pub encode_type: String,
    pub props: JsonMap<String, JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<EncoderTransformRequest>,
}

impl EncoderConfigRequest {
    fn new(encode_type: impl Into<String>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            encode_type: encode_type.into(),
            props,
            transform: None,
        }
    }
}

impl Default for EncoderConfigRequest {
    fn default() -> Self {
        Self::new("json", JsonMap::new())
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EncoderTransformRequest {
    pub template: String,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkOutputConfigRequest {
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<SinkDeltaOutputConfigRequest>,
    #[serde(default)]
    pub omit_if_empty: bool,
    #[serde(default)]
    pub include_columns: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_columns: Option<Vec<String>>,
}

impl SinkOutputConfigRequest {
    pub(super) fn to_output_config(&self) -> Result<SinkOutputConfig, String> {
        match self.mode.trim().to_ascii_lowercase().as_str() {
            "full" => {
                if self.delta.is_some() {
                    return Err(
                        "sink output.delta is only supported when output.mode=delta".to_string()
                    );
                }
                let mut config = SinkOutputConfig::new(SinkOutputMode::Full)
                    .with_omit_if_empty(self.omit_if_empty);
                if let Some(include) = &self.include_columns {
                    config = config.with_include_columns(include.iter().cloned());
                }
                if let Some(exclude) = &self.exclude_columns {
                    config = config.with_exclude_columns(exclude.iter().cloned());
                }
                config.validate()?;
                Ok(config)
            }
            "delta" => {
                let mut config = SinkOutputConfig {
                    mode: SinkOutputMode::Delta,
                    delta: self.delta.as_ref().map(|delta| SinkDeltaOutputConfig {
                        columns: delta.columns.clone(),
                    }),
                    omit_if_empty: self.omit_if_empty,
                    include_columns: None,
                    exclude_columns: None,
                };
                if let Some(include) = &self.include_columns {
                    config = config.with_include_columns(include.iter().cloned());
                }
                if let Some(exclude) = &self.exclude_columns {
                    config = config.with_exclude_columns(exclude.iter().cloned());
                }
                config.validate()?;
                Ok(config)
            }
            other => Err(format!(
                "invalid sink output.mode `{other}` (expected full|delta)"
            )),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkDeltaOutputConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkDeliveryConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<SinkCompressionConfigRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<SinkEncryptionConfigRequest>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkRetryConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_backoff_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_backoff_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jitter: Option<bool>,
}

impl SinkRetryConfigRequest {
    pub(super) fn to_retry_config(&self) -> Result<SinkRetryConfig, String> {
        let mut config = SinkRetryConfig {
            max_attempts: self.max_attempts,
            ..SinkRetryConfig::default()
        };
        if let Some(initial_backoff_ms) = self.initial_backoff_ms {
            config.initial_backoff_ms = initial_backoff_ms;
        }
        if let Some(max_backoff_ms) = self.max_backoff_ms {
            config.max_backoff_ms = max_backoff_ms;
        }
        if let Some(jitter) = self.jitter {
            config.jitter = jitter;
        }
        config.validate()?;
        Ok(config)
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkCompressionConfigRequest {
    pub codec: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<i32>,
}

impl SinkCompressionConfigRequest {
    pub(super) fn to_compression_codec(&self) -> Result<CompressionCodec, String> {
        match self.codec.trim().to_ascii_lowercase().as_str() {
            "gzip" => match self.level {
                Some(level) if !(0..=9).contains(&level) => Err(format!(
                    "invalid delivery.compression.level `{level}` for gzip"
                )),
                Some(level) => Ok(CompressionCodec::gzip_with_level(level as u32)),
                None => Ok(CompressionCodec::gzip()),
            },
            "zstd" => Ok(match self.level {
                Some(level) => CompressionCodec::zstd_with_level(level),
                None => CompressionCodec::zstd(),
            }),
            other => Err(format!(
                "invalid delivery.compression.codec `{other}` (expected gzip|zstd)"
            )),
        }
    }
}

/// Sink delivery encryption config, modeled as a discriminated union keyed on
/// `algorithm` (VF-51). `algorithm` is required. One config selects exactly one
/// algorithm, and each variant carries only its own parameters — adding a future
/// suite (e.g. `chacha20-poly1305`, `aes-cbc-hmac` with its own `mac_key`) does
/// not touch the existing variants. The IV/nonce is never a config field; it is
/// generated per delivery and embedded in the ciphertext header.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(tag = "algorithm", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SinkEncryptionConfigRequest {
    /// AES-GCM (AEAD). `key` is base64-encoded key bytes; the decoded length
    /// selects AES-128/192/256.
    AesGcm {
        /// Key material: `store:NAME` (recommended) or an inline base64 literal.
        key: SecretRef,
    },
}

impl SinkEncryptionConfigRequest {
    pub(super) fn to_encryption_config(
        &self,
        secrets: &SecretContext,
    ) -> Result<SinkEncryptionConfig, String> {
        match self {
            SinkEncryptionConfigRequest::AesGcm { key } => {
                // The store name is a stable, non-secret key identifier; inline
                // keys have no name, so they fall back to a constant id.
                let key_id = key.store_name().unwrap_or("inline").to_string();
                // Resolve the key material (store ref or inline literal, subject
                // to the policy). The resolved value is base64-encoded key bytes.
                let (resolved, warning) = secrets
                    .resolve(key, "delivery.encryption.key")
                    .map_err(|err| err.to_string())?;
                if let Some(message) = warning {
                    tracing::warn!(target: "veloflux::secret", "{message}");
                }
                SinkEncryptionConfig::from_inline_key(
                    EncryptionAlgorithm::AesGcm,
                    key_id,
                    InlineEncryptionKey::new(resolved.expose().to_string(), SecretEncoding::Base64),
                )
                .map_err(|err| err.to_string())
            }
        }
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct SinkPropsRequest {
    #[serde(flatten)]
    fields: JsonMap<String, JsonValue>,
}

impl SinkPropsRequest {
    pub(super) fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MqttSinkPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub retain: Option<bool>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
    pub max_packet_size: Option<usize>,
    pub protocol_version: Option<flow::MqttProtocolVersion>,
    pub user_properties: Vec<flow::MqttUserProperty>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct NopSinkPropsRequest {
    pub log: Option<bool>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct MemorySinkPropsRequest {
    pub topic: String,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct FileSinkPropsRequest {
    pub path: Option<String>,
    pub filename_prefix: Option<String>,
    pub filename_suffix: Option<String>,
    pub retention: FileRetentionConfigRequest,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct FileRetentionConfigRequest {
    pub max_file_count: Option<u64>,
    pub max_file_age_days: Option<u64>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct KuraSinkPropsRequest {
    pub addr: Option<String>,
    #[serde(rename = "mapping_path")]
    pub mapping_path: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct NngPubSubSinkPropsRequest {
    pub url: Option<String>,
    pub topic: Option<String>,
    pub topic_delimiter: Option<String>,
    #[serde(rename = "topicDelimiter")]
    pub topic_delimiter_camel: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct HttpSinkPropsRequest {
    pub url: Option<String>,
    pub method: Option<String>,
    pub timeout_secs: Option<u64>,
    pub headers: Option<BTreeMap<String, String>>,
    /// Structured authentication (bearer/basic). Secret material is a `SecretRef`.
    pub auth: Option<HttpAuthRequest>,
    /// Catch-all for custom auth headers: header name -> secret reference.
    pub secret_headers: Option<BTreeMap<String, SecretRef>>,
    pub content_type: Option<String>,
    pub max_body_size: Option<usize>,
    pub retry_max_attempts: Option<usize>,
    pub retry_backoff_ms: Option<u64>,
    pub retry_max_backoff_ms: Option<u64>,
    pub body: Option<HttpBodyConfigRequest>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpBodyConfigRequest {
    Raw,
    Multipart {
        file_field_name: String,
        #[serde(default = "default_multipart_file_name")]
        file_name: String,
        #[serde(default)]
        fields: BTreeMap<String, String>,
    },
}

fn default_multipart_file_name() -> String {
    "payload.bin".to_string()
}

/// Header names that must not appear in the plain `headers` map (VF-51 §7.4):
/// they carry secrets and belong in `auth` / `secret_headers`.
const SENSITIVE_HEADER_NAMES: &[&str] = &["authorization", "proxy-authorization", "cookie"];

/// Structured HTTP authentication. The secret is always a `SecretRef`.
#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum HttpAuthRequest {
    Bearer {
        token: SecretRef,
    },
    Basic {
        username: String,
        password: SecretRef,
    },
}

impl HttpAuthRequest {
    /// Resolve into the `Authorization` header `(name, value)`.
    fn to_header(&self, secrets: &SecretContext) -> Result<(String, String), String> {
        let value = match self {
            HttpAuthRequest::Bearer { token } => {
                let (v, warning) = secrets
                    .resolve(token, "http.auth.token")
                    .map_err(|e| e.to_string())?;
                log_secret_warning(warning);
                format!("Bearer {}", v.expose())
            }
            HttpAuthRequest::Basic { username, password } => {
                let (v, warning) = secrets
                    .resolve(password, "http.auth.password")
                    .map_err(|e| e.to_string())?;
                log_secret_warning(warning);
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{}", v.expose()));
                format!("Basic {encoded}")
            }
        };
        Ok(("Authorization".to_string(), value))
    }
}

fn log_secret_warning(warning: Option<String>) {
    if let Some(message) = warning {
        tracing::warn!(target: "veloflux::secret", "{message}");
    }
}

impl HttpSinkPropsRequest {
    pub(super) fn to_body_config(&self) -> Result<HttpBodyConfig, String> {
        let Some(body) = &self.body else {
            return Ok(HttpBodyConfig::Raw);
        };

        match body {
            HttpBodyConfigRequest::Raw => Ok(HttpBodyConfig::Raw),
            HttpBodyConfigRequest::Multipart {
                file_field_name,
                file_name,
                fields,
            } => {
                if self
                    .content_type
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                {
                    return Err("http multipart body does not allow props.content_type".to_string());
                }
                if self.headers.as_ref().is_some_and(|headers| {
                    headers
                        .keys()
                        .any(|name| name.trim().eq_ignore_ascii_case("content-type"))
                }) {
                    return Err(
                        "http multipart body does not allow a Content-Type header".to_string()
                    );
                }

                let file_field_name = normalize_multipart_name(file_field_name, "file_field_name")?;
                let file_name = normalize_multipart_name(file_name, "file_name")?;
                let mut normalized_fields = BTreeMap::new();
                for (name, value) in fields {
                    let name = normalize_multipart_name(name, "text field name")?;
                    if name == file_field_name {
                        return Err(format!(
                            "http multipart text field `{name}` conflicts with file_field_name"
                        ));
                    }
                    if normalized_fields
                        .insert(name.clone(), flow::ConnectorString::plain(value.clone()))
                        .is_some()
                    {
                        return Err(format!(
                            "http multipart text field `{name}` is duplicated after trimming"
                        ));
                    }
                }

                Ok(HttpBodyConfig::Multipart(HttpMultipartConfig {
                    file_field_name,
                    file_name,
                    fields: normalized_fields,
                }))
            }
        }
    }

    /// Reject sensitive auth headers placed in the plain `headers` map; they must
    /// use `auth`/`secret_headers` so the value never lands in scannable config.
    pub(super) fn reject_sensitive_plain_headers(&self) -> Result<(), String> {
        if let Some(headers) = &self.headers {
            for name in headers.keys() {
                if SENSITIVE_HEADER_NAMES.contains(&name.to_ascii_lowercase().as_str()) {
                    return Err(format!(
                        "http sink header `{name}` carries a secret; use `auth` or `secret_headers`"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Resolve `auth` and `secret_headers` into concrete header `(name, value)`
    /// pairs (runtime only; never persisted).
    pub(super) fn resolve_secret_headers(
        &self,
        secrets: &SecretContext,
    ) -> Result<Vec<(String, String)>, String> {
        let mut out = Vec::new();
        if let Some(auth) = &self.auth {
            out.push(auth.to_header(secrets)?);
        }
        if let Some(secret_headers) = &self.secret_headers {
            for (name, reference) in secret_headers {
                let (value, warning) = secrets
                    .resolve(reference, "http.secret_headers")
                    .map_err(|e| e.to_string())?;
                log_secret_warning(warning);
                out.push((name.clone(), value.expose().to_string()));
            }
        }
        Ok(out)
    }
}

fn normalize_multipart_name(value: &str, field: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("http multipart {field} must not be empty"));
    }
    if value
        .chars()
        .any(|character| matches!(character, '\r' | '\n' | '\0'))
    {
        return Err(format!(
            "http multipart {field} must not contain CR, LF, or NUL"
        ));
    }
    Ok(value.to_string())
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct CommonSinkPropsRequest {
    #[serde(rename = "batch_count")]
    pub batch_count: Option<usize>,
    #[serde(rename = "batch_duration")]
    pub batch_duration_ms: Option<u64>,
}

impl CommonSinkPropsRequest {
    pub(super) fn to_common_props(&self) -> CommonSinkProps {
        let duration = self.batch_duration_ms.map(Duration::from_millis);
        CommonSinkProps {
            batch_count: self.batch_count,
            batch_duration: duration,
        }
    }
}

#[cfg(test)]
mod secret_tests {
    use super::*;
    use flow::secret::{SecretContext, SecretPolicy, SecretStore};
    use std::sync::Arc;

    fn ctx_with(name: &str, value: &str) -> SecretContext {
        let mut store = SecretStore::empty();
        store.set(name, value);
        SecretContext::new(Arc::new(store), SecretPolicy::Warn)
    }

    fn encryption_req(key_value: SecretRef) -> SinkEncryptionConfigRequest {
        SinkEncryptionConfigRequest::AesGcm { key: key_value }
    }

    #[test]
    fn sink_encryption_key_resolves_from_store() {
        // Stored value is the base64-encoded 32-byte key text.
        let key_b64 = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        let ctx = ctx_with("sink-aes-key", &key_b64);
        let cfg = encryption_req(SecretRef::store("sink-aes-key"))
            .to_encryption_config(&ctx)
            .expect("resolve store key");
        assert_eq!(cfg.key_bits, 256);
        // key_id is derived from the store name.
        assert_eq!(cfg.key_id, "sink-aes-key");
    }

    #[test]
    fn sink_encryption_parses_explicit_algorithm() {
        let req: SinkEncryptionConfigRequest =
            serde_json::from_str(r#"{"algorithm":"aes-gcm","key":"store:k"}"#).unwrap();
        assert_eq!(
            req,
            SinkEncryptionConfigRequest::AesGcm {
                key: SecretRef::store("k")
            }
        );
    }

    #[test]
    fn sink_encryption_requires_algorithm() {
        // `algorithm` is mandatory; omitting it is rejected.
        assert!(
            serde_json::from_str::<SinkEncryptionConfigRequest>(r#"{"key":"store:k"}"#).is_err()
        );
    }

    #[test]
    fn sink_encryption_rejects_unknown_algorithm_and_fields() {
        // Unknown algorithm tag -> rejected (no such variant).
        assert!(
            serde_json::from_str::<SinkEncryptionConfigRequest>(
                r#"{"algorithm":"rot13","key":"store:k"}"#
            )
            .is_err()
        );
        // Field not belonging to the selected algorithm -> rejected.
        assert!(
            serde_json::from_str::<SinkEncryptionConfigRequest>(
                r#"{"algorithm":"aes-gcm","key":"store:k","mac_key":"store:m"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn sink_encryption_inline_key_id_is_constant() {
        let key_b64 = base64::engine::general_purpose::STANDARD.encode([1u8; 16]);
        let cfg = encryption_req(SecretRef::inline(key_b64))
            .to_encryption_config(&SecretContext::empty())
            .expect("inline key under warn");
        assert_eq!(cfg.key_id, "inline");
    }

    #[test]
    fn sink_encryption_inline_key_still_works() {
        let key_b64 = base64::engine::general_purpose::STANDARD.encode([1u8; 16]);
        let ctx = SecretContext::empty(); // warn policy, empty store
        let cfg = encryption_req(SecretRef::inline(key_b64))
            .to_encryption_config(&ctx)
            .expect("inline key under warn");
        assert_eq!(cfg.key_bits, 128);
    }

    #[test]
    fn sink_encryption_missing_store_key_errors() {
        let ctx = ctx_with("other", "x");
        let err = encryption_req(SecretRef::store("does-not-exist"))
            .to_encryption_config(&ctx)
            .unwrap_err();
        assert!(err.contains("does-not-exist"), "{err}");
    }

    #[test]
    fn sink_encryption_request_serializes_with_pointer() {
        let json =
            serde_json::to_string(&encryption_req(SecretRef::store("sink-aes-key"))).unwrap();
        assert!(json.contains("store:sink-aes-key"), "{json}");
    }

    #[test]
    fn rejects_sensitive_plain_headers() {
        let mut req = HttpSinkPropsRequest::default();
        let mut headers = BTreeMap::new();
        headers.insert("Authorization".to_string(), "Bearer leak".to_string());
        req.headers = Some(headers);
        let err = req.reject_sensitive_plain_headers().unwrap_err();
        assert!(err.contains("Authorization"));
        // The plain value is not echoed.
        assert!(!err.contains("leak"));
    }

    #[test]
    fn bearer_auth_resolves_to_authorization_header() {
        let ctx = ctx_with("api-token", "t0ken");
        let req = HttpSinkPropsRequest {
            auth: Some(HttpAuthRequest::Bearer {
                token: SecretRef::store("api-token"),
            }),
            ..Default::default()
        };
        let headers = req.resolve_secret_headers(&ctx).unwrap();
        assert_eq!(
            headers,
            vec![("Authorization".to_string(), "Bearer t0ken".to_string())]
        );
    }

    #[test]
    fn basic_auth_encodes_credentials() {
        let ctx = ctx_with("pw", "s3cr3t");
        let req = HttpSinkPropsRequest {
            auth: Some(HttpAuthRequest::Basic {
                username: "alice".to_string(),
                password: SecretRef::store("pw"),
            }),
            ..Default::default()
        };
        let headers = req.resolve_secret_headers(&ctx).unwrap();
        let expected = base64::engine::general_purpose::STANDARD.encode("alice:s3cr3t");
        assert_eq!(headers[0].1, format!("Basic {expected}"));
    }

    #[test]
    fn secret_headers_resolve_from_store() {
        let ctx = ctx_with("xkey", "xyz");
        let mut secret_headers = BTreeMap::new();
        secret_headers.insert("X-Api-Key".to_string(), SecretRef::store("xkey"));
        let req = HttpSinkPropsRequest {
            secret_headers: Some(secret_headers),
            ..Default::default()
        };
        let headers = req.resolve_secret_headers(&ctx).unwrap();
        assert_eq!(headers, vec![("X-Api-Key".to_string(), "xyz".to_string())]);
    }

    #[test]
    fn auth_request_serializes_with_secret_pointer() {
        let req = HttpSinkPropsRequest {
            auth: Some(HttpAuthRequest::Bearer {
                token: SecretRef::store("api-token"),
            }),
            ..Default::default()
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("store:api-token"));
        assert!(json.contains("\"type\":\"bearer\""));
    }

    #[test]
    fn http_body_defaults_to_raw() {
        let req = HttpSinkPropsRequest::default();
        assert_eq!(req.to_body_config().unwrap(), HttpBodyConfig::Raw);
    }

    #[test]
    fn multipart_body_uses_default_file_name_and_normalizes_names() {
        let req: HttpSinkPropsRequest = serde_json::from_value(serde_json::json!({
            "body": {
                "type": "multipart",
                "file_field_name": " d ",
                "fields": {
                    " rid ": "cold",
                    "tp": ""
                }
            }
        }))
        .unwrap();

        assert_eq!(
            req.to_body_config().unwrap(),
            HttpBodyConfig::Multipart(HttpMultipartConfig {
                file_field_name: "d".to_string(),
                file_name: "payload.bin".to_string(),
                fields: BTreeMap::from([
                    ("rid".to_string(), flow::ConnectorString::plain("cold")),
                    ("tp".to_string(), flow::ConnectorString::plain("")),
                ]),
            })
        );
    }

    #[test]
    fn multipart_body_rejects_conflicting_field_names() {
        let req: HttpSinkPropsRequest = serde_json::from_value(serde_json::json!({
            "body": {
                "type": "multipart",
                "file_field_name": "d",
                "fields": {
                    " d ": "value"
                }
            }
        }))
        .unwrap();

        let error = req.to_body_config().unwrap_err();
        assert!(error.contains("conflicts with file_field_name"), "{error}");
        assert!(!error.contains("value"), "{error}");
    }

    #[test]
    fn multipart_body_rejects_content_type_override() {
        let req: HttpSinkPropsRequest = serde_json::from_value(serde_json::json!({
            "content_type": "multipart/form-data",
            "body": {
                "type": "multipart",
                "file_field_name": "d"
            }
        }))
        .unwrap();

        let error = req.to_body_config().unwrap_err();
        assert!(error.contains("props.content_type"), "{error}");
    }

    #[test]
    fn multipart_body_rejects_unknown_fields() {
        let error = serde_json::from_value::<HttpSinkPropsRequest>(serde_json::json!({
            "body": {
                "type": "multipart",
                "file_field_name": "d",
                "dynamic_field": "unsupported"
            }
        }))
        .err()
        .expect("unknown multipart field should fail");

        assert!(error.to_string().contains("unknown field"), "{error}");
    }
}
