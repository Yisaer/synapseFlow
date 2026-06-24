use flow::codec::{
    CompressionCodec, EncryptionAlgorithm, InlineEncryptionKey, SecretEncoding,
    SinkEncryptionConfig,
};
use flow::connector::SharedMqttClientConfig;
use flow::pipeline::{SourceDefinition, SourceInputConfig, SourceInputMode, SourceOnChangeConfig};
use flow::planner::sink::{
    CommonSinkProps, SinkDeltaOutputConfig, SinkOutputConfig, SinkOutputMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Deserialize, Serialize, Clone)]
pub struct CreatePipelineRequest {
    pub id: String,
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
    }
}

#[derive(Deserialize, Serialize)]
pub struct UpsertPipelineRequest {
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

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct EventtimeOptionsRequest {
    pub enabled: bool,
    pub late_tolerance_ms: u64,
}

#[derive(Serialize)]
pub struct CreatePipelineResponse {
    pub id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct ListPipelineItem {
    pub id: String,
    pub status: String,
    pub flow_instance_id: String,
}

#[derive(Serialize)]
pub struct GetPipelineResponse {
    pub id: String,
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

#[derive(Serialize)]
pub struct BuildPipelineContextResponse {
    pub pipeline: CreatePipelineRequest,
    pub streams: BTreeMap<String, crate::stream::CreateStreamRequest>,
    pub shared_mqtt_clients: Vec<SharedMqttClientConfig>,
    pub memory_topics: Vec<MemoryTopicSpec>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MemoryTopicSpec {
    pub topic: String,
    pub kind: storage::StoredMemoryTopicKind,
    pub capacity: usize,
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
                Ok(SinkOutputConfig::new(SinkOutputMode::Full)
                    .with_omit_if_empty(self.omit_if_empty))
            }
            "delta" => Ok(SinkOutputConfig {
                mode: SinkOutputMode::Delta,
                delta: self.delta.as_ref().map(|delta| SinkDeltaOutputConfig {
                    columns: delta.columns.clone(),
                }),
                omit_if_empty: self.omit_if_empty,
            }),
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

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkEncryptionConfigRequest {
    pub algorithm: String,
    pub key_id: String,
    pub key: InlineEncryptionKeyRequest,
}

impl SinkEncryptionConfigRequest {
    pub(super) fn to_encryption_config(&self) -> Result<SinkEncryptionConfig, String> {
        let algorithm = EncryptionAlgorithm::try_from(self.algorithm.as_str())
            .map_err(|err| err.to_string())?;
        let key_id = self.key_id.trim();
        if key_id.is_empty() {
            return Err("delivery.encryption.key_id must be non-empty".to_string());
        }
        let encoding =
            SecretEncoding::try_from(self.key.encoding.as_str()).map_err(|err| err.to_string())?;
        let config = SinkEncryptionConfig::from_inline_key(
            algorithm,
            key_id.to_string(),
            InlineEncryptionKey::new(self.key.value.clone(), encoding),
        )
        .map_err(|err| err.to_string())?;
        Ok(config)
    }
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InlineEncryptionKeyRequest {
    pub value: String,
    pub encoding: String,
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
    pub content_type: Option<String>,
    pub max_body_size: Option<usize>,
    pub retry_max_attempts: Option<usize>,
    pub retry_backoff_ms: Option<u64>,
    pub retry_max_backoff_ms: Option<u64>,
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
