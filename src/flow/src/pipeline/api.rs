use crate::catalog::Catalog;
use crate::codec::{CompressionCodec, SinkEncryptionConfig};
pub use crate::planner::sink::SinkRetryConfig;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig, SinkOutputConfig};
use crate::PipelineRegistries;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::RwLock;
use std::time::Duration;
use url::Url;

/// Errors that can occur when mutating pipeline definitions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PipelineError {
    #[error("pipeline already exists: {0}")]
    AlreadyExists(String),
    #[error("pipeline not found: {0}")]
    NotFound(String),
    #[error("pipeline build error: {0}")]
    BuildFailure(String),
    #[error("pipeline runtime error: {0}")]
    Runtime(String),
}

/// Supported sink types for pipeline outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    /// MQTT sink.
    Mqtt,
    /// No-op sink that discards payloads.
    Nop,
    /// Kuksa sink that updates VSS paths via kuksa.val.v2.
    Kuksa,
    /// Kura sink that sends VSS values to a kura server via gRPC (yoriito VISS producer).
    Kura,
    /// Memory sink that publishes to an in-process pub/sub topic.
    Memory,
    /// File sink that writes encoded payloads to local files.
    File,
    /// Video sink that records video frame tuples.
    Video,
    /// NNG pub/sub sink.
    NngPubSub,
    /// HTTP sink that delivers encoded payloads to a remote endpoint.
    Http,
}

/// Sink configuration payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkProps {
    /// MQTT sink configuration.
    Mqtt(MqttSinkProps),
    /// No-op sink config.
    Nop(NopSinkProps),
    /// Kuksa sink config.
    Kuksa(KuksaSinkProps),
    /// Kura sink config.
    Kura(KuraSinkProps),
    /// Memory sink config.
    Memory(MemorySinkProps),
    /// File sink config.
    File(FileSinkProps),
    /// Video sink config.
    Video(VideoSinkProps),
    /// NNG pub/sub sink config.
    NngPubSub(NngPubSubSinkProps),
    /// HTTP sink config.
    Http(HttpSinkProps),
}

/// Runtime state for pipeline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Stopped,
    Running,
}

#[derive(Debug, Clone)]
pub struct CreatePipelineRequest {
    pub definition: PipelineDefinition,
}

impl CreatePipelineRequest {
    pub fn new(definition: PipelineDefinition) -> Self {
        Self { definition }
    }
}

#[derive(Debug, Clone)]
pub struct CreatePipelineResult {
    pub snapshot: PipelineSnapshot,
}

pub enum ExplainPipelineTarget<'a> {
    Id(&'a str),
    Definition(&'a PipelineDefinition),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStopMode {
    Graceful,
    Quick,
}

/// Concrete MQTT sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSinkProps {
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
    pub max_packet_size: Option<usize>,
}

/// Concrete Nop sink configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NopSinkProps {
    pub log: bool,
}

/// Concrete Kuksa sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KuksaSinkProps {
    pub addr: String,
    pub vss_path: String,
}

/// Concrete Kura sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KuraSinkProps {
    pub addr: String,
    pub mapping_path: String,
}

/// Concrete memory sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySinkProps {
    pub topic: String,
}

/// Concrete file sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSinkProps {
    pub path: String,
    pub filename_prefix: String,
    pub filename_suffix: String,
    pub retention: FileRetentionConfig,
}

/// File sink retention configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileRetentionConfig {
    pub max_file_count: u64,
    pub max_file_age_days: u64,
}

/// Concrete video sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoSinkProps {
    pub path: String,
    pub filename_prefix: Option<String>,
    pub codec: VideoCodec,
    pub container: VideoContainer,
    pub rolling: VideoRollingConfig,
}

/// Concrete NNG pub/sub sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NngPubSubSinkProps {
    pub url: String,
    pub topic: String,
    pub topic_delimiter: String,
}

/// Concrete HTTP sink configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSinkProps {
    /// Target URL (required).
    pub url: String,
    /// HTTP method ("POST", "PUT", etc.), defaults to "POST".
    pub method: Option<String>,
    /// Request timeout in seconds, defaults to 30.
    pub timeout_secs: Option<u64>,
    /// Additional headers to include in every request.
    pub headers: HashMap<String, String>,
    /// Explicit Content-Type. When `None`, inferred from the encoder kind.
    pub content_type: Option<String>,
    /// Maximum single-delivery body size in bytes, defaults to 64 MiB.
    pub max_body_size: Option<usize>,
    /// HTTP request body mode.
    pub body: HttpBodyConfig,
}

/// HTTP request body mode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum HttpBodyConfig {
    /// Send the encoded delivery as the complete request body.
    #[default]
    Raw,
    /// Send the encoded delivery as a file part in a multipart request.
    Multipart(HttpMultipartConfig),
}

/// Multipart request configuration for an HTTP sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpMultipartConfig {
    /// Form field name of the file part.
    pub file_field_name: String,
    /// Filename reported in the file part's Content-Disposition header.
    pub file_name: String,
    /// Static UTF-8 text fields included in every request.
    pub fields: BTreeMap<String, String>,
}

impl HttpSinkProps {
    /// Create a new HTTP sink configuration with the required URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: None,
            timeout_secs: None,
            headers: HashMap::new(),
            content_type: None,
            max_body_size: None,
            body: HttpBodyConfig::Raw,
        }
    }

    /// Set the HTTP method.
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Set the request timeout in seconds.
    pub fn with_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    /// Add a custom header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the Content-Type explicitly.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set the maximum body size for a single delivery.
    pub fn with_max_body_size(mut self, max_bytes: usize) -> Self {
        self.max_body_size = Some(max_bytes);
        self
    }

    /// Set the HTTP request body mode.
    pub fn with_body(mut self, body: HttpBodyConfig) -> Self {
        self.body = body;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoCodec {
    #[default]
    H264,
    H265,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoContainer {
    #[default]
    Mp4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VideoRollingConfig {
    Duration { seconds: u64 },
}

impl MemorySinkProps {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }
}

impl FileSinkProps {
    pub fn new(path: impl Into<String>, filename_prefix: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            filename_prefix: filename_prefix.into(),
            filename_suffix: String::new(),
            retention: FileRetentionConfig::default(),
        }
    }

    pub fn with_filename_suffix(mut self, filename_suffix: impl Into<String>) -> Self {
        self.filename_suffix = filename_suffix.into();
        self
    }

    pub fn with_retention(mut self, retention: FileRetentionConfig) -> Self {
        self.retention = retention;
        self
    }
}

impl NngPubSubSinkProps {
    pub fn new(url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            topic: topic.into(),
            topic_delimiter: crate::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER.to_string(),
        }
    }

    pub fn with_topic_delimiter(mut self, topic_delimiter: impl Into<String>) -> Self {
        self.topic_delimiter = topic_delimiter.into();
        self
    }
}

impl VideoSinkProps {
    pub fn new(path: impl Into<String>, rolling: VideoRollingConfig) -> Self {
        Self {
            path: path.into(),
            filename_prefix: None,
            codec: VideoCodec::default(),
            container: VideoContainer::default(),
            rolling,
        }
    }

    pub fn with_filename_prefix(mut self, filename_prefix: impl Into<String>) -> Self {
        self.filename_prefix = Some(filename_prefix.into());
        self
    }

    pub fn with_codec(mut self, codec: VideoCodec) -> Self {
        self.codec = codec;
        self
    }

    pub fn with_container(mut self, container: VideoContainer) -> Self {
        self.container = container;
        self
    }
}

pub fn validate_video_filename_prefix(prefix: &str) -> Result<(), String> {
    if prefix.trim().is_empty() {
        return Err("filename_prefix must not be empty".to_string());
    }
    if prefix.contains('/') || prefix.contains('\\') {
        return Err("filename_prefix must not contain path separators".to_string());
    }
    Ok(())
}

pub fn is_rtsp_video_url(raw_url: &str) -> bool {
    Url::parse(raw_url)
        .map(|url| matches!(url.scheme(), "rtsp" | "rtsps"))
        .unwrap_or(false)
}

pub fn is_hls_video_url(raw_url: &str) -> bool {
    Url::parse(raw_url)
        .map(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.path().to_ascii_lowercase().ends_with(".m3u8")
        })
        .unwrap_or(false)
}

impl MqttSinkProps {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>, qos: u8) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
            connector_key: None,
            max_packet_size: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    pub fn with_connector_key(mut self, connector_key: impl Into<String>) -> Self {
        self.connector_key = Some(connector_key.into());
        self
    }

    pub fn with_max_packet_size(mut self, max_packet_size: usize) -> Self {
        self.max_packet_size = Some(max_packet_size);
        self
    }
}

/// Sink definition for a pipeline.
#[derive(Debug, Clone)]
pub struct SinkDefinition {
    pub sink_id: String,
    pub sink_type: SinkType,
    pub props: SinkProps,
    pub common: CommonSinkProps,
    pub encoder: SinkEncoderConfig,
    pub output: SinkOutputConfig,
    pub compression: Option<CompressionCodec>,
    pub encryption: Option<SinkEncryptionConfig>,
    pub retry: SinkRetryConfig,
}

impl SinkDefinition {
    pub fn new(sink_id: impl Into<String>, sink_type: SinkType, props: SinkProps) -> Self {
        let sink_id_str = sink_id.into();
        Self {
            sink_id: sink_id_str.clone(),
            sink_type,
            props,
            common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::default(),
            compression: None,
            encryption: None,
            retry: SinkRetryConfig::default(),
        }
    }

    pub fn with_common_props(mut self, common: CommonSinkProps) -> Self {
        self.common = common;
        self
    }

    pub fn with_encoder(mut self, encoder: SinkEncoderConfig) -> Self {
        self.encoder = encoder;
        self
    }

    pub fn with_output(mut self, output: SinkOutputConfig) -> Self {
        self.output = output;
        self
    }

    pub fn with_compression(mut self, compression: CompressionCodec) -> Self {
        self.compression = Some(compression);
        self
    }

    pub fn with_encryption(mut self, encryption: SinkEncryptionConfig) -> Self {
        self.encryption = Some(encryption);
        self
    }

    pub fn with_retry(mut self, retry: SinkRetryConfig) -> Self {
        self.retry = retry;
        self
    }
}

/// Source binding definition for a pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDefinition {
    pub stream: String,
    pub input: SourceInputConfig,
}

impl SourceDefinition {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            input: SourceInputConfig::default(),
        }
    }

    pub fn with_input(mut self, input: SourceInputConfig) -> Self {
        self.input = input;
        self
    }
}

/// Source-side input behavior configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceInputConfig {
    pub mode: SourceInputMode,
    pub on_change: Option<SourceOnChangeConfig>,
}

impl SourceInputConfig {
    pub fn new(mode: SourceInputMode) -> Self {
        Self {
            mode,
            on_change: None,
        }
    }

    pub fn on_change() -> Self {
        Self::new(SourceInputMode::OnChange)
    }

    pub fn on_change_with_columns(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::on_change().with_on_change_columns(columns)
    }

    pub fn is_on_change(&self) -> bool {
        matches!(self.mode, SourceInputMode::OnChange)
    }

    pub fn with_on_change_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.mode = SourceInputMode::OnChange;
        self.on_change = Some(SourceOnChangeConfig {
            columns: Some(columns.into_iter().map(Into::into).collect()),
        });
        self
    }

    pub fn on_change_columns(&self) -> Option<&[String]> {
        self.on_change
            .as_ref()
            .and_then(|cfg| cfg.columns.as_deref())
    }
}

/// Input delivery mode for a source branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceInputMode {
    #[default]
    Full,
    OnChange,
}

impl SourceInputMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceInputMode::Full => "full",
            SourceInputMode::OnChange => "on_change",
        }
    }
}

/// On-change-specific source input configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceOnChangeConfig {
    pub columns: Option<Vec<String>>,
}

/// Pipeline definition referencing SQL + sinks.
#[derive(Debug, Clone)]
pub struct PipelineDefinition {
    id: String,
    sql: String,
    sources: Vec<SourceDefinition>,
    sinks: Vec<SinkDefinition>,
    options: PipelineOptions,
}

impl PipelineDefinition {
    pub fn new(id: impl Into<String>, sql: impl Into<String>, sinks: Vec<SinkDefinition>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            sources: Vec::new(),
            sinks,
            options: PipelineOptions::default(),
        }
    }

    pub fn with_sources(mut self, sources: Vec<SourceDefinition>) -> Self {
        self.sources = sources;
        self
    }

    pub fn with_options(mut self, options: PipelineOptions) -> Self {
        self.options = options;
        self
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn sources(&self) -> &[SourceDefinition] {
        &self.sources
    }

    pub fn sinks(&self) -> &[SinkDefinition] {
        &self.sinks
    }

    pub fn options(&self) -> &PipelineOptions {
        &self.options
    }
}

/// Cron-based schedule configuration for automatic pipeline start/stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineScheduleConfig {
    /// 5-field cron expression: "min hour dom month dow"
    pub cron: String,
    /// How long the pipeline runs after each cron trigger, in seconds.
    /// Must be greater than 0.
    pub duration_secs: u64,
    /// Absolute timestamp ranges in which cron windows may run.
    /// Empty means no datetime restriction.
    pub datetime_ranges: Vec<PipelineScheduleDatetimeRange>,
}

impl PipelineScheduleConfig {
    pub fn new(cron: impl Into<String>, duration_secs: u64) -> Self {
        Self {
            cron: cron.into(),
            duration_secs,
            datetime_ranges: Vec::new(),
        }
    }

    pub fn with_datetime_ranges(
        mut self,
        datetime_ranges: Vec<PipelineScheduleDatetimeRange>,
    ) -> Self {
        self.datetime_ranges = datetime_ranges;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineScheduleDatetimeRange {
    pub begin_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
}

impl PipelineScheduleDatetimeRange {
    pub fn new(begin_timestamp_ms: i64, end_timestamp_ms: i64) -> Self {
        Self {
            begin_timestamp_ms,
            end_timestamp_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineOptions {
    pub data_channel_capacity: usize,
    pub eventtime: EventtimeOptions,
    pub schedule: Option<PipelineScheduleConfig>,
    #[doc(hidden)]
    pub shared_slice_projection_enabled: Option<bool>,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            data_channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            eventtime: EventtimeOptions::default(),
            schedule: None,
            shared_slice_projection_enabled: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventtimeOptions {
    pub enabled: bool,
    pub late_tolerance: Duration,
}

impl Default for EventtimeOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            late_tolerance: Duration::ZERO,
        }
    }
}

/// User-facing view of a pipeline entry.
#[derive(Debug, Clone)]
pub struct PipelineSnapshot {
    pub definition: Arc<PipelineDefinition>,
    pub streams: Vec<String>,
    pub status: PipelineStatus,
}

/// Stores all registered pipelines and manages their lifecycle.
pub(crate) struct PipelineManager {
    pub(super) pipelines: RwLock<HashMap<String, super::internal::ManagedPipeline>>,
    pub(super) catalog: Arc<Catalog>,
    pub(super) context: super::PipelineContext,
    pub(super) registries: RwLock<PipelineRegistries>,
}
