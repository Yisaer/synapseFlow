use crate::codec::{CompressionCodec, SinkEncryptionConfig};
use crate::connector::sink::file::FileSinkConfig;
use crate::connector::sink::http::HttpSinkConfig;
use crate::connector::sink::kuksa::KuksaSinkConfig;
use crate::connector::sink::kura::KuraSinkConfig;
use crate::connector::sink::memory::MemorySinkConfig;
use crate::connector::sink::mqtt::MqttSinkConfig;
use crate::connector::sink::video::VideoSinkConfig;
use crate::connector::NngPubSubSinkConfig;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Retry configuration for sink deliveries.
///
/// Shared across all sink connector types. When `max_attempts` is `None`,
/// each delivery is attempted exactly once (no retry).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkRetryConfig {
    /// Maximum delivery attempts including the first one.
    /// `None` means no retry. Default: `None` (single attempt).
    pub max_attempts: Option<usize>,
    /// Initial backoff in milliseconds, doubles after each failed attempt.
    /// Default: 1000 (1 second).
    pub initial_backoff_ms: u64,
    /// Upper bound on backoff in milliseconds.
    /// Default: 30000 (30 seconds).
    pub max_backoff_ms: u64,
    /// Reserved for adding randomized backoff jitter. Current backoff is deterministic.
    /// Default: true.
    pub jitter: bool,
}

impl Default for SinkRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: None,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30000,
            jitter: true,
        }
    }
}

impl SinkRetryConfig {
    /// Validate the configuration. Returns an error for invalid combinations.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(max) = self.max_attempts {
            if max == 0 {
                return Err("retry.max_attempts must be >= 1".into());
            }
        }
        if self.initial_backoff_ms == 0 {
            return Err("retry.initial_backoff_ms must be > 0".into());
        }
        if self.max_backoff_ms < self.initial_backoff_ms {
            return Err("retry.max_backoff_ms must be >= retry.initial_backoff_ms".into());
        }
        Ok(())
    }
}

/// Declarative description of a sink processor in the logical/physical plans.
#[derive(Clone)]
pub struct PipelineSink {
    pub sink_id: String,
    pub forward_to_result: bool,
    pub common: CommonSinkProps,
    pub output: SinkOutputConfig,
    pub connector: PipelineSinkConnector,
    pub retry: SinkRetryConfig,
}

impl PipelineSink {
    /// Create a new sink descriptor with the provided connector configuration.
    pub fn new(sink_id: impl Into<String>, connector: PipelineSinkConnector) -> Self {
        Self {
            sink_id: sink_id.into(),
            forward_to_result: false,
            common: CommonSinkProps::default(),
            output: SinkOutputConfig::default(),
            connector,
            retry: SinkRetryConfig::default(),
        }
    }

    /// Configure whether this sink should forward records to the result collector.
    pub fn with_forward_to_result(mut self, forward: bool) -> Self {
        self.forward_to_result = forward;
        self
    }

    pub fn with_common_props(mut self, common: CommonSinkProps) -> Self {
        self.common = common;
        self
    }

    pub fn with_output(mut self, output: SinkOutputConfig) -> Self {
        self.output = output;
        self
    }

    pub fn with_retry(mut self, retry: SinkRetryConfig) -> Self {
        self.retry = retry;
        self
    }
}

impl fmt::Debug for PipelineSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineSink")
            .field("sink_id", &self.sink_id)
            .field("forward_to_result", &self.forward_to_result)
            .field("common", &self.common)
            .field("output", &self.output)
            .field("connector", &self.connector)
            .field("retry", &self.retry)
            .finish()
    }
}

/// Sink-level output behavior configuration.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SinkOutputConfig {
    pub mode: SinkOutputMode,
    pub delta: Option<SinkDeltaOutputConfig>,
    pub omit_if_empty: bool,
    /// If set, only these columns are emitted to this sink (whitelist).
    /// Mutually exclusive with `exclude_columns`.
    pub include_columns: Option<Vec<String>>,
    /// If set, all columns except these are emitted to this sink (blacklist).
    /// Mutually exclusive with `include_columns`.
    pub exclude_columns: Option<Vec<String>>,
}

impl SinkOutputConfig {
    pub fn new(mode: SinkOutputMode) -> Self {
        Self {
            mode,
            delta: None,
            omit_if_empty: false,
            include_columns: None,
            exclude_columns: None,
        }
    }

    pub fn delta() -> Self {
        Self::new(SinkOutputMode::Delta)
    }

    pub fn delta_with_columns(columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::delta().with_delta_columns(columns)
    }

    pub fn is_delta(&self) -> bool {
        matches!(self.mode, SinkOutputMode::Delta)
    }

    pub fn with_delta_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.mode = SinkOutputMode::Delta;
        self.delta = Some(SinkDeltaOutputConfig {
            columns: Some(columns.into_iter().map(Into::into).collect()),
        });
        self
    }

    pub fn delta_columns(&self) -> Option<&[String]> {
        self.delta
            .as_ref()
            .and_then(|delta| delta.columns.as_deref())
    }

    pub fn with_omit_if_empty(mut self, omit_if_empty: bool) -> Self {
        self.omit_if_empty = omit_if_empty;
        self
    }

    pub fn omit_if_empty(&self) -> bool {
        self.omit_if_empty
    }

    pub fn with_include_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.include_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_exclude_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.exclude_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Returns `true` when this sink filters columns via include or exclude.
    pub fn has_column_filter(&self) -> bool {
        self.include_columns.is_some() || self.exclude_columns.is_some()
    }

    /// Validate mutually exclusive constraints.
    pub fn validate(&self) -> Result<(), String> {
        if self.include_columns.is_some() && self.exclude_columns.is_some() {
            return Err(
                "output.include_columns and output.exclude_columns are mutually exclusive"
                    .to_string(),
            );
        }
        if let Some(columns) = &self.include_columns {
            if columns.is_empty() {
                return Err("output.include_columns must not be empty".to_string());
            }
        }
        if let Some(columns) = &self.exclude_columns {
            if columns.is_empty() {
                return Err("output.exclude_columns must not be empty".to_string());
            }
        }
        Ok(())
    }
}

/// Output delivery mode for a sink branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SinkOutputMode {
    #[default]
    Full,
    Delta,
}

impl SinkOutputMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            SinkOutputMode::Full => "full",
            SinkOutputMode::Delta => "delta",
        }
    }
}

/// Delta-mode-specific sink output configuration.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SinkDeltaOutputConfig {
    pub columns: Option<Vec<String>>,
}

/// Declarative description of a connector bound to a sink.
#[derive(Clone)]
pub struct PipelineSinkConnector {
    pub connector_id: String,
    pub connector: SinkConnectorConfig,
    pub encoder: SinkEncoderConfig,
    pub compression: Option<CompressionCodec>,
    pub encryption: Option<SinkEncryptionConfig>,
}

impl PipelineSinkConnector {
    pub fn new(
        connector_id: impl Into<String>,
        connector: SinkConnectorConfig,
        encoder: SinkEncoderConfig,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            connector,
            encoder,
            compression: None,
            encryption: None,
        }
    }

    pub fn with_compression(mut self, compression: Option<CompressionCodec>) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_encryption(mut self, encryption: Option<SinkEncryptionConfig>) -> Self {
        self.encryption = encryption;
        self
    }
}

impl fmt::Debug for PipelineSinkConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineSinkConnector")
            .field("connector_id", &self.connector_id)
            .field("connector", &self.connector)
            .field("encoder", &self.encoder)
            .field("compression", &self.compression)
            .field("encryption", &self.encryption)
            .finish()
    }
}

/// Configuration for supported sink connectors.
#[derive(Clone, Debug)]
pub enum SinkConnectorConfig {
    Mqtt(MqttSinkConfig),
    Nop(NopSinkConfig),
    Kuksa(KuksaSinkConfig),
    Kura(KuraSinkConfig),
    Memory(MemorySinkConfig),
    File(FileSinkConfig),
    Video(VideoSinkConfig),
    NngPubSub(NngPubSubSinkConfig),
    Http(HttpSinkConfig),
    Custom(CustomSinkConnectorConfig),
}

impl SinkConnectorConfig {
    pub fn kind(&self) -> &str {
        match self {
            SinkConnectorConfig::Mqtt(_) => "mqtt",
            SinkConnectorConfig::Nop(_) => "nop",
            SinkConnectorConfig::Kuksa(_) => "kuksa",
            SinkConnectorConfig::Kura(_) => "kura",
            SinkConnectorConfig::Memory(_) => "memory",
            SinkConnectorConfig::File(_) => "file",
            SinkConnectorConfig::Video(_) => "video",
            SinkConnectorConfig::NngPubSub(_) => "nng_pubsub",
            SinkConnectorConfig::Http(_) => "http",
            SinkConnectorConfig::Custom(custom) => custom.kind.as_str(),
        }
    }

    pub fn custom_settings(&self) -> Option<&JsonValue> {
        match self {
            SinkConnectorConfig::Custom(custom) => Some(&custom.settings),
            _ => None,
        }
    }
}

/// JSON-based payload for custom connectors.
#[derive(Clone, Debug)]
pub struct CustomSinkConnectorConfig {
    pub kind: String,
    pub settings: JsonValue,
}

/// Configuration for a no-op sink connector.
#[derive(Clone, Debug, Default)]
pub struct NopSinkConfig {
    pub log: bool,
}

/// Configuration for supported sink encoders.
#[derive(Clone, Debug)]
pub struct SinkEncoderConfig {
    kind: SinkEncoderKind,
    props: JsonMap<String, JsonValue>,
    transform: Option<SinkEncoderTransformConfig>,
    proto_bundle: Option<Arc<crate::codec::ProtoDescriptorBundle>>,
    property_context: crate::PropertyContext,
}

impl PartialEq for SinkEncoderConfig {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.props == other.props && self.transform == other.transform
    }
}

impl Eq for SinkEncoderConfig {}

/// Supported encoder kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkEncoderKind {
    Csv,
    Json,
    Protobuf,
    None,
    Custom(String),
}

/// Supported encoder transform kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkEncoderTransformConfig {
    Template { template: String },
}

impl SinkEncoderTransformConfig {
    pub fn kind_str(&self) -> &'static str {
        match self {
            SinkEncoderTransformConfig::Template { .. } => "template",
        }
    }

    pub fn template(&self) -> &str {
        match self {
            SinkEncoderTransformConfig::Template { template } => template.as_str(),
        }
    }
}

impl SinkEncoderKind {
    pub fn as_str(&self) -> &str {
        match self {
            SinkEncoderKind::Csv => "csv",
            SinkEncoderKind::Json => "json",
            SinkEncoderKind::Protobuf => "protobuf",
            SinkEncoderKind::None => "none",
            SinkEncoderKind::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<String> for SinkEncoderKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "csv" => SinkEncoderKind::Csv,
            "json" => SinkEncoderKind::Json,
            "protobuf" => SinkEncoderKind::Protobuf,
            "none" => SinkEncoderKind::None,
            other => SinkEncoderKind::Custom(other.to_string()),
        }
    }
}

impl From<&str> for SinkEncoderKind {
    fn from(value: &str) -> Self {
        match value {
            "csv" => SinkEncoderKind::Csv,
            "json" => SinkEncoderKind::Json,
            "protobuf" => SinkEncoderKind::Protobuf,
            "none" => SinkEncoderKind::None,
            other => SinkEncoderKind::Custom(other.to_string()),
        }
    }
}

impl SinkEncoderConfig {
    pub fn new(kind: impl Into<SinkEncoderKind>, props: JsonMap<String, JsonValue>) -> Self {
        Self {
            kind: kind.into(),
            props,
            transform: None,
            proto_bundle: None,
            property_context: crate::PropertyContext::default(),
        }
    }

    pub fn json() -> Self {
        Self::new(SinkEncoderKind::Json, JsonMap::new())
    }

    pub fn csv() -> Self {
        Self::new(SinkEncoderKind::Csv, JsonMap::new())
    }

    pub fn json_with_transform_template(template: impl Into<String>) -> Self {
        Self::json().with_transform_template(template)
    }

    pub fn kind(&self) -> &SinkEncoderKind {
        &self.kind
    }

    pub fn kind_str(&self) -> &str {
        self.kind.as_str()
    }

    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }

    pub fn json_omit_null_columns(&self) -> Result<bool, String> {
        match self.props.get("omit_null_columns") {
            None => Ok(true),
            Some(value) => value
                .as_bool()
                .ok_or_else(|| "encoder.props.omit_null_columns must be a boolean".to_string()),
        }
    }

    pub fn csv_delimiter(&self) -> Result<u8, String> {
        let delimiter = match self.props.get("delimiter") {
            None => return Ok(b','),
            Some(JsonValue::String(value)) => value.as_bytes(),
            Some(_) => {
                return Err("encoder.props.delimiter must be a string".to_string());
            }
        };
        if delimiter.len() != 1 || !delimiter[0].is_ascii() {
            return Err("encoder.props.delimiter must contain exactly one ASCII byte".to_string());
        }
        if matches!(delimiter[0], b'"' | b'\r' | b'\n') {
            return Err(
                "encoder.props.delimiter must not be a quote or line terminator".to_string(),
            );
        }
        Ok(delimiter[0])
    }

    pub fn csv_header(&self) -> Result<bool, String> {
        match self.props.get("header") {
            None => Ok(false),
            Some(value) => value
                .as_bool()
                .ok_or_else(|| "encoder.props.header must be a boolean".to_string()),
        }
    }

    pub fn transform(&self) -> Option<&SinkEncoderTransformConfig> {
        if matches!(self.kind, SinkEncoderKind::None) {
            return None;
        }
        self.transform.as_ref()
    }

    pub fn with_transform_template(mut self, template: impl Into<String>) -> Self {
        self.transform = Some(SinkEncoderTransformConfig::Template {
            template: template.into(),
        });
        self
    }

    pub fn transform_template(&self) -> Option<&str> {
        self.transform().map(SinkEncoderTransformConfig::template)
    }

    pub fn transform_kind(&self) -> Option<&'static str> {
        self.transform().map(SinkEncoderTransformConfig::kind_str)
    }

    pub fn with_transform(mut self, transform: SinkEncoderTransformConfig) -> Self {
        self.transform = Some(transform);
        self
    }

    pub(crate) fn property_context(&self) -> &crate::PropertyContext {
        &self.property_context
    }

    pub(crate) fn with_property_context(
        mut self,
        property_context: crate::PropertyContext,
    ) -> Self {
        self.property_context = property_context;
        self
    }

    pub fn proto_bundle(&self) -> Option<&Arc<crate::codec::ProtoDescriptorBundle>> {
        self.proto_bundle.as_ref()
    }

    pub fn with_proto_bundle(mut self, bundle: Arc<crate::codec::ProtoDescriptorBundle>) -> Self {
        self.proto_bundle = Some(bundle);
        self
    }

    pub fn with_json_omit_null_columns(mut self, omit_null_columns: bool) -> Self {
        self.props.insert(
            "omit_null_columns".to_string(),
            JsonValue::Bool(omit_null_columns),
        );
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if matches!(self.kind, SinkEncoderKind::Csv) {
            self.csv_delimiter()?;
            self.csv_header()?;
        }

        if matches!(self.kind, SinkEncoderKind::Json) {
            self.json_omit_null_columns()?;
        }

        if matches!(self.kind, SinkEncoderKind::Protobuf) && self.proto_bundle.is_none() {
            return Err(
                "protobuf encoder requires a proto descriptor bundle (schema_ref)".to_string(),
            );
        }

        let Some(_transform) = self.transform.as_ref() else {
            return Ok(());
        };

        if matches!(self.kind, SinkEncoderKind::None) {
            return Ok(());
        }

        if !matches!(self.kind, SinkEncoderKind::Json) {
            return Err(format!(
                "encoder transform is only supported for encoder.type=json, got `{}`",
                self.kind.as_str()
            ));
        }

        Ok(())
    }
}

/// Common sink-level properties (batching, etc.).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommonSinkProps {
    pub batch_count: Option<usize>,
    pub batch_duration: Option<Duration>,
}

impl CommonSinkProps {
    pub fn is_batching_enabled(&self) -> bool {
        self.batch_count.is_some() || self.batch_duration.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_none_transform_is_ignored() {
        let config = SinkEncoderConfig::new("none", JsonMap::new())
            .with_transform_template("{\"x\":{{ json(.row.a) }} }");

        assert_eq!(config.transform(), None);
        assert_eq!(config.transform_template(), None);
        assert_eq!(config.transform_kind(), None);
        assert!(
            config.validate().is_ok(),
            "none+transform should be ignored"
        );
    }

    #[test]
    fn custom_encoder_transform_is_rejected() {
        let config = SinkEncoderConfig::new("custom_encoder", JsonMap::new())
            .with_transform_template("{\"x\":{{ json(.row.a) }} }");

        let err = config
            .validate()
            .expect_err("custom encoder should reject transform");
        assert!(
            err.contains("only supported for encoder.type=json"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn json_encoder_rejects_non_boolean_omit_null_columns() {
        let mut props = JsonMap::new();
        props.insert(
            "omit_null_columns".to_string(),
            JsonValue::String("false".to_string()),
        );
        let config = SinkEncoderConfig::new("json", props);

        let err = config
            .validate()
            .expect_err("non-boolean omit_null_columns should be rejected");
        assert!(
            err.contains("omit_null_columns must be a boolean"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn csv_encoder_validates_props() {
        let mut props = JsonMap::new();
        props.insert("delimiter".to_string(), JsonValue::String("::".to_string()));
        let err = SinkEncoderConfig::new("csv", props)
            .validate()
            .expect_err("multi-byte delimiter should fail");
        assert!(
            err.contains("exactly one ASCII byte"),
            "unexpected error: {err}"
        );

        let mut props = JsonMap::new();
        props.insert("header".to_string(), JsonValue::String("true".to_string()));
        let err = SinkEncoderConfig::new("csv", props)
            .validate()
            .expect_err("non-boolean header should fail");
        assert!(
            err.contains("header must be a boolean"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn csv_encoder_rejects_template_transform() {
        let err = SinkEncoderConfig::csv()
            .with_transform_template("{{ json(.row) }}")
            .validate()
            .expect_err("CSV transform should fail");
        assert!(
            err.contains("only supported for encoder.type=json"),
            "unexpected error: {err}"
        );
    }
}
