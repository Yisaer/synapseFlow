use crate::connector::sink::mqtt::MqttSinkConfig;
use std::fmt;
use std::time::Duration;

/// Declarative description of a sink processor in the logical/physical plans.
#[derive(Clone)]
pub struct PipelineSink {
    pub sink_id: String,
    pub forward_to_result: bool,
    pub common: CommonSinkProps,
    pub connector: PipelineSinkConnector,
}

impl PipelineSink {
    /// Create a new sink descriptor with the provided connector configuration.
    pub fn new(sink_id: impl Into<String>, connector: PipelineSinkConnector) -> Self {
        Self {
            sink_id: sink_id.into(),
            forward_to_result: false,
            common: CommonSinkProps::default(),
            connector,
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
}

impl fmt::Debug for PipelineSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineSink")
            .field("sink_id", &self.sink_id)
            .field("forward_to_result", &self.forward_to_result)
            .field("common", &self.common)
            .field("connector", &self.connector)
            .finish()
    }
}

/// Declarative description of a connector bound to a sink.
#[derive(Clone)]
pub struct PipelineSinkConnector {
    pub connector_id: String,
    pub connector: SinkConnectorConfig,
    pub encoder: SinkEncoderConfig,
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
        }
    }
}

impl fmt::Debug for PipelineSinkConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineSinkConnector")
            .field("connector_id", &self.connector_id)
            .field("connector", &self.connector)
            .field("encoder", &self.encoder)
            .finish()
    }
}

/// Configuration for supported sink connectors.
#[derive(Clone, Debug)]
pub enum SinkConnectorConfig {
    Mqtt(MqttSinkConfig),
    Nop(NopSinkConfig),
}

/// Configuration for a no-op sink connector.
#[derive(Clone, Debug, Default)]
pub struct NopSinkConfig;

/// Configuration for supported sink encoders.
#[derive(Clone, Debug)]
pub enum SinkEncoderConfig {
    Json { encoder_id: String },
}

impl SinkEncoderConfig {
    /// Whether this encoder supports streaming aggregation.
    pub fn supports_streaming(&self) -> bool {
        match self {
            SinkEncoderConfig::Json { .. } => true,
        }
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
