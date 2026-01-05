use crate::catalog::Catalog;
use crate::connector::MqttClientManager;
use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig};
use crate::shared_stream::SharedStreamRegistry;
use crate::PipelineRegistries;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
}

/// Runtime state for pipeline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Stopped,
    Running,
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

impl MqttSinkProps {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>, qos: u8) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
            connector_key: None,
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
}

/// Sink definition for a pipeline.
#[derive(Debug, Clone)]
pub struct SinkDefinition {
    pub sink_id: String,
    pub sink_type: SinkType,
    pub props: SinkProps,
    pub common: CommonSinkProps,
    pub encoder: SinkEncoderConfig,
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
}

/// Pipeline definition referencing SQL + sinks.
#[derive(Debug, Clone)]
pub struct PipelineDefinition {
    id: String,
    sql: String,
    sinks: Vec<SinkDefinition>,
    options: PipelineOptions,
}

impl PipelineDefinition {
    pub fn new(id: impl Into<String>, sql: impl Into<String>, sinks: Vec<SinkDefinition>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            sinks,
            options: PipelineOptions::default(),
        }
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

    pub fn sinks(&self) -> &[SinkDefinition] {
        &self.sinks
    }

    pub fn options(&self) -> &PipelineOptions {
        &self.options
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineOptions {
    pub plan_cache: PlanCacheOptions,
    pub eventtime: EventtimeOptions,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanCacheOptions {
    pub enabled: bool,
}

/// User-facing view of a pipeline entry.
#[derive(Clone)]
pub struct PipelineSnapshot {
    pub definition: Arc<PipelineDefinition>,
    pub streams: Vec<String>,
    pub status: PipelineStatus,
}

/// Stores all registered pipelines and manages their lifecycle.
pub struct PipelineManager {
    pub(super) pipelines: RwLock<HashMap<String, super::internal::ManagedPipeline>>,
    pub(super) catalog: Arc<Catalog>,
    pub(super) shared_stream_registry: &'static SharedStreamRegistry,
    pub(super) mqtt_client_manager: MqttClientManager,
    pub(super) registries: PipelineRegistries,
}
