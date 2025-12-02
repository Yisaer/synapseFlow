use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Errors that can occur when mutating pipeline definitions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PipelineError {
    #[error("pipeline already exists: {0}")]
    AlreadyExists(String),
    #[error("pipeline not found: {0}")]
    NotFound(String),
}

/// Supported sink types for pipeline outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkType {
    /// MQTT sink.
    Mqtt,
}

/// Sink configuration payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkProps {
    /// MQTT sink configuration.
    Mqtt(MqttSinkProps),
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
}

impl SinkDefinition {
    pub fn new(sink_id: impl Into<String>, sink_type: SinkType, props: SinkProps) -> Self {
        Self {
            sink_id: sink_id.into(),
            sink_type,
            props,
        }
    }
}

/// Pipeline definition referencing SQL + sinks.
#[derive(Debug, Clone)]
pub struct PipelineDefinition {
    id: String,
    sql: String,
    sinks: Vec<SinkDefinition>,
}

impl PipelineDefinition {
    pub fn new(id: impl Into<String>, sql: impl Into<String>, sinks: Vec<SinkDefinition>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            sinks,
        }
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
}

/// Stores all registered pipelines and provides CRUD helpers.
#[derive(Default)]
pub struct PipelineManager {
    pipelines: RwLock<HashMap<String, Arc<PipelineDefinition>>>,
}

impl PipelineManager {
    pub fn new() -> Self {
        Self {
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new pipeline definition.
    pub fn register(
        &self,
        pipeline: PipelineDefinition,
    ) -> Result<Arc<PipelineDefinition>, PipelineError> {
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
        let pipeline_id = pipeline.id().to_string();
        if guard.contains_key(&pipeline_id) {
            return Err(PipelineError::AlreadyExists(pipeline_id));
        }
        let entry = Arc::new(pipeline);
        guard.insert(pipeline_id, entry.clone());
        Ok(entry)
    }

    /// Remove a pipeline by id.
    pub fn remove(&self, pipeline_id: &str) -> Result<(), PipelineError> {
        let mut guard = self.pipelines.write().expect("pipeline manager poisoned");
        guard
            .remove(pipeline_id)
            .map(|_| ())
            .ok_or_else(|| PipelineError::NotFound(pipeline_id.to_string()))
    }

    /// Fetch a pipeline by id.
    pub fn get(&self, pipeline_id: &str) -> Option<Arc<PipelineDefinition>> {
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
        guard.get(pipeline_id).cloned()
    }

    /// List all pipelines.
    pub fn list(&self) -> Vec<Arc<PipelineDefinition>> {
        let guard = self.pipelines.read().expect("pipeline manager poisoned");
        guard.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_pipeline(name: &str) -> PipelineDefinition {
        let sink = SinkDefinition::new(
            format!("{name}_sink"),
            SinkType::Mqtt,
            SinkProps::Mqtt(
                MqttSinkProps::new("mqtt://broker:1883", "topic", 1).with_client_id("client"),
            ),
        );
        PipelineDefinition::new(name.to_string(), "SELECT * FROM stream", vec![sink])
    }

    #[test]
    fn register_and_get_pipeline() {
        let manager = PipelineManager::new();
        let definition = sample_pipeline("pipe_a");
        let stored = manager.register(definition).expect("register pipeline");

        assert_eq!(stored.id(), "pipe_a");
        assert_eq!(stored.sql(), "SELECT * FROM stream");
        assert_eq!(stored.sinks().len(), 1);

        let fetched = manager.get("pipe_a").expect("fetch pipeline");
        assert_eq!(fetched.id(), "pipe_a");
    }

    #[test]
    fn prevent_duplicate_pipeline() {
        let manager = PipelineManager::new();
        manager
            .register(sample_pipeline("dup_pipeline"))
            .expect("first insert");
        let result = manager.register(sample_pipeline("dup_pipeline"));
        assert!(matches!(result, Err(PipelineError::AlreadyExists(_))));
    }

    #[test]
    fn remove_pipeline() {
        let manager = PipelineManager::new();
        manager
            .register(sample_pipeline("pipe_del"))
            .expect("register pipeline");
        manager.remove("pipe_del").expect("remove pipeline");
        assert!(manager.get("pipe_del").is_none());
        let err = manager.remove("pipe_del").unwrap_err();
        assert!(matches!(err, PipelineError::NotFound(_)));
    }
}
