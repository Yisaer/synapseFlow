use crate::catalog::{EventtimeDefinition, StreamDecoderConfig, StreamType};
use crate::pipeline::SourceInputConfig;
use crate::planner::decode_projection::DecodeProjection;
use crate::planner::logical::BaseLogicalPlan;
use crate::processor::SamplerConfig;
use datatypes::Schema;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DataSource {
    pub base: BaseLogicalPlan,
    pub source_name: String,
    pub stream_type: StreamType,
    pub alias: Option<String>,
    pub decoder: StreamDecoderConfig,
    pub schema: Arc<Schema>,
    pub decode_projection: Option<DecodeProjection>,
    /// For shared sources, this stores the per-pipeline required top-level columns.
    ///
    /// When VF-56 slot-schema projection is enabled, this is the planned slot-schema
    /// column list and `schema` is also slot-ordered. Otherwise this is the legacy
    /// dynamic decode requirement while `schema` keeps the full source layout.
    pub shared_required_schema: Option<Vec<String>>,
    /// Slot-schema version used when this shared source was planned.
    pub shared_slot_version: Option<u64>,
    pub source_input: SourceInputConfig,
    pub eventtime: Option<EventtimeDefinition>,
    pub sampler: Option<SamplerConfig>,
}

impl DataSource {
    pub fn new(
        source_name: String,
        alias: Option<String>,
        decoder: StreamDecoderConfig,
        index: i64,
        schema: Arc<Schema>,
        eventtime: Option<EventtimeDefinition>,
        sampler: Option<SamplerConfig>,
    ) -> Self {
        let base = BaseLogicalPlan::new(vec![], index);
        Self {
            base,
            source_name,
            stream_type: StreamType::Mock,
            alias,
            decoder,
            schema,
            decode_projection: None,
            shared_required_schema: None,
            shared_slot_version: None,
            source_input: SourceInputConfig::default(),
            eventtime,
            sampler,
        }
    }

    pub fn with_stream_type(mut self, stream_type: StreamType) -> Self {
        self.stream_type = stream_type;
        self
    }

    pub fn decoder(&self) -> &StreamDecoderConfig {
        &self.decoder
    }

    pub fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn decode_projection(&self) -> Option<&DecodeProjection> {
        self.decode_projection.as_ref()
    }

    pub fn shared_required_schema(&self) -> Option<&[String]> {
        self.shared_required_schema.as_deref()
    }

    pub fn source_input(&self) -> &SourceInputConfig {
        &self.source_input
    }

    pub fn eventtime(&self) -> Option<&EventtimeDefinition> {
        self.eventtime.as_ref()
    }

    pub fn sampler(&self) -> Option<&SamplerConfig> {
        self.sampler.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::StreamDecoderConfig;
    use std::time::Duration;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![]))
    }

    fn create_test_decoder() -> StreamDecoderConfig {
        StreamDecoderConfig::json()
    }

    #[test]
    fn test_datasource_creation_without_sampler() {
        let ds = DataSource::new(
            "test_stream".to_string(),
            None,
            create_test_decoder(),
            0,
            create_test_schema(),
            None,
            None,
        );
        assert_eq!(ds.source_name, "test_stream");
        assert!(ds.sampler().is_none());
    }

    #[test]
    fn test_datasource_creation_with_sampler() {
        let sampler_config = SamplerConfig::new(Duration::from_millis(100));
        let ds = DataSource::new(
            "test_stream".to_string(),
            Some("alias".to_string()),
            create_test_decoder(),
            0,
            create_test_schema(),
            None,
            Some(sampler_config.clone()),
        );
        assert_eq!(ds.source_name, "test_stream");
        assert_eq!(ds.alias, Some("alias".to_string()));
        let retrieved = ds.sampler().expect("sampler should be present");
        assert_eq!(retrieved.interval, Duration::from_millis(100));
    }

    #[test]
    fn test_datasource_sampler_accessor() {
        let sampler_config = SamplerConfig::new(Duration::from_secs(5));
        let ds = DataSource::new(
            "stream".to_string(),
            None,
            create_test_decoder(),
            1,
            create_test_schema(),
            None,
            Some(sampler_config),
        );
        let sampler = ds.sampler().unwrap();
        assert_eq!(sampler.interval, Duration::from_secs(5));
    }

    #[test]
    fn test_datasource_other_accessors() {
        let ds = DataSource::new(
            "my_stream".to_string(),
            Some("my_alias".to_string()),
            create_test_decoder(),
            42,
            create_test_schema(),
            None,
            None,
        );
        assert_eq!(ds.base.index(), 42);
        assert!(ds.decode_projection().is_none());
        assert!(ds.shared_required_schema().is_none());
        assert!(ds.eventtime().is_none());
    }
}
