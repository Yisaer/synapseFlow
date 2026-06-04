//! Encoder abstractions for turning collection-backed input records into outbound sink payloads.

mod json;
mod template_transform;

use crate::model::Collection;
use crate::planner::physical::output_schema::OutputSchema;
use crate::planner::physical::ByIndexProjection;
use bytes::Bytes;
use std::sync::Arc;

pub use json::JsonEncoder;

/// Errors that can occur during encoding.
#[derive(thiserror::Error, Debug)]
pub enum EncodeError {
    /// Failed to serialize into the requested format.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Custom error.
    #[error("{0}")]
    Other(String),
}

/// Factory and shared configuration for every sink encoder.
pub trait SinkEncoderFactory: Send + Sync + 'static {
    /// Identifier for metrics/logging.
    fn id(&self) -> &str;
    /// Start a processor-local reusable encoder.
    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError>;
    /// Whether this encoder supports index-based lazy materialization (`ByIndexProjection`).
    fn supports_index_lazy_materialization(&self) -> bool {
        false
    }
    /// Attach a by-index projection spec to enable index-based lazy materialization.
    fn with_by_index_projection(
        self: Arc<Self>,
        _spec: Arc<ByIndexProjection>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Err(EncodeError::Other(
            "index lazy materialization is not supported for this encoder".to_string(),
        ))
    }
    /// Attach the final output schema for mask-aware encoding when needed.
    fn with_output_schema(
        self: Arc<Self>,
        _output_schema: Arc<OutputSchema>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError>;
}

/// Processor-local reusable sink encoder.
pub trait SinkEncoder: Send {
    /// Start a new delivery, reset per-delivery state, and optionally emit start bytes.
    fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError>;
    /// Append one input record and optionally emit one intermediate chunk.
    ///
    /// The runtime representation of an input record is [`Collection`].
    fn append(&mut self, record: &dyn Collection) -> Result<Option<Bytes>, EncodeError>;
    /// Finalize the current delivery and optionally emit end bytes.
    fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError>;
    /// Abort the current delivery and clear any partial state.
    fn abort_delivery(&mut self) {}
}
