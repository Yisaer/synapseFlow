//! Stream data types for processor communication
//!
//! Defines the data types that flow between processors in the stream processing pipeline.

use crate::model::Collection;
use std::sync::Arc;

/// Control signals for stream processing
#[derive(Debug, Clone, PartialEq)]
pub enum ControlSignal {
    /// Stream end signal  
    StreamEnd,
    /// Watermark for time-based processing
    Watermark(std::time::SystemTime),
    /// Resume normal processing
    Resume,
    /// Flush buffered data
    Flush,
}

/// Core data type for stream processing - unified enum for all data types
#[derive(Clone)]
pub enum StreamData {
    /// Data payload - Collection (shared reference)
    Collection(Arc<dyn Collection>),
    /// Control signal for flow management
    Control(ControlSignal),
    /// Error that occurred during processing - wrapped for flow continuation
    Error(StreamError),
}

/// Error type for stream processing that can be sent through the pipeline
#[derive(Debug, Clone, PartialEq)]
pub struct StreamError {
    /// The error message
    pub message: String,
    /// Optional source processor identifier
    pub source: Option<String>,
    /// Optional timestamp when the error occurred
    pub timestamp: Option<std::time::SystemTime>,
}

impl StreamError {
    /// Create a new stream error with just a message
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
            timestamp: None,
        }
    }

    /// Create a new stream error with source processor
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Create a new stream error with timestamp
    pub fn with_timestamp(mut self, timestamp: std::time::SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamError: {}", self.message)?;
        if let Some(source) = &self.source {
            write!(f, " (from: {})", source)?;
        }
        if let Some(timestamp) = &self.timestamp {
            write!(f, " at {:?}", timestamp)?;
        }
        Ok(())
    }
}

impl std::error::Error for StreamError {}

impl StreamData {
    /// Create Collection data
    pub fn collection(collection: Box<dyn Collection>) -> Self {
        StreamData::Collection(Arc::from(collection))
    }

    /// Create control signal
    pub fn control(signal: ControlSignal) -> Self {
        StreamData::Control(signal)
    }

    /// Create error data
    pub fn error(error: StreamError) -> Self {
        StreamData::Error(error)
    }

    /// Create error from string message
    pub fn error_message(message: impl Into<String>) -> Self {
        StreamData::Error(StreamError::new(message))
    }

    /// Check if this is data (Collection)
    pub fn is_data(&self) -> bool {
        matches!(self, StreamData::Collection(_))
    }

    /// Check if this is a control signal
    pub fn is_control(&self) -> bool {
        matches!(self, StreamData::Control(_))
    }

    /// Check if this is an error
    pub fn is_error(&self) -> bool {
        matches!(self, StreamData::Error(_))
    }

    /// Check if this is a terminal signal (StreamEnd)
    pub fn is_terminal(&self) -> bool {
        matches!(self, StreamData::Control(ControlSignal::StreamEnd))
    }

    /// Extract Collection if present
    pub fn as_collection(&self) -> Option<&dyn Collection> {
        match self {
            StreamData::Collection(collection) => Some(collection.as_ref()),
            _ => None,
        }
    }

    /// Extract control signal if present
    pub fn as_control(&self) -> Option<&ControlSignal> {
        match self {
            StreamData::Control(signal) => Some(signal),
            _ => None,
        }
    }

    /// Extract error if present
    pub fn as_error(&self) -> Option<&StreamError> {
        match self {
            StreamData::Error(error) => Some(error),
            _ => None,
        }
    }

    /// Get a human-readable description
    pub fn description(&self) -> String {
        match self {
            StreamData::Collection(collection) => {
                format!("Collection with {} rows", collection.num_rows())
            }
            StreamData::Control(signal) => format!("Control signal: {:?}", signal),
            StreamData::Error(error) => format!("Error: {}", error),
        }
    }
}

/// Convenience methods for common control signals
impl StreamData {
    /// Create stream end signal
    pub fn stream_end() -> Self {
        StreamData::control(ControlSignal::StreamEnd)
    }

    /// Create resume signal
    pub fn resume() -> Self {
        StreamData::control(ControlSignal::Resume)
    }

    /// Create flush signal
    pub fn flush() -> Self {
        StreamData::control(ControlSignal::Flush)
    }

    /// Create watermark signal
    pub fn watermark(time: std::time::SystemTime) -> Self {
        StreamData::control(ControlSignal::Watermark(time))
    }
}
