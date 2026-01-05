//! Stream data types for processor communication
//!
//! Defines the data types that flow between processors in the stream processing pipeline.

use crate::model::Collection;
use std::time::SystemTime;

/// Control signals for stream processing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ControlSignal {
    /// Barrier-style control signal that must be aligned across upstreams
    /// before being processed by some join processors.
    Barrier(BarrierControlSignal),
    /// Immediate control signal that does not require upstream alignment.
    Instant(InstantControlSignal),
}

/// Kinds of barrier control signals without an attached barrier id.
///
/// The pipeline head is responsible for assigning monotonically increasing
/// `barrier_id` values and constructing the corresponding [`BarrierControlSignal`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BarrierControlSignalKind {
    StreamGracefulEnd,
    SyncTest,
}

impl BarrierControlSignalKind {
    pub fn with_id(self, barrier_id: u64) -> BarrierControlSignal {
        match self {
            BarrierControlSignalKind::StreamGracefulEnd => {
                BarrierControlSignal::StreamGracefulEnd { barrier_id }
            }
            BarrierControlSignalKind::SyncTest => BarrierControlSignal::SyncTest { barrier_id },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BarrierControlSignal {
    StreamGracefulEnd {
        barrier_id: u64,
    },
    /// A no-op barrier signal used by tests to validate barrier alignment.
    ///
    /// This signal has no semantic meaning beyond "wait until all upstreams
    /// deliver the same `(barrier_id, kind)` pair".
    SyncTest {
        barrier_id: u64,
    },
}

impl BarrierControlSignal {
    pub fn barrier_id(&self) -> u64 {
        match self {
            BarrierControlSignal::StreamGracefulEnd { barrier_id } => *barrier_id,
            BarrierControlSignal::SyncTest { barrier_id } => *barrier_id,
        }
    }

    pub fn kind(&self) -> BarrierControlSignalKind {
        match self {
            BarrierControlSignal::StreamGracefulEnd { .. } => {
                BarrierControlSignalKind::StreamGracefulEnd
            }
            BarrierControlSignal::SyncTest { .. } => BarrierControlSignalKind::SyncTest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InstantControlSignal {
    StreamQuickEnd { signal_id: u64 },
}

impl ControlSignal {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { .. })
                | ControlSignal::Instant(InstantControlSignal::StreamQuickEnd { .. })
        )
    }

    pub fn is_graceful_end(&self) -> bool {
        matches!(
            self,
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { .. })
        )
    }
}

/// Core data type for stream processing - unified enum for all data types
#[derive(Clone)]
pub enum StreamData {
    /// Data payload - Collection (owned)
    Collection(Box<dyn Collection>),
    /// Encoded bytes for sink connectors
    EncodedBytes { payload: Vec<u8>, num_rows: u64 },
    /// Raw bytes that still need to be decoded into a collection
    Bytes(Vec<u8>),
    /// Control signal for flow management
    Control(ControlSignal),
    /// Watermark for time progression
    Watermark(SystemTime),
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
        StreamData::Collection(collection)
    }

    /// Create raw byte payload
    pub fn bytes(payload: Vec<u8>) -> Self {
        StreamData::Bytes(payload)
    }

    /// Create encoded payload
    pub fn encoded_bytes(payload: Vec<u8>, num_rows: u64) -> Self {
        StreamData::EncodedBytes { payload, num_rows }
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

    /// Check if this is data (Collection or raw bytes)
    pub fn is_data(&self) -> bool {
        matches!(
            self,
            StreamData::Collection(_) | StreamData::Bytes(_) | StreamData::EncodedBytes { .. }
        )
    }

    /// Check if this is a control signal
    pub fn is_control(&self) -> bool {
        matches!(self, StreamData::Control(_))
    }

    /// Check if this is an error
    pub fn is_error(&self) -> bool {
        matches!(self, StreamData::Error(_))
    }

    /// Check if this is a terminal signal (Stream end variants)
    pub fn is_terminal(&self) -> bool {
        match self {
            StreamData::Control(signal) => signal.is_terminal(),
            _ => false,
        }
    }

    /// Check if this is a watermark signal
    pub fn is_watermark(&self) -> bool {
        matches!(self, StreamData::Watermark(_))
    }

    /// Extract Collection if present
    pub fn as_collection(&self) -> Option<&dyn Collection> {
        match self {
            StreamData::Collection(collection) => Some(collection.as_ref()),
            _ => None,
        }
    }

    /// Extract mutable collection reference if present
    pub fn as_collection_mut(&mut self) -> Option<&mut dyn Collection> {
        match self {
            StreamData::Collection(collection) => Some(collection.as_mut()),
            _ => None,
        }
    }

    /// Consume and return the owned collection if present
    pub fn into_collection(self) -> Option<Box<dyn Collection>> {
        match self {
            StreamData::Collection(collection) => Some(collection),
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
            StreamData::EncodedBytes { payload, num_rows } => {
                format!(
                    "Encoded payload ({} bytes, {} rows)",
                    payload.len(),
                    num_rows
                )
            }
            StreamData::Bytes(payload) => format!("Bytes payload ({} bytes)", payload.len()),
            StreamData::Control(signal) => format!("Control signal: {:?}", signal),
            StreamData::Watermark(ts) => format!("Watermark at {:?}", ts),
            StreamData::Error(error) => format!("Error: {}", error),
        }
    }
}

/// Convenience methods for common control signals
impl StreamData {
    /// Create stream end signal
    pub fn stream_end() -> Self {
        StreamData::control(ControlSignal::Barrier(
            BarrierControlSignal::StreamGracefulEnd { barrier_id: 0 },
        ))
    }

    /// Create quick stream end signal
    pub fn quick_end() -> Self {
        StreamData::control(ControlSignal::Instant(
            InstantControlSignal::StreamQuickEnd { signal_id: 0 },
        ))
    }

    /// Create watermark signal
    pub fn watermark(timestamp: SystemTime) -> Self {
        StreamData::Watermark(timestamp)
    }
}
