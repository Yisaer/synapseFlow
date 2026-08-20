//! Stream data types for processor communication
//!
//! Defines the data types that flow between processors in the stream processing pipeline.

use crate::checkpoint::CheckpointMode;
use crate::model::Collection;
use bytes::Bytes;
use std::time::SystemTime;

/// Flags attached to sink-side encoded delivery events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EncodedDeliveryFlags(u8);

impl EncodedDeliveryFlags {
    pub const START: Self = Self(0b0000_0001);
    pub const END: Self = Self(0b0000_0010);
    pub const ABORT: Self = Self(0b0000_0100);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl std::ops::BitOr for EncodedDeliveryFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

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
    Checkpoint { mode: CheckpointMode },
    StreamGracefulEnd,
    SyncTest,
}

impl BarrierControlSignalKind {
    pub fn with_id(self, barrier_id: u64) -> BarrierControlSignal {
        match self {
            BarrierControlSignalKind::Checkpoint { mode } => BarrierControlSignal::Checkpoint {
                checkpoint_id: barrier_id,
                mode,
            },
            BarrierControlSignalKind::StreamGracefulEnd => {
                BarrierControlSignal::StreamGracefulEnd { barrier_id }
            }
            BarrierControlSignalKind::SyncTest => BarrierControlSignal::SyncTest { barrier_id },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BarrierControlSignal {
    Checkpoint {
        checkpoint_id: u64,
        mode: CheckpointMode,
    },
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
            BarrierControlSignal::Checkpoint { checkpoint_id, .. } => *checkpoint_id,
            BarrierControlSignal::StreamGracefulEnd { barrier_id } => *barrier_id,
            BarrierControlSignal::SyncTest { barrier_id } => *barrier_id,
        }
    }

    pub fn kind(&self) -> BarrierControlSignalKind {
        match self {
            BarrierControlSignal::Checkpoint { mode, .. } => {
                BarrierControlSignalKind::Checkpoint { mode: *mode }
            }
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
    pub fn id(&self) -> u64 {
        match self {
            ControlSignal::Barrier(barrier) => barrier.barrier_id(),
            ControlSignal::Instant(instant) => match instant {
                InstantControlSignal::StreamQuickEnd { signal_id } => *signal_id,
            },
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { .. })
                | ControlSignal::Barrier(BarrierControlSignal::Checkpoint {
                    mode: CheckpointMode::Final,
                    ..
                })
                | ControlSignal::Instant(InstantControlSignal::StreamQuickEnd { .. })
        )
    }

    pub fn checkpoint_mode(&self) -> Option<CheckpointMode> {
        match self {
            ControlSignal::Barrier(BarrierControlSignal::Checkpoint { mode, .. }) => Some(*mode),
            _ => None,
        }
    }

    pub fn is_checkpoint(&self) -> bool {
        self.checkpoint_mode().is_some()
    }

    pub fn is_graceful_end(&self) -> bool {
        matches!(
            self,
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { .. })
                | ControlSignal::Barrier(BarrierControlSignal::Checkpoint {
                    mode: CheckpointMode::Final,
                    ..
                })
        )
    }
}

/// Core data type for stream processing - unified enum for all data types
#[derive(Clone)]
pub enum StreamData {
    /// Data payload - Collection (owned)
    Collection(Box<dyn Collection>),
    /// Encoded bytes delivery event for sink connectors.
    EncodedDelivery {
        flags: EncodedDeliveryFlags,
        bytes: Bytes,
    },
    /// Raw bytes that still need to be decoded into a collection
    Bytes(Bytes),
    /// Control signal for flow management
    Control(ControlSignal),
    /// Watermark for time progression
    Watermark(SystemTime),
    /// Error event emitted through the data channel.
    ///
    /// Note: processors typically record runtime errors via [`crate::processor::ProcessorStats`]
    /// and rely on logs for diagnostics instead of forwarding errors as data.
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
    pub fn bytes(payload: impl Into<Bytes>) -> Self {
        StreamData::Bytes(payload.into())
    }

    /// Create encoded delivery event.
    pub fn encoded_delivery(flags: EncodedDeliveryFlags, bytes: impl Into<Bytes>) -> Self {
        StreamData::EncodedDelivery {
            flags,
            bytes: bytes.into(),
        }
    }

    pub fn encoded_delivery_start(bytes: impl Into<Bytes>) -> Self {
        Self::encoded_delivery(EncodedDeliveryFlags::START, bytes)
    }

    pub fn encoded_delivery_chunk(bytes: impl Into<Bytes>) -> Self {
        Self::encoded_delivery(EncodedDeliveryFlags::empty(), bytes)
    }

    pub fn encoded_delivery_end(bytes: impl Into<Bytes>) -> Self {
        Self::encoded_delivery(EncodedDeliveryFlags::END, bytes)
    }

    pub fn encoded_delivery_single(bytes: impl Into<Bytes>) -> Self {
        Self::encoded_delivery(
            EncodedDeliveryFlags::START | EncodedDeliveryFlags::END,
            bytes,
        )
    }

    pub fn encoded_delivery_abort() -> Self {
        Self::encoded_delivery(EncodedDeliveryFlags::ABORT, Bytes::new())
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
            StreamData::Collection(_) | StreamData::Bytes(_) | StreamData::EncodedDelivery { .. }
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

    /// Return the row count when this item directly exposes rows.
    ///
    /// Raw and encoded byte payloads are messages, not rows. Their row cardinality is unknown at
    /// this boundary and must not be inferred as one.
    pub fn row_count(&self) -> Option<u64> {
        match self {
            StreamData::Collection(collection) => Some(collection.num_rows() as u64),
            StreamData::Bytes(_)
            | StreamData::EncodedDelivery { .. }
            | StreamData::Control(_)
            | StreamData::Watermark(_)
            | StreamData::Error(_) => None,
        }
    }

    /// Get a human-readable description
    pub fn description(&self) -> String {
        match self {
            StreamData::Collection(collection) => {
                format!("Collection with {} rows", collection.num_rows())
            }
            StreamData::EncodedDelivery { flags, bytes } => {
                format!(
                    "Encoded delivery (flags={:?}, {} bytes)",
                    flags,
                    bytes.len()
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
    /// Create a checkpoint barrier on the data channel.
    pub fn checkpoint(checkpoint_id: u64, mode: CheckpointMode) -> Self {
        StreamData::control(ControlSignal::Barrier(BarrierControlSignal::Checkpoint {
            checkpoint_id,
            mode,
        }))
    }

    /// Create stream end signal with an explicit barrier id.
    pub fn stream_graceful_end(barrier_id: u64) -> Self {
        StreamData::control(ControlSignal::Barrier(
            BarrierControlSignal::StreamGracefulEnd { barrier_id },
        ))
    }

    /// Create stream end signal
    pub fn stream_end() -> Self {
        Self::stream_graceful_end(0)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_delivery_single_sets_start_and_end() {
        let data = StreamData::encoded_delivery_single("secret-payload");
        let StreamData::EncodedDelivery { flags, bytes } = data else {
            panic!("expected encoded delivery");
        };

        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
        assert_eq!(bytes.as_ref(), b"secret-payload");
    }

    #[test]
    fn encoded_delivery_hint_and_description_do_not_include_payload() {
        let data = StreamData::encoded_delivery_single("secret-payload");

        assert_eq!(data.row_count(), None);
        let description = data.description();
        assert!(description.contains("Encoded delivery"));
        assert!(description.contains("14 bytes"));
        assert!(!description.contains("secret-payload"));
    }
}
