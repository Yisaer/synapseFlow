//! Stream data types for processor communication
//! 
//! Defines the data types that flow between processors in the stream processing pipeline.

use crate::model::Collection;

/// Control signals for stream processing
#[derive(Debug, Clone, PartialEq)]
pub enum ControlSignal {
    /// Stream start signal
    StreamStart,
    /// Stream end signal  
    StreamEnd,
    /// Watermark for time-based processing
    Watermark(std::time::SystemTime),
    /// Backpressure signal - slow down
    Backpressure,
    /// Resume normal processing
    Resume,
    /// Flush buffered data
    Flush,
}

/// Core data type for stream processing - unified enum for all data types
#[derive(Clone)]
pub enum StreamData {
    /// Data payload - Collection (boxed trait object)
    Collection(Box<dyn Collection>),
    /// Control signal for flow management
    Control(ControlSignal),
}

impl std::fmt::Debug for StreamData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamData::Collection(collection) => {
                f.debug_struct("Collection")
                    .field("num_rows", &collection.num_rows())
                    .field("num_columns", &collection.num_columns())
                    .finish()
            }
            StreamData::Control(signal) => {
                f.debug_tuple("Control")
                    .field(signal)
                    .finish()
            }
        }
    }
}

impl StreamData {
    /// Create Collection data
    pub fn collection(collection: Box<dyn Collection>) -> Self {
        StreamData::Collection(collection)
    }
    
    /// Create control signal
    pub fn control(signal: ControlSignal) -> Self {
        StreamData::Control(signal)
    }
    
    /// Check if this is data (Collection)
    pub fn is_data(&self) -> bool {
        matches!(self, StreamData::Collection(_))
    }
    
    /// Check if this is a control signal
    pub fn is_control(&self) -> bool {
        matches!(self, StreamData::Control(_))
    }
    
    /// Check if this is a terminal signal (StreamEnd)
    pub fn is_terminal(&self) -> bool {
        match self {
            StreamData::Control(ControlSignal::StreamEnd) => true,
            _ => false,
        }
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
    
    /// Get a human-readable description
    pub fn description(&self) -> String {
        match self {
            StreamData::Collection(collection) => {
                format!("Collection with {} rows", collection.num_rows())
            }
            StreamData::Control(signal) => format!("Control signal: {:?}", signal),
        }
    }
}

/// Convenience methods for common control signals
impl StreamData {
    /// Create stream start signal
    pub fn stream_start() -> Self {
        StreamData::control(ControlSignal::StreamStart)
    }
    
    /// Create stream end signal
    pub fn stream_end() -> Self {
        StreamData::control(ControlSignal::StreamEnd)
    }
    
    /// Create backpressure signal
    pub fn backpressure() -> Self {
        StreamData::control(ControlSignal::Backpressure)
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