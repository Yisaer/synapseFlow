//! Pluggable payload decoder abstraction for GBF frames.
//!
//! The GBF transport layer extracts `(timestamp, format_id, payload)` tuples
//! from binary packets.  This module defines the trait that payload decoders
//! implement to convert those tuples into structured output rows.

use flow::model::Tuple;
use flow::planner::decode_projection::DecodeProjection;

/// A decoded frame ready for payload-level decoding.
#[derive(Debug, Clone)]
pub struct GbfPayloadFrame<'a> {
    /// Packet timestamp.
    pub timestamp: u64,
    /// Message / format identifier (CAN ID or SOME/IP message ID).
    pub format_id: u32,
    /// Raw payload bytes (borrowed from the transport buffer).
    pub payload: &'a [u8],
}

/// Trait for pluggable GBF payload decoders.
///
/// Implementations handle CAN (DBC), SOME/IP (ARXML), and future formats.
pub trait PayloadDecoder: Send + Sync {
    /// Return `true` when this decoder recognises `format_id`.
    fn contains_format_id(&self, format_id: u32) -> bool;

    /// Decode a batch of frames into a single [`Tuple`].
    ///
    /// Returns `None` when no frame produced any matching output.
    fn decode_frames(
        &self,
        frames: Vec<GbfPayloadFrame<'_>>,
        projection: Option<&DecodeProjection>,
    ) -> Option<Tuple>;
}
