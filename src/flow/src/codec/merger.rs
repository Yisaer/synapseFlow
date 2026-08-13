//! Merger trait for accumulating raw bytes and producing batched binary output.
//!
//! The Merger is used by the Sampler processor's "Packer" strategy to
//! accumulate raw bytes (e.g., CAN frames in GBF packets) over an interval
//! and emit merged binary data. The merged output is then passed to a
//! Decoder to produce RecordBatches.

use crate::codec::CodecError;
use crate::model::Collection;
use crate::planner::decode_projection::DecodeProjection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergerOutputKind {
    Bytes,
    Collection,
}

/// Trait for merging raw byte data into accumulated state and triggering emission.
///
/// Implementations accumulate incoming byte payloads via `merge()` and produce
/// combined binary output via `trigger()` when the sampling interval elapses.
pub trait Merger: Send + Sync {
    /// Accumulate new byte data into the merger state.
    ///
    /// The bytes typically represent a raw payload (e.g., a GBF packet)
    /// before any decoding has occurred.
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError>;

    /// Trigger emission, returning the accumulated binary result.
    ///
    /// Returns `Ok(Some(bytes))` if there is data to emit,
    /// `Ok(None)` if no data accumulated, or `Err` on failure.
    fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError>;

    /// Whether this merger can decode accumulated data directly into a
    /// [`Collection`] via [`Merger::trigger_decoded`], skipping the
    /// re-encode + re-parse round-trip through [`Merger::trigger`].
    ///
    /// When `true`, the sampler calls [`Merger::trigger_decoded`] on tick and
    /// emits a `Collection` instead of `Bytes`; the downstream decoder node
    /// then forwards the already-decoded collection unchanged.
    fn supports_fused_decode(&self) -> bool {
        false
    }

    /// Trigger emission, decoding the accumulated data directly into a
    /// [`Collection`] (skipping the binary round-trip).
    ///
    /// `projection`, when present, restricts the decoded columns. Returns
    /// `Ok(None)` when nothing accumulated. The default implementation returns
    /// `Ok(None)`; only mergers reporting [`Merger::supports_fused_decode`] need
    /// to override it.
    fn trigger_decoded(
        &mut self,
        _projection: Option<&DecodeProjection>,
    ) -> Result<Option<Box<dyn Collection>>, CodecError> {
        Ok(None)
    }
}
