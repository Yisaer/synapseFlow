//! Sink delivery compression transforms.
//!
//! Provides `CompressWriter` trait and gzip/zstd implementations for
//! natural-drain streaming compression in the sink delivery pipeline.

mod gzip;
mod zstd;

pub use gzip::GzipWriter;
pub use zstd::ZstdWriter;

use flate2::Compression;

/// Errors produced by compression writers.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    #[error("compress error: {0}")]
    Compress(String),
    #[error("invalid compression level: {0}")]
    InvalidLevel(String),
}

/// Codec variant and level carried from planner through to processor builder.
#[derive(Debug, Clone, PartialEq)]
pub enum CompressionCodec {
    Gzip { level: Option<u32> },
    Zstd { level: Option<i32> },
}

impl CompressionCodec {
    pub fn gzip() -> Self {
        Self::Gzip { level: None }
    }

    pub fn gzip_with_level(level: u32) -> Self {
        Self::Gzip { level: Some(level) }
    }

    pub fn zstd() -> Self {
        Self::Zstd { level: None }
    }

    pub fn zstd_with_level(level: i32) -> Self {
        Self::Zstd { level: Some(level) }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            CompressionCodec::Gzip { .. } => "gzip",
            CompressionCodec::Zstd { .. } => "zstd",
        }
    }

    pub fn level_display(&self) -> Option<String> {
        match self {
            CompressionCodec::Gzip { level: Some(l) } => Some(l.to_string()),
            CompressionCodec::Zstd { level: Some(l) } => Some(l.to_string()),
            _ => None,
        }
    }

    /// Build a `CompressWriter` for this codec. Validates level ranges.
    pub fn build_writer(&self) -> Result<Box<dyn CompressWriter>, CompressError> {
        match self {
            CompressionCodec::Gzip { level } => {
                let compression = match level {
                    None => Compression::default(),
                    Some(l) => {
                        if *l > 9 {
                            return Err(CompressError::InvalidLevel(format!(
                                "gzip level {l} out of range (0..=9)"
                            )));
                        }
                        Compression::new(*l)
                    }
                };
                Ok(Box::new(GzipWriter::new(compression)))
            }
            CompressionCodec::Zstd { level } => {
                let l = level.unwrap_or(0);
                let range = ::zstd::compression_level_range();
                if !range.contains(&l) {
                    return Err(CompressError::InvalidLevel(format!(
                        "zstd level {l} out of range ({}..={})",
                        range.start(),
                        range.end()
                    )));
                }
                Ok(Box::new(ZstdWriter::new(l)?))
            }
        }
    }
}

/// Streaming compressor for one delivery at a time.
///
/// Writer instances are reused across deliveries; `begin_delivery` resets per-delivery
/// state without reallocating the underlying compressor context.
pub trait CompressWriter: Send {
    /// Reset per-delivery state. Called on `START`.
    fn begin_delivery(&mut self) -> Result<(), CompressError>;
    /// Compress `input`, appending naturally-produced bytes to `out`. May append
    /// nothing if the compressor's internal block has not filled yet.
    fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError>;
    /// Finalize the stream, flushing any buffered data and appending the trailer.
    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError>;
    /// Discard in-progress delivery state. Called on `ABORT`.
    fn abort_delivery(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_codec_build_writer_gzip_level_validation() {
        assert!(CompressionCodec::gzip_with_level(9).build_writer().is_ok());
        assert!(CompressionCodec::gzip_with_level(10)
            .build_writer()
            .is_err());
    }

    #[test]
    fn compression_codec_build_writer_zstd_default_level() {
        assert!(CompressionCodec::zstd().build_writer().is_ok());
    }
}
