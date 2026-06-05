use flate2::{Compress, Compression, FlushCompress, Status};

use super::{CompressError, CompressWriter};

// Fixed gzip header bytes (RFC 1952):
//   ID1=0x1f, ID2=0x8b, CM=8, FLG=0, MTIME=0x00000000, XFL=0, OS=255
const GZIP_HEADER: [u8; 10] = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255];

/// Gzip `CompressWriter` using `flate2::Compress` (raw deflate) with manual framing.
///
/// The gzip header is written lazily on the first `write()` or `finish()` call in
/// a delivery, so that an empty `begin_delivery()` (followed only by `finish()`)
/// still produces a valid gzip stream containing the header and trailer.
pub struct GzipWriter {
    compress: Compress,
    hasher: crc32fast::Hasher,
    input_size: u64,
    header_written: bool,
}

impl GzipWriter {
    pub fn new(level: Compression) -> Self {
        Self {
            // false = raw deflate (no zlib wrapper); we write gzip framing manually
            compress: Compress::new(level, false),
            hasher: crc32fast::Hasher::new(),
            input_size: 0,
            header_written: false,
        }
    }

    fn ensure_header(&mut self, out: &mut Vec<u8>) {
        if !self.header_written {
            out.extend_from_slice(&GZIP_HEADER);
            self.header_written = true;
        }
    }
}

impl CompressWriter for GzipWriter {
    fn begin_delivery(&mut self) -> Result<(), CompressError> {
        self.compress.reset();
        self.hasher = crc32fast::Hasher::new();
        self.input_size = 0;
        self.header_written = false;
        Ok(())
    }

    fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError> {
        self.ensure_header(out);
        self.hasher.update(input);
        self.input_size += input.len() as u64;

        let mut offset = 0;
        while offset < input.len() {
            out.reserve(input.len().max(4096));
            let before_in = self.compress.total_in() as usize;
            let status = self
                .compress
                .compress_vec(&input[offset..], out, FlushCompress::None)
                .map_err(|e| CompressError::Compress(e.to_string()))?;
            let advanced = self.compress.total_in() as usize - before_in;
            offset += advanced;
            // If no progress and no spare capacity, reserve more.
            if advanced == 0 && !matches!(status, Status::StreamEnd) {
                out.reserve(4096);
            }
        }
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError> {
        self.ensure_header(out);
        loop {
            out.reserve(4096);
            let status = self
                .compress
                .compress_vec(&[], out, FlushCompress::Finish)
                .map_err(|e| CompressError::Compress(e.to_string()))?;
            if matches!(status, Status::StreamEnd) {
                break;
            }
        }
        let crc = self.hasher.clone().finalize();
        let size_mod = (self.input_size & 0xFFFF_FFFF) as u32;
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&size_mod.to_le_bytes());
        Ok(())
    }

    fn abort_delivery(&mut self) {
        self.compress.reset();
        self.header_written = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decompress(data: &[u8]) -> Vec<u8> {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(data);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("gzip decompress");
        out
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[test]
    fn write_finish_produces_valid_gzip() {
        let mut w = GzipWriter::new(Compression::default());
        w.begin_delivery().unwrap();
        let mut out = Vec::new();
        w.write(b"hello world", &mut out).unwrap();
        w.finish(&mut out).unwrap();
        assert_eq!(decompress(&out), b"hello world");
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[test]
    fn two_deliveries_are_independent() {
        let mut w = GzipWriter::new(Compression::default());

        w.begin_delivery().unwrap();
        let mut out1 = Vec::new();
        w.write(b"first delivery", &mut out1).unwrap();
        w.finish(&mut out1).unwrap();

        w.begin_delivery().unwrap();
        let mut out2 = Vec::new();
        w.write(b"second delivery", &mut out2).unwrap();
        w.finish(&mut out2).unwrap();

        assert_eq!(decompress(&out1), b"first delivery");
        assert_eq!(decompress(&out2), b"second delivery");
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[test]
    fn empty_input_produces_valid_empty_gzip() {
        let mut w = GzipWriter::new(Compression::default());
        w.begin_delivery().unwrap();
        let mut out = Vec::new();
        w.finish(&mut out).unwrap();
        assert_eq!(decompress(&out), b"");
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[test]
    fn multi_chunk_matches_single_write() {
        let input = b"chunk one chunk two chunk three";
        let mut w = GzipWriter::new(Compression::default());

        w.begin_delivery().unwrap();
        let mut single_out = Vec::new();
        w.write(input, &mut single_out).unwrap();
        w.finish(&mut single_out).unwrap();

        w.begin_delivery().unwrap();
        let mut multi_out = Vec::new();
        w.write(b"chunk one ", &mut multi_out).unwrap();
        w.write(b"chunk two ", &mut multi_out).unwrap();
        w.write(b"chunk three", &mut multi_out).unwrap();
        w.finish(&mut multi_out).unwrap();

        assert_eq!(decompress(&single_out), input);
        assert_eq!(decompress(&multi_out), input);
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[test]
    fn abort_then_begin_produces_clean_delivery() {
        let mut w = GzipWriter::new(Compression::default());
        w.begin_delivery().unwrap();
        let mut out = Vec::new();
        w.write(b"will be aborted", &mut out).unwrap();
        w.abort_delivery();

        w.begin_delivery().unwrap();
        let mut out2 = Vec::new();
        w.write(b"clean", &mut out2).unwrap();
        w.finish(&mut out2).unwrap();
        assert_eq!(decompress(&out2), b"clean");
    }
}
