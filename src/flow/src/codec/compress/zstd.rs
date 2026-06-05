use zstd::stream::raw::Operation;

use super::{CompressError, CompressWriter};

/// Zstd `CompressWriter` using `zstd::stream::raw::Encoder` with natural drain.
pub struct ZstdWriter {
    encoder: zstd::stream::raw::Encoder<'static>,
}

impl ZstdWriter {
    pub fn new(level: i32) -> Result<Self, CompressError> {
        let encoder = zstd::stream::raw::Encoder::new(level)
            .map_err(|e| CompressError::Compress(e.to_string()))?;
        Ok(Self { encoder })
    }
}

impl CompressWriter for ZstdWriter {
    fn begin_delivery(&mut self) -> Result<(), CompressError> {
        self.encoder
            .reinit()
            .map_err(|e| CompressError::Compress(e.to_string()))
    }

    fn write(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), CompressError> {
        let mut in_buf = zstd::stream::raw::InBuffer::around(input);
        while in_buf.pos() < input.len() {
            out.reserve(input.len().max(4096));
            let pos = out.len();
            let mut out_buf = zstd::stream::raw::OutBuffer::around_pos(out, pos);
            self.encoder
                .run(&mut in_buf, &mut out_buf)
                .map_err(|e| CompressError::Compress(e.to_string()))?;
            // OutBuffer drop calls filled_until() to update out.len()
        }
        Ok(())
    }

    fn finish(&mut self, out: &mut Vec<u8>) -> Result<(), CompressError> {
        loop {
            out.reserve(4096);
            let pos = out.len();
            let mut out_buf = zstd::stream::raw::OutBuffer::around_pos(out, pos);
            let remaining = self
                .encoder
                .finish(&mut out_buf, true)
                .map_err(|e| CompressError::Compress(e.to_string()))?;
            // OutBuffer drop calls filled_until() to update out.len()
            if remaining == 0 {
                break;
            }
        }
        Ok(())
    }

    fn abort_delivery(&mut self) {
        let _ = self.encoder.reinit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decompress(data: &[u8]) -> Vec<u8> {
        zstd::stream::decode_all(data).expect("zstd decompress")
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[test]
    fn write_finish_produces_valid_zstd() {
        let mut w = ZstdWriter::new(0).unwrap();
        w.begin_delivery().unwrap();
        let mut out = Vec::new();
        w.write(b"hello world", &mut out).unwrap();
        w.finish(&mut out).unwrap();
        assert_eq!(decompress(&out), b"hello world");
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[test]
    fn two_deliveries_are_independent() {
        let mut w = ZstdWriter::new(0).unwrap();

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

    // coverage-covers: sink.compress.zstd_delivery
    #[test]
    fn empty_input_produces_valid_empty_zstd() {
        let mut w = ZstdWriter::new(0).unwrap();
        w.begin_delivery().unwrap();
        let mut out = Vec::new();
        w.finish(&mut out).unwrap();
        assert_eq!(decompress(&out), b"");
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[test]
    fn multi_chunk_matches_single_write() {
        let input = b"chunk one chunk two chunk three";
        let mut w = ZstdWriter::new(0).unwrap();

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

    // coverage-covers: sink.compress.zstd_delivery
    #[test]
    fn abort_then_begin_produces_clean_delivery() {
        let mut w = ZstdWriter::new(0).unwrap();
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
