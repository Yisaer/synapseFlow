//! GBF (General Binary Format) decoder for configurable binary packet decoding.
//!
//! This decoder reads a JSON schema that describes the binary packet structure
//! and decodes data accordingly. It supports:
//! - Primitive types: u8, u16be, u16le, u32be, u64be
//! - Sequences with byte-length limits
//! - Nested struct types
//! - DBC payload format for CAN signal decoding

use std::sync::Arc;

use datatypes::Schema;
use flow::{
    Merger,
    codec::{CodecError, RecordDecoder},
    model::{Collection, RecordBatch},
    planner::decode_projection::DecodeProjection,
};
use serde_json::Value as JsonValue;

use super::can::{CanDecoder, CanFrame};
use crate::schema::dbc::load_can_schema;
use crate::schema::gbf::GbfSchema;

/// Register the `gbf` decoder with the global decoder registry.
pub fn register_gbf_decoder(registry: &flow::DecoderRegistry) {
    registry.register_decoder(
        "gbf",
        Arc::new(|config, schema, stream_name| {
            let schema_path = config
                .props()
                .get("schema_path")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    CodecError::Other("decoder `gbf` requires `schema_path` prop".into())
                })?;

            let gbf_schema = GbfSchema::load(schema_path)
                .map_err(|e| CodecError::Other(format!("failed to load gbf schema: {e}")))?;

            // Get format type (required)
            let format_type = config
                .props()
                .get("format_type")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    CodecError::Other("decoder `gbf` requires `format_type` prop".into())
                })?;

            // Get format schema path
            let format_schema_path = config
                .props()
                .get("format_schema_path")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    CodecError::Other("decoder `gbf` requires `format_schema_path` prop".into())
                })?;

            // Validate format type
            if format_type != "can" {
                return Err(CodecError::Other(format!(
                    "unsupported format_type: {format_type}"
                )));
            }

            let dbc = load_can_schema(format_schema_path).map_err(|e| {
                CodecError::Other(format!(
                    "failed to load can schema from {}: {e}",
                    format_schema_path
                ))
            })?;

            let pattern = config
                .props()
                .get("signal_name_pattern")
                .and_then(JsonValue::as_str)
                .map(|s| s.to_string());

            // Clamp decoded values to the DBC min/max range (default true, to
            // match eKuiper's can_dbc). Set `clamp_to_range: false` to keep raw
            // out-of-range physical values.
            let clamp_to_range = config
                .props()
                .get("clamp_to_range")
                .and_then(JsonValue::as_bool)
                .unwrap_or(true);

            GbfDecoder::new(
                stream_name,
                schema.clone(),
                gbf_schema,
                dbc,
                pattern,
                clamp_to_range,
            )
            .map(|decoder| Arc::new(decoder) as Arc<dyn RecordDecoder>)
        }),
    );
}

/// GBF decoder that converts binary packets into RecordBatch based on JSON schema.
pub struct GbfDecoder {
    parser: crate::codec::gbf_parser::GbfParser,
    can_decoder: CanDecoder,
}

impl GbfDecoder {
    /// Create a new GBF decoder.
    pub fn new(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        gbf_schema: GbfSchema,
        dbc: crate::schema::dbc::DbcJson,
        pattern: Option<String>,
        clamp_to_range: bool,
    ) -> Result<Self, CodecError> {
        let can_decoder = CanDecoder::new(source_name, schema, dbc, pattern, clamp_to_range)?;
        let parser = crate::codec::gbf_parser::GbfParser::new(gbf_schema)?;

        Ok(Self {
            parser,
            can_decoder,
        })
    }
}

impl RecordDecoder for GbfDecoder {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        self.decode_with_projection(payload, None)
    }

    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        let packets = self.parser.split_packets(payload);
        let mut all_tuples = Vec::with_capacity(packets.len());

        // Frame slots as (can_id, offset-into-packet, len). Parsing directly into
        // borrowed offsets avoids the legacy `parse_packet` round-trip, which
        // allocated an owned `Vec<GbfFrame>` with a `.to_vec()` payload copy per
        // frame and then remapped to a second `Vec<CanFrame>` — pure overhead the
        // signal whitelist couldn't elide (issue emqx/VeloFlux#56). Reused across
        // packets within this payload.
        let mut frame_slots: Vec<(u16, u32, u32)> = Vec::new();

        for packet in packets {
            frame_slots.clear();
            let base = packet.as_ptr() as usize;
            let timestamp = self
                .parser
                .parse_packet_with(packet, &mut |can_id, fpayload| {
                    // `parse_packet_with` always emits sub-slices of `packet`, so
                    // the pointer offset is a valid index back into it (zero-copy).
                    if let Ok(cid) = u16::try_from(can_id) {
                        let off = (fpayload.as_ptr() as usize - base) as u32;
                        debug_assert!(off as usize + fpayload.len() <= packet.len());
                        frame_slots.push((cid, off, fpayload.len() as u32));
                    }
                })?;

            // If packet has no frames (e.g., heartbeat or all invalid), decode a
            // single dummy frame so the timestamp-only row is still emitted.
            let can_frames: Vec<CanFrame> = if frame_slots.is_empty() {
                vec![CanFrame {
                    timestamp,
                    can_id: 0xFFFF,
                    payload: &[],
                }]
            } else {
                frame_slots
                    .iter()
                    .map(|&(can_id, off, len)| CanFrame {
                        timestamp,
                        can_id,
                        payload: &packet[off as usize..off as usize + len as usize],
                    })
                    .collect()
            };

            if let Some(tuple) = self.can_decoder.decode_frames(can_frames, projection) {
                all_tuples.push(tuple);
            }
        }

        if all_tuples.is_empty() {
            return Err(CodecError::Other("no valid frames decoded".into()));
        }

        Ok(RecordBatch::new(all_tuples)?)
    }
}

/// Fused GBF sampler: accumulates raw GBF packets and decodes them directly to a
/// [`RecordBatch`] on trigger, without the intermediate GBF re-encode + re-parse
/// round-trip used by the separate [`GbfMerger`] + [`GbfDecoder`] pipeline.
///
/// The legacy Packer path does:
///   merge (parse_packet + HashMap dedup) → trigger (GbfEncoder::encode → bytes)
///   → GbfDecoder::decode (parse_packet AGAIN → CanFrame → decode_frames)
///
/// This type collapses that to:
///   merge (parse_packet, append) → trigger_decoded (CanFrame → decode_frames)
///
/// It also drops the merger's `HashMap<can_id, payload>` dedup, because
/// [`CanDecoder::decode_frames`] already keeps the last frame per `can_id` (and
/// per mux value), so the HashMap was redundant work — and dropping it fixes the
/// multiplexed-frame loss tracked in #41 (the merger's CAN-ID-only key collapsed
/// distinct mux pages; the decoder dedups per (message, mux) instead).
///
/// [`GbfMerger`]: crate::codec::GbfMerger
/// A frame slot referencing a `[start, start+len)` range in `payload_buf`.
struct FrameSlot {
    can_id: u32,
    start: u32,
    len: u32,
}

pub struct GbfFusedSampler {
    parser: crate::codec::gbf_parser::GbfParser,
    can_decoder: CanDecoder,
    /// All frame payloads for the current interval, concatenated into one
    /// reusable buffer to avoid a per-frame allocation.
    payload_buf: Vec<u8>,
    /// Frame index into `payload_buf` (insertion order preserved).
    frames: Vec<FrameSlot>,
    /// Timestamp of the most recent packet merged this interval.
    last_ts: u64,
}

impl GbfFusedSampler {
    /// Create a fused sampler from the same inputs as [`GbfDecoder::new`].
    pub fn new(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        gbf_schema: GbfSchema,
        dbc: crate::schema::dbc::DbcJson,
        pattern: Option<String>,
        clamp_to_range: bool,
    ) -> Result<Self, CodecError> {
        let can_decoder = CanDecoder::new(source_name, schema, dbc, pattern, clamp_to_range)?;
        let parser = crate::codec::gbf_parser::GbfParser::new(gbf_schema)?;
        Ok(Self {
            parser,
            can_decoder,
            payload_buf: Vec::with_capacity(64 * 8),
            frames: Vec::with_capacity(64),
            last_ts: 0,
        })
    }

    /// Decode all accumulated frames into a [`RecordBatch`] and reset the buffer.
    ///
    /// Returns `Ok(None)` if no frames accumulated or no frame matched the DBC.
    pub fn decode_window(
        &mut self,
        projection: Option<&DecodeProjection>,
    ) -> Result<Option<RecordBatch>, CodecError> {
        if self.frames.is_empty() {
            return Ok(None);
        }

        let ts = self.last_ts;
        let buf = self.payload_buf.as_slice();
        let can_frames: Vec<CanFrame<'_>> = self
            .frames
            .iter()
            .filter_map(|slot| {
                u16::try_from(slot.can_id).ok().map(|can_id| CanFrame {
                    timestamp: ts,
                    can_id,
                    payload: &buf[slot.start as usize..slot.start as usize + slot.len as usize],
                })
            })
            .collect();

        let batch = match self.can_decoder.decode_frames(can_frames, projection) {
            Some(tuple) => Some(RecordBatch::new(vec![tuple])?),
            None => None,
        };

        // Retain capacity across intervals to avoid re-allocating the buffers.
        self.payload_buf.clear();
        self.frames.clear();
        Ok(batch)
    }
}

impl Merger for GbfFusedSampler {
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError> {
        // Split borrows so the parse callback can append into the arena while
        // `parser` is borrowed immutably.
        let Self {
            parser,
            payload_buf,
            frames,
            last_ts,
            ..
        } = self;
        let timestamp = parser.parse_packet_with(data, &mut |can_id, payload| {
            let start = payload_buf.len() as u32;
            payload_buf.extend_from_slice(payload);
            frames.push(FrameSlot {
                can_id,
                start,
                len: payload.len() as u32,
            });
        })?;
        *last_ts = timestamp;
        Ok(())
    }

    /// Not used: the fused sampler always emits a decoded collection via
    /// [`Merger::trigger_decoded`]. Returning `None` keeps the trait contract
    /// satisfied without producing binary output.
    fn trigger(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
        Ok(None)
    }

    fn supports_fused_decode(&self) -> bool {
        true
    }

    fn trigger_decoded(
        &mut self,
        projection: Option<&DecodeProjection>,
    ) -> Result<Option<Box<dyn Collection>>, CodecError> {
        Ok(self
            .decode_window(projection)?
            .map(|batch| Box::new(batch) as Box<dyn Collection>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::dbc::{load_dbc_json, schema_from_dbc};
    use datatypes::Value;
    use flow::codec::RecordDecoder;

    fn get_test_schema() -> GbfSchema {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { 
                        "name": "frames", 
                        "type": "sequence",
                        "length_ref": "total_len", 
                        "structure": { 
                            "type": "struct",
                            "fields": [
                                { "name": "magic", "type": "u8", "const": 85 },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "data_len", "type": "u8" },
                                { 
                                    "name": "payload", 
                                    "type": "bytes",
                                    "length_ref": "data_len",
                                    "format": { "type": "dbc", "id_ref": "can_id" }
                                }
                            ]
                        },
                        "length_unit": "bytes"
                    }
                ]
            }
        }
        "#;
        serde_json::from_str(json).expect("parse schema")
    }

    fn get_test_decoder() -> GbfDecoder {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = Arc::new(schema_from_dbc("can", &dbc, None));
        let gbf_schema = get_test_schema();
        GbfDecoder::new("can", schema.clone(), gbf_schema, dbc.clone(), None, true)
            .expect("build decoder")
    }

    #[test]
    fn test_gbf_decoder_with_dbc() {
        let decoder = get_test_decoder();

        // SPI packet consistent with upstream test
        let hex_string = "00000190A5A0EC4A001855158688085465737400001155124A880854657374000011";
        let packet: Vec<u8> = (0..hex_string.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_string[i..i + 2], 16).unwrap())
            .collect();

        let batch = decoder.decode(&packet).expect("decode");
        let rows = batch.rows();
        assert_eq!(rows.len(), 1);

        let tuple = &rows[0];
        let ts = tuple.value_by_name("can", "ts").expect("ts not found");
        assert_eq!(*ts, Value::Int64(1720765705290));

        // Frame 1: CAN ID 0x1586 -> Mess1 (PropulsionCAN, frameId 0x586)
        let sig1 = tuple
            .value_by_name("can", "Mess1_Sig1")
            .expect("Mess1_Sig1 not found");
        assert_eq!(*sig1, Value::Int64(84));
    }

    fn get_test_fused() -> GbfFusedSampler {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = Arc::new(schema_from_dbc("can", &dbc, None));
        let gbf_schema = get_test_schema();
        GbfFusedSampler::new("can", schema, gbf_schema, dbc, None, true).expect("build fused")
    }

    /// The fused merge+decode path must produce the same decoded values as the
    /// round-trip (GbfMerger -> bytes -> GbfDecoder) path for the same input.
    #[test]
    fn test_fused_matches_roundtrip() {
        use crate::codec::GbfMerger;
        use flow::Merger;

        let hex_string = "00000190A5A0EC4A001855158688085465737400001155124A880854657374000011";
        let packet: Vec<u8> = (0..hex_string.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_string[i..i + 2], 16).unwrap())
            .collect();

        // Round-trip path.
        let mut merger = GbfMerger::new(get_test_schema()).expect("merger");
        merger.merge(&packet).expect("merge");
        let bytes = merger.trigger().expect("trigger").expect("bytes");
        let rt = get_test_decoder().decode(&bytes).expect("roundtrip decode");

        // Fused path on the same raw packet.
        let mut fused = get_test_fused();
        fused.merge(&packet).expect("fused merge");
        let fu = fused
            .decode_window(None)
            .expect("fused decode")
            .expect("fused batch");

        assert_eq!(rt.rows().len(), fu.rows().len());
        let (rt_t, fu_t) = (&rt.rows()[0], &fu.rows()[0]);
        for name in ["ts", "Mess0_Sig1", "Mess1_Sig1", "Mess1_Sig2"] {
            assert_eq!(
                rt_t.value_by_name("can", name),
                fu_t.value_by_name("can", name),
                "value mismatch for `{name}` between fused and round-trip paths"
            );
        }
    }

    /// The fused path must honor the decode projection: only requested columns
    /// are decoded; columns from other messages stay null. This exercises the
    /// projection plumbing the SamplerProcessor passes from the shared decode
    /// state on each tick.
    #[test]
    fn test_fused_projection_restricts_decoded_columns() {
        use flow::Merger;
        use flow::planner::decode_projection::DecodeProjection;

        // One Mess0 frame (can_id 586) + one Mess1 frame (can_id 1414).
        fn push_frame(buf: &mut Vec<u8>, can_id: u16, payload: &[u8]) {
            buf.push(0x55);
            buf.extend_from_slice(&can_id.to_be_bytes());
            buf.push(payload.len() as u8);
            buf.extend_from_slice(payload);
        }
        // CAN ids are bus-prefixed: (bus_id << 12) | msg_id with bus_id=1.
        let pay = [0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44];
        let mut frames = Vec::new();
        push_frame(&mut frames, 0x124A, &pay); // Mess0 (586)
        push_frame(&mut frames, 0x1586, &pay); // Mess1 (1414)
        let mut packet = Vec::new();
        packet.extend_from_slice(&1_000u64.to_be_bytes());
        packet.extend_from_slice(&(frames.len() as u16).to_be_bytes());
        packet.extend_from_slice(&frames);

        // Full decode: signals from both messages present.
        let mut full = get_test_fused();
        full.merge(&packet).expect("merge full");
        let full_batch = full.decode_window(None).expect("full").expect("batch");
        let full_row = &full_batch.rows()[0];
        assert_ne!(
            full_row.value_by_name("can", "Mess0_Sig1"),
            Some(&Value::Null),
            "Mess0_Sig1 should decode under full projection"
        );

        // Projected decode: only ts + Mess1_Sig1 requested.
        let projection = DecodeProjection::from_top_level_columns_with_version(
            &["ts".to_string(), "Mess1_Sig1".to_string()],
            1,
        );
        let mut proj = get_test_fused();
        proj.merge(&packet).expect("merge proj");
        let proj_batch = proj
            .decode_window(Some(&projection))
            .expect("proj")
            .expect("batch");
        let proj_row = &proj_batch.rows()[0];

        // Requested column is decoded and matches the full value.
        assert_eq!(
            proj_row.value_by_name("can", "Mess1_Sig1"),
            full_row.value_by_name("can", "Mess1_Sig1"),
            "projected column must decode to the same value"
        );
        // A column from a non-requested message is pruned (null).
        assert_eq!(
            proj_row.value_by_name("can", "Mess0_Sig1"),
            Some(&Value::Null),
            "non-projected column must stay null under projection"
        );
    }

    #[test]
    fn test_decode_empty_payload() {
        let decoder = get_test_decoder();
        let result = decoder.decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_packets_multiple() {
        let decoder = get_test_decoder();

        // Create two consecutive packets
        let mut payload = Vec::new();

        // Packet 1: timestamp + frames_len=0
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // TS=1
        payload.extend_from_slice(&[0, 0]); // total_len=0

        // Packet 2: timestamp + frames_len=0
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 2]); // TS=2
        payload.extend_from_slice(&[0, 0]); // total_len=0

        let packets = decoder.parser.split_packets(&payload);
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].len(), 10);
        assert_eq!(packets[1].len(), 10);
    }

    #[test]
    fn test_decode_heartbeat() {
        let decoder = get_test_decoder();

        // Create packet with no valid frames (heartbeat)
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 100]); // TS=100
        packet.extend_from_slice(&[0, 0]); // total_len=0

        let batch = decoder.decode(&packet).expect("decode");
        let rows = batch.rows();
        assert_eq!(rows.len(), 1);
        let tuple = &rows[0];
        let ts = tuple.value_by_name("can", "ts").expect("ts not found");
        assert_eq!(*ts, Value::Int64(100));
    }

    #[test]
    fn test_decode_with_mask() {
        // Test that read_mask works correctly for data_len field
        // The test schema (spi_packet.json) has read_mask: 127 for data_len
        let decoder = get_test_decoder();

        // Create a packet where data_len byte is 0x88 (136 without mask, 8 with mask 0x7F)
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // TS=1
        packet.extend_from_slice(&[0, 12]); // total_len=12 (1 frame: magic + can_id + data_len + payload)

        // Frame: magic(1) + can_id(2) + data_len(1) + payload(8) = 12 bytes
        packet.push(0x55); // magic
        packet.extend_from_slice(&[0x15, 0x86]); // can_id = 0x1586
        packet.push(0x88); // data_len = 0x88 -> 8 with mask 0x7F
        packet.extend_from_slice(&[0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x11]); // 8 bytes payload

        let batch = decoder.decode(&packet).expect("decode");
        let rows = batch.rows();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_decode_multiple_frames() {
        let decoder = get_test_decoder();

        // Create a packet with 2 frames
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 42]); // TS=42
        packet.extend_from_slice(&[0, 24]); // total_len=24 (2 frames * 12 bytes each)

        // Frame 1: Mess1 (CAN ID 0x1586)
        packet.push(0x55); // magic
        packet.extend_from_slice(&[0x15, 0x86]); // can_id = 0x1586
        packet.push(0x08); // data_len = 8
        packet.extend_from_slice(&[0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x11]); // payload

        // Frame 2: Mess0 (CAN ID 0x124A)
        packet.push(0x55); // magic
        packet.extend_from_slice(&[0x12, 0x4A]); // can_id = 0x124A
        packet.push(0x08); // data_len = 8
        packet.extend_from_slice(&[0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x11]); // payload

        let batch = decoder.decode(&packet).expect("decode");
        let rows = batch.rows();
        assert_eq!(rows.len(), 1);

        // Both signals from both frames should be decoded
        let tuple = &rows[0];
        let sig1 = tuple.value_by_name("can", "Mess1_Sig1");
        assert!(sig1.is_some());
        let sig0 = tuple.value_by_name("can", "Mess0_Sig1");
        assert!(sig0.is_some());
    }

    #[test]
    fn test_decode_invalid_magic() {
        let decoder = get_test_decoder();

        // Create a packet with invalid magic byte
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // TS=1
        packet.extend_from_slice(&[0, 12]); // total_len=12

        // Frame with wrong magic (0xAA instead of 0x55)
        packet.push(0xAA); // WRONG magic
        packet.extend_from_slice(&[0x15, 0x86]); // can_id
        packet.push(0x08); // data_len
        packet.extend_from_slice(&[0x00; 8]); // payload

        // Should still decode without crashing (frame skipped due to invalid magic)
        let batch = decoder.decode(&packet).expect("decode");
        let rows = batch.rows();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_decode_truncated_frame() {
        let decoder = get_test_decoder();

        // Create a packet that's too short to be valid
        // Header is 10 bytes (8 ts + 2 total_len), but total_len claims more data
        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // TS=1
        packet.extend_from_slice(&[0, 100]); // total_len=100 (claims 100 bytes of frames)

        // Only 4 bytes of frame data (not enough for a complete frame)
        packet.push(0x55); // magic
        packet.extend_from_slice(&[0x15, 0x86]); // can_id
        packet.push(0x40); // data_len = 64 (but payload is missing)
        // No payload!

        // Decoder should error since the packet is truncated
        let result = decoder.decode(&packet);
        assert!(result.is_err());
    }
}
