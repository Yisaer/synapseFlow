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
    codec::{CodecError, RecordDecoder},
    model::RecordBatch,
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
        let mut all_tuples = Vec::new();

        for packet in packets {
            let (timestamp, gbf_frames) = self.parser.parse_packet(packet)?;

            // Convert GbfFrame to CanFrame
            let can_frames: Vec<CanFrame> = gbf_frames
                .iter()
                .map(|gbf_frame| -> Result<CanFrame, CodecError> {
                    let can_id = u16::try_from(gbf_frame.can_id).map_err(|_| {
                        CodecError::Other(format!(
                            "CAN ID {} exceeds u16 range (max 65535)",
                            gbf_frame.can_id
                        ))
                    })?;
                    Ok(CanFrame {
                        timestamp,
                        can_id,
                        payload: &gbf_frame.payload,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            // If packet has no frames (e.g., heartbeat or all invalid), create a dummy frame
            let frames_to_decode = if can_frames.is_empty() {
                vec![CanFrame {
                    timestamp,
                    can_id: 0xFFFF,
                    payload: &[],
                }]
            } else {
                can_frames
            };

            if let Some(tuple) = self.can_decoder.decode_frames(frames_to_decode, projection) {
                all_tuples.push(tuple);
            }
        }

        if all_tuples.is_empty() {
            return Err(CodecError::Other("no valid frames decoded".into()));
        }

        Ok(RecordBatch::new(all_tuples)?)
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
