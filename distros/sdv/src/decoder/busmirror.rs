//! AUTOSAR Bus Mirroring decoder and fused packer merger.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datatypes::Schema;
use flow::{
    Merger,
    codec::{CodecError, RecordDecoder},
    model::{Collection, RecordBatch},
    planner::decode_projection::DecodeProjection,
};

use super::can::{CanDecoder, CanMuxKeyResolver, DbcFrame, DbcWindowAccumulator};
use super::payload::FrameIdentity;
use crate::codec::busmirror_parser::{BusMirrorFrameSlot, packet_len, parse_packet};
use crate::schema::busmirror::CompiledBusMirrorSchema;

static MALFORMED_PACKET_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

/// Register the `busmirror` decoder.
pub fn register_busmirror_decoder(registry: &flow::DecoderRegistry) {
    registry.register_decoder(
        "busmirror",
        Arc::new(|config, schema, stream_name| {
            let compiled = config
                .schema_artifact::<CompiledBusMirrorSchema>()
                .ok_or_else(|| {
                    CodecError::Other(
                        "decoder `busmirror` requires a resolved BusMirror schema".into(),
                    )
                })?;
            BusMirrorDecoder::from_compiled(stream_name, schema.clone(), &compiled)
                .map(|decoder| Arc::new(decoder) as Arc<dyn RecordDecoder>)
        }),
    );
}

/// Packet-local BusMirror decoder. Each valid destination frame produces one row.
pub struct BusMirrorDecoder {
    decoder: CanDecoder,
}

impl BusMirrorDecoder {
    pub fn from_compiled(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        compiled: &CompiledBusMirrorSchema,
    ) -> Result<Self, CodecError> {
        Ok(Self {
            decoder: CanDecoder::build_from_busmirror(source_name, schema, compiled.dbc(), true)?,
        })
    }
}

impl RecordDecoder for BusMirrorDecoder {
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        self.decode_with_projection(payload, None)
    }

    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        let mut cursor = 0usize;
        let mut rows = Vec::new();
        let mut slots = Vec::new();
        let mut last_error = None;

        while cursor < payload.len() {
            let length = match packet_len(payload, cursor) {
                Ok(length) => length,
                Err(error) => {
                    last_error = Some(error);
                    break;
                }
            };
            let packet = &payload[cursor..cursor + length];
            match parse_packet(packet, &mut slots) {
                Ok(timestamp) => {
                    let frames = packet_frames(packet, timestamp, &slots);
                    if let Some(row) = self.decoder.decode_dbc_frames(frames, projection) {
                        rows.push(row);
                    }
                }
                Err(error) => {
                    log_malformed_packet(cursor, &error, false);
                    last_error = Some(error);
                }
            }
            cursor += length;
        }

        if rows.is_empty() {
            return Err(last_error.unwrap_or_else(|| {
                CodecError::Other("no valid BusMirror destination frame decoded".to_string())
            }));
        }
        Ok(RecordBatch::new(rows)?)
    }
}

/// Sampling-window BusMirror merger using the shared DBC accumulator.
pub struct BusMirrorFusedMerger {
    accumulator: DbcWindowAccumulator,
    slots: Vec<BusMirrorFrameSlot>,
}

impl BusMirrorFusedMerger {
    pub fn from_compiled(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        compiled: &CompiledBusMirrorSchema,
    ) -> Result<Self, CodecError> {
        let dbc = compiled.dbc().dbc();
        let mux_resolver = CanMuxKeyResolver::from_busmirror_dbc(&dbc);
        let decoder = CanDecoder::build_from_busmirror(source_name, schema, compiled.dbc(), true)?;
        Ok(Self {
            accumulator: DbcWindowAccumulator::new(decoder, mux_resolver),
            slots: Vec::with_capacity(64),
        })
    }
}

impl Merger for BusMirrorFusedMerger {
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError> {
        let mut cursor = 0usize;
        let mut valid_packets = 0usize;
        let mut last_error = None;

        while cursor < data.len() {
            let length = match packet_len(data, cursor) {
                Ok(length) => length,
                Err(error) => {
                    last_error = Some(error);
                    break;
                }
            };
            let packet = &data[cursor..cursor + length];
            match parse_packet(packet, &mut self.slots) {
                Ok(timestamp) => {
                    for slot in &self.slots {
                        let start = slot.payload_offset as usize;
                        let end = start + usize::from(slot.payload_len);
                        self.accumulator
                            .merge_frame(timestamp, slot.identity, &packet[start..end]);
                    }
                    self.accumulator.observe_timestamp(timestamp);
                    valid_packets += 1;
                }
                Err(error) => {
                    log_malformed_packet(cursor, &error, true);
                    last_error = Some(error);
                }
            }
            cursor += length;
        }

        if valid_packets == 0
            && let Some(error) = last_error
        {
            return Err(error);
        }
        Ok(())
    }

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
            .accumulator
            .decode_window(projection)?
            .map(|batch| Box::new(batch) as Box<dyn Collection>))
    }
}

fn packet_frames<'a>(
    packet: &'a [u8],
    timestamp: u64,
    slots: &[BusMirrorFrameSlot],
) -> Vec<DbcFrame<'a>> {
    if slots.is_empty() {
        return vec![DbcFrame {
            timestamp,
            identity: FrameIdentity::gbf(u32::MAX),
            payload: &[],
        }];
    }
    slots
        .iter()
        .map(|slot| {
            let start = slot.payload_offset as usize;
            let end = start + usize::from(slot.payload_len);
            DbcFrame {
                timestamp,
                identity: slot.identity,
                payload: &packet[start..end],
            }
        })
        .collect()
}

fn log_malformed_packet(offset: usize, error: &CodecError, sampling_window: bool) {
    let count = MALFORMED_PACKET_LOG_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if count <= 8 || count.is_power_of_two() {
        tracing::debug!(
            packet_offset = offset,
            error = %error,
            sampling_window,
            malformed_packet_count = count,
            "discarding malformed BusMirror destination frame"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::Value;
    use flow::codec::RecordDecoder;

    use crate::schema::busmirror::CompiledBusMirrorSchema;
    use crate::schema::dbc::{CompiledDbcSchema, load_can_schema, load_dbc_json};

    fn compiled_schema() -> (Arc<Schema>, CompiledBusMirrorSchema) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
        let mut dbc = load_can_schema(path.to_str().expect("fixture path")).expect("load DBC");
        dbc.buses[0].id = 0x101;
        dbc.buses[0].name = Some("Powertrain".to_string());
        let dbc = Arc::new(
            CompiledDbcSchema::new_busmirror(dbc, "{network_id}__{msg_id_hex_lower}__{sig_name}")
                .expect("compile BusMirror DBC"),
        );
        let schema = Arc::new(dbc.schema("mirror"));
        (schema, CompiledBusMirrorSchema::from_dbc(dbc))
    }

    fn destination_frame(timestamp_ms: u64, body: &[u8]) -> Vec<u8> {
        let seconds = timestamp_ms / 1_000;
        let nanoseconds = ((timestamp_ms % 1_000) * 1_000_000) as u32;
        let mut packet = vec![1, 0];
        packet.extend_from_slice(&seconds.to_be_bytes()[2..]);
        packet.extend_from_slice(&nanoseconds.to_be_bytes());
        packet.extend_from_slice(&(body.len() as u16).to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn can_item(frame_id: u32, payload: &[u8]) -> Vec<u8> {
        can_item_for(1, frame_id, payload)
    }

    fn can_item_for(network_id: u8, frame_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut item = vec![0, 0, 0x61, network_id];
        item.extend_from_slice(&frame_id.to_be_bytes());
        item.push(payload.len() as u8);
        item.extend_from_slice(payload);
        item
    }

    fn lin_item(network_id: u8, frame_id: u8, payload: &[u8]) -> Vec<u8> {
        let mut item = vec![0, 0, 0x62, network_id, frame_id, payload.len() as u8];
        item.extend_from_slice(payload);
        item
    }

    #[test]
    fn decoder_keeps_valid_rows_around_a_known_boundary_error() {
        let (schema, compiled) = compiled_schema();
        let decoder =
            BusMirrorDecoder::from_compiled("mirror", schema, &compiled).expect("build decoder");
        let first = destination_frame(1_001, &can_item(0xc000_0100, &[42, 0, 0, 0, 0, 0, 0, 0]));
        let malformed = destination_frame(1_002, &[0, 0, 0x61, 1]);
        let second = destination_frame(1_003, &can_item(0x100, &[43, 0, 0, 0, 0, 0, 0, 0]));
        let payload = [first, malformed, second].concat();

        let batch = decoder.decode(&payload).expect("decode valid packets");

        assert_eq!(batch.rows().len(), 2);
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "1__100__StandardUnsigned"),
            Some(&Value::Int64(42))
        );
        assert_eq!(
            batch.rows()[1].value_by_name("mirror", "1__100__StandardUnsigned"),
            Some(&Value::Int64(43))
        );
        assert_eq!(
            batch.rows()[1].value_by_name("mirror", "ts"),
            Some(&Value::Int64(1_003))
        );
    }

    #[test]
    fn decoder_emits_timestamp_row_for_empty_destination_frame() {
        let (schema, compiled) = compiled_schema();
        let decoder =
            BusMirrorDecoder::from_compiled("mirror", schema, &compiled).expect("build decoder");

        let batch = decoder
            .decode(&destination_frame(1_234, &[]))
            .expect("decode empty destination frame");

        assert_eq!(batch.rows().len(), 1);
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "ts"),
            Some(&Value::Int64(1_234))
        );
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "1__100__StandardUnsigned"),
            Some(&Value::Null)
        );
    }

    #[test]
    fn fused_merger_uses_last_frame_and_clears_after_trigger() {
        let (schema, compiled) = compiled_schema();
        let mut merger =
            BusMirrorFusedMerger::from_compiled("mirror", schema, &compiled).expect("build merger");
        merger
            .merge(&destination_frame(
                1_001,
                &can_item(0x100, &[42, 0, 0, 0, 0, 0, 0, 0]),
            ))
            .expect("merge first packet");
        merger
            .merge(&destination_frame(
                1_003,
                &can_item(0x100, &[43, 0, 0, 0, 0, 0, 0, 0]),
            ))
            .expect("merge second packet");

        let batch = merger
            .accumulator
            .decode_window(None)
            .expect("decode window")
            .expect("window row");
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "1__100__StandardUnsigned"),
            Some(&Value::Int64(43))
        );
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "ts"),
            Some(&Value::Int64(1_003))
        );
        assert!(
            merger
                .accumulator
                .decode_window(None)
                .expect("decode cleared window")
                .is_none()
        );
    }

    #[test]
    fn decoder_keeps_equal_can_ids_on_different_networks() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
        let dbc = load_can_schema(path.to_str().expect("fixture path")).expect("load DBC");
        let mut first = dbc.buses[0].clone();
        first.id = 0x101;
        first.name = Some("First".to_string());
        let mut second = first.clone();
        second.id = 0x102;
        second.name = Some("Second".to_string());
        let dbc = Arc::new(
            CompiledDbcSchema::new_busmirror(
                crate::schema::dbc::DbcJson {
                    buses: vec![first, second],
                },
                "{network_id}__{msg_id_hex_lower}__{sig_name}",
            )
            .expect("compile BusMirror DBC"),
        );
        let schema = Arc::new(dbc.schema("mirror"));
        let compiled = CompiledBusMirrorSchema::from_dbc(dbc);
        let decoder =
            BusMirrorDecoder::from_compiled("mirror", schema, &compiled).expect("build decoder");
        let body = [
            can_item_for(1, 0x100, &[42, 0, 0, 0, 0, 0, 0, 0]),
            can_item_for(2, 0x100, &[43, 0, 0, 0, 0, 0, 0, 0]),
        ]
        .concat();

        let batch = decoder
            .decode(&destination_frame(1_001, &body))
            .expect("decode both networks");

        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "1__100__StandardUnsigned"),
            Some(&Value::Int64(42))
        );
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "2__100__StandardUnsigned"),
            Some(&Value::Int64(43))
        );
    }

    #[test]
    fn decoder_handles_mixed_can_fd_and_lin_items() {
        let dbc: crate::schema::dbc::DbcJson = serde_json::from_value(serde_json::json!({
            "buses": [
                {
                    "id": 0x100,
                    "name": "PT_CAN",
                    "messages": [{
                        "name": "EngineData",
                        "id": 0x4f0,
                        "frameId": "0x4F0",
                        "length": 8,
                        "signals": [{
                            "name": "RPM", "start": 0, "length": 8,
                            "scale": 1, "offset": 0
                        }]
                    }]
                },
                {
                    "id": 0x200,
                    "name": "Body_LIN",
                    "messages": [{
                        "name": "LinNode1",
                        "id": 0x92,
                        "frameId": "0x92",
                        "length": 1,
                        "signals": [{
                            "name": "Temp", "start": 0, "length": 8,
                            "scale": 1, "offset": 0
                        }]
                    }]
                }
            ]
        }))
        .expect("parse mixed DBC");
        let dbc = Arc::new(
            CompiledDbcSchema::new_busmirror(dbc, "{msg_name}__{sig_name}")
                .expect("compile mixed DBC"),
        );
        let schema = Arc::new(dbc.schema("mirror"));
        let compiled = CompiledBusMirrorSchema::from_dbc(dbc);
        let decoder =
            BusMirrorDecoder::from_compiled("mirror", schema, &compiled).expect("build decoder");
        let body = [
            can_item_for(0, 0x4000_04f0, &[42, 0, 0, 0, 0, 0, 0, 0]),
            lin_item(0, 0x92, &[85]),
        ]
        .concat();

        let batch = decoder
            .decode(&destination_frame(1_002, &body))
            .expect("decode mixed packet");

        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "EngineData__RPM"),
            Some(&Value::Int64(42))
        );
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "LinNode1__Temp"),
            Some(&Value::Int64(85))
        );
    }

    #[test]
    fn fused_merger_preserves_distinct_mux_values() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/mul.json");
        let mut dbc = load_dbc_json(path.to_str().expect("fixture path")).expect("load mux DBC");
        dbc.buses[0].id = 0x101;
        let dbc =
            Arc::new(CompiledDbcSchema::new_busmirror(dbc, "{sig_name}").expect("compile mux DBC"));
        let schema = Arc::new(dbc.schema("mirror"));
        let compiled = CompiledBusMirrorSchema::from_dbc(dbc);
        let mut merger =
            BusMirrorFusedMerger::from_compiled("mirror", schema, &compiled).expect("build merger");
        let mux_one = [0x01u8, 0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11];
        let mux_zero = [0x00u8, 0x24, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11];
        merger
            .merge(&destination_frame(1_001, &can_item(200, &mux_one)))
            .expect("merge mux one");
        merger
            .merge(&destination_frame(1_002, &can_item(200, &mux_zero)))
            .expect("merge mux zero");

        let batch = merger
            .accumulator
            .decode_window(None)
            .expect("decode window")
            .expect("window row");
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "SENSOR_SONARS_left"),
            Some(&Value::Float64(86.9))
        );
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "SENSOR_SONARS_no_filt_left"),
            Some(&Value::Float64(86.9))
        );
    }

    #[test]
    fn malformed_packet_does_not_pollute_existing_window_state() {
        let (schema, compiled) = compiled_schema();
        let mut merger =
            BusMirrorFusedMerger::from_compiled("mirror", schema, &compiled).expect("build merger");
        merger
            .merge(&destination_frame(
                1_001,
                &can_item(0x100, &[42, 0, 0, 0, 0, 0, 0, 0]),
            ))
            .expect("merge valid packet");
        let mut malformed_body = can_item(0x100, &[99, 0, 0, 0, 0, 0, 0, 0]);
        malformed_body.extend_from_slice(&[0, 0, 0x61, 1]);
        assert!(
            merger
                .merge(&destination_frame(1_002, &malformed_body))
                .is_err()
        );

        let batch = merger
            .accumulator
            .decode_window(None)
            .expect("decode window")
            .expect("window row");
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "1__100__StandardUnsigned"),
            Some(&Value::Int64(42))
        );
        assert_eq!(
            batch.rows()[0].value_by_name("mirror", "ts"),
            Some(&Value::Int64(1_001))
        );
    }
}
