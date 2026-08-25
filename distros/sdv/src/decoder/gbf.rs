//! GBF (General Binary Format) decoder for configurable binary packet decoding.
//!
//! This decoder reads a JSON schema that describes the binary packet structure
//! and decodes data accordingly. It supports:
//! - Primitive types: u8, u16be, u16le, u32be, u64be
//! - Sequences with byte-length limits
//! - Nested struct types
//! - DBC payload format for CAN signal decoding

use std::sync::Arc;

use super::can::{
    CanDecoder, CanFrameIdentityMapping, CanIdMapping, CanMuxKeyResolver, DbcWindowAccumulator,
};
use super::payload::{GbfPayloadFrame, PayloadDecoder};
use crate::schema::gbf::{
    CompiledGbfFormat, CompiledGbfSchema, GbfSchema, resolve_can_identity_mapping,
};
use datatypes::Schema;
use flow::{
    Merger,
    codec::{CodecError, RecordDecoder},
    model::{Collection, RecordBatch},
    planner::decode_projection::DecodeProjection,
};

/// Register the `gbf` decoder with the global decoder registry.
pub fn register_gbf_decoder(registry: &flow::DecoderRegistry) {
    registry.register_decoder(
        "gbf",
        Arc::new(|config, schema, stream_name| {
            let compiled = config
                .schema_artifact::<CompiledGbfSchema>()
                .ok_or_else(|| {
                    CodecError::Other("decoder `gbf` requires a resolved GBF schema".into())
                })?;
            GbfDecoder::from_compiled_gbf(stream_name, schema.clone(), &compiled)
                .map(|decoder| Arc::new(decoder) as Arc<dyn RecordDecoder>)
        }),
    );
}

/// GBF decoder that converts binary packets into RecordBatch based on JSON schema.
pub struct GbfDecoder {
    parser: crate::codec::gbf_parser::GbfParser,
    payload_decoder: Box<dyn PayloadDecoder>,
}

impl GbfDecoder {
    pub fn from_compiled_gbf(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        compiled: &CompiledGbfSchema,
    ) -> Result<Self, CodecError> {
        let source_name = source_name.into();
        match compiled.format() {
            CompiledGbfFormat::Can {
                schema: dbc,
                clamp_to_range,
                identity_mapping,
            } => {
                let dbc_json = dbc.dbc();
                let payload_decoder = Box::new(CanDecoder::build_from_compiled_with_mapping(
                    source_name,
                    schema,
                    &dbc_json,
                    dbc,
                    *clamp_to_range,
                    *identity_mapping,
                )?);
                Ok(Self {
                    parser: compiled.parser(),
                    payload_decoder,
                })
            }
            CompiledGbfFormat::SomeIp { schema: arxml } => {
                let payload_decoder = Box::new(crate::decoder::someip::SomeIpPayloadDecoder::new(
                    source_name,
                    schema,
                    arxml.codec(),
                    Some(arxml.signal_name_pattern()),
                ));
                Ok(Self {
                    parser: compiled.parser(),
                    payload_decoder,
                })
            }
        }
    }

    /// Create a new GBF decoder for CAN (DBC JSON).
    pub fn new(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        gbf_schema: GbfSchema,
        dbc: crate::schema::dbc::DbcJson,
        pattern: Option<String>,
        clamp_to_range: bool,
        mapping: CanIdMapping,
    ) -> Result<Self, CodecError> {
        let parser = crate::codec::gbf_parser::GbfParser::new(gbf_schema)?;
        let identity_mapping = resolve_can_identity_mapping(&parser, mapping)?;
        let compiled = crate::schema::dbc::CompiledDbcSchema::new(
            dbc,
            pattern.as_deref().unwrap_or("{sig_name}"),
        )
        .map_err(CodecError::Other)?;
        let dbc = compiled.dbc();
        let can_decoder = Box::new(CanDecoder::build_from_compiled_with_mapping(
            source_name,
            schema,
            &dbc,
            &compiled,
            clamp_to_range,
            identity_mapping,
        )?);
        Ok(Self {
            parser,
            payload_decoder: can_decoder,
        })
    }

    /// Create a GBF decoder with an arbitrary payload decoder.
    pub fn with_payload_decoder(
        gbf_schema: GbfSchema,
        payload_decoder: Box<dyn PayloadDecoder>,
    ) -> Result<Self, CodecError> {
        let parser = crate::codec::gbf_parser::GbfParser::new(gbf_schema)?;
        Ok(Self {
            parser,
            payload_decoder,
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
        let mut frame_slots: Vec<(Option<u32>, u32, u32, u32)> = Vec::new();

        for packet in packets {
            frame_slots.clear();
            let base = packet.as_ptr() as usize;
            let timestamp = self
                .parser
                .parse_packet_with(packet, &mut |frame_id, fpayload| {
                    // `parse_packet_with` always emits sub-slices of `packet`, so
                    // the pointer offset is a valid index back into it (zero-copy).
                    let off = (fpayload.as_ptr() as usize - base) as u32;
                    debug_assert!(off as usize + fpayload.len() <= packet.len());
                    frame_slots.push((
                        frame_id.bus_id,
                        frame_id.format_id,
                        off,
                        fpayload.len() as u32,
                    ));
                })?;

            // If packet has no frames (e.g., heartbeat or all invalid), decode a
            // single dummy frame so the timestamp-only row is still emitted.
            let payload_frames: Vec<GbfPayloadFrame<'_>> = if frame_slots.is_empty() {
                vec![GbfPayloadFrame {
                    timestamp,
                    bus_id: None,
                    format_id: u32::MAX,
                    payload: &[],
                }]
            } else {
                frame_slots
                    .iter()
                    .map(|&(bus_id, can_id, off, len)| GbfPayloadFrame {
                        timestamp,
                        bus_id,
                        format_id: can_id,
                        payload: &packet[off as usize..off as usize + len as usize],
                    })
                    .collect()
            };

            if let Some(tuple) = self
                .payload_decoder
                .decode_frames(payload_frames, projection)
            {
                all_tuples.push(tuple);
            }
        }

        if all_tuples.is_empty() {
            return Err(CodecError::Other("no valid frames decoded".into()));
        }

        Ok(RecordBatch::new(all_tuples)?)
    }
}

/// Fused GBF merger: accumulates raw GBF packets and decodes them directly to a
/// [`RecordBatch`] on trigger.
///
/// The GBF layer parses outer packets only. The inner format handler is currently
/// CAN/DBC: unknown CAN IDs are discarded at merge time, non-multiplexed frames
/// use the configured CAN identity, and multiplexed frames additionally include
/// the mux value. The sampler calls this type through the generic [`Merger`] trait.
pub struct GbfFusedMerger {
    parser: crate::codec::gbf_parser::GbfParser,
    accumulator: DbcWindowAccumulator,
    identity_mapping: CanFrameIdentityMapping,
}

impl GbfFusedMerger {
    pub fn from_compiled_gbf(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        compiled: &CompiledGbfSchema,
    ) -> Result<Self, CodecError> {
        match compiled.format() {
            CompiledGbfFormat::Can {
                schema: dbc,
                clamp_to_range,
                identity_mapping,
            } => {
                let dbc_json = dbc.dbc();
                let mux_resolver =
                    CanMuxKeyResolver::from_dbc_with_mapping(&dbc_json, *identity_mapping);
                let decoder = CanDecoder::build_from_compiled_with_mapping(
                    source_name,
                    schema,
                    &dbc_json,
                    dbc,
                    *clamp_to_range,
                    *identity_mapping,
                )?;
                Ok(Self {
                    parser: compiled.parser(),
                    accumulator: DbcWindowAccumulator::new(decoder, mux_resolver),
                    identity_mapping: *identity_mapping,
                })
            }
            CompiledGbfFormat::SomeIp { .. } => Err(CodecError::Other(
                "GBF fused merger currently supports only CAN format".into(),
            )),
        }
    }

    /// Create a fused merger from the same inputs as [`GbfDecoder::new`].
    pub fn new(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        gbf_schema: GbfSchema,
        dbc: crate::schema::dbc::DbcJson,
        pattern: Option<String>,
        clamp_to_range: bool,
        mapping: CanIdMapping,
    ) -> Result<Self, CodecError> {
        let parser = crate::codec::gbf_parser::GbfParser::new(gbf_schema)?;
        let identity_mapping = resolve_can_identity_mapping(&parser, mapping)?;
        let compiled = crate::schema::dbc::CompiledDbcSchema::new(
            dbc,
            pattern.as_deref().unwrap_or("{sig_name}"),
        )
        .map_err(CodecError::Other)?;
        let dbc = compiled.dbc();
        let mux_resolver = CanMuxKeyResolver::from_dbc_with_mapping(&dbc, identity_mapping);
        let can_decoder = CanDecoder::build_from_compiled_with_mapping(
            source_name,
            schema,
            &dbc,
            &compiled,
            clamp_to_range,
            identity_mapping,
        )?;
        Ok(Self {
            parser,
            accumulator: DbcWindowAccumulator::new(can_decoder, mux_resolver),
            identity_mapping,
        })
    }

    /// Decode all accumulated frames into a [`RecordBatch`] and reset the buffer.
    ///
    /// Returns `Ok(None)` if no frames accumulated or no frame matched the DBC.
    pub fn decode_window(
        &mut self,
        projection: Option<&DecodeProjection>,
    ) -> Result<Option<RecordBatch>, CodecError> {
        self.accumulator.decode_window(projection)
    }
}

impl Merger for GbfFusedMerger {
    fn merge(&mut self, data: &[u8]) -> Result<(), CodecError> {
        let Self {
            parser,
            accumulator,
            identity_mapping,
        } = self;
        for packet in parser.split_packets(data) {
            let timestamp = parser.parse_packet_with(packet, &mut |frame_id, payload| {
                accumulator.merge_frame(
                    0,
                    identity_mapping.wire_identity(frame_id.bus_id, frame_id.format_id),
                    payload,
                );
            })?;
            accumulator.observe_timestamp(timestamp);
        }
        Ok(())
    }

    /// Not used: the fused merger always emits a decoded collection via
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
    use crate::schema::dbc::{CompiledDbcSchema, load_dbc_json, schema_from_dbc};
    use crate::schema::gbf::CompiledGbfSchema;
    use datatypes::Value;
    use flow::catalog::StreamDecoderConfig;
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

    fn get_u32_can_id_schema() -> GbfSchema {
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
                                { "name": "can_id", "type": "u32be" },
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
        serde_json::from_str(json).expect("parse u32 can id schema")
    }

    fn get_bus_id_ref_schema() -> GbfSchema {
        serde_json::from_value(serde_json::json!({
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "total_len",
                        "length_unit": "bytes",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "bus_id", "type": "u8" },
                                { "name": "can_id", "type": "u32be" },
                                { "name": "data_len", "type": "u8" },
                                {
                                    "name": "payload",
                                    "type": "bytes",
                                    "length_ref": "data_len",
                                    "format": {
                                        "id_ref": "can_id",
                                        "bus_id_ref": "bus_id"
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }))
        .expect("parse bus-id-ref schema")
    }

    fn get_test_decoder() -> GbfDecoder {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = Arc::new(schema_from_dbc("can", &dbc, None).expect("compile DBC schema"));
        let gbf_schema = get_test_schema();
        // sim.json is bus-prefixed (bus.id = 1) -> historical bus_shift packing.
        GbfDecoder::new(
            "can",
            schema.clone(),
            gbf_schema,
            dbc.clone(),
            None,
            true,
            CanIdMapping::BusShift { bits: 12 },
        )
        .expect("build decoder")
    }

    #[test]
    fn registry_builds_can_decoder_from_schema_artifact() {
        let dbc_path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let compiled = Arc::new(
            CompiledDbcSchema::new(
                load_dbc_json(dbc_path.to_str().unwrap()).unwrap(),
                "{sig_name}",
            )
            .expect("compile DBC schema"),
        );
        let schema = Arc::new(compiled.schema("can"));
        let compiled = Arc::new(
            CompiledGbfSchema::can(
                get_test_schema(),
                compiled,
                true,
                CanIdMapping::BusShift { bits: 12 },
            )
            .expect("compile GBF schema"),
        );
        let config =
            StreamDecoderConfig::new("gbf", serde_json::Map::new()).with_schema_artifact(compiled);
        let registry = flow::DecoderRegistry::new();
        register_gbf_decoder(&registry);

        registry
            .instantiate(&config, "can", schema)
            .expect("build CAN-over-GBF from DBC artifact");
    }

    #[test]
    fn programmatic_constructors_reject_bus_shift_with_bus_id_ref() {
        let dbc_path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(dbc_path.to_str().unwrap()).expect("load sim.json");
        let compiled = Arc::new(
            CompiledDbcSchema::new(dbc.clone(), "{sig_name}").expect("compile DBC schema"),
        );
        let schema = Arc::new(compiled.schema("can"));
        let mapping = CanIdMapping::BusShift { bits: 12 };

        let decoder_error = GbfDecoder::new(
            "can",
            Arc::clone(&schema),
            get_bus_id_ref_schema(),
            dbc.clone(),
            None,
            true,
            mapping,
        )
        .err()
        .expect("decoder must reject conflicting mapping");
        assert!(decoder_error.to_string().contains("must not be combined"));

        let merger_error = GbfFusedMerger::new(
            "can",
            Arc::clone(&schema),
            get_bus_id_ref_schema(),
            dbc,
            None,
            true,
            mapping,
        )
        .err()
        .expect("merger must reject conflicting mapping");
        assert!(merger_error.to_string().contains("must not be combined"));

        let compiled_error =
            CompiledGbfSchema::can(get_bus_id_ref_schema(), compiled, true, mapping)
                .err()
                .expect("compiled schema must reject conflicting mapping");
        assert!(compiled_error.to_string().contains("must not be combined"));
    }

    #[test]
    fn registry_builds_someip_decoder_from_schema_artifact() {
        crate::schema::arxml::register_arxml_schema();
        let arxml_path =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_data/baq.arxml");
        let mut schema_props = serde_json::Map::new();
        schema_props.insert(
            "schema_path".into(),
            serde_json::Value::String(arxml_path.to_string_lossy().into_owned()),
        );
        let (schema, _, artifact) = manager::schema_registry()
            .parse("arxml", "someip", &schema_props)
            .expect("parse ARXML schema");
        let arxml = artifact
            .expect("ARXML artifact")
            .downcast::<crate::schema::arxml::CompiledArxmlSchema>()
            .expect("ARXML artifact type");
        let gbf_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/test_data/someip_packet.json");
        let layout = GbfSchema::load(gbf_path.to_str().expect("GBF path")).expect("GBF layout");
        let compiled =
            Arc::new(CompiledGbfSchema::someip(layout, arxml).expect("compile GBF schema"));
        let config =
            StreamDecoderConfig::new("gbf", serde_json::Map::new()).with_schema_artifact(compiled);
        let registry = flow::DecoderRegistry::new();
        register_gbf_decoder(&registry);

        registry
            .instantiate(&config, "someip", Arc::new(schema))
            .expect("build SOME/IP-over-GBF from ARXML artifact");
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

    /// Under `raw` mapping, the wire CAN ID is the bare DBC `msg.id` (no bus
    /// prefix). Mess1 (id 1414 = 0x0586) decodes the same signals as the
    /// bus-prefixed `bus_shift` case (issue #217).
    #[test]
    fn test_gbf_decoder_raw_mapping_uses_bare_msg_id() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = Arc::new(schema_from_dbc("can", &dbc, None).expect("compile DBC schema"));
        let decoder = GbfDecoder::new(
            "can",
            schema,
            get_test_schema(),
            dbc,
            None,
            true,
            CanIdMapping::Raw,
        )
        .expect("build raw decoder");

        // Same frame/payload as `test_gbf_decoder_with_dbc` but the wire CAN ID
        // is the bare msg.id 0x0586 instead of the bus-prefixed 0x1586.
        let mut packet = Vec::new();
        packet.extend_from_slice(&1720765705290u64.to_be_bytes()); // ts
        packet.extend_from_slice(&12u16.to_be_bytes()); // total_len (one 12-byte frame)
        packet.push(0x55); // magic
        packet.extend_from_slice(&0x0586u16.to_be_bytes()); // can_id = bare msg.id
        packet.push(0x08); // data_len
        packet.extend_from_slice(&[0x08, 0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11]); // payload

        let batch = decoder.decode(&packet).expect("decode");
        let tuple = &batch.rows()[0];
        assert_eq!(
            tuple.value_by_name("can", "ts"),
            Some(&Value::Int64(1720765705290))
        );
        assert_eq!(
            tuple.value_by_name("can", "Mess1_Sig1"),
            Some(&Value::Int64(84))
        );
    }

    fn get_test_fused() -> GbfFusedMerger {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = Arc::new(schema_from_dbc("can", &dbc, None).expect("compile DBC schema"));
        let gbf_schema = get_test_schema();
        GbfFusedMerger::new(
            "can",
            schema,
            gbf_schema,
            dbc,
            None,
            true,
            CanIdMapping::BusShift { bits: 12 },
        )
        .expect("build fused")
    }

    /// The fused merge+decode path must produce the same decoded values as
    /// direct GBF decode for a non-multiplexed window.
    #[test]
    fn test_fused_matches_direct_decode_for_non_mux() {
        use flow::Merger;

        let hex_string = "00000190A5A0EC4A001855158688085465737400001155124A880854657374000011";
        let packet: Vec<u8> = (0..hex_string.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_string[i..i + 2], 16).unwrap())
            .collect();

        let direct = get_test_decoder().decode(&packet).expect("direct decode");

        // Fused path on the same raw packet.
        let mut fused = get_test_fused();
        fused.merge(&packet).expect("fused merge");
        let fused_batch = fused
            .decode_window(None)
            .expect("fused decode")
            .expect("fused batch");

        assert_eq!(direct.rows().len(), fused_batch.rows().len());
        let (direct_t, fused_t) = (&direct.rows()[0], &fused_batch.rows()[0]);
        for name in ["ts", "Mess0_Sig1", "Mess1_Sig1", "Mess1_Sig2"] {
            assert_eq!(
                direct_t.value_by_name("can", name),
                fused_t.value_by_name("can", name),
                "value mismatch for `{name}` between fused and direct decode paths"
            );
        }
    }

    #[test]
    fn test_fused_merge_splits_concatenated_packets() {
        use flow::Merger;

        fn push_frame(buf: &mut Vec<u8>, can_id: u16, payload: &[u8]) {
            buf.push(0x55);
            buf.extend_from_slice(&can_id.to_be_bytes());
            buf.push(payload.len() as u8);
            buf.extend_from_slice(payload);
        }

        fn packet(ts: u64, can_id: u16, payload: &[u8]) -> Vec<u8> {
            let mut frames = Vec::new();
            push_frame(&mut frames, can_id, payload);

            let mut packet = Vec::new();
            packet.extend_from_slice(&ts.to_be_bytes());
            packet.extend_from_slice(&(frames.len() as u16).to_be_bytes());
            packet.extend_from_slice(&frames);
            packet
        }

        let payload = [0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44];
        let mut concatenated = packet(1_000, 0x124A, &payload);
        concatenated.extend_from_slice(&packet(2_000, 0x1586, &payload));

        let mut fused = get_test_fused();
        fused
            .merge(&concatenated)
            .expect("merge concatenated packets");
        let batch = fused
            .decode_window(None)
            .expect("decode")
            .expect("fused batch");
        let row = &batch.rows()[0];

        assert_eq!(
            row.value_by_name("can", "ts"),
            Some(&Value::Int64(2_000)),
            "fused timestamp should use the newest packet in the payload"
        );
        assert_ne!(
            row.value_by_name("can", "Mess0_Sig1"),
            Some(&Value::Null),
            "first packet's frame must be merged"
        );
        assert_ne!(
            row.value_by_name("can", "Mess1_Sig1"),
            Some(&Value::Null),
            "second packet's frame must be merged"
        );
    }

    #[test]
    fn test_fused_preserves_mux_groups_by_composite_key() {
        use flow::Merger;

        fn push_frame(buf: &mut Vec<u8>, can_id: u16, payload: &[u8]) {
            buf.push(0x55);
            buf.extend_from_slice(&can_id.to_be_bytes());
            buf.push(payload.len() as u8);
            buf.extend_from_slice(payload);
        }

        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/mul.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load mul.json");
        let schema = Arc::new(schema_from_dbc("mul", &dbc, None).expect("compile DBC schema"));
        // mul.json is bus 0 -> raw and bus_shift keys coincide.
        let mut fused = GbfFusedMerger::new(
            "mul",
            schema,
            get_test_schema(),
            dbc,
            None,
            true,
            CanIdMapping::Raw,
        )
        .expect("build fused");

        let payload_g1 = [0x01u8, 0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11];
        let payload_g0 = [0x00u8, 0x24, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11];
        let mut frames = Vec::new();
        push_frame(&mut frames, 0x00C8, &payload_g1);
        push_frame(&mut frames, 0x00C8, &payload_g0);

        let mut packet = Vec::new();
        packet.extend_from_slice(&1_000u64.to_be_bytes());
        packet.extend_from_slice(&(frames.len() as u16).to_be_bytes());
        packet.extend_from_slice(&frames);

        fused.merge(&packet).expect("merge");
        let batch = fused
            .decode_window(None)
            .expect("decode")
            .expect("fused batch");
        let row = &batch.rows()[0];

        assert_eq!(
            row.value_by_name("mul", "SENSOR_SONARS_left"),
            Some(&Value::Float64(86.9)),
            "mux=0 group should survive fused accumulator"
        );
        assert_eq!(
            row.value_by_name("mul", "SENSOR_SONARS_no_filt_left"),
            Some(&Value::Float64(86.9)),
            "mux=1 group should survive fused accumulator"
        );
    }

    #[test]
    fn test_fused_drops_unknown_can_id_before_decode() {
        use flow::Merger;

        fn push_frame(buf: &mut Vec<u8>, can_id: u16, payload: &[u8]) {
            buf.push(0x55);
            buf.extend_from_slice(&can_id.to_be_bytes());
            buf.push(payload.len() as u8);
            buf.extend_from_slice(payload);
        }

        let mut frames = Vec::new();
        push_frame(&mut frames, 0x0FFF, &[0xAA, 0xBB, 0xCC]);

        let mut packet = Vec::new();
        packet.extend_from_slice(&1_000u64.to_be_bytes());
        packet.extend_from_slice(&(frames.len() as u16).to_be_bytes());
        packet.extend_from_slice(&frames);

        let mut fused = get_test_fused();
        fused.merge(&packet).expect("merge unknown");

        assert!(
            fused.decode_window(None).expect("decode").is_none(),
            "unknown CAN IDs should be discarded during merge"
        );
    }

    #[test]
    fn test_fused_accepts_u32_can_id_from_gbf_schema() {
        use flow::Merger;

        let dbc: crate::schema::dbc::DbcJson = serde_json::from_str(
            r#"
            {
                "buses": [
                    {
                        "name": "Bus0",
                        "id": 0,
                        "messages": [
                            {
                                "name": "WideMsg",
                                "id": 703710,
                                "frameId": "0xABCDE",
                                "length": 8,
                                "signals": [
                                    {
                                        "name": "WideSig",
                                        "start": 0,
                                        "length": 8,
                                        "scale": 1,
                                        "offset": 0,
                                        "isBigEndian": false,
                                        "isSigned": false
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            "#,
        )
        .expect("parse wide can dbc");
        let schema = Arc::new(schema_from_dbc("wide", &dbc, None).expect("compile DBC schema"));
        let mut fused = GbfFusedMerger::new(
            "wide",
            schema,
            get_u32_can_id_schema(),
            dbc,
            None,
            true,
            CanIdMapping::Raw,
        )
        .expect("build fused");

        let mut packet = Vec::new();
        packet.extend_from_slice(&1_000u64.to_be_bytes());
        packet.extend_from_slice(&14u16.to_be_bytes());
        packet.push(0x55);
        packet.extend_from_slice(&703710u32.to_be_bytes());
        packet.push(8);
        packet.extend_from_slice(&[42, 0, 0, 0, 0, 0, 0, 0]);

        fused.merge(&packet).expect("merge u32 can id");
        let batch = fused
            .decode_window(None)
            .expect("decode u32 can id")
            .expect("fused batch");
        assert_eq!(
            batch.rows()[0].value_by_name("wide", "WideSig"),
            Some(&Value::Int64(42))
        );
    }

    /// Extended CAN: a full 29-bit message id (0x1FFF_FFFF) decodes end-to-end
    /// through both the direct and fused paths, with no code change beyond DBC +
    /// GBF schema config (issue #202). Guards against any latent u16 narrowing.
    #[test]
    fn test_extended_29bit_can_id_decodes_both_paths() {
        use flow::Merger;

        // bus 0 + raw mapping -> the lookup key is the bare 29-bit msg.id.
        const EXT_ID: u32 = 0x1FFF_FFFF;
        let dbc: crate::schema::dbc::DbcJson = serde_json::from_str(&format!(
            r#"
            {{
                "buses": [{{
                    "name": "Bus0",
                    "id": 0,
                    "messages": [{{
                        "name": "ExtMsg",
                        "id": {EXT_ID},
                        "frameId": "0x1FFFFFFF",
                        "length": 8,
                        "signals": [{{
                            "name": "ExtSig",
                            "start": 0,
                            "length": 8,
                            "scale": 1,
                            "offset": 0,
                            "isBigEndian": false,
                            "isSigned": false
                        }}]
                    }}]
                }}]
            }}"#
        ))
        .expect("parse extended can dbc");
        let schema = Arc::new(schema_from_dbc("ext", &dbc, None).expect("compile DBC schema"));

        // Packet: ts(8) + total_len(2) + one frame [magic, u32be id, len, payload].
        let mut packet = Vec::new();
        packet.extend_from_slice(&1_000u64.to_be_bytes());
        packet.extend_from_slice(&14u16.to_be_bytes());
        packet.push(0x55);
        packet.extend_from_slice(&EXT_ID.to_be_bytes());
        packet.push(8);
        packet.extend_from_slice(&[77, 0, 0, 0, 0, 0, 0, 0]);

        // Direct (non-fused) decode.
        let decoder = GbfDecoder::new(
            "ext",
            schema.clone(),
            get_u32_can_id_schema(),
            dbc.clone(),
            None,
            true,
            CanIdMapping::Raw,
        )
        .expect("build decoder");
        let batch = decoder.decode(&packet).expect("direct decode");
        assert_eq!(
            batch.rows()[0].value_by_name("ext", "ExtSig"),
            Some(&Value::Int64(77)),
            "extended id must decode via the direct path"
        );

        // Fused decode.
        let mut fused = GbfFusedMerger::new(
            "ext",
            schema,
            get_u32_can_id_schema(),
            dbc,
            None,
            true,
            CanIdMapping::Raw,
        )
        .expect("build fused");
        fused.merge(&packet).expect("merge extended id");
        let fused_batch = fused
            .decode_window(None)
            .expect("fused decode")
            .expect("fused batch");
        assert_eq!(
            fused_batch.rows()[0].value_by_name("ext", "ExtSig"),
            Some(&Value::Int64(77)),
            "extended id must decode via the fused path"
        );
    }

    #[test]
    fn test_fused_skips_bad_mux_frame_without_dropping_good_frame() {
        use flow::Merger;

        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/mul.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load mul.json");
        let schema = Arc::new(schema_from_dbc("mul", &dbc, None).expect("compile DBC schema"));
        // mul.json is bus 0 -> raw and bus_shift keys coincide.
        let mut fused = GbfFusedMerger::new(
            "mul",
            schema,
            get_test_schema(),
            dbc,
            None,
            true,
            CanIdMapping::Raw,
        )
        .expect("build fused");

        fn push_frame(buf: &mut Vec<u8>, can_id: u16, payload: &[u8]) {
            buf.push(0x55);
            buf.extend_from_slice(&can_id.to_be_bytes());
            buf.push(payload.len() as u8);
            buf.extend_from_slice(payload);
        }

        let mut frames = Vec::new();
        push_frame(
            &mut frames,
            0x00C8,
            &[0x01u8, 0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11],
        );
        push_frame(&mut frames, 0x00C8, &[]);

        let mut packet = Vec::new();
        packet.extend_from_slice(&1_000u64.to_be_bytes());
        packet.extend_from_slice(&(frames.len() as u16).to_be_bytes());
        packet.extend_from_slice(&frames);

        fused.merge(&packet).expect("merge mixed mux packet");
        let batch = fused
            .decode_window(None)
            .expect("decode")
            .expect("fused batch");
        let row = &batch.rows()[0];
        assert_eq!(
            row.value_by_name("mul", "SENSOR_SONARS_no_filt_left"),
            Some(&Value::Float64(86.9)),
            "bad mux frame must not drop a good frame from the same packet"
        );
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
