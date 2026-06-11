use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;

use flow::planner::decode_projection::{DecodeProjection, FieldPath};
use veloflux_sdv::decoder::can::CanDecoder;
use veloflux_sdv::schema::dbc::{load_dbc_json, schema_from_dbc};

fn get_multiplex_decoder() -> CanDecoder {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/mul.json");
    let dbc = load_dbc_json(path.to_str().unwrap()).expect("load mul.json");
    let schema = Arc::new(schema_from_dbc("mul", &dbc, None));
    CanDecoder::new("mul", schema.clone(), dbc.clone(), None, true).expect("build decoder")
}

fn bench_decoder_projection(c: &mut Criterion) {
    let decoder = get_multiplex_decoder();

    // Create a CAN packet simulating multiplex group 1
    // Hex: 00000190a5a0ec4a00185500c888015465737400001155124a880854657374000011
    let hex = "00000190a5a0ec4a00185500c888015465737400001155124a880854657374000011";
    let packet: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();

    // Construct frames (GBF decoder logic normally does this splitting, here we do it manually to bench CanDecoder directly)
    // We actually need to bench `CanDecoder::decode_frames` but that needs CanFrame which has a lifetime bound to payload.
    // For simpler benching, let's just construct the input frames once.
    let timestamp = u64::from_be_bytes(packet[0..8].try_into().unwrap());
    let frame1_payload = &packet[14..22];

    // We need to re-create frames vector inside the bench loop because it's consumed by decode_frames?
    // Wait, decode_frames takes Vec<CanFrame>, so yes.

    // 1. Benchmark Full Decode (No Projection)
    c.bench_function("can_decode_full", |b| {
        b.iter(|| {
            let frames = vec![veloflux_sdv::decoder::can::CanFrame {
                timestamp,
                can_id: 0x00C8,
                payload: frame1_payload,
            }];
            decoder.decode_frames(frames, None)
        })
    });

    // 2. Benchmark With Projection (Subset of signals)
    let mut projection = DecodeProjection::default();
    projection.mark_field_path_used(&FieldPath {
        column: "SENSOR_SONARS_mux1".to_string(), // Only 1 signal out of many
        segments: vec![],
    });

    c.bench_function("can_decode_with_projection", |b| {
        b.iter(|| {
            let frames = vec![veloflux_sdv::decoder::can::CanFrame {
                timestamp,
                can_id: 0x00C8,
                payload: frame1_payload,
            }];
            decoder.decode_frames(frames, Some(&projection))
        })
    });
}

criterion_group!(benches, bench_decoder_projection);
criterion_main!(benches);
