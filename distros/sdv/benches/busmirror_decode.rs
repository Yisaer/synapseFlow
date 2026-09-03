use std::sync::Arc;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use veloflux_sdv::codec::busmirror_parser::{BusMirrorFrameSlot, parse_packet};
use veloflux_sdv::decoder::can::{CanDecoder, DbcFrame};
use veloflux_sdv::schema::dbc::{CompiledDbcSchema, load_can_schema};

fn destination_frame(timestamp_ms: u64, frame_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut body = vec![0, 0, 0x61, 1];
    body.extend_from_slice(&frame_id.to_be_bytes());
    body.push(payload.len() as u8);
    body.extend_from_slice(payload);

    let seconds = timestamp_ms / 1_000;
    let nanoseconds = ((timestamp_ms % 1_000) * 1_000_000) as u32;
    let mut packet = vec![1, 0];
    packet.extend_from_slice(&seconds.to_be_bytes()[2..]);
    packet.extend_from_slice(&nanoseconds.to_be_bytes());
    packet.extend_from_slice(&(body.len() as u16).to_be_bytes());
    packet.extend_from_slice(&body);
    packet
}

fn benchmark_busmirror_decode(criterion: &mut Criterion) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
    let mut dbc = load_can_schema(path.to_str().expect("fixture path")).expect("load DBC");
    dbc.buses[0].id = 0x101;
    dbc.buses[0].name = Some("Powertrain".to_string());
    let compiled =
        CompiledDbcSchema::new_busmirror(dbc, "{network_id}__{msg_id_hex_lower}__{sig_name}")
            .expect("compile BusMirror DBC");
    let schema = Arc::new(compiled.schema("mirror"));
    let decoder =
        CanDecoder::build_from_busmirror("mirror", schema, &compiled, true).expect("build decoder");
    let packet = destination_frame(1_720_765_705_290, 0x4000_0100, &[42, 1, 2, 3, 4, 5, 6, 7]);
    let mut slots: Vec<BusMirrorFrameSlot> = Vec::with_capacity(8);

    criterion.bench_function("busmirror_parse_identity_and_dbc_decode", |bencher| {
        bencher.iter(|| {
            let timestamp = parse_packet(black_box(&packet), &mut slots).expect("parse packet");
            let frames = slots
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
                .collect();
            black_box(decoder.decode_dbc_frames(frames, None))
        });
    });
}

criterion_group!(benches, benchmark_busmirror_decode);
criterion_main!(benches);
