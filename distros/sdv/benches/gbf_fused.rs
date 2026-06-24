//! Benchmarks the GBF fused merger used by the Packer sampler.
//!
//! The fused merger parses GBF outer packets, accumulates useful inner CAN
//! frames by semantic key, and decodes the retained frames directly.

use std::path::PathBuf;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use flow::Merger;
use flow::codec::RecordDecoder;
use veloflux_sdv::decoder::{GbfDecoder, GbfFusedMerger};
use veloflux_sdv::schema::dbc::{DbcJson, load_can_schema, load_dbc_json, schema_from_dbc};
use veloflux_sdv::schema::gbf::GbfSchema;

/// GBF schema whose payload carries a DBC-formatted CAN frame keyed by can_id.
fn gbf_schema_json() -> &'static str {
    r#"
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
    "#
}

/// Build a single-frame GBF packet for the given can_id (8-byte payload).
fn make_packet(can_id: u32, seed: u8) -> Vec<u8> {
    let encoded_can_id = u16::try_from(can_id).expect("bench GBF schema encodes can_id as u16be");
    let payload_len: u8 = 8;
    let frame_size = 1 + 2 + 1 + payload_len as usize;
    let mut data = Vec::with_capacity(10 + frame_size);
    data.extend_from_slice(&0x3B9ACA00u64.to_be_bytes()); // ts
    data.extend_from_slice(&(frame_size as u16).to_be_bytes()); // total_len
    data.push(0x55); // magic
    data.extend_from_slice(&encoded_can_id.to_be_bytes());
    data.push(payload_len);
    data.extend_from_slice(&[seed, 0x11, 0x22, 0x33, seed ^ 0xFF, 0x55, 0x66, 0x77]);
    data
}

/// One sampling window: `num_packets` single-frame packets, alternating between
/// the two bus-prefixed DBC CAN ids defined in sim.json.
fn make_window(num_packets: usize) -> Vec<Vec<u8>> {
    let ids = [0x124Au32, 0x1586u32];
    (0..num_packets)
        .map(|i| make_packet(ids[i % ids.len()], i as u8))
        .collect()
}

fn dbc_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json")
}

fn build_fused() -> GbfFusedMerger {
    let dbc = load_dbc_json(dbc_path().to_str().unwrap()).expect("load sim.json");
    let schema = Arc::new(schema_from_dbc("can", &dbc, None));
    let gbf_schema: GbfSchema = serde_json::from_str(gbf_schema_json()).expect("gbf schema");
    GbfFusedMerger::new("can", schema, gbf_schema, dbc, None, true).expect("fused")
}

fn bench_paths(c: &mut Criterion) {
    // Sanity: fused decode must emit one packed row for a non-empty window.
    {
        let window = make_window(4);
        let mut fused = build_fused();
        for p in &window {
            fused.merge(p).unwrap();
        }
        let fu_batch = fused
            .decode_window(None)
            .expect("fused decode")
            .expect("fused batch");
        assert_eq!(fu_batch.rows().len(), 1, "one packed row per window");
    }

    let mut group = c.benchmark_group("gbf_sample_window");
    for &num_packets in &[10usize, 50, 200] {
        let window = make_window(num_packets);

        group.bench_with_input(
            BenchmarkId::new("fused", num_packets),
            &window,
            |b, window| {
                let mut fused = build_fused();
                b.iter(|| {
                    for p in window {
                        fused.merge(black_box(p)).unwrap();
                    }
                    let batch = fused.decode_window(None).unwrap();
                    black_box(batch);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Representative multi-message workload: 1_TestBus.dbc has 5 distinct CAN IDs,
// so windows have low per-ID repetition — closer to a real "all signals" replay
// across a full DBC than the 2-ID sim.json case.
// ---------------------------------------------------------------------------

fn testbus_dbc() -> DbcJson {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
    load_can_schema(path.to_str().unwrap()).expect("load 1_TestBus.dbc")
}

fn build_decoder_with(dbc: DbcJson) -> GbfDecoder {
    let schema = Arc::new(schema_from_dbc("can", &dbc, None));
    let gbf_schema: GbfSchema = serde_json::from_str(gbf_schema_json()).expect("gbf schema");
    GbfDecoder::new("can", schema, gbf_schema, dbc, None, true).expect("decoder")
}

fn build_fused_with(dbc: DbcJson) -> GbfFusedMerger {
    let schema = Arc::new(schema_from_dbc("can", &dbc, None));
    let gbf_schema: GbfSchema = serde_json::from_str(gbf_schema_json()).expect("gbf schema");
    GbfFusedMerger::new("can", schema, gbf_schema, dbc, None, true).expect("fused")
}

/// Window cycling all 5 TestBus message ids.
fn make_window_testbus(num_packets: usize) -> Vec<Vec<u8>> {
    let ids = [256u32, 512, 768, 1024, 1280];
    (0..num_packets)
        .map(|i| make_packet(ids[i % ids.len()], i as u8))
        .collect()
}

fn bench_multimsg(c: &mut Criterion) {
    let mut group = c.benchmark_group("gbf_sample_window_5msg");
    // 10 packets / 5 ids = 2 repeats per ID; 25 = 5 repeats; 50 = 10 repeats.
    for &num_packets in &[10usize, 25, 50] {
        let window = make_window_testbus(num_packets);

        group.bench_with_input(
            BenchmarkId::new("fused", num_packets),
            &window,
            |b, window| {
                let mut fused = build_fused_with(testbus_dbc());
                b.iter(|| {
                    for p in window {
                        fused.merge(black_box(p)).unwrap();
                    }
                    black_box(fused.decode_window(None).unwrap());
                });
            },
        );
    }
    group.finish();
}

fn mux_dbc() -> DbcJson {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/mul.json");
    load_can_schema(path.to_str().unwrap()).expect("load mul.json")
}

fn make_mux_packet(mux_value: u8, seed: u8) -> Vec<u8> {
    let payload_len: u8 = 8;
    let frame_size = 1 + 2 + 1 + payload_len as usize;
    let mut data = Vec::with_capacity(10 + frame_size);
    data.extend_from_slice(&0x3B9ACA00u64.to_be_bytes());
    data.extend_from_slice(&(frame_size as u16).to_be_bytes());
    data.push(0x55);
    data.extend_from_slice(&0x00C8u16.to_be_bytes());
    data.push(payload_len);
    data.extend_from_slice(&[mux_value, seed, 0x22, 0x33, seed ^ 0xFF, 0x55, 0x66, 0x77]);
    data
}

fn make_mux_window(num_packets: usize) -> Vec<Vec<u8>> {
    (0..num_packets)
        .map(|i| make_mux_packet((i % 4) as u8, i as u8))
        .collect()
}

fn make_unknown_window(num_packets: usize) -> Vec<Vec<u8>> {
    (0..num_packets)
        .map(|i| make_packet(0x0FFF, i as u8))
        .collect()
}

fn bench_fused_accumulator(c: &mut Criterion) {
    let mut group = c.benchmark_group("gbf_fused_accumulator");
    for &num_packets in &[10usize, 100, 1000] {
        let non_mux = make_window(num_packets);
        group.bench_with_input(
            BenchmarkId::new("non_mux_known", num_packets),
            &non_mux,
            |b, window| {
                let mut fused = build_fused();
                b.iter(|| {
                    for p in window {
                        fused.merge(black_box(p)).unwrap();
                    }
                    black_box(fused.decode_window(None).unwrap());
                });
            },
        );

        let mux = make_mux_window(num_packets);
        group.bench_with_input(
            BenchmarkId::new("mux_known", num_packets),
            &mux,
            |b, window| {
                let mut fused = build_fused_dbc(mux_dbc());
                b.iter(|| {
                    for p in window {
                        fused.merge(black_box(p)).unwrap();
                    }
                    black_box(fused.decode_window(None).unwrap());
                });
            },
        );

        let unknown = make_unknown_window(num_packets);
        group.bench_with_input(
            BenchmarkId::new("unknown_can_id", num_packets),
            &unknown,
            |b, window| {
                let mut fused = build_fused();
                b.iter(|| {
                    for p in window {
                        fused.merge(black_box(p)).unwrap();
                    }
                    black_box(fused.decode_window(None).unwrap());
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// High-cardinality workload: a synthetic DBC with many distinct messages, so
// the per-frame `CanDecoder.messages` lookup (HashMap<u32, _>) is exercised at
// production-like cardinality. Reproduces the ~12% `hash_one` seen in the
// box.home pprof, to evaluate whether a faster CAN-id hasher is worth it.
// ---------------------------------------------------------------------------

/// Synthesize a DBC JSON with `num_msgs` messages (bus 0, ids 1..=num_msgs),
/// each carrying `sigs_per_msg` 10-bit signals.
fn synth_dbc(num_msgs: u32, sigs_per_msg: u32) -> DbcJson {
    let mut msgs = Vec::with_capacity(num_msgs as usize);
    for id in 1..=num_msgs {
        let sigs: Vec<String> = (0..sigs_per_msg)
            .map(|s| {
                let start = 1 + s * 16; // 1,17,33,49 -> fits 64-bit payload
                format!(
                    r#"{{"name":"M{id}_S{s}","start":{start},"length":10,"isBigEndian":true,"isSigned":false,"scale":1,"offset":0,"min":0,"max":1023}}"#
                )
            })
            .collect();
        msgs.push(format!(
            r#"{{"name":"M{id}","id":{id},"frameId":"0x{id:X}","length":8,"signals":[{}]}}"#,
            sigs.join(",")
        ));
    }
    let json = format!(
        r#"{{"version":"v1","source_file":"synth","buses":[{{"name":"Bus0","id":0,"messages":[{}]}}]}}"#,
        msgs.join(",")
    );
    serde_json::from_str(&json).expect("parse synthetic dbc")
}

fn build_fused_dbc(dbc: DbcJson) -> GbfFusedMerger {
    let schema = Arc::new(schema_from_dbc("can", &dbc, None));
    let gbf_schema: GbfSchema = serde_json::from_str(gbf_schema_json()).expect("gbf schema");
    GbfFusedMerger::new("can", schema, gbf_schema, dbc, None, true).expect("fused")
}

fn bench_highcard(c: &mut Criterion) {
    let mut group = c.benchmark_group("gbf_highcard_decode");
    // ~300 distinct messages (≈ a real propulsion+body DBC slice). One frame per
    // id per window -> num_msgs per-frame `messages.get` lookups, low repetition.
    for &num_msgs in &[100u32, 300] {
        let window: Vec<Vec<u8>> = (1..=num_msgs).map(|id| make_packet(id, id as u8)).collect();
        group.bench_with_input(BenchmarkId::new("fused", num_msgs), &window, |b, window| {
            let mut fused = build_fused_dbc(synth_dbc(num_msgs, 4));
            b.iter(|| {
                for p in window {
                    fused.merge(black_box(p)).unwrap();
                }
                black_box(fused.decode_window(None).unwrap());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Case-family pattern: the event-detection rules (case1..case6) read `spiStream`
// directly via the raw per-frame decode path (`GbfDecoder::decode_with_projection`),
// NOT the sampler/Packer fusion. Each SPI packet carries one frame per active
// can_id; the rule references only a handful of signals, so a NARROW projection
// (whitelist) is pushed down. This reproduces the box.home `allcases` profile,
// where the cost is per-frame overhead (parse-copy + can_id hash + remap), not
// signal decoding — see issue emqx/VeloFlux#56 (GBF filter signals whitelist).
// ---------------------------------------------------------------------------

/// Build one multi-frame GBF packet carrying a single 8-byte frame per id.
fn make_multiframe_packet(ids: &[u32]) -> Vec<u8> {
    let payload_len: u8 = 8;
    let frame_size = 1 + 2 + 1 + payload_len as usize;
    let total_len = frame_size * ids.len();
    let mut data = Vec::with_capacity(10 + total_len);
    data.extend_from_slice(&0x3B9ACA00u64.to_be_bytes()); // ts
    data.extend_from_slice(&(total_len as u16).to_be_bytes()); // total_len
    for (i, &can_id) in ids.iter().enumerate() {
        let seed = i as u8;
        data.push(0x55); // magic
        let encoded_can_id =
            u16::try_from(can_id).expect("bench GBF schema encodes can_id as u16be");
        data.extend_from_slice(&encoded_can_id.to_be_bytes());
        data.push(payload_len);
        data.extend_from_slice(&[seed, 0x11, 0x22, 0x33, seed ^ 0xFF, 0x55, 0x66, 0x77]);
    }
    data
}

fn bench_case_raw_decode(c: &mut Criterion) {
    use flow::planner::decode_projection::DecodeProjection;

    let num_msgs = 250u32;
    // One SPI packet carrying one frame per message id (250 frames/packet).
    let ids: Vec<u32> = (1..=num_msgs).collect();
    let packet = make_multiframe_packet(&ids);
    let decoder = build_decoder_with(synth_dbc(num_msgs, 4));

    // Narrow whitelist: 3 signals from 3 messages — the case-rule shape (case1
    // references 2 signals, case6 ~36). Compares against None (decode all).
    let narrow = DecodeProjection::from_top_level_columns_with_version(
        &[
            "M1_S0".to_string(),
            "M2_S0".to_string(),
            "M3_S0".to_string(),
        ],
        1,
    );

    let mut group = c.benchmark_group("case_raw_decode");
    group.bench_function("projection_none", |b| {
        b.iter(|| black_box(decoder.decode(black_box(&packet)).unwrap()));
    });
    group.bench_function("projection_narrow", |b| {
        b.iter(|| {
            black_box(
                decoder
                    .decode_with_projection(black_box(&packet), Some(&narrow))
                    .unwrap(),
            )
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_paths,
    bench_multimsg,
    bench_fused_accumulator,
    bench_highcard,
    bench_case_raw_decode
);
criterion_main!(benches);
