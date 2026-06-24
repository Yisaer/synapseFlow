//! Integration test for the fused GBF merger as wired through the production
//! `MergerRegistry` path (the same path `SamplerProcessor`'s Packer strategy
//! uses). Verifies that when the merger config carries the CAN format schema,
//! the registry builds a decode-capable merger that emits a decoded collection
//! directly (skipping the GBF re-encode + re-parse round-trip).

use std::sync::Arc;

use flow::FlowInstance;
use flow::instance::{FlowInstanceDedicatedRuntimeOptions, FlowInstanceOptions};
use serde_json::{Map, Value};
use veloflux_sdv::schema::dbc::{load_dbc_json, schema_from_dbc};

fn fixture(rel: &str) -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel)
}

/// Build a single-frame GBF packet (matches src/tests/spi_packet.json layout).
fn gbf_packet(can_id: u16, payload: &[u8]) -> Vec<u8> {
    let frame_size = 1 + 2 + 1 + payload.len();
    let mut data = Vec::with_capacity(10 + frame_size);
    data.extend_from_slice(&1_000_000_000u64.to_be_bytes()); // ts
    data.extend_from_slice(&(frame_size as u16).to_be_bytes()); // total_len
    data.push(0x55); // magic
    data.extend_from_slice(&can_id.to_be_bytes());
    data.push(payload.len() as u8);
    data.extend_from_slice(payload);
    data
}

#[test]
fn registry_builds_fused_gbf_merger_and_decodes() {
    let instance = FlowInstance::new(FlowInstanceOptions::dedicated_runtime(
        "default",
        None,
        FlowInstanceDedicatedRuntimeOptions::default(),
    ))
    .expect("create flow instance");
    veloflux_sdv::register(&instance);

    // Schema the framework would hand the merger (derived from the DBC).
    let dbc = load_dbc_json(&fixture("src/tests/sim.json")).expect("load dbc json");
    let schema = Arc::new(schema_from_dbc("can", &dbc, None));

    // Merger props now carry the CAN format schema, which activates fusion.
    let mut props = Map::new();
    props.insert(
        "schema".to_string(),
        Value::String(fixture("src/tests/spi_packet.json")),
    );
    props.insert(
        "format_schema_path".to_string(),
        Value::String(fixture("src/tests/sim.json")),
    );
    props.insert("format_type".to_string(), Value::String("can".to_string()));

    let mut merger = instance
        .merger_registry()
        .instantiate("gbf", &props, Arc::clone(&schema))
        .expect("instantiate fused gbf merger");

    assert!(
        merger.supports_fused_decode(),
        "merger with format props must support fused decode"
    );

    // Empty window decodes to nothing.
    assert!(
        merger
            .trigger_decoded(None)
            .expect("trigger empty")
            .is_none()
    );

    // Accumulate a window with both DBC messages and decode. CAN ids are
    // bus-prefixed: (bus_id << 12) | msg_id, bus_id=1 -> Mess0=0x124A (586),
    // Mess1=0x1586 (1414).
    merger
        .merge(&gbf_packet(
            0x124A,
            &[0x01, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77],
        ))
        .expect("merge Mess0");
    merger
        .merge(&gbf_packet(
            0x1586,
            &[0xAA, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77],
        ))
        .expect("merge Mess1");

    let collection = merger
        .trigger_decoded(None)
        .expect("trigger decode")
        .expect("decoded collection");

    assert_eq!(collection.num_rows(), 1, "one packed row per window");
    let row = &collection.rows()[0];
    // Both messages actually decoded into the single row (not just present as
    // null schema columns) — assert non-null values.
    assert_ne!(
        row.value_by_name("can", "Mess0_Sig1"),
        Some(&datatypes::Value::Null),
        "Mess0 signal should be decoded to a non-null value"
    );
    assert_ne!(
        row.value_by_name("can", "Mess1_Sig1"),
        Some(&datatypes::Value::Null),
        "Mess1 signal should be decoded to a non-null value"
    );

    // Buffer reset after trigger: next empty window yields nothing.
    assert!(
        merger
            .trigger_decoded(None)
            .expect("trigger empty")
            .is_none()
    );
}

#[test]
fn registry_rejects_gbf_merger_without_format_schema_path() {
    let instance = FlowInstance::new(FlowInstanceOptions::dedicated_runtime(
        "default",
        None,
        FlowInstanceDedicatedRuntimeOptions::default(),
    ))
    .expect("create flow instance");
    veloflux_sdv::register(&instance);

    let dbc = load_dbc_json(&fixture("src/tests/sim.json")).expect("load dbc json");
    let schema = Arc::new(schema_from_dbc("can", &dbc, None));

    let mut props = Map::new();
    props.insert(
        "schema".to_string(),
        Value::String(fixture("src/tests/spi_packet.json")),
    );

    let err = match instance
        .merger_registry()
        .instantiate("gbf", &props, schema)
    {
        Ok(_) => panic!("gbf merger must require format schema"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("format_schema_path"),
        "unexpected error: {err}"
    );
}
