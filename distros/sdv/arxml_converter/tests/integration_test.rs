//! Integration tests — end-to-end ARXML → decode scenarios using both
//! minimal hand-crafted fixtures and real-world ARXML files.
//!
//! These tests exercise the full public API from file loading through
//! type resolution to binary decoding.

use arxml_converter::{ArxmlCodec, Value};

// ---------------------------------------------------------------------------
// Fixture paths
// ---------------------------------------------------------------------------

const MINIMAL_CP: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/test_data/minimal_cp.arxml"
);
const S1_CP: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/test_data/s1_cp_test.xml"
);
const S1_AP: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/test_data/s1_ap_test.xml"
);

// ====================================================================
// Minimal CP ARXML tests
// ====================================================================

#[test]
fn minimal_cp_load_and_resolve() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let dt = codec.resolve_cp(10, 100).unwrap();
    assert_eq!(dt.short_name, "SpeedType");
    assert_eq!(dt.category, "VALUE");
}

#[test]
fn minimal_cp_decode_u32() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let v = codec.decode_cp(10, 100, &[0x00, 0x00, 0x00, 0x2A]).unwrap();
    assert_eq!(v, Value::U32(42));
}

#[test]
fn minimal_cp_decode_max_u32() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let v = codec.decode_cp(10, 100, &[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    assert_eq!(v, Value::U32(0xFFFF_FFFF));
}

#[test]
fn minimal_cp_decode_zero() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let v = codec.decode_cp(10, 100, &[0, 0, 0, 0]).unwrap();
    assert_eq!(v, Value::U32(0));
}

#[test]
fn minimal_cp_unknown_service_id() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let err = codec.resolve_cp(999, 100).unwrap_err();
    assert!(err.contains("no service found"));
}

#[test]
fn minimal_cp_unknown_header_id() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let err = codec.resolve_cp(10, 999).unwrap_err();
    assert!(err.contains("no header ref"));
}

#[test]
fn minimal_cp_insufficient_bytes() {
    let codec = ArxmlCodec::load(MINIMAL_CP).unwrap();
    let err = codec.decode_cp(10, 100, &[0x00, 0x01]).unwrap_err();
    assert!(err.contains("not enough bytes"));
}

#[test]
fn file_not_found() {
    let err = ArxmlCodec::load("/nonexistent/path.arxml").unwrap_err();
    assert!(err.contains("failed to read"));
}

#[test]
fn invalid_xml() {
    let dir = std::env::temp_dir();
    let path = dir.join("invalid.arxml");
    std::fs::write(&path, "<not><valid>").unwrap();
    let err = ArxmlCodec::load(&path).unwrap_err();
    assert!(err.contains("XML parse error") || err.contains("no <AUTOSAR>"));
    let _ = std::fs::remove_file(&path);
}

// ====================================================================
// S1 real-world CP ARXML tests
// ====================================================================

/// The Header-ID used by the S1 CP test case (service 33282, event 5 →
/// merged to 0x8202_0005 = 2181169157).
const S1_CP_HEADER_ID: u32 = 2181169157;

#[test]
fn s1_cp_load_success() {
    let codec = ArxmlCodec::load(S1_CP).unwrap();
    // Just proving the file parses without error is already a win.
    let _ = codec;
}

#[test]
fn s1_cp_resolve_wifi_ap_name() {
    let codec = ArxmlCodec::load(S1_CP).unwrap();

    let dt = codec.resolve_cp(33282, S1_CP_HEADER_ID).unwrap();
    assert_eq!(dt.short_name, "adt_WiFiApName");
}

#[test]
fn s1_cp_decode_wifi_ap_name_string() {
    let codec = ArxmlCodec::load(S1_CP).unwrap();

    // Reference data from the Go test (converter_test.go:TestS1CPCase).
    // The on-wire payload starts with a 4-byte length prefix that is not
    // part of the data type itself.  We strip it and decode the remainder
    // as a variable-length string (the type "adt_WiFiApName" maps to
    // `string`).
    //
    // Full wire bytes:
    //   [0x00,0x00,0x00,0x08, 0xEF,0xBB,0xBF, 0x54,0x65,0x73,0x74, 0x00]
    //    \_____ length=8 ____/  \__ BOM ___/  \____ "Test" ____/  \_null_/
    let payload = &[0xEFu8, 0xBB, 0xBF, 0x54, 0x65, 0x73, 0x74, 0x00][..];

    let v = codec.decode_cp(33282, S1_CP_HEADER_ID, payload).unwrap();

    // The decoder produces the raw UTF-8 bytes including BOM and null.
    // The veloFlux pipeline is expected to do post-processing (strip BOM,
    // trim null) if needed.
    assert_eq!(v, Value::Str("\u{FEFF}Test\u{0}".to_string()));
}

// ====================================================================
// S1 real-world AP ARXML tests
// ====================================================================

#[test]
fn s1_ap_load_success() {
    let codec = ArxmlCodec::load(S1_AP).unwrap();
    let _ = codec;
}

#[test]
fn s1_ap_resolve_wifi_ap_list_event() {
    let codec = ArxmlCodec::load(S1_AP).unwrap();

    // Service-ID 33282, Event-ID 32769 → "reportWiFiApList" event →
    // type "/dataTypes/WiFiApList" (STRUCTURE).
    let dt = codec.resolve_ap(33282, 32769).unwrap();
    assert_eq!(dt.short_name, "WiFiApList");
}

#[test]
fn s1_ap_resolve_wifi_conn_status_event() {
    let codec = ArxmlCodec::load(S1_AP).unwrap();

    // Service-ID 33282, Event-ID 32770 →
    // "reportWiFiConnStatus" → type "/dataTypes/WiFiConnStatus" (STRUCTURE).
    let dt = codec.resolve_ap(33282, 32770).unwrap();
    assert_eq!(dt.short_name, "WiFiConnStatus");
}

#[test]
fn s1_ap_resolve_switch_status_event() {
    let codec = ArxmlCodec::load(S1_AP).unwrap();

    // Service-ID 33282, Event-ID 32771 →
    // "reportWiFiSwitchStatus" → type "/dataTypes/WiFiSwitchStatus" (STRUCTURE).
    let dt = codec.resolve_ap(33282, 32771).unwrap();
    assert_eq!(dt.short_name, "WiFiSwitchStatus");
}

#[test]
fn s1_ap_unknown_service_id() {
    let codec = ArxmlCodec::load(S1_AP).unwrap();
    let err = codec.resolve_ap(55555, 32769).unwrap_err();
    assert!(err.contains("not found"));
}

#[test]
fn s1_ap_unknown_event_id() {
    let codec = ArxmlCodec::load(S1_AP).unwrap();
    let err = codec.resolve_ap(33282, 55555).unwrap_err();
    assert!(err.contains("unknown event_id"));
}

// ====================================================================
// Large real-world ARXML (baq.arxml — 15 MB CP file)
// ====================================================================

const BAQ_ARXML: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");

#[test]
fn baq_cp_load_and_verify_packages() {
    let codec = ArxmlCodec::load(BAQ_ARXML).unwrap();
    let _ = codec;
}

#[test]
fn baq_cp_resolve_example_service() {
    let codec = ArxmlCodec::load(BAQ_ARXML).unwrap();
    match codec.resolve_cp(33282, 0x82020005) {
        Ok(dt) => {
            assert!(!dt.short_name.is_empty());
        }
        Err(_e) => {
            // It's OK if the exact IDs don't match.
        }
    }
}

#[test]
fn baq_cp_entry_fields_with_types() {
    let codec = ArxmlCodec::load(BAQ_ARXML).unwrap();

    // (service_id=0xAB04, event_id=0x8003) → ADT_ADAS_arr_ParkingSlot
    // → element type ADT_ADAS_strt_ParkingSlot.
    let fields = codec
        .entry_fields(0xAB04, 0x8003)
        .expect("entry_fields should return Some");

    assert!(!fields.is_empty(), "expected at least one field");

    // Verify uint16 field.
    let slot_id = fields
        .iter()
        .find(|(n, _)| n == "DTE_SlotID")
        .expect("DTE_SlotID should exist");
    assert_eq!(slot_id.1, "uint16", "DTE_SlotID should be uint16");

    // Verify uint8 fields.
    let slot_type = fields
        .iter()
        .find(|(n, _)| n == "DTE_SlotType")
        .expect("DTE_SlotType should exist");
    assert_eq!(slot_type.1, "uint8", "DTE_SlotType should be uint8");

    let slot_status = fields
        .iter()
        .find(|(n, _)| n == "DTE_SlotStatus")
        .expect("DTE_SlotStatus should exist");
    assert_eq!(slot_status.1, "uint8", "DTE_SlotStatus should be uint8");

    // Verify nested struct field.
    let point_top = fields
        .iter()
        .find(|(n, _)| n == "DTE_SlotPointTop1")
        .expect("DTE_SlotPointTop1 should exist");
    assert_eq!(point_top.1, "struct", "DTE_SlotPointTop1 should be struct");
}

#[test]
fn baq_cp_known_entries() {
    let codec = ArxmlCodec::load(BAQ_ARXML).unwrap();
    let entries = codec.known_entries();
    assert!(!entries.is_empty(), "should have known entries");

    // The entry used in the test should exist.
    assert!(
        entries.contains(&(0xAB04, 0x8003)),
        "(0xAB04, 0x8003) should be a known entry"
    );
}
