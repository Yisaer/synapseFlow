//! Integration test for GBF Packer strategy.
//!
//! Same as gbf_test but with sampler/merger enabled.
//! Verifies:
//! - Data accumulation and packing
//! - Override behavior (same CAN IDs get latest value)
//! - Signal decoding correctness
//! - Pipeline statistics (input/output counts)

mod common;

use common::{ApiClient, MQTT_PORT, TestEnvironment, get_server, write_schema_zip};
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

#[derive(Clone, Copy)]
enum CanIdentityLayout {
    BusShift,
    BusIdRef,
    CustomerEnvelope,
}

/// Get the absolute path to the JSON test schema file.
fn json_schema_path() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/sim.json")
        .to_str()
        .unwrap()
        .to_string()
}

fn gbf_schema_path(layout: CanIdentityLayout) -> String {
    static BUS_SHIFT_PATH: OnceLock<String> = OnceLock::new();
    static BUS_ID_REF_PATH: OnceLock<String> = OnceLock::new();
    static CUSTOMER_ENVELOPE_PATH: OnceLock<String> = OnceLock::new();
    let path = match layout {
        CanIdentityLayout::BusShift => &BUS_SHIFT_PATH,
        CanIdentityLayout::BusIdRef => &BUS_ID_REF_PATH,
        CanIdentityLayout::CustomerEnvelope => &CUSTOMER_ENVELOPE_PATH,
    };
    path.get_or_init(|| {
        let mut document: Value = match layout {
            CanIdentityLayout::CustomerEnvelope => json!({
                "signal_name_pattern": "b{bus_id}_{msg_id_hex_lower}_{sig_name}",
                "structure": {
                    "type": "struct",
                    "fields": [
                        { "name": "package_id", "type": "u32be" },
                        { "name": "ts", "type": "u64be" },
                        { "name": "payload_length", "type": "u32be" },
                        {
                            "name": "frames",
                            "type": "sequence",
                            "length_ref": "payload_length",
                            "length_unit": "bytes",
                            "structure": {
                                "type": "struct",
                                "fields": [
                                    { "name": "frame_type", "type": "u8", "const": 0 },
                                    { "name": "frame_time", "type": "u64be" },
                                    { "name": "frame_channel", "type": "u8" },
                                    { "name": "frame_extend", "type": "u8" },
                                    { "name": "frame_id", "type": "u32be" },
                                    { "name": "frame_dir", "type": "u8" },
                                    { "name": "frame_length", "type": "u32be" },
                                    {
                                        "name": "frame_data",
                                        "type": "bytes",
                                        "length_ref": "frame_length",
                                        "format": {
                                            "id_ref": "frame_id",
                                            "bus_id_ref": "frame_channel"
                                        }
                                    }
                                ]
                            }
                        },
                        { "name": "crc", "type": "u16be" }
                    ]
                },
                "format": {
                    "type": "can",
                    "props": { "dbc_path": "format/sim.json" }
                }
            }),
            CanIdentityLayout::BusShift | CanIdentityLayout::BusIdRef => {
                let source_layout =
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/spi_packet.json");
                serde_json::from_slice(
                    &std::fs::read(source_layout).expect("read GBF packet layout"),
                )
                .expect("parse GBF packet layout")
            }
        };
        if !matches!(layout, CanIdentityLayout::CustomerEnvelope) {
            document["format"] = match layout {
                CanIdentityLayout::BusShift => json!({
                    "type": "can",
                    "props": {
                        "dbc_path": "format/sim.json",
                        "can_id_mapping": { "mode": "bus_shift", "bits": 12 }
                    }
                }),
                CanIdentityLayout::BusIdRef => {
                    let fields = document
                        .pointer_mut("/structure/fields/2/structure/fields")
                        .and_then(Value::as_array_mut)
                        .expect("GBF frame fields");
                    fields.insert(1, json!({ "name": "bus_id", "type": "u8" }));
                    fields[2]["type"] = json!("u32be");
                    fields[4]["format"]["bus_id_ref"] = json!("bus_id");
                    json!({
                        "type": "can",
                        "props": { "dbc_path": "format/sim.json" }
                    })
                }
                CanIdentityLayout::CustomerEnvelope => unreachable!(),
            };
        }
        let entry = serde_json::to_vec_pretty(&document).expect("encode GBF schema");
        let dbc = std::fs::read(json_schema_path()).expect("read private DBC source");
        let suffix = match layout {
            CanIdentityLayout::BusShift => "bus-shift",
            CanIdentityLayout::BusIdRef => "bus-id-ref",
            CanIdentityLayout::CustomerEnvelope => "customer-envelope",
        };
        let archive = std::env::temp_dir().join(format!(
            "veloflux-gbf-packer-{suffix}-{}.zip",
            std::process::id()
        ));
        write_schema_zip(
            &archive,
            &[
                ("spi_packet.json", &entry),
                ("spi_packet/format/sim.json", &dbc),
            ],
        );
        archive.to_string_lossy().into_owned()
    })
    .clone()
}

/// Build a GBF packet with given timestamp and frames.
/// Frame format: (can_id, payload_bytes)
fn build_gbf_packet(layout: CanIdentityLayout, timestamp: u64, frames: &[(u16, &[u8])]) -> Vec<u8> {
    if matches!(layout, CanIdentityLayout::CustomerEnvelope) {
        return build_customer_packet(timestamp, frames);
    }

    let mut packet = Vec::new();
    // Timestamp (8 bytes, big-endian)
    packet.extend_from_slice(&timestamp.to_be_bytes());

    // Calculate total frames length
    let frame_header_len = match layout {
        CanIdentityLayout::BusShift => 4,
        CanIdentityLayout::BusIdRef => 7,
        CanIdentityLayout::CustomerEnvelope => 20,
    };
    let frames_len: usize = frames
        .iter()
        .map(|(_, payload)| frame_header_len + payload.len())
        .sum();
    packet.extend_from_slice(&(frames_len as u16).to_be_bytes());

    // Add frames
    for (can_id, payload) in frames {
        packet.push(0x55); // magic
        match layout {
            CanIdentityLayout::BusShift => packet.extend_from_slice(&can_id.to_be_bytes()),
            CanIdentityLayout::BusIdRef => {
                packet.push((can_id >> 12) as u8);
                packet.extend_from_slice(&u32::from(can_id & 0x0fff).to_be_bytes());
            }
            CanIdentityLayout::CustomerEnvelope => unreachable!(),
        }
        packet.push(0x80 | payload.len() as u8); // length with high bit set (per spi_packet.json)
        packet.extend_from_slice(payload);
    }

    packet
}

fn build_customer_packet(timestamp: u64, frames: &[(u16, &[u8])]) -> Vec<u8> {
    const FRAME_HEADER_LEN: usize = 20;

    let mut packet = Vec::new();
    packet.extend_from_slice(&(timestamp as u32).to_be_bytes()); // package_id
    packet.extend_from_slice(&timestamp.to_be_bytes()); // collect_time, Unix milliseconds
    let payload_length: usize = frames
        .iter()
        .map(|(_, payload)| FRAME_HEADER_LEN + payload.len())
        .sum();
    packet.extend_from_slice(&(payload_length as u32).to_be_bytes());

    for (packed_can_id, payload) in frames {
        let frame_channel = (packed_can_id >> 12) as u8;
        let frame_id = u32::from(packed_can_id & 0x0fff);
        packet.push(0); // frame_type: CAN
        packet.extend_from_slice(&5_678_u64.to_be_bytes()); // frame_time in microseconds
        packet.push(frame_channel);
        packet.push(u8::from(frame_id > 0x7ff)); // frame_extend
        packet.extend_from_slice(&frame_id.to_be_bytes());
        packet.push(1); // frame_dir: Rx
        packet.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        packet.extend_from_slice(payload);
    }

    packet.extend_from_slice(&0xabcd_u16.to_be_bytes()); // crc (not validated)
    packet
}

fn run_gbf_packer_integration(layout: CanIdentityLayout, case_name: &str) {
    let server = get_server();
    let client = ApiClient::new(&server.base_url);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let test_name = format!("gbf_packer_{case_name}_{timestamp}");

    // Use unique names to avoid conflicts with other tests
    let stream_name = format!("{}_stream", test_name);
    let schema_name = format!("{}_schema", test_name);
    let pipeline_id = format!("{}_pipeline", test_name);
    let input_topic = format!("/{}/input", test_name);
    let output_topic = format!("/{}/output", test_name);

    // Cleanup any leftover resources from previous runs
    let _ = client.delete(&format!("/pipelines/{}", pipeline_id));
    thread::sleep(Duration::from_millis(100));
    let _ = client.delete(&format!("/streams/{}", stream_name));
    thread::sleep(Duration::from_millis(100));
    let _ = client.delete(&format!("/schemas/{}", schema_name));

    let schema_payload = json!({
        "name": schema_name,
        "revision": 1,
        "type": "gbf",
        "props": { "schema_path": gbf_schema_path(layout) }
    });
    let resp = client.post_json("/schemas", &schema_payload);
    assert!(
        resp.status().is_success(),
        "create schema failed: {} - {:?}",
        resp.status(),
        resp.text()
    );

    // 1. Create stream with gbf decoder and dbc schema (same as gbf_test)
    //    PLUS sampler with packer strategy
    let stream_payload = json!({
        "name": stream_name,
        "revision": 1,
        "type": "mqtt",
        "schema": { "ref": schema_name },
        "props": {
            "broker_url": TestEnvironment::mqtt_addr(),
            "topic": input_topic,
            "qos": 0
        },
        "shared": false,
        "decoder": { "type": "gbf", "props": {} },
        "sampler": {
            "interval": "500ms",
            "strategy": {
                "type": "packer",
                "props": {
                    "merger": {
                        "type": "gbf",
                        "props": {}
                    }
                }
            }
        }
    });

    let resp = client.post_json("/streams", &stream_payload);
    assert!(
        resp.status().is_success(),
        "create stream failed: {} - {:?}",
        resp.status(),
        resp.text()
    );

    // 2. Create pipeline with columnar_json encoder (same as gbf_test)
    let pipeline_payload = json!({
        "id": pipeline_id,
        "revision": 1,
        "sql": format!("SELECT * FROM {}", stream_name),
        "sinks": [
            {
                "id": format!("{}_sink", test_name),
                "type": "mqtt",
                "props": {
                    "broker_url": TestEnvironment::mqtt_addr(),
                    "topic": output_topic,
                    "qos": 0
                },
                "encoder": {
                    "type": "columnar_json",
                    "props": {}
                }
            }
        ]
    });

    let resp = client.post_json("/pipelines", &pipeline_payload);
    assert!(
        resp.status().is_success(),
        "create pipeline failed: {} - {:?}",
        resp.status(),
        resp.text()
    );

    // 3. Setup MQTT subscriber for output WITH TIMEOUT
    let received_data: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received_data.clone();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();

    let mut mqtt_opts = MqttOptions::new(format!("{}_sub", test_name), "127.0.0.1", MQTT_PORT);
    mqtt_opts.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_connection) = Client::new(mqtt_opts, 10);
    sub_client
        .subscribe(&output_topic, QoS::AtLeastOnce)
        .unwrap();

    let sub_handle = thread::spawn(move || {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(5);

        for event in sub_connection.iter() {
            if stop_clone.load(Ordering::Relaxed) || start.elapsed() > timeout {
                break;
            }
            if let Ok(Event::Incoming(Packet::Publish(publish))) = event {
                if let Ok(json) = serde_json::from_slice::<Value>(&publish.payload) {
                    let mut data = received_clone.lock().unwrap();
                    data.push(json);
                    // We expect one packed batch
                    if data.len() >= 1 {
                        break;
                    }
                }
            }
        }
    });

    // Give subscriber time to connect
    thread::sleep(Duration::from_millis(300));

    // 4. Start pipeline
    let resp = client.post_json(&format!("/pipelines/{}/start", pipeline_id), &json!({}));
    assert!(
        resp.status().is_success(),
        "start pipeline failed: {} - {:?}",
        resp.status(),
        resp.text()
    );

    // Give pipeline time to start
    thread::sleep(Duration::from_millis(300));

    // 5. Publish 3 GBF packets with overlapping and unique CAN IDs
    let mut pub_opts = MqttOptions::new(format!("{}_pub", test_name), "127.0.0.1", MQTT_PORT);
    pub_opts.set_keep_alive(Duration::from_secs(5));
    let (pub_client, mut pub_connection) = Client::new(pub_opts, 10);

    // Spawn connection handler for publisher
    let pub_stop = Arc::new(AtomicBool::new(false));
    let pub_stop_clone = pub_stop.clone();
    thread::spawn(move || {
        for _ in pub_connection.iter() {
            if pub_stop_clone.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    // Original payload from HEX_DATA that produces: Sig1=84, Sig2=0, Sig3=464
    // Hex breakdown: 0854657374000011 = first byte 08, then "Test\x00\x00\x11"
    let payload_original = hex::decode("0854657374000011").expect("valid hex");

    // Different payloads to verify override behavior
    let payload_override_mess1 = hex::decode("08AABBCCDD000011").expect("valid hex");
    let payload_override_mess0 = hex::decode("08EEFF00112200FF").expect("valid hex");

    // Timestamps for each packet (ts3 is latest, should appear in output)
    let ts1: u64 = 1720765705290;
    let ts2: u64 = 1720765705300;
    let ts3: u64 = 1720765705310;

    // Packet 1: CAN IDs 0x1586 (Mess1) and 0x124A (Mess0) with original values
    let packet1 = build_gbf_packet(
        layout,
        ts1,
        &[(0x1586, &payload_original), (0x124A, &payload_original)],
    );
    pub_client
        .publish(&input_topic, QoS::AtLeastOnce, false, packet1)
        .expect("publish 1");
    thread::sleep(Duration::from_millis(50));

    // Packet 2: Override CAN ID 0x1586 (Mess1) with new payload + Unique ID 0x1000
    // 0x1000 is not in schema, so it won't appear as signals, but must be in raw data.
    let payload_1000 = hex::decode("1122334455667788").expect("valid hex");
    let packet2 = build_gbf_packet(
        layout,
        ts2,
        &[(0x1586, &payload_override_mess1), (0x1000, &payload_1000)],
    );
    pub_client
        .publish(&input_topic, QoS::AtLeastOnce, false, packet2)
        .expect("publish 2");
    thread::sleep(Duration::from_millis(50));

    // Packet 3: Override CAN ID 0x124A (Mess0) with new payload + Unique ID 0x2000
    let payload_2000 = hex::decode("8877665544332211").expect("valid hex");
    let packet3 = build_gbf_packet(
        layout,
        ts3,
        &[(0x124A, &payload_override_mess0), (0x2000, &payload_2000)],
    );
    pub_client
        .publish(&input_topic, QoS::AtLeastOnce, false, packet3)
        .expect("publish 3");

    // Wait for sampler interval + buffer
    thread::sleep(Duration::from_millis(800));

    // Stop subscriber
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for subscriber thread with timeout
    let _ = sub_handle.join();
    pub_stop.store(true, Ordering::Relaxed);

    // 6. Verify Output
    let received = received_data.lock().unwrap();
    println!("Received {} messages", received.len());
    for (i, msg) in received.iter().enumerate() {
        println!("Message {}: {:?}", i, msg);
    }

    assert!(!received.is_empty(), "No data received from pipeline");
    let result = &received[0];

    // Verify timestamp - should be the latest (ts3)
    if let Some(ts_arr) = result.get("ts").and_then(|v| v.as_array()) {
        assert!(!ts_arr.is_empty(), "ts array is empty");
        let received_ts = ts_arr[0].as_i64().unwrap();
        assert_eq!(received_ts, ts3 as i64, "Expected latest timestamp ts3");
        println!("✓ Timestamp verified: {}", received_ts);
    } else {
        panic!("Missing 'ts' field in output");
    }

    // Verify Mess0_Sig1 (CAN ID 0x124A) - should be from packet 3 (OVERRIDDEN)
    // Observed value: 238 (from payload byte 0xEE)
    let mess0_sig1 = match layout {
        CanIdentityLayout::CustomerEnvelope => "b1_24a_Mess0_Sig1",
        CanIdentityLayout::BusShift | CanIdentityLayout::BusIdRef => "Mess0_Sig1",
    };
    let sig_arr = result
        .get(mess0_sig1)
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("Missing '{mess0_sig1}' field in output"));
    let val = sig_arr[0].as_i64().unwrap();
    assert_eq!(val, 238, "Expected Mess0_Sig1 = 238 (from 0xEE)");
    println!("✓ Mess0_Sig1 verified (overridden): {}", val);

    // Verify Mess1_Sig1 (CAN ID 0x1586) - should be from packet 2 (OVERRIDDEN)
    // Observed value: 170 (from payload byte 0xAA)
    let mess1_sig1 = match layout {
        CanIdentityLayout::CustomerEnvelope => "b1_586_Mess1_Sig1",
        CanIdentityLayout::BusShift | CanIdentityLayout::BusIdRef => "Mess1_Sig1",
    };
    let sig_arr = result
        .get(mess1_sig1)
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("Missing '{mess1_sig1}' field in output"));
    let val = sig_arr[0].as_i64().unwrap();
    assert_eq!(val, 170, "Expected Mess1_Sig1 = 170 (from 0xAA)");
    println!("✓ Mess1_Sig1 verified (overridden): {}", val);

    // 7. Verify Stats
    thread::sleep(Duration::from_millis(200));
    if let Some(stats) = client.get_pipeline_stats(&pipeline_id) {
        println!("Pipeline Stats: {:?}", stats);
        for stat in stats.as_array().unwrap_or(&vec![]) {
            if let Some(id) = stat.get("processor_id").and_then(|v| v.as_str()) {
                if id.contains("Sampler") {
                    let messages_in = stat
                        .get("stats")
                        .and_then(|s| s.get("messages_in"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let records_out = stat
                        .get("stats")
                        .and_then(|s| s.get("records_out"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    assert_eq!(messages_in, 3, "Expected sampler to receive 3 messages");
                    assert_eq!(records_out, 1, "Expected sampler to emit 1 merged packet");
                    println!(
                        "✓ Sampler stats verified: messages_in={}, records_out={}",
                        messages_in, records_out
                    );
                }
            }
        }
    }

    // Cleanup
    let _ = client.delete(&format!("/pipelines/{}", pipeline_id));
    let _ = client.delete(&format!("/streams/{}", stream_name));
    let _ = client.delete(&format!("/schemas/{}", schema_name));
}

#[test]
#[serial_test::serial]
fn test_gbf_packer_integration() {
    run_gbf_packer_integration(CanIdentityLayout::BusShift, "bus_shift");
}

#[test]
#[serial_test::serial]
fn test_gbf_packer_bus_id_ref_integration() {
    run_gbf_packer_integration(CanIdentityLayout::BusIdRef, "bus_id_ref");
}

#[test]
#[serial_test::serial]
fn test_gbf_packer_customer_envelope_integration() {
    run_gbf_packer_integration(CanIdentityLayout::CustomerEnvelope, "customer_envelope");
}
