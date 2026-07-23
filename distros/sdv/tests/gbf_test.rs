//! Integration test for GBF decoder with columnar_json encoder.
//!
//! Verifies the full pipeline by:
//! - Creating stream with `gbf` decoder and `dbc` schema
//! - Creating pipeline with `columnar_json` encoder
//! - Publishing hex data via MQTT
//! - Verifying output matches expected columnar JSON
//!
//! Note: All tests share a singleton server instance.
//! Each test uses unique stream/pipeline names and cleans up after itself.

mod common;

use common::{ApiClient, MQTT_PORT, TestEnvironment, get_server, write_schema_zip};
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

/// Get the absolute path to the JSON test schema file.
fn json_schema_path() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/sim.json")
        .to_str()
        .unwrap()
        .to_string()
}

/// Get the absolute path to the DBC test schema directory.
fn dbc_schema_path() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/dbc")
        .to_str()
        .unwrap()
        .to_string()
}

fn gbf_schema_path() -> String {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| {
        let source_layout =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/spi_packet.json");
        let mut document: Value =
            serde_json::from_slice(&std::fs::read(source_layout).expect("read GBF packet layout"))
                .expect("parse GBF packet layout");
        document["format"] = json!({
            "type": "can",
            "props": {
                "dbc_path": "format/sim.json",
                "can_id_mapping": { "mode": "bus_shift", "bits": 12 }
            }
        });
        let entry = serde_json::to_vec_pretty(&document).expect("encode GBF schema");
        let dbc = std::fs::read(json_schema_path()).expect("read private DBC source");
        let archive = std::env::temp_dir().join(format!("veloflux-gbf-{}.zip", std::process::id()));
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

/// Hex data from README example
const HEX_DATA: &str = "00000190A5A0EC4A001855158688085465737400001155124A880854657374000011";

/// Run the GBF decoder integration test with the specified stream schema.
fn run_gbf_test(test_name: &str, _dbc_schema_path: &str) {
    let server = get_server();
    let client = ApiClient::new(&server.base_url);

    // Use unique names to avoid conflicts with other tests
    let stream_name = format!("{}_stream", test_name);
    let schema_name = format!("{}_schema", test_name);
    let pipeline_id = format!("{}_pipeline", test_name);
    let input_topic = format!("/{}/input", test_name);
    let output_topic = format!("/{}/output", test_name);

    // Cleanup any leftover resources from previous runs
    let _ = client.delete(&format!("/pipelines/{}", pipeline_id));
    let _ = client.delete(&format!("/streams/{}", stream_name));
    let _ = client.delete(&format!("/schemas/{}", schema_name));

    let schema_payload = json!({
        "name": schema_name,
        "type": "gbf",
        "props": { "schema_path": gbf_schema_path() }
    });
    let resp = client.post_json("/schemas", &schema_payload);
    assert!(
        resp.status().is_success(),
        "create schema failed: {} - {:?}",
        resp.status(),
        resp.text()
    );

    // 1. Create stream with gbf decoder and dbc schema
    let stream_payload = json!({
        "name": stream_name,
        "type": "mqtt",
        "schema": { "ref": schema_name },
        "props": {
            "broker_url": TestEnvironment::mqtt_addr(),
            "topic": input_topic,
            "qos": 0
        },
        "shared": false,
        "decoder": { "type": "gbf", "props": {} }
    });

    let resp = client.post_json("/streams", &stream_payload);
    assert!(
        resp.status().is_success(),
        "create stream failed: {}",
        resp.status()
    );

    // 2. Create pipeline with columnar_json encoder and mqtt sink
    let pipeline_payload = json!({
        "id": pipeline_id,
        "sql": format!("SELECT * from {}", stream_name),
        "sinks": [
            {
                "id": format!("{}_sink", test_name),
                "type": "mqtt",
                "props": {
                    "broker_url": TestEnvironment::mqtt_addr(),
                    "topic": output_topic,
                    "qos": 0
                },
                "common_sink_props": {
                    "batch_count": 2
                },
                "encoder": {
                    "type": "columnar_json",
                    "props": {}
                }
            }
        ]
    });

    let resp = client.post_json("/pipelines", &pipeline_payload);
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        panic!("create pipeline failed: {} - {}", status, body);
    }

    // 3. Setup MQTT subscriber for output
    let received_data: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let received_clone = received_data.clone();

    let mut mqtt_opts =
        MqttOptions::new(format!("{}_subscriber", test_name), "127.0.0.1", MQTT_PORT);
    mqtt_opts.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_connection) = Client::new(mqtt_opts, 10);
    sub_client
        .subscribe(&output_topic, QoS::AtLeastOnce)
        .unwrap();

    let sub_handle = thread::spawn(move || {
        for event in sub_connection.iter() {
            if let Ok(Event::Incoming(Packet::Publish(publish))) = event {
                if let Ok(json) = serde_json::from_slice::<Value>(&publish.payload) {
                    let mut data = received_clone.lock().unwrap();
                    *data = Some(json);
                    // Got the expected message, exit the loop
                    return;
                }
            }
        }
    });

    // Give subscriber time to connect
    thread::sleep(Duration::from_millis(500));

    // 4. Start the pipeline
    let resp = client.post_json(&format!("/pipelines/{}/start", pipeline_id), &json!({}));
    assert!(
        resp.status().is_success(),
        "start pipeline failed: {}",
        resp.status()
    );

    // Give pipeline time to start
    thread::sleep(Duration::from_millis(500));

    // 5. Publish hex data twice (as per README example)
    let mut pub_opts = MqttOptions::new(format!("{}_publisher", test_name), "127.0.0.1", MQTT_PORT);
    pub_opts.set_keep_alive(Duration::from_secs(5));
    let (pub_client, mut pub_connection) = Client::new(pub_opts, 10);

    // Spawn connection handler
    thread::spawn(move || for _ in pub_connection.iter() {});

    let payload = hex::decode(HEX_DATA).expect("decode hex");
    pub_client
        .publish(&input_topic, QoS::AtLeastOnce, false, payload.clone())
        .expect("publish 1");
    pub_client
        .publish(&input_topic, QoS::AtLeastOnce, false, payload)
        .expect("publish 2");

    // Wait for subscriber to receive the batch
    sub_handle.join().expect("subscriber thread panicked");

    // 6. Assert received data matches expected
    let expected = json!({
        "ts": [1720765705290i64, 1720765705290i64],
        "Mess0_Sig1": [84, 84],
        "Mess0_Sig2": [0, 0],
        "Mess0_Sig3": [464, 464],
        "Mess1_Sig1": [84, 84],
        "Mess1_Sig2": [0, 0],
        "Mess1_Sig3": [464, 464]
    });

    let received = received_data.lock().unwrap();
    assert!(received.is_some(), "did not receive output data");
    let actual = received.as_ref().unwrap();
    assert_eq!(
        actual, &expected,
        "output mismatch\nexpected: {}\nactual: {}",
        expected, actual
    );

    // 6.5. Verify pipeline stats show data was processed
    let stats = client.verify_pipeline_stats(&pipeline_id);
    println!("Pipeline {} stats: {:?}", pipeline_id, stats);

    // 7. Cleanup (tolerate 404 if already deleted)
    let resp = client.delete(&format!("/pipelines/{}", pipeline_id));
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 404,
        "delete pipeline failed: {}",
        resp.status()
    );

    let resp = client.delete(&format!("/streams/{}", stream_name));
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 404,
        "delete stream failed: {}",
        resp.status()
    );
    let resp = client.delete(&format!("/schemas/{}", schema_name));
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 404,
        "delete schema failed: {}",
        resp.status()
    );
}

#[test]
fn test_gbf_integration() {
    // Run JSON schema test
    run_gbf_test("gbf_json", &json_schema_path());

    // Run DBC schema test
    run_gbf_test("gbf_dbc", &dbc_schema_path());

    // Stop server to flush coverage data (static singletons don't auto-drop)
    get_server().stop();
}
