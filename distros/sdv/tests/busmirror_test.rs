//! End-to-end coverage for complete BusMirror schemas, direct decode, and packing.

mod common;

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{ApiClient, MQTT_PORT, TestEnvironment, get_server, write_schema_zip};
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};

const TEST_TIMEOUT: Duration = Duration::from_secs(8);

struct JsonSubscriber {
    _client: Client,
    messages: Receiver<Value>,
}

impl JsonSubscriber {
    fn connect(client_id: &str, topic: &str) -> Self {
        let mut options = MqttOptions::new(client_id, "127.0.0.1", MQTT_PORT);
        options.set_keep_alive(Duration::from_secs(5));
        let (client, mut connection) = Client::new(options, 16);
        client
            .subscribe(topic, QoS::AtLeastOnce)
            .expect("subscribe to output topic");

        let (ready_tx, ready_rx) = mpsc::channel();
        let (message_tx, messages) = mpsc::channel();
        thread::spawn(move || {
            let mut ready_tx = Some(ready_tx);
            for event in connection.iter() {
                match event {
                    Ok(Event::Incoming(Packet::SubAck(_))) => {
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        let value = serde_json::from_slice(&publish.payload)
                            .expect("decode MQTT sink JSON");
                        if message_tx.send(value).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
        ready_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("MQTT output subscription was not acknowledged");
        Self {
            _client: client,
            messages,
        }
    }

    fn recv(&self, label: &str) -> Value {
        self.messages
            .recv_timeout(TEST_TIMEOUT)
            .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
    }
}

struct MqttPublisher {
    client: Client,
}

impl MqttPublisher {
    fn connect(client_id: &str) -> Self {
        let mut options = MqttOptions::new(client_id, "127.0.0.1", MQTT_PORT);
        options.set_keep_alive(Duration::from_secs(5));
        options.set_inflight(1);
        let (client, mut connection) = Client::new(options, 32);
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut ready_tx = Some(ready_tx);
            for event in connection.iter() {
                match event {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
        ready_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("MQTT publisher did not connect");
        Self { client }
    }

    fn publish(&self, topic: &str, payload: Vec<u8>) {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .expect("publish BusMirror payload");
    }
}

fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/busmirror")
        .join(path)
}

fn schema_archive(suffix: &str) -> PathBuf {
    let archive = std::env::temp_dir().join(format!("veloflux-busmirror-{suffix}.zip"));
    let entry = std::fs::read(fixture("vehicle.json")).expect("read BusMirror entry");
    let propulsion = std::fs::read(fixture("vehicle/propulsion.dbc")).expect("read propulsion DBC");
    let chassis = std::fs::read(fixture("vehicle/chassis.dbc")).expect("read chassis DBC");
    write_schema_zip(
        &archive,
        &[
            ("vehicle.json", &entry),
            ("vehicle/propulsion.dbc", &propulsion),
            ("vehicle/chassis.dbc", &chassis),
        ],
    );
    archive
}

fn busmirror_frames() -> Vec<Vec<u8>> {
    std::fs::read_to_string(fixture("busmirror.lines"))
        .expect("read BusMirror payloads")
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| hex::decode(line).expect("decode BusMirror hex payload"))
        .collect()
}

fn assert_success(response: reqwest::blocking::Response, operation: &str) {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        panic!("{operation} failed: {status} - {body}");
    }
}

fn create_stream(client: &ApiClient, name: &str, schema_name: &str, topic: &str, packer: bool) {
    let mut body = json!({
        "name": name,
        "revision": 1,
        "type": "mqtt",
        "schema": { "ref": schema_name },
        "props": {
            "broker_url": TestEnvironment::mqtt_addr(),
            "topic": topic,
            "qos": 1
        },
        "shared": false,
        "decoder": { "type": "busmirror", "props": {} }
    });
    if packer {
        body["sampler"] = json!({
            "interval": "1h",
            "strategy": {
                "type": "packer",
                "props": {
                    "merger": { "type": "busmirror", "props": {} }
                }
            }
        });
    }
    assert_success(client.post_json("/streams", &body), "create stream");
}

fn create_pipeline(
    client: &ApiClient,
    id: &str,
    stream_name: &str,
    sink: PipelineSink<'_>,
    include_extended: bool,
) {
    let mut columns = vec![
        "ts",
        "`can1__100__RPM`",
        "`can1__101__GearPos`",
        "`can2__200__FL`",
        "`can2__201__BrakePressure`",
    ];
    if include_extended {
        columns.insert(3, "`can1__103__ExtendedRPM`");
    }
    let sink = match sink {
        PipelineSink::Mqtt(topic) => json!({
            "id": format!("{id}_sink"),
            "type": "mqtt",
            "props": {
                "broker_url": TestEnvironment::mqtt_addr(),
                "topic": topic,
                "qos": 1
            },
            "encoder": { "type": "json", "props": {} }
        }),
        PipelineSink::File(path) => json!({
            "id": format!("{id}_sink"),
            "type": "file",
            "props": {
                "path": path,
                "filename_prefix": "packed_",
                "filename_suffix": ".json"
            },
            "encoder": { "type": "json", "props": {} }
        }),
    };
    let body = json!({
        "id": id,
        "revision": 1,
        "sql": format!("SELECT {} FROM {stream_name}", columns.join(", ")),
        "sinks": [sink]
    });
    assert_success(client.post_json("/pipelines", &body), "create pipeline");
}

enum PipelineSink<'a> {
    Mqtt(&'a str),
    File(&'a Path),
}

fn start_pipeline(client: &ApiClient, id: &str) {
    assert_success(
        client.post_json(&format!("/pipelines/{id}/start"), &json!({})),
        "start pipeline",
    );
}

fn wait_for_buffered_sampler_input(client: &ApiClient, pipeline_id: &str) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut last_stats = None;
    while Instant::now() < deadline {
        if let Some(stats) = client.get_pipeline_stats(pipeline_id) {
            let matched = stats.as_array().is_some_and(|processors| {
                processors.iter().any(|processor| {
                    let is_sampler = processor
                        .get("processor_id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| id.contains("PhysicalSampler"));
                    let records_in = processor
                        .pointer("/stats/records_in")
                        .and_then(Value::as_u64);
                    let records_out = processor
                        .pointer("/stats/records_out")
                        .and_then(Value::as_u64);
                    is_sampler && records_in == Some(11) && records_out == Some(0)
                })
            });
            if matched {
                return;
            }
            last_stats = Some(stats);
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("sampler did not buffer 11 inputs without emitting; last stats: {last_stats:?}");
}

#[test]
fn decodes_and_packs_busmirror_end_to_end() {
    let server = get_server();
    let client = ApiClient::new(&server.base_url);
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_millis()
    );
    let schema_name = format!("busmirror_schema_{suffix}");
    let direct_stream = format!("busmirror_direct_{suffix}");
    let packer_stream = format!("busmirror_packer_{suffix}");
    let direct_pipeline = format!("busmirror_direct_pipe_{suffix}");
    let packer_pipeline = format!("busmirror_packer_pipe_{suffix}");
    let direct_input = format!("/{suffix}/busmirror/direct/input");
    let packer_input = format!("/{suffix}/busmirror/packer/input");
    let direct_output = format!("/{suffix}/busmirror/direct/output");
    let packer_output = std::env::temp_dir().join(format!("veloflux-busmirror-{suffix}"));
    let archive = schema_archive(&suffix);
    std::fs::create_dir(&packer_output).expect("create BusMirror packer output directory");

    assert_success(
        client.post_json(
            "/schemas",
            &json!({
                "name": schema_name,
                "revision": 1,
                "type": "busmirror",
                "props": { "schema_path": archive }
            }),
        ),
        "install BusMirror schema",
    );
    create_stream(&client, &direct_stream, &schema_name, &direct_input, false);
    create_stream(&client, &packer_stream, &schema_name, &packer_input, true);
    create_pipeline(
        &client,
        &direct_pipeline,
        &direct_stream,
        PipelineSink::Mqtt(&direct_output),
        true,
    );
    create_pipeline(
        &client,
        &packer_pipeline,
        &packer_stream,
        PipelineSink::File(&packer_output),
        false,
    );

    let direct_subscriber =
        JsonSubscriber::connect(&format!("busmirror_direct_sub_{suffix}"), &direct_output);
    start_pipeline(&client, &direct_pipeline);
    start_pipeline(&client, &packer_pipeline);
    thread::sleep(Duration::from_millis(500));

    let publisher = MqttPublisher::connect(&format!("busmirror_pub_{suffix}"));
    let frames = busmirror_frames();
    assert_eq!(frames.len(), 11, "unexpected BusMirror fixture count");

    publisher.publish(&direct_input, frames[..5].concat());
    assert_eq!(
        direct_subscriber.recv("first direct BusMirror batch"),
        json!([
            {"ts": 1_000_000, "can1__100__RPM": 3000, "can1__101__GearPos": 3},
            {"ts": 1_000_000, "can2__200__FL": 60.0, "can2__201__BrakePressure": 80.0},
            {"ts": 1_000_000, "can1__100__RPM": 1500, "can2__200__FL": 45.0},
            {"ts": 1_000_000},
            {"ts": 1_000_000}
        ])
    );

    publisher.publish(&direct_input, frames[5..].concat());
    assert_eq!(
        direct_subscriber.recv("second direct BusMirror batch"),
        json!([
            {"ts": 1_000_000, "can1__100__RPM": 2200},
            {"ts": 1_000_000},
            {
                "ts": 1_000_000,
                "can1__100__RPM": 4200,
                "can1__101__GearPos": 5,
                "can2__200__FL": 80.0,
                "can2__201__BrakePressure": 50.0
            },
            {"ts": 1_000_000},
            {
                "ts": 1_000_000,
                "can1__100__RPM": 5000,
                "can2__201__BrakePressure": 30.0
            },
            {"ts": 1_000_000, "can1__103__ExtendedRPM": 6100}
        ])
    );

    for frame in frames {
        publisher.publish(&packer_input, frame);
    }
    // Use sampler stats as a delivery barrier before triggering a graceful
    // stop. The long interval prevents a wall-clock tick from splitting the
    // MQTT burst, while records_in=11 proves that all separate messages have
    // reached the same packer window. Graceful stop then exercises the
    // sampler's terminal flush path deterministically. Periodic tick emission
    // is covered by the flow-level sampler tests.
    wait_for_buffered_sampler_input(&client, &packer_pipeline);
    assert_success(
        client.post_json(
            &format!("/pipelines/{packer_pipeline}/stop?mode=graceful&timeout_ms=5000"),
            &json!({}),
        ),
        "gracefully stop packer pipeline",
    );
    let packed_files = std::fs::read_dir(&packer_output)
        .expect("read BusMirror packer output directory")
        .map(|entry| entry.expect("read BusMirror packer output entry").path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    assert_eq!(packed_files.len(), 1, "unexpected packed output files");
    assert_eq!(
        serde_json::from_slice::<Value>(
            &std::fs::read(&packed_files[0]).expect("read packed BusMirror output")
        )
        .expect("decode packed BusMirror output"),
        json!([{
            "ts": 1_000_000,
            "can1__100__RPM": 5000,
            "can1__101__GearPos": 5,
            "can2__200__FL": 80.0,
            "can2__201__BrakePressure": 30.0
        }])
    );
    client.verify_pipeline_stats(&direct_pipeline);

    let _ = client.delete(&format!("/pipelines/{direct_pipeline}"));
    let _ = client.delete(&format!("/pipelines/{packer_pipeline}"));
    let _ = client.delete(&format!("/streams/{direct_stream}"));
    let _ = client.delete(&format!("/streams/{packer_stream}"));
    let _ = client.delete(&format!("/schemas/{schema_name}"));
    let _ = std::fs::remove_file(archive);
    let _ = std::fs::remove_dir_all(packer_output);
    server.stop();
}
