use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix,
    write_schema_zip,
};
use reqwest::StatusCode;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpStream;

// ── embedded MQTT broker helpers ──────────────────────────────────

struct EmbeddedMqttBroker {
    port: u16,
}

impl EmbeddedMqttBroker {
    async fn start() -> Self {
        let port = reserve_local_port();
        let (_startup_tx, startup_rx) = mpsc::channel();

        let config = Config {
            id: 0,
            router: RouterConfig {
                max_connections: 32,
                max_outgoing_packet_count: 64,
                max_segment_size: 1024,
                max_segment_count: 8,
                custom_segment: None,
                initialized_filters: None,
                shared_subscriptions_strategy: Default::default(),
            },
            v4: Some(HashMap::from([(
                "test".to_string(),
                ServerSettings {
                    name: "mqtt-test".to_string(),
                    listen: SocketAddr::from(([127, 0, 0, 1], port)),
                    tls: None,
                    next_connection_delay_ms: 1,
                    connections: ConnectionSettings {
                        connection_timeout_ms: 5_000,
                        max_payload_size: 1024 * 1024,
                        max_inflight_count: 32,
                        auth: None,
                        external_auth: None,
                        dynamic_filters: true,
                    },
                },
            )])),
            ..Config::default()
        };

        thread::Builder::new()
            .name(format!("rumqttd-e2e-{port}"))
            .spawn(move || {
                let mut broker = Broker::new(config);
                let _ = _startup_tx.send(broker.start());
            })
            .expect("spawn embedded mqtt broker thread");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return Self { port };
            }
            if tokio::time::Instant::now() > deadline {
                let msg = startup_rx
                    .try_recv()
                    .unwrap_or_else(|_| Ok(()))
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "broker did not start in time".to_string());
                panic!("embedded mqtt broker startup failed: {msg}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn broker_url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port)
    }
}

fn reserve_local_port() -> u16 {
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral tcp listener");
    listener
        .local_addr()
        .expect("read ephemeral tcp listener address")
        .port()
}

// ── proto content ─────────────────────────────────────────────────

const SIMPLE_PROTO: &str = r#"
syntax = "proto3";
message Simple {
  int32  a = 1;
  double d = 2;
  int64  i = 3;
  bool   ok = 4;
  string s = 5;
  bytes  bin = 6;
  repeated int32 nums = 7;
}
"#;

// ── protobuf wire-format mini decoder ─────────────────────────────

const WIRE_VARINT: u32 = 0;
const WIRE_FIXED64: u32 = 1;
const WIRE_LENGTH_DELIMITED: u32 = 2;

fn read_varint(bytes: &[u8]) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut shift = 0;
    for (i, &b) in bytes.iter().enumerate() {
        value |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return (value, i + 1);
        }
        shift += 7;
        if shift >= 64 {
            panic!("varint too long");
        }
    }
    panic!("truncated varint");
}

fn read_fixed64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}

/// Parse a single field from wire-format bytes at `pos`.
fn parse_field(bytes: &[u8], pos: usize) -> (u32, Vec<u8>, usize) {
    let tag = bytes[pos];
    let field_number = (tag as u32) >> 3;
    let wire_type = (tag as u32) & 0x07;

    match wire_type {
        WIRE_VARINT => {
            let (_, consumed) = read_varint(&bytes[pos + 1..]);
            let end = pos + 1 + consumed;
            (field_number, bytes[pos..end].to_vec(), end)
        }
        WIRE_FIXED64 => (field_number, bytes[pos..pos + 9].to_vec(), pos + 9),
        WIRE_LENGTH_DELIMITED => {
            let (len, consumed) = read_varint(&bytes[pos + 1..]);
            let data_start = pos + 1 + consumed;
            let end = data_start + len as usize;
            (field_number, bytes[pos..end].to_vec(), end)
        }
        other => panic!("unexpected wire type {other}"),
    }
}

// ── test ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protobuf_encoder_covers_all_value_types() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let injector = instance.clone();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── 1. Start embedded MQTT broker ──────────────────────────────

    let broker = EmbeddedMqttBroker::start().await;
    let broker_url = broker.broker_url();
    let mqtt_topic = format!("e2e/proto_encode/{}", random_suffix());

    // ── 2. Write proto file to temp dir ────────────────────────────

    let proto_path = temp_dir.path().join("simple.zip");
    write_schema_zip(&proto_path, &[("simple.proto", SIMPLE_PROTO.as_bytes())]);

    // ── 3. Create proto schema via REST ────────────────────────────

    let schema_name = format!("e2e_enc_test_schema_{}", random_suffix());
    let create_schema_resp = http
        .post(format!("{manager_base}/schemas"))
        .json(&serde_json::json!({
            "name": schema_name,
            "type": "proto",
            "props": {
                "proto_path": proto_path.to_string_lossy(),
                "message_type": "Simple"
            }
        }))
        .send()
        .await
        .expect("create schema request");
    assert_eq!(
        create_schema_resp.status(),
        StatusCode::CREATED,
        "create proto schema should return 201: {}",
        create_schema_resp.text().await.unwrap_or_default()
    );

    // ── 4. Create mock stream (JSON decoder, schema from proto) ────

    let stream_name = format!("e2e_enc_test_stream_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "type": "mock",
            "schema": { "ref": schema_name },
            "props": {},
            "shared": true,
            "decoder": { "type": "json", "props": {} }
        }))
        .send()
        .await
        .expect("create stream request");
    assert_eq!(
        create_stream_resp.status(),
        StatusCode::CREATED,
        "create mock stream should return 201: {}",
        create_stream_resp.text().await.unwrap_or_default()
    );

    // ── 5. Create pipeline with protobuf encoder → MQTT sink ──────

    let pipeline_id = format!("e2e_enc_test_pipe_{}", random_suffix());
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "sql": format!("SELECT a, d, i, ok, s, bin, nums FROM {stream_name}"),
            "sinks": [{
                "type": "mqtt",
                "props": {
                    "broker_url": broker_url,
                    "topic": mqtt_topic,
                    "qos": 0
                },
                "encoder": {
                    "type": "protobuf",
                    "props": { "ref": schema_name }
                }
            }]
        }))
        .send()
        .await
        .expect("create pipeline request");
    let create_status = create_pipeline_resp.status();
    let create_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert_eq!(
        create_status,
        StatusCode::CREATED,
        "create pipeline with protobuf encoder should return 201, got {create_status}: {create_body}"
    );

    let start_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    let start_status = start_resp.status();
    let start_body = start_resp.text().await.unwrap_or_default();
    assert_eq!(
        start_status,
        StatusCode::OK,
        "start pipeline should return 200, got {start_status}: {start_body}"
    );

    // ── 6. Subscribe to MQTT output topic ──────────────────────────

    let (host, port) = {
        let url = broker_url.strip_prefix("tcp://").unwrap_or(&broker_url);
        let (h, p) = url.split_once(':').expect("broker_url must have port");
        (
            h.to_string(),
            p.parse::<u16>().expect("invalid broker port"),
        )
    };
    let sub_client_id = format!("sub-{}", random_suffix());
    let mut sub_options = MqttOptions::new(&sub_client_id, &host, port);
    sub_options.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_event_loop) = AsyncClient::new(sub_options, 10);
    sub_client
        .subscribe(&mqtt_topic, QoS::AtLeastOnce)
        .await
        .expect("subscribe to mqtt topic");

    let (connack_tx, connack_rx) = tokio::sync::oneshot::channel();
    let received: std::sync::Arc<std::sync::Mutex<Option<Vec<u8>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let received_clone = received.clone();
    tokio::spawn(async move {
        let mut connack_tx = Some(connack_tx);
        loop {
            match sub_event_loop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    if let Some(tx) = connack_tx.take() {
                        let _ = tx.send(());
                    }
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    *received_clone.lock().unwrap() = Some(publish.payload.to_vec());
                    return;
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!("mqtt event loop error: {err}");
                    return;
                }
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(15), connack_rx)
        .await
        .expect("subscriber connack timeout")
        .expect("subscriber connack channel closed");

    // ── 7. Inject JSON data ────────────────────────────────────────

    const MAX_INJECT_ATTEMPTS: usize = 10;
    const INJECT_RETRY_DELAY: Duration = Duration::from_secs(3);
    for _attempt in 1..=MAX_INJECT_ATTEMPTS {
        injector
            .send_shared_mock_stream_payload(
                &stream_name,
                serde_json::to_vec(&serde_json::json!({
                    "a": 42,
                    "d": 1.5,
                    "i": 999,
                    "ok": true,
                    "s": "hello",
                    "bin": "d29ybGQ=",
                    "nums": [1, 2, 3]
                }))
                .unwrap(),
            )
            .await
            .expect("inject payload into mock stream");

        tokio::time::sleep(INJECT_RETRY_DELAY).await;

        if received.lock().unwrap().is_some() {
            break;
        }
    }

    let output_bytes = received
        .lock()
        .unwrap()
        .take()
        .expect("timed out waiting for mqtt protobuf output after retries");

    let _ = sub_client.disconnect().await;

    // ── 8. Decode protobuf wire format ─────────────────────────────

    let mut pos = 0;
    let mut fields: HashMap<u32, Vec<u8>> = HashMap::new();
    while pos < output_bytes.len() {
        let (fnbr, raw, next) = parse_field(&output_bytes, pos);
        fields.insert(fnbr, raw);
        pos = next;
    }

    // field 1: int32 a = 42
    let raw = fields.get(&1).expect("field 1 (a) missing");
    let (val, _) = read_varint(&raw[1..]);
    assert_eq!(val, 42, "a mismatch");

    // field 2: double d = 1.5
    let raw = fields.get(&2).expect("field 2 (d) missing");
    let bits = read_fixed64(&raw[1..]);
    let d = f64::from_bits(bits);
    assert!((d - 1.5).abs() < 0.001, "d mismatch: {d}");

    // field 3: int64 i = 999
    let raw = fields.get(&3).expect("field 3 (i) missing");
    let (val, _) = read_varint(&raw[1..]);
    assert_eq!(val as i64, 999, "i mismatch");

    // field 4: bool ok = true
    let raw = fields.get(&4).expect("field 4 (ok) missing");
    let (val, _) = read_varint(&raw[1..]);
    assert_eq!(val, 1, "ok should be 1 (true)");

    // field 5: string s = "hello"
    let raw = fields.get(&5).expect("field 5 (s) missing");
    let (len, consumed) = read_varint(&raw[1..]);
    let data = &raw[1 + consumed..1 + consumed + len as usize];
    assert_eq!(data, b"hello", "s mismatch");

    // field 6: bytes bin = base64("world")
    let raw = fields.get(&6).expect("field 6 (bin) missing");
    let (len, consumed) = read_varint(&raw[1..]);
    let data = &raw[1 + consumed..1 + consumed + len as usize];
    assert_eq!(data, b"world", "bin mismatch");

    // field 7: repeated int32 nums = [1, 2, 3] (packed)
    let raw = fields.get(&7).expect("field 7 (nums) missing");
    let (packed_len, consumed) = read_varint(&raw[1..]);
    let packed_start = 1 + consumed;
    let mut nums = Vec::new();
    let mut p = packed_start;
    while p < packed_start + packed_len as usize {
        let (v, c) = read_varint(&raw[p..]);
        nums.push(v as i32);
        p += c;
    }
    assert_eq!(nums, vec![1, 2, 3], "nums mismatch");

    // ── 9. Cleanup ─────────────────────────────────────────────────

    let stop_resp = http
        .post(format!(
            "{manager_base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000"
        ))
        .send()
        .await
        .expect("stop pipeline request");
    assert_eq!(stop_resp.status(), StatusCode::OK);

    let delete_pipe_resp = http
        .delete(format!("{manager_base}/pipelines/{pipeline_id}"))
        .send()
        .await
        .expect("delete pipeline request");
    assert_eq!(delete_pipe_resp.status(), StatusCode::OK);

    let delete_stream_resp = http
        .delete(format!("{manager_base}/streams/{stream_name}"))
        .send()
        .await
        .expect("delete stream request");
    assert_eq!(delete_stream_resp.status(), StatusCode::OK);

    let delete_schema_resp = http
        .delete(format!("{manager_base}/schemas/{schema_name}"))
        .send()
        .await
        .expect("delete schema request");
    assert_eq!(delete_schema_resp.status(), StatusCode::OK);

    server.abort();
    let _ = server.await;
}
