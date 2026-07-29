use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix,
    write_schema_zip,
};
use reqwest::StatusCode;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use serde_json::Value as JsonValue;
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
        let (startup_tx, startup_rx) = mpsc::channel();

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
                let _ = startup_tx.send(broker.start());
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

/// Minimal proto message for decoder e2e testing.
const SIMPLE_PROTO: &str = r#"
syntax = "proto3";
message Simple {
  int32 a = 1;
  int32 b = 2;
}
"#;

// ── test ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_with_protobuf_decoder_decodes_and_pipelines_data() {
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

    // ── 1. Start embedded MQTT broker for output verification ──────

    let broker = EmbeddedMqttBroker::start().await;
    let broker_url = broker.broker_url();
    let mqtt_topic = format!("e2e/proto_decode/{}", random_suffix());

    // ── 2. Write proto file to temp dir ────────────────────────────

    let proto_path = temp_dir.path().join("simple.zip");
    write_schema_zip(&proto_path, &[("simple.proto", SIMPLE_PROTO.as_bytes())]);

    // ── 3. Create proto schema via REST ────────────────────────────

    let schema_name = format!("e2e_proto_decode_schema_{}", random_suffix());
    let create_schema_resp = http
        .post(format!("{manager_base}/schemas"))
        .json(&serde_json::json!({
            "name": schema_name,
            "revision": 1,
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

    // ── 4. Create stream with protobuf decoder ─────────────────────

    let stream_name = format!("e2e_proto_stream_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "mock",
            "schema": { "ref": schema_name },
            "props": {},
            "shared": true,
            "decoder": { "type": "protobuf", "props": {} }
        }))
        .send()
        .await
        .expect("create stream request");
    assert_eq!(
        create_stream_resp.status(),
        StatusCode::CREATED,
        "create stream with protobuf decoder should return 201: {}",
        create_stream_resp.text().await.unwrap_or_default()
    );

    // ── 5. Create and start pipeline with MQTT sink ────────────────

    let pipeline_id = format!("e2e_proto_pipe_{}", random_suffix());
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!("SELECT a, b FROM {stream_name}"),
            "sinks": [{
                "type": "mqtt",
                "props": {
                    "broker_url": broker_url,
                    "topic": mqtt_topic,
                    "qos": 0
                },
                "encoder": { "type": "json", "props": {} }
            }]
        }))
        .send()
        .await
        .expect("create pipeline request");
    assert_eq!(
        create_pipeline_resp.status(),
        StatusCode::CREATED,
        "create pipeline should return 201: {}",
        create_pipeline_resp.text().await.unwrap_or_default()
    );

    let start_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    assert_eq!(
        start_resp.status(),
        StatusCode::OK,
        "start pipeline should return 200: {}",
        start_resp.text().await.unwrap_or_default()
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
                    eprintln!("mqtt event loop error in background task: {err}");
                    return;
                }
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(15), connack_rx)
        .await
        .expect("subscriber connack timeout")
        .expect("subscriber connack channel closed");

    // ── 7. Inject protobuf binary data ─────────────────────────────

    // Encode protobuf message using the generated `Simple` type.
    let proto_payload =
        flow::test_proto::encode_simple(&flow::test_proto::simple::Simple { a: 42, b: 99 });

    const MAX_INJECT_ATTEMPTS: usize = 10;
    const INJECT_RETRY_DELAY: Duration = Duration::from_secs(3);
    for _attempt in 1..=MAX_INJECT_ATTEMPTS {
        injector
            .send_shared_mock_stream_payload(&stream_name, proto_payload.clone())
            .await
            .expect("inject protobuf payload into mock stream");

        tokio::time::sleep(INJECT_RETRY_DELAY).await;

        if received.lock().unwrap().is_some() {
            break;
        }
    }

    let output_bytes = received
        .lock()
        .unwrap()
        .take()
        .expect("timed out waiting for mqtt output after retries");

    let _ = sub_client.disconnect().await;

    let output: JsonValue =
        serde_json::from_slice(&output_bytes).expect("parse mqtt output as json");

    // JSON encoder wraps records in an array; extract the first record.
    let record = &output[0];
    assert_eq!(record["a"].as_i64().unwrap(), 42, "output a mismatch");
    assert_eq!(record["b"].as_i64().unwrap(), 99, "output b mismatch");

    // ── 8. Cleanup ─────────────────────────────────────────────────

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
