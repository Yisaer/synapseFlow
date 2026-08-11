use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rumqttc::v5::mqttbytes::v5::Packet as V5Packet;
use rumqttc::v5::mqttbytes::QoS as V5Qos;
use rumqttc::v5::{AsyncClient as V5Client, Event as V5Event, MqttOptions as V5Options};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, OnceCell};
use tokio::time::sleep;
use uuid::Uuid;

const EMBEDDED_MQTT_BROKER_START_RETRIES: usize = 16;

static SHARED_EMBEDDED_MQTT_BROKER: OnceCell<SharedEmbeddedMqttBroker> = OnceCell::const_new();

struct SharedEmbeddedMqttBroker {
    v4_port: u16,
    v5_port: u16,
}

pub(crate) struct EmbeddedMqttBroker {
    port: u16,
    v5_port: u16,
    topic_prefix: String,
}

impl EmbeddedMqttBroker {
    pub(crate) async fn start() -> Self {
        let shared = SHARED_EMBEDDED_MQTT_BROKER
            .get_or_init(start_shared_broker)
            .await;

        Self {
            port: shared.v4_port,
            v5_port: shared.v5_port,
            topic_prefix: format!("tests/{}", Uuid::new_v4()),
        }
    }

    pub(crate) fn broker_url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port)
    }

    pub(crate) fn broker_url_v5(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.v5_port)
    }

    pub(crate) fn scoped_filter(&self, filter: &str) -> String {
        self.scoped_topic(filter)
    }

    pub(crate) fn scoped_topic(&self, topic: &str) -> String {
        let topic = topic.trim_start_matches('/');
        format!("{}/{}", self.topic_prefix, topic)
    }

    pub(crate) async fn publish(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let scoped_topic = self.scoped_topic(topic);
        let mut options = MqttOptions::new(
            format!("publisher-{}", Uuid::new_v4()),
            "127.0.0.1",
            self.port,
        );
        options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(options, 8);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (publish_tx, publish_rx) = oneshot::channel();
        let pump = tokio::spawn(async move {
            let mut ready_tx = Some(ready_tx);
            let mut publish_tx = Some(publish_tx);
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Ok(()));
                        }
                    }
                    Ok(Event::Incoming(Packet::PubAck(_))) => {
                        if let Some(publish_tx) = publish_tx.take() {
                            let _ = publish_tx.send(Ok(()));
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Err(err.to_string()));
                        }
                        if let Some(publish_tx) = publish_tx.take() {
                            let _ = publish_tx.send(Err(err.to_string()));
                        }
                        break;
                    }
                }
            }
        });

        let publish_result = async {
            match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(err))) => return Err(err),
                Ok(Err(_)) => {
                    return Err("embedded mqtt publisher readiness channel closed".to_string())
                }
                Err(_) => {
                    return Err(
                        "timed out waiting for embedded mqtt publisher connection".to_string()
                    )
                }
            }

            client
                .publish(scoped_topic, QoS::AtLeastOnce, false, payload.into())
                .await
                .map_err(|err| err.to_string())?;

            match tokio::time::timeout(Duration::from_secs(5), publish_rx).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(err))) => Err(err),
                Ok(Err(_)) => Err("embedded mqtt publisher ack channel closed".to_string()),
                Err(_) => Err("timed out waiting for embedded mqtt publish ack".to_string()),
            }
        }
        .await;

        let _ = client.disconnect().await;
        pump.abort();
        let _ = pump.await;
        publish_result
    }

    pub(crate) async fn publish_v5(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), String> {
        let scoped_topic = self.scoped_topic(topic);
        let mut options = V5Options::new(
            format!("publisher-v5-{}", Uuid::new_v4()),
            "127.0.0.1",
            self.v5_port,
        );
        options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = V5Client::new(options, 8);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (publish_tx, publish_rx) = oneshot::channel();
        let pump = tokio::spawn(async move {
            let mut ready_tx = Some(ready_tx);
            let mut publish_tx = Some(publish_tx);
            loop {
                match event_loop.poll().await {
                    Ok(V5Event::Incoming(V5Packet::ConnAck(_))) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Ok(()));
                        }
                    }
                    Ok(V5Event::Incoming(V5Packet::PubAck(_))) => {
                        if let Some(publish_tx) = publish_tx.take() {
                            let _ = publish_tx.send(Ok(()));
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(Err(err.to_string()));
                        }
                        if let Some(publish_tx) = publish_tx.take() {
                            let _ = publish_tx.send(Err(err.to_string()));
                        }
                        break;
                    }
                }
            }
        });

        let publish_result = async {
            match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(err))) => return Err(err),
                Ok(Err(_)) => {
                    return Err("embedded MQTT 5 publisher readiness channel closed".to_string())
                }
                Err(_) => {
                    return Err(
                        "timed out waiting for embedded MQTT 5 publisher connection".to_string()
                    )
                }
            }

            client
                .publish(scoped_topic, V5Qos::AtLeastOnce, false, payload.into())
                .await
                .map_err(|err| err.to_string())?;

            match tokio::time::timeout(Duration::from_secs(5), publish_rx).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(err))) => Err(err),
                Ok(Err(_)) => Err("embedded MQTT 5 publisher ack channel closed".to_string()),
                Err(_) => Err("timed out waiting for embedded MQTT 5 publish ack".to_string()),
            }
        }
        .await;

        let _ = client.disconnect().await;
        pump.abort();
        let _ = pump.await;
        publish_result
    }
}

async fn start_shared_broker() -> SharedEmbeddedMqttBroker {
    for attempt in 1..=EMBEDDED_MQTT_BROKER_START_RETRIES {
        let v4_port = reserve_local_port();
        let mut v5_port = reserve_local_port();
        while v5_port == v4_port {
            v5_port = reserve_local_port();
        }
        let startup_rx = spawn_embedded_broker_thread(v4_port, v5_port);
        match wait_for_tcp_listeners(&[v4_port, v5_port], startup_rx).await {
            Ok(()) => return SharedEmbeddedMqttBroker { v4_port, v5_port },
            Err(_) if attempt < EMBEDDED_MQTT_BROKER_START_RETRIES => continue,
            Err(err) => panic!("start shared embedded mqtt broker after retries: {err}"),
        }
    }

    unreachable!("embedded mqtt broker retry loop should return or panic")
}

fn spawn_embedded_broker_thread(v4_port: u16, v5_port: u16) -> std_mpsc::Receiver<String> {
    let config = embedded_broker_config(v4_port, v5_port);
    let (startup_tx, startup_rx) = std_mpsc::channel();

    thread::Builder::new()
        .name(format!("rumqttd-test-{v4_port}-{v5_port}"))
        .spawn(move || {
            let mut broker = Broker::new(config);
            let result = broker.start();
            let message = match result {
                Ok(()) => {
                    format!("embedded mqtt broker exited unexpectedly on ports {v4_port}/{v5_port}")
                }
                Err(err) => {
                    format!("start embedded mqtt broker on ports {v4_port}/{v5_port}: {err}")
                }
            };
            let _ = startup_tx.send(message);
        })
        .expect("spawn embedded mqtt broker thread");

    startup_rx
}

fn embedded_broker_config(v4_port: u16, v5_port: u16) -> Config {
    let server = |name: &str, port: u16| ServerSettings {
        name: name.to_string(),
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
    };
    Config {
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
            server("mqtt-test-v4", v4_port),
        )])),
        v5: Some(HashMap::from([(
            "test-v5".to_string(),
            server("mqtt-test-v5", v5_port),
        )])),
        ..Config::default()
    }
}

fn reserve_local_port() -> u16 {
    let listener =
        TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral tcp listener for mqtt test");
    let port = listener
        .local_addr()
        .expect("read ephemeral tcp listener address")
        .port();
    drop(listener);
    port
}

async fn wait_for_tcp_listeners(
    ports: &[u16],
    startup_rx: std_mpsc::Receiver<String>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut all_ready = true;
        for port in ports {
            if TcpStream::connect(("127.0.0.1", *port)).await.is_err() {
                all_ready = false;
                break;
            }
        }
        if all_ready {
            return Ok(());
        }

        match startup_rx.try_recv() {
            Ok(err) => return Err(err),
            Err(std_mpsc::TryRecvError::Disconnected) => {
                return Err(
                    "embedded mqtt broker thread exited before all listeners were ready"
                        .to_string(),
                )
            }
            Err(std_mpsc::TryRecvError::Empty) => {}
        }

        if Instant::now() >= deadline {
            return Err("embedded mqtt broker listeners did not start within timeout".to_string());
        }

        sleep(Duration::from_millis(20)).await;
    }
}
