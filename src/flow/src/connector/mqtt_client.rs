use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use once_cell::sync::Lazy;
use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, Packet, QoS, Transport,
};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use url::Url;

use crate::connector::ConnectorError;

static MQTT_CLIENT_MANAGER: Lazy<MqttClientManager> = Lazy::new(MqttClientManager::new);

#[derive(Debug, Clone)]
pub struct SharedMqttClientConfig {
    pub key: String,
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

#[derive(Debug, Clone)]
pub enum SharedMqttEvent {
    Payload(Vec<u8>),
    EndOfStream,
}

pub struct SharedMqttClient {
    key: String,
    entry: Arc<MqttClientEntry>,
}

impl SharedMqttClient {
    fn new(key: String, entry: Arc<MqttClientEntry>) -> Self {
        entry.ref_count.fetch_add(1, Ordering::AcqRel);
        Self { key, entry }
    }

    pub fn client(&self) -> AsyncClient {
        self.entry.client.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Result<SharedMqttEvent, ConnectorError>> {
        self.entry.events_tx.subscribe()
    }
}

impl Drop for SharedMqttClient {
    fn drop(&mut self) {
        MQTT_CLIENT_MANAGER.release(&self.key);
    }
}

struct MqttClientEntry {
    client: AsyncClient,
    events_tx: broadcast::Sender<Result<SharedMqttEvent, ConnectorError>>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    ref_count: AtomicUsize,
}

impl MqttClientEntry {
    async fn new(config: &SharedMqttClientConfig) -> Result<(Self, EventLoop), ConnectorError> {
        let mut options = build_mqtt_options(config)?;
        options.set_keep_alive(Duration::from_secs(30));

        let qos = map_qos(config.qos)?;

        let (client, event_loop) = AsyncClient::new(options, 64);
        client
            .subscribe(config.topic.clone(), qos)
            .await
            .map_err(|err| ConnectorError::Connection(err.to_string()))?;

        let (events_tx, _) = broadcast::channel(1024);
        let (shutdown_tx, _) = watch::channel(false);

        Ok((
            Self {
                client,
                events_tx,
                shutdown_tx,
                join_handle: Mutex::new(None),
                ref_count: AtomicUsize::new(0),
            },
            event_loop,
        ))
    }

    fn start_event_loop(entry: &Arc<MqttClientEntry>, mut event_loop: EventLoop, topic: String) {
        let mut shutdown_rx = entry.shutdown_tx.subscribe();
        let events_tx = entry.events_tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    event = event_loop.poll() => match event {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            if publish.topic == topic {
                                let _ = events_tx.send(Ok(SharedMqttEvent::Payload(publish.payload.to_vec())));
                            }
                        }
                        Ok(Event::Incoming(Packet::Disconnect)) => {
                            let _ = events_tx.send(Ok(SharedMqttEvent::EndOfStream));
                            break;
                        }
                        Ok(_) => {}
                        Err(ConnectionError::RequestsDone) => {
                            let _ = events_tx.send(Ok(SharedMqttEvent::EndOfStream));
                            break;
                        }
                        Err(err) => {
                            let _ = events_tx.send(Err(ConnectorError::Connection(err.to_string())));
                            break;
                        }
                    }
                }
            }
        });

        *entry.join_handle.lock().unwrap() = Some(handle);
    }

    fn spawn_shutdown(entry: Arc<Self>) {
        tokio::spawn(async move {
            let _ = entry.shutdown_tx.send(true);
            let _ = entry.client.disconnect().await;
            if let Some(handle) = entry.join_handle.lock().unwrap().take() {
                handle.abort();
            }
            let _ = entry.events_tx.send(Ok(SharedMqttEvent::EndOfStream));
        });
    }
}

pub struct MqttClientManager {
    entries: Mutex<HashMap<String, Arc<MqttClientEntry>>>,
}

impl MqttClientManager {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    async fn create(&self, config: SharedMqttClientConfig) -> Result<(), ConnectorError> {
        let key = config.key.clone();
        {
            if self.entries.lock().unwrap().contains_key(&key) {
                return Err(ConnectorError::AlreadyExists(key));
            }
        }

        let (entry, event_loop) = MqttClientEntry::new(&config).await?;
        let entry = Arc::new(entry);
        MqttClientEntry::start_event_loop(&entry, event_loop, config.topic.clone());

        self.entries.lock().unwrap().insert(key, entry);
        Ok(())
    }

    async fn acquire(&self, key: &str) -> Result<SharedMqttClient, ConnectorError> {
        if let Some(entry) = self.entries.lock().unwrap().get(key).cloned() {
            return Ok(SharedMqttClient::new(key.to_string(), entry));
        }
        Err(ConnectorError::NotFound(key.to_string()))
    }

    fn drop_client(&self, key: &str) -> Result<(), ConnectorError> {
        let mut guard = self.entries.lock().unwrap();
        let entry = guard
            .get(key)
            .cloned()
            .ok_or_else(|| ConnectorError::NotFound(key.to_string()))?;

        if entry.ref_count.load(Ordering::Acquire) > 0 {
            return Err(ConnectorError::ResourceBusy(key.to_string()));
        }

        guard.remove(key);
        MqttClientEntry::spawn_shutdown(entry);
        Ok(())
    }

    fn release(&self, key: &str) {
        if let Some(entry) = self.entries.lock().unwrap().get(key) {
            entry.ref_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

pub async fn create_shared_client(config: SharedMqttClientConfig) -> Result<(), ConnectorError> {
    MQTT_CLIENT_MANAGER.create(config).await
}

pub async fn acquire_shared_client(key: &str) -> Result<SharedMqttClient, ConnectorError> {
    MQTT_CLIENT_MANAGER.acquire(key).await
}

pub fn drop_shared_client(key: &str) -> Result<(), ConnectorError> {
    MQTT_CLIENT_MANAGER.drop_client(key)
}

fn normalize_broker_url(url: &str) -> String {
    if url.contains("://") {
        url.to_owned()
    } else {
        format!("tcp://{url}")
    }
}

fn build_mqtt_options(config: &SharedMqttClientConfig) -> Result<MqttOptions, ConnectorError> {
    let endpoint = Url::parse(&normalize_broker_url(&config.broker_url)).map_err(|err| {
        ConnectorError::Connection(format!("invalid broker URL `{}`: {err}", config.broker_url))
    })?;

    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        ConnectorError::Connection(format!("broker URL `{}` missing host", config.broker_url))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            ConnectorError::Connection(format!("broker URL `{}` missing port", config.broker_url))
        })?;

    let mut options = MqttOptions::new(config.client_id.clone(), host, port);

    if is_tls_scheme(scheme) {
        options.set_transport(Transport::tls_with_default_config());
    }

    Ok(options)
}

fn map_qos(qos: u8) -> Result<QoS, ConnectorError> {
    match qos {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        other => Err(ConnectorError::Other(format!(
            "unsupported MQTT QoS level: {other}"
        ))),
    }
}

fn default_port_for_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "mqtt" | "tcp" => Some(1883),
        "mqtts" | "ssl" | "tcps" => Some(8883),
        _ => None,
    }
}

fn is_tls_scheme(scheme: &str) -> bool {
    matches!(scheme, "mqtts" | "ssl" | "tcps")
}
