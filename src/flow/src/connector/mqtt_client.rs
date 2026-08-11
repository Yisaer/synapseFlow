use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use rumqttc::Transport;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

use crate::backpressure_hub::BackpressureHub;
use crate::connector::mqtt_protocol::{
    MqttClient, MqttConnectionError, MqttEvent, MqttOptions, MqttQos,
};
use crate::connector::{mask_url_userinfo, url_has_userinfo, ConnectorError};
use crate::runtime::TaskSpawner;
use crate::secret::{SecretRef, SecretString};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMqttClientConfig {
    pub key: String,
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
    pub max_packet_size: Option<usize>,
    #[serde(default)]
    pub protocol_version: crate::connector::MqttProtocolVersion,
    /// MQTT username (non-secret identifier, plaintext). VF-51 §7.3.
    #[serde(default)]
    pub username: Option<String>,
    /// MQTT password as a config-layer reference (`store:NAME` or inline). This
    /// is what gets persisted/listed: a store ref serializes to `store:NAME`,
    /// never the resolved value; inline literals redact in Debug.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretRef>,
    /// Resolved password, injected at registration from the secret store. Never
    /// serialized (in-memory only); read by `build_mqtt_options`.
    #[serde(skip)]
    pub resolved_password: Option<SecretString>,
}

impl SharedMqttClientConfig {
    /// Resolve `password` against the secret context, populating
    /// `resolved_password`. Returns an optional warning message (no value).
    pub fn resolve_secrets(
        &mut self,
        secrets: &crate::secret::SecretContext,
    ) -> Result<Option<String>, crate::secret::SecretError> {
        match self.password.as_ref() {
            Some(reference) => {
                let (value, warning) = secrets.resolve(reference, "mqtt.password")?;
                self.resolved_password = Some(value);
                Ok(warning)
            }
            None => {
                self.resolved_password = None;
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum SharedMqttEvent {
    Payload(Vec<u8>),
    EndOfStream,
}

const MQTT_SHARED_CLIENT_ERROR_SUBSCRIBE: &str = "subscribe";
const MQTT_SHARED_CLIENT_ERROR_PUBLISH: &str = "publish";
const MQTT_SHARED_CLIENT_ERROR_DISCONNECT: &str = "disconnect";
const MQTT_SHARED_CLIENT_ERROR_REQUESTS_DONE: &str = "requests_done";
const MQTT_SHARED_CLIENT_ERROR_POLL: &str = "poll";

fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filter_levels = filter.split('/').collect::<Vec<_>>();
    let topic_levels = topic.split('/').collect::<Vec<_>>();
    let mut filter_idx = 0usize;
    let mut topic_idx = 0usize;

    while filter_idx < filter_levels.len() {
        match filter_levels[filter_idx] {
            "#" => return filter_idx == filter_levels.len() - 1,
            "+" => {
                if topic_idx >= topic_levels.len() {
                    return false;
                }
                filter_idx += 1;
                topic_idx += 1;
            }
            level => {
                if topic_idx >= topic_levels.len() || topic_levels[topic_idx] != level {
                    return false;
                }
                filter_idx += 1;
                topic_idx += 1;
            }
        }
    }

    topic_idx == topic_levels.len()
}

pub struct SharedMqttClient {
    entry: Arc<MqttClientEntry>,
    manager: MqttClientManager,
}

impl SharedMqttClient {
    fn from_acquired(entry: Arc<MqttClientEntry>, manager: MqttClientManager) -> Self {
        Self { entry, manager }
    }

    pub async fn subscribe(
        &self,
    ) -> Result<mpsc::Receiver<Result<SharedMqttEvent, ConnectorError>>, ConnectorError> {
        self.entry.subscribe().await
    }

    pub fn is_connected(&self) -> bool {
        self.entry.connected.load(Ordering::Acquire)
    }

    pub fn protocol_version(&self) -> crate::connector::MqttProtocolVersion {
        self.entry.config.protocol_version
    }

    pub async fn publish(
        &self,
        topic: String,
        qos: rumqttc::QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), ConnectorError> {
        self.publish_with_user_properties(topic, qos.into(), retain, payload, &[])
            .await
    }

    pub(crate) async fn publish_with_user_properties(
        &self,
        topic: String,
        qos: MqttQos,
        retain: bool,
        payload: Vec<u8>,
        user_properties: &[crate::connector::MqttUserProperty],
    ) -> Result<(), ConnectorError> {
        if !self.is_connected() {
            self.entry
                .record_error_metric(MQTT_SHARED_CLIENT_ERROR_PUBLISH);
            let last_error = self.entry.last_error.read().clone();
            let message = last_error
                .map(|err| format!("mqtt not connected: {err}"))
                .unwrap_or_else(|| "mqtt not connected".to_string());
            return Err(ConnectorError::Connection(message));
        }

        let client = self.entry.client().await?;
        let publish_result = if user_properties.is_empty() {
            client.publish(topic, qos, retain, payload).await
        } else {
            client
                .publish_with_user_properties(topic, qos, retain, payload, user_properties)
                .await
        };
        publish_result
            .map(|_| self.entry.record_tx_message_metric())
            .map_err(|err| {
                self.entry
                    .record_error_metric(MQTT_SHARED_CLIENT_ERROR_PUBLISH);
                ConnectorError::Connection(err.to_string())
            })
    }
}

impl Drop for SharedMqttClient {
    fn drop(&mut self) {
        self.manager.release(&self.entry);
    }
}

struct MqttClientRuntime {
    client: MqttClient,
    events_hub: Arc<BackpressureHub<Result<SharedMqttEvent, ConnectorError>>>,
    shutdown_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

struct MqttClientEntry {
    flow_instance_id: Arc<str>,
    config: SharedMqttClientConfig,
    runtime: AsyncMutex<Option<MqttClientRuntime>>,
    ref_count: AtomicUsize,
    closing: AtomicBool,
    connected: AtomicBool,
    last_error: RwLock<Option<String>>,
    spawner: TaskSpawner,
}

impl MqttClientEntry {
    fn new(
        flow_instance_id: Arc<str>,
        config: SharedMqttClientConfig,
        spawner: TaskSpawner,
    ) -> Self {
        Self {
            flow_instance_id,
            config,
            runtime: AsyncMutex::new(None),
            ref_count: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            last_error: RwLock::new(None),
            spawner,
        }
    }

    fn metric_labels(&self) -> [&str; 2] {
        [self.flow_instance_id.as_ref(), self.config.key.as_str()]
    }

    fn set_connected_metric(&self, connected: bool) {
        let value = if connected { 1 } else { 0 };
        veloflux_metrics::mqtt_shared_client_connected()
            .with_label_values(&self.metric_labels())
            .set(value);
    }

    fn set_active_refs_metric(&self, refs: usize) {
        let value = i64::try_from(refs).unwrap_or(i64::MAX);
        veloflux_metrics::mqtt_shared_client_active_refs()
            .with_label_values(&self.metric_labels())
            .set(value);
    }

    fn record_error_metric(&self, kind: &'static str) {
        veloflux_metrics::mqtt_shared_client_errors_total()
            .with_label_values(&[
                self.flow_instance_id.as_ref(),
                self.config.key.as_str(),
                kind,
            ])
            .inc();
    }

    fn record_rx_message_metric(&self) {
        veloflux_metrics::mqtt_shared_client_rx_messages_total()
            .with_label_values(&self.metric_labels())
            .inc();
    }

    fn record_tx_message_metric(&self) {
        veloflux_metrics::mqtt_shared_client_tx_messages_total()
            .with_label_values(&self.metric_labels())
            .inc();
    }

    fn record_reconnect_metric(&self) {
        veloflux_metrics::mqtt_shared_client_reconnects_total()
            .with_label_values(&self.metric_labels())
            .inc();
    }

    async fn subscribe(
        self: &Arc<Self>,
    ) -> Result<mpsc::Receiver<Result<SharedMqttEvent, ConnectorError>>, ConnectorError> {
        self.ensure_runtime_started().await?;
        let hub = {
            let runtime = self.runtime.lock().await;
            runtime
                .as_ref()
                .map(|runtime| Arc::clone(&runtime.events_hub))
                .ok_or_else(|| {
                    ConnectorError::ResourceBusy(
                        "shared mqtt client runtime is unavailable".to_string(),
                    )
                })?
        };
        hub.subscribe()
            .await
            .map_err(|_| ConnectorError::ResourceBusy("shared mqtt client is closed".to_string()))
    }

    async fn client(self: &Arc<Self>) -> Result<MqttClient, ConnectorError> {
        self.ensure_runtime_started().await?;
        let runtime = self.runtime.lock().await;
        runtime
            .as_ref()
            .map(|runtime| runtime.client.clone())
            .ok_or_else(|| {
                ConnectorError::ResourceBusy(
                    "shared mqtt client runtime is unavailable".to_string(),
                )
            })
    }

    async fn ensure_runtime_started(self: &Arc<Self>) -> Result<(), ConnectorError> {
        let mut runtime = self.runtime.lock().await;
        if runtime.is_some() {
            return Ok(());
        }

        *runtime = Some(self.start_runtime().await?);
        Ok(())
    }

    async fn start_runtime(self: &Arc<Self>) -> Result<MqttClientRuntime, ConnectorError> {
        let config = self.config.clone();
        let mut options = build_mqtt_options(&config)?;
        options.set_keep_alive(Duration::from_secs(30));

        let qos = map_qos(config.qos)?;
        let topic = config.topic.clone();

        let (client, mut event_loop) = MqttClient::new(options, 64);
        client.subscribe(topic.clone(), qos).await.map_err(|err| {
            self.record_error_metric(MQTT_SHARED_CLIENT_ERROR_SUBSCRIBE);
            ConnectorError::Connection(err.to_string())
        })?;

        let events_hub = Arc::new(BackpressureHub::new(1024));
        let (shutdown_tx, _) = watch::channel(false);

        let entry_for_task = Arc::clone(self);
        let mut shutdown_rx = shutdown_tx.subscribe();
        let events_hub_for_task = Arc::clone(&events_hub);
        let client_for_task = client.clone();
        let handle = self.spawner.spawn(async move {
            let mut backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(5);
            let mut seen_connected = false;

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    event = event_loop.poll() => match event {
                        Ok(MqttEvent::Publish { topic: publish_topic, payload }) => {
                            entry_for_task.connected.store(true, Ordering::Release);
                            backoff = Duration::from_millis(100);
                            if mqtt_topic_matches(&topic, &publish_topic) {
                                entry_for_task.record_rx_message_metric();
                                let _ = events_hub_for_task
                                    .send(Ok(SharedMqttEvent::Payload(payload.to_vec())))
                                    .await;
                            }
                        }
                        Ok(MqttEvent::Disconnected) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            entry_for_task.set_connected_metric(false);
                            entry_for_task.record_error_metric(MQTT_SHARED_CLIENT_ERROR_DISCONNECT);
                            *entry_for_task.last_error.write() = Some("disconnect".to_string());
                            tracing::warn!("shared mqtt client disconnected; reconnecting");
                            event_loop.clean();
                        }
                        Ok(MqttEvent::Connected) => {
                            let was_connected = entry_for_task.connected.swap(true, Ordering::AcqRel);
                            entry_for_task.set_connected_metric(true);
                            if seen_connected && !was_connected {
                                entry_for_task.record_reconnect_metric();
                            }
                            seen_connected = true;
                            *entry_for_task.last_error.write() = None;
                            backoff = Duration::from_millis(100);
                            let _ = client_for_task.subscribe(topic.clone(), qos).await;
                        }
                        Ok(MqttEvent::Other) => {}
                        Err(MqttConnectionError::RequestsDone) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            entry_for_task.set_connected_metric(false);
                            if entry_for_task.closing.load(Ordering::Acquire) {
                                break;
                            }
                            entry_for_task.record_error_metric(MQTT_SHARED_CLIENT_ERROR_REQUESTS_DONE);
                            let message = "mqtt requests done".to_string();
                            *entry_for_task.last_error.write() = Some(message.clone());
                            let _ = events_hub_for_task
                                .send(Err(ConnectorError::Connection(message)))
                                .await;
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                        Err(MqttConnectionError::Connection(err)) => {
                            entry_for_task.connected.store(false, Ordering::Release);
                            entry_for_task.set_connected_metric(false);
                            entry_for_task.record_error_metric(MQTT_SHARED_CLIENT_ERROR_POLL);
                            *entry_for_task.last_error.write() = Some(err.clone());
                            let _ = events_hub_for_task
                                .send(Err(ConnectorError::Connection(err)))
                                .await;
                            sleep(backoff).await;
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                    }
                }
            }
        });

        Ok(MqttClientRuntime {
            client,
            events_hub,
            shutdown_tx,
            join_handle: handle,
        })
    }

    async fn shutdown_runtime(self: &Arc<Self>, emit_terminal: bool) {
        let runtime = {
            let mut runtime = self.runtime.lock().await;
            if self.ref_count.load(Ordering::Acquire) > 0 {
                return;
            }
            if emit_terminal {
                if !self.closing.load(Ordering::Acquire) {
                    return;
                }
            } else if self.closing.load(Ordering::Acquire) {
                return;
            }
            runtime.take()
        };
        let Some(runtime) = runtime else {
            return;
        };

        let _ = runtime.shutdown_tx.send(true);
        let _ = runtime.client.disconnect().await;
        runtime.join_handle.abort();
        let _ = runtime.join_handle.await;
        self.connected.store(false, Ordering::Release);
        self.set_connected_metric(false);
        if emit_terminal {
            runtime
                .events_hub
                .close(Some(Ok(SharedMqttEvent::EndOfStream)))
                .await;
        } else {
            runtime.events_hub.close(None).await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct MqttClientManager {
    flow_instance_id: Arc<str>,
    entries: Arc<Mutex<HashMap<String, Arc<MqttClientEntry>>>>,
    spawner: TaskSpawner,
}

impl MqttClientManager {
    pub(crate) fn new(flow_instance_id: impl Into<Arc<str>>, spawner: TaskSpawner) -> Self {
        Self {
            flow_instance_id: flow_instance_id.into(),
            entries: Arc::new(Mutex::new(HashMap::new())),
            spawner,
        }
    }

    pub(crate) async fn create_client(
        &self,
        config: SharedMqttClientConfig,
    ) -> Result<(), ConnectorError> {
        validate_shared_mqtt_client_config(&config)?;

        let key = config.key.clone();
        {
            if self.entries.lock().contains_key(&key) {
                return Err(ConnectorError::AlreadyExists(key));
            }
        }

        let entry = Arc::new(MqttClientEntry::new(
            Arc::clone(&self.flow_instance_id),
            config,
            self.spawner.clone(),
        ));
        entry.set_connected_metric(false);
        entry.set_active_refs_metric(0);

        self.entries.lock().insert(key, entry);
        Ok(())
    }

    pub(crate) async fn acquire_client(
        &self,
        key: &str,
    ) -> Result<SharedMqttClient, ConnectorError> {
        let entry = {
            let guard = self.entries.lock();
            let entry = guard
                .get(key)
                .cloned()
                .ok_or_else(|| ConnectorError::NotFound(key.to_string()))?;
            let refs = entry.ref_count.fetch_add(1, Ordering::AcqRel) + 1;
            entry.set_active_refs_metric(refs);
            entry
        };
        if let Err(err) = entry.ensure_runtime_started().await {
            let refs = entry.ref_count.fetch_sub(1, Ordering::AcqRel) - 1;
            entry.set_active_refs_metric(refs);
            return Err(err);
        }
        Ok(SharedMqttClient::from_acquired(entry, self.clone()))
    }

    pub(crate) fn drop_client(&self, key: &str) -> Result<(), ConnectorError> {
        let entry = self
            .entries
            .lock()
            .remove(key)
            .ok_or_else(|| ConnectorError::NotFound(key.to_string()))?;

        entry.closing.store(true, Ordering::Release);
        if entry.ref_count.load(Ordering::Acquire) == 0 {
            let shutdown_entry = Arc::clone(&entry);
            self.spawner.spawn(async move {
                shutdown_entry.shutdown_runtime(true).await;
            });
        }
        Ok(())
    }

    fn release(&self, entry: &Arc<MqttClientEntry>) {
        let previous = entry.ref_count.fetch_sub(1, Ordering::AcqRel);
        let refs = previous.saturating_sub(1);
        entry.set_active_refs_metric(refs);
        if previous == 1 {
            let emit_terminal = entry.closing.load(Ordering::Acquire);
            let shutdown_entry = Arc::clone(entry);
            self.spawner.spawn(async move {
                shutdown_entry.shutdown_runtime(emit_terminal).await;
            });
        }
    }

    #[cfg(test)]
    pub(crate) async fn test_runtime_started(&self, key: &str) -> bool {
        let entry = {
            let guard = self.entries.lock();
            guard.get(key).cloned()
        };
        let Some(entry) = entry else {
            return false;
        };
        let started = entry.runtime.lock().await.is_some();
        started
    }
}

fn validate_shared_mqtt_client_config(
    config: &SharedMqttClientConfig,
) -> Result<(), ConnectorError> {
    let _ = build_mqtt_options(config)?;
    let _ = map_qos(config.qos)?;
    Ok(())
}

fn normalize_broker_url(url: &str) -> String {
    if url.contains("://") {
        url.to_owned()
    } else {
        format!("tcp://{url}")
    }
}

fn build_mqtt_options(config: &SharedMqttClientConfig) -> Result<MqttOptions, ConnectorError> {
    let normalized = normalize_broker_url(&config.broker_url);
    // Credentials must live in `username`/`password`, never in the URL. A
    // userinfo-bearing broker_url is rejected as invalid config (VF-51 §7.3) so
    // it cannot land in a resource manifest / redb in the first place.
    if url_has_userinfo(&normalized) {
        return Err(ConnectorError::Connection(format!(
            "broker URL `{}` must not embed credentials; use the `username`/`password` fields",
            mask_url_userinfo(&config.broker_url)
        )));
    }
    let endpoint = Url::parse(&normalized).map_err(|err| {
        ConnectorError::Connection(format!(
            "invalid broker URL `{}`: {err}",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;

    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        ConnectorError::Connection(format!(
            "broker URL `{}` missing host",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            ConnectorError::Connection(format!(
                "broker URL `{}` missing port",
                mask_url_userinfo(&config.broker_url)
            ))
        })?;

    let max_packet_size = config.max_packet_size.unwrap_or(64 * 1024 * 1024);
    let mut options = MqttOptions::new(
        config.protocol_version,
        config.client_id.clone(),
        host,
        port,
        max_packet_size,
    )
    .map_err(ConnectorError::Connection)?;

    // Wire MQTT credentials (previously dropped). Password is already resolved
    // from the secret store before reaching the runtime config.
    if let Some(username) = config.username.as_ref().filter(|u| !u.is_empty()) {
        let password = config
            .resolved_password
            .as_ref()
            .map(|p| p.expose().to_string())
            .unwrap_or_default();
        options.set_credentials(username.clone(), password);
    }

    if is_tls_scheme(scheme) {
        options.set_transport(Transport::tls_with_default_config());
    }

    Ok(options)
}

fn map_qos(qos: u8) -> Result<MqttQos, ConnectorError> {
    MqttQos::try_from(qos).map_err(ConnectorError::Other)
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

#[cfg(test)]
mod tests {
    use super::{
        build_mqtt_options, mqtt_topic_matches, MqttClientManager, SharedMqttClientConfig,
        SharedMqttEvent,
    };
    use crate::connector::mqtt_protocol::MqttQos;
    use crate::connector::ConnectorError;
    use crate::runtime::TaskSpawner;
    use crate::test_support::EmbeddedMqttBroker;
    use rumqttc::{QoS, Transport};
    use tokio::runtime::Handle;
    use tokio::time::{sleep, timeout, Duration};

    #[test]
    fn mqtt_topic_match_exact() {
        assert!(mqtt_topic_matches("topic/123", "topic/123"));
        assert!(!mqtt_topic_matches("topic/123", "topic/456"));
    }

    #[test]
    fn mqtt_topic_match_single_level_wildcard() {
        assert!(mqtt_topic_matches("topic/+", "topic/123"));
        assert!(!mqtt_topic_matches("topic/+", "topic/123/456"));
        assert!(!mqtt_topic_matches("topic/+", "topic"));
    }

    #[test]
    fn mqtt_topic_match_multi_level_wildcard() {
        assert!(mqtt_topic_matches("topic/#", "topic"));
        assert!(mqtt_topic_matches("topic/#", "topic/123"));
        assert!(mqtt_topic_matches("topic/#", "topic/123/456"));
        assert!(!mqtt_topic_matches("topic/#", "other/123"));
    }

    #[test]
    fn mqtt_topic_match_mixed_wildcards() {
        assert!(mqtt_topic_matches(
            "fleet/+/telemetry/#",
            "fleet/car_a/telemetry"
        ));
        assert!(mqtt_topic_matches(
            "fleet/+/telemetry/#",
            "fleet/car_a/telemetry/speed"
        ));
        assert!(!mqtt_topic_matches(
            "fleet/+/telemetry/#",
            "fleet/car_a/state/speed"
        ));
    }

    #[tokio::test]
    async fn drop_client_removes_busy_client_from_registry() {
        let broker = EmbeddedMqttBroker::start().await;
        let manager =
            MqttClientManager::new("default", TaskSpawner::from_handle(Handle::current()));
        let topic_filter = broker.scoped_filter("fleet/+/telemetry");
        let cfg = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: broker.broker_url(),
            topic: topic_filter,
            client_id: "client_shared".to_string(),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared mqtt client");
        let held = manager
            .acquire_client("shared")
            .await
            .expect("acquire shared mqtt client");

        manager
            .drop_client("shared")
            .expect("best-effort drop busy shared mqtt client");

        assert!(matches!(
            manager.acquire_client("shared").await,
            Err(ConnectorError::NotFound(_))
        ));

        drop(held);
    }

    // coverage-covers: source.shared_mqtt_client.lifecycle
    #[tokio::test]
    async fn shared_mqtt_runtime_is_lazy_started_and_reclaimed_when_last_reference_drops() {
        let broker = EmbeddedMqttBroker::start().await;
        let manager =
            MqttClientManager::new("default", TaskSpawner::from_handle(Handle::current()));
        let topic_filter = broker.scoped_filter("fleet/+/telemetry");
        let cfg = SharedMqttClientConfig {
            key: "lazy".to_string(),
            broker_url: broker.broker_url(),
            topic: topic_filter,
            client_id: "client_lazy".to_string(),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared mqtt client");
        assert!(
            !manager.test_runtime_started("lazy").await,
            "runtime should not start before first acquisition"
        );

        let held = manager
            .acquire_client("lazy")
            .await
            .expect("acquire shared mqtt client");
        assert!(
            manager.test_runtime_started("lazy").await,
            "runtime should start on first acquisition"
        );

        drop(held);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if !manager.test_runtime_started("lazy").await {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared mqtt runtime did not stop after last reference dropped"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn shared_mqtt_subscribers_receive_payloads_from_embedded_broker() {
        let broker = EmbeddedMqttBroker::start().await;
        let manager =
            MqttClientManager::new("default", TaskSpawner::from_handle(Handle::current()));
        let topic_filter = broker.scoped_filter("fleet/+/telemetry");
        let cfg = SharedMqttClientConfig {
            key: "embedded".to_string(),
            broker_url: broker.broker_url(),
            topic: topic_filter,
            client_id: "client_embedded".to_string(),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared mqtt client");
        let held = manager
            .acquire_client("embedded")
            .await
            .expect("acquire shared mqtt client");
        let mut events = held
            .subscribe()
            .await
            .expect("subscribe shared mqtt events");
        sleep(Duration::from_millis(300)).await;

        broker
            .publish("fleet/device_a/telemetry", b"first-payload".to_vec())
            .await
            .expect("publish first embedded mqtt payload");
        broker
            .publish("fleet/device_b/telemetry", b"second-payload".to_vec())
            .await
            .expect("publish second embedded mqtt payload");

        let first = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("recv first payload timeout")
            .expect("first payload event should exist");
        let second = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("recv second payload timeout")
            .expect("second payload event should exist");

        match first {
            Ok(SharedMqttEvent::Payload(payload)) => assert_eq!(payload, b"first-payload".to_vec()),
            other => panic!("expected first payload event, got {other:?}"),
        }
        match second {
            Ok(SharedMqttEvent::Payload(payload)) => {
                assert_eq!(payload, b"second-payload".to_vec());
            }
            other => panic!("expected second payload event, got {other:?}"),
        }

        held.publish(
            broker.scoped_topic("fleet/device_c/telemetry"),
            QoS::AtMostOnce,
            false,
            b"published-through-shared-client".to_vec(),
        )
        .await
        .expect("publish through shared mqtt client");

        drop(held);
    }

    // coverage-covers: source.shared_mqtt_client.v5_runtime
    #[tokio::test]
    async fn shared_mqtt_v5_client_receives_and_publishes_payloads() {
        let broker = EmbeddedMqttBroker::start().await;
        let manager =
            MqttClientManager::new("default", TaskSpawner::from_handle(Handle::current()));
        let topic_filter = broker.scoped_filter("fleet/+/telemetry");
        let cfg = SharedMqttClientConfig {
            key: "embedded_v5".to_string(),
            broker_url: broker.broker_url_v5(),
            topic: topic_filter,
            client_id: "client_embedded_v5".to_string(),
            qos: 1,
            max_packet_size: None,
            protocol_version: crate::connector::MqttProtocolVersion::V5,
            username: None,
            password: None,
            resolved_password: None,
        };

        manager
            .create_client(cfg)
            .await
            .expect("create shared MQTT 5 client");
        let held = manager
            .acquire_client("embedded_v5")
            .await
            .expect("acquire shared MQTT 5 client");
        let mut events = held
            .subscribe()
            .await
            .expect("subscribe shared MQTT 5 events");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !held.is_connected() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared MQTT 5 client did not connect"
            );
            sleep(Duration::from_millis(20)).await;
        }

        broker
            .publish_v5("fleet/device_a/telemetry", b"from-mqtt-v5-broker".to_vec())
            .await
            .expect("publish inbound MQTT 5 payload");
        let inbound = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("receive inbound MQTT 5 payload timeout")
            .expect("inbound MQTT 5 event should exist");
        match inbound {
            Ok(SharedMqttEvent::Payload(payload)) => {
                assert_eq!(payload, b"from-mqtt-v5-broker".to_vec());
            }
            other => panic!("expected inbound MQTT 5 payload, got {other:?}"),
        }

        let user_properties = vec![crate::connector::MqttUserProperty {
            key: "source".to_string(),
            value: "shared-client".to_string(),
        }];
        held.publish_with_user_properties(
            broker.scoped_topic("fleet/device_b/telemetry"),
            MqttQos::Qos1,
            false,
            b"through-shared-mqtt-v5".to_vec(),
            &user_properties,
        )
        .await
        .expect("publish through shared MQTT 5 client");
        let outbound = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("receive shared MQTT 5 published payload timeout")
            .expect("shared MQTT 5 published event should exist");
        match outbound {
            Ok(SharedMqttEvent::Payload(payload)) => {
                assert_eq!(payload, b"through-shared-mqtt-v5".to_vec());
            }
            other => panic!("expected shared MQTT 5 published payload, got {other:?}"),
        }

        drop(held);
    }

    #[test]
    fn shared_mqtt_build_mqtt_options_uses_shared_client_packet_limit() {
        let config = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: "shared.example.com".to_string(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "shared_client".to_string(),
            qos: 1,
            max_packet_size: Some(16384),
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };

        let options = build_mqtt_options(&config).expect("build shared mqtt options");

        assert_eq!(
            options.broker_address(),
            ("shared.example.com".to_string(), 1883)
        );
        assert_eq!(options.client_id(), "shared_client");
        assert_eq!(options.max_packet_size(), 16384);
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn shared_mqtt_build_mqtt_options_enables_tls_and_secure_default_port() {
        let config = SharedMqttClientConfig {
            key: "shared_tls".to_string(),
            broker_url: "mqtts://secure-shared.example.com".to_string(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: "shared_tls_client".to_string(),
            qos: 1,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };

        let options = build_mqtt_options(&config).expect("build secure shared mqtt options");

        assert_eq!(
            options.broker_address(),
            ("secure-shared.example.com".to_string(), 8883)
        );
        assert_eq!(options.client_id(), "shared_tls_client");
        assert_eq!(options.max_packet_size(), 64 * 1024 * 1024);
        assert!(matches!(options.transport(), Transport::Tls(_)));
    }

    #[test]
    fn build_mqtt_options_rejects_broker_url_userinfo() {
        let config = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: "mqtt://alice:s3cr3t@broker.example.com:1883".to_string(),
            topic: "t".to_string(),
            client_id: "c".to_string(),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };
        let err = build_mqtt_options(&config)
            .err()
            .expect("userinfo-bearing broker URL should fail")
            .to_string();
        assert!(err.contains("must not embed credentials"), "{err}");
        assert!(!err.contains("s3cr3t"), "error leaked password: {err}");
    }

    #[test]
    fn resolve_secrets_populates_resolved_password() {
        use crate::secret::{SecretContext, SecretPolicy, SecretRef, SecretStore};

        let mut store = SecretStore::empty();
        store.set("mqtt-pass", "s3cr3t");
        let ctx = SecretContext::new(std::sync::Arc::new(store), SecretPolicy::Warn);

        let mut config = SharedMqttClientConfig {
            key: "shared".to_string(),
            broker_url: "broker.example.com".to_string(),
            topic: "t".to_string(),
            client_id: "c".to_string(),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: Some("alice".to_string()),
            password: Some(SecretRef::store("mqtt-pass")),
            resolved_password: None,
        };
        let warning = config.resolve_secrets(&ctx).expect("resolve");
        assert!(warning.is_none());
        assert_eq!(
            config.resolved_password.as_ref().map(|s| s.expose()),
            Some("s3cr3t")
        );
        // Persisted form keeps the pointer, never the value.
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("store:mqtt-pass"));
        assert!(!json.contains("s3cr3t"));
    }
}
