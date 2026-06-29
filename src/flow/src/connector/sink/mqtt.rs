//! MQTT sink connector supporting shared or standalone clients.

use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use parking_lot::RwLock;
use rumqttc::{
    AsyncClient, ClientError, ConnectionError, Event, EventLoop, MqttOptions, Packet, QoS,
    Transport,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

use crate::connector::mask_url_userinfo;
use crate::connector::mqtt_client::{MqttClientManager, SharedMqttClient};
use crate::runtime::TaskSpawner;

/// Basic MQTT configuration for sinks.
#[derive(Debug, Clone)]
pub struct MqttSinkConfig {
    pub sink_name: String,
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
    pub max_packet_size: Option<usize>,
}

impl MqttSinkConfig {
    pub fn new(
        sink_name: impl Into<String>,
        broker_url: impl Into<String>,
        topic: impl Into<String>,
        qos: u8,
    ) -> Self {
        Self {
            sink_name: sink_name.into(),
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            retain: false,
            client_id: None,
            connector_key: None,
            max_packet_size: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    pub fn with_connector_key(mut self, connector_key: impl Into<String>) -> Self {
        self.connector_key = Some(connector_key.into());
        self
    }

    pub fn with_max_packet_size(mut self, max_packet_size: usize) -> Self {
        self.max_packet_size = Some(max_packet_size);
        self
    }

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| self.sink_name.clone())
    }
}

pub(crate) struct MqttSinkConnector {
    id: String,
    flow_instance_id: Arc<str>,
    config: MqttSinkConfig,
    client: Option<SinkClient>,
    buffer: Option<Vec<u8>>,
    mqtt_clients: MqttClientManager,
    spawner: TaskSpawner,
}

enum SinkClient {
    Shared(SharedMqttClient),
    Standalone(StandaloneMqttClient),
}

impl SinkClient {
    async fn publish(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), SinkConnectorError> {
        match self {
            SinkClient::Shared(shared) => shared
                .publish(topic.to_string(), qos, retain, payload)
                .await
                .map_err(|err| SinkConnectorError::Other(format!("mqtt publish error: {err}"))),
            SinkClient::Standalone(standalone) => {
                standalone.publish(topic, qos, retain, payload).await
            }
        }
    }

    async fn shutdown(self) -> Result<(), SinkConnectorError> {
        match self {
            SinkClient::Shared(_) => Ok(()),
            SinkClient::Standalone(standalone) => standalone.shutdown().await,
        }
    }
}

struct StandaloneMqttClient {
    client: AsyncClient,
    event_loop_handle: JoinHandle<()>,
    state: MqttConnectionState,
}

#[derive(Clone, Default)]
struct MqttConnectionState {
    connected: Arc<AtomicBool>,
    last_error: Arc<RwLock<Option<String>>>,
}

impl MqttConnectionState {
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);
        if connected {
            *self.last_error.write() = None;
        }
    }

    fn set_error(&self, err: impl Into<String>) {
        self.connected.store(false, Ordering::Release);
        *self.last_error.write() = Some(err.into());
    }

    fn last_error(&self) -> Option<String> {
        self.last_error.read().clone()
    }
}

impl StandaloneMqttClient {
    async fn new(
        config: &MqttSinkConfig,
        spawner: &TaskSpawner,
    ) -> Result<Self, SinkConnectorError> {
        let options = build_mqtt_options(config)?;
        let (client, event_loop) = AsyncClient::new(options, 32);
        let state = MqttConnectionState::default();
        let event_loop_handle = spawner.spawn(run_event_loop(event_loop, state.clone()));
        Ok(Self {
            client,
            event_loop_handle,
            state,
        })
    }

    async fn publish(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), SinkConnectorError> {
        if !self.state.is_connected() {
            let message = self
                .state
                .last_error()
                .map(|err| format!("mqtt not connected: {err}"))
                .unwrap_or_else(|| "mqtt not connected".to_string());
            return Err(SinkConnectorError::Other(message));
        }

        self.client
            .publish(topic.to_string(), qos, retain, payload)
            .await
            .map_err(|err| SinkConnectorError::Other(format!("mqtt publish error: {err}")))
    }

    async fn shutdown(self) -> Result<(), SinkConnectorError> {
        match self.client.disconnect().await {
            Ok(()) => {}
            Err(ClientError::Request(_) | ClientError::TryRequest(_)) => {
                tracing::debug!("mqtt sink disconnect skipped: eventloop not available");
            }
        }
        self.event_loop_handle.abort();
        Ok(())
    }
}

async fn run_event_loop(mut event_loop: EventLoop, state: MqttConnectionState) {
    let mut backoff = std::time::Duration::from_millis(100);
    let max_backoff = std::time::Duration::from_secs(5);
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                state.set_connected(true);
                backoff = std::time::Duration::from_millis(100);
            }
            Ok(Event::Incoming(Packet::Disconnect)) => {
                tracing::warn!("mqtt sink disconnected; reconnecting");
                state.set_error("disconnect");
                event_loop.clean();
                backoff = std::time::Duration::from_millis(100);
            }
            Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => {
                state.set_connected(true);
                backoff = std::time::Duration::from_millis(100);
            }
            Err(ConnectionError::RequestsDone) => break,
            Err(err) => {
                tracing::warn!(error = %err, "mqtt sink event loop error; reconnecting");
                state.set_error(err.to_string());
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
    }
}

impl MqttSinkConnector {
    pub fn new(
        id: impl Into<String>,
        config: MqttSinkConfig,
        flow_instance_id: impl Into<Arc<str>>,
        mqtt_clients: MqttClientManager,
        spawner: TaskSpawner,
    ) -> Self {
        Self {
            id: id.into(),
            flow_instance_id: flow_instance_id.into(),
            config,
            client: None,
            buffer: None,
            mqtt_clients,
            spawner,
        }
    }

    async fn ensure_client(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }

        if let Some(connector_key) = self.config.connector_key.clone() {
            let client = self
                .mqtt_clients
                .acquire_client(&connector_key)
                .await
                .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
            tracing::info!(
                connector_id = %self.id,
                connector_key = %connector_key,
                "mqtt sink starting with shared client"
            );
            self.client = Some(SinkClient::Shared(client));
        } else {
            let standalone = StandaloneMqttClient::new(&self.config, &self.spawner).await?;
            tracing::info!(connector_id = %self.id, "mqtt sink starting standalone client");
            self.client = Some(SinkClient::Standalone(standalone));
        }
        Ok(())
    }

    fn publish_qos(&self) -> Result<QoS, SinkConnectorError> {
        match self.config.qos {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            other => Err(SinkConnectorError::Other(format!(
                "unsupported MQTT QoS level: {other}"
            ))),
        }
    }
}

#[async_trait]
impl SinkConnector for MqttSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_client().await?;
        self.buffer = Some(Vec::new());
        Ok(())
    }

    async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Err(SinkConnectorError::Other(format!(
                "mqtt sink `{}` received chunk without active delivery",
                self.id
            )));
        };
        buffer.extend_from_slice(payload);
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let payload = self.buffer.take().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "mqtt sink `{}` finished without active delivery",
                self.id
            ))
        })?;
        let bytes_written = payload.len() as u64;
        let qos = self.publish_qos()?;
        if let Some(client) = &self.client {
            veloflux_metrics::mqtt_sink_records_in_total()
                .with_label_values(&[self.flow_instance_id.as_ref(), self.id.as_str()])
                .inc();
            client
                .publish(&self.config.topic, qos, self.config.retain, payload)
                .await
                .map(|_| {
                    veloflux_metrics::mqtt_sink_records_out_total()
                        .with_label_values(&[self.flow_instance_id.as_ref(), self.id.as_str()])
                        .inc()
                })?;
            Ok(DeliveryResult { bytes_written })
        } else {
            Err(SinkConnectorError::Unavailable(format!(
                "mqtt sink `{}` not connected",
                self.id
            )))
        }
    }

    async fn abort_delivery(&mut self) {
        self.buffer = None;
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_client().await
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if let Some(client) = self.client.take() {
            client.shutdown().await?;
            tracing::info!(connector_id = %self.id, "mqtt sink closed");
        }
        Ok(())
    }
}

fn build_mqtt_options(config: &MqttSinkConfig) -> Result<MqttOptions, SinkConnectorError> {
    let normalized = normalize_broker_url(&config.broker_url);
    let endpoint = Url::parse(&normalized).map_err(|err| {
        SinkConnectorError::Other(format!(
            "invalid broker URL `{}`: {err}",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;
    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        SinkConnectorError::Other(format!(
            "broker URL `{}` is missing a host",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "broker URL `{}` is missing a port",
                mask_url_userinfo(&config.broker_url)
            ))
        })?;

    let max_packet_size = config.max_packet_size.unwrap_or(64 * 1024 * 1024);
    let mut options = MqttOptions::new(config.client_id(), host, port);
    options.set_max_packet_size(max_packet_size, max_packet_size);
    if is_tls_scheme(scheme) {
        options.set_transport(Transport::tls_with_default_config());
    }
    Ok(options)
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

fn normalize_broker_url(url: &str) -> String {
    if url.contains("://") {
        url.to_owned()
    } else {
        format!("tcp://{url}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::SinkConnector;
    use crate::model::batch_from_columns_simple;
    use crate::runtime::TaskSpawner;
    use crate::test_support::EmbeddedMqttBroker;
    use datatypes::Value;
    use tokio::runtime::Handle;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    #[tokio::test]
    async fn mqtt_sink_connector_rejects_collection_payloads() {
        let spawner = test_spawner();
        let mut connector = MqttSinkConnector::new(
            "mqtt_sink_collection_contract",
            MqttSinkConfig::new(
                "mqtt_sink_collection_contract",
                "mqtt://localhost:1883",
                "tests/mqtt/out",
                0,
            ),
            Arc::<str>::from("default"),
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );

        let batch = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "value".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("build collection payload");

        let err = connector
            .send_collection(&batch)
            .await
            .expect_err("mqtt sink should reject collection payloads");
        assert!(
            err.to_string()
                .contains("does not support collection payloads"),
            "unexpected error: {err}"
        );
    }

    // coverage-covers: sink.connector.mqtt_output
    #[tokio::test]
    async fn mqtt_sink_connector_publishes_bytes_to_embedded_broker() {
        let broker = EmbeddedMqttBroker::start().await;
        let topic = broker.scoped_topic("sink/out");

        let mut options = MqttOptions::new(
            "mqtt_sink_output_subscriber",
            "127.0.0.1",
            Url::parse(&broker.broker_url())
                .expect("parse embedded broker url")
                .port()
                .expect("embedded broker url should include port"),
        );
        options.set_keep_alive(Duration::from_secs(5));
        let (subscriber, mut event_loop) = AsyncClient::new(options, 8);
        let (connack_tx, connack_rx) = oneshot::channel();
        let (suback_tx, suback_rx) = oneshot::channel();
        let (payload_tx, mut payload_rx) = mpsc::channel::<Vec<u8>>(1);
        let subscriber_task = tokio::spawn(async move {
            let mut connack_tx = Some(connack_tx);
            let mut suback_tx = Some(suback_tx);
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        if let Some(tx) = connack_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(Event::Incoming(Packet::SubAck(_))) => {
                        if let Some(tx) = suback_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if payload_tx.send(publish.payload.to_vec()).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });

        timeout(Duration::from_secs(5), connack_rx)
            .await
            .expect("subscriber connack timeout")
            .expect("subscriber connack channel");
        subscriber
            .subscribe(topic.clone(), QoS::AtLeastOnce)
            .await
            .expect("subscribe embedded broker topic");
        timeout(Duration::from_secs(5), suback_rx)
            .await
            .expect("subscriber suback timeout")
            .expect("subscriber suback channel");

        let spawner = TaskSpawner::from_handle(Handle::current());
        let mut connector = MqttSinkConnector::new(
            "mqtt_sink_output",
            MqttSinkConfig::new("mqtt_sink_output", broker.broker_url(), topic, 1),
            Arc::<str>::from("default"),
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );
        connector.ready().await.expect("mqtt sink ready");

        let expected_payload = br#"{"a":1}"#;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match async {
                connector.start_delivery().await?;
                connector.write_chunk(expected_payload).await?;
                connector.finish_delivery().await.map(|_| ())
            }
            .await
            {
                Ok(()) => break,
                Err(err)
                    if err.to_string().contains("mqtt not connected")
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("mqtt sink publish failed: {err}"),
            }
        }

        let received = timeout(Duration::from_secs(5), payload_rx.recv())
            .await
            .expect("mqtt sink output payload timeout")
            .expect("mqtt sink output payload");
        assert_eq!(received, expected_payload);

        connector.close().await.expect("close mqtt sink connector");
        let _ = subscriber.disconnect().await;
        subscriber_task.abort();
        let _ = subscriber_task.await;
    }

    #[test]
    fn mqtt_sink_build_mqtt_options_uses_sink_local_client_and_packet_limit() {
        let config = MqttSinkConfig::new("mqtt_sink", "sink.example.com", "tests/mqtt/out", 1)
            .with_client_id("sink_client")
            .with_max_packet_size(4096);

        let options = build_mqtt_options(&config).expect("build mqtt sink options");

        assert_eq!(
            options.broker_address(),
            ("sink.example.com".to_string(), 1883)
        );
        assert_eq!(options.client_id(), "sink_client");
        assert_eq!(options.max_packet_size(), 4096);
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn mqtt_sink_build_mqtt_options_enables_tls_and_secure_default_port() {
        let config = MqttSinkConfig::new(
            "mqtt_sink",
            "mqtts://secure-sink.example.com",
            "tests/mqtt/out",
            1,
        )
        .with_max_packet_size(8192);

        let options = build_mqtt_options(&config).expect("build secure mqtt sink options");

        assert_eq!(
            options.broker_address(),
            ("secure-sink.example.com".to_string(), 8883)
        );
        assert_eq!(options.client_id(), "mqtt_sink");
        assert_eq!(options.max_packet_size(), 8192);
        assert!(matches!(options.transport(), Transport::Tls(_)));
    }
}
