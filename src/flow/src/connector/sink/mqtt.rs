//! MQTT sink connector supporting shared or standalone clients.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, QoS, Transport};
use tokio::task::JoinHandle;
use url::Url;

use crate::connector::mqtt_client::{acquire_shared_client, SharedMqttClient};

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

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| self.sink_name.clone())
    }
}

pub struct MqttSinkConnector {
    id: String,
    config: MqttSinkConfig,
    client: Option<SinkClient>,
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
                .client()
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
}

impl StandaloneMqttClient {
    async fn new(config: &MqttSinkConfig) -> Result<Self, SinkConnectorError> {
        let options = build_mqtt_options(config)?;
        let (client, event_loop) = AsyncClient::new(options, 32);
        let event_loop_handle = tokio::spawn(run_event_loop(event_loop));
        Ok(Self {
            client,
            event_loop_handle,
        })
    }

    async fn publish(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), SinkConnectorError> {
        self.client
            .publish(topic.to_string(), qos, retain, payload)
            .await
            .map_err(|err| SinkConnectorError::Other(format!("mqtt publish error: {err}")))
    }

    async fn shutdown(self) -> Result<(), SinkConnectorError> {
        self.client
            .disconnect()
            .await
            .map_err(|err| SinkConnectorError::Other(format!("mqtt disconnect error: {err}")))?;
        self.event_loop_handle.abort();
        Ok(())
    }
}

async fn run_event_loop(mut event_loop: EventLoop) {
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => {}
            Err(ConnectionError::RequestsDone) => break,
            Err(err) => {
                eprintln!("[mqtt_sink] event loop error: {err}");
                break;
            }
        }
    }
}

impl MqttSinkConnector {
    pub fn new(id: impl Into<String>, config: MqttSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
            client: None,
        }
    }

    async fn ensure_client(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }

        if let Some(connector_key) = self.config.connector_key.clone() {
            let client = acquire_shared_client(&connector_key)
                .await
                .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
            self.client = Some(SinkClient::Shared(client));
        } else {
            let standalone = StandaloneMqttClient::new(&self.config).await?;
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

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        self.ensure_client().await?;
        let qos = self.publish_qos()?;
        if let Some(client) = &self.client {
            client
                .publish(
                    &self.config.topic,
                    qos,
                    self.config.retain,
                    payload.to_vec(),
                )
                .await
        } else {
            Err(SinkConnectorError::Unavailable(format!(
                "mqtt sink `{}` not connected",
                self.id
            )))
        }
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if let Some(client) = self.client.take() {
            client.shutdown().await?;
        }
        Ok(())
    }
}

fn build_mqtt_options(config: &MqttSinkConfig) -> Result<MqttOptions, SinkConnectorError> {
    let normalized = normalize_broker_url(&config.broker_url);
    let endpoint = Url::parse(&normalized).map_err(|err| {
        SinkConnectorError::Other(format!("invalid broker URL `{}`: {err}", config.broker_url))
    })?;
    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        SinkConnectorError::Other(format!(
            "broker URL `{}` is missing a host",
            config.broker_url
        ))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "broker URL `{}` is missing a port",
                config.broker_url
            ))
        })?;

    let mut options = MqttOptions::new(config.client_id(), host, port);
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
