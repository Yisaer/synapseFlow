//! MQTT connector backed by the pure-Rust `rumqttc` async client.

use std::time::Duration;

use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS, Transport};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};

/// Basic MQTT configuration.
#[derive(Debug, Clone)]
pub struct MqttSourceConfig {
    /// Logical identifier for the upstream MQTT source.
    pub source_name: String,
    /// Broker endpoint (e.g., `tcp://localhost:1883`).
    pub broker_url: String,
    /// Topic to subscribe to.
    pub topic: String,
    /// Requested QoS level.
    pub qos: u8,
    /// Optional MQTT client id. Defaults to `source_name` when omitted.
    pub client_id: Option<String>,
}

impl MqttSourceConfig {
    pub fn new(
        source_name: impl Into<String>,
        broker_url: impl Into<String>,
        topic: impl Into<String>,
        qos: u8,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            broker_url: broker_url.into(),
            topic: topic.into(),
            qos,
            client_id: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| self.source_name.clone())
    }
}

/// Connector that subscribes to an MQTT topic using an async client.
pub struct MqttSourceConnector {
    id: String,
    config: MqttSourceConfig,
    receiver: Option<mpsc::Receiver<Result<ConnectorEvent, ConnectorError>>>,
}

impl MqttSourceConnector {
    /// Create a new MQTT connector.
    pub fn new(id: impl Into<String>, config: MqttSourceConfig) -> Self {
        Self {
            id: id.into(),
            config,
            receiver: None,
        }
    }

    fn spawn_worker(
        config: MqttSourceConfig,
        sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) {
        tokio::spawn(async move {
            if let Err(err) = Self::run_event_loop(config, sender.clone()).await {
                let _ = sender.send(Err(err)).await;
            }
        });
    }

    async fn run_event_loop(
        config: MqttSourceConfig,
        sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let mut mqtt_options = Self::build_mqtt_options(&config)?;
        mqtt_options.set_keep_alive(Duration::from_secs(30));

        let qos = Self::map_qos(config.qos)?;
        let topic = config.topic.clone();

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 32);

        client
            .subscribe(topic, qos)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        loop {
            match event_loop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let payload = publish.payload.to_vec();
                    if sender
                        .send(Ok(ConnectorEvent::Payload(payload)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    break;
                }
                Ok(_) => {}
                Err(ConnectionError::RequestsDone) => break,
                Err(err) => {
                    return Err(ConnectorError::Connection(err.to_string()));
                }
            }
        }

        let _ = client.disconnect().await;
        let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;

        Ok(())
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

    fn build_mqtt_options(config: &MqttSourceConfig) -> Result<MqttOptions, ConnectorError> {
        let normalized = Self::normalize_broker_url(&config.broker_url);
        let endpoint = Url::parse(&normalized).map_err(|err| {
            ConnectorError::Connection(format!("invalid broker URL `{}`: {err}", config.broker_url))
        })?;
        let scheme = endpoint.scheme();

        let host = endpoint.host_str().ok_or_else(|| {
            ConnectorError::Connection(format!(
                "broker URL `{}` is missing a host",
                config.broker_url
            ))
        })?;

        let port = endpoint
            .port()
            .or_else(|| Self::default_port_for_scheme(scheme))
            .ok_or_else(|| {
                ConnectorError::Connection(format!(
                    "broker URL `{}` is missing a port",
                    config.broker_url
                ))
            })?;

        let mut options = MqttOptions::new(config.client_id(), host, port);

        if Self::is_tls_scheme(scheme) {
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
}

impl SourceConnector for MqttSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.receiver.is_some() {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }

        let (sender, receiver) = mpsc::channel(256);
        Self::spawn_worker(self.config.clone(), sender);
        self.receiver = Some(receiver);

        let stream = ReceiverStream::new(self.receiver.take().unwrap());
        Ok(Box::pin(stream))
    }
}
