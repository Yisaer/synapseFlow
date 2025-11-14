//! MQTT source connector supporting shared or standalone clients.

use crate::connector::mqtt_client::{acquire_shared_client, SharedMqttEvent};
use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};
use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS, Transport};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

/// Basic MQTT configuration for sources.
#[derive(Debug, Clone)]
pub struct MqttSourceConfig {
    pub source_name: String,
    pub broker_url: String,
    pub topic: String,
    pub qos: u8,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
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
            connector_key: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn with_connector_key(mut self, connector_key: impl Into<String>) -> Self {
        self.connector_key = Some(connector_key.into());
        self
    }

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| self.source_name.clone())
    }
}

pub struct MqttSourceConnector {
    id: String,
    config: MqttSourceConfig,
    receiver: Option<mpsc::Receiver<Result<ConnectorEvent, ConnectorError>>>,
}

static MQTT_SOURCE_RECORDS_IN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "mqtt_source_records_in_total",
        "Number of records received from MQTT sources",
        &["connector"]
    )
    .expect("create mqtt source records_in counter vec")
});

static MQTT_SOURCE_RECORDS_OUT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "mqtt_source_records_out_total",
        "Number of records emitted downstream by MQTT sources",
        &["connector"]
    )
    .expect("create mqtt source records_out counter vec")
});

impl MqttSourceConnector {
    pub fn new(id: impl Into<String>, config: MqttSourceConfig) -> Self {
        Self {
            id: id.into(),
            config,
            receiver: None,
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
        let config = self.config.clone();
        let connector_id = self.id.clone();

        if let Some(connector_key) = config.connector_key.clone() {
            let metrics_id = connector_id.clone();
            tokio::spawn(async move {
                match acquire_shared_client(&connector_key).await {
                    Ok(shared_client) => {
                        let mut events = shared_client.subscribe();
                        while let Ok(event) = events.recv().await {
                            match event {
                                Ok(SharedMqttEvent::Payload(payload)) => {
                                    println!(
                                        "[MqttSourceConnector:{metrics_id}] received payload ({} bytes)",
                                        payload.len()
                                    );
                                    MQTT_SOURCE_RECORDS_IN
                                        .with_label_values(&[metrics_id.as_str()])
                                        .inc();
                                    match sender.send(Ok(ConnectorEvent::Payload(payload))).await {
                                        Ok(_) => {
                                            MQTT_SOURCE_RECORDS_OUT
                                                .with_label_values(&[metrics_id.as_str()])
                                                .inc();
                                        }
                                        Err(_) => break,
                                    }
                                }
                                Ok(SharedMqttEvent::EndOfStream) => {
                                    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
                                    break;
                                }
                                Err(err) => {
                                    let _ = sender.send(Err(err)).await;
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let _ = sender.send(Err(err)).await;
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(err) =
                    run_standalone_loop(connector_id.clone(), config, sender.clone()).await
                {
                    let _ = sender.send(Err(err)).await;
                }
            });
        }

        self.receiver = Some(receiver);
        let stream = ReceiverStream::new(self.receiver.take().unwrap());
        Ok(Box::pin(stream))
    }
}

async fn run_standalone_loop(
    connector_id: String,
    config: MqttSourceConfig,
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
) -> Result<(), ConnectorError> {
    let mut mqtt_options = build_mqtt_options(&config)?;
    mqtt_options.set_keep_alive(Duration::from_secs(30));

    let qos = map_qos(config.qos)?;
    let topic = config.topic.clone();

    let (client, mut event_loop) = AsyncClient::new(mqtt_options, 32);

    client
        .subscribe(topic, qos)
        .await
        .map_err(|e| ConnectorError::Connection(e.to_string()))?;

    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                println!(
                    "[MqttSourceConnector:{connector_id}] received payload ({} bytes)",
                    publish.payload.len()
                );
                MQTT_SOURCE_RECORDS_IN
                    .with_label_values(&[connector_id.as_str()])
                    .inc();
                let payload = publish.payload.to_vec();
                match sender.send(Ok(ConnectorEvent::Payload(payload))).await {
                    Ok(_) => {
                        MQTT_SOURCE_RECORDS_OUT
                            .with_label_values(&[connector_id.as_str()])
                            .inc();
                    }
                    Err(_) => break,
                }
            }
            Ok(Event::Incoming(Packet::Disconnect)) => break,
            Ok(_) => {}
            Err(ConnectionError::RequestsDone) => break,
            Err(err) => return Err(ConnectorError::Connection(err.to_string())),
        }
    }

    let _ = client.disconnect().await;
    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
    Ok(())
}

fn build_mqtt_options(config: &MqttSourceConfig) -> Result<MqttOptions, ConnectorError> {
    let normalized = normalize_broker_url(&config.broker_url);
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
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            ConnectorError::Connection(format!(
                "broker URL `{}` is missing a port",
                config.broker_url
            ))
        })?;

    let mut options = MqttOptions::new(config.client_id(), host, port);
    options.set_max_packet_size(64 * 1024 * 1024, 64 * 1024 * 1024);
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

fn normalize_broker_url(url: &str) -> String {
    if url.contains("://") {
        url.to_owned()
    } else {
        format!("tcp://{url}")
    }
}
