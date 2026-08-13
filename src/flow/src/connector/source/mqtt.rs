//! MQTT source connector supporting shared or standalone clients.

use crate::connector::mqtt_client::{MqttClientManager, SharedMqttEvent};
use crate::connector::mqtt_protocol::{
    MqttClient, MqttConnectionError, MqttEvent, MqttOptions, MqttQos,
};
use crate::connector::{
    mask_url_userinfo, ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector,
};
use crate::processor::base::normalize_channel_capacity;
use crate::runtime::TaskSpawner;
use rumqttc::Transport;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
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
    pub protocol_version: Option<crate::connector::MqttProtocolVersion>,
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
            protocol_version: None,
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

    pub fn with_protocol_version(
        mut self,
        protocol_version: crate::connector::MqttProtocolVersion,
    ) -> Self {
        self.protocol_version = Some(protocol_version);
        self
    }

    fn client_id(&self) -> String {
        self.client_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
    }
}

pub(crate) struct MqttSourceConnector {
    id: String,
    flow_instance_id: Arc<str>,
    config: MqttSourceConfig,
    channel_capacity: usize,
    shutdown_tx: Option<oneshot::Sender<()>>,
    mqtt_clients: MqttClientManager,
    spawner: TaskSpawner,
}

impl MqttSourceConnector {
    pub fn new(
        id: impl Into<String>,
        config: MqttSourceConfig,
        flow_instance_id: impl Into<Arc<str>>,
        mqtt_clients: MqttClientManager,
        spawner: TaskSpawner,
    ) -> Self {
        Self {
            id: id.into(),
            flow_instance_id: flow_instance_id.into(),
            config,
            channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            shutdown_tx: None,
            mqtt_clients,
            spawner,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = normalize_channel_capacity(capacity);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedEventForwardOutcome {
    Continue,
    Break,
}

async fn forward_shared_mqtt_event(
    sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    flow_instance_id: &Arc<str>,
    metrics_id: &str,
    event: Option<Result<SharedMqttEvent, ConnectorError>>,
) -> SharedEventForwardOutcome {
    match event {
        Some(Ok(SharedMqttEvent::Payload(payload))) => {
            veloflux_metrics::mqtt_source_messages_in_total()
                .with_label_values(&[flow_instance_id.as_ref(), metrics_id])
                .inc();
            match sender.send(Ok(ConnectorEvent::Payload(payload))).await {
                Ok(_) => {
                    veloflux_metrics::mqtt_source_messages_out_total()
                        .with_label_values(&[flow_instance_id.as_ref(), metrics_id])
                        .inc();
                    SharedEventForwardOutcome::Continue
                }
                Err(_) => SharedEventForwardOutcome::Break,
            }
        }
        Some(Ok(SharedMqttEvent::EndOfStream)) => {
            let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
            SharedEventForwardOutcome::Break
        }
        Some(Err(err)) => {
            let _ = sender.send(Err(err)).await;
            SharedEventForwardOutcome::Continue
        }
        None => SharedEventForwardOutcome::Break,
    }
}

impl SourceConnector for MqttSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.shutdown_tx.is_some() {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }
        if self.config.connector_key.is_some() && self.config.protocol_version.is_some() {
            return Err(ConnectorError::Other(
                "mqtt source protocol_version is owned by connector_key and must not be set locally"
                    .to_string(),
            ));
        }

        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        let config = self.config.clone();
        let connector_id = self.id.clone();
        let flow_instance_id = Arc::clone(&self.flow_instance_id);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let spawner = self.spawner.clone();
        if let Some(connector_key) = config.connector_key.clone() {
            let metrics_id = connector_id.clone();
            let manager = self.mqtt_clients.clone();
            spawner.spawn(async move {
                let mut shutdown_rx = shutdown_rx;
                match manager.acquire_client(&connector_key).await {
                    Ok(shared_client) => {
                        let mut events = match shared_client.subscribe().await {
                            Ok(events) => events,
                            Err(err) => {
                                let _ = sender.send(Err(err)).await;
                                return;
                            }
                        };
                        loop {
                            tokio::select! {
                                _ = &mut shutdown_rx => break,
                                event = events.recv() => {
                                    if matches!(
                                        forward_shared_mqtt_event(
                                            &sender,
                                            &flow_instance_id,
                                            metrics_id.as_str(),
                                            event,
                                        )
                                        .await,
                                        SharedEventForwardOutcome::Break
                                    ) {
                                        break;
                                    }
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
            spawner.spawn(async move {
                if let Err(err) = run_standalone_loop(
                    connector_id.clone(),
                    Arc::clone(&flow_instance_id),
                    config,
                    sender.clone(),
                    shutdown_rx,
                )
                .await
                {
                    let _ = sender.send(Err(err)).await;
                }
            });
        }

        let stream = ReceiverStream::new(receiver);
        tracing::info!(connector_id = %self.id, "mqtt source starting");
        Ok(Box::pin(stream))
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        tracing::info!(connector_id = %self.id, "mqtt source closed");
        Ok(())
    }
}

async fn run_standalone_loop(
    connector_id: String,
    flow_instance_id: Arc<str>,
    config: MqttSourceConfig,
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), ConnectorError> {
    let mut mqtt_options = build_mqtt_options(&config)?;
    mqtt_options.set_keep_alive(Duration::from_secs(30));

    let qos = map_qos(config.qos)?;
    let topic = config.topic.clone();

    let (client, mut event_loop) = MqttClient::new(mqtt_options, 32);

    client
        .subscribe(topic.clone(), qos)
        .await
        .map_err(|e| ConnectorError::Connection(e.to_string()))?;

    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            event = event_loop.poll() => {
                match event {
                    Ok(MqttEvent::Publish { payload, .. }) => {
                        veloflux_metrics::mqtt_source_messages_in_total()
                            .with_label_values(&[
                                flow_instance_id.as_ref(),
                                connector_id.as_str(),
                            ])
                            .inc();
                        let payload = payload.to_vec();
                        match sender.send(Ok(ConnectorEvent::Payload(payload))).await {
                            Ok(_) => {
                                veloflux_metrics::mqtt_source_messages_out_total()
                                    .with_label_values(&[
                                        flow_instance_id.as_ref(),
                                        connector_id.as_str(),
                                    ])
                                    .inc();
                            }
                            Err(_) => break,
                        }
                    }
                    Ok(MqttEvent::Connected) => {
                        backoff = Duration::from_millis(100);
                        let _ = client.subscribe(topic.clone(), qos).await;
                    }
                    Ok(MqttEvent::Disconnected) => {
                        tracing::warn!(connector_id = %connector_id, "mqtt source disconnected; reconnecting");
                        event_loop.clean();
                    }
                    Ok(MqttEvent::Other) => {}
                    Err(MqttConnectionError::RequestsDone) => break,
                    Err(MqttConnectionError::Connection(err)) => {
                        let _ = sender.send(Err(ConnectorError::Connection(err))).await;
                        sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                    }
                }
            }
        }
    }

    let _ = client.disconnect().await;
    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
    Ok(())
}

fn build_mqtt_options(config: &MqttSourceConfig) -> Result<MqttOptions, ConnectorError> {
    let normalized = normalize_broker_url(&config.broker_url);
    let endpoint = Url::parse(&normalized).map_err(|err| {
        ConnectorError::Connection(format!(
            "invalid broker URL `{}`: {err}",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;
    let scheme = endpoint.scheme();

    let host = endpoint.host_str().ok_or_else(|| {
        ConnectorError::Connection(format!(
            "broker URL `{}` is missing a host",
            mask_url_userinfo(&config.broker_url)
        ))
    })?;

    let port = endpoint
        .port()
        .or_else(|| default_port_for_scheme(scheme))
        .ok_or_else(|| {
            ConnectorError::Connection(format!(
                "broker URL `{}` is missing a port",
                mask_url_userinfo(&config.broker_url)
            ))
        })?;

    let mut options = MqttOptions::new(
        config.protocol_version.unwrap_or_default(),
        config.client_id(),
        host,
        port,
        64 * 1024 * 1024,
    )
    .map_err(ConnectorError::Connection)?;
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

fn normalize_broker_url(url: &str) -> String {
    if url.contains("://") {
        url.to_owned()
    } else {
        format!("tcp://{url}")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_mqtt_options, forward_shared_mqtt_event, MqttSourceConfig, MqttSourceConnector,
        SharedEventForwardOutcome,
    };
    use crate::connector::mqtt_client::{MqttClientManager, SharedMqttEvent};
    use crate::connector::{
        ConnectorError, ConnectorEvent, SharedMqttClientConfig, SourceConnector,
    };
    use crate::runtime::TaskSpawner;
    use crate::test_support::EmbeddedMqttBroker;
    use futures::StreamExt;
    use rumqttc::Transport;
    use std::sync::Arc;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout, Duration};

    #[test]
    fn mqtt_source_build_mqtt_options_uses_stream_local_broker_and_client_id() {
        let config =
            MqttSourceConfig::new("source_stream", "broker.example.com", "fleet/telemetry", 1)
                .with_client_id("source_client");

        let options = build_mqtt_options(&config).expect("build mqtt source options");

        assert_eq!(
            options.broker_address(),
            ("broker.example.com".to_string(), 1883)
        );
        assert_eq!(options.client_id(), "source_client");
        assert_eq!(options.max_packet_size(), 64 * 1024 * 1024);
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn mqtt_source_build_mqtt_options_enables_tls_and_secure_default_port() {
        let config = MqttSourceConfig::new(
            "source_stream",
            "mqtts://secure.example.com",
            "fleet/telemetry",
            1,
        )
        .with_client_id("source_tls_client");

        let options = build_mqtt_options(&config).expect("build secure mqtt source options");

        assert_eq!(
            options.broker_address(),
            ("secure.example.com".to_string(), 8883)
        );
        assert_eq!(options.client_id(), "source_tls_client");
        assert!(matches!(options.transport(), Transport::Tls(_)));
    }

    #[tokio::test]
    async fn shared_client_backed_source_forwards_errors_without_ending_stream() {
        let flow_instance_id = Arc::<str>::from("default");
        let (sender, mut receiver) = mpsc::channel(4);

        let first = forward_shared_mqtt_event(
            &sender,
            &flow_instance_id,
            "shared_source_connector",
            Some(Err(ConnectorError::Connection(
                "injected source runtime error".to_string(),
            ))),
        )
        .await;
        let second = forward_shared_mqtt_event(
            &sender,
            &flow_instance_id,
            "shared_source_connector",
            Some(Ok(SharedMqttEvent::Payload(b"after-error".to_vec()))),
        )
        .await;

        assert_eq!(first, SharedEventForwardOutcome::Continue);
        assert_eq!(second, SharedEventForwardOutcome::Continue);

        match receiver
            .recv()
            .await
            .expect("receive forwarded error event")
        {
            Err(ConnectorError::Connection(message)) => {
                assert_eq!(message, "injected source runtime error");
            }
            other => panic!("expected forwarded runtime error, got {other:?}"),
        }
        match receiver
            .recv()
            .await
            .expect("receive forwarded payload event")
        {
            Ok(ConnectorEvent::Payload(payload)) => {
                assert_eq!(payload, b"after-error".to_vec());
            }
            other => panic!("expected payload after runtime error, got {other:?}"),
        }
    }

    // coverage-covers: source.mqtt.basic_ingest
    #[tokio::test]
    async fn shared_client_backed_source_receives_payloads_from_embedded_broker() {
        let broker = EmbeddedMqttBroker::start().await;
        let spawner = TaskSpawner::from_handle(Handle::current());
        let manager = MqttClientManager::new("default", spawner.clone());
        let topic_filter = broker.scoped_filter("fleet/+/telemetry");
        let shared_cfg = SharedMqttClientConfig {
            key: "shared_source".to_string(),
            broker_url: broker.broker_url(),
            topic: topic_filter.clone(),
            client_id: "shared_source_client".to_string(),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        };
        manager
            .create_client(shared_cfg)
            .await
            .expect("create shared mqtt client");

        let config = MqttSourceConfig::new("source_stream", broker.broker_url(), &topic_filter, 0)
            .with_connector_key("shared_source");
        let mut connector = MqttSourceConnector::new(
            "shared_source_connector",
            config,
            "default",
            manager.clone(),
            spawner,
        );
        let mut stream = connector.subscribe().expect("subscribe source connector");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if manager.test_runtime_started("shared_source").await {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared mqtt runtime did not start for shared-client-backed source"
            );
            sleep(Duration::from_millis(20)).await;
        }
        sleep(Duration::from_millis(300)).await;

        broker
            .publish("fleet/device_a/telemetry", b"from-embedded-broker".to_vec())
            .await
            .expect("publish embedded mqtt payload");

        let item = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("recv embedded broker payload timeout")
            .expect("shared-client-backed source event should exist");

        match item {
            Ok(ConnectorEvent::Payload(payload)) => {
                assert_eq!(payload, b"from-embedded-broker".to_vec());
            }
            other => panic!("expected payload from embedded broker, got {other:?}"),
        }

        connector.close().expect("close source connector");
    }

    // coverage-covers: source.mqtt.v5_ingest
    #[tokio::test]
    async fn standalone_mqtt_v5_source_receives_payload_from_embedded_broker() {
        let broker = EmbeddedMqttBroker::start().await;
        let topic_filter = broker.scoped_filter("standalone/v5/source");
        let config =
            MqttSourceConfig::new("mqtt_v5_source", broker.broker_url_v5(), topic_filter, 1)
                .with_protocol_version(crate::connector::MqttProtocolVersion::V5);
        let spawner = TaskSpawner::from_handle(Handle::current());
        let mut connector = MqttSourceConnector::new(
            "mqtt_v5_source_connector",
            config,
            "default",
            MqttClientManager::new("default", spawner.clone()),
            spawner,
        );
        let mut stream = connector.subscribe().expect("subscribe MQTT 5 source");

        sleep(Duration::from_millis(300)).await;
        broker
            .publish_v5("standalone/v5/source", b"mqtt-v5-payload".to_vec())
            .await
            .expect("publish MQTT 5 embedded payload");

        let item = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("receive MQTT 5 source payload timeout")
            .expect("MQTT 5 source event should exist");
        match item {
            Ok(ConnectorEvent::Payload(payload)) => {
                assert_eq!(payload, b"mqtt-v5-payload".to_vec());
            }
            other => panic!("expected MQTT 5 source payload, got {other:?}"),
        }

        connector.close().expect("close MQTT 5 source connector");
    }
}
