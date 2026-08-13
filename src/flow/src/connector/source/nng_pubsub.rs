use crate::connector::nng_pubsub::{strip_topic_prefix, NngPubSubSourceConfig};
use crate::connector::{
    mask_url_userinfo, ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector,
};
use crate::processor::base::normalize_channel_capacity;
use crate::runtime::TaskSpawner;
use anng::protocols::pubsub0;
use std::ffi::CString;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;

pub(crate) struct NngPubSubSourceConnector {
    id: String,
    flow_instance_id: Arc<str>,
    config: NngPubSubSourceConfig,
    channel_capacity: usize,
    shutdown_tx: Option<oneshot::Sender<()>>,
    spawner: TaskSpawner,
}

impl NngPubSubSourceConnector {
    pub fn new(
        id: impl Into<String>,
        config: NngPubSubSourceConfig,
        flow_instance_id: impl Into<Arc<str>>,
        spawner: TaskSpawner,
    ) -> Self {
        Self {
            id: id.into(),
            flow_instance_id: flow_instance_id.into(),
            config,
            channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            shutdown_tx: None,
            spawner,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = normalize_channel_capacity(capacity);
        self
    }
}

impl SourceConnector for NngPubSubSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.shutdown_tx.is_some() {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }
        self.config.validate().map_err(ConnectorError::Connection)?;

        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let connector_id = self.id.clone();
        let flow_instance_id = Arc::clone(&self.flow_instance_id);
        let config = self.config.clone();
        self.spawner.spawn(async move {
            run_nng_source_loop(connector_id, flow_instance_id, config, sender, shutdown_rx).await;
        });

        tracing::info!(connector_id = %self.id, "nng pubsub source starting");
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        tracing::info!(connector_id = %self.id, "nng pubsub source closed");
        Ok(())
    }
}

async fn run_nng_source_loop(
    connector_id: String,
    flow_instance_id: Arc<str>,
    config: NngPubSubSourceConfig,
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let url = match CString::new(config.url.as_str()) {
        Ok(url) => url,
        Err(err) => {
            let _ = sender
                .send(Err(ConnectorError::Connection(format!(
                    "invalid nng url `{}`: {err}",
                    config.url
                ))))
                .await;
            let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
            return;
        }
    };
    let topic = config.topic.into_bytes();
    let delimiter = config.topic_delimiter.into_bytes();
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(5);

    loop {
        let socket = match pubsub0::Sub0::socket() {
            Ok(socket) => socket,
            Err(err) => {
                if !report_and_wait(
                    &sender,
                    &mut shutdown_rx,
                    &mut backoff,
                    max_backoff,
                    ConnectorError::Connection(format!("nng sub socket open failed: {err}")),
                )
                .await
                {
                    break;
                }
                continue;
            }
        };

        if let Err(err) = socket.dial(url.as_c_str()).await {
            if !report_and_wait(
                &sender,
                &mut shutdown_rx,
                &mut backoff,
                max_backoff,
                ConnectorError::Connection(format!(
                    "nng sub dial `{}` failed: {err}",
                    mask_url_userinfo(&config.url)
                )),
            )
            .await
            {
                break;
            }
            continue;
        }

        let mut sub = socket.context();
        if topic.is_empty() {
            sub.disable_filtering();
        } else {
            sub.subscribe_to(&topic);
        }
        backoff = Duration::from_millis(100);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
                    return;
                }
                result = sub.next() => {
                    match result {
                        Ok(message) => {
                            veloflux_metrics::nng_pubsub_source_messages_in_total()
                                .with_label_values(&[flow_instance_id.as_ref(), connector_id.as_str()])
                                .inc();
                            let Some(payload) = strip_topic_prefix(message.as_slice(), &delimiter, &topic) else {
                                tracing::warn!(
                                    connector_id = %connector_id,
                                    len = message.len(),
                                    "nng pubsub source dropped message with mismatched topic framing"
                                );
                                continue;
                            };
                            match sender.send(Ok(ConnectorEvent::Payload(payload.to_vec()))).await {
                                Ok(_) => {
                                    veloflux_metrics::nng_pubsub_source_messages_out_total()
                                        .with_label_values(&[flow_instance_id.as_ref(), connector_id.as_str()])
                                        .inc();
                                }
                                Err(_) => return,
                            }
                        }
                        Err(err) => {
                            let _ = sender
                                .send(Err(ConnectorError::Connection(format!(
                                    "nng sub recv failed: {err}"
                                ))))
                                .await;
                            break;
                        }
                    }
                }
            }
        }
    }

    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
}

async fn report_and_wait(
    sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    shutdown_rx: &mut oneshot::Receiver<()>,
    backoff: &mut Duration,
    max_backoff: Duration,
    err: ConnectorError,
) -> bool {
    let _ = sender.send(Err(err)).await;
    tokio::select! {
        _ = shutdown_rx => false,
        _ = sleep(*backoff) => {
            *backoff = std::cmp::min(*backoff * 2, max_backoff);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NngPubSubSourceConfig, NngPubSubSourceConnector};
    use crate::connector::{ConnectorEvent, SourceConnector};
    use crate::runtime::TaskSpawner;
    use anng::{protocols::pubsub0, Message};
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn source_receives_and_strips_topic_prefix() {
        let url = format!("inproc://vf-nng-source-{}", uuid::Uuid::new_v4());
        let c_url = std::ffi::CString::new(url.clone()).expect("valid url");
        let mut publisher = pubsub0::Pub0::listen(c_url.as_c_str())
            .await
            .expect("start publisher");

        let config = NngPubSubSourceConfig::new("source", url, "topic/can");
        let mut connector = NngPubSubSourceConnector::new(
            "nng_source",
            config,
            "test",
            TaskSpawner::from_handle(tokio::runtime::Handle::current()),
        );
        let mut stream = connector.subscribe().expect("subscribe");
        sleep(Duration::from_millis(100)).await;

        let frame = Message::from(&b"topic/can:{\"v\":1}"[..]);
        publisher.publish(frame).await.expect("publish");

        let event = timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("receive timeout")
            .expect("stream item")
            .expect("source event");
        match event {
            ConnectorEvent::Payload(payload) => assert_eq!(payload, br#"{"v":1}"#),
            other => panic!("unexpected event: {other:?}"),
        }

        connector.close().expect("close source");
    }
}
