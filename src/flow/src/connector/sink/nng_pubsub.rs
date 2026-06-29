use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use crate::connector::nng_pubsub::NngPubSubSinkConfig;
use anng::protocols::pubsub0;
use anng::{Message, Socket};
use async_trait::async_trait;
use std::ffi::CString;
use std::io::Write;
use std::sync::Arc;

pub(crate) struct NngPubSubSinkConnector {
    id: String,
    flow_instance_id: Arc<str>,
    config: NngPubSubSinkConfig,
    socket: Option<Socket<pubsub0::Pub0>>,
    buffer: Option<Vec<u8>>,
}

impl NngPubSubSinkConnector {
    pub fn new(
        id: impl Into<String>,
        config: NngPubSubSinkConfig,
        flow_instance_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            id: id.into(),
            flow_instance_id: flow_instance_id.into(),
            config,
            socket: None,
            buffer: None,
        }
    }

    async fn ensure_socket(&mut self) -> Result<(), SinkConnectorError> {
        if self.socket.is_some() {
            return Ok(());
        }
        self.config
            .validate()
            .map_err(|err| SinkConnectorError::Other(format!("invalid nng pubsub sink: {err}")))?;
        let url = CString::new(self.config.url.as_str()).map_err(|err| {
            SinkConnectorError::Other(format!(
                "invalid nng url `{}`: {err}",
                crate::connector::mask_url_userinfo(&self.config.url)
            ))
        })?;
        let socket = pubsub0::Pub0::socket().map_err(|err| {
            SinkConnectorError::Other(format!("nng pub socket open failed: {err}"))
        })?;
        socket.dial(url.as_c_str()).await.map_err(|err| {
            SinkConnectorError::Unavailable(format!(
                "nng pubsub sink `{}` dial `{}` failed: {err}",
                self.id, self.config.url
            ))
        })?;
        tracing::info!(connector_id = %self.id, "nng pubsub sink connected");
        self.socket = Some(socket);
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for NngPubSubSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_socket().await
    }

    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_socket().await?;
        self.buffer = Some(Vec::new());
        Ok(())
    }

    async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Err(SinkConnectorError::Other(format!(
                "nng pubsub sink `{}` received chunk without active delivery",
                self.id
            )));
        };
        buffer.extend_from_slice(payload);
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let payload = self.buffer.take().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "nng pubsub sink `{}` finished without active delivery",
                self.id
            ))
        })?;
        let bytes_written = payload.len() as u64;
        veloflux_metrics::nng_pubsub_sink_records_in_total()
            .with_label_values(&[self.flow_instance_id.as_ref(), self.id.as_str()])
            .inc();

        let message = build_message(&self.config.topic, &self.config.topic_delimiter, &payload)?;
        let socket = self.socket.as_mut().ok_or_else(|| {
            SinkConnectorError::Unavailable(format!("nng pubsub sink `{}` not connected", self.id))
        })?;
        match socket.publish(message).await {
            Ok(()) => {
                veloflux_metrics::nng_pubsub_sink_records_out_total()
                    .with_label_values(&[self.flow_instance_id.as_ref(), self.id.as_str()])
                    .inc();
                Ok(DeliveryResult { bytes_written })
            }
            Err((err, _message)) => {
                self.socket = None;
                Err(SinkConnectorError::Unavailable(format!(
                    "nng pubsub sink `{}` publish failed: {err}",
                    self.id
                )))
            }
        }
    }

    async fn abort_delivery(&mut self) {
        self.buffer = None;
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.socket = None;
        tracing::info!(connector_id = %self.id, "nng pubsub sink closed");
        Ok(())
    }
}

fn build_message(
    topic: &str,
    delimiter: &str,
    payload: &[u8],
) -> Result<Message, SinkConnectorError> {
    let topic = topic.as_bytes();
    let delimiter = delimiter.as_bytes();
    let mut message = Message::with_capacity(topic.len() + delimiter.len() + payload.len());
    message
        .write_all(topic)
        .and_then(|_| message.write_all(delimiter))
        .and_then(|_| message.write_all(payload))
        .map_err(|err| {
            SinkConnectorError::Other(format!("build nng pubsub message failed: {err}"))
        })?;
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::{build_message, NngPubSubSinkConfig, NngPubSubSinkConnector};
    use crate::connector::SinkConnector;
    use anng::protocols::pubsub0;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn sink_publishes_topic_delimited_frame() {
        let url = format!("inproc://vf-nng-sink-{}", uuid::Uuid::new_v4());
        let c_url = std::ffi::CString::new(url.clone()).expect("valid url");
        let subscriber = pubsub0::Sub0::listen(c_url.as_c_str())
            .await
            .expect("start subscriber");
        let mut sub = subscriber.context();
        sub.subscribe_to(b"topic/can");

        let config = NngPubSubSinkConfig::new("sink", url, "topic/can");
        let mut connector = NngPubSubSinkConnector::new("nng_sink", config, "test");
        connector.ready().await.expect("ready");
        sleep(Duration::from_millis(100)).await;

        connector.start_delivery().await.expect("start delivery");
        connector
            .write_chunk(br#"{"v":1}"#)
            .await
            .expect("write chunk");
        connector.finish_delivery().await.expect("finish delivery");
        let message = timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("receive timeout")
            .expect("receive frame");
        assert_eq!(message.as_slice(), b"topic/can:{\"v\":1}");

        connector.close().await.expect("close sink");
    }

    #[test]
    fn build_message_prepends_topic_and_delimiter() {
        let message = build_message("topic/can", ":", br#"{"v":1}"#).expect("build message");

        assert_eq!(message.as_slice(), b"topic/can:{\"v\":1}");
    }
}
