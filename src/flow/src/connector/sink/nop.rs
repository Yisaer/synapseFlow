//! A sink connector that simply discards every payload.

use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use async_trait::async_trait;

use crate::planner::sink::NopSinkConfig;

/// A no-op sink connector useful for benchmarks or tests.
pub struct NopSinkConnector {
    id: String,
    config: NopSinkConfig,
    active_bytes: Option<u64>,
}

impl NopSinkConnector {
    pub fn new(id: impl Into<String>, config: NopSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
            active_bytes: None,
        }
    }
}

#[async_trait]
impl SinkConnector for NopSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        if self.config.log {
            tracing::info!(connector_id = %self.id, log = true, "nop sink ready");
        }
        Ok(())
    }

    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        self.active_bytes = Some(0);
        Ok(())
    }

    async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(bytes_written) = self.active_bytes.as_mut() else {
            return Err(SinkConnectorError::Other(format!(
                "nop sink `{}` received chunk without active delivery",
                self.id
            )));
        };
        *bytes_written += payload.len() as u64;
        if self.config.log {
            tracing::info!(
                connector_id = %self.id,
                payload_len = payload.len(),
                "nop sink received chunk"
            );
        }
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let bytes_written = self.active_bytes.take().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "nop sink `{}` finished without active delivery",
                self.id
            ))
        })?;
        Ok(DeliveryResult { bytes_written })
    }

    async fn abort_delivery(&mut self) {
        self.active_bytes = None;
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if self.config.log {
            tracing::info!(connector_id = %self.id, log = true, "nop sink closed");
        }
        Ok(())
    }
}
