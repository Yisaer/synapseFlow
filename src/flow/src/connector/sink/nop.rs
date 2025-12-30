//! A sink connector that simply discards every payload.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;

use crate::planner::sink::NopSinkConfig;

/// A no-op sink connector useful for benchmarks or tests.
pub struct NopSinkConnector {
    id: String,
    config: NopSinkConfig,
}

impl NopSinkConnector {
    pub fn new(id: impl Into<String>, config: NopSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
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

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        if self.config.log {
            tracing::info!(
                connector_id = %self.id,
                payload_len = payload.len(),
                "nop sink received payload"
            );
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        if self.config.log {
            tracing::info!(connector_id = %self.id, log = true, "nop sink closed");
        }
        Ok(())
    }
}
