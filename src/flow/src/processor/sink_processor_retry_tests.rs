//! Unit tests for SinkProcessor retry behaviour.

#[cfg(test)]
mod tests {
    use crate::connector::sink::{DeliveryResult, SinkConnector, SinkConnectorError};
    use crate::planner::sink::SinkRetryConfig;
    use crate::processor::{
        EncodedDeliveryFlags, Processor, ProcessorStats, SinkProcessor, StreamData,
    };
    use crate::runtime::TaskSpawner;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    /// Returns `Permanent` (prototype: no clone needed).
    fn permanent(msg: &str) -> SinkConnectorError {
        SinkConnectorError::Permanent(msg.into())
    }

    fn transient(msg: &str) -> SinkConnectorError {
        SinkConnectorError::Transient(msg.into())
    }

    /// A connector that fails `fail_count` times before forwarding payloads.
    struct RetryMockConnector {
        fail_count: Arc<AtomicUsize>,
        fail_with_kind: &'static str, // "transient" or "permanent"
        sender: mpsc::Sender<Vec<u8>>,
        buffer: Option<Vec<u8>>,
    }

    struct RetryMockHandle {
        receiver: mpsc::Receiver<Vec<u8>>,
    }

    impl RetryMockConnector {
        fn new(fail_count: usize, fail_with_kind: &'static str) -> (Self, RetryMockHandle) {
            let (sender, receiver) = mpsc::channel(16);
            (
                Self {
                    fail_count: Arc::new(AtomicUsize::new(fail_count)),
                    fail_with_kind,
                    sender,
                    buffer: None,
                },
                RetryMockHandle { receiver },
            )
        }

        fn build_error(&self) -> SinkConnectorError {
            match self.fail_with_kind {
                "transient" => transient("injected failure"),
                "permanent" => permanent("bad request"),
                _ => permanent("unknown"),
            }
        }
    }

    impl RetryMockHandle {
        async fn recv(&mut self) -> Option<Vec<u8>> {
            self.receiver.recv().await
        }
    }

    #[async_trait]
    impl SinkConnector for RetryMockConnector {
        fn id(&self) -> &str {
            "retry_mock"
        }

        async fn ready(&mut self) -> Result<(), SinkConnectorError> {
            Ok(())
        }

        async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
            self.buffer = Some(Vec::new());
            Ok(())
        }

        async fn write_chunk(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
            let Some(buf) = self.buffer.as_mut() else {
                return Err(permanent("retry_mock: chunk without active delivery"));
            };
            buf.extend_from_slice(payload);
            Ok(())
        }

        async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
            let payload = self
                .buffer
                .take()
                .ok_or_else(|| permanent("retry_mock: finish without active delivery"))?;
            let bytes_written = payload.len() as u64;

            if self.fail_count.fetch_sub(1, Ordering::Relaxed) > 0 {
                return Err(self.build_error());
            }

            let _ = self.sender.send(payload).await;
            Ok(DeliveryResult { bytes_written })
        }

        async fn abort_delivery(&mut self) {
            self.buffer = None;
        }

        async fn close(&mut self) -> Result<(), SinkConnectorError> {
            Ok(())
        }
    }

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(tokio::runtime::Handle::current())
    }

    fn make_encoded_delivery(payload: &[u8]) -> Vec<StreamData> {
        let split = 1.min(payload.len());
        let rest = &payload[split..];
        let mut chunks = vec![StreamData::EncodedDelivery {
            flags: EncodedDeliveryFlags::START,
            bytes: bytes::Bytes::copy_from_slice(&payload[..split]),
        }];
        chunks.push(StreamData::EncodedDelivery {
            flags: EncodedDeliveryFlags::END,
            bytes: bytes::Bytes::copy_from_slice(rest),
        });
        chunks
    }

    async fn drive_and_assert(
        handle: tokio::task::JoinHandle<Result<(), crate::processor::ProcessorError>>,
        mut handle_rx: RetryMockHandle,
        expect_success: bool,
    ) {
        if expect_success {
            let got = timeout(Duration::from_secs(10), handle_rx.recv())
                .await
                .expect("timeout")
                .expect("delivery received");
            let json: serde_json::Value = serde_json::from_slice(&got).expect("json parse");
            assert_eq!(json, serde_json::json!([{"a": 42}]));
        }
        // Don't unwrap the join handle: the processor may have exited or be idle.
        // Just ensure we don't hang.
        let _ = timeout(Duration::from_secs(5), handle).await;
    }

    fn assert_message_metrics(
        stats: &ProcessorStats,
        messages_out: u64,
        messages_dropped: u64,
        bytes_delivered: u64,
    ) {
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_in, None);
        assert_eq!(snapshot.records_out, None);
        assert_eq!(snapshot.custom["messages_in"], 1);
        assert_eq!(snapshot.custom["bytes_in"], 10);
        assert_eq!(snapshot.custom["messages_out"], messages_out);
        assert_eq!(snapshot.custom["messages_dropped"], messages_dropped);
        assert_eq!(snapshot.custom["bytes_delivered"], bytes_delivered);
    }

    // ── test cases ──

    #[tokio::test]
    async fn transient_failure_with_retry_succeeds() {
        let (connector, handle) = RetryMockConnector::new(2, "transient");

        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        let ready = start.take_ready().expect("ready");
        ready.await.expect("ready ok").expect("processor ready");

        let payload = br#"[{"a":42}]"#;
        for data in make_encoded_delivery(payload) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        drive_and_assert(start.handle, handle, true).await;
        assert_message_metrics(stats.as_ref(), 1, 0, payload.len() as u64);
    }

    #[tokio::test]
    async fn permanent_failure_is_retried_then_dropped() {
        let (connector, handle) = RetryMockConnector::new(5, "permanent");

        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        let ready = start.take_ready().expect("ready");
        ready.await.expect("ready ok").expect("processor ready");

        let payload = br#"[{"a":42}]"#;
        for data in make_encoded_delivery(payload) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        // All connector errors are retried. Exhausted attempts drop the delivery
        // and allow the processor to consume the terminal signal.
        drive_and_assert(start.handle, handle, false).await;
        assert_message_metrics(stats.as_ref(), 0, 1, 0);
    }

    #[tokio::test]
    async fn exhausted_transient_retry_drops_delivery_and_continues() {
        let (connector, handle) = RetryMockConnector::new(3, "transient");

        let mut processor = SinkProcessor::new("retry_sink");
        let stats = Arc::new(ProcessorStats::default());
        processor.set_stats(Arc::clone(&stats));
        processor.set_retry_config(SinkRetryConfig {
            max_attempts: Some(3),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
            ..Default::default()
        });
        processor.add_connector(Box::new(connector));

        let (tx, rx) = tokio::sync::broadcast::channel(8);
        processor.add_input(rx);
        let mut start = processor.start(&test_spawner());
        let ready = start.take_ready().expect("ready");
        ready.await.expect("ready ok").expect("processor ready");

        let payload = br#"[{"a":42}]"#;
        for data in make_encoded_delivery(payload) {
            let _ = tx.send(data);
        }
        let _ = tx.send(StreamData::stream_end());

        // 3 failures, 3 attempts -> exhausted -> drop -> pipeline continues,
        // no data received because all attempts failed.
        drive_and_assert(start.handle, handle, false).await;
        assert_message_metrics(stats.as_ref(), 0, 1, 0);
    }
}
