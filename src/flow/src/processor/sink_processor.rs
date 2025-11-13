//! SinkProcessor - routes collections to SinkConnectors and forwards results.

use crate::connector::SinkConnector;
use crate::encoder::CollectionEncoder;
use crate::model::Collection;
use crate::processor::base::{broadcast_all, fan_in_streams};
use crate::processor::{Processor, ProcessorError, StreamData, StreamError};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;

struct ConnectorBinding {
    connector: Box<dyn SinkConnector>,
    encoder: Arc<dyn CollectionEncoder>,
}

impl ConnectorBinding {
    async fn publish(&mut self, collection: &dyn Collection) -> Result<(), ProcessorError> {
        let payloads = self
            .encoder
            .encode(collection)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

        for payload in payloads {
            self.connector
                .send(&payload)
                .await
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), ProcessorError> {
        self.connector
            .close()
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }
}

/// Processor that fans out collections to registered sink connectors.
///
/// The processor exposes a single logical input/output so it can sit between
/// the PhysicalPlan root and the result collector. Every `Collection` routed
/// through it is encoded and delivered to each connector binding.
pub struct SinkProcessor {
    id: String,
    inputs: Vec<mpsc::Receiver<StreamData>>,
    outputs: Vec<mpsc::Sender<StreamData>>,
    connectors: Vec<ConnectorBinding>,
}

impl SinkProcessor {
    /// Create a new sink processor with the provided identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            connectors: Vec::new(),
        }
    }

    /// Register a connector + encoder pair.
    pub fn add_connector(
        &mut self,
        connector: Box<dyn SinkConnector>,
        encoder: Arc<dyn CollectionEncoder>,
    ) {
        self.connectors
            .push(ConnectorBinding { connector, encoder });
    }

    async fn handle_collection(
        connectors: &mut [ConnectorBinding],
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        for connector in connectors.iter_mut() {
            connector.publish(collection).await?;
        }

        // Forward downstream after successful delivery.
        Ok(())
    }

    async fn handle_terminal(connectors: &mut [ConnectorBinding]) -> Result<(), ProcessorError> {
        for connector in connectors.iter_mut() {
            connector.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::MockSinkConnector;
    use crate::encoder::JsonEncoder;
    use crate::model::{Column, RecordBatch};
    use crate::processor::StreamData;
    use datatypes::Value;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn sink_processor_encodes_and_forwards_collections() {
        let mut sink = SinkProcessor::new("sink_test");
        let (input_tx, input_rx) = mpsc::channel(10);
        sink.add_input(input_rx);
        let (output_tx, mut output_rx) = mpsc::channel(10);
        sink.add_output(output_tx);

        let (connector, mut handle) = MockSinkConnector::new("mock_sink");
        let encoder = Arc::new(JsonEncoder::new("json"));
        sink.add_connector(Box::new(connector), encoder);

        let sink_handle = sink.start();

        let column = Column::new(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(5)],
        );
        let batch = RecordBatch::new(vec![column]).expect("record batch");

        input_tx
            .send(StreamData::collection(Box::new(batch.clone())))
            .await
            .expect("send collection");
        input_tx
            .send(StreamData::stream_end())
            .await
            .expect("send end");

        // Expect the data to be forwarded downstream.
        let forwarded = output_rx.recv().await.expect("forwarded data");
        assert!(forwarded.is_data());

        // Expect the connector to receive encoded payload.
        let payload = handle.recv().await.expect("connector payload");
        let json: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");
        assert_eq!(
            json,
            serde_json::json!({"orders.amount":5}),
            "encoded row should include column identifier"
        );

        // Stream end should also be propagated.
        let terminal = output_rx.recv().await.expect("terminal signal");
        assert!(terminal.is_terminal());

        sink_handle.await.expect("join").expect("processor ok");
    }
}

impl Processor for SinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let outputs = self.outputs.clone();

        let mut connectors = std::mem::take(&mut self.connectors);
        let processor_id = self.id.clone();

        tokio::spawn(async move {
            while let Some(data) = input_streams.next().await {
                if let Some(collection) = data.as_collection() {
                    if let Err(err) = Self::handle_collection(&mut connectors, collection).await {
                        let error = StreamData::error(
                            StreamError::new(err.to_string()).with_source(processor_id.clone()),
                        );
                        broadcast_all(&outputs, error).await?;
                        return Err(err);
                    }
                }

                broadcast_all(&outputs, data.clone()).await?;

                if data.is_terminal() {
                    Self::handle_terminal(&mut connectors).await?;
                    return Ok(());
                }
            }

            Self::handle_terminal(&mut connectors).await?;
            Ok(())
        })
    }

    fn output_senders(&self) -> Vec<mpsc::Sender<StreamData>> {
        self.outputs.clone()
    }

    fn add_input(&mut self, receiver: mpsc::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.outputs.push(sender);
    }
}
