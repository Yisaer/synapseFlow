//! EncoderProcessor - encodes collections into payload bytes for sinks.
//!
//! This processor sits between the physical plan root and sink processors.
//! It transforms [`StreamData::Collection`] items using the configured
//! [`CollectionEncoder`] and produces [`StreamData::EncodedBytes`] records.

use crate::codec::encoder::CollectionEncoder;
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Processor that encodes collections into payload bytes.
pub struct EncoderProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    encoder: Arc<dyn CollectionEncoder>,
}

impl EncoderProcessor {
    /// Create a new encoder processor with the provided encoder instance.
    pub fn new(id: impl Into<String>, encoder: Arc<dyn CollectionEncoder>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            encoder,
        }
    }
}

impl Processor for EncoderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let encoder = Arc::clone(&self.encoder);
        let processor_id = self.id.clone();
        tracing::info!(processor_id = %processor_id, "encoder processor starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        let rows = collection.num_rows() as u64;
                                        match encoder.encode(collection.as_ref()) {
                                            Ok(payload) => {
                                                send_with_backpressure(
                                                    &output,
                                                    StreamData::encoded_bytes(payload, rows),
                                                )
                                                .await?;
                                            }
                                            Err(err) => {
                                                let message = format!("encode error: {err}");
                                                tracing::error!(processor_id = %processor_id, error = %err, "encode error");
                                                forward_error(&output, &processor_id, message).await?;
                                                continue;
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        send_with_backpressure(&output, data).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            tracing::info!(processor_id = %processor_id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "encoder data input");
                                continue;
                            }
                            None => {
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
