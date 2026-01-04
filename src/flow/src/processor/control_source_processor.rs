//! ControlSourceProcessor - starting point for data flow
//!
//! This processor is the pipeline head. External callers send [`Ingress`] items into it,
//! and it forwards them to downstream processors while preserving channel isolation.

use crate::processor::base::{
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{
    BarrierControlSignal, ControlSignal, Processor, ProcessorError, StreamData,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressTarget {
    Data,
    Control,
}

#[derive(Clone)]
pub struct Ingress {
    pub target: IngressTarget,
    pub data: StreamData,
}

impl Ingress {
    pub fn data(data: StreamData) -> Self {
        Self {
            target: IngressTarget::Data,
            data,
        }
    }

    pub fn control(signal: ControlSignal) -> Self {
        Self {
            target: IngressTarget::Control,
            data: StreamData::control(signal),
        }
    }
}

/// ControlSourceProcessor - handles control signals for the pipeline
///
/// This processor acts as the starting point of the data flow. It:
/// - Receives `Ingress` items from a single input (single-input)
/// - Forwards StreamData to multiple downstream processors (multi-output)
/// - Coordinates the start/end of stream processing
pub struct ControlSourceProcessor {
    /// Processor identifier
    id: String,
    /// Single input channel for receiving ingress items.
    ingress: Option<mpsc::Receiver<Ingress>>,
    /// Broadcast channel for all downstream processors
    output: broadcast::Sender<StreamData>,
    /// Dedicated control channel for urgent control propagation
    control_output: broadcast::Sender<ControlSignal>,
    /// Monotonically increasing barrier id allocator (shared across control/data channels).
    next_barrier_id: Arc<AtomicU64>,
}

impl ControlSourceProcessor {
    /// Create a new ControlSourceProcessor
    pub fn new(id: impl Into<String>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            ingress: None,
            output,
            control_output,
            next_barrier_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn set_ingress_input(&mut self, receiver: mpsc::Receiver<Ingress>) {
        self.ingress = Some(receiver);
    }

    pub fn allocate_barrier_id(&self) -> u64 {
        self.next_barrier_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send StreamData to all downstream processors
    pub async fn send(&self, data: StreamData) -> Result<(), ProcessorError> {
        send_with_backpressure(&self.output, data).await
    }

    /// Send StreamData to a specific downstream processor by id.
    ///
    /// The broadcast-based fan-out no longer supports targeted sends, so this
    /// method now returns an error to signal that behavior.
    pub async fn send_stream_data(
        &self,
        _processor_id: &str,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        self.send(data).await
    }
}

impl Processor for ControlSourceProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let input_result = self.ingress.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ControlSourceProcessor ingress must be set before starting".to_string(),
            )
        });
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let barrier_id_allocator = Arc::clone(&self.next_barrier_id);
        tracing::info!(processor_id = %processor_id, "control source processor starting");

        tokio::spawn(async move {
            let input = match input_result {
                Ok(input) => input,
                Err(e) => return Err(e),
            };

            let mut receiver = input;
            while let Some(ingress) = receiver.recv().await {
                match ingress.target {
                    IngressTarget::Data => {
                        let is_terminal = ingress.data.is_terminal();
                        send_with_backpressure(&output, ingress.data).await?;
                        if is_terminal {
                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                            return Ok(());
                        }
                    }
                    IngressTarget::Control => {
                        let StreamData::Control(signal) = ingress.data else {
                            tracing::warn!(
                                processor_id = %processor_id,
                                "invalid ingress: control target requires StreamData::Control"
                            );
                            continue;
                        };

                        let is_terminal = signal.is_terminal();
                        send_control_with_backpressure(&control_output, signal).await?;
                        if is_terminal {
                            tracing::info!(
                                processor_id = %processor_id,
                                "received terminal control signal (control)"
                            );
                            return Ok(());
                        }
                    }
                }
            }

            // Input closed without explicit StreamEnd, propagate shutdown on the data path.
            tracing::info!(processor_id = %processor_id, "stopping (ingress closed)");
            let barrier_id = barrier_id_allocator.fetch_add(1, Ordering::Relaxed);
            send_with_backpressure(
                &output,
                StreamData::control(ControlSignal::Barrier(
                    BarrierControlSignal::StreamGracefulEnd { barrier_id },
                )),
            )
            .await?;
            Ok(())
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        Some(self.output.subscribe())
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.control_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        tracing::warn!(
            processor_id = %self.id,
            "ControlSourceProcessor does not accept broadcast inputs; drop unused receiver"
        );
        drop(receiver);
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        tracing::warn!(
            processor_id = %self.id,
            "ControlSourceProcessor does not accept broadcast control inputs; drop unused receiver"
        );
        drop(receiver);
    }
}
