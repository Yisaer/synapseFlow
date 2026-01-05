//! ResultCollectProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and forwards it to a single output.

use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub(crate) trait OutputBusHook: Send + Sync {
    fn on_receive(&self, _processor_id: &str, _item: &StreamData) {}
}

#[derive(Debug, Default)]
pub(crate) struct ErrorLoggingHook;

impl OutputBusHook for ErrorLoggingHook {
    fn on_receive(&self, processor_id: &str, item: &StreamData) {
        let StreamData::Error(err) = item else {
            return;
        };
        tracing::error!(
            processor_id = %processor_id,
            source = err.source.as_deref(),
            message = %err.message,
            "pipeline emitted error"
        );
    }
}

/// ResultCollectProcessor - forwards received data to a single output
///
/// This processor acts as the final destination in the data flow. It:
/// - Receives StreamData from multiple upstream processors (multi-input)
/// - Forwards all received data to a single output channel (single-output)
/// - Forwards both data-channel and control-channel messages to the pipeline output
pub struct ResultCollectProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving data (multi-input)
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels for high-priority signals
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
    /// Hooks that observe items after they have been written to the output bus.
    bus_hooks: Vec<Arc<dyn OutputBusHook>>,
}

impl ResultCollectProcessor {
    /// Create a new ResultCollectProcessor
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output: None,
            bus_hooks: Vec::new(),
        }
    }

    /// Get the output receiver (for connecting to external systems)
    /// Returns None if output is not set
    pub fn output_receiver(&self) -> Option<&mpsc::Sender<StreamData>> {
        self.output.as_ref()
    }

    /// Set the downstream output channel (typically the pipeline output)
    pub fn set_output(&mut self, sender: mpsc::Sender<StreamData>) {
        self.output = Some(sender);
    }

    pub(crate) fn add_bus_hook(&mut self, hook: Arc<dyn OutputBusHook>) {
        self.bus_hooks.push(hook);
    }
}

impl Processor for ResultCollectProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        async fn forward_to_output_bus(
            output: &mpsc::Sender<StreamData>,
            processor_id: &str,
            bus_hooks: &[Arc<dyn OutputBusHook>],
            data: StreamData,
        ) -> Result<(), ProcessorError> {
            for hook in bus_hooks {
                hook.on_receive(processor_id, &data);
            }
            output
                .send(data)
                .await
                .map_err(|_| ProcessorError::ChannelClosed)?;
            Ok(())
        }

        let data_receivers = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let output = self.output.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ResultCollectProcessor output must be set before starting".to_string(),
            )
        });
        let processor_id = self.id.clone();
        let bus_hooks = self.bus_hooks.clone();
        tracing::info!(processor_id = %processor_id, "result collect processor starting");

        tokio::spawn(async move {
            let output = match output {
                Ok(output) => output,
                Err(e) => return Err(e),
            };

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                let out = StreamData::control(control_signal);
                                let is_terminal = out.is_terminal();
                                forward_to_output_bus(&output, &processor_id, &bus_hooks, out)
                                    .await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %processor_id, "received terminal signal (control)");
                                    tracing::info!(processor_id = %processor_id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "control input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                let is_terminal = data.is_terminal();
                                forward_to_output_bus(&output, &processor_id, &bus_hooks, data)
                                    .await?;
                                if is_terminal {
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        "received terminal item (data)"
                                    );
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "data input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
                        }
                    }
                }
            }
        })
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        None
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        None
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}
