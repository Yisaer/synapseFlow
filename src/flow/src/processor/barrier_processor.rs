//! BarrierProcessor - aligns barrier control signals across multiple upstreams.
//!
//! This processor is a dedicated operator inserted by physical plan optimization for fan-in nodes
//! (`children.len() > 1`). It forwards all data downstream, while aligning barrier-style control
//! signals per-channel before forwarding them.

use crate::processor::barrier::{align_control_signal, BarrierAligner};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// BarrierProcessor forwards all data and aligns barrier control signals per channel.
pub struct BarrierProcessor {
    id: String,
    expected_upstreams: usize,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl BarrierProcessor {
    pub fn new(id: impl Into<String>, expected_upstreams: usize) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            expected_upstreams,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }
}

impl Processor for BarrierProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let expected_upstreams = self.expected_upstreams;

        let data_receivers = std::mem::take(&mut self.inputs);
        let expected_data_upstreams = data_receivers.len();
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let expected_control_upstreams = control_receivers.len();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let output = self.output.clone();
        let control_output = self.control_output.clone();

        tracing::info!(
            processor_id = %id,
            expected_upstreams = expected_upstreams,
            "barrier processor starting"
        );

        tokio::spawn(async move {
            if expected_upstreams == 0 {
                return Err(ProcessorError::InvalidConfiguration(
                    "BarrierProcessor expected_upstreams must be > 0".to_string(),
                ));
            }
            if expected_data_upstreams != expected_upstreams
                || expected_control_upstreams != expected_upstreams
            {
                return Err(ProcessorError::InvalidConfiguration(format!(
                    "BarrierProcessor upstream mismatch: expected_upstreams={}, data_upstreams={}, control_upstreams={}",
                    expected_upstreams, expected_data_upstreams, expected_control_upstreams
                )));
            }

            let mut data_barrier = BarrierAligner::new("data", expected_data_upstreams);
            let mut control_barrier = BarrierAligner::new("control", expected_control_upstreams);

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                if let Some(signal) =
                                    align_control_signal(&mut control_barrier, control_signal)?
                                {
                                    let is_terminal = signal.is_terminal();
                                    send_control_with_backpressure(&control_output, signal).await?;
                                    if is_terminal {
                                        tracing::info!(processor_id = %id, "received terminal signal (control)");
                                        tracing::info!(processor_id = %id, "stopped");
                                        return Ok(());
                                    }
                                }
                                continue;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "barrier control input");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&id, &data);
                                match data {
                                    StreamData::Control(control_signal) => {
                                        if let Some(signal) =
                                            align_control_signal(&mut data_barrier, control_signal)?
                                        {
                                            let is_terminal = signal.is_terminal();
                                            send_with_backpressure(&output, StreamData::control(signal)).await?;
                                            if is_terminal {
                                                tracing::info!(processor_id = %id, "received terminal signal (data)");
                                                tracing::info!(processor_id = %id, "stopped");
                                                return Ok(());
                                            }
                                        }
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        send_with_backpressure(&output, other).await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %id, "received terminal item (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&id, skipped, "barrier data input");
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
