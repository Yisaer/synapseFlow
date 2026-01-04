//! ResultCollectProcessor - final destination for data flow
//!
//! This processor receives data from upstream processors and forwards it to a single output.

use crate::processor::barrier::{align_control_signal, BarrierAligner};
use crate::processor::base::{fan_in_control_streams, fan_in_streams, log_received_data};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// ResultCollectProcessor - forwards received data to a single output
///
/// This processor acts as the final destination in the data flow. It:
/// - Receives StreamData from multiple upstream processors (multi-input)
/// - Forwards all received data to a single output channel (single-output)
/// - Can be used to collect results or forward to external systems
pub struct ResultCollectProcessor {
    /// Processor identifier
    id: String,
    /// Input channels for receiving data (multi-input)
    inputs: Vec<broadcast::Receiver<StreamData>>,
    /// Control input channels for high-priority signals
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    /// Single output channel for forwarding received data (single-output)
    output: Option<mpsc::Sender<StreamData>>,
    /// Broadcast sender for downstream subscriptions
    broadcast_output: broadcast::Sender<StreamData>,
    /// Broadcast sender for control signals
    broadcast_control_output: broadcast::Sender<ControlSignal>,
}

impl ResultCollectProcessor {
    /// Create a new ResultCollectProcessor
    pub fn new(id: impl Into<String>) -> Self {
        let (broadcast_output, _) =
            broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
        let (broadcast_control_output, _) =
            broadcast::channel(crate::processor::base::DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output: None,
            broadcast_output,
            broadcast_control_output,
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
}

impl Processor for ResultCollectProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let data_receivers = std::mem::take(&mut self.inputs);
        let expected_data_upstreams = data_receivers.len();
        let mut input_streams = fan_in_streams(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let expected_control_upstreams = control_receivers.len();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let mut data_barrier = BarrierAligner::new("data", expected_data_upstreams);
        let mut control_barrier = BarrierAligner::new("control", expected_control_upstreams);

        let output = self.output.take().ok_or_else(|| {
            ProcessorError::InvalidConfiguration(
                "ResultCollectProcessor output must be set before starting".to_string(),
            )
        });
        let broadcast_output = self.broadcast_output.clone();
        let broadcast_control_output = self.broadcast_control_output.clone();
        let processor_id = self.id.clone();
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
                                if let Some(signal) =
                                    align_control_signal(&mut control_barrier, control_signal)?
                                {
                                    let is_terminal = signal.is_terminal();
                                    let _ = broadcast_control_output.send(signal);
                                    if is_terminal {
                                        tracing::info!(processor_id = %processor_id, "received terminal signal (control)");
                                        tracing::info!(processor_id = %processor_id, "stopped");
                                        return Ok(());
                                    }
                                }
                                continue;
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                tracing::warn!(processor_id = %processor_id, skipped = skipped, "control channel lagged");
                                continue;
                            }
                            None => return Err(ProcessorError::ChannelClosed),
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                match data {
                                    StreamData::Control(control_signal) => {
                                        if let Some(signal) =
                                            align_control_signal(&mut data_barrier, control_signal)?
                                        {
                                            let is_terminal = signal.is_terminal();
                                            let out = StreamData::control(signal);
                                            let _ = broadcast_output.send(out.clone());
                                            output
                                                .send(out)
                                                .await
                                                .map_err(|_| ProcessorError::ChannelClosed)?;
                                            if is_terminal {
                                                tracing::info!(
                                                    processor_id = %processor_id,
                                                    "received terminal signal (data)"
                                                );
                                                return Ok(());
                                            }
                                        }
                                    }
                                    other => {
                                        let is_terminal = other.is_terminal();
                                        let _ = broadcast_output.send(other.clone());
                                        output
                                            .send(other)
                                            .await
                                            .map_err(|_| ProcessorError::ChannelClosed)?;
                                        if is_terminal {
                                            tracing::info!(
                                                processor_id = %processor_id,
                                                "received terminal item (data)"
                                            );
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                tracing::warn!(
                                    processor_id = %processor_id,
                                    skipped = skipped,
                                    "input lagged"
                                );
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
        Some(self.broadcast_output.subscribe())
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        self.inputs.push(receiver);
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        Some(self.broadcast_control_output.subscribe())
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        self.control_inputs.push(receiver);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::{BarrierControlSignal, InstantControlSignal};
    use tokio::time::{timeout, Duration};

    struct UpstreamHandles {
        data: Vec<broadcast::Sender<StreamData>>,
        control: Vec<broadcast::Sender<ControlSignal>>,
    }

    fn setup_processor(
        upstreams: usize,
    ) -> (
        ResultCollectProcessor,
        UpstreamHandles,
        mpsc::Receiver<StreamData>,
    ) {
        let mut processor = ResultCollectProcessor::new("test_result_collect");
        let (output_tx, output_rx) = mpsc::channel(16);
        processor.set_output(output_tx);

        let mut data = Vec::with_capacity(upstreams);
        let mut control = Vec::with_capacity(upstreams);
        for _ in 0..upstreams {
            let (data_tx, data_rx) = broadcast::channel(16);
            let (control_tx, control_rx) = broadcast::channel(16);
            processor.add_input(data_rx);
            processor.add_control_input(control_rx);
            data.push(data_tx);
            control.push(control_tx);
        }

        (processor, UpstreamHandles { data, control }, output_rx)
    }

    #[tokio::test]
    async fn data_channel_barrier_waits_until_all_upstreams_arrive() {
        let (mut processor, upstreams, mut output_rx) = setup_processor(2);
        let handle = processor.start();

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 1 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier (data upstream 0)"));

        assert!(
            timeout(Duration::from_millis(200), output_rx.recv())
                .await
                .is_err(),
            "barrier should not be forwarded before all upstreams arrive"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier (data upstream 1)"));

        let item = timeout(Duration::from_millis(200), output_rx.recv())
            .await
            .expect("timeout waiting for forwarded barrier")
            .expect("output channel closed");
        match item {
            StreamData::Control(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id,
            })) => {
                assert_eq!(barrier_id, 1);
            }
            other => panic!("unexpected output item: {}", other.description()),
        }

        assert!(
            timeout(Duration::from_millis(200), output_rx.recv())
                .await
                .is_err(),
            "barrier should be forwarded only once"
        );

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Instant(InstantControlSignal::StreamQuickEnd))
            .expect("send terminal control signal");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn control_channel_barrier_waits_until_all_upstreams_arrive() {
        let (mut processor, upstreams, mut output_rx) = setup_processor(2);
        let mut control_output = processor
            .subscribe_control_output()
            .expect("control output receiver");
        let handle = processor.start();

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 1 });
        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(barrier.clone())
            .expect("send barrier (control upstream 0)");

        assert!(
            timeout(Duration::from_millis(200), control_output.recv())
                .await
                .is_err(),
            "barrier should not be forwarded on control output before all upstreams arrive"
        );

        upstreams
            .control
            .get(1)
            .expect("upstream 1 control")
            .send(barrier.clone())
            .expect("send barrier (control upstream 1)");

        let received = timeout(Duration::from_millis(200), control_output.recv())
            .await
            .expect("timeout waiting for forwarded control barrier")
            .expect("control output channel closed");
        match received {
            ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id }) => {
                assert_eq!(barrier_id, 1);
            }
            other => panic!("unexpected control output signal: {other:?}"),
        }

        assert!(
            timeout(Duration::from_millis(200), control_output.recv())
                .await
                .is_err(),
            "barrier should be forwarded only once on control output"
        );

        upstreams
            .data
            .get(0)
            .expect("upstream 0 data")
            .send(StreamData::control(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd,
            )))
            .unwrap_or_else(|_| panic!("send terminal via data channel"));

        let _ = timeout(Duration::from_millis(200), output_rx.recv())
            .await
            .expect("timeout waiting for output drain");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }
}
