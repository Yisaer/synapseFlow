//! BarrierProcessor - aligns barrier control signals across multiple upstreams.
//!
//! This processor is a dedicated operator inserted by physical plan optimization for fan-in nodes
//! (`children.len() > 1`). It forwards all data downstream, while aligning barrier-style control
//! signals per-channel before forwarding them.

use crate::processor::barrier::{align_control_signal, BarrierAligner};
use crate::processor::base::{
    attach_stats_to_collect_barrier, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
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
    stats: Arc<ProcessorStats>,
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
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
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
        let stats = Arc::clone(&self.stats);

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
                                    let signal = attach_stats_to_collect_barrier(signal, &id, &stats);
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
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::Control(control_signal) => {
                                        if let Some(signal) =
                                            align_control_signal(&mut data_barrier, control_signal)?
                                        {
                                            let signal = attach_stats_to_collect_barrier(signal, &id, &stats);
                                            let is_terminal = signal.is_terminal();
                                            let out = StreamData::control(signal);
                                            send_with_backpressure(&output, out).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::{BarrierControlSignal, InstantControlSignal};
    use tokio::time::{timeout, Duration};

    struct Upstreams {
        data: Vec<broadcast::Sender<StreamData>>,
        control: Vec<broadcast::Sender<ControlSignal>>,
    }

    fn setup_processor(
        upstreams: usize,
    ) -> (
        BarrierProcessor,
        Upstreams,
        broadcast::Receiver<StreamData>,
        broadcast::Receiver<ControlSignal>,
    ) {
        let mut processor = BarrierProcessor::new("test_barrier", upstreams);

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

        let out = processor.subscribe_output().expect("output receiver");
        let control_out = processor
            .subscribe_control_output()
            .expect("control output receiver");

        (processor, Upstreams { data, control }, out, control_out)
    }

    #[tokio::test]
    async fn data_channel_barrier_waits_until_all_upstreams_arrive() {
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start();

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 1 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier (data upstream 0)"));

        assert!(
            timeout(Duration::from_millis(200), out.recv())
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

        let item = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for forwarded barrier")
            .expect("output channel closed");
        match item {
            StreamData::Control(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id,
            })) => assert_eq!(barrier_id, 1),
            other => panic!("unexpected output item: {}", other.description()),
        }

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "barrier should be forwarded only once"
        );

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd { signal_id: 0 },
            ))
            .expect("send terminal control signal");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn control_channel_barrier_waits_until_all_upstreams_arrive() {
        let (mut processor, upstreams, mut out, mut control_out) = setup_processor(2);
        let handle = processor.start();

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 1 });
        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(barrier.clone())
            .expect("send barrier (control upstream 0)");

        assert!(
            timeout(Duration::from_millis(200), control_out.recv())
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

        let received = timeout(Duration::from_millis(200), control_out.recv())
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
            timeout(Duration::from_millis(200), control_out.recv())
                .await
                .is_err(),
            "barrier should be forwarded only once on control output"
        );

        upstreams
            .data
            .get(0)
            .expect("upstream 0 data")
            .send(StreamData::control(ControlSignal::Instant(
                InstantControlSignal::StreamQuickEnd { signal_id: 0 },
            )))
            .unwrap_or_else(|_| panic!("send terminal via data channel"));

        let _ = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for output drain");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }
}
