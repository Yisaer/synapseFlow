//! BarrierProcessor - aligns barrier control signals across multiple upstreams.
//!
//! This processor is a dedicated operator inserted by physical plan optimization for fan-in nodes
//! (`children.len() > 1`). It forwards all data downstream, while aligning barrier-style control
//! signals per-channel before forwarding them.

use crate::planner::physical::DataDomain;
use crate::processor::barrier::{align_control_signal, BarrierAligner};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams_indexed,
    log_broadcast_lagged, log_received_data, send_control_with_backpressure,
    send_with_backpressure, IndexedInput, LinkOutput, LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::data_metrics::{PassthroughMeasurement, PassthroughMetrics, BARRIER_METRICS};
use crate::processor::{
    BarrierControlSignal, BarrierControlSignalKind, ControlSignal, Processor, ProcessorError,
    ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

struct PendingCheckpoint {
    checkpoint_id: u64,
    kind: BarrierControlSignalKind,
    arrived: Vec<bool>,
    signal: BarrierControlSignal,
}

enum CheckpointDataAction {
    Forward {
        data: StreamData,
        measurement: PassthroughMeasurement,
    },
    Pause,
    Complete {
        signal: BarrierControlSignal,
    },
}

struct DataForwardContext<'a> {
    data_barrier: &'a mut BarrierAligner,
    output: &'a LinkOutput<StreamData>,
    channel_capacity: usize,
    stats: &'a ProcessorStats,
    metrics: &'a PassthroughMetrics,
}

fn handle_checkpoint_data(
    pending: &mut Option<PendingCheckpoint>,
    expected_upstreams: usize,
    upstream: usize,
    data: StreamData,
    measurement: PassthroughMeasurement,
    data_barrier_pending: bool,
) -> Result<CheckpointDataAction, ProcessorError> {
    if upstream >= expected_upstreams {
        return Err(ProcessorError::InvalidConfiguration(format!(
            "checkpoint barrier received from invalid upstream index {upstream}"
        )));
    }

    let checkpoint = match &data {
        StreamData::Control(ControlSignal::Barrier(
            barrier @ BarrierControlSignal::Checkpoint { .. },
        )) => Some(barrier),
        _ => None,
    };

    if let Some(state) = pending.as_mut() {
        if state.arrived[upstream] {
            return Err(ProcessorError::ProcessingError(format!(
                "received data from paused upstream {upstream} while waiting for checkpoint_id={}",
                state.checkpoint_id
            )));
        }

        if let Some(barrier) = checkpoint {
            if barrier.barrier_id() != state.checkpoint_id || barrier.kind() != state.kind {
                return Err(ProcessorError::ProcessingError(format!(
                    "checkpoint barrier overlap: pending checkpoint_id={}, got checkpoint_id={}",
                    state.checkpoint_id,
                    barrier.barrier_id()
                )));
            }

            state.arrived[upstream] = true;
            if state.arrived.iter().all(|arrived| *arrived) {
                let state = pending.take().expect("pending checkpoint state must exist");
                return Ok(CheckpointDataAction::Complete {
                    signal: state.signal,
                });
            }
            return Ok(CheckpointDataAction::Pause);
        }

        if matches!(data, StreamData::Control(ControlSignal::Barrier(_))) {
            return Err(ProcessorError::ProcessingError(format!(
                "barrier overlap while waiting for checkpoint_id={}",
                state.checkpoint_id
            )));
        }

        return Ok(CheckpointDataAction::Forward { data, measurement });
    }

    let Some(barrier) = checkpoint else {
        return Ok(CheckpointDataAction::Forward { data, measurement });
    };

    if data_barrier_pending {
        return Err(ProcessorError::ProcessingError(format!(
            "checkpoint barrier overlaps with pending barrier on data channel: checkpoint_id={}",
            barrier.barrier_id()
        )));
    }

    let mut arrived = vec![false; expected_upstreams];
    arrived[upstream] = true;
    let signal = barrier.clone();
    if arrived.iter().all(|arrived| *arrived) {
        return Ok(CheckpointDataAction::Complete { signal });
    }

    *pending = Some(PendingCheckpoint {
        checkpoint_id: barrier.barrier_id(),
        kind: barrier.kind(),
        arrived,
        signal,
    });
    Ok(CheckpointDataAction::Pause)
}

async fn forward_data_item(
    output: &LinkOutput<StreamData>,
    channel_capacity: usize,
    stats: &ProcessorStats,
    metrics: &PassthroughMetrics,
    data: StreamData,
    measurement: PassthroughMeasurement,
) -> Result<bool, ProcessorError> {
    let is_terminal = data.is_terminal();
    send_with_backpressure(output, channel_capacity, data, Some(stats)).await?;
    metrics.record_output(stats, measurement);
    Ok(is_terminal)
}

async fn forward_data(
    context: &mut DataForwardContext<'_>,
    data: StreamData,
    measurement: PassthroughMeasurement,
) -> Result<bool, ProcessorError> {
    let data = match data {
        StreamData::Control(control_signal) => {
            let Some(signal) = align_control_signal(context.data_barrier, control_signal)? else {
                return Ok(false);
            };
            StreamData::control(signal)
        }
        other => other,
    };
    forward_data_item(
        context.output,
        context.channel_capacity,
        context.stats,
        context.metrics,
        data,
        measurement,
    )
    .await
}

/// BarrierProcessor forwards all data and aligns barrier control signals per channel.
pub struct BarrierProcessor {
    id: String,
    expected_upstreams: usize,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    input_domain: DataDomain,
    stats: Arc<ProcessorStats>,
}

impl BarrierProcessor {
    pub fn new(id: impl Into<String>, expected_upstreams: usize) -> Self {
        Self::new_with_channel_capacities(id, expected_upstreams, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        expected_upstreams: usize,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            expected_upstreams,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            input_domain: DataDomain::Collection,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    pub(crate) fn set_input_domain(&mut self, input_domain: DataDomain) {
        self.input_domain = input_domain;
    }
}

impl Processor for BarrierProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let id = self.id.clone();
        let expected_upstreams = self.expected_upstreams;

        let data_receivers = std::mem::take(&mut self.inputs);
        let expected_data_upstreams = data_receivers.len();
        let mut input_streams = fan_in_streams_indexed(data_receivers);

        let control_receivers = std::mem::take(&mut self.control_inputs);
        let expected_control_upstreams = control_receivers.len();
        let mut control_streams = fan_in_control_streams(control_receivers);
        let control_active = !control_streams.is_empty();

        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let metrics = PassthroughMetrics::new(stats.as_ref(), BARRIER_METRICS, self.input_domain);

        tracing::info!(
            processor_id = %id,
            expected_upstreams = expected_upstreams,
            "barrier processor starting"
        );

        ProcessorStart::ready(spawner.spawn(async move {
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
            let mut pending_checkpoint = None;

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
                                    send_control_with_backpressure(
                                        &control_output,
                                        channel_capacities.control,
                                        signal,
                                    )
                                    .await?;
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
                            Some(Ok(IndexedInput::Item { upstream, item: data })) => {
                                log_received_data(&id, &data);
                                let measurement = metrics.record_input(stats.as_ref(), &data)?;
                                let action = handle_checkpoint_data(
                                    &mut pending_checkpoint,
                                    expected_data_upstreams,
                                    upstream,
                                    data,
                                    measurement,
                                    data_barrier.is_pending(),
                                )?;
                                match action {
                                    CheckpointDataAction::Pause => {
                                        input_streams.pause(upstream);
                                    }
                                    CheckpointDataAction::Complete { signal } => {
                                        let terminal = forward_data_item(
                                            &output,
                                            channel_capacities.data,
                                            stats.as_ref(),
                                            &metrics,
                                            StreamData::control(ControlSignal::Barrier(signal)),
                                            PassthroughMeasurement::Other,
                                        )
                                        .await?;
                                        if terminal {
                                            tracing::info!(processor_id = %id, "received terminal signal (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                        input_streams.resume_all();
                                    }
                                    CheckpointDataAction::Forward { data, measurement } => {
                                        let mut context = DataForwardContext {
                                            data_barrier: &mut data_barrier,
                                            output: &output,
                                            channel_capacity: channel_capacities.data,
                                            stats: stats.as_ref(),
                                            metrics: &metrics,
                                        };
                                        if forward_data(&mut context, data, measurement).await? {
                                            tracing::info!(processor_id = %id, "received terminal signal (data)");
                                            tracing::info!(processor_id = %id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Ok(IndexedInput::Closed)) => {
                                if pending_checkpoint.is_some() {
                                    return Err(ProcessorError::ChannelClosed);
                                }
                                return Err(ProcessorError::ChannelClosed);
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
        }))
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointMode;
    use crate::processor::base::{DEFAULT_CONTROL_CHANNEL_CAPACITY, DEFAULT_DATA_CHANNEL_CAPACITY};
    use crate::processor::{BarrierControlSignal, InstantControlSignal};
    use crate::runtime::TaskSpawner;
    use std::time::SystemTime;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build test tokio runtime"),
        )
    }

    struct Upstreams {
        data: Vec<LinkOutput<StreamData>>,
        control: Vec<LinkOutput<ControlSignal>>,
    }

    fn setup_processor(
        upstreams: usize,
    ) -> (
        BarrierProcessor,
        Upstreams,
        LinkReceiver<StreamData>,
        LinkReceiver<ControlSignal>,
    ) {
        let mut processor = BarrierProcessor::new("test_barrier", upstreams);

        let mut data = Vec::with_capacity(upstreams);
        let mut control = Vec::with_capacity(upstreams);
        for _ in 0..upstreams {
            let data_tx = LinkOutput::broadcast(DEFAULT_DATA_CHANNEL_CAPACITY);
            let control_tx = LinkOutput::broadcast(DEFAULT_CONTROL_CHANNEL_CAPACITY);
            let data_rx = data_tx.subscribe().expect("data receiver");
            let control_rx = control_tx.subscribe().expect("control receiver");
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

    // coverage-covers: processor.barrier.alignment
    #[tokio::test]
    async fn data_channel_barrier_waits_until_all_upstreams_arrive() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

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
    async fn checkpoint_pauses_post_barrier_data_until_all_upstreams_arrive() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::checkpoint(10, CheckpointMode::Continue))
            .expect("send checkpoint (data upstream 0)");
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::watermark(SystemTime::UNIX_EPOCH))
            .expect("send post-barrier data (data upstream 0)");

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "post-barrier data must stay paused until all upstreams arrive"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1")
            .send(StreamData::checkpoint(10, CheckpointMode::Continue))
            .expect("send checkpoint (data upstream 1)");

        let barrier = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for aligned checkpoint")
            .expect("output channel closed");
        assert_eq!(
            barrier
                .as_control()
                .and_then(ControlSignal::checkpoint_mode),
            Some(CheckpointMode::Continue)
        );

        let post_barrier_data = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for post-barrier data")
            .expect("output channel closed");
        assert!(matches!(
            post_barrier_data,
            StreamData::Watermark(SystemTime::UNIX_EPOCH)
        ));

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
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, mut control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

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

    #[tokio::test]
    async fn terminal_barrier_waits_until_all_data_upstreams_arrive_before_forwarding() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        let terminal =
            ControlSignal::Barrier(BarrierControlSignal::StreamGracefulEnd { barrier_id: 9 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0")
            .send(StreamData::control(terminal.clone()))
            .unwrap_or_else(|_| panic!("send terminal barrier (data upstream 0)"));

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "terminal barrier should stay pending until all upstreams arrive"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1")
            .send(StreamData::control(terminal.clone()))
            .unwrap_or_else(|_| panic!("send terminal barrier (data upstream 1)"));

        let item = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for forwarded terminal barrier")
            .expect("output channel closed");
        match item {
            StreamData::Control(ControlSignal::Barrier(
                BarrierControlSignal::StreamGracefulEnd { barrier_id },
            )) => assert_eq!(barrier_id, 9),
            other => panic!("unexpected output item: {}", other.description()),
        }

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to stop")
            .expect("join error");
        assert!(result.is_ok(), "processor should stop cleanly: {result:?}");
    }

    #[tokio::test]
    async fn overlapping_barriers_on_same_channel_return_error() {
        let spawner = test_spawner();
        let (mut processor, upstreams, _out, _control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id: 1,
            }))
            .expect("send first pending barrier");
        upstreams
            .control
            .get(1)
            .expect("upstream 1 control")
            .send(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id: 2,
            }))
            .expect("send overlapping barrier");

        let result = timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout waiting for processor to fail")
            .expect("join error");
        match result {
            Err(ProcessorError::ProcessingError(message)) => {
                assert!(
                    message.contains("barrier overlap on control channel"),
                    "unexpected overlap error: {message}"
                );
            }
            other => panic!("expected overlap error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn barrier_alignment_state_is_isolated_per_channel() {
        let spawner = test_spawner();
        let (mut processor, upstreams, mut out, mut control_out) = setup_processor(2);
        let handle = processor.start(&spawner);

        let barrier = ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id: 11 });
        upstreams
            .data
            .get(0)
            .expect("upstream 0 data")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier on data upstream 0"));
        upstreams
            .control
            .get(0)
            .expect("upstream 0 control")
            .send(barrier.clone())
            .expect("send barrier on control upstream 0");

        assert!(
            timeout(Duration::from_millis(200), out.recv())
                .await
                .is_err(),
            "data barrier should not complete from control-channel arrivals"
        );
        assert!(
            timeout(Duration::from_millis(200), control_out.recv())
                .await
                .is_err(),
            "control barrier should not complete from data-channel arrivals"
        );

        upstreams
            .data
            .get(1)
            .expect("upstream 1 data")
            .send(StreamData::control(barrier.clone()))
            .unwrap_or_else(|_| panic!("send barrier on data upstream 1"));
        upstreams
            .control
            .get(1)
            .expect("upstream 1 control")
            .send(barrier.clone())
            .expect("send barrier on control upstream 1");

        let data_item = timeout(Duration::from_millis(200), out.recv())
            .await
            .expect("timeout waiting for data barrier")
            .expect("output channel closed");
        match data_item {
            StreamData::Control(ControlSignal::Barrier(BarrierControlSignal::SyncTest {
                barrier_id,
            })) => assert_eq!(barrier_id, 11),
            other => panic!("unexpected data output item: {}", other.description()),
        }

        let control_item = timeout(Duration::from_millis(200), control_out.recv())
            .await
            .expect("timeout waiting for control barrier")
            .expect("control output channel closed");
        match control_item {
            ControlSignal::Barrier(BarrierControlSignal::SyncTest { barrier_id }) => {
                assert_eq!(barrier_id, 11);
            }
            other => panic!("unexpected control output signal: {other:?}"),
        }

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
}
