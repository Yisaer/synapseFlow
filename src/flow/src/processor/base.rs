//! Processor trait and implementations for stream processing
//!
//! This module defines the core Processor trait and concrete implementations:
//! - ControlSourceProcessor: Starting point for data flow, handles control signals
//! - DataSourceProcessor: Processes data from PhysicalDatasource
//! - ResultCollectProcessor: Final destination, prints received data

use crate::checkpoint::{CheckpointSnapshotCollector, OperatorSnapshot};
use crate::processor::{ControlSignal, ProcessorStats, StreamData};
use crate::runtime::TaskSpawner;
use futures::stream::{BoxStream, SelectAll};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
pub(crate) use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

/// Log received StreamData for debugging
pub fn log_received_data(processor_id: &str, data: &StreamData) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    match data {
        StreamData::Collection(collection) => {
            tracing::debug!(
                processor_id = %processor_id,
                rows = collection.num_rows(),
                "received collection"
            );
            // Print first few rows for debugging
            let rows = collection.rows();
            for (i, row) in rows.iter().take(3).enumerate() {
                tracing::debug!(processor_id = %processor_id, row_idx = i, row = ?row, "row");
            }
            if rows.len() > 3 {
                tracing::debug!(
                    processor_id = %processor_id,
                    remaining = rows.len() - 3,
                    "more rows"
                );
            }
        }
        StreamData::EncodedDelivery { flags, bytes } => {
            tracing::debug!(
                processor_id = %processor_id,
                flags = ?flags,
                bytes = bytes.len(),
                "received encoded delivery"
            );
        }
        StreamData::Bytes(payload) => {
            tracing::debug!(
                processor_id = %processor_id,
                bytes = payload.len(),
                "received bytes"
            );
        }
        StreamData::Control(signal) => {
            tracing::debug!(
                processor_id = %processor_id,
                signal = ?signal,
                "received control"
            );
        }
        StreamData::Watermark(ts) => {
            tracing::debug!(processor_id = %processor_id, ts = ?ts, "received watermark");
        }
        StreamData::Error(error) => {
            tracing::debug!(
                processor_id = %processor_id,
                message = %error.message,
                "received error"
            );
        }
    }
}

/// Log a broadcast receiver lag event.
///
/// This should be rare when all sends are routed through cooperative backpressure helpers.
pub fn log_broadcast_lagged(processor_id: &str, skipped: u64, context: &str) {
    tracing::warn!(
        processor_id = %processor_id,
        skipped = skipped,
        context = %context,
        "broadcast receiver lagged"
    );
}

/// Default buffer size for processor data broadcast channels.
pub(crate) const DEFAULT_DATA_CHANNEL_CAPACITY: usize = 16;

/// Default buffer size for processor control broadcast channels.
pub(crate) const DEFAULT_CONTROL_CHANNEL_CAPACITY: usize = 2;

pub(crate) fn normalize_channel_capacity(capacity: usize) -> usize {
    capacity.max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessorChannelCapacities {
    pub data: usize,
    pub control: usize,
    pub data_link_kind: LinkKind,
    pub control_link_kind: LinkKind,
}

impl ProcessorChannelCapacities {
    pub(crate) fn new(data: usize, control: usize) -> Self {
        Self {
            data: normalize_channel_capacity(data),
            control: normalize_channel_capacity(control),
            data_link_kind: LinkKind::Broadcast,
            control_link_kind: LinkKind::Broadcast,
        }
    }

    pub(crate) fn with_link_kind(mut self, kind: LinkKind) -> Self {
        self.data_link_kind = kind;
        self.control_link_kind = kind;
        self
    }
}

pub(crate) fn default_channel_capacities() -> ProcessorChannelCapacities {
    ProcessorChannelCapacities::new(
        DEFAULT_DATA_CHANNEL_CAPACITY,
        DEFAULT_CONTROL_CHANNEL_CAPACITY,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkKind {
    Broadcast,
    Mpsc,
}

pub(crate) enum LinkReceiver<T> {
    Broadcast(broadcast::Receiver<T>),
    Mpsc(mpsc::Receiver<T>),
}

impl<T> From<broadcast::Receiver<T>> for LinkReceiver<T> {
    fn from(receiver: broadcast::Receiver<T>) -> Self {
        LinkReceiver::Broadcast(receiver)
    }
}

impl<T> From<mpsc::Receiver<T>> for LinkReceiver<T> {
    fn from(receiver: mpsc::Receiver<T>) -> Self {
        LinkReceiver::Mpsc(receiver)
    }
}

impl<T> LinkReceiver<T>
where
    T: Clone,
{
    #[cfg(test)]
    pub(crate) async fn recv(&mut self) -> Result<T, ProcessorError> {
        match self {
            LinkReceiver::Broadcast(receiver) => receiver
                .recv()
                .await
                .map_err(|_| ProcessorError::ChannelClosed),
            LinkReceiver::Mpsc(receiver) => {
                receiver.recv().await.ok_or(ProcessorError::ChannelClosed)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<T, ProcessorError> {
        match self {
            LinkReceiver::Broadcast(receiver) => receiver
                .try_recv()
                .map_err(|_| ProcessorError::ChannelClosed),
            LinkReceiver::Mpsc(receiver) => receiver
                .try_recv()
                .map_err(|_| ProcessorError::ChannelClosed),
        }
    }
}

pub(crate) enum LinkSender<T> {
    Broadcast(broadcast::Sender<T>),
    Mpsc(mpsc::Sender<T>),
}

impl<T> Clone for LinkSender<T> {
    fn clone(&self) -> Self {
        match self {
            LinkSender::Broadcast(sender) => LinkSender::Broadcast(sender.clone()),
            LinkSender::Mpsc(sender) => LinkSender::Mpsc(sender.clone()),
        }
    }
}

pub(crate) struct LinkOutput<T> {
    sender: LinkSender<T>,
    mpsc_receiver: Option<Arc<Mutex<Option<mpsc::Receiver<T>>>>>,
}

impl<T> Clone for LinkOutput<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            mpsc_receiver: self.mpsc_receiver.clone(),
        }
    }
}

impl<T> LinkOutput<T>
where
    T: Clone + Send + 'static,
{
    pub(crate) fn new(kind: LinkKind, capacity: usize) -> Self {
        match kind {
            LinkKind::Broadcast => {
                let (sender, _) = broadcast::channel(normalize_channel_capacity(capacity));
                Self {
                    sender: LinkSender::Broadcast(sender),
                    mpsc_receiver: None,
                }
            }
            LinkKind::Mpsc => {
                let (sender, receiver) = mpsc::channel(normalize_channel_capacity(capacity));
                Self {
                    sender: LinkSender::Mpsc(sender),
                    mpsc_receiver: Some(Arc::new(Mutex::new(Some(receiver)))),
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn broadcast(capacity: usize) -> Self {
        Self::new(LinkKind::Broadcast, capacity)
    }

    pub(crate) fn subscribe(&self) -> Option<LinkReceiver<T>> {
        match &self.sender {
            LinkSender::Broadcast(sender) => Some(LinkReceiver::Broadcast(sender.subscribe())),
            LinkSender::Mpsc(_) => {
                let receiver = self.mpsc_receiver.as_ref()?;
                receiver.lock().ok()?.take().map(LinkReceiver::Mpsc)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn send(&self, item: T) -> Result<(), ProcessorError> {
        match &self.sender {
            LinkSender::Broadcast(sender) => sender
                .send(item)
                .map(|_| ())
                .map_err(|_| ProcessorError::ChannelClosed),
            LinkSender::Mpsc(sender) => sender
                .try_send(item)
                .map_err(|_| ProcessorError::ChannelClosed),
        }
    }
}

pub(crate) type ProcessorReadyReceiver = oneshot::Receiver<Result<(), ProcessorError>>;

pub(crate) struct ProcessorStart {
    pub(crate) handle: JoinHandle<Result<(), ProcessorError>>,
    ready: Option<ProcessorReadyReceiver>,
}

impl ProcessorStart {
    pub(crate) fn ready(handle: JoinHandle<Result<(), ProcessorError>>) -> Self {
        Self {
            handle,
            ready: None,
        }
    }

    pub(crate) fn failed(spawner: &TaskSpawner, err: ProcessorError) -> Self {
        let (ready_tx, ready_rx) = oneshot::channel();
        let task_err = err.clone();
        let _ = ready_tx.send(Err(err));
        Self {
            handle: spawner.spawn(async move { Err(task_err) }),
            ready: Some(ready_rx),
        }
    }

    pub(crate) fn with_ready(
        handle: JoinHandle<Result<(), ProcessorError>>,
        ready: ProcessorReadyReceiver,
    ) -> Self {
        Self {
            handle,
            ready: Some(ready),
        }
    }

    pub(crate) fn take_ready(&mut self) -> Option<ProcessorReadyReceiver> {
        self.ready.take()
    }
}

impl std::future::IntoFuture for ProcessorStart {
    type Output = <JoinHandle<Result<(), ProcessorError>> as std::future::Future>::Output;
    type IntoFuture = JoinHandle<Result<(), ProcessorError>>;

    fn into_future(self) -> Self::IntoFuture {
        self.handle
    }
}

/// Trait for all stream processors
///
/// Processors are the building blocks of the stream processing pipeline.
/// Each processor can have multiple inputs and multiple outputs, communicating
/// via tokio mpsc channels with StreamData.
pub(crate) trait Processor: Send + Sync {
    /// Get the processor identifier
    fn id(&self) -> &str;

    /// Return the stable semantic key used to match persisted checkpoint state.
    fn checkpoint_key(&self) -> &str {
        self.id()
    }

    /// Attach the optional in-memory snapshot collector used by checkpoint-aware participants.
    ///
    /// Ordinary processors keep the default no-op implementation and only forward checkpoint
    /// barriers. Stateful participants can override this method and use the collector from their
    /// data-channel checkpoint handling without serializing state on the hot path.
    fn set_checkpoint_snapshot_collector(
        &mut self,
        _collector: Option<Arc<CheckpointSnapshotCollector>>,
    ) {
    }

    /// Validate one previously committed snapshot without changing processor state.
    fn validate_checkpoint(&self, snapshot: &OperatorSnapshot) -> Result<(), ProcessorError> {
        Err(ProcessorError::InvalidConfiguration(format!(
            "processor `{}` does not support checkpoint state version {}",
            self.checkpoint_key(),
            snapshot.state_version
        )))
    }

    /// Restore one previously committed snapshot before the processor task starts.
    ///
    /// Processors without recoverable state reject snapshots by default. Stateful participants
    /// can override this method and restore their in-memory state without involving storage.
    fn restore_checkpoint(&mut self, snapshot: &OperatorSnapshot) -> Result<(), ProcessorError> {
        Err(ProcessorError::InvalidConfiguration(format!(
            "processor `{}` does not support checkpoint restore for state version {}",
            self.id(),
            snapshot.state_version
        )))
    }

    /// Discard checkpoint state staged before processor startup.
    fn clear_checkpoint_restore(&mut self) {}

    /// Start the processor asynchronously.
    ///
    /// Processors with external startup work should resolve the readiness receiver only after
    /// they can safely accept upstream data.
    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart;

    /// Get output channel senders (for connecting downstream processors)
    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>>;

    /// Subscribe to the processor's control signal output (high priority path)
    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>>;

    /// Add an input channel (connect upstream processor)
    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>;

    /// Add a control-signal input channel (connect upstream control path)
    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>;
}

/// Error type for processor operations
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessorError {
    /// Channel closed unexpectedly
    ChannelClosed,
    /// Processing error with message
    ProcessingError(String),
    /// Invalid configuration
    InvalidConfiguration(String),
    /// Timeout waiting for data
    Timeout,
}

impl std::fmt::Display for ProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorError::ChannelClosed => write!(f, "Channel closed unexpectedly"),
            ProcessorError::ProcessingError(msg) => write!(f, "Processing error: {}", msg),
            ProcessorError::InvalidConfiguration(msg) => {
                write!(f, "Invalid configuration: {}", msg)
            }
            ProcessorError::Timeout => write!(f, "Timeout waiting for data"),
        }
    }
}

impl std::error::Error for ProcessorError {}

/// Combined input stream built from one or more link receivers.
pub(crate) type ProcessorInputStream = FanInStream<StreamData>;
pub(crate) type ControlInputStream = FanInStream<ControlSignal>;

pub(crate) enum FanInStream<T> {
    Empty,
    SingleMpsc(mpsc::Receiver<T>),
    SingleBroadcast(BroadcastStream<T>),
    Many(SelectAll<BoxStream<'static, Result<T, BroadcastStreamRecvError>>>),
}

pub(crate) enum IndexedInput<T> {
    Item { upstream: usize, item: T },
    Closed,
}

pub(crate) struct PausableFanInStream<T> {
    inputs: Vec<PausableInput<T>>,
    active: Vec<bool>,
    closed: Vec<bool>,
    cursor: usize,
}

enum PausableInput<T> {
    Broadcast(BroadcastStream<T>),
    Mpsc(mpsc::Receiver<T>),
}

impl<T> PausableFanInStream<T> {
    pub(crate) fn pause(&mut self, upstream: usize) {
        if let Some(active) = self.active.get_mut(upstream) {
            *active = false;
        }
    }

    pub(crate) fn resume_all(&mut self) {
        for (active, closed) in self.active.iter_mut().zip(&self.closed) {
            if !closed {
                *active = true;
            }
        }
    }
}

impl<T> Stream for PausableFanInStream<T>
where
    T: Clone + Unpin + Send + 'static,
{
    type Item = Result<IndexedInput<T>, BroadcastStreamRecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let input_count = this.inputs.len();
        let mut has_open_input = false;

        for offset in 0..input_count {
            let upstream = (this.cursor + offset) % input_count;
            if this.closed[upstream] || !this.active[upstream] {
                if !this.closed[upstream] {
                    has_open_input = true;
                }
                continue;
            }
            has_open_input = true;

            let poll_result = match &mut this.inputs[upstream] {
                PausableInput::Broadcast(stream) => Pin::new(stream).poll_next(cx),
                PausableInput::Mpsc(receiver) => {
                    Pin::new(receiver).poll_recv(cx).map(|item| item.map(Ok))
                }
            };

            match poll_result {
                Poll::Ready(Some(Ok(item))) => {
                    this.cursor = (upstream + 1) % input_count;
                    return Poll::Ready(Some(Ok(IndexedInput::Item { upstream, item })));
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    this.cursor = (upstream + 1) % input_count;
                    return Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped))));
                }
                Poll::Ready(None) => {
                    this.closed[upstream] = true;
                    this.active[upstream] = false;
                    this.cursor = (upstream + 1) % input_count;
                    return Poll::Ready(Some(Ok(IndexedInput::Closed)));
                }
                Poll::Pending => {}
            }
        }

        if !has_open_input {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<T> FanInStream<T> {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            FanInStream::Empty => true,
            FanInStream::SingleMpsc(_) | FanInStream::SingleBroadcast(_) => false,
            FanInStream::Many(streams) => streams.is_empty(),
        }
    }
}

impl<T> Stream for FanInStream<T>
where
    T: Clone + Unpin + Send + 'static,
{
    type Item = Result<T, BroadcastStreamRecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            FanInStream::Empty => Poll::Ready(None),
            FanInStream::SingleMpsc(receiver) => {
                Pin::new(receiver).poll_recv(cx).map(|item| item.map(Ok))
            }
            FanInStream::SingleBroadcast(stream) => Pin::new(stream).poll_next(cx),
            FanInStream::Many(streams) => Pin::new(streams).poll_next(cx),
        }
    }
}

fn fan_in_receivers<T>(mut inputs: Vec<LinkReceiver<T>>) -> FanInStream<T>
where
    T: Clone + Unpin + Send + 'static,
{
    if inputs.is_empty() {
        return FanInStream::Empty;
    }
    if inputs.len() == 1 {
        return match inputs.pop().expect("single input checked above") {
            LinkReceiver::Broadcast(receiver) => {
                FanInStream::SingleBroadcast(BroadcastStream::new(receiver))
            }
            LinkReceiver::Mpsc(receiver) => FanInStream::SingleMpsc(receiver),
        };
    }

    let mut streams = SelectAll::new();
    for receiver in inputs {
        match receiver {
            LinkReceiver::Broadcast(receiver) => {
                streams.push(BroadcastStream::new(receiver).boxed())
            }
            LinkReceiver::Mpsc(receiver) => {
                streams.push(ReceiverStream::new(receiver).map(Ok).boxed());
            }
        }
    }
    FanInStream::Many(streams)
}

/// Convert a list of link receivers into a single input stream.
pub(crate) fn fan_in_streams(inputs: Vec<LinkReceiver<StreamData>>) -> ProcessorInputStream {
    fan_in_receivers(inputs)
}

pub(crate) fn fan_in_control_streams(
    inputs: Vec<LinkReceiver<ControlSignal>>,
) -> ControlInputStream {
    fan_in_receivers(inputs)
}

pub(crate) fn fan_in_streams_indexed<T>(inputs: Vec<LinkReceiver<T>>) -> PausableFanInStream<T>
where
    T: Clone + Unpin + Send + 'static,
{
    let input_count = inputs.len();
    PausableFanInStream {
        inputs: inputs
            .into_iter()
            .map(|input| match input {
                LinkReceiver::Broadcast(receiver) => {
                    PausableInput::Broadcast(BroadcastStream::new(receiver))
                }
                LinkReceiver::Mpsc(receiver) => PausableInput::Mpsc(receiver),
            })
            .collect(),
        active: vec![true; input_count],
        closed: vec![false; input_count],
        cursor: 0,
    }
}

/// Send data over a link while applying cooperative backpressure.
///
/// For a `broadcast` link, `tokio::broadcast` drops the oldest messages when the
/// channel is full; to avoid that we proactively wait until space becomes available
/// (or until there are no receivers left) before sending. For an `mpsc` link,
/// `.send().await` backpressures naturally, so we simply await it.
pub(crate) async fn send_with_backpressure(
    sender: &LinkOutput<StreamData>,
    capacity: usize,
    data: StreamData,
    stats: Option<&ProcessorStats>,
) -> Result<(), ProcessorError> {
    const BACKOFF: Duration = Duration::from_millis(1);
    let capacity = normalize_channel_capacity(capacity);
    let mut payload = Some(data);
    match &sender.sender {
        LinkSender::Broadcast(sender) => loop {
            if sender.receiver_count() == 0 || sender.len() < capacity {
                let value = payload.take().ok_or_else(|| {
                    ProcessorError::ProcessingError(
                        "send_with_backpressure payload state corrupted".to_string(),
                    )
                })?;
                sender
                    .send(value)
                    .map(|_| ())
                    .map_err(|_| ProcessorError::ChannelClosed)?;
                return Ok(());
            }
            if let Some(stats) = stats {
                // One tick per cooperative backpressure sleep.
                stats.record_send_backpressure_wait_tick();
            }
            sleep(BACKOFF).await;
        },
        LinkSender::Mpsc(sender) => {
            if sender.capacity() == 0 {
                if let Some(stats) = stats {
                    stats.record_send_backpressure_wait_tick();
                }
            }
            let value = payload.take().ok_or_else(|| {
                ProcessorError::ProcessingError(
                    "send_with_backpressure payload state corrupted".to_string(),
                )
            })?;
            sender
                .send(value)
                .await
                .map_err(|_| ProcessorError::ChannelClosed)
        }
    }
}

pub(crate) async fn send_control_with_backpressure(
    sender: &LinkOutput<ControlSignal>,
    capacity: usize,
    signal: ControlSignal,
) -> Result<(), ProcessorError> {
    const BACKOFF: Duration = Duration::from_millis(1);
    let capacity = normalize_channel_capacity(capacity);
    let mut payload = Some(signal);
    match &sender.sender {
        LinkSender::Broadcast(sender) => loop {
            if sender.receiver_count() == 0 || sender.len() < capacity {
                let value = payload.take().ok_or_else(|| {
                    ProcessorError::ProcessingError(
                        "send_control_with_backpressure payload state corrupted".to_string(),
                    )
                })?;
                sender
                    .send(value)
                    .map(|_| ())
                    .map_err(|_| ProcessorError::ChannelClosed)?;
                return Ok(());
            }
            sleep(BACKOFF).await;
        },
        LinkSender::Mpsc(sender) => {
            let value = payload.take().ok_or_else(|| {
                ProcessorError::ProcessingError(
                    "send_control_with_backpressure payload state corrupted".to_string(),
                )
            })?;
            sender
                .send(value)
                .await
                .map_err(|_| ProcessorError::ChannelClosed)
        }
    }
}
