//! SinkProcessor - routes collections to SinkConnectors and forwards results.
use crate::connector::{SinkConnector, SinkConnectorError};
use crate::model::Collection;
use crate::planner::sink::SinkRetryConfig;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, EncodedDeliveryFlags, Processor, ProcessorError, ProcessorStart, ProcessorStats,
    StreamData,
};
use crate::runtime::TaskSpawner;
use bytes::Bytes;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{sleep_until, timeout, Instant};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[cfg(test)]
#[path = "sink_processor_retry_tests.rs"]
mod sink_processor_retry_tests;

const SINK_READY_TIMEOUT: Duration = Duration::from_secs(2);

enum ControlAction {
    Continue,
    Deactivate,
    Terminal(ControlSignal),
}

struct CompletedDelivery {
    payload: Bytes,
    forward_events: Vec<(EncodedDeliveryFlags, Bytes)>,
}

enum AccumulatorAction {
    Pending,
    Aborted(Option<(EncodedDeliveryFlags, Bytes)>),
    Completed(CompletedDelivery),
}

struct DeliveryAccumulator {
    buffer: Vec<u8>,
    forward_events: Vec<(EncodedDeliveryFlags, Bytes)>,
    active: bool,
}

impl DeliveryAccumulator {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            forward_events: Vec::new(),
            active: false,
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.forward_events.clear();
        self.active = false;
    }

    fn push(
        &mut self,
        flags: EncodedDeliveryFlags,
        bytes: Bytes,
        forward_data: bool,
        max_delivery_bytes: Option<usize>,
    ) -> Result<AccumulatorAction, ProcessorError> {
        if flags.contains(EncodedDeliveryFlags::ABORT) {
            self.clear();
            let event = forward_data.then_some((flags, bytes));
            return Ok(AccumulatorAction::Aborted(event));
        }

        if flags.contains(EncodedDeliveryFlags::START) {
            if self.active {
                self.clear();
                return Err(ProcessorError::ProcessingError(
                    "encoded delivery protocol error: START received while delivery is active"
                        .to_string(),
                ));
            }
            self.active = true;
        }

        if !self.active {
            return Err(ProcessorError::ProcessingError(
                "encoded delivery protocol error: chunk received without active delivery"
                    .to_string(),
            ));
        }

        if let Some(max_delivery_bytes) = max_delivery_bytes {
            let next_len = self.buffer.len().saturating_add(bytes.len());
            if next_len > max_delivery_bytes {
                self.clear();
                return Err(ProcessorError::ProcessingError(format!(
                    "encoded delivery size {next_len} exceeds connector limit {max_delivery_bytes}"
                )));
            }
        }

        self.buffer.extend_from_slice(bytes.as_ref());
        if forward_data {
            self.forward_events.push((flags, bytes));
        }

        if flags.contains(EncodedDeliveryFlags::END) {
            self.active = false;
            return Ok(AccumulatorAction::Completed(CompletedDelivery {
                payload: std::mem::take(&mut self.buffer).into(),
                forward_events: std::mem::take(&mut self.forward_events),
            }));
        }

        Ok(AccumulatorAction::Pending)
    }
}

struct PendingDelivery {
    payload: Bytes,
    forward_events: Vec<(EncodedDeliveryFlags, Bytes)>,
    attempt: usize,
    retry_at: Option<Instant>,
    backoff_ms: u64,
}

impl PendingDelivery {
    fn new(completed: CompletedDelivery, retry_config: &SinkRetryConfig) -> Self {
        Self {
            payload: completed.payload,
            forward_events: completed.forward_events,
            attempt: 0,
            retry_at: None,
            backoff_ms: retry_config.initial_backoff_ms,
        }
    }
}

enum DeliveryAttemptOutcome {
    Delivered,
    RetryPending,
    Dropped,
}

struct ConnectorBinding {
    connector: Box<dyn SinkConnector>,
    active_delivery: bool,
}

impl ConnectorBinding {
    async fn ready(&mut self) -> Result<(), ProcessorError> {
        self.connector
            .ready()
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    async fn publish_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        self.connector
            .send_collection(collection)
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    async fn close(&mut self) -> Result<(), ProcessorError> {
        if self.active_delivery {
            self.connector.abort_delivery().await;
            self.active_delivery = false;
        }
        self.connector
            .close()
            .await
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
    }

    /// Attempt a full single-shot delivery: start → write_chunk(payload) → finish.
    /// On any error, aborts the delivery and returns the classified error.
    async fn attempt_delivery(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        self.connector.start_delivery().await?;
        self.active_delivery = true;
        if let Err(err) = self.connector.write_chunk(payload).await {
            self.connector.abort_delivery().await;
            self.active_delivery = false;
            return Err(err);
        }
        match self.connector.finish_delivery().await {
            Ok(_) => {
                self.active_delivery = false;
                Ok(())
            }
            Err(err) => {
                self.connector.abort_delivery().await;
                self.active_delivery = false;
                Err(err)
            }
        }
    }

    async fn abort_active_delivery(&mut self) {
        if self.active_delivery {
            self.connector.abort_delivery().await;
            self.active_delivery = false;
        }
    }
}

/// Processor that fans out collections to registered sink connectors.
///
/// Each encoded delivery is accumulated into a single payload and sent to
/// the connector in one shot. Delivery failures are retried with exponential
/// backoff. Exhausted retries drop the delivery and processing continues.
pub struct SinkProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    connector: Option<ConnectorBinding>,
    forward_to_result: bool,
    stats: Arc<ProcessorStats>,

    retry_config: SinkRetryConfig,
}

impl SinkProcessor {
    /// Create a new sink processor with the provided identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self::new_with_channel_capacities(id, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            connector: None,
            forward_to_result: false,
            stats: Arc::new(ProcessorStats::default()),
            retry_config: SinkRetryConfig::default(),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    /// Enable forwarding collections/control signals to downstream consumers (tests).
    pub fn enable_result_forwarding(&mut self) {
        self.forward_to_result = true;
    }

    /// Disable forwarding to downstream consumers (default for production).
    pub fn disable_result_forwarding(&mut self) {
        self.forward_to_result = false;
    }

    /// Register a connector binding.
    pub fn add_connector(&mut self, connector: Box<dyn SinkConnector>) {
        self.connector = Some(ConnectorBinding {
            connector,
            active_delivery: false,
        });
    }

    /// Set the retry configuration for this sink processor.
    pub fn set_retry_config(&mut self, config: SinkRetryConfig) {
        self.retry_config = config;
    }

    /// Maximum number of delivery attempts including the first one.
    fn effective_max_attempts(retry_config: &SinkRetryConfig) -> usize {
        retry_config.max_attempts.unwrap_or(1).max(1)
    }

    async fn forward_encoded_events(
        output: &LinkOutput<StreamData>,
        data_channel_capacity: usize,
        stats: &ProcessorStats,
        events: Vec<(EncodedDeliveryFlags, Bytes)>,
    ) -> Result<(), ProcessorError> {
        let total_len = events.iter().map(|(_, bytes)| bytes.len()).sum();
        let mut payload = Vec::with_capacity(total_len);
        for (_, bytes) in events {
            payload.extend_from_slice(bytes.as_ref());
        }
        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_single(payload),
            Some(stats),
        )
        .await
    }

    async fn forward_control_terminal(
        control_output: &LinkOutput<ControlSignal>,
        control_channel_capacity: usize,
        connector: &mut ConnectorBinding,
        signal: ControlSignal,
    ) -> Result<(), ProcessorError> {
        send_control_with_backpressure(control_output, control_channel_capacity, signal).await?;
        Self::handle_terminal(connector).await
    }

    /// Attempt to send the current payload to the connector and handle the result.
    async fn handle_delivery_attempt(
        processor_id: &str,
        stats: &ProcessorStats,
        connector: &mut ConnectorBinding,
        retry_config: &SinkRetryConfig,
        pending: &mut PendingDelivery,
    ) -> DeliveryAttemptOutcome {
        pending.attempt += 1;
        pending.retry_at = None;

        let handle_start = Instant::now();
        match connector.attempt_delivery(&pending.payload).await {
            Ok(()) => {
                stats.record_out(1);
                stats.record_handle_duration(handle_start.elapsed());
                DeliveryAttemptOutcome::Delivered
            }
            Err(err) if pending.attempt < Self::effective_max_attempts(retry_config) => {
                tracing::warn!(
                    processor_id = %processor_id,
                    attempt = %pending.attempt,
                    max_attempts = %Self::effective_max_attempts(retry_config),
                    next_backoff_ms = %pending.backoff_ms,
                    error = %err,
                    "sink delivery failed, scheduling retry"
                );
                stats.record_error(err.to_string());
                let backoff = pending.backoff_ms;
                pending.retry_at = Some(Instant::now() + Duration::from_millis(backoff));
                pending.backoff_ms = pending.backoff_ms.saturating_mul(2).min(
                    retry_config
                        .max_backoff_ms
                        .max(retry_config.initial_backoff_ms),
                );
                stats.record_handle_duration(handle_start.elapsed());
                DeliveryAttemptOutcome::RetryPending
            }
            Err(err) => {
                tracing::error!(
                    processor_id = %processor_id,
                    attempt = %pending.attempt,
                    max_attempts = %Self::effective_max_attempts(retry_config),
                    error = %err,
                    "sink delivery failed, dropping delivery"
                );
                stats.record_handle_duration(handle_start.elapsed());
                stats.record_error(err.to_string());
                DeliveryAttemptOutcome::Dropped
            }
        }
    }

    // ── control / data handlers ──

    async fn handle_collection(
        connector: &mut ConnectorBinding,
        collection: &dyn Collection,
    ) -> Result<(), ProcessorError> {
        connector.publish_collection(collection).await?;
        Ok(())
    }

    async fn handle_terminal(connector: &mut ConnectorBinding) -> Result<(), ProcessorError> {
        connector.close().await
    }

    async fn attempt_ready_with_timeout(
        connector: &mut ConnectorBinding,
        timeout_duration: Duration,
    ) -> Result<(), String> {
        match timeout(timeout_duration, connector.ready()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.to_string()),
            Err(_) => Err(format!(
                "sink connector ready timeout after {:?}",
                timeout_duration
            )),
        }
    }

    async fn handle_control_item(
        processor_id: &str,
        control_output: &LinkOutput<ControlSignal>,
        control_channel_capacity: usize,
        item: Option<Result<ControlSignal, BroadcastStreamRecvError>>,
    ) -> Result<ControlAction, ProcessorError> {
        let Some(Ok(control_signal)) = item else {
            return Ok(ControlAction::Deactivate);
        };
        let is_terminal = control_signal.is_terminal();
        if is_terminal {
            tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
            return Ok(ControlAction::Terminal(control_signal));
        }
        send_control_with_backpressure(control_output, control_channel_capacity, control_signal)
            .await?;
        Ok(ControlAction::Continue)
    }
}

impl Processor for SinkProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let forward_data = self.forward_to_result;
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let retry_config = self.retry_config.clone();

        let Some(mut connector) = self.connector.take() else {
            return ProcessorStart::failed(
                spawner,
                ProcessorError::InvalidConfiguration("sink connector missing".to_string()),
            );
        };
        let processor_id = self.id.clone();
        let max_delivery_bytes = connector.connector.max_delivery_bytes();

        tracing::info!(processor_id = %processor_id, "sink processor starting");

        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = spawner.spawn(async move {
            match Self::attempt_ready_with_timeout(&mut connector, SINK_READY_TIMEOUT).await {
                Ok(()) => {
                    tracing::info!(processor_id = %processor_id, "sink connector ready");
                    let _ = ready_tx.send(Ok(()));
                }
                Err(message) => {
                    tracing::warn!(
                        processor_id = %processor_id,
                        error = %message,
                        "sink connector ready error"
                    );
                    stats.record_error(message.clone());
                    let err = ProcessorError::ProcessingError(message);
                    let _ = ready_tx.send(Err(err.clone()));
                    return Err(err);
                }
            }
            let mut accumulator = DeliveryAccumulator::new();
            let mut pending_delivery: Option<PendingDelivery> = None;
            let mut parked_control_terminal: Option<ControlSignal> = None;

            loop {
                if pending_delivery.is_none() && !accumulator.is_active() {
                    if let Some(control_signal) = parked_control_terminal.take() {
                        Self::forward_control_terminal(
                            &control_output,
                            channel_capacities.control,
                            &mut connector,
                            control_signal,
                        )
                        .await?;
                        tracing::info!(processor_id = %processor_id, "stopped");
                        return Ok(());
                    }
                }

                let retry_deadline = pending_delivery
                    .as_ref()
                    .and_then(|delivery| delivery.retry_at);

                tokio::select! {
                    biased;

                    // ── ① control signals — always active ──
                    control_item = control_streams.next(), if control_active => {
                        match Self::handle_control_item(
	                            &processor_id,
	                            &control_output,
	                            channel_capacities.control,
	                            control_item,
	                        )
	                        .await?
	                        {
	                            ControlAction::Continue => {}
	                            ControlAction::Deactivate => control_active = false,
	                            ControlAction::Terminal(control_signal) => {
                                    control_active = false;
                                    if pending_delivery.is_some() || accumulator.is_active() {
                                        parked_control_terminal = Some(control_signal);
                                    } else {
                                        Self::forward_control_terminal(
                                            &control_output,
                                            channel_capacities.control,
                                            &mut connector,
                                            control_signal,
                                        )
                                        .await?;
                                        tracing::info!(processor_id = %processor_id, "stopped");
                                        return Ok(());
                                    }
                                }
	                        };
	                    }

                    // ── ② retry timer — active when backoff is pending ──
                    _ = sleep_until(retry_deadline.unwrap_or_else(Instant::now)), if retry_deadline.is_some() => {
                        let Some(delivery) = pending_delivery.as_mut() else {
                            continue;
                        };
                        match Self::handle_delivery_attempt(
                            &processor_id,
                            stats.as_ref(),
                            &mut connector,
                            &retry_config,
                            delivery,
                        )
                        .await
                        {
                            DeliveryAttemptOutcome::Delivered => {
                                let delivery = pending_delivery.take().expect("pending delivery exists");
                                if forward_data {
                                    Self::forward_encoded_events(
                                        &output,
                                        channel_capacities.data,
                                        stats.as_ref(),
                                        delivery.forward_events,
                                    )
                                    .await?;
                                }
                            }
                            DeliveryAttemptOutcome::RetryPending => {}
                            DeliveryAttemptOutcome::Dropped => {
                                pending_delivery = None;
                            }
                        }
                    }

                    // ── ③ data — only when no delivery is pending or retrying ──
                    item = input_streams.next(), if pending_delivery.is_none() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                if let Some(rows) = data.num_rows_hint() {
                                    stats.record_in(rows);
                                }
                                match data {
                                    StreamData::EncodedDelivery { flags, bytes } => {
                                        let handle_start = Instant::now();
                                        match accumulator.push(flags, bytes, forward_data, max_delivery_bytes)? {
                                            AccumulatorAction::Pending => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                            }
                                            AccumulatorAction::Aborted(event) => {
                                                connector.abort_active_delivery().await;
                                                if let Some((flags, bytes)) = event {
                                                    send_with_backpressure(
                                                        &output,
                                                        channel_capacities.data,
                                                        StreamData::EncodedDelivery { flags, bytes },
                                                        Some(stats.as_ref()),
                                                    )
                                                    .await?;
                                                }
                                                stats.record_handle_duration(handle_start.elapsed());
                                            }
                                            AccumulatorAction::Completed(completed) => {
                                                stats.record_handle_duration(handle_start.elapsed());
                                                let mut delivery = PendingDelivery::new(completed, &retry_config);
                                                match Self::handle_delivery_attempt(
                                                    &processor_id,
                                                    stats.as_ref(),
                                                    &mut connector,
                                                    &retry_config,
                                                    &mut delivery,
                                                )
                                                .await
                                                {
                                                    DeliveryAttemptOutcome::Delivered => {
                                                        if forward_data {
                                                            Self::forward_encoded_events(
                                                                &output,
                                                                channel_capacities.data,
                                                                stats.as_ref(),
                                                                delivery.forward_events,
                                                            )
                                                            .await?;
                                                        }
                                                    }
                                                    DeliveryAttemptOutcome::RetryPending => {
                                                        pending_delivery = Some(delivery);
                                                    }
                                                    DeliveryAttemptOutcome::Dropped => {}
                                                }
                                            }
                                        }
                                    }
                                    StreamData::Collection(collection) => {
                                        let in_rows = collection.num_rows() as u64;
                                        let handle_start = Instant::now();
                                        if let Err(err) = Self::handle_collection(&mut connector, collection.as_ref()).await {
                                            tracing::error!(
                                                processor_id = %processor_id,
                                                error = %err,
                                                "collection handling error"
                                            );
                                            stats.record_handle_duration(handle_start.elapsed());
                                            stats.record_error(err.to_string());
                                            continue;
                                        }
                                        stats.record_out(in_rows);
                                        if forward_data {
                                            let send_res = send_with_backpressure(
                                                &output,
                                                channel_capacities.data,
                                                StreamData::Collection(collection),
                                                Some(stats.as_ref()),
                                            )
                                            .await;
                                            stats.record_handle_duration(handle_start.elapsed());
                                            send_res?;
                                        } else {
                                            stats.record_handle_duration(handle_start.elapsed());
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal && accumulator.is_active() {
                                            return Err(ProcessorError::ProcessingError(
                                                "encoded delivery protocol error: terminal received while delivery is active"
                                                    .to_string(),
                                            ));
                                        }
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            Self::handle_terminal(&mut connector).await?;
                                            tracing::info!(processor_id = %processor_id, "stopped");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "sink data input");
                                continue;
	                            }
	                            None => {
                                    if accumulator.is_active() {
                                        return Err(ProcessorError::ProcessingError(
                                            "encoded delivery protocol error: data channel closed while delivery is active"
                                                .to_string(),
                                        ));
                                    }
	                                Self::handle_terminal(&mut connector).await?;
	                                tracing::info!(processor_id = %processor_id, "stopped");
	                                return Ok(());
                            }
                        }
                    }
                }
            }
        });
        ProcessorStart::with_ready(handle, ready_rx)
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
