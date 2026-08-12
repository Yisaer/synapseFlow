//! Sink encrypt processor — delivery-boundary encryption transform.
//!
//! Sits between `SinkEncoderProcessor` / `SinkCompressProcessor` and
//! `SinkProcessor` (connector), encrypting each `EncodedDelivery` stream.

use crate::codec::EncryptWriter;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::EncodedDeliveryFlags;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use bytes::Bytes;
use futures::stream::StreamExt;
use std::sync::{Arc, Mutex};
#[cfg(test)]
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct SinkEncryptProcessor {
    id: String,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    // Taken into the task on first start.
    writer: Mutex<Option<Box<dyn EncryptWriter>>>,
    stats: Arc<ProcessorStats>,
}

#[derive(Default)]
struct EncryptDelivery {
    active: bool,
    /// Whether a START chunk has already been forwarded downstream.
    emitted_chunk: bool,
}

impl SinkEncryptProcessor {
    pub fn new(id: impl Into<String>, writer: Box<dyn EncryptWriter>) -> Self {
        Self::new_with_channel_capacities(id, writer, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        writer: Box<dyn EncryptWriter>,
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
            writer: Mutex::new(Some(writer)),
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    fn take_writer(&self) -> Result<Option<Box<dyn EncryptWriter>>, ProcessorError> {
        self.writer
            .lock()
            .map(|mut w| w.take())
            .map_err(|_| ProcessorError::ProcessingError("encrypt writer mutex poisoned".into()))
    }
}

impl Processor for SinkEncryptProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let mut writer = match self.take_writer() {
            Ok(Some(w)) => w,
            Ok(None) => {
                return ProcessorStart::failed(
                    spawner,
                    ProcessorError::ProcessingError(
                        "sink encrypt processor already started".into(),
                    ),
                );
            }
            Err(err) => return ProcessorStart::failed(spawner, err),
        };
        let processor_id = self.id.clone();
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %processor_id, "sink encrypt processor starting");

        ProcessorStart::ready(spawner.spawn(async move {
            let mut delivery = EncryptDelivery::default();
            let mut pending_terminal: Option<ControlSignal> = None;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                let is_terminal = control_signal.is_terminal();
                                if is_terminal && delivery.active {
                                    pending_terminal = Some(control_signal);
                                    control_active = false;
                                    continue;
                                }
                                send_control_with_backpressure(
                                    &control_output,
                                    channel_capacities.control,
                                    control_signal,
                                )
                                .await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                    return Ok(());
                                }
                            }
                            _ => {
                                control_active = false;
                            }
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                match data {
                                    StreamData::EncodedDelivery { flags, bytes } => {
                                        let handle_start = std::time::Instant::now();
                                        let res = handle_delivery(
                                            writer.as_mut(),
                                            &mut delivery,
                                            flags,
                                            bytes,
                                            &output,
                                            channel_capacities.data,
                                            &stats,
                                        )
                                        .await;
                                        stats.record_handle_duration(handle_start.elapsed());
                                        if let Err(err) = res {
                                            tracing::error!(
                                                processor_id = %processor_id,
                                                error = %err,
                                                "encrypt delivery error"
                                            );
                                            stats.record_error(err.to_string());
                                            if matches!(err, ProcessorError::ChannelClosed) {
                                                return Err(err);
                                            }
                                            continue;
                                        }
                                        if !delivery.active {
                                            if let Some(terminal) = pending_terminal.take() {
                                                send_control_with_backpressure(
                                                    &control_output,
                                                    channel_capacities.control,
                                                    terminal,
                                                )
                                                .await?;
                                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control, after delivery drain)");
                                                return Ok(());
                                            }
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal && delivery.active {
                                            abort_in_flight(
                                                writer.as_mut(),
                                                &mut delivery,
                                                &output,
                                                channel_capacities.data,
                                                &stats,
                                                &processor_id,
                                            )
                                            .await;
                                        }
                                        let out_rows = data.num_rows_hint();
                                        send_with_backpressure(
                                            &output,
                                            channel_capacities.data,
                                            data,
                                            Some(stats.as_ref()),
                                        )
                                        .await?;
                                        if let Some(rows) = out_rows {
                                            stats.record_out(rows);
                                        }
                                        if is_terminal {
                                            tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &processor_id,
                                    skipped,
                                    "sink encrypt data input",
                                );
                            }
                            None => {
                                if delivery.active {
                                    abort_in_flight(
                                        writer.as_mut(),
                                        &mut delivery,
                                        &output,
                                        channel_capacities.data,
                                        &stats,
                                        &processor_id,
                                    )
                                    .await;
                                }
                                if let Some(terminal) = pending_terminal.take() {
                                    send_control_with_backpressure(
                                        &control_output,
                                        channel_capacities.control,
                                        terminal,
                                    )
                                    .await?;
                                }
                                tracing::info!(processor_id = %processor_id, "input streams closed");
                                return Ok(());
                            }
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

async fn handle_delivery(
    writer: &mut dyn EncryptWriter,
    delivery: &mut EncryptDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    output: &LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
) -> Result<(), ProcessorError> {
    stats.record_in(1);

    let is_abort = flags.contains(EncodedDeliveryFlags::ABORT);
    let is_start = flags.contains(EncodedDeliveryFlags::START);
    let is_end = flags.contains(EncodedDeliveryFlags::END);

    if is_abort {
        writer.abort_delivery();
        let was_started = delivery.emitted_chunk;
        *delivery = EncryptDelivery::default();
        if was_started {
            send_with_backpressure(
                output,
                data_channel_capacity,
                StreamData::encoded_delivery_abort(),
                Some(stats.as_ref()),
            )
            .await?;
        }
        return Ok(());
    }

    if is_start {
        if delivery.active {
            return fail_delivery(
                writer,
                delivery,
                output,
                data_channel_capacity,
                stats,
                "START received while delivery already active",
            )
            .await;
        }

        let mut out = Vec::new();
        if let Err(e) = writer.begin_delivery(&mut out) {
            return fail_delivery(
                writer,
                delivery,
                output,
                data_channel_capacity,
                stats,
                &e.to_string(),
            )
            .await;
        }

        if is_end {
            if let Err(e) = writer.finish(&bytes, &mut out) {
                return fail_delivery(
                    writer,
                    delivery,
                    output,
                    data_channel_capacity,
                    stats,
                    &e.to_string(),
                )
                .await;
            }
            send_with_backpressure(
                output,
                data_channel_capacity,
                StreamData::encoded_delivery_single(out),
                Some(stats.as_ref()),
            )
            .await?;
            stats.record_out(1);
            *delivery = EncryptDelivery::default();
            return Ok(());
        }

        if !bytes.is_empty() {
            if let Err(e) = writer.write(&bytes, &mut out) {
                return fail_delivery(
                    writer,
                    delivery,
                    output,
                    data_channel_capacity,
                    stats,
                    &e.to_string(),
                )
                .await;
            }
        }

        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_start(out),
            Some(stats.as_ref()),
        )
        .await?;
        delivery.active = true;
        delivery.emitted_chunk = true;
        return Ok(());
    }

    if !delivery.active {
        return Err(ProcessorError::ProcessingError(
            "chunk/END received without active delivery".into(),
        ));
    }

    if is_end {
        let mut out = Vec::new();
        if let Err(e) = writer.finish(&bytes, &mut out) {
            return fail_delivery(
                writer,
                delivery,
                output,
                data_channel_capacity,
                stats,
                &e.to_string(),
            )
            .await;
        }
        *delivery = EncryptDelivery::default();
        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_end(out),
            Some(stats.as_ref()),
        )
        .await?;
        stats.record_out(1);
        return Ok(());
    }

    if bytes.is_empty() {
        return Ok(());
    }

    let mut out = Vec::new();
    if let Err(e) = writer.write(&bytes, &mut out) {
        return fail_delivery(
            writer,
            delivery,
            output,
            data_channel_capacity,
            stats,
            &e.to_string(),
        )
        .await;
    }
    send_with_backpressure(
        output,
        data_channel_capacity,
        StreamData::encoded_delivery_chunk(out),
        Some(stats.as_ref()),
    )
    .await?;

    Ok(())
}

async fn abort_in_flight(
    writer: &mut dyn EncryptWriter,
    delivery: &mut EncryptDelivery,
    output: &LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
    processor_id: &str,
) {
    writer.abort_delivery();
    let was_started = delivery.emitted_chunk;
    *delivery = EncryptDelivery::default();
    if was_started {
        if let Err(err) = send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_abort(),
            Some(stats.as_ref()),
        )
        .await
        {
            tracing::error!(
                processor_id = %processor_id,
                error = %err,
                "failed to forward ABORT during in-flight abort"
            );
        }
    }
    tracing::error!(
        processor_id = %processor_id,
        "aborted in-flight delivery: terminal signal received mid-delivery"
    );
    stats.record_error("terminal signal received mid-delivery".to_string());
}

async fn fail_delivery(
    writer: &mut dyn EncryptWriter,
    delivery: &mut EncryptDelivery,
    output: &LinkOutput<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
    reason: &str,
) -> Result<(), ProcessorError> {
    writer.abort_delivery();
    let was_started = delivery.emitted_chunk;
    *delivery = EncryptDelivery::default();
    if was_started {
        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_abort(),
            Some(stats.as_ref()),
        )
        .await?;
    }
    Err(ProcessorError::ProcessingError(reason.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{
        AesGcmStreamWriter, InlineEncryptionKey, SecretEncoding, SinkEncryptionConfig,
    };
    use crate::runtime::TaskSpawner;
    use tokio::runtime::Handle;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(Handle::current())
    }

    fn writer() -> AesGcmStreamWriter {
        let config = SinkEncryptionConfig::aes_gcm(
            "sink-aes-v1",
            InlineEncryptionKey::new(hex::encode([4u8; 32]), SecretEncoding::Hex),
        )
        .expect("encryption config");
        AesGcmStreamWriter::from_config(&config).expect("writer")
    }

    async fn recv_output(output: &mut LinkReceiver<StreamData>) -> StreamData {
        timeout(Duration::from_secs(2), output.recv())
            .await
            .expect("output timeout")
            .expect("output")
    }

    async fn assert_no_output(output: &mut LinkReceiver<StreamData>) {
        assert!(
            timeout(Duration::from_millis(50), output.recv())
                .await
                .is_err(),
            "unexpected output"
        );
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn single_chunk_delivery_forwards_start_end() {
        let output = LinkOutput::broadcast(16);
        let mut output_rx = output.subscribe().expect("output receiver");
        let stats = Arc::new(ProcessorStats::default());
        let mut writer = writer();
        let mut delivery = EncryptDelivery::default();

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::START | EncodedDeliveryFlags::END,
            Bytes::from_static(b"hello"),
            &output,
            16,
            &stats,
        )
        .await
        .expect("handle delivery");

        let StreamData::EncodedDelivery { flags, bytes } = recv_output(&mut output_rx).await else {
            panic!("expected encoded delivery")
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(!bytes.is_empty());
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn multi_chunk_empty_middle_and_empty_end_lifecycle() {
        let output = LinkOutput::broadcast(16);
        let mut output_rx = output.subscribe().expect("output receiver");
        let stats = Arc::new(ProcessorStats::default());
        let mut writer = writer();
        let mut delivery = EncryptDelivery::default();

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::new(),
            &output,
            16,
            &stats,
        )
        .await
        .expect("start");
        let StreamData::EncodedDelivery { flags, bytes } = recv_output(&mut output_rx).await else {
            panic!("expected start")
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));
        assert!(!flags.contains(EncodedDeliveryFlags::END));
        assert!(
            !bytes.is_empty(),
            "START should carry encrypted stream header"
        );

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::empty(),
            Bytes::new(),
            &output,
            16,
            &stats,
        )
        .await
        .expect("empty middle");
        assert_no_output(&mut output_rx).await;

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::END,
            Bytes::new(),
            &output,
            16,
            &stats,
        )
        .await
        .expect("end");
        let StreamData::EncodedDelivery { flags, bytes } = recv_output(&mut output_rx).await else {
            panic!("expected end")
        };
        assert!(flags.contains(EncodedDeliveryFlags::END));
        assert!(!flags.contains(EncodedDeliveryFlags::START));
        assert!(!bytes.is_empty(), "END should carry final encrypted frame");
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn abort_forwarding_depends_on_downstream_start() {
        let output = LinkOutput::broadcast(16);
        let mut output_rx = output.subscribe().expect("output receiver");
        let stats = Arc::new(ProcessorStats::default());
        let mut writer = writer();
        let mut delivery = EncryptDelivery::default();

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::ABORT,
            Bytes::new(),
            &output,
            16,
            &stats,
        )
        .await
        .expect("abort before start");
        assert_no_output(&mut output_rx).await;

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::from_static(b"hello"),
            &output,
            16,
            &stats,
        )
        .await
        .expect("start");
        let _ = recv_output(&mut output_rx).await;

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::ABORT,
            Bytes::new(),
            &output,
            16,
            &stats,
        )
        .await
        .expect("abort after start");
        let StreamData::EncodedDelivery { flags, bytes } = recv_output(&mut output_rx).await else {
            panic!("expected abort")
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(bytes.is_empty());
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn protocol_errors_reset_and_forward_abort_when_needed() {
        let output = LinkOutput::broadcast(16);
        let mut output_rx = output.subscribe().expect("output receiver");
        let stats = Arc::new(ProcessorStats::default());
        let mut writer = writer();
        let mut delivery = EncryptDelivery::default();

        let err = handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::empty(),
            Bytes::from_static(b"chunk"),
            &output,
            16,
            &stats,
        )
        .await
        .expect_err("chunk without start");
        assert!(err.to_string().contains("without active delivery"));

        handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::from_static(b"hello"),
            &output,
            16,
            &stats,
        )
        .await
        .expect("start");
        let _ = recv_output(&mut output_rx).await;

        let err = handle_delivery(
            &mut writer,
            &mut delivery,
            EncodedDeliveryFlags::START,
            Bytes::from_static(b"again"),
            &output,
            16,
            &stats,
        )
        .await
        .expect_err("second start");
        assert!(err.to_string().contains("already active"));
        let StreamData::EncodedDelivery { flags, .. } = recv_output(&mut output_rx).await else {
            panic!("expected abort")
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(!delivery.active);
    }

    // coverage-covers: sink.encrypt.aes_gcm_delivery
    #[tokio::test]
    async fn terminal_mid_delivery_forwards_abort_without_end() {
        let mut processor = SinkEncryptProcessor::new("terminal_mid_delivery", Box::new(writer()));
        let (input, input_rx) = broadcast::channel(16);
        let (control, control_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().unwrap();
        let mut control_output = processor.subscribe_control_output().unwrap();
        let handle = processor.start(&test_spawner());

        assert!(input
            .send(StreamData::encoded_delivery_start(b"hello".to_vec()))
            .is_ok());
        let StreamData::EncodedDelivery { flags, .. } = recv_output(&mut output).await else {
            panic!("expected start output")
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));

        assert!(control
            .send(ControlSignal::Instant(
                crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 },
            ))
            .is_ok());
        drop(input);

        let StreamData::EncodedDelivery { flags, bytes } = recv_output(&mut output).await else {
            panic!("expected abort output")
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));
        assert!(bytes.is_empty());

        let control_signal = timeout(Duration::from_secs(2), control_output.recv())
            .await
            .expect("control timeout")
            .expect("control signal");
        assert!(control_signal.is_terminal());
        handle.await.expect("join").expect("processor result");
    }
}
