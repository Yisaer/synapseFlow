//! Sink compress processor — delivery-boundary compression transform.
//!
//! Sits between `SinkEncoderProcessor` and `SinkProcessor` (connector), compressing
//! each `EncodedDelivery` stream using the configured codec.

use crate::codec::CompressWriter;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    ProcessorChannelCapacities,
};
use crate::processor::EncodedDeliveryFlags;
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use bytes::Bytes;
use futures::stream::StreamExt;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct SinkCompressProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    // Taken into the task on first start.
    writer: Mutex<Option<Box<dyn CompressWriter>>>,
    stats: Arc<ProcessorStats>,
}

#[derive(Default)]
struct CompressDelivery {
    active: bool,
    /// Whether a START chunk has already been forwarded to downstream.
    /// Used to decide whether ABORT must be propagated.
    emitted_chunk: bool,
}

impl SinkCompressProcessor {
    pub fn new(id: impl Into<String>, writer: Box<dyn CompressWriter>) -> Self {
        Self::new_with_channel_capacities(id, writer, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        writer: Box<dyn CompressWriter>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let (output, _) = broadcast::channel(channel_capacities.data);
        let (control_output, _) = broadcast::channel(channel_capacities.control);
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

    fn take_writer(&self) -> Result<Option<Box<dyn CompressWriter>>, ProcessorError> {
        self.writer
            .lock()
            .map(|mut w| w.take())
            .map_err(|_| ProcessorError::ProcessingError("compress writer mutex poisoned".into()))
    }
}

impl Processor for SinkCompressProcessor {
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
                        "sink compress processor already started".into(),
                    ),
                );
            }
            Err(err) => return ProcessorStart::failed(spawner, err),
        };
        let processor_id = self.id.clone();
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %processor_id, "sink compress processor starting");

        ProcessorStart::ready(spawner.spawn(async move {
            let mut delivery = CompressDelivery::default();
            // The upstream encoder always flushes (sends END to the data channel) before
            // forwarding the terminal control signal. With biased select, both branches can
            // be ready simultaneously and control would win, causing the END to be skipped.
            // To avoid losing the last delivery, we park a terminal received while a delivery
            // is active and let the data loop drain the END naturally first.
            let mut pending_terminal: Option<ControlSignal> = None;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        match control_item {
                            Some(Ok(control_signal)) => {
                                let is_terminal = control_signal.is_terminal();
                                if is_terminal && delivery.active {
                                    // Park the terminal and stop polling control. The END data
                                    // chunk is already in the data channel; the data branch will
                                    // complete the delivery and then forward the parked terminal.
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
                                                "compress delivery error"
                                            );
                                            stats.record_error(err.to_string());
                                            return Err(err);
                                        }
                                        // Delivery completed (END processed): forward parked terminal.
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
                                    "sink compress data input",
                                );
                            }
                            None => {
                                // Data channel closed. If a terminal was parked and the delivery
                                // is still active, the END never arrived (genuine mid-delivery
                                // truncation): abort first, then forward the terminal.
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

/// Handle one `EncodedDelivery` event through the compress state machine.
async fn handle_delivery(
    writer: &mut dyn CompressWriter,
    delivery: &mut CompressDelivery,
    flags: EncodedDeliveryFlags,
    bytes: Bytes,
    output: &broadcast::Sender<StreamData>,
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
        *delivery = CompressDelivery::default();
        if was_started {
            // Forward ABORT only if downstream already received a START.
            // ABORT is not a completed delivery → not counted in record_out.
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
        writer
            .begin_delivery()
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
        delivery.active = true;
    } else if !delivery.active {
        return Err(ProcessorError::ProcessingError(
            "chunk/END received without active delivery".into(),
        ));
    }

    // Compress the input bytes (if any).
    let mut out = Vec::new();
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

    if is_end {
        if let Err(e) = writer.finish(&mut out) {
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
        *delivery = CompressDelivery::default();
        // Always forward END (even if out is empty).
        let data = if is_start {
            StreamData::encoded_delivery_single(out)
        } else {
            StreamData::encoded_delivery_end(out)
        };
        send_with_backpressure(output, data_channel_capacity, data, Some(stats.as_ref())).await?;
        // One completed delivery = 1 out (matches connector accounting).
        stats.record_out(1);
    } else if is_start {
        // Always forward START (even if out is empty). START is not a completed
        // delivery → not counted in record_out.
        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_start(out),
            Some(stats.as_ref()),
        )
        .await?;
        delivery.emitted_chunk = true;
    } else if !out.is_empty() {
        // Middle chunk: only forward when natural drain produced output.
        // Not a completed delivery → not counted in record_out.
        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_chunk(out),
            Some(stats.as_ref()),
        )
        .await?;
    }

    Ok(())
}

/// Abort an in-progress delivery: reset writer state, notify downstream if a
/// START was already emitted, record the error, and reset the delivery state.
///
/// This does **not** return an error — the caller records the error separately
/// and decides whether to propagate it.
async fn abort_in_flight(
    writer: &mut dyn CompressWriter,
    delivery: &mut CompressDelivery,
    output: &broadcast::Sender<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
    processor_id: &str,
) {
    writer.abort_delivery();
    let was_started = delivery.emitted_chunk;
    *delivery = CompressDelivery::default();
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

/// Reset writer + delivery state and notify downstream when START was already
/// emitted, then surface the error.
async fn fail_delivery(
    writer: &mut dyn CompressWriter,
    delivery: &mut CompressDelivery,
    output: &broadcast::Sender<StreamData>,
    data_channel_capacity: usize,
    stats: &Arc<ProcessorStats>,
    reason: &str,
) -> Result<(), ProcessorError> {
    writer.abort_delivery();
    let was_started = delivery.emitted_chunk;
    *delivery = CompressDelivery::default();
    if was_started {
        // ABORT is not a completed delivery → not counted in record_out.
        let _ = send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_abort(),
            Some(stats.as_ref()),
        )
        .await;
    }
    Err(ProcessorError::ProcessingError(reason.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::compress::{CompressionCodec, GzipWriter, ZstdWriter};
    use crate::processor::EncodedDeliveryFlags;
    use crate::runtime::TaskSpawner;
    use flate2::Compression;
    use tokio::runtime::Handle;
    use tokio::time::{timeout, Duration};

    fn test_spawner() -> TaskSpawner {
        TaskSpawner::from_handle(Handle::current())
    }

    fn gzip_processor(id: &str) -> SinkCompressProcessor {
        let writer = GzipWriter::new(Compression::default());
        SinkCompressProcessor::new(id, Box::new(writer))
    }

    fn zstd_processor(id: &str) -> SinkCompressProcessor {
        let writer = ZstdWriter::new(0).unwrap();
        SinkCompressProcessor::new(id, Box::new(writer))
    }

    async fn recv_output(
        output: &mut broadcast::Receiver<StreamData>,
    ) -> Result<StreamData, ProcessorError> {
        timeout(Duration::from_secs(2), output.recv())
            .await
            .map_err(|_| ProcessorError::ProcessingError("timed out waiting for output".into()))?
            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))
    }

    fn decompress_gzip(data: &[u8]) -> Vec<u8> {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(data);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("gzip decompress");
        out
    }

    fn decompress_zstd(data: &[u8]) -> Vec<u8> {
        zstd::stream::decode_all(data).expect("zstd decompress")
    }

    async fn run_single_chunk_delivery(
        processor: SinkCompressProcessor,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut processor = processor;
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        assert!(input
            .send(StreamData::encoded_delivery_single(payload.to_vec()))
            .is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let mut chunks: Vec<bytes::Bytes> = Vec::new();
        loop {
            let data = recv_output(&mut output).await.unwrap();
            if data.is_terminal() {
                break;
            }
            if let StreamData::EncodedDelivery { bytes, flags } = data {
                assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
                chunks.push(bytes);
            }
        }
        handle.await.unwrap().unwrap();
        chunks.concat()
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn gzip_single_chunk_delivery_compresses_correctly() {
        let payload = b"hello gzip world";
        let compressed = run_single_chunk_delivery(gzip_processor("gzip"), payload).await;
        assert_eq!(decompress_gzip(&compressed), payload);
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[tokio::test]
    async fn zstd_single_chunk_delivery_compresses_correctly() {
        let payload = b"hello zstd world";
        let compressed = run_single_chunk_delivery(zstd_processor("zstd"), payload).await;
        assert_eq!(decompress_zstd(&compressed), payload);
    }

    async fn run_multi_chunk_delivery(codec: CompressionCodec, chunks: Vec<Vec<u8>>) -> Vec<u8> {
        let writer = codec.build_writer().unwrap();
        let mut processor = SinkCompressProcessor::new("multi", writer);
        let (input, input_rx) = broadcast::channel(32);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        let n = chunks.len();
        for (i, chunk) in chunks.iter().enumerate() {
            let flags = if i == 0 && n == 1 {
                EncodedDeliveryFlags::START | EncodedDeliveryFlags::END
            } else if i == 0 {
                EncodedDeliveryFlags::START
            } else if i == n - 1 {
                EncodedDeliveryFlags::END
            } else {
                EncodedDeliveryFlags::empty()
            };
            assert!(input
                .send(StreamData::encoded_delivery(flags, chunk.clone()))
                .is_ok());
        }
        assert!(input.send(StreamData::stream_end()).is_ok());

        let mut out_chunks = Vec::new();
        loop {
            let data = recv_output(&mut output).await.unwrap();
            if data.is_terminal() {
                break;
            }
            if let StreamData::EncodedDelivery { bytes, flags } = data {
                assert!(!flags.contains(EncodedDeliveryFlags::ABORT));
                out_chunks.push(bytes);
            }
        }
        handle.await.unwrap().unwrap();
        out_chunks.concat()
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn gzip_multi_chunk_delivery_roundtrips() {
        let chunks = vec![
            b"chunk one ".to_vec(),
            b"chunk two ".to_vec(),
            b"chunk three".to_vec(),
        ];
        let expected: Vec<u8> = chunks.concat();
        let compressed = run_multi_chunk_delivery(CompressionCodec::gzip(), chunks).await;
        assert_eq!(decompress_gzip(&compressed), expected);
    }

    // coverage-covers: sink.compress.zstd_delivery
    #[tokio::test]
    async fn zstd_multi_chunk_delivery_roundtrips() {
        let chunks = vec![
            b"chunk one ".to_vec(),
            b"chunk two ".to_vec(),
            b"chunk three".to_vec(),
        ];
        let expected: Vec<u8> = chunks.concat();
        let compressed = run_multi_chunk_delivery(CompressionCodec::zstd(), chunks).await;
        assert_eq!(decompress_zstd(&compressed), expected);
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn abort_before_start_does_not_forward_abort() {
        let mut processor = gzip_processor("abort_before");
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        assert!(input.send(StreamData::encoded_delivery_abort()).is_ok());
        assert!(input.send(StreamData::stream_end()).is_ok());

        let terminal = recv_output(&mut output).await.unwrap();
        assert!(terminal.is_terminal());
        handle.await.unwrap().unwrap();
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn abort_after_start_forwards_abort_to_downstream() {
        let mut processor = gzip_processor("abort_after");
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        // Send START to trigger begin_delivery and emitted_chunk=true
        assert!(input
            .send(StreamData::encoded_delivery_start(b"partial".to_vec()))
            .is_ok());
        // Wait for the START to arrive downstream
        let start_out = recv_output(&mut output).await.unwrap();
        let StreamData::EncodedDelivery { flags, .. } = start_out else {
            panic!("expected encoded delivery");
        };
        assert!(flags.contains(EncodedDeliveryFlags::START));

        // Send ABORT
        assert!(input.send(StreamData::encoded_delivery_abort()).is_ok());
        let abort_out = recv_output(&mut output).await.unwrap();
        let StreamData::EncodedDelivery { flags, .. } = abort_out else {
            panic!("expected encoded delivery for ABORT");
        };
        assert!(flags.contains(EncodedDeliveryFlags::ABORT));

        assert!(input.send(StreamData::stream_end()).is_ok());
        let terminal = recv_output(&mut output).await.unwrap();
        assert!(terminal.is_terminal());
        handle.await.unwrap().unwrap();
    }

    // coverage-covers: sink.compress.gzip_delivery
    #[tokio::test]
    async fn protocol_error_start_without_prior_end_returns_error() {
        let mut processor = gzip_processor("protocol_err");
        let (input, input_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        let mut output = processor.subscribe_output().unwrap();
        let handle = processor.start(&test_spawner());

        // First START
        assert!(input
            .send(StreamData::encoded_delivery_start(b"first".to_vec()))
            .is_ok());
        let first = recv_output(&mut output).await.unwrap();
        assert!(
            matches!(&first, StreamData::EncodedDelivery { flags, .. } if flags.contains(EncodedDeliveryFlags::START))
        );

        // Second START without END → protocol error
        assert!(input
            .send(StreamData::encoded_delivery_start(b"second".to_vec()))
            .is_ok());

        // The processor should error out; drain output including possible ABORT
        let mut saw_error = false;
        for _ in 0..5 {
            match recv_output(&mut output).await {
                Ok(StreamData::EncodedDelivery { flags, .. })
                    if flags.contains(EncodedDeliveryFlags::ABORT) =>
                {
                    saw_error = true;
                    break;
                }
                Ok(StreamData::EncodedDelivery { .. }) => continue,
                Ok(data) if data.is_terminal() => {
                    saw_error = true;
                    break;
                }
                _ => break,
            }
        }
        // Either the ABORT was forwarded or the task terminated with error
        drop(input);
        let result = handle.await.unwrap();
        assert!(result.is_err() || saw_error, "expected error or abort");
    }

    // coverage-covers: sink.compress.gzip_delivery
    // Regression test: encoder always sends END to the data channel before forwarding
    // the control terminal. With biased select the control branch could win when both
    // are ready simultaneously, causing abort_in_flight to discard a complete delivery.
    // The fix parks the terminal until the data loop drains the END.
    #[tokio::test]
    async fn end_and_control_terminal_simultaneous_does_not_abort_delivery() {
        let mut processor = gzip_processor("drain_race");
        let (input, input_rx) = broadcast::channel(16);
        let (control, control_rx) = broadcast::channel(16);
        processor.add_input(input_rx);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().unwrap();
        let mut control_output = processor.subscribe_control_output().unwrap();
        let handle = processor.start(&test_spawner());

        // Send START and wait for it so delivery is active before we race.
        assert!(input
            .send(StreamData::encoded_delivery_start(b"hello ".to_vec()))
            .is_ok());
        let start_out = recv_output(&mut output).await.unwrap();
        assert!(
            matches!(&start_out, StreamData::EncodedDelivery { flags, .. }
                if flags.contains(EncodedDeliveryFlags::START))
        );

        // Now queue END on the data channel and terminal on the control channel
        // back-to-back without yielding — replicating what the encoder does.
        assert!(input
            .send(StreamData::encoded_delivery_end(b"world".to_vec()))
            .is_ok());
        assert!(control
            .send(ControlSignal::Instant(
                crate::processor::InstantControlSignal::StreamQuickEnd { signal_id: 1 }
            ))
            .is_ok());

        // The processor sends END data to the output channel before it forwards the
        // parked terminal to the control channel, so we can receive them sequentially.
        let end_out = recv_output(&mut output).await.unwrap();
        let StreamData::EncodedDelivery {
            flags: end_flags,
            bytes: end_bytes,
        } = end_out
        else {
            panic!("expected END delivery chunk");
        };
        assert!(
            !end_flags.contains(EncodedDeliveryFlags::ABORT),
            "last delivery must not be aborted"
        );
        assert!(end_flags.contains(EncodedDeliveryFlags::END));

        let ctrl = tokio::time::timeout(Duration::from_secs(2), control_output.recv())
            .await
            .expect("timed out waiting for terminal")
            .expect("control channel closed");
        assert!(ctrl.is_terminal());
        handle.await.unwrap().unwrap();

        let start_bytes = match start_out {
            StreamData::EncodedDelivery { bytes, .. } => bytes,
            _ => unreachable!(),
        };
        let compressed: Vec<u8> = [start_bytes.as_ref(), end_bytes.as_ref()].concat();
        assert_eq!(decompress_gzip(&compressed), b"hello world");
    }
}
