//! Sink encoder processor combines batching and encoding when the encoder
//! supports incremental streaming.

use crate::codec::{CollectionEncoder, CollectionEncoderStream};
use crate::model::Collection;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure,
    ProcessorChannelCapacities,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, ProcessorStats, StreamData};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Sleep};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub struct SinkEncoderProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    encoder: Arc<dyn CollectionEncoder>,
    batch_count: Option<usize>,
    batch_duration: Option<Duration>,
    stats: Arc<ProcessorStats>,
}

enum StreamingBatchMode {
    Immediate,
    CountOnly { count: usize },
    DurationOnly { duration: Duration },
    Combined { count: usize, duration: Duration },
}

impl StreamingBatchMode {
    fn new(batch_count: Option<usize>, batch_duration: Option<Duration>) -> Option<Self> {
        match (batch_count, batch_duration) {
            (Some(count), Some(duration)) => Some(Self::Combined { count, duration }),
            (Some(count), None) => Some(Self::CountOnly { count }),
            (None, Some(duration)) => Some(Self::DurationOnly { duration }),
            (None, None) => Some(Self::Immediate),
        }
    }

    fn count_threshold(&self) -> Option<usize> {
        match self {
            StreamingBatchMode::CountOnly { count }
            | StreamingBatchMode::Combined { count, .. } => Some(*count),
            StreamingBatchMode::Immediate | StreamingBatchMode::DurationOnly { .. } => None,
        }
    }

    fn duration(&self) -> Option<Duration> {
        match self {
            StreamingBatchMode::DurationOnly { duration }
            | StreamingBatchMode::Combined { duration, .. } => Some(*duration),
            StreamingBatchMode::Immediate | StreamingBatchMode::CountOnly { .. } => None,
        }
    }
}

impl SinkEncoderProcessor {
    pub(crate) fn validate_batch_config(
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Result<(), ProcessorError> {
        if matches!(batch_count, Some(0)) {
            return Err(ProcessorError::InvalidConfiguration(
                "sink encoder processor requires batch_count > 0 when configured".to_string(),
            ));
        }
        if matches!(batch_duration, Some(duration) if duration.is_zero()) {
            return Err(ProcessorError::InvalidConfiguration(
                "sink encoder processor requires batch_duration > 0 when configured".to_string(),
            ));
        }
        Ok(())
    }

    pub fn new(
        id: impl Into<String>,
        encoder: Arc<dyn CollectionEncoder>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            encoder,
            batch_count,
            batch_duration,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        encoder: Arc<dyn CollectionEncoder>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
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
            encoder,
            batch_count,
            batch_duration,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    fn ensure_stream<'a>(
        encoder: &Arc<dyn CollectionEncoder>,
        stream: &'a mut Option<Box<dyn CollectionEncoderStream>>,
    ) -> Result<&'a mut Box<dyn CollectionEncoderStream>, ProcessorError> {
        if stream.is_none() {
            let state = encoder.start_stream().ok_or_else(|| {
                ProcessorError::ProcessingError(
                    "sink encoder is not available for this processor".to_string(),
                )
            })?;
            *stream = Some(state);
        }
        stream
            .as_mut()
            .ok_or_else(|| ProcessorError::ProcessingError("encoder stream unavailable".into()))
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_collection(
        processor_id: &str,
        collection: Box<dyn Collection>,
        encoder: &Arc<dyn CollectionEncoder>,
        row_count: &mut usize,
        stream_state: &mut Option<Box<dyn CollectionEncoderStream>>,
        mode: &StreamingBatchMode,
        output: &broadcast::Sender<StreamData>,
        data_channel_capacity: usize,
        timer: &mut Option<Pin<Box<Sleep>>>,
        stats: &Arc<ProcessorStats>,
    ) -> Result<(), ProcessorError> {
        stats.record_in(collection.num_rows() as u64);
        if collection.num_rows() == 0 {
            if matches!(mode, StreamingBatchMode::Immediate) {
                let stream = encoder.start_stream().ok_or_else(|| {
                    ProcessorError::ProcessingError(
                        "sink encoder is not available for this processor".to_string(),
                    )
                })?;
                let payload = stream.finish().map_err(|err| {
                    ProcessorError::ProcessingError(format!("stream finish error: {err}"))
                })?;
                send_with_backpressure(
                    output,
                    data_channel_capacity,
                    StreamData::encoded_delivery_single(payload),
                    Some(stats.as_ref()),
                )
                .await?;
                tracing::debug!(processor_id = %processor_id, "flushed empty collection");
            }
            return Ok(());
        }
        for tuple in collection.rows().iter() {
            let stream = Self::ensure_stream(encoder, stream_state)?;
            stream.append(tuple).map_err(|err| {
                ProcessorError::ProcessingError(format!("stream append error: {err}"))
            })?;
            *row_count += 1;
            if let Some(count) = mode.count_threshold() {
                if *row_count >= count {
                    let flushed = Self::flush_buffer(
                        processor_id,
                        row_count,
                        stream_state,
                        output,
                        data_channel_capacity,
                        stats,
                    )
                    .await?;
                    if flushed > 0 {
                        stats.record_out(flushed as u64);
                    }
                }
            }
            if let Some(duration) = mode.duration() {
                Self::schedule_timer(timer, duration, *row_count > 0);
            }
        }
        if matches!(mode, StreamingBatchMode::Immediate) {
            let flushed = Self::flush_buffer(
                processor_id,
                row_count,
                stream_state,
                output,
                data_channel_capacity,
                stats,
            )
            .await?;
            if flushed > 0 {
                stats.record_out(flushed as u64);
            }
        }
        Ok(())
    }

    async fn flush_buffer(
        processor_id: &str,
        row_count: &mut usize,
        stream_state: &mut Option<Box<dyn CollectionEncoderStream>>,
        output: &broadcast::Sender<StreamData>,
        data_channel_capacity: usize,
        stats: &Arc<ProcessorStats>,
    ) -> Result<usize, ProcessorError> {
        if *row_count == 0 {
            *stream_state = None;
            return Ok(0);
        }
        let flushed_rows = std::mem::take(row_count);
        let stream = stream_state.take().ok_or_else(|| {
            ProcessorError::ProcessingError(
                "encoder stream missing when attempting to flush".to_string(),
            )
        })?;
        let payload = stream.finish().map_err(|err| {
            ProcessorError::ProcessingError(format!("stream finish error: {err}"))
        })?;
        send_with_backpressure(
            output,
            data_channel_capacity,
            StreamData::encoded_delivery_single(payload),
            Some(stats.as_ref()),
        )
        .await?;
        tracing::debug!(processor_id = %processor_id, "flushed batch");
        Ok(flushed_rows)
    }

    fn schedule_timer(timer: &mut Option<Pin<Box<Sleep>>>, duration: Duration, has_data: bool) {
        if has_data {
            if timer.is_none() {
                *timer = Some(Box::pin(sleep(duration)));
            }
        } else if timer.is_some() {
            *timer = None;
        }
    }
}

impl Processor for SinkEncoderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(
        &mut self,
        spawner: &TaskSpawner,
    ) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let encoder = Arc::clone(&self.encoder);
        let batch_count = self.batch_count;
        let batch_duration = self.batch_duration;
        let mode = StreamingBatchMode::new(batch_count, batch_duration)
            .expect("sink encoder batch mode should always be available");
        let processor_id = self.id.clone();
        let stats = Arc::clone(&self.stats);
        tracing::info!(processor_id = %processor_id, "sink encoder processor starting");

        spawner.spawn(async move {
            let mut row_count: usize = 0;
            let mut stream_state: Option<Box<dyn CollectionEncoderStream>> = None;
            let mut timer: Option<Pin<Box<Sleep>>> = None;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                if let Err(err) = async {
                                    let flushed = SinkEncoderProcessor::flush_buffer(
                                        &processor_id,
                                        &mut row_count,
                                        &mut stream_state,
                                        &output,
                                        channel_capacities.data,
                                        &stats,
                                    )
                                    .await?;
                                    if flushed > 0 {
                                        stats.record_out(flushed as u64);
                                    }
                                    Ok::<(), ProcessorError>(())
                                }
                                .await
                                {
                                    tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                                    stats.record_error(err.to_string());
                                }
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    _ = async {
                        if let Some(timer) = &mut timer {
                            timer.as_mut().await;
                        }
                    }, if timer.is_some() => {
                        if let Err(err) = async {
                            let flushed = SinkEncoderProcessor::flush_buffer(
                                &processor_id,
                                &mut row_count,
                                &mut stream_state,
                                &output,
                                channel_capacities.data,
                                &stats,
                            )
                            .await?;
                            if flushed > 0 {
                                stats.record_out(flushed as u64);
                            }
                            Ok::<(), ProcessorError>(())
                        }
                        .await
                        {
                            tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                            stats.record_error(err.to_string());
                        }
                        if let Some(duration) = mode.duration() {
                            SinkEncoderProcessor::schedule_timer(&mut timer, duration, row_count > 0);
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(data)) => {
                                log_received_data(&processor_id, &data);
                                match data {
                                    StreamData::Collection(collection) => {
                                        let handle_start = std::time::Instant::now();
                                        let res = SinkEncoderProcessor::handle_collection(
                                                &processor_id,
                                                collection,
                                                &encoder,
                                                &mut row_count,
                                                &mut stream_state,
                                                &mode,
                                                &output,
                                                channel_capacities.data,
                                                &mut timer,
                                                &stats,
                                            )
                                        .await;
                                        // For synchronous processors, handle duration includes downstream send/backpressure
                                        // time (if a flush happens on the input path).
                                        stats.record_handle_duration(handle_start.elapsed());
                                        if let Err(err) = res {
                                            tracing::error!(
                                                processor_id = %processor_id,
                                                error = %err,
                                                "handle collection error"
                                            );
                                            stats.record_error(err.to_string());
                                        }
                                    }
                                    data => {
                                        let is_terminal = data.is_terminal();
                                        if is_terminal {
                                            if let Err(err) = async {
                                                let flushed = SinkEncoderProcessor::flush_buffer(
                                                    &processor_id,
                                                    &mut row_count,
                                                    &mut stream_state,
                                                    &output,
                                                    channel_capacities.data,
                                                    &stats,
                                                )
                                                .await?;
                                                if flushed > 0 {
                                                    stats.record_out(flushed as u64);
                                                }
                                                Ok::<(), ProcessorError>(())
                                            }
                                            .await
                                            {
                                                tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                                                stats.record_error(err.to_string());
                                            }
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
                                        if let Some(duration) = mode.duration() {
                                            SinkEncoderProcessor::schedule_timer(
                                                &mut timer,
                                                duration,
                                                row_count > 0,
                                            );
                                        }
                                    }
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(
                                    &processor_id,
                                    skipped,
                                    "sink encoder data input",
                                );
                                continue;
                            }
                            None => {
                                if let Err(err) = async {
                                    let flushed = SinkEncoderProcessor::flush_buffer(
                                        &processor_id,
                                        &mut row_count,
                                        &mut stream_state,
                                        &output,
                                        channel_capacities.data,
                                        &stats,
                                    )
                                    .await?;
                                    if flushed > 0 {
                                        stats.record_out(flushed as u64);
                                    }
                                    Ok::<(), ProcessorError>(())
                                }
                                .await
                                {
                                    tracing::error!(processor_id = %processor_id, error = %err, "flush error");
                                    stats.record_error(err.to_string());
                                }
                                tracing::info!(processor_id = %processor_id, "input streams closed");
                                return Ok(());
                            }
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
