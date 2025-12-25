//! DecoderProcessor - decodes StreamData::Bytes into StreamData::Collection.

use crate::codec::RecordDecoder;
use crate::eventtime::{EventtimeParseError, EventtimeTypeParser};
use crate::planner::decode_projection::DecodeProjection;
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, log_received_data,
    send_control_with_backpressure, send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

#[derive(Clone)]
pub struct EventtimeDecodeConfig {
    pub source_name: String,
    pub column_name: String,
    pub column_index: usize,
    pub type_key: String,
    pub parser: Arc<dyn EventtimeTypeParser>,
}

pub struct DecoderProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    decoder: Arc<dyn RecordDecoder>,
    projection: Option<Arc<std::sync::RwLock<Vec<String>>>>,
    decode_projection: Option<DecodeProjection>,
    eventtime: Option<EventtimeDecodeConfig>,
}

impl DecoderProcessor {
    pub fn new(plan_name: impl Into<String>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let plan_name = plan_name.into();
        Self::with_custom_id(plan_name.clone(), decoder)
    }

    pub fn with_custom_id(id: impl Into<String>, decoder: Arc<dyn RecordDecoder>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            decoder,
            projection: None,
            decode_projection: None,
            eventtime: None,
        }
    }

    pub fn with_projection(mut self, projection: Arc<std::sync::RwLock<Vec<String>>>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_decode_projection(mut self, projection: DecodeProjection) -> Self {
        self.decode_projection = Some(projection);
        self
    }

    pub fn with_eventtime(mut self, eventtime: EventtimeDecodeConfig) -> Self {
        self.eventtime = Some(eventtime);
        self
    }
}

impl Processor for DecoderProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let decoder = Arc::clone(&self.decoder);
        let projection = self.projection.clone();
        let decode_projection = self.decode_projection.clone();
        let eventtime = self.eventtime.clone();
        let processor_id = self.id.clone();
        let base_inputs = std::mem::take(&mut self.inputs);
        let mut input_streams = fan_in_streams(base_inputs);
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        tracing::info!(processor_id = %processor_id, "decoder processor starting");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                tracing::info!(processor_id = %processor_id, "received StreamEnd (control)");
                                tracing::info!(processor_id = %processor_id, "stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(mut data)) => {
                                log_received_data(&processor_id, &data);
                                if let StreamData::Bytes(payload) = &data {
                                    let decoded = if let Some(proj) = decode_projection.as_ref() {
                                        decoder.decode_with_decode_projection(payload, Some(proj))
                                    } else if let Some(lock) = &projection {
                                        let cols = lock
                                            .read()
                                            .expect("decoder projection lock poisoned")
                                            .clone();
                                        decoder.decode_with_projection(payload, Some(&cols))
                                    } else {
                                        decoder.decode(payload)
                                    };
                                    match decoded {
                                        Ok(batch) => {
                                            let result = apply_eventtime(batch, &eventtime);
                                            for err in result.errors {
                                                forward_error(&output, &processor_id, err).await?;
                                            }
                                            if let Some(batch) = result.batch {
                                                data = StreamData::collection(Box::new(batch));
                                            } else {
                                                continue;
                                            }
                                        }
                                        Err(err) => {
                                            let message = format!("decode error: {}", err);
                                            forward_error(&output, &processor_id, message).await?;
                                            continue;
                                        }
                                    }
                                }
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    tracing::info!(processor_id = %processor_id, "received StreamEnd (data)");
                                    tracing::info!(processor_id = %processor_id, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message =
                                    format!("Decoder input lagged by {} messages", skipped);
                                tracing::warn!(processor_id = %processor_id, skipped = skipped, "input lagged");
                                forward_error(&output, &processor_id, message).await?;
                                continue;
                            }
                            None => {
                                tracing::info!(processor_id = %processor_id, "stopped");
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

struct EventtimeApplyResult {
    batch: Option<crate::model::RecordBatch>,
    errors: Vec<String>,
}

fn apply_eventtime(
    batch: crate::model::RecordBatch,
    cfg: &Option<EventtimeDecodeConfig>,
) -> EventtimeApplyResult {
    let Some(cfg) = cfg else {
        return EventtimeApplyResult {
            batch: Some(batch),
            errors: Vec::new(),
        };
    };

    let mut errors = Vec::new();
    let mut rows = batch.into_rows();
    rows.retain_mut(|tuple| match extract_timestamp(tuple, cfg) {
        Ok(ts) => {
            tuple.timestamp = ts;
            true
        }
        Err(err) => {
            errors.push(format!(
                "eventtime parse error: source={}, column={}, type={}, {}",
                cfg.source_name, cfg.column_name, cfg.type_key, err
            ));
            false
        }
    });

    if rows.is_empty() {
        return EventtimeApplyResult {
            batch: None,
            errors,
        };
    }
    match crate::model::RecordBatch::new(rows) {
        Ok(batch) => EventtimeApplyResult {
            batch: Some(batch),
            errors,
        },
        Err(err) => {
            errors.push(format!("eventtime batch build error: {err}"));
            EventtimeApplyResult {
                batch: None,
                errors,
            }
        }
    }
}

fn extract_timestamp(
    tuple: &crate::model::Tuple,
    cfg: &EventtimeDecodeConfig,
) -> Result<SystemTime, EventtimeParseError> {
    let value = tuple
        .value_by_index(cfg.source_name.as_str(), cfg.column_index)
        .ok_or_else(|| {
            EventtimeParseError::new(format!(
                "eventtime column `{}` (index={}) missing in tuple",
                cfg.column_name, cfg.column_index
            ))
        })?;
    cfg.parser.parse(value)
}
