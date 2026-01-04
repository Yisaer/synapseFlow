use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, log_broadcast_lagged, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use crate::shared_stream_registry;
use futures::stream::StreamExt;
use std::collections::HashSet;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Instant};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

pub struct SharedStreamProcessor {
    id: String,
    stream_name: String,
    pipeline_id: Option<String>,
    required_columns: Vec<String>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl SharedStreamProcessor {
    pub fn new(plan_name: &str, stream_name: impl Into<String>) -> Self {
        let stream_name = stream_name.into();
        let id = plan_name.to_string();
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id,
            stream_name,
            pipeline_id: None,
            required_columns: Vec::new(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    pub fn set_pipeline_id(&mut self, pipeline_id: impl Into<String>) {
        self.pipeline_id = Some(pipeline_id.into());
    }

    /// Set the required columns (by name) for this pipeline's shared stream consumption.
    ///
    /// This is used to coordinate dynamic projection decode in the shared stream runtime.
    pub fn set_required_columns(&mut self, required_columns: Vec<String>) {
        self.required_columns = required_columns;
    }
}

impl Processor for SharedStreamProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let has_data_inputs = !self.inputs.is_empty();
        let mut inputs = fan_in_streams(std::mem::take(&mut self.inputs));
        let mut input_active = has_data_inputs;
        let has_control_inputs = !self.control_inputs.is_empty();
        let mut control_inputs = fan_in_control_streams(std::mem::take(&mut self.control_inputs));
        let mut control_inputs_active = has_control_inputs;
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let stream_name = self.stream_name.clone();
        let processor_id = self.id.clone();
        let required_columns = std::mem::take(&mut self.required_columns);
        let pipeline_id = self
            .pipeline_id
            .clone()
            .unwrap_or_else(|| format!("pipeline-{}", Uuid::new_v4()));
        tokio::spawn(async move {
            tracing::info!(
                processor_id = %processor_id,
                stream = %stream_name,
                pipeline_id = %pipeline_id,
                "subscribing to shared stream"
            );
            let registry = shared_stream_registry();
            let consumer_id = format!("{pipeline_id}-{processor_id}");
            let mut subscription = registry
                .subscribe(&stream_name, consumer_id.clone())
                .await
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

            if !required_columns.is_empty() {
                registry
                    .set_consumer_required_columns(
                        &stream_name,
                        &consumer_id,
                        required_columns.clone(),
                    )
                    .await
                    .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

                let required: HashSet<String> = required_columns.into_iter().collect();
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let info = registry
                        .get_stream(&stream_name)
                        .await
                        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
                    let applied: HashSet<String> = info.decoding_columns.into_iter().collect();
                    if required.is_subset(&applied) {
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err(ProcessorError::ProcessingError(format!(
                            "shared stream decoder not ready for consumer {consumer_id}: required columns not applied"
                        )));
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            }

            let (shared_data_rx, shared_control_rx) = subscription.take_receivers();
            let mut shared_data = BroadcastStream::new(shared_data_rx);
            let mut shared_control = BroadcastStream::new(shared_control_rx);
            loop {
                tokio::select! {
                    biased;
                    control_msg = control_inputs.next(), if control_inputs_active => {
                        if let Some(Ok(signal)) = control_msg {
                            let is_terminal = signal.is_terminal();
                            send_control_with_backpressure(&control_output, signal).await?;
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            control_inputs_active = false;
                        }
                    }
                    shared_control_msg = shared_control.next() => {
                        if let Some(Ok(signal)) = shared_control_msg {
                            let is_terminal = signal.is_terminal();
                            send_control_with_backpressure(&control_output, signal).await?;
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            return Ok(());
                        }
                    }
                    data_msg = inputs.next(), if input_active => {
                        if let Some(Ok(data)) = data_msg {
                            let is_terminal = data.is_terminal();
                            send_with_backpressure(&output, data).await?;
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            input_active = false;
                        }
                    }
                    shared_data_msg = shared_data.next() => {
                        match shared_data_msg {
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "shared stream data");
                                continue;
                            }
                            None => return Ok(()),
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
