use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use crate::shared_stream::SharedStreamRegistry;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};
use uuid::Uuid;

pub struct SharedStreamProcessor {
    id: String,
    stream_name: String,
    pipeline_id: Option<String>,
    required_columns: Vec<String>,
    required_slot_version: u64,
    registry: Arc<SharedStreamRegistry>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl SharedStreamProcessor {
    pub fn new(
        plan_name: &str,
        stream_name: impl Into<String>,
        registry: Arc<SharedStreamRegistry>,
    ) -> Self {
        Self::new_with_channel_capacities(
            plan_name,
            stream_name,
            registry,
            default_channel_capacities(),
        )
    }

    pub(crate) fn new_with_channel_capacities(
        plan_name: &str,
        stream_name: impl Into<String>,
        registry: Arc<SharedStreamRegistry>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let stream_name = stream_name.into();
        let id = plan_name.to_string();
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id,
            stream_name,
            pipeline_id: None,
            required_columns: Vec::new(),
            required_slot_version: 0,
            registry,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
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

    pub fn set_required_slot_version(&mut self, required_slot_version: u64) {
        self.required_slot_version = required_slot_version;
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

impl Processor for SharedStreamProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let has_data_inputs = !self.inputs.is_empty();
        let mut inputs = fan_in_streams(std::mem::take(&mut self.inputs));
        let mut input_active = has_data_inputs;
        let has_control_inputs = !self.control_inputs.is_empty();
        let mut control_inputs = fan_in_control_streams(std::mem::take(&mut self.control_inputs));
        let mut control_inputs_active = has_control_inputs;
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let channel_capacities = self.channel_capacities;
        let stream_name = self.stream_name.clone();
        let processor_id = self.id.clone();
        let stats = Arc::clone(&self.stats);
        let required_columns = std::mem::take(&mut self.required_columns);
        let required_slot_version = self.required_slot_version;
        let registry = Arc::clone(&self.registry);
        let pipeline_id = self
            .pipeline_id
            .clone()
            .unwrap_or_else(|| format!("pipeline-{}", Uuid::new_v4()));
        ProcessorStart::ready(spawner.spawn(async move {
            tracing::info!(
                processor_id = %processor_id,
                stream = %stream_name,
                pipeline_id = %pipeline_id,
                "subscribing to shared stream"
            );
            let consumer_id = format!("{pipeline_id}-{processor_id}");
            let mut subscription = registry
                .subscribe_with_requirements(
                    &stream_name,
                    consumer_id.clone(),
                    required_columns.clone(),
                    required_slot_version,
                )
                .await
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;

            if !required_columns.is_empty() || required_slot_version > 0 {
                let required: HashSet<String> = required_columns.into_iter().collect();
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let info = registry
                        .get_stream(&stream_name)
                        .await
                        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
                    let applied: HashSet<String> = info.decoding_columns.into_iter().collect();
                    if required.is_subset(&applied) && info.slot_version >= required_slot_version {
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err(ProcessorError::ProcessingError(format!(
                            "shared stream decoder not ready for consumer {consumer_id}: required columns or slot version not applied"
                        )));
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            }

            let (shared_data_rx, shared_control_rx) = subscription
                .take_receivers()
                .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
            let mut shared_data = shared_data_rx;
            let mut shared_control = shared_control_rx;
            loop {
                tokio::select! {
                    biased;
                    control_msg = control_inputs.next(), if control_inputs_active => {
                        if let Some(Ok(signal)) = control_msg {
                            let is_terminal = signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                signal,
                            )
                            .await?;
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            control_inputs_active = false;
                        }
                    }
                    shared_control_msg = shared_control.recv() => {
                        if let Some(signal) = shared_control_msg {
                            let is_terminal = signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                signal,
                            )
                            .await?;
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            return Ok(());
                        }
                    }
                    data_msg = inputs.next(), if input_active => {
                        if let Some(Ok(data)) = data_msg {
                            if let Some(rows) = data.row_count() {
                                stats.record_collection_in(rows);
                            }
                            let is_terminal = data.is_terminal();
                            let out_rows = data.row_count();
                            send_with_backpressure(
                                &output,
                                channel_capacities.data,
                                data,
                                Some(stats.as_ref()),
                            )
                            .await?;
                            if let Some(rows) = out_rows {
                                stats.record_collection_out(rows);
                            }
                            if is_terminal {
                                return Ok(());
                            }
                        } else {
                            input_active = false;
                        }
                    }
                    shared_data_msg = shared_data.recv() => {
                        match shared_data_msg {
                            Some(data) => {
                                if let Some(rows) = data.row_count() {
                                    stats.record_collection_in(rows);
                                }
                                let is_terminal = data.is_terminal();
                                let out_rows = data.row_count();
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    data,
                                    Some(stats.as_ref()),
                                )
                                .await?;
                                if let Some(rows) = out_rows {
                                    stats.record_collection_out(rows);
                                }
                                if is_terminal {
                                    return Ok(());
                                }
                            }
                            None => return Ok(()),
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
