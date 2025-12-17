//! WatermarkProcessor - emits or forwards watermarks以驱动时间相关算子。

use crate::planner::physical::{
    PhysicalPlan, PhysicalWatermark, WatermarkConfig, WatermarkStrategy,
};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast;
use tokio::time::{interval, Interval, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Watermark processor variants by window/operator kind.
pub enum WatermarkProcessor {
    Tumbling(TumblingWatermarkProcessor),
    Sliding(TumblingWatermarkProcessor),
}

impl WatermarkProcessor {
    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::Watermark(watermark) => match &watermark.config {
                WatermarkConfig::Tumbling { .. } => Some(WatermarkProcessor::Tumbling(
                    TumblingWatermarkProcessor::new(id, Arc::new(watermark.clone())),
                )),
                WatermarkConfig::Sliding { .. } => Some(WatermarkProcessor::Sliding(
                    TumblingWatermarkProcessor::new(id, Arc::new(watermark.clone())),
                )),
            },
            _ => None,
        }
    }
}

impl Processor for WatermarkProcessor {
    fn id(&self) -> &str {
        match self {
            WatermarkProcessor::Tumbling(p) => p.id(),
            WatermarkProcessor::Sliding(p) => p.id(),
        }
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        match self {
            WatermarkProcessor::Tumbling(p) => p.start(),
            WatermarkProcessor::Sliding(p) => p.start(),
        }
    }

    fn subscribe_output(&self) -> Option<broadcast::Receiver<StreamData>> {
        match self {
            WatermarkProcessor::Tumbling(p) => p.subscribe_output(),
            WatermarkProcessor::Sliding(p) => p.subscribe_output(),
        }
    }

    fn subscribe_control_output(&self) -> Option<broadcast::Receiver<ControlSignal>> {
        match self {
            WatermarkProcessor::Tumbling(p) => p.subscribe_control_output(),
            WatermarkProcessor::Sliding(p) => p.subscribe_control_output(),
        }
    }

    fn add_input(&mut self, receiver: broadcast::Receiver<StreamData>) {
        match self {
            WatermarkProcessor::Tumbling(p) => p.add_input(receiver),
            WatermarkProcessor::Sliding(p) => p.add_input(receiver),
        }
    }

    fn add_control_input(&mut self, receiver: broadcast::Receiver<ControlSignal>) {
        match self {
            WatermarkProcessor::Tumbling(p) => p.add_control_input(receiver),
            WatermarkProcessor::Sliding(p) => p.add_control_input(receiver),
        }
    }
}

/// Watermark operator processor.
pub struct TumblingWatermarkProcessor {
    id: String,
    physical: Arc<PhysicalWatermark>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl TumblingWatermarkProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalWatermark>) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            physical,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
        }
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn build_interval(strategy: &WatermarkStrategy) -> Option<Interval> {
        strategy.interval_duration().map(|duration| {
            let mut ticker = interval(duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker
        })
    }
}

impl Processor for TumblingWatermarkProcessor {
    fn id(&self) -> &str {
        self.id()
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mut ticker = Self::build_interval(self.physical.config.strategy());
        println!("[WatermarkProcessor:{id}] starting");

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                println!("[WatermarkProcessor:{id}] received StreamEnd (control)");
                                println!("[WatermarkProcessor:{id}] stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    _tick = async {
                        if let Some(ticker) = ticker.as_mut() {
                            ticker.tick().await;
                            Some(())
                        } else {
                            None
                        }
                    }, if ticker.is_some() => {
                        let ts = SystemTime::now();
                        send_with_backpressure(&output, StreamData::watermark(ts)).await?;
                        continue;
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    println!("[WatermarkProcessor:{id}] received StreamEnd (data)");
                                    println!("[WatermarkProcessor:{id}] stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                send_with_backpressure(&output, data).await?;
                                if is_terminal {
                                    println!("[WatermarkProcessor:{id}] received StreamEnd (data)");
                                    println!("[WatermarkProcessor:{id}] stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "WatermarkProcessor input lagged by {} messages",
                                    skipped
                                );
                                println!("[WatermarkProcessor:{id}] input lagged by {skipped} messages");
                                forward_error(&output, &id, message).await?;
                            }
                            None => {
                                println!("[WatermarkProcessor:{id}] stopped");
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
