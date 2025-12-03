//! BatchProcessor - aggregates collections based on count/duration thresholds.

use crate::model::{Collection, RecordBatch, Tuple};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
#[cfg(test)]
use datatypes::Value;
use futures::stream::StreamExt;
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Sleep};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// Processor that buffers collections before releasing them downstream.
pub struct BatchProcessor {
    id: String,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
    batch_count: Option<usize>,
    batch_duration: Option<Duration>,
}

enum BatchMode {
    CountOnly { count: usize },
    DurationOnly { duration: Duration },
    Combined { count: usize, duration: Duration },
}

impl BatchProcessor {
    pub fn new(
        id: impl Into<String>,
        batch_count: Option<usize>,
        batch_duration: Option<Duration>,
    ) -> Self {
        let (output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        let (control_output, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        Self {
            id: id.into(),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            batch_count,
            batch_duration,
        }
    }

    fn append_collection(buffer: &mut Vec<Tuple>, collection: &dyn Collection) {
        buffer.extend(collection.rows().iter().cloned());
    }

    async fn emit_batch(
        processor_id: &str,
        rows: Vec<Tuple>,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        let batch = RecordBatch::new(rows)
            .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
        let collection: Box<dyn Collection> = Box::new(batch);
        send_with_backpressure(output, StreamData::collection(collection)).await?;
        println!("[BatchProcessor:{processor_id}] flushed batch");
        Ok(())
    }

    async fn flush_all(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &broadcast::Sender<StreamData>,
    ) -> Result<(), ProcessorError> {
        if buffer.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(buffer);
        Self::emit_batch(processor_id, rows, output).await
    }

    async fn flush_count(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &broadcast::Sender<StreamData>,
        count: usize,
    ) -> Result<(), ProcessorError> {
        if buffer.len() < count {
            return Ok(());
        }
        let rows: Vec<Tuple> = buffer.drain(..count).collect();
        Self::emit_batch(processor_id, rows, output).await
    }

    async fn drain_by_count(
        processor_id: &str,
        buffer: &mut Vec<Tuple>,
        output: &broadcast::Sender<StreamData>,
        count: usize,
    ) -> Result<(), ProcessorError> {
        while buffer.len() >= count {
            Self::flush_count(processor_id, buffer, output, count).await?;
        }
        Ok(())
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

impl Processor for BatchProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let mode = match (self.batch_count, self.batch_duration) {
            (Some(count), Some(duration)) => BatchMode::Combined { count, duration },
            (Some(count), None) => BatchMode::CountOnly { count },
            (None, Some(duration)) => BatchMode::DurationOnly { duration },
            (None, None) => {
                panic!("BatchProcessor created without batch configuration");
            }
        };
        let processor_id = self.id.clone();

        tokio::spawn(async move {
            let mut buffer: Vec<Tuple> = Vec::new();
            let mut timer: Option<Pin<Box<Sleep>>> = None;
            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                BatchProcessor::flush_all(&processor_id, &mut buffer, &output).await?;
                                println!("[BatchProcessor:{processor_id}] received StreamEnd (control)");
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
                        BatchProcessor::flush_all(&processor_id, &mut buffer, &output).await?;
                        if let BatchMode::DurationOnly { duration } | BatchMode::Combined { duration, .. } = &mode {
                            BatchProcessor::schedule_timer(&mut timer, *duration, !buffer.is_empty());
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                BatchProcessor::append_collection(&mut buffer, collection.as_ref());
                                match &mode {
                                    BatchMode::CountOnly { count } => {
                                        BatchProcessor::drain_by_count(&processor_id, &mut buffer, &output, *count).await?;
                                    }
                                    BatchMode::DurationOnly { duration } => {
                                        BatchProcessor::schedule_timer(&mut timer, *duration, !buffer.is_empty());
                                    }
                                    BatchMode::Combined { count, duration } => {
                                        BatchProcessor::drain_by_count(&processor_id, &mut buffer, &output, *count).await?;
                                        BatchProcessor::schedule_timer(&mut timer, *duration, !buffer.is_empty());
                                    }
                                }
                            }
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                if is_terminal {
                                    BatchProcessor::flush_all(&processor_id, &mut buffer, &output).await?;
                                }
                                send_with_backpressure(&output, data.clone()).await?;
                                if is_terminal {
                                    println!("[BatchProcessor:{processor_id}] received StreamEnd (data)");
                                    return Ok(());
                                }
                                if let BatchMode::DurationOnly { duration } | BatchMode::Combined { duration, .. } = &mode {
                                    BatchProcessor::schedule_timer(&mut timer, *duration, !buffer.is_empty());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                let message = format!(
                                    "BatchProcessor input lagged by {} messages",
                                    skipped
                                );
                                println!("[BatchProcessor:{processor_id}] input lagged by {skipped} messages");
                                forward_error(&output, &processor_id, message).await?;
                            }
                            None => {
                                BatchProcessor::flush_all(&processor_id, &mut buffer, &output).await?;
                                println!("[BatchProcessor:{processor_id}] input streams closed");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_batch_processor_count_only() {
        let mut processor = BatchProcessor::new("batch_count", Some(2), None);
        let (tx, rx) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(rx);
        let (control_tx, control_rx) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_control_input(control_rx);
        processor.output.subscribe(); // ensure there is at least one subscriber
        let mut output = processor
            .subscribe_output()
            .expect("output available to subscribe");
        processor.start();

        let data = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "val".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )])
        .expect("batch");
        let _ = tx.send(StreamData::collection(Box::new(data)));

        let first = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("first batch timeout")
            .expect("first batch missing");
        assert_eq!(
            first.as_collection().unwrap().num_rows(),
            2,
            "count-only should flush first two rows"
        );
        drop(tx);
        let second = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("second batch timeout")
            .expect("second batch missing");
        assert_eq!(
            second.as_collection().unwrap().num_rows(),
            1,
            "remaining row should flush on close"
        );
        let _ = control_tx.send(ControlSignal::StreamQuickEnd);
        let _ = control_tx.send(ControlSignal::StreamQuickEnd);
    }

    #[tokio::test]
    async fn test_batch_processor_duration_only() {
        let mut processor =
            BatchProcessor::new("batch_duration", None, Some(Duration::from_millis(50)));
        let (tx, rx) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(rx);
        let (control_tx, control_rx) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_control_input(control_rx);
        let mut output = processor.subscribe_output().expect("output");
        processor.start();

        let data = batch_from_columns_simple(vec![(
            "stream".to_string(),
            "val".to_string(),
            vec![Value::Int64(1)],
        )])
        .expect("batch");
        let _ = tx.send(StreamData::collection(Box::new(data)));

        let batch = tokio::time::timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("duration batch timeout")
            .expect("duration batch missing");
        assert_eq!(
            batch.as_collection().unwrap().num_rows(),
            1,
            "duration-only should flush after timeout"
        );
        let _ = control_tx.send(ControlSignal::StreamQuickEnd);
    }
}
