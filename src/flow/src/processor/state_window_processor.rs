//! StateWindowProcessor - buffers rows between open/emit conditions.
//!
//! Semantics:
//! - When inactive and `open == true`, start buffering (do not emit even if `emit == true`).
//! - When active, buffer every incoming tuple. If `emit == true`, emit the buffered batch and close.
//! - When inactive and `emit == true`, ignore.

use crate::planner::physical::{PhysicalPlan, PhysicalStateWindow};
use crate::processor::base::{
    fan_in_control_streams, fan_in_streams, forward_error, send_control_with_backpressure,
    send_with_backpressure, DEFAULT_CHANNEL_CAPACITY,
};
use crate::processor::{ControlSignal, Processor, ProcessorError, StreamData};
use datatypes::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;

pub struct StateWindowProcessor {
    id: String,
    physical: Arc<PhysicalStateWindow>,
    inputs: Vec<broadcast::Receiver<StreamData>>,
    control_inputs: Vec<broadcast::Receiver<ControlSignal>>,
    output: broadcast::Sender<StreamData>,
    control_output: broadcast::Sender<ControlSignal>,
}

impl StateWindowProcessor {
    pub fn new(id: impl Into<String>, physical: Arc<PhysicalStateWindow>) -> Self {
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

    pub fn from_physical_plan(id: impl Into<String>, plan: Arc<PhysicalPlan>) -> Option<Self> {
        match plan.as_ref() {
            PhysicalPlan::StateWindow(window) => {
                Some(Self::new(id, Arc::new(window.as_ref().clone())))
            }
            _ => None,
        }
    }
}

impl Processor for StateWindowProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self) -> tokio::task::JoinHandle<Result<(), ProcessorError>> {
        let id = self.id.clone();
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let control_receivers = std::mem::take(&mut self.control_inputs);
        let mut control_streams = fan_in_control_streams(control_receivers);
        let mut control_active = !control_streams.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();

        let open_expr = self.physical.open_scalar.clone();
        let emit_expr = self.physical.emit_scalar.clone();

        tokio::spawn(async move {
            let mut active = false;
            let mut rows: VecDeque<crate::model::Tuple> = VecDeque::new();

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(&control_output, control_signal).await?;
                            if is_terminal {
                                println!("[StateWindowProcessor:{id}] stopped");
                                return Ok(());
                            }
                            continue;
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next() => {
                        match item {
                            Some(Ok(StreamData::Collection(collection))) => {
                                let tuples = match collection.into_rows() {
                                    Ok(rows) => rows,
                                    Err(e) => {
                                        forward_error(&output, &id, format!("failed to extract rows: {e}")).await?;
                                        continue;
                                    }
                                };

                                for tuple in tuples {
                                    let open = match open_expr.eval_with_tuple(&tuple) {
                                        Ok(Value::Bool(v)) => v,
                                        Ok(other) => {
                                            forward_error(&output, &id, format!("statewindow open must be bool, got {other:?}")).await?;
                                            continue;
                                        }
                                        Err(e) => {
                                            forward_error(&output, &id, format!("failed to evaluate statewindow open: {e}")).await?;
                                            continue;
                                        }
                                    };

                                    let emit = match emit_expr.eval_with_tuple(&tuple) {
                                        Ok(Value::Bool(v)) => v,
                                        Ok(other) => {
                                            forward_error(&output, &id, format!("statewindow emit must be bool, got {other:?}")).await?;
                                            continue;
                                        }
                                        Err(e) => {
                                            forward_error(&output, &id, format!("failed to evaluate statewindow emit: {e}")).await?;
                                            continue;
                                        }
                                    };

                                    if !active {
                                        if open {
                                            active = true;
                                            rows.push_back(tuple);
                                        }
                                        continue;
                                    }

                                    rows.push_back(tuple);
                                    if emit {
                                        if rows.is_empty() {
                                            active = false;
                                            continue;
                                        }
                                        let batch_rows: Vec<_> = rows.drain(..).collect();
                                        let batch = crate::model::RecordBatch::new(batch_rows)
                                            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
                                        send_with_backpressure(&output, StreamData::collection(Box::new(batch))).await?;
                                        active = false;
                                    }
                                }
                            }
                            Some(Ok(StreamData::Watermark(ts))) => {
                                send_with_backpressure(&output, StreamData::watermark(ts)).await?;
                            }
                            Some(Ok(StreamData::Control(signal))) => {
                                let is_terminal = signal.is_terminal();
                                let is_graceful = matches!(signal, ControlSignal::StreamGracefulEnd);
                                send_with_backpressure(&output, StreamData::control(signal)).await?;
                                if is_terminal {
                                    if is_graceful && active && !rows.is_empty() {
                                        let batch_rows: Vec<_> = rows.drain(..).collect();
                                        let batch = crate::model::RecordBatch::new(batch_rows)
                                            .map_err(|e| ProcessorError::ProcessingError(e.to_string()))?;
                                        send_with_backpressure(&output, StreamData::collection(Box::new(batch))).await?;
                                    }
                                    println!("[StateWindowProcessor:{id}] stopped");
                                    return Ok(());
                                }
                            }
                            Some(Ok(other)) => {
                                let is_terminal = other.is_terminal();
                                send_with_backpressure(&output, other).await?;
                                if is_terminal {
                                    println!("[StateWindowProcessor:{id}] stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                forward_error(&output, &id, format!("StateWindowProcessor input lagged by {skipped} messages")).await?;
                            }
                            None => {
                                println!("[StateWindowProcessor:{id}] stopped");
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
    use crate::expr::ScalarExpr;
    use datatypes::{BooleanType, ConcreteDatatype, Value};
    use sqlparser::ast::{Expr, Ident};
    use std::time::{Duration, UNIX_EPOCH};

    fn tuple_at(sec: u64) -> crate::model::Tuple {
        crate::model::Tuple::with_timestamp(Vec::new(), UNIX_EPOCH + Duration::from_secs(sec))
    }

    fn lit_bool(v: bool) -> ScalarExpr {
        ScalarExpr::Literal(Value::Bool(v), ConcreteDatatype::Bool(BooleanType))
    }

    #[tokio::test]
    async fn statewindow_open_then_emit_buffers_and_flushes() {
        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            lit_bool(true),
            lit_bool(true),
            Vec::new(),
            0,
        );
        let mut processor = StateWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        // First tuple opens the window (emit ignored because it was inactive).
        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());

        // Second tuple is buffered and triggers emit -> output includes both tuples.
        let batch = crate::model::RecordBatch::new(vec![tuple_at(2)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());

        match output_rx.recv().await.unwrap() {
            StreamData::Collection(collection) => {
                assert_eq!(collection.rows().len(), 2);
            }
            other => panic!("unexpected output: {}", other.description()),
        }
    }

    #[tokio::test]
    async fn statewindow_emit_ignored_when_inactive() {
        let physical = PhysicalStateWindow::new(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            lit_bool(false),
            lit_bool(true),
            Vec::new(),
            0,
        );
        let mut processor = StateWindowProcessor::new("sw", Arc::new(physical));
        let (input, _) = broadcast::channel(DEFAULT_CHANNEL_CAPACITY);
        processor.add_input(input.subscribe());
        let mut output_rx = processor.subscribe_output().unwrap();
        let _handle = processor.start();

        let batch = crate::model::RecordBatch::new(vec![tuple_at(1)]).expect("batch");
        assert!(input.send(StreamData::collection(Box::new(batch))).is_ok());
        tokio::task::yield_now().await;
        assert!(output_rx.try_recv().is_err());
    }
}
