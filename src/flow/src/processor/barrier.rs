use crate::processor::ControlSignal;
use crate::processor::{BarrierControlSignal, BarrierControlSignalKind, ProcessorError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BarrierKey {
    barrier_id: u64,
    kind: BarrierControlSignalKind,
}

#[derive(Debug)]
struct BarrierState {
    key: BarrierKey,
    arrived_count: usize,
}

#[derive(Debug)]
pub enum BarrierOutcome {
    Pending,
    Complete(BarrierControlSignal),
}

pub(crate) fn align_control_signal(
    aligner: &mut BarrierAligner,
    signal: ControlSignal,
) -> Result<Option<ControlSignal>, ProcessorError> {
    match signal {
        ControlSignal::Instant(_) => Ok(Some(signal)),
        ControlSignal::Barrier(barrier) => match aligner.on_barrier(barrier)? {
            BarrierOutcome::Pending => Ok(None),
            BarrierOutcome::Complete(barrier) => Ok(Some(ControlSignal::Barrier(barrier))),
        },
    }
}

/// Align barrier control signals across multiple upstream inputs.
///
/// Rules enforced:
/// - Channel isolation: callers maintain separate aligners for data/control channels.
/// - No overlap: a new barrier (different `barrier_id` or kind) cannot start before the current
///   barrier is complete.
pub struct BarrierAligner {
    channel: &'static str,
    expected_upstreams: usize,
    state: Option<BarrierState>,
}

impl BarrierAligner {
    pub fn new(channel: &'static str, expected_upstreams: usize) -> Self {
        Self {
            channel,
            expected_upstreams,
            state: None,
        }
    }

    pub fn on_barrier(
        &mut self,
        signal: BarrierControlSignal,
    ) -> Result<BarrierOutcome, ProcessorError> {
        if self.expected_upstreams == 0 {
            return Err(ProcessorError::InvalidConfiguration(format!(
                "barrier aligner ({}) cannot align barrier_id={} with expected_upstreams=0",
                self.channel,
                signal.barrier_id()
            )));
        }

        let key = BarrierKey {
            barrier_id: signal.barrier_id(),
            kind: signal.kind(),
        };

        match &mut self.state {
            None => {
                if self.expected_upstreams == 1 {
                    return Ok(BarrierOutcome::Complete(signal));
                }
                self.state = Some(BarrierState {
                    key,
                    arrived_count: 1,
                });
                Ok(BarrierOutcome::Pending)
            }
            Some(state) => {
                if state.key != key {
                    return Err(ProcessorError::ProcessingError(format!(
                        "barrier overlap on {} channel: pending barrier_id={}, got barrier_id={}",
                        self.channel, state.key.barrier_id, key.barrier_id
                    )));
                }
                state.arrived_count = state.arrived_count.saturating_add(1);
                if state.arrived_count > self.expected_upstreams {
                    return Err(ProcessorError::ProcessingError(format!(
                        "duplicate barrier on {} channel: barrier_id={} received_count={} expected_upstreams={}",
                        self.channel, key.barrier_id, state.arrived_count, self.expected_upstreams
                    )));
                }
                if state.arrived_count == self.expected_upstreams {
                    self.state = None;
                    return Ok(BarrierOutcome::Complete(signal));
                }
                Ok(BarrierOutcome::Pending)
            }
        }
    }
}
