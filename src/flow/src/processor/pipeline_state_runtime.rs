use crate::planner::physical::PipelineStateUsage;
use crate::processor::processor_state::ProcessorState;
use crate::processor::ProcessorError;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn update_row_hit_state(
    state: &ProcessorState,
    usage: PipelineStateUsage,
    timestamp: SystemTime,
) -> Result<(), ProcessorError> {
    let last_hit_time_unix_ms = if usage.last_hit_time_unix_ms {
        Some(system_time_to_unix_ms(timestamp)?)
    } else {
        None
    };

    if usage.last_hit_count {
        state.last_hit_count.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(last_hit_time_unix_ms) = last_hit_time_unix_ms {
        state
            .last_hit_time_unix_ms
            .store(last_hit_time_unix_ms, Ordering::Relaxed);
    }
    Ok(())
}

pub(crate) fn update_collection_hit_state(state: &ProcessorState, usage: PipelineStateUsage) {
    if usage.last_agg_hit_count {
        state.last_agg_hit_count.fetch_add(1, Ordering::Relaxed);
    }
}

fn system_time_to_unix_ms(timestamp: SystemTime) -> Result<i64, ProcessorError> {
    let millis = timestamp
        .duration_since(UNIX_EPOCH)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid timestamp: {err}")))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| ProcessorError::ProcessingError("timestamp millis exceeds i64".to_string()))
}
