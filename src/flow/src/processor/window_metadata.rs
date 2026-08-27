use crate::model::{Collection, CollectionMetadata, RecordBatch, Tuple, WindowMetadata};
use crate::processor::ProcessorError;
use datatypes::TimestampValue;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn metadata_from_system_time(
    start: SystemTime,
    end: SystemTime,
) -> Result<CollectionMetadata, ProcessorError> {
    Ok(CollectionMetadata::with_window(WindowMetadata::new(
        timestamp_from_system_time(start)?,
        timestamp_from_system_time(end)?,
    )))
}

pub(crate) fn validate_system_time(time: SystemTime) -> Result<(), ProcessorError> {
    timestamp_from_system_time(time).map(|_| ())
}

pub(crate) fn attach_from_system_time(
    collection: Box<dyn Collection>,
    start: SystemTime,
    end: SystemTime,
) -> Result<Box<dyn Collection>, ProcessorError> {
    attach(collection, metadata_from_system_time(start, end)?)
}

pub(crate) fn record_batch_from_system_time(
    rows: Vec<Tuple>,
    start: SystemTime,
    end: SystemTime,
) -> Result<RecordBatch, ProcessorError> {
    RecordBatch::new_with_metadata(rows, metadata_from_system_time(start, end)?)
        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
}

fn attach(
    collection: Box<dyn Collection>,
    metadata: CollectionMetadata,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let rows = collection.into_rows().map_err(|err| {
        ProcessorError::ProcessingError(format!("failed to attach window metadata: {err}"))
    })?;
    let batch = RecordBatch::new_with_metadata(rows, metadata)
        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))?;
    Ok(Box::new(batch))
}

/// Subtracts `duration` from `time`, clamping the result to the Unix epoch. Sliding windows treat
/// the epoch as the earliest valid time coordinate, so a window start cannot precede it.
pub(crate) fn saturating_sub_to_epoch(time: SystemTime, duration: Duration) -> SystemTime {
    match time.checked_sub(duration) {
        Some(result) if result >= UNIX_EPOCH => result,
        _ => UNIX_EPOCH,
    }
}

/// Floors a timestamp down to the most recent window boundary, where boundaries are aligned to
/// multiples of `length` from the Unix epoch. This keeps window-boundary math independent of the
/// configured time unit, so processors can treat all units uniformly via `SystemTime`/`Duration`.
pub(crate) fn floor_to_window_start(
    time: SystemTime,
    length: Duration,
    label: &str,
) -> Result<SystemTime, ProcessorError> {
    let since_epoch = time
        .duration_since(UNIX_EPOCH)
        .map_err(|err| ProcessorError::ProcessingError(format!("invalid {label}: {err}")))?;
    let len_nanos = length.as_nanos().max(1);
    let start_nanos = since_epoch.as_nanos() / len_nanos * len_nanos;
    let start_nanos = u64::try_from(start_nanos).map_err(|_| {
        ProcessorError::ProcessingError("window start nanos exceeds u64 range".to_string())
    })?;
    Ok(UNIX_EPOCH + Duration::from_nanos(start_nanos))
}

fn timestamp_from_system_time(time: SystemTime) -> Result<TimestampValue, ProcessorError> {
    let duration = time.duration_since(UNIX_EPOCH).map_err(|err| {
        ProcessorError::ProcessingError(format!("invalid window timestamp: {err}"))
    })?;
    let micros = duration
        .as_secs()
        .checked_mul(1_000_000)
        .and_then(|secs_micros| secs_micros.checked_add(u64::from(duration.subsec_micros())))
        .ok_or_else(|| {
            ProcessorError::ProcessingError("window timestamp micros overflow".to_string())
        })?;
    let micros = i64::try_from(micros).map_err(|_| {
        ProcessorError::ProcessingError(format!(
            "window timestamp micros exceeds i64 range: {micros}"
        ))
    })?;
    Ok(TimestampValue::from_epoch_micros(micros))
}
