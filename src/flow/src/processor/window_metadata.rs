use crate::model::{Collection, CollectionMetadata, RecordBatch, Tuple, WindowMetadata};
use crate::processor::ProcessorError;
use datatypes::TimestampValue;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn metadata_from_epoch_secs(
    start_secs: u64,
    end_secs: u64,
) -> Result<CollectionMetadata, ProcessorError> {
    Ok(CollectionMetadata::with_window(WindowMetadata::new(
        timestamp_from_epoch_secs(start_secs)?,
        timestamp_from_epoch_secs(end_secs)?,
    )))
}

pub(crate) fn metadata_from_system_time(
    start: SystemTime,
    end: SystemTime,
) -> Result<CollectionMetadata, ProcessorError> {
    Ok(CollectionMetadata::with_window(WindowMetadata::new(
        timestamp_from_system_time(start)?,
        timestamp_from_system_time(end)?,
    )))
}

pub(crate) fn attach_from_epoch_secs(
    collection: Box<dyn Collection>,
    start_secs: u64,
    end_secs: u64,
) -> Result<Box<dyn Collection>, ProcessorError> {
    attach(collection, metadata_from_epoch_secs(start_secs, end_secs)?)
}

pub(crate) fn attach_from_system_time(
    collection: Box<dyn Collection>,
    start: SystemTime,
    end: SystemTime,
) -> Result<Box<dyn Collection>, ProcessorError> {
    attach(collection, metadata_from_system_time(start, end)?)
}

pub(crate) fn record_batch_from_epoch_secs(
    rows: Vec<Tuple>,
    start_secs: u64,
    end_secs: u64,
) -> Result<RecordBatch, ProcessorError> {
    RecordBatch::new_with_metadata(rows, metadata_from_epoch_secs(start_secs, end_secs)?)
        .map_err(|err| ProcessorError::ProcessingError(err.to_string()))
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

fn timestamp_from_epoch_secs(secs: u64) -> Result<TimestampValue, ProcessorError> {
    let micros = secs.checked_mul(1_000_000).ok_or_else(|| {
        ProcessorError::ProcessingError(format!("window timestamp seconds overflow: {secs}"))
    })?;
    let micros = i64::try_from(micros).map_err(|_| {
        ProcessorError::ProcessingError(format!(
            "window timestamp micros exceeds i64 range: {micros}"
        ))
    })?;
    Ok(TimestampValue::from_epoch_micros(micros))
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
