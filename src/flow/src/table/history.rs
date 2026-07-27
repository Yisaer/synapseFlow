use arrow::array::{Array, BinaryArray, Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) const DEFAULT_HISTORY_BATCH_SIZE: usize = 100;
pub(crate) const HISTORY_DATA_COLUMN: &str = "data";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryFile {
    pub path: PathBuf,
    pub start_ts: i64,
    pub end_ts: i64,
    pub seq: u64,
}

pub(crate) fn discover_history_files(
    datasource: &Path,
    topic: &str,
) -> std::io::Result<Vec<HistoryFile>> {
    let mut files = Vec::new();
    let prefix = format!("nanomq_{}-", topic);

    for entry in fs::read_dir(datasource)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }

        let Some(filename) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(file) = parse_history_filename(filename, &prefix) else {
            continue;
        };

        files.push(HistoryFile { path, ..file });
    }

    Ok(files)
}

fn parse_history_filename(filename: &str, prefix: &str) -> Option<HistoryFile> {
    if !filename.starts_with(prefix) {
        return None;
    }

    let rest = &filename[prefix.len()..];
    let tilde_pos = rest.find('~')?;
    let start_ts_str = &rest[..tilde_pos];

    let rest = &rest[tilde_pos + 1..];
    let underscore_pos = rest.find('_')?;
    let end_ts_str = &rest[..underscore_pos];

    let rest = &rest[underscore_pos + 1..];
    let underscore_pos2 = rest.find('_')?;
    let seq_str = &rest[..underscore_pos2];

    let start_ts = start_ts_str.parse::<i64>().ok()?;
    let end_ts = end_ts_str.parse::<i64>().ok()?;
    let seq = seq_str.parse::<u64>().ok()?;

    Some(HistoryFile {
        path: PathBuf::new(),
        start_ts,
        end_ts,
        seq,
    })
}

pub(crate) fn prune_history_files(
    files: Vec<HistoryFile>,
    start: Option<i64>,
    end: Option<i64>,
) -> Vec<HistoryFile> {
    files
        .into_iter()
        .filter(|file| {
            let start_ok = end.map(|end| file.start_ts <= end).unwrap_or(true);
            let end_ok = start.map(|start| file.end_ts >= start).unwrap_or(true);
            start_ok && end_ok
        })
        .collect()
}

pub(crate) fn read_history_parquet_file(
    path: PathBuf,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, String> {
    let file = fs::File::open(&path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    let reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| e.to_string())?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| e.to_string())?);
    }
    Ok(batches)
}

pub(crate) fn extract_history_payloads(
    batch: &RecordBatch,
    time_column: &str,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
) -> Result<Vec<Vec<u8>>, String> {
    let ts_col = batch.column_by_name(time_column).ok_or_else(|| {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        format!("history time column `{time_column}` not found; available columns: {columns:?}")
    })?;
    let data_col = batch.column_by_name(HISTORY_DATA_COLUMN).ok_or_else(|| {
        let columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        format!(
            "history data column `{}` not found; available columns: {columns:?}",
            HISTORY_DATA_COLUMN
        )
    })?;

    let ts_values = if let Some(ts_array) = ts_col.as_any().downcast_ref::<Int64Array>() {
        ts_array
            .iter()
            .map(|value| value.unwrap_or(0))
            .collect::<Vec<_>>()
    } else if let Some(ts_array) = ts_col.as_any().downcast_ref::<UInt64Array>() {
        ts_array
            .iter()
            .map(|value| value.map(|ts| ts as i64).unwrap_or(0))
            .collect::<Vec<_>>()
    } else {
        return Err(format!(
            "history time column `{time_column}` has unsupported type {:?}",
            ts_col.data_type()
        ));
    };

    let data_array = data_col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            format!(
                "history data column `{}` has unsupported type {:?}",
                HISTORY_DATA_COLUMN,
                data_col.data_type()
            )
        })?;

    let mut payloads = Vec::new();
    for (idx, ts) in ts_values.iter().enumerate() {
        if start_ts.is_some_and(|start| *ts < start) || end_ts.is_some_and(|end| *ts > end) {
            continue;
        }
        if data_array.is_null(idx) {
            return Err(format!("history data column contains NULL at row {idx}"));
        }
        payloads.push(data_array.value(idx).to_vec());
    }

    Ok(payloads)
}
