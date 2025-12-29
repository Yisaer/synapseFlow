use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};
use arrow::array::{Array, BinaryArray, Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct HistorySourceConfig {
    pub datasource: PathBuf,
    pub topic: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub batch_size: usize,
    pub send_interval: Option<Duration>,
}

impl HistorySourceConfig {
    pub fn new(datasource: impl Into<PathBuf>, topic: impl Into<String>) -> Self {
        Self {
            datasource: datasource.into(),
            topic: topic.into(),
            start: None,
            end: None,
            batch_size: 100,
            send_interval: None,
        }
    }
}

pub struct HistorySourceConnector {
    id: String,
    config: HistorySourceConfig,
    receiver: Option<mpsc::Receiver<Result<ConnectorEvent, ConnectorError>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl HistorySourceConnector {
    pub fn new(id: impl Into<String>, config: HistorySourceConfig) -> Self {
        Self {
            id: id.into(),
            config,
            receiver: None,
            shutdown_tx: None,
        }
    }
}

impl SourceConnector for HistorySourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.shutdown_tx.is_some() {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }

        let (sender, receiver) = mpsc::channel(256);
        let config = self.config.clone();
        let connector_id = self.id.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        tokio::spawn(async move {
            info!(connector_id = %connector_id, "starting history replay");

            // 1. Discover and Sort Files
            let mut files = match discover_files(&config.datasource, &config.topic) {
                Ok(f) => f,
                Err(e) => {
                    error!(connector_id = %connector_id, error = %e, "failed to discover files");
                    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
                    return;
                }
            };

            files.sort_by_key(|f| f.seq);

            // 2. Filter Files
            let filtered_files: Vec<_> = files
                .into_iter()
                .filter(|f| {
                    let start_ok = config.end.map(|end| f.start_ts <= end).unwrap_or(true);
                    let end_ok = config.start.map(|start| f.end_ts >= start).unwrap_or(true);
                    start_ok && end_ok
                })
                .collect();

            if filtered_files.is_empty() {
                info!(connector_id = %connector_id, "no matching files found");
                let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
                return;
            }

            // 3. Process Files
            for file_info in filtered_files {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }

                info!(connector_id = %connector_id, file = %file_info.path.display(), "reading file");

                let path = file_info.path.clone();
                let batch_size = config.batch_size;
                let start_ts = config.start;
                let end_ts = config.end;
                let sender = sender.clone();
                let send_interval = config.send_interval;

                // Blocking reading via spawn_blocking
                let result =
                    tokio::task::spawn_blocking(move || read_parquet_file(path, batch_size)).await;

                match result {
                    Ok(Ok(batches)) => {
                        info!(connector_id = %connector_id, batch_count = batches.len(), "read parquet batches");
                        for batch in batches {
                            if let Some(interval) = send_interval {
                                tokio::time::sleep(interval).await;
                            }
                            // Process batch
                            if process_batch(batch, start_ts, end_ts, &sender)
                                .await
                                .is_err()
                            {
                                return; // Sended closed
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        error!(
                            connector_id = %connector_id,
                            file = %file_info.path.display(),
                            error = %e,
                            "failed to read parquet file"
                        );
                    }
                    Err(e) => {
                        error!(connector_id = %connector_id, error = %e, "task join error");
                        break;
                    }
                }
            }

            let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
        });

        self.receiver = Some(receiver);
        Ok(Box::pin(ReceiverStream::new(self.receiver.take().unwrap())))
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.receiver = None;
        info!(connector_id = %self.id, "history source closed");
        Ok(())
    }
}

struct FileInfo {
    path: PathBuf,
    start_ts: i64,
    end_ts: i64,
    seq: u64,
}

fn discover_files(datasource: &Path, topic: &str) -> std::io::Result<Vec<FileInfo>> {
    let mut files = Vec::new();
    let prefix = format!("nanomq_{}-", topic);

    for entry in fs::read_dir(datasource)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }

        if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
            if !filename.starts_with(&prefix) {
                continue;
            }

            // Parse: nanomq_{topic}-{start}~{end}_{seq}_{hash}.parquet
            // Remove prefix
            let rest = &filename[prefix.len()..];
            // Find ~
            let Some(tilde_pos) = rest.find('~') else {
                continue;
            };
            let start_ts_str = &rest[..tilde_pos];

            let rest = &rest[tilde_pos + 1..];
            // Find _
            let Some(underscore_pos) = rest.find('_') else {
                continue;
            };
            let end_ts_str = &rest[..underscore_pos];

            let rest = &rest[underscore_pos + 1..];
            // Find next _
            let Some(underscore_pos2) = rest.find('_') else {
                continue;
            };
            let seq_str = &rest[..underscore_pos2];

            if let (Ok(start), Ok(end), Ok(seq)) = (
                start_ts_str.parse::<i64>(),
                end_ts_str.parse::<i64>(),
                seq_str.parse::<u64>(),
            ) {
                files.push(FileInfo {
                    path,
                    start_ts: start,
                    end_ts: end,
                    seq,
                });
            }
        }
    }
    Ok(files)
}

fn read_parquet_file(path: PathBuf, batch_size: usize) -> Result<Vec<RecordBatch>, String> {
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

async fn process_batch(
    batch: RecordBatch,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
) -> Result<(), ()> {
    let ts_col = batch
        .column_by_name("ts")
        .or_else(|| {
            // Log actual column names for debugging
            let col_names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            println!("[WARN] ts column not found, available: {:?}", col_names);
            None
        })
        .ok_or(())?;
    let data_col = batch
        .column_by_name("data")
        .or_else(|| {
            let col_names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            println!("[WARN] data column not found, available: {:?}", col_names);
            None
        })
        .ok_or(())?;

    // Support both Int64 and UInt64 timestamp columns
    let ts_values: Vec<i64> = if let Some(ts_array) = ts_col.as_any().downcast_ref::<Int64Array>() {
        ts_array.iter().map(|v| v.unwrap_or(0)).collect()
    } else if let Some(ts_array) = ts_col.as_any().downcast_ref::<UInt64Array>() {
        ts_array
            .iter()
            .map(|v| v.map(|u| u as i64).unwrap_or(0))
            .collect()
    } else {
        println!("[WARN] ts column type mismatch: {:?}", ts_col.data_type());
        return Err(());
    };

    let data_array = data_col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            println!(
                "[WARN] data column type mismatch: {:?}",
                data_col.data_type()
            );
        })?;

    // tracing::debug!(num_rows = batch.num_rows(), "processing batch");
    for (i, &ts) in ts_values.iter().enumerate() {
        if let Some(start) = start_ts {
            if ts < start {
                continue;
            }
        }
        if let Some(end) = end_ts {
            if ts > end {
                continue;
            }
        }
        let data = data_array.value(i);
        let payload = data.to_vec();

        if sender
            .send(Ok(ConnectorEvent::Payload(payload)))
            .await
            .is_err()
        {
            return Err(());
        }
    }
    // tracing::debug!("batch processing complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn create_parquet_file(path: PathBuf, start: i64, num_rows: usize) {
        let mut tss = Vec::new();
        let mut datas = Vec::new();
        for i in 0..num_rows {
            let ts = start + i as i64;
            tss.push(ts);
            let mut row_data = ts.to_be_bytes().to_vec();
            row_data.extend_from_slice(format!("data{}", i).as_bytes());
            datas.push(row_data);
        }

        let ts_array = Int64Array::from_iter_values(tss);
        let data_array = BinaryArray::from_iter_values(datas.iter().map(|v| v.as_slice()));

        let schema = Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("data", DataType::Binary, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(ts_array), Arc::new(data_array)],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_discover_files() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let file1 = path.join("nanomq_test-100~200_1_hash1.parquet");
        File::create(&file1).unwrap();

        let file2 = path.join("nanomq_test-200~300_2_hash2.parquet");
        File::create(&file2).unwrap();

        // Wrong topic
        let file3 = path.join("nanomq_other-100~200_3_hash3.parquet");
        File::create(&file3).unwrap();

        // Not parquet
        let file4 = path.join("nanomq_test-100~200_4_hash4.txt");
        File::create(&file4).unwrap();

        let mut files = discover_files(path, "test").unwrap();
        files.sort_by_key(|f| f.seq);

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].seq, 1);
        assert_eq!(files[0].start_ts, 100);
        assert_eq!(files[0].end_ts, 200);
        assert_eq!(files[1].seq, 2);
    }

    #[tokio::test]
    async fn test_connector_flow() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // Create 2 files
        // File 1: ts 100..105 (5 rows)
        create_parquet_file(path.join("nanomq_flow-100~105_1_h.parquet"), 100, 5);
        // File 2: ts 105..110 (5 rows)
        create_parquet_file(path.join("nanomq_flow-105~110_2_h.parquet"), 105, 5);

        let config = HistorySourceConfig {
            datasource: path.to_path_buf(),
            topic: "flow".to_string(),
            start: Some(102), // Start from middle of first file
            end: Some(107),   // End in middle of second file
            batch_size: 2,
            send_interval: None,
        };

        let mut connector = HistorySourceConnector::new("test", config);
        let mut stream = connector.subscribe().unwrap();

        let mut row_count = 0;
        let mut ts_values = Vec::new();

        while let Some(event) = stream.next().await {
            match event {
                Ok(ConnectorEvent::Payload(payload)) => {
                    // Extract ts (first 8 bytes)
                    let (ts_bytes, data) = payload.split_at(8);
                    let ts = i64::from_be_bytes(ts_bytes.try_into().unwrap());

                    let s = String::from_utf8(data.to_vec()).unwrap();
                    if s.starts_with("data") {
                        ts_values.push(ts);
                        row_count += 1;
                    }
                }
                Ok(ConnectorEvent::EndOfStream) => break,
                Err(e) => panic!("Connector error: {}", e),
            }
        }

        // Expected: 102, 103, 104 (from file 1), 105, 106, 107 (from file 2) -> 6 rows
        assert_eq!(row_count, 6);
        assert_eq!(ts_values, vec![102, 103, 104, 105, 106, 107]);
    }

    #[tokio::test]
    async fn test_process_batch_uint64() {
        let (sender, mut receiver) = mpsc::channel(10);
        let start = 100;
        let num_rows = 5;

        // Create batch with UInt64 timestamp
        let ts_array = UInt64Array::from_iter_values((0..num_rows).map(|i| (start + i) as u64));
        let data_array = BinaryArray::from_iter_values((0..num_rows).map(|i| format!("data{}", i)));

        let schema = Schema::new(vec![
            Field::new("ts", DataType::UInt64, false),
            Field::new("data", DataType::Binary, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(ts_array), Arc::new(data_array)],
        )
        .unwrap();

        // Process
        super::process_batch(batch, None, None, &sender)
            .await
            .expect("process_batch failed");

        // Verify
        let mut count = 0;
        while let Some(Ok(ConnectorEvent::Payload(_))) = receiver.recv().await {
            count += 1;
            if count == num_rows {
                break;
            }
        }
        assert_eq!(count, num_rows);
    }

    #[tokio::test]
    async fn test_process_batch_missing_columns() {
        let (sender, _) = mpsc::channel(1);
        let schema = Schema::new(vec![Field::new("other", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();

        // Should fail
        assert!(super::process_batch(batch, None, None, &sender)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_process_batch_type_mismatch() {
        let (sender, _) = mpsc::channel(1);
        // ts as String instead of Int64/UInt64
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Utf8, false),
            Field::new("data", DataType::Binary, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["1", "2"])),
                Arc::new(BinaryArray::from(vec![&b"d1"[..], &b"d2"[..]])),
            ],
        )
        .unwrap();

        assert!(super::process_batch(batch, None, None, &sender)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_subscribe_already_subscribed() {
        let config = HistorySourceConfig::new("path", "topic");
        let mut connector = HistorySourceConnector::new("test", config);

        // First subscribe OK
        assert!(connector.subscribe().is_ok());

        // Second subscribe Error
        match connector.subscribe() {
            Err(ConnectorError::AlreadySubscribed(id)) => assert_eq!(id, "test"),
            _ => panic!("Expected AlreadySubscribed error"),
        }
    }

    #[test]
    fn test_discover_files_edge_cases() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // Valid file
        File::create(path.join("nanomq_t-100~200_1_h.parquet")).unwrap();

        // Invalid: missing parts
        File::create(path.join("nanomq_t-100~200_h.parquet")).unwrap();

        // Invalid: bad timestamp
        File::create(path.join("nanomq_t-abc~200_2_h.parquet")).unwrap();

        // Valid: different sequence
        File::create(path.join("nanomq_t-200~300_0_h.parquet")).unwrap(); // seq 0

        let mut files = discover_files(path, "t").unwrap();
        files.sort_by_key(|f| f.seq);

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].seq, 0); // Should sort seq 0 first
        assert_eq!(files[1].seq, 1);
    }
}
