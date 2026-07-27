use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};
use crate::processor::base::normalize_channel_capacity;
use crate::runtime::TaskSpawner;
use crate::table::history::{
    discover_history_files, extract_history_payloads, prune_history_files,
    read_history_parquet_file, DEFAULT_HISTORY_BATCH_SIZE,
};
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;
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
            batch_size: DEFAULT_HISTORY_BATCH_SIZE,
            send_interval: None,
        }
    }
}

pub(crate) struct HistorySourceConnector {
    id: String,
    config: HistorySourceConfig,
    channel_capacity: usize,
    shutdown_tx: Option<oneshot::Sender<()>>,
    spawner: TaskSpawner,
}

impl HistorySourceConnector {
    pub fn new(id: impl Into<String>, config: HistorySourceConfig, spawner: TaskSpawner) -> Self {
        Self {
            id: id.into(),
            config,
            channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            shutdown_tx: None,
            spawner,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = normalize_channel_capacity(capacity);
        self
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

        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        let config = self.config.clone();
        let connector_id = self.id.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        let spawner = self.spawner.clone();

        let _task = self.spawner.spawn(async move {
            info!(connector_id = %connector_id, "starting history replay");

            // 1. Discover and Sort Files
            let mut files = match discover_history_files(&config.datasource, &config.topic) {
                Ok(f) => f,
                Err(e) => {
                    error!(connector_id = %connector_id, error = %e, "failed to discover files");
                    let _ = sender.send(Ok(ConnectorEvent::EndOfStream)).await;
                    return;
                }
            };

            files.sort_by_key(|f| f.seq);

            // 2. Filter Files
            let filtered_files = prune_history_files(files, config.start, config.end);

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

                // Blocking reading via instance-scoped spawn_blocking.
                let result = spawner
                    .spawn_blocking(move || read_history_parquet_file(path, batch_size))
                    .await;

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

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        info!(connector_id = %self.id, "history source closed");
        Ok(())
    }
}

async fn process_batch(
    batch: RecordBatch,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
) -> Result<(), ()> {
    let payloads = extract_history_payloads(&batch, "ts", start_ts, end_ts).map_err(|err| {
        tracing::warn!(error = %err, "failed to extract history payloads");
    })?;

    for payload in payloads {
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

        let mut files = discover_history_files(path, "test").unwrap();
        files.sort_by_key(|f| f.seq);

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].seq, 1);
        assert_eq!(files[0].start_ts, 100);
        assert_eq!(files[0].end_ts, 200);
        assert_eq!(files[1].seq, 2);
    }

    // coverage-covers: source.history.replay
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

        let spawner = crate::runtime::TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime"),
        );
        let mut connector = HistorySourceConnector::new("test", config, spawner);
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
                Ok(ConnectorEvent::Collection(_)) => {
                    panic!("history source should not emit collections");
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
        let spawner = crate::runtime::TaskSpawner::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime"),
        );
        let mut connector = HistorySourceConnector::new("test", config, spawner);

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

        let mut files = discover_history_files(path, "t").unwrap();
        files.sort_by_key(|f| f.seq);

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].seq, 0); // Should sort seq 0 first
        assert_eq!(files[1].seq, 1);
    }
}
