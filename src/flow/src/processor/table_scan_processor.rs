//! TableScanProcessor - bounded table scan source processor.

use crate::catalog::HistoryTableProps;
use crate::codec::RecordDecoder;
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams, log_broadcast_lagged,
    log_received_data, send_control_with_backpressure, send_with_backpressure, LinkOutput,
    LinkReceiver, ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use crate::table::history::{
    discover_history_files, extract_history_payloads, prune_history_files,
    read_history_parquet_file, DEFAULT_HISTORY_BATCH_SIZE,
};
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

const TABLE_SCAN_END_BARRIER_ID: u64 = 0;

enum TableScanEvent {
    Collection(crate::model::RecordBatch),
    Done,
    Error(String),
}

pub struct TableScanProcessor {
    id: String,
    table_name: String,
    props: HistoryTableProps,
    decoder: Arc<dyn RecordDecoder>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl TableScanProcessor {
    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        table_name: impl Into<String>,
        props: HistoryTableProps,
        decoder: Arc<dyn RecordDecoder>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Self {
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Self {
            id: id.into(),
            table_name: table_name.into(),
            props,
            decoder,
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::default()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        id: impl Into<String>,
        table_name: impl Into<String>,
        props: HistoryTableProps,
        decoder: Arc<dyn RecordDecoder>,
    ) -> Self {
        Self::new_with_channel_capacities(
            id,
            table_name,
            props,
            decoder,
            default_channel_capacities(),
        )
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        self.stats = stats;
    }

    async fn run_history_scan(
        processor_id: String,
        table_name: String,
        props: HistoryTableProps,
        decoder: Arc<dyn RecordDecoder>,
        scan_tx: mpsc::Sender<TableScanEvent>,
        spawner: TaskSpawner,
    ) {
        let result = Self::run_history_scan_inner(
            &processor_id,
            &table_name,
            props,
            decoder,
            scan_tx.clone(),
            spawner,
        )
        .await;
        match result {
            Ok(()) => {
                let _ = scan_tx.send(TableScanEvent::Done).await;
            }
            Err(err) => {
                let _ = scan_tx.send(TableScanEvent::Error(err)).await;
            }
        }
    }

    async fn run_history_scan_inner(
        processor_id: &str,
        table_name: &str,
        props: HistoryTableProps,
        decoder: Arc<dyn RecordDecoder>,
        scan_tx: mpsc::Sender<TableScanEvent>,
        spawner: TaskSpawner,
    ) -> Result<(), String> {
        let datasource = PathBuf::from(&props.datasource);
        let batch_size = props.batch_size.unwrap_or(DEFAULT_HISTORY_BATCH_SIZE);
        let mut files = discover_history_files(&datasource, &props.topic)
            .map_err(|err| format!("discover history files for table `{table_name}`: {err}"))?;
        files.sort_by_key(|file| file.seq);
        let files = prune_history_files(files, None, None);

        tracing::info!(
            processor_id,
            table = table_name,
            topic = %props.topic,
            files = files.len(),
            "table scan discovered history files"
        );

        for file in files {
            tracing::info!(
                processor_id,
                table = table_name,
                file = %file.path.display(),
                "table scan reading history file"
            );
            let path = file.path.clone();
            let batches = spawner
                .spawn_blocking(move || read_history_parquet_file(path, batch_size))
                .await
                .map_err(|err| format!("history parquet read task join error: {err}"))?
                .map_err(|err| {
                    format!("read history parquet file `{}`: {err}", file.path.display())
                })?;

            for batch in batches {
                let payloads = extract_history_payloads(&batch, &props.time_column, None, None)?;
                for payload in payloads {
                    let decoded = decoder
                        .decode(payload.as_slice())
                        .map_err(|err| format!("decode table `{table_name}` payload: {err}"))?;
                    if decoded.num_rows() == 0 {
                        continue;
                    }
                    if scan_tx
                        .send(TableScanEvent::Collection(decoded))
                        .await
                        .is_err()
                    {
                        tracing::info!(
                            processor_id,
                            table = table_name,
                            "table scan receiver closed"
                        );
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    async fn forward_data(
        processor_id: &str,
        output: &LinkOutput<StreamData>,
        channel_capacity: usize,
        stats: &ProcessorStats,
        data: StreamData,
    ) -> Result<(), ProcessorError> {
        log_received_data(processor_id, &data);
        if let Some(rows) = data.num_rows_hint() {
            stats.record_in(rows);
        }
        let out_rows = data.num_rows_hint();
        send_with_backpressure(output, channel_capacity, data, Some(stats)).await?;
        if let Some(rows) = out_rows {
            stats.record_out(rows);
        }
        Ok(())
    }
}

impl Processor for TableScanProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let processor_id = self.id.clone();
        let table_name = self.table_name.clone();
        let props = self.props.clone();
        let decoder = Arc::clone(&self.decoder);
        let channel_capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        let mut input_streams = fan_in_streams(std::mem::take(&mut self.inputs));
        let mut control_streams = fan_in_control_streams(std::mem::take(&mut self.control_inputs));
        let mut data_active = !input_streams.is_empty();
        let mut control_active = !control_streams.is_empty();
        let scan_spawner = spawner.clone();

        tracing::info!(
            processor_id = %processor_id,
            table = %table_name,
            "table scan processor starting"
        );
        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = spawner.spawn(async move {
            let (scan_tx, mut scan_rx) = mpsc::channel(channel_capacities.data);
            let producer_processor_id = processor_id.clone();
            let producer_table_name = table_name.clone();
            scan_spawner.spawn(Self::run_history_scan(
                producer_processor_id,
                producer_table_name,
                props,
                decoder,
                scan_tx,
                scan_spawner.clone(),
            ));
            let _ = ready_tx.send(Ok(()));

            loop {
                tokio::select! {
                    biased;
                    control_item = control_streams.next(), if control_active => {
                        if let Some(Ok(control_signal)) = control_item {
                            let is_terminal = control_signal.is_terminal();
                            send_control_with_backpressure(
                                &control_output,
                                channel_capacities.control,
                                control_signal,
                            )
                            .await?;
                            if is_terminal {
                                tracing::info!(
                                    processor_id = %processor_id,
                                    table = %table_name,
                                    "received StreamEnd (control)"
                                );
                                tracing::info!(processor_id = %processor_id, table = %table_name, "stopped");
                                return Ok(());
                            }
                        } else {
                            control_active = false;
                        }
                    }
                    item = input_streams.next(), if data_active => {
                        match item {
                            Some(Ok(data)) => {
                                let is_terminal = data.is_terminal();
                                Self::forward_data(
                                    &processor_id,
                                    &output,
                                    channel_capacities.data,
                                    stats.as_ref(),
                                    data,
                                )
                                .await?;
                                if is_terminal {
                                    tracing::info!(
                                        processor_id = %processor_id,
                                        table = %table_name,
                                        "received StreamEnd (data)"
                                    );
                                    tracing::info!(processor_id = %processor_id, table = %table_name, "stopped");
                                    return Ok(());
                                }
                            }
                            Some(Err(BroadcastStreamRecvError::Lagged(skipped))) => {
                                log_broadcast_lagged(&processor_id, skipped, "table scan data input");
                            }
                            None => {
                                data_active = false;
                            }
                        }
                    }
                    scan_item = scan_rx.recv() => {
                        match scan_item {
                            Some(TableScanEvent::Collection(batch)) => {
                                let data = StreamData::collection(Box::new(batch));
                                let out_rows = data.num_rows_hint();
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    data,
                                    Some(stats.as_ref()),
                                )
                                .await?;
                                if let Some(rows) = out_rows {
                                    stats.record_out(rows);
                                }
                            }
                            Some(TableScanEvent::Done) | None => {
                                let end = StreamData::stream_graceful_end(TABLE_SCAN_END_BARRIER_ID);
                                send_with_backpressure(
                                    &output,
                                    channel_capacities.data,
                                    end,
                                    Some(stats.as_ref()),
                                )
                                .await?;
                                tracing::info!(
                                    processor_id = %processor_id,
                                    table = %table_name,
                                    "table scan completed"
                                );
                                tracing::info!(processor_id = %processor_id, table = %table_name, "stopped");
                                return Ok(());
                            }
                            Some(TableScanEvent::Error(err)) => {
                                stats.record_error_logged("table scan processor error", err.clone());
                                return Err(ProcessorError::ProcessingError(err));
                            }
                        }
                    }
                }
            }
        });
        ProcessorStart::with_ready(handle, ready_rx)
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }

    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }
}
