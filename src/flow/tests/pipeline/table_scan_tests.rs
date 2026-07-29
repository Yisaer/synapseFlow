use arrow::array::{BinaryArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, Value};
use flow::catalog::{HistoryTableProps, StreamDecoderConfig, TableProps};
use flow::connector::{MemoryData, MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{MemorySinkProps, PipelineDefinition};
use flow::{
    CreatePipelineRequest, FlowInstance, PipelineError, PipelineStopMode, SinkDefinition, SinkProps,
};
use flow::{SinkType, TableDefinition as FlowTableDefinition};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value as JsonValue;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{timeout, Duration};

use super::common::{build_expected_json, make_memory_topics, normalize_json, ColumnCheck};

struct HistoryPayloadRow {
    ts: i64,
    json: &'static str,
}

struct TableScanCase {
    name: &'static str,
    sql: &'static str,
    rows: Vec<HistoryPayloadRow>,
    expected_rows: usize,
    column_checks: Vec<ColumnCheck>,
    sort_by: Option<&'static str>,
}

fn history_table_schema(table_name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new(
            table_name.to_string(),
            "ts".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            table_name.to_string(),
            "vehicle_id".to_string(),
            ConcreteDatatype::String(StringType),
        ),
        ColumnSchema::new(
            table_name.to_string(),
            "speed".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]))
}

fn write_history_parquet_file(dir: &TempDir, topic: &str, seq: u64, rows: &[HistoryPayloadRow]) {
    let start_ts = rows.first().map(|row| row.ts).unwrap_or(0);
    let end_ts = rows.last().map(|row| row.ts).unwrap_or(start_ts);
    let path = dir.path().join(format!(
        "nanomq_{topic}-{start_ts}~{end_ts}_{seq}_test.parquet"
    ));

    let ts_array = Int64Array::from_iter_values(rows.iter().map(|row| row.ts));
    let payloads = rows
        .iter()
        .map(|row| row.json.as_bytes())
        .collect::<Vec<_>>();
    let data_array = BinaryArray::from_iter_values(payloads);
    let schema = ArrowSchema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("data", DataType::Binary, false),
    ]);
    let batch = ArrowRecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(ts_array), Arc::new(data_array)],
    )
    .expect("build history parquet record batch");

    let file = File::create(path).expect("create history parquet file");
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, Arc::new(schema), Some(props)).expect("create parquet writer");
    writer.write(&batch).expect("write history parquet batch");
    writer.close().expect("close history parquet writer");
}

async fn create_history_table(
    instance: &FlowInstance,
    table_name: &str,
    dir: &TempDir,
    topic: &str,
) {
    let definition = FlowTableDefinition::new(
        table_name.to_string(),
        history_table_schema(table_name),
        TableProps::History(
            HistoryTableProps::new(dir.path().to_string_lossy().to_string(), topic.to_string())
                .with_batch_size(2),
        ),
        StreamDecoderConfig::json(),
    );
    instance
        .create_table(definition)
        .await
        .expect("create history table");
}

async fn collect_json_rows(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    expected_rows: usize,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let mut rows = Vec::new();

    while rows.len() < expected_rows {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for table scan output");

        match item {
            Ok(MemoryData::Bytes(payload)) => {
                let value: JsonValue =
                    serde_json::from_slice(payload.as_ref()).expect("invalid JSON output");
                match value {
                    JsonValue::Array(items) => rows.extend(items),
                    other => rows.push(other),
                }
            }
            Ok(MemoryData::Collection(_)) => {
                panic!("unexpected collection payload on bytes output topic")
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }

    JsonValue::Array(rows)
}

async fn assert_no_extra_json_output(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) {
    loop {
        match timeout(timeout_duration, output.recv()).await {
            Err(_) => return,
            Ok(Ok(MemoryData::Bytes(payload))) => {
                panic!(
                    "unexpected extra table scan output: {}",
                    String::from_utf8_lossy(payload.as_ref())
                );
            }
            Ok(Ok(MemoryData::Collection(_))) => {
                panic!("unexpected collection payload on bytes output topic")
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => return,
        }
    }
}

fn sort_json_rows_by_field(value: JsonValue, field: &str) -> JsonValue {
    let JsonValue::Array(mut rows) = value else {
        return value;
    };

    rows.sort_by(|left, right| {
        json_sort_key(left, field)
            .as_str()
            .cmp(json_sort_key(right, field).as_str())
    });
    JsonValue::Array(rows)
}

fn json_sort_key(row: &JsonValue, field: &str) -> String {
    row.as_object()
        .and_then(|obj| obj.get(field))
        .map(|value| match value {
            JsonValue::String(value) => format!("s:{value}"),
            JsonValue::Number(value) => format!("n:{value}"),
            JsonValue::Bool(value) => format!("b:{value}"),
            JsonValue::Null => "z:null".to_string(),
            other => format!("x:{other}"),
        })
        .unwrap_or_default()
}

async fn run_table_scan_case(case: TableScanCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (_, output_topic) = make_memory_topics("pipeline_table_scan", case.name);
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output memory topic");

    let table_name = "history_table";
    let topic = "vehicle";
    let dir = tempfile::tempdir().expect("create temp history dir");
    write_history_parquet_file(&dir, topic, 1, &case.rows);
    create_history_table(&instance, table_name, &dir, topic).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );

    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create table scan pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start table scan pipeline");

    let timeout_duration = Duration::from_secs(5);
    let mut expected = build_expected_json(case.expected_rows, &case.column_checks);
    let mut actual = collect_json_rows(&mut output, case.expected_rows, timeout_duration).await;
    if let Some(field) = case.sort_by {
        expected = sort_json_rows_by_field(expected, field);
        actual = sort_json_rows_by_field(actual, field);
    }
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "wrong output JSON for test: {}",
        case.name
    );
    assert_no_extra_json_output(&mut output, Duration::from_millis(200)).await;

    match instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
    {
        Ok(()) => {}
        Err(PipelineError::Runtime(err)) if err.contains("Timeout waiting for data") => {}
        Err(err) => panic!("stop table scan pipeline: {err}"),
    }
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete table scan pipeline");
}

// coverage-covers: source.table.history_scan, processor.table_scan, sink.connector.memory_output
#[tokio::test]
async fn pipeline_table_scan_table_driven() {
    let cases = vec![
        TableScanCase {
            name: "select_wildcard_from_history_table",
            sql: "SELECT * FROM history_table",
            rows: vec![
                HistoryPayloadRow {
                    ts: 1,
                    json: r#"{"ts":1,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 2,
                    json: r#"{"ts":2,"vehicle_id":"v2","speed":20}"#,
                },
                HistoryPayloadRow {
                    ts: 3,
                    json: r#"{"ts":3,"vehicle_id":"v1","speed":30}"#,
                },
            ],
            expected_rows: 3,
            sort_by: None,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "ts".to_string(),
                    expected_values: vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                },
                ColumnCheck {
                    expected_name: "vehicle_id".to_string(),
                    expected_values: vec![
                        Value::String("v1".into()),
                        Value::String("v2".into()),
                        Value::String("v1".into()),
                    ],
                },
                ColumnCheck {
                    expected_name: "speed".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
            ],
        },
        TableScanCase {
            name: "select_expr_from_filtered_history_table",
            sql: "SELECT speed + 1 AS next_speed FROM history_table WHERE ts > 1",
            rows: vec![
                HistoryPayloadRow {
                    ts: 1,
                    json: r#"{"ts":1,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 2,
                    json: r#"{"ts":2,"vehicle_id":"v2","speed":20}"#,
                },
                HistoryPayloadRow {
                    ts: 3,
                    json: r#"{"ts":3,"vehicle_id":"v1","speed":30}"#,
                },
            ],
            expected_rows: 2,
            sort_by: None,
            column_checks: vec![ColumnCheck {
                expected_name: "next_speed".to_string(),
                expected_values: vec![Value::Int64(21), Value::Int64(31)],
            }],
        },
        TableScanCase {
            name: "eos_global_incremental_avg_from_history_table",
            sql: "SELECT avg(speed) AS avg_speed FROM history_table GROUP BY eoswindow()",
            rows: vec![
                HistoryPayloadRow {
                    ts: 1,
                    json: r#"{"ts":1,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 2,
                    json: r#"{"ts":2,"vehicle_id":"v2","speed":20}"#,
                },
                HistoryPayloadRow {
                    ts: 3,
                    json: r#"{"ts":3,"vehicle_id":"v1","speed":30}"#,
                },
            ],
            expected_rows: 1,
            sort_by: None,
            column_checks: vec![ColumnCheck {
                expected_name: "avg_speed".to_string(),
                expected_values: vec![Value::Float64(20.0)],
            }],
        },
        TableScanCase {
            name: "eos_grouped_incremental_avg_from_history_table",
            sql: "SELECT vehicle_id, avg(speed) AS avg_speed FROM history_table GROUP BY vehicle_id, eoswindow()",
            rows: vec![
                HistoryPayloadRow {
                    ts: 1,
                    json: r#"{"ts":1,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 2,
                    json: r#"{"ts":2,"vehicle_id":"v2","speed":20}"#,
                },
                HistoryPayloadRow {
                    ts: 3,
                    json: r#"{"ts":3,"vehicle_id":"v1","speed":30}"#,
                },
            ],
            expected_rows: 2,
            sort_by: Some("vehicle_id"),
            column_checks: vec![
                ColumnCheck {
                    expected_name: "vehicle_id".to_string(),
                    expected_values: vec![
                        Value::String("v1".into()),
                        Value::String("v2".into()),
                    ],
                },
                ColumnCheck {
                    expected_name: "avg_speed".to_string(),
                    expected_values: vec![Value::Float64(20.0), Value::Float64(20.0)],
                },
            ],
        },
        TableScanCase {
            name: "eos_window_filter_before_post_aggregation_where",
            sql: "SELECT vehicle_id, avg(speed) AS avg_speed FROM history_table WHERE vehicle_id = 'v1' GROUP BY vehicle_id, eoswindow() FILTER (WHERE speed > 0)",
            rows: vec![
                HistoryPayloadRow {
                    ts: 1,
                    json: r#"{"ts":1,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 2,
                    json: r#"{"ts":2,"vehicle_id":"v1","speed":-100}"#,
                },
                HistoryPayloadRow {
                    ts: 3,
                    json: r#"{"ts":3,"vehicle_id":"v1","speed":30}"#,
                },
                HistoryPayloadRow {
                    ts: 4,
                    json: r#"{"ts":4,"vehicle_id":"v2","speed":20}"#,
                },
            ],
            expected_rows: 1,
            sort_by: None,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "vehicle_id".to_string(),
                    expected_values: vec![Value::String("v1".into())],
                },
                ColumnCheck {
                    expected_name: "avg_speed".to_string(),
                    expected_values: vec![Value::Float64(20.0)],
                },
            ],
        },
        TableScanCase {
            name: "eos_non_incremental_ndv_from_history_table",
            sql: "SELECT ndv(speed) AS distinct_speed FROM history_table GROUP BY eoswindow()",
            rows: vec![
                HistoryPayloadRow {
                    ts: 1,
                    json: r#"{"ts":1,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 2,
                    json: r#"{"ts":2,"vehicle_id":"v2","speed":20}"#,
                },
                HistoryPayloadRow {
                    ts: 3,
                    json: r#"{"ts":3,"vehicle_id":"v1","speed":10}"#,
                },
                HistoryPayloadRow {
                    ts: 4,
                    json: r#"{"ts":4,"vehicle_id":"v3","speed":30}"#,
                },
            ],
            expected_rows: 1,
            sort_by: None,
            column_checks: vec![ColumnCheck {
                expected_name: "distinct_speed".to_string(),
                expected_values: vec![Value::Int64(3)],
            }],
        },
    ];

    for case in cases {
        run_table_scan_case(case).await;
    }
}
