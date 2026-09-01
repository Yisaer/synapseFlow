use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{FileSinkProps, PipelineDefinition};
use flow::planner::sink::CommonSinkProps;
use flow::{
    ConnectorString, CreatePipelineRequest, FlowInstance, PipelineStopMode, SinkDefinition,
    SinkEncoderConfig, SinkProps, SinkType,
};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const FILE_SINK_TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn test_instance() -> FlowInstance {
    FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance")
}

fn json_schema(source_name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![ColumnSchema::new(
        source_name.to_string(),
        "v".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]))
}

fn generated_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = fs::read_dir(dir)
        .expect("read output dir")
        .map(|entry| entry.expect("read dir entry").path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    files.sort();
    files
}

async fn wait_for_generated_files(
    dir: &std::path::Path,
    expected_count: usize,
) -> Vec<std::path::PathBuf> {
    timeout(FILE_SINK_TEST_TIMEOUT, async {
        loop {
            let files = generated_files(dir);
            if files.len() >= expected_count {
                break files;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("file output timeout")
}

// coverage-covers: sink.connector.file_output
#[tokio::test]
async fn memory_source_feeds_file_sink_pipeline() {
    let instance = test_instance();
    let source_name = "memory_stream";
    let input_topic = format!("tests.file.sink.input.{}", uuid::Uuid::new_v4());
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    let stream = StreamDefinition::new(
        source_name,
        json_schema(source_name),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create memory stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = "memory_to_file_sink";
    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(FileSinkProps::new(
            output_dir.path().to_string_lossy(),
            ConnectorString::sensitive("VIN-123_{write_start_ms}_{seq}.json"),
        )),
    );
    let pipeline = PipelineDefinition::new(
        pipeline_id,
        format!("SELECT v FROM {source_name}"),
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    publisher
        .publish_bytes(bytes::Bytes::from_static(br#"[{"v":7}]"#))
        .expect("publish memory input");

    let file_path = wait_for_generated_files(output_dir.path(), 1)
        .await
        .remove(0);

    let file_name = file_path
        .file_name()
        .and_then(|value| value.to_str())
        .expect("file name");
    assert!(file_name.starts_with("VIN-123_"));
    assert!(file_name.ends_with("_000001.json"));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&fs::read(file_path).expect("read file"))
            .expect("decode file json"),
        json!([{ "v": 7 }])
    );

    instance
        .stop_pipeline(pipeline_id, PipelineStopMode::Quick, FILE_SINK_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: sink.encoder.csv_output, sink.connector.file_output
#[tokio::test]
async fn csv_encoder_writes_parseable_file_delivery() {
    let instance = test_instance();
    let source_name = format!("memory_stream_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.file.csv.input.{}", uuid::Uuid::new_v4());
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    let stream = StreamDefinition::new(
        source_name.clone(),
        json_schema(&source_name),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create memory stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("memory_to_csv_file_{}", uuid::Uuid::new_v4().as_simple());
    let mut encoder_props = JsonMap::new();
    encoder_props.insert("header".to_string(), JsonValue::Bool(true));
    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(FileSinkProps::new(
            output_dir.path().to_string_lossy(),
            "values_{write_start_ms}_{seq}.csv",
        )),
    )
    .with_encoder(SinkEncoderConfig::new("csv", encoder_props));
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        format!("SELECT v AS value FROM {source_name}"),
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    publisher
        .publish_bytes(bytes::Bytes::from_static(br#"[{"v":7},{"v":8}]"#))
        .expect("publish memory input");

    let file_path = wait_for_generated_files(output_dir.path(), 1)
        .await
        .remove(0);
    assert_eq!(
        file_path.extension().and_then(|value| value.to_str()),
        Some("csv")
    );
    let file_bytes = fs::read(file_path).expect("read CSV file");
    assert_eq!(file_bytes, b"value\n7\n8\n");
    let mut reader = csv::Reader::from_reader(file_bytes.as_slice());
    assert_eq!(reader.headers().expect("CSV header").get(0), Some("value"));
    let values = reader
        .records()
        .map(|record| {
            record
                .expect("valid CSV record")
                .get(0)
                .expect("value field")
                .parse::<i64>()
                .expect("integer value")
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![7, 8]);

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: sink.connector.file_output, sink.output.batching
#[tokio::test]
async fn file_sink_rolls_files_by_common_batch_count() {
    let instance = test_instance();
    let source_name = format!("memory_stream_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.file.sink.rolling.input.{}", uuid::Uuid::new_v4());
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    let stream = StreamDefinition::new(
        source_name.clone(),
        json_schema(&source_name),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create memory stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("memory_to_file_sink_{}", uuid::Uuid::new_v4().as_simple());
    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(FileSinkProps::new(
            output_dir.path().to_string_lossy(),
            "speed_{write_start_ms}_{seq}.json",
        )),
    )
    .with_common_props(CommonSinkProps {
        batch_count: Some(2),
        batch_duration: None,
    });
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        format!("SELECT v FROM {source_name}"),
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");

    for value in 1..=4 {
        publisher
            .publish_bytes(bytes::Bytes::from(format!(r#"{{"v":{value}}}"#)))
            .expect("publish memory input");
    }

    let files = wait_for_generated_files(output_dir.path(), 2).await;
    assert_eq!(files.len(), 2);
    let names = files
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .expect("file name")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert!(names.iter().all(|name| name.starts_with("speed_")));
    assert!(names.iter().all(|name| name.ends_with(".json")));

    let outputs = files
        .iter()
        .map(|path| {
            serde_json::from_slice::<serde_json::Value>(&fs::read(path).expect("read file"))
                .expect("decode file json")
        })
        .collect::<Vec<_>>();
    assert_eq!(outputs[0], json!([{ "v": 1 }, { "v": 2 }]));
    assert_eq!(outputs[1], json!([{ "v": 3 }, { "v": 4 }]));

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: sink.encoder.ndjson_output, sink.connector.file_output, sink.output.batching
#[tokio::test]
async fn ndjson_encoder_writes_batched_file_delivery() {
    let instance = test_instance();
    let source_name = format!("memory_stream_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.file.ndjson.input.{}", uuid::Uuid::new_v4());
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    let stream = StreamDefinition::new(
        source_name.clone(),
        json_schema(&source_name),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create memory stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("memory_to_ndjson_file_{}", uuid::Uuid::new_v4().as_simple());
    let mut encoder_props = JsonMap::new();
    encoder_props.insert(
        "format".to_string(),
        JsonValue::String("ndjson".to_string()),
    );
    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(FileSinkProps::new(
            output_dir.path().to_string_lossy(),
            "events_{write_start_ms}_{seq}.ndjson",
        )),
    )
    .with_encoder(SinkEncoderConfig::new("json", encoder_props))
    .with_common_props(CommonSinkProps {
        batch_count: Some(2),
        batch_duration: None,
    });
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        format!("SELECT v FROM {source_name}"),
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    for value in [1, 2] {
        publisher
            .publish_bytes(bytes::Bytes::from(format!(r#"{{"v":{value}}}"#)))
            .expect("publish memory input");
    }

    let files = wait_for_generated_files(output_dir.path(), 1).await;
    assert_eq!(files.len(), 1);
    assert!(files[0]
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".ndjson")));
    assert_eq!(
        fs::read(&files[0]).expect("read NDJSON file"),
        b"{\"v\":1}\n{\"v\":2}\n"
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            FILE_SINK_TEST_TIMEOUT,
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}
