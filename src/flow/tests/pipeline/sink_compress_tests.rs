use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::CompressionCodec;
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{FileSinkProps, PipelineDefinition};
use flow::planner::sink::CommonSinkProps;
use flow::{
    CreatePipelineRequest, FlowInstance, PipelineStopMode, SinkDefinition, SinkProps, SinkType,
};
use serde_json::json;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const COMPRESS_TEST_TIMEOUT: Duration = Duration::from_secs(10);

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

async fn wait_for_files(dir: &std::path::Path, count: usize) -> Vec<std::path::PathBuf> {
    timeout(COMPRESS_TEST_TIMEOUT, async {
        loop {
            let mut files = fs::read_dir(dir)
                .expect("read output dir")
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_file())
                .collect::<Vec<_>>();
            files.sort();
            if files.len() >= count {
                break files;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("file output timeout")
}

// coverage-covers: sink.compress.gzip_delivery, sink.connector.file_output
#[tokio::test]
async fn memory_source_gzip_compress_file_sink_roundtrips() {
    let instance = test_instance();
    let source_name = format!("mem_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.compress.gzip.{}", uuid::Uuid::new_v4());

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
        .expect("create stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("compress_gzip_{}", uuid::Uuid::new_v4().as_simple());

    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(
            FileSinkProps::new(output_dir.path().to_string_lossy(), "gzip_")
                .with_filename_suffix(".json.gz"),
        ),
    )
    .with_compression(CompressionCodec::gzip());

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
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            COMPRESS_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    publisher
        .publish_bytes(bytes::Bytes::from_static(br#"[{"v":42}]"#))
        .expect("publish bytes");

    let files = wait_for_files(output_dir.path(), 1).await;
    assert_eq!(files.len(), 1);

    let file_name = files[0]
        .file_name()
        .and_then(|n| n.to_str())
        .expect("file name");
    assert!(
        file_name.ends_with(".json.gz"),
        "expected .json.gz suffix, got: {file_name}"
    );

    // Decompress and verify JSON content
    let compressed_bytes = fs::read(&files[0]).expect("read compressed file");
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(compressed_bytes.as_slice());
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .expect("gzip decompress");
    let json: serde_json::Value = serde_json::from_slice(&decompressed).expect("json parse");
    assert_eq!(json, json!([{"v": 42}]));

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, COMPRESS_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: sink.compress.zstd_delivery, sink.connector.file_output
#[tokio::test]
async fn memory_source_zstd_compress_file_sink_roundtrips() {
    let instance = test_instance();
    let source_name = format!("mem_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.compress.zstd.{}", uuid::Uuid::new_v4());

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
        .expect("create stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("compress_zstd_{}", uuid::Uuid::new_v4().as_simple());

    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(
            FileSinkProps::new(output_dir.path().to_string_lossy(), "zstd_")
                .with_filename_suffix(".json.zst"),
        ),
    )
    .with_compression(CompressionCodec::zstd());

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
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            COMPRESS_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    publisher
        .publish_bytes(bytes::Bytes::from_static(br#"[{"v":99}]"#))
        .expect("publish bytes");

    let files = wait_for_files(output_dir.path(), 1).await;
    assert_eq!(files.len(), 1);

    let file_name = files[0]
        .file_name()
        .and_then(|n| n.to_str())
        .expect("file name");
    assert!(
        file_name.ends_with(".json.zst"),
        "expected .json.zst suffix, got: {file_name}"
    );

    // Decompress and verify JSON content
    let compressed_bytes = fs::read(&files[0]).expect("read compressed file");
    let decompressed =
        zstd::stream::decode_all(compressed_bytes.as_slice()).expect("zstd decompress");
    let json: serde_json::Value = serde_json::from_slice(&decompressed).expect("json parse");
    assert_eq!(json, json!([{"v": 99}]));

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, COMPRESS_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: sink.compress.gzip_delivery, sink.connector.file_output
#[tokio::test]
async fn gzip_compress_batch_count_matches_uncompressed_delivery_count() {
    let instance = test_instance();
    let source_name = format!("mem_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.compress.batch.{}", uuid::Uuid::new_v4());

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
        .expect("create stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("compress_batch_{}", uuid::Uuid::new_v4().as_simple());

    let sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(
            FileSinkProps::new(output_dir.path().to_string_lossy(), "batch_")
                .with_filename_suffix(".json.gz"),
        ),
    )
    .with_compression(CompressionCodec::gzip())
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
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            COMPRESS_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");

    for v in 1..=4 {
        publisher
            .publish_bytes(bytes::Bytes::from(format!(r#"{{"v":{v}}}"#)))
            .expect("publish bytes");
    }

    let files = wait_for_files(output_dir.path(), 2).await;
    assert_eq!(files.len(), 2, "expected 2 files (batch_count=2, 4 rows)");

    // Verify each file decompresses to valid JSON with 2 rows
    use flate2::read::GzDecoder;
    use std::io::Read;
    for file in &files {
        let compressed = fs::read(file).expect("read compressed file");
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .expect("gzip decompress");
        let json: serde_json::Value = serde_json::from_slice(&decompressed).expect("json parse");
        let arr = json.as_array().expect("expected json array");
        assert_eq!(arr.len(), 2, "expected 2 rows per file, got {}", arr.len());
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, COMPRESS_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}
