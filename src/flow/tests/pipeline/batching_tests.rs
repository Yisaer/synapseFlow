//! Table-driven tests for batching behavior (encoder flush) in pipelines.

use datatypes::Value;
use flow::connector::{MemoryData, MemoryTopicKind};
use flow::model::{batch_from_columns_simple, Collection};
use flow::pipeline::MemorySinkProps;
use flow::pipeline::PipelineDefinition;
use flow::planner::sink::{CommonSinkProps, SinkOutputConfig};
use flow::FlowInstance;
use flow::{
    CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkEncoderConfig, SinkProps, SinkType,
};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;
use tokio::time::Duration;

use super::common::{
    assert_no_json_output, declare_memory_input_output_topics, install_memory_stream_schema,
    make_memory_topics, publish_input_collection, recv_next_collection, recv_next_json,
};

struct BatchCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    sink_common: CommonSinkProps,
    expected_batches: Vec<JsonValue>,
}

async fn run_batch_case(case: BatchCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_batching", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_common_props(case.sink_common);
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    for expected in case.expected_batches {
        let actual = recv_next_json(&mut output, timeout_duration).await;
        assert_eq!(
            actual, expected,
            "Wrong output JSON batch for test: {}",
            case.name
        );
    }

    // Stop after reading expected batches to avoid relying on stop/close to flush the final batch.
    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

struct BatchBytesCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    sink_common: CommonSinkProps,
    encoder: SinkEncoderConfig,
    output: Option<SinkOutputConfig>,
    expected_batches: Vec<&'static [u8]>,
}

async fn recv_next_bytes(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output");

        match item {
            Ok(MemoryData::Bytes(payload)) => return payload.as_ref().to_vec(),
            Ok(MemoryData::Collection(_)) => panic!("unexpected collection payload on bytes topic"),
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => panic!("pipeline output topic closed"),
        }
    }
}

async fn run_batch_bytes_case(case: BatchBytesCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_batching_transform", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_common_props(case.sink_common)
    .with_encoder(case.encoder);
    let sink = match case.output {
        Some(output) => sink.with_output(output),
        None => sink,
    };
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    for expected in case.expected_batches {
        let actual = recv_next_bytes(&mut output, timeout_duration).await;
        assert_eq!(
            actual.as_slice(),
            expected,
            "Wrong output bytes batch for test: {}",
            case.name
        );
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

#[tokio::test]
async fn pipeline_batching_table_driven() {
    let cases = vec![BatchCase {
        name: "flushes_by_count_then_by_duration",
        sql: "SELECT a FROM stream",
        input_data: vec![(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
        sink_common: CommonSinkProps {
            batch_count: Some(2),
            // Ensure the final partial batch is flushed without needing stop/close.
            batch_duration: Some(Duration::from_millis(50)),
        },
        expected_batches: vec![
            serde_json::json!([{"a": 1}, {"a": 2}]),
            serde_json::json!([{"a": 3}]),
        ],
    }];

    for case in cases {
        run_batch_case(case).await;
    }
}

#[tokio::test]
async fn pipeline_encoder_transform_table_driven() {
    let cases = vec![
        BatchBytesCase {
            name: "sink_encoder_flushes_transformed_json_bytes",
            sql: "SELECT a + 1 AS c, b + 10 AS d FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(3), Value::Int64(5)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(2), Value::Int64(4), Value::Int64(6)],
                ),
            ],
            sink_common: CommonSinkProps {
                batch_count: Some(2),
                // Ensure the final partial batch is flushed without relying on stop/close.
                batch_duration: Some(Duration::from_millis(50)),
            },
            encoder: SinkEncoderConfig::json_with_transform_template(
                r#"{"x":{{ json(.row.c) }},"y":{{ json(.row.d) }} }"#,
            ),
            output: None,
            expected_batches: vec![
                br#"[{"x":2,"y":12 },{"x":4,"y":14 }]"#,
                br#"[{"x":6,"y":16 }]"#,
            ],
        },
        BatchBytesCase {
            name: "delta_transform_batched_output_keeps_dense_rows_and_flushes_correctly",
            sql: "SELECT a, b FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
            ],
            sink_common: CommonSinkProps {
                batch_count: Some(16),
                batch_duration: Some(Duration::from_millis(50)),
            },
            encoder: SinkEncoderConfig::json_with_transform_template("{{ json(.row) }}"),
            output: Some(SinkOutputConfig::delta()),
            expected_batches: vec![br#"[{"a":1,"b":10},{"a":null,"b":null},{"a":2,"b":null}]"#],
        },
    ];

    for case in cases {
        run_batch_bytes_case(case).await;
    }
}

// coverage-covers: sink.output.batching
#[tokio::test]
async fn sink_encoder_flushes_partial_batch_on_graceful_stop() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_batching",
        "sink_encoder_flushes_partial_batch_on_graceful_stop",
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    let input_data = vec![(
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )];
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_common_props(CommonSinkProps {
        batch_count: Some(8),
        batch_duration: None,
    });
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), "SELECT a FROM stream", vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns).expect("create input batch");

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Graceful, timeout_duration)
        .await
        .expect("stop pipeline");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        actual,
        serde_json::json!([{"a": 1}, {"a": 2}, {"a": 3}]),
        "stop should flush the final partial batch"
    );

    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn multi_sink_graceful_stop_flushes_partial_batch_and_keeps_collection_sink_consistent() {
    let case_name =
        "multi_sink_graceful_stop_flushes_partial_batch_and_keeps_collection_sink_consistent";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, bytes_output_topic) = make_memory_topics("pipeline_batching", case_name);
    let (_, collection_output_topic) =
        make_memory_topics("pipeline_batching_collection", case_name);

    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input memory topic");
    instance
        .declare_memory_topic(
            &bytes_output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare bytes output memory topic");
    instance
        .declare_memory_topic(
            &collection_output_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare collection output memory topic");

    let input_data = vec![
        (
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        ),
        (
            "b".to_string(),
            vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
        ),
    ];
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut bytes_output = instance
        .open_memory_subscribe_bytes(&bytes_output_topic)
        .expect("subscribe bytes output");
    let mut collection_output = instance
        .open_memory_subscribe_collection(&collection_output_topic)
        .expect("subscribe collection output");

    let pipeline_id = format!("pipe_{}", bytes_output_topic);
    let bytes_sink = SinkDefinition::new(
        "mem_sink_bytes",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(bytes_output_topic.clone())),
    )
    .with_common_props(CommonSinkProps {
        batch_count: Some(8),
        batch_duration: None,
    });
    let collection_sink = SinkDefinition::new(
        "mem_sink_collection",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(collection_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()));
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a, b FROM stream",
        vec![bytes_sink, collection_sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns).expect("create input batch");

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    assert_no_json_output(&mut bytes_output, Duration::from_millis(300)).await;

    let collection = recv_next_collection(&mut collection_output, timeout_duration).await;
    assert_eq!(
        collection.num_rows(),
        3,
        "collection row count should match input"
    );
    for (row_idx, tuple) in collection.rows().iter().enumerate() {
        assert!(tuple.affiliate().is_none(), "affiliate should be empty");
        assert_eq!(
            tuple.messages().len(),
            1,
            "collection sink should materialize one message per tuple"
        );
        let message = &tuple.messages()[0];
        let entries: Vec<(&str, datatypes::Value)> =
            message.entries().map(|(k, v)| (k, v.clone())).collect();
        assert_eq!(
            entries,
            vec![
                ("a", Value::Int64((row_idx + 1) as i64)),
                ("b", Value::Int64(((row_idx + 1) as i64) * 10)),
            ],
            "collection sink should preserve full-row values before graceful stop"
        );
        assert!(
            tuple.output_mask().is_none(),
            "full collection branch should not add output mask"
        );
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Graceful, timeout_duration)
        .await
        .expect("stop pipeline");

    let actual = recv_next_json(&mut bytes_output, timeout_duration).await;
    assert_eq!(
        actual,
        serde_json::json!([
            {"a": 1, "b": 10},
            {"a": 2, "b": 20},
            {"a": 3, "b": 30}
        ]),
        "graceful stop should flush the buffered batch without regressing sibling branch output"
    );

    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: sink.output.row_diff, sink.output.batching, sink.output.omit_if_empty
#[tokio::test]
async fn delta_batched_output_suppresses_empty_batches_but_keeps_non_empty_ones() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_batching",
        "delta_batched_output_suppresses_empty_batches_but_keeps_non_empty_ones",
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    let schema_hint = vec![("a".to_string(), vec![Value::Int64(1), Value::Int64(2)])];
    install_memory_stream_schema(&instance, &input_topic, &schema_hint).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_common_props(CommonSinkProps {
        batch_count: Some(16),
        batch_duration: Some(Duration::from_millis(50)),
    })
    .with_output(SinkOutputConfig::delta().with_omit_if_empty(true));
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), "SELECT a FROM stream", vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let timeout_duration = Duration::from_secs(5);

    let first = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1)],
    )])
    .expect("create first input batch");
    publish_input_collection(&instance, &input_topic, Box::new(first), timeout_duration).await;
    let first_output = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(first_output, serde_json::json!([{"a": 1}]));

    let second = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1)],
    )])
    .expect("create second input batch");
    publish_input_collection(&instance, &input_topic, Box::new(second), timeout_duration).await;
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    let third = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(2)],
    )])
    .expect("create third input batch");
    publish_input_collection(&instance, &input_topic, Box::new(third), timeout_duration).await;
    let third_output = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(third_output, serde_json::json!([{"a": 2}]));

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}
