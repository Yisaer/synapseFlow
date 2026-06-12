//! Table-driven tests for pipeline creation helpers.

use datatypes::{
    BytesType, ColumnSchema, ConcreteDatatype, Int64Type, ListType, Schema, StringType,
    StructField, StructType, TimestampValue, Value,
};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::MemoryTopicKind;
use flow::model::{batch_from_columns_simple, Message, RecordBatch, Tuple};
use flow::pipeline::{MemorySinkProps, PipelineDefinition, SourceDefinition, SourceInputConfig};
use flow::planner::sink::{CommonSinkProps, SinkOutputConfig};
use flow::Collection;
use flow::FlowInstance;
use flow::SinkEncoderConfig;
use flow::{CreatePipelineRequest, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

use super::common::ColumnCheck;
use super::common::{
    assert_no_json_output, build_expected_json, declare_memory_input_output_topics,
    install_memory_stream_schema, install_memory_stream_schema_with_name, make_memory_topics,
    normalize_json, publish_input_collection, recv_next_json,
};
use super::common::{declare_memory_input_output_topics_with_output_kind, recv_next_collection};

struct TestCase {
    name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>, // checks for specific columns by name
}

async fn run_test_case(test_case: TestCase) {
    println!("Running test: {}", test_case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_table_driven_queries", test_case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &test_case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        test_case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", test_case.name));

    let columns = test_case
        .input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();

    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        timeout_duration,
    )
    .await;
    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        test_case.expected_columns,
        test_case.column_checks.len(),
        "expected_columns should match column_checks for JSON comparison (test: {})",
        test_case.name
    );
    let expected: JsonValue =
        build_expected_json(test_case.expected_rows, &test_case.column_checks);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong output JSON for test: {}",
        test_case.name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", test_case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", test_case.name));
}

struct PublishedRowSpec {
    keys_order: Vec<&'static str>,
    values: Vec<Value>,
}

struct SourceLayoutTestCase {
    name: &'static str,
    sql: &'static str,
    covers: &'static [&'static str],
    stream_schema_hint: Vec<(String, Vec<Value>)>, // (column_name, sample values)
    published_rows: Vec<PublishedRowSpec>,
    expected_rows: usize,
    expected_columns: usize,
    column_checks: Vec<ColumnCheck>,
}

async fn run_source_layout_test_case(test_case: SourceLayoutTestCase) {
    println!("Running test: {}", test_case.name);
    assert!(
        !test_case.covers.is_empty(),
        "test={} missing covers",
        test_case.name
    );

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_table_driven_memory_collection_sources_layout_normalize",
        test_case.name,
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema(&instance, &input_topic, &test_case.stream_schema_hint).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        test_case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", test_case.name));

    let topic_source: Arc<str> = Arc::<str>::from(input_topic.as_str());
    let rows = test_case
        .published_rows
        .iter()
        .map(|row| {
            assert_eq!(
                row.keys_order.len(),
                row.values.len(),
                "keys_order length must match values length"
            );
            let keys: Vec<Arc<str>> = row
                .keys_order
                .iter()
                .map(|k| Arc::<str>::from(*k))
                .collect();
            let values: Vec<Arc<Value>> = row.values.iter().cloned().map(Arc::new).collect();
            let msg = Arc::new(Message::new_shared_keys(
                Arc::clone(&topic_source),
                Arc::from(keys),
                values,
            ));
            Tuple::new(vec![msg])
        })
        .collect::<Vec<_>>();

    let test_batch = RecordBatch::new(rows)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        timeout_duration,
    )
    .await;

    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        test_case.expected_columns,
        test_case.column_checks.len(),
        "expected_columns should match column_checks for JSON comparison (test: {})",
        test_case.name
    );
    let expected: JsonValue =
        build_expected_json(test_case.expected_rows, &test_case.column_checks);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong output JSON for test: {}",
        test_case.name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", test_case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", test_case.name));
}

struct CollectionSinkTestCase {
    name: &'static str,
    sql: &'static str,
    covers: &'static [&'static str],
    input_data: Vec<(String, Vec<Value>)>, // (column_name, values)
    expected_rows: usize,
    expected_message_count: usize,
    expected_affiliate_none: bool,
    expected_columns: Vec<ColumnCheck>, // order matters
}

async fn run_collection_sink_test_case(test_case: CollectionSinkTestCase) {
    println!("Running test: {}", test_case.name);
    assert!(
        !test_case.covers.is_empty(),
        "test={} missing covers",
        test_case.name
    );

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_table_driven_collection_sinks", test_case.name);
    declare_memory_input_output_topics_with_output_kind(
        &instance,
        &input_topic,
        &output_topic,
        MemoryTopicKind::Collection,
    );
    install_memory_stream_schema(&instance, &input_topic, &test_case.input_data).await;

    let mut output = instance
        .open_memory_subscribe_collection(&output_topic)
        .expect("subscribe output collection");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        test_case.sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", test_case.name));

    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", test_case.name));

    let columns = test_case
        .input_data
        .clone()
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();

    let test_batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", test_case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        timeout_duration,
    )
    .await;

    let got = recv_next_collection(&mut output, timeout_duration).await;
    assert_eq!(
        got.num_rows(),
        test_case.expected_rows,
        "row count mismatch (test: {})",
        test_case.name
    );

    for (row_idx, tuple) in got.rows().iter().enumerate() {
        if test_case.expected_affiliate_none {
            assert!(tuple.affiliate().is_none(), "affiliate should be empty");
        }

        assert_eq!(
            tuple.messages().len(),
            test_case.expected_message_count,
            "message count mismatch"
        );

        let msg = &tuple.messages()[0];
        assert_eq!(
            msg.source(),
            "",
            "message source should be empty (downstream memory source normalizes it)"
        );

        let entries: Vec<(&str, datatypes::Value)> =
            msg.entries().map(|(k, v)| (k, v.clone())).collect();
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        let expected_keys: Vec<&str> = test_case
            .expected_columns
            .iter()
            .map(|c| c.expected_name.as_str())
            .collect();
        assert_eq!(keys, expected_keys, "column order mismatch");

        assert_eq!(
            entries.len(),
            test_case.expected_columns.len(),
            "unexpected column count"
        );

        for (col_idx, check) in test_case.expected_columns.iter().enumerate() {
            let got_name = entries[col_idx].0;
            assert_eq!(
                got_name,
                check.expected_name.as_str(),
                "column name mismatch"
            );
            let expected_value = check.expected_values[row_idx].clone();
            assert_eq!(entries[col_idx].1, expected_value, "value mismatch");
        }
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", test_case.name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", test_case.name));
}

struct RowDiffJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    covers: &'static [&'static str],
    input_data: Vec<(String, Vec<Value>)>,
    sink_common: CommonSinkProps,
    encoder: SinkEncoderConfig,
    output: SinkOutputConfig,
    expected: JsonValue,
}

async fn run_row_diff_json_case(case: RowDiffJsonCase) {
    println!("Running test: {}", case.name);
    assert!(!case.covers.is_empty(), "case={} missing covers", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_row_diff_json", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.input_data,
    )
    .await;

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
    .with_encoder(case.encoder)
    .with_output(case.output);
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
        .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual),
        normalize_json(case.expected),
        "Wrong row diff JSON for test: {}",
        case.name
    );

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

struct OmitIfEmptyJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    covers: &'static [&'static str],
    schema_hint: Vec<(String, Vec<Value>)>,
    input_batches: Vec<Vec<(String, Vec<Value>)>>,
    sink_common: CommonSinkProps,
    encoder: SinkEncoderConfig,
    output: SinkOutputConfig,
    expected_outputs: Vec<Option<JsonValue>>,
}

async fn run_omit_if_empty_json_case(case: OmitIfEmptyJsonCase) {
    println!("Running test: {}", case.name);
    assert!(!case.covers.is_empty(), "case={} missing covers", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_omit_if_empty_json", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.schema_hint,
    )
    .await;

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
    .with_encoder(case.encoder)
    .with_output(case.output);
    let pipeline = PipelineDefinition::new(pipeline_id.clone(), case.sql, vec![sink]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));

    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    assert_eq!(
        case.input_batches.len(),
        case.expected_outputs.len(),
        "input_batches and expected_outputs length mismatch for test: {}",
        case.name
    );

    for (batch_idx, (input_data, expected_output)) in case
        .input_batches
        .into_iter()
        .zip(case.expected_outputs.into_iter())
        .enumerate()
    {
        let columns = input_data
            .into_iter()
            .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
            .collect();
        let batch = batch_from_columns_simple(columns).unwrap_or_else(|_| {
            panic!(
                "Failed to create test RecordBatch for: {} batch={}",
                case.name, batch_idx
            )
        });

        publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

        match expected_output {
            Some(expected) => {
                let actual = recv_next_json(&mut output, timeout_duration).await;
                assert_eq!(
                    normalize_json(actual),
                    normalize_json(expected),
                    "Wrong omit_if_empty JSON for test: {} batch={}",
                    case.name,
                    batch_idx
                );
            }
            None => {
                assert_no_json_output(&mut output, Duration::from_millis(300)).await;
            }
        }
    }

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

struct SourceOnChangeJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    covers: &'static [&'static str],
    schema_hint: Vec<(String, Vec<Value>)>,
    source_input: SourceInputConfig,
    output: SinkOutputConfig,
    input_rows: Vec<JsonValue>,
    expected_outputs: Vec<Option<JsonValue>>,
}

async fn install_memory_json_stream_schema_with_name(
    instance: &FlowInstance,
    input_topic: &str,
    source_name: &str,
    columns: &[(String, Vec<Value>)],
) {
    let schema_columns = columns
        .iter()
        .map(|(name, values)| {
            let datatype = values
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            ColumnSchema::new(source_name.to_string(), name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        source_name.to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create json memory stream");
}

async fn install_memory_json_stream_4_schema(instance: &FlowInstance, input_topic: &str) {
    let element_xy_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("x".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("y".to_string(), ConcreteDatatype::String(StringType), true),
    ])));
    let b_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), true),
        StructField::new(
            "items".to_string(),
            ConcreteDatatype::List(ListType::new(Arc::new(element_xy_struct))),
            false,
        ),
    ])));
    let schema = Schema::new(vec![
        ColumnSchema::new(
            "stream_4".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new("stream_4".to_string(), "b".to_string(), b_struct),
    ]);
    let definition = StreamDefinition::new(
        "stream_4".to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream_4 json memory stream");
}

async fn run_source_on_change_json_case(case: SourceOnChangeJsonCase) {
    let SourceOnChangeJsonCase {
        name,
        source_name,
        sql,
        covers,
        schema_hint,
        source_input,
        output: sink_output,
        input_rows,
        expected_outputs,
    } = case;

    let _ = covers;
    println!("Running test: {}", name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_source_on_change", name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
    install_memory_json_stream_schema_with_name(&instance, &input_topic, source_name, &schema_hint)
        .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_output(sink_output);
    let pipeline =
        PipelineDefinition::new(pipeline_id.clone(), sql, vec![sink]).with_sources(vec![
            SourceDefinition::new(source_name).with_input(source_input),
        ]);
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", name));

    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", name));

    let timeout_duration = Duration::from_secs(5);
    assert_eq!(
        input_rows.len(),
        expected_outputs.len(),
        "input_rows and expected_outputs length mismatch for test: {}",
        name
    );

    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for memory source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");

    for (row_idx, (input_row, expected_output)) in input_rows
        .into_iter()
        .zip(expected_outputs.into_iter())
        .enumerate()
    {
        let payload = serde_json::to_vec(&input_row).unwrap_or_else(|_| {
            panic!(
                "Failed to encode test input row as JSON for: {} row={}",
                name, row_idx
            )
        });
        publisher.publish_bytes(payload).unwrap_or_else(|_| {
            panic!(
                "Failed to publish input bytes for: {} row={}",
                name, row_idx
            )
        });

        match expected_output {
            Some(expected) => {
                let actual = recv_next_json(&mut output, timeout_duration).await;
                assert_eq!(
                    normalize_json(actual),
                    normalize_json(expected),
                    "Wrong source on_change JSON for test: {} row={}",
                    name,
                    row_idx
                );
            }
            None => {
                assert_no_json_output(&mut output, Duration::from_millis(300)).await;
            }
        }
    }

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", name));
}

// coverage-covers: parser.select.alias_computing, planner.physical.by_index_projection_into_encoder_rewrite, sink.connector.memory_output
#[tokio::test]
async fn pipeline_table_driven_queries() {
    let test_cases = vec![
        TestCase {
            name: "wildcard",
            sql: "SELECT * FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 3,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                },
                ColumnCheck {
                    expected_name: "c".to_string(),
                    expected_values: vec![
                        Value::Int64(1000),
                        Value::Int64(2000),
                        Value::Int64(3000),
                    ],
                },
            ],
        },
        TestCase {
            name: "simple_projection",
            sql: "SELECT a + 1, b + 2 FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 1".to_string(),
                    expected_values: vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],
                },
                ColumnCheck {
                    expected_name: "b + 2".to_string(),
                    expected_values: vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],
                },
            ],
        },
        TestCase {
            name: "projection_with_alias",
            sql: "SELECT a, a + b + c AS abc FROM stream",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
                (
                    "c".to_string(),
                    vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)],
                ),
            ],
            expected_rows: 3,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "abc".to_string(),
                    expected_values: vec![
                        Value::Int64(1110),
                        Value::Int64(2220),
                        Value::Int64(3330),
                    ],
                },
            ],
        },
        TestCase {
            name: "by_index_projection_with_alias",
            sql: "SELECT a AS a1 FROM stream",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            )],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "a1".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            }],
        },
        TestCase {
            name: "simple_filter",
            sql: "SELECT a, b FROM stream WHERE a > 15",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(200), Value::Int64(300)],
                },
            ],
        },
        TestCase {
            name: "filter_with_projection",
            sql: "SELECT a + 5, b * 2 FROM stream WHERE a > 15",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a + 5".to_string(),
                    expected_values: vec![Value::Int64(25), Value::Int64(35)],
                },
                ColumnCheck {
                    expected_name: "b * 2".to_string(),
                    expected_values: vec![Value::Int64(400), Value::Int64(600)],
                },
            ],
        },
        TestCase {
            name: "filter_no_matches",
            sql: "SELECT a, b FROM stream WHERE a > 100",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 0,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![],
                },
            ],
        },
        TestCase {
            name: "filter_all_match",
            sql: "SELECT a FROM stream WHERE a > 5",
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            )],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
            }],
        },
        TestCase {
            name: "alias_can_be_used_in_select_and_where",
            sql: "SELECT a + 1 AS b1, b1 + 1 AS c1 FROM stream WHERE b1 > 1 AND c1 > 1",
            input_data: vec![("a".to_string(), vec![Value::Int64(0), Value::Int64(1), Value::Int64(2)])],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "b1".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(3)],
                },
                ColumnCheck {
                    expected_name: "c1".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(4)],
                },
            ],
        },
        TestCase {
            name: "order_by_projection_alias_non_aggregate",
            sql: "SELECT a + 1 AS b FROM stream ORDER BY b DESC",
            input_data: vec![("a".to_string(), vec![Value::Int64(0), Value::Int64(2), Value::Int64(1)])],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "b".to_string(),
                expected_values: vec![Value::Int64(3), Value::Int64(2), Value::Int64(1)],
            }],
        },
        TestCase {
            name: "order_by_group_key_alias_with_countwindow",
            sql: "SELECT b AS k, sum(a) AS s FROM stream GROUP BY countwindow(4), b ORDER BY k",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(2),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(1),
                    ],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "k".to_string(),
                    expected_values: vec![Value::Int64(1), Value::Int64(2)],
                },
                ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(2)],
                },
            ],
        },
        TestCase {
            name: "order_by_aggregate_alias_with_countwindow",
            sql: "SELECT b, sum(a) AS s FROM stream GROUP BY countwindow(4), b ORDER BY s DESC",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(1)],
                },
                ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(1)],
                },
            ],
        },
        TestCase {
            name: "having_filters_groups_countwindow",
            sql: "SELECT sum(a) AS s, b FROM stream GROUP BY countwindow(4), b HAVING sum(a) > 2 AND sum(a) < 10",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(5),
                        Value::Int64(0),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_rows: 1,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "s".to_string(),
                    expected_values: vec![Value::Int64(5)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(2)],
                },
            ],
        },
        TestCase {
            name: "having_filters_all_groups_emits_empty",
            sql: "SELECT sum(a) AS s FROM stream GROUP BY countwindow(4) HAVING sum(a) > 100",
            input_data: vec![(
                "a".to_string(),
                vec![
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Int64(4),
                ],
            )],
            expected_rows: 0,
            expected_columns: 0,
            column_checks: vec![],
        },
        TestCase {
            name: "aggregation_group_by_window_and_expr_order_by",
            sql: "SELECT sum(a) + 1, b + 1 FROM stream GROUP BY countwindow(4), b + 1 ORDER BY b + 1 ASC",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1),
                    ],
                ),
                (
                    "b".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(2),
                    ],
                ),
                (
                    "c".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(1),
                        Value::Int64(2),
                    ],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "sum(a) + 1".to_string(),
                    expected_values: vec![Value::Int64(3), Value::Int64(3)],
                },
                ColumnCheck {
                    expected_name: "b + 1".to_string(),
                    expected_values: vec![Value::Int64(2), Value::Int64(3)],
                },
            ],
        },
        // ========== Cross-type numeric equality (SQL eval layer) ==========
        TestCase {
            name: "where_float64_eq_int64_literal",
            sql: "SELECT * FROM stream WHERE speed = 0",
            input_data: vec![
                (
                    "speed".to_string(),
                    vec![
                        Value::Float64(0.0),
                        Value::Float64(1.0),
                        Value::Float64(0.5),
                    ],
                ),
                (
                    "name".to_string(),
                    vec![
                        Value::String("a".into()),
                        Value::String("b".into()),
                        Value::String("c".into()),
                    ],
                ),
            ],
            expected_rows: 1,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "speed".to_string(),
                    expected_values: vec![Value::Float64(0.0)],
                },
                ColumnCheck {
                    expected_name: "name".to_string(),
                    expected_values: vec![Value::String("a".into())],
                },
            ],
        },
        TestCase {
            name: "where_float64_neq_int64_literal",
            sql: "SELECT * FROM stream WHERE speed != 1",
            input_data: vec![
                (
                    "speed".to_string(),
                    vec![
                        Value::Float64(0.0),
                        Value::Float64(1.0),
                        Value::Float64(2.0),
                    ],
                ),
                (
                    "id".to_string(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Int64(3),
                    ],
                ),
            ],
            expected_rows: 2,
            expected_columns: 2,
            column_checks: vec![
                ColumnCheck {
                    expected_name: "speed".to_string(),
                    expected_values: vec![Value::Float64(0.0), Value::Float64(2.0)],
                },
                ColumnCheck {
                    expected_name: "id".to_string(),
                    expected_values: vec![Value::Int64(1), Value::Int64(3)],
                },
            ],
        },
        TestCase {
            name: "simple_case_operand_cross_type",
            sql: "SELECT CASE speed WHEN 0 THEN 'zero' WHEN 1 THEN 'one' ELSE 'other' END AS label FROM stream",
            input_data: vec![(
                "speed".to_string(),
                vec![
                    Value::Float64(0.0),
                    Value::Float64(1.0),
                    Value::Float64(2.0),
                ],
            )],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "label".to_string(),
                expected_values: vec![
                    Value::String("zero".into()),
                    Value::String("one".into()),
                    Value::String("other".into()),
                ],
            }],
        },
        TestCase {
            name: "array_contains_cross_type_numeric",
            sql: "SELECT array_contains(array_create(speed), 0) AS found FROM stream",
            input_data: vec![(
                "speed".to_string(),
                vec![
                    Value::Float64(0.0),
                    Value::Float64(1.0),
                    Value::Float64(2.0),
                ],
            )],
            expected_rows: 3,
            expected_columns: 1,
            column_checks: vec![ColumnCheck {
                expected_name: "found".to_string(),
                expected_values: vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(false),
                ],
            }],
        },
    ];

    for test_case in test_cases {
        run_test_case(test_case).await;
    }
}

#[tokio::test]
async fn pipeline_row_diff_json_table_driven() {
    let cases = vec![
        RowDiffJsonCase {
            name: "emits_sparse_object_for_unchanged_columns",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.row_diff"],
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
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {},
                {"a": 2}
            ]),
        },
        RowDiffJsonCase {
            name: "preserves_changed_to_null",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.row_diff"],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Null, Value::Null],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": null},
                {}
            ]),
        },
        RowDiffJsonCase {
            name: "respects_tracked_column_subset",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.row_diff"],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]),
        },
        RowDiffJsonCase {
            name: "row_diff_with_batch_keeps_row_diff_and_uses_sink_encoder",
            source_name: "stream_ab",
            sql: "SELECT a, b FROM stream_ab",
            covers: &[
                "sink.output.row_diff",
                "sink.output.batching",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
            ],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            sink_common: CommonSinkProps {
                batch_count: Some(16),
                batch_duration: Some(Duration::from_millis(50)),
            },
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]),
        },
        RowDiffJsonCase {
            name: "splits_partial_late_materialization_between_row_diff_and_encoder",
            source_name: "stream",
            sql: "SELECT a, b, flag AS c FROM stream",
            covers: &[
                "sink.output.row_diff",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
                "planner.physical.partial_by_index_row_diff_and_encoder_rewrite",
            ],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(100), Value::Int64(101), Value::Int64(102)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10, "c": 100},
                {"a": 2, "c": 101},
                {"a": 3, "b": 11, "c": 102}
            ]),
        },
        RowDiffJsonCase {
            name: "row_diff_batching_partial_late_materialization_keeps_encoder_owned_columns",
            source_name: "stream",
            sql: "SELECT a, b, flag AS c FROM stream",
            covers: &[
                "sink.output.row_diff",
                "sink.output.batching",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
                "planner.physical.partial_by_index_row_diff_and_encoder_rewrite",
            ],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(100), Value::Int64(101), Value::Int64(102)],
                ),
            ],
            sink_common: CommonSinkProps {
                batch_count: Some(16),
                batch_duration: Some(Duration::from_millis(50)),
            },
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]),
            expected: serde_json::json!([
                {"a": 1, "b": 10, "c": 100},
                {"a": 2, "c": 101},
                {"a": 3, "b": 11, "c": 102}
            ]),
        },
        RowDiffJsonCase {
            name: "splits_partial_late_materialization_between_row_diff_and_encoder_with_aliases",
            source_name: "stream",
            sql: "SELECT a AS x, b AS y, flag AS z FROM stream",
            covers: &[
                "sink.output.row_diff",
                "parser.select.alias_computing",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
                "planner.physical.partial_by_index_row_diff_and_encoder_rewrite",
            ],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
                (
                    "flag".to_string(),
                    vec![Value::Int64(100), Value::Int64(101), Value::Int64(102)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["y"]),
            expected: serde_json::json!([
                {"x": 1, "y": 10, "z": 100},
                {"x": 2, "z": 101},
                {"x": 3, "y": 11, "z": 102}
            ]),
        },
        RowDiffJsonCase {
            name: "supports_alias_in_by_index_row_diff_rewrite",
            source_name: "stream",
            sql: "SELECT a AS x FROM stream",
            covers: &["sink.output.row_diff"],
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
            )],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"x": 1},
                {},
                {"x": 2}
            ]),
        },
        RowDiffJsonCase {
            name: "works_with_computed_affiliate_column",
            source_name: "stream",
            sql: "SELECT a, a + 1 AS x FROM stream",
            covers: &["sink.output.row_diff"],
            input_data: vec![(
                "a".to_string(),
                vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
            )],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["x"]),
            expected: serde_json::json!([
                {"a": 1, "x": 2},
                {"a": 1},
                {"a": 2, "x": 3}
            ]),
        },
        RowDiffJsonCase {
            name: "supports_mixed_alias_partial_by_index_row_diff_rewrite",
            source_name: "stream_ab",
            sql: "SELECT a AS x, b + 1 AS y FROM stream_ab",
            covers: &["sink.output.row_diff"],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(11), Value::Int64(11)],
                ),
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"x": 1, "y": 11},
                {"y": 12},
                {"x": 2}
            ]),
        },
        RowDiffJsonCase {
            name: "template_transform_still_sees_dense_row_on_delta_branch",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.row_diff", "sink.encoder.transform"],
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
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json_with_transform_template("{{ json(.row) }}"),
            output: SinkOutputConfig::delta(),
            expected: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": null, "b": null},
                {"a": 2, "b": null}
            ]),
        },
    ];

    for case in cases {
        run_row_diff_json_case(case).await;
    }
}

// coverage-covers: parser.select.alias_computing, sink.encoder.transform
#[tokio::test]
async fn transform_template_with_alias_projection_keeps_output_correct() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics(
        "pipeline_transform_template",
        "transform_template_with_alias_projection_keeps_output_correct",
    );
    declare_memory_input_output_topics(&instance, &input_topic, &output_topic);
    let input_data = vec![
        (
            "a".to_string(),
            vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
        ),
        (
            "b".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        ),
    ];
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a AS x, b AS y FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_encoder(SinkEncoderConfig::json_with_transform_template(
            r#"{"x":{{ json(.row.x) }},"y":{{ json(.row.y) }} }"#,
        ))],
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
    let test_batch = batch_from_columns_simple(columns).expect("create input batch");
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(test_batch),
        Duration::from_secs(5),
    )
    .await;

    let actual = recv_next_json(&mut output, Duration::from_secs(5)).await;
    assert_eq!(
        normalize_json(actual),
        normalize_json(serde_json::json!([
            {"x": 10, "y": 1},
            {"x": 20, "y": 2},
            {"x": 30, "y": 3}
        ])),
        "transform template should see aliased row values"
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            PipelineStopMode::Quick,
            Duration::from_secs(5),
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn pipeline_omit_if_empty_json_table_driven() {
    let cases = vec![
        OmitIfEmptyJsonCase {
            name: "full_non_empty_collection_is_forwarded",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.omit_if_empty"],
            schema_hint: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
            ],
            input_batches: vec![vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
            ]],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::default().with_omit_if_empty(true),
            expected_outputs: vec![Some(serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2, "b": 20},
                {"a": 3, "b": 30}
            ]))],
        },
        OmitIfEmptyJsonCase {
            name: "full_empty_collection_is_suppressed",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.omit_if_empty"],
            schema_hint: vec![
                ("a".to_string(), vec![Value::Int64(1)]),
                ("b".to_string(), vec![Value::Int64(10)]),
            ],
            input_batches: vec![vec![("a".to_string(), vec![]), ("b".to_string(), vec![])]],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::default().with_omit_if_empty(true),
            expected_outputs: vec![None],
        },
        OmitIfEmptyJsonCase {
            name: "delta_non_empty_collection_is_forwarded",
            source_name: "stream",
            sql: "SELECT a, b FROM stream",
            covers: &["sink.output.omit_if_empty"],
            schema_hint: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            input_batches: vec![vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ]],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta_with_columns(["b"]).with_omit_if_empty(true),
            expected_outputs: vec![Some(serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]))],
        },
        OmitIfEmptyJsonCase {
            name: "delta_repeated_same_row_is_suppressed_after_first_emit",
            source_name: "stream",
            sql: "SELECT a FROM stream",
            covers: &["sink.output.omit_if_empty"],
            schema_hint: vec![("a".to_string(), vec![Value::Int64(1)])],
            input_batches: vec![
                vec![("a".to_string(), vec![Value::Int64(1)])],
                vec![("a".to_string(), vec![Value::Int64(1)])],
                vec![("a".to_string(), vec![Value::Int64(1)])],
            ],
            sink_common: CommonSinkProps::default(),
            encoder: SinkEncoderConfig::json(),
            output: SinkOutputConfig::delta().with_omit_if_empty(true),
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"a": 1}
                ])),
                None,
                None,
            ],
        },
    ];

    for case in cases {
        run_omit_if_empty_json_case(case).await;
    }
}

#[tokio::test]
async fn pipeline_source_on_change_json_table_driven() {
    let cases = vec![
        SourceOnChangeJsonCase {
            name: "explicit_columns_filters_rows_within_single_collection",
            source_name: "stream",
            sql: "SELECT speed, rpm FROM stream",
            covers: &["source.on_change.gating"],
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(20)],
                ),
                (
                    "rpm".to_string(),
                    vec![Value::Int64(1000), Value::Int64(1000), Value::Int64(2000)],
                ),
            ],
            source_input: SourceInputConfig::on_change_with_columns(["speed", "rpm"]),
            output: SinkOutputConfig::default(),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 20, "rpm": 2000}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10, "rpm": 1000}
                ])),
                None,
                Some(serde_json::json!([
                    {"speed": 20, "rpm": 2000}
                ])),
            ],
        },
        SourceOnChangeJsonCase {
            name: "hidden_tracked_column_controls_emission",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            covers: &["source.on_change.gating"],
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![Value::Int64(10), Value::Int64(11), Value::Int64(12)],
                ),
                (
                    "rpm".to_string(),
                    vec![Value::Int64(1000), Value::Int64(1000), Value::Int64(2000)],
                ),
            ],
            source_input: SourceInputConfig::on_change_with_columns(["rpm"]),
            output: SinkOutputConfig::default(),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 11, "rpm": 1000}),
                serde_json::json!({"speed": 12, "rpm": 2000}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                None,
                Some(serde_json::json!([
                    {"speed": 12}
                ])),
            ],
        },
        SourceOnChangeJsonCase {
            name: "on_change_without_columns_tracks_all_top_level_columns",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            covers: &["source.on_change.gating"],
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
                ),
                (
                    "rpm".to_string(),
                    vec![Value::Int64(1000), Value::Int64(1000), Value::Int64(1000)],
                ),
                (
                    "gear".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(2)],
                ),
            ],
            source_input: SourceInputConfig::on_change(),
            output: SinkOutputConfig::default(),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000, "gear": 1}),
                serde_json::json!({"speed": 10, "rpm": 1000, "gear": 2}),
                serde_json::json!({"speed": 10, "rpm": 1000, "gear": 2}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                None,
            ],
        },
        SourceOnChangeJsonCase {
            name: "state_is_preserved_across_batches",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            covers: &["source.on_change.gating"],
            schema_hint: vec![("speed".to_string(), vec![Value::Int64(1), Value::Int64(2)])],
            source_input: SourceInputConfig::on_change_with_columns(["speed"]),
            output: SinkOutputConfig::default(),
            input_rows: vec![
                serde_json::json!({"speed": 1}),
                serde_json::json!({"speed": 1}),
                serde_json::json!({"speed": 2}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 1}
                ])),
                None,
                Some(serde_json::json!([
                    {"speed": 2}
                ])),
            ],
        },
        SourceOnChangeJsonCase {
            name: "hidden_tracked_column_with_delta_omit_if_empty_suppresses_redundant_rows",
            source_name: "stream",
            sql: "SELECT speed FROM stream",
            covers: &[
                "planner.logical.top_level_column_pruning",
                "source.on_change.gating",
                "sink.output.row_diff",
                "sink.output.omit_if_empty",
            ],
            schema_hint: vec![
                (
                    "speed".to_string(),
                    vec![
                        Value::Int64(10),
                        Value::Int64(11),
                        Value::Int64(10),
                        Value::Int64(12),
                    ],
                ),
                (
                    "rpm".to_string(),
                    vec![
                        Value::Int64(1000),
                        Value::Int64(1000),
                        Value::Int64(2000),
                        Value::Int64(3000),
                    ],
                ),
            ],
            source_input: SourceInputConfig::on_change_with_columns(["rpm"]),
            output: SinkOutputConfig::delta_with_columns(["speed"]).with_omit_if_empty(true),
            input_rows: vec![
                serde_json::json!({"speed": 10, "rpm": 1000}),
                serde_json::json!({"speed": 11, "rpm": 1000}),
                serde_json::json!({"speed": 10, "rpm": 2000}),
                serde_json::json!({"speed": 12, "rpm": 3000}),
            ],
            expected_outputs: vec![
                Some(serde_json::json!([
                    {"speed": 10}
                ])),
                None,
                None,
                Some(serde_json::json!([
                    {"speed": 12}
                ])),
            ],
        },
    ];

    for case in cases {
        run_source_on_change_json_case(case).await;
    }
}

// coverage-covers: source.memory.bytes_input
#[tokio::test]
async fn memory_source_bytes_topic_with_json_decoder_emits_expected_rows() {
    let case_name = "memory_source_bytes_topic_with_json_decoder_emits_expected_rows";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_memory_source_bytes", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
    install_memory_json_stream_schema_with_name(
        &instance,
        &input_topic,
        "stream",
        &[
            ("a".to_string(), vec![Value::Int64(1), Value::Int64(2)]),
            ("b".to_string(), vec![Value::Int64(10), Value::Int64(20)]),
        ],
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a, b FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");
    publisher
        .publish_bytes(
            serde_json::to_vec(&serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2, "b": 20}
            ]))
            .expect("encode bytes source input rows"),
        )
        .expect("publish bytes source input rows");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {"a": 1, "b": 10},
        {"a": 2, "b": 20}
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong JSON output for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for {}: {err}", case_name));
}

// coverage-covers: stream.datatype.bytes, source.memory.bytes_input, sink.connector.memory_output
#[tokio::test]
async fn memory_source_json_bytes_decodes_and_encodes_base64() {
    let case_name = "memory_source_json_bytes_decodes_and_encodes_base64";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_bytes_datatype", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");

    let schema = Schema::new(vec![ColumnSchema::new(
        "stream_bytes".to_string(),
        "payload".to_string(),
        ConcreteDatatype::Bytes(BytesType),
    )]);
    let stream = StreamDefinition::new(
        "stream_bytes".to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::new("json", JsonMap::new()),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create bytes stream");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT payload FROM stream_bytes",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");
    publisher
        .publish_bytes(br#"{"payload":"aGVsbG8="}"#.as_ref())
        .expect("publish base64 bytes source row");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {"payload": "aGVsbG8="}
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong JSON output for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for {}: {err}", case_name));
}

// coverage-covers: stream.datatype.timestamp, source.memory.bytes_input, sink.connector.memory_output
#[tokio::test]
async fn memory_source_json_timestamp_decodes_filters_and_encodes_utc() {
    let case_name = "memory_source_json_timestamp_decodes_filters_and_encodes_utc";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_timestamp", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");

    let matching_ts =
        TimestampValue::parse_rfc3339("2026-05-08T10:20:30Z").expect("valid timestamp");
    install_memory_json_stream_schema_with_name(
        &instance,
        &input_topic,
        "stream_ts",
        &[
            (
                "event_time".to_string(),
                vec![Value::Timestamp(matching_ts)],
            ),
            (
                "expected_time".to_string(),
                vec![Value::Timestamp(matching_ts)],
            ),
            ("name".to_string(), vec![Value::String("match".to_string())]),
        ],
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT event_time, name FROM stream_ts WHERE event_time = expected_time",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");
    publisher
        .publish_bytes(
            serde_json::to_vec(&serde_json::json!([
                {
                    "event_time": "2026-05-08T18:20:30+08:00",
                    "expected_time": "2026-05-08T10:20:30Z",
                    "name": "match"
                },
                {
                    "event_time": "2026-05-08T10:20:31Z",
                    "expected_time": "2026-05-08T10:20:30Z",
                    "name": "skip"
                },
                {
                    "event_time": "bad-timestamp",
                    "expected_time": "2026-05-08T10:20:30Z",
                    "name": "bad"
                }
            ]))
            .expect("encode timestamp source input rows"),
        )
        .expect("publish timestamp source input rows");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {
            "event_time": "2026-05-08T10:20:30.000000Z",
            "name": "match"
        }
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong timestamp JSON output for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for {}: {err}", case_name));
}

// coverage-covers: stream.datatype.timestamp, stream.function.date_time_scalars, source.memory.bytes_input, sink.connector.memory_output
#[tokio::test]
async fn memory_source_json_timestamp_feeds_date_time_scalars() {
    let case_name = "memory_source_json_timestamp_feeds_date_time_scalars";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_timestamp", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");

    let schema_ts = TimestampValue::parse_rfc3339("2026-05-08T10:20:30Z").expect("valid timestamp");
    install_memory_json_stream_schema_with_name(
        &instance,
        &input_topic,
        "stream_ts",
        &[("event_time".to_string(), vec![Value::Timestamp(schema_ts)])],
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT format_time(event_time, '%Y-%m-%d %H:%M:%S') AS formatted, day_of_month(event_time) AS day, month_name(event_time) AS month_name, second(event_time) AS second FROM stream_ts WHERE day(event_time) = 8",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");
    publisher
        .publish_bytes(
            serde_json::to_vec(&serde_json::json!([
                {"event_time": "2026-05-08T18:20:30+08:00"},
                {"event_time": "2026-05-09T00:00:01Z"}
            ]))
            .expect("encode timestamp date-time source rows"),
        )
        .expect("publish timestamp date-time source rows");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {
            "formatted": "2026-05-08 10:20:30",
            "day": 8,
            "month_name": "May",
            "second": 30
        }
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong timestamp date-time JSON output for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|err| panic!("Failed to stop pipeline for {}: {err}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for {}: {err}", case_name));
}

// coverage-covers: source.memory.bytes_input, sink.output.batching
#[tokio::test]
async fn memory_source_bytes_topic_batches_json_output() {
    let case_name = "memory_source_bytes_topic_batches_json_output";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_memory_source_bytes", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
    install_memory_json_stream_schema_with_name(
        &instance,
        &input_topic,
        "stream",
        &[
            ("a".to_string(), vec![Value::Int64(1), Value::Int64(2)]),
            ("b".to_string(), vec![Value::Int64(10), Value::Int64(20)]),
        ],
    )
    .await;

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
        batch_count: Some(2),
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

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");

    publisher
        .publish_bytes(serde_json::to_vec(&serde_json::json!({"a": 1, "b": 10})).unwrap())
        .expect("publish first bytes source row");
    assert_no_json_output(&mut output, Duration::from_millis(300)).await;

    publisher
        .publish_bytes(serde_json::to_vec(&serde_json::json!({"a": 2, "b": 20})).unwrap())
        .expect("publish second bytes source row");
    let actual = recv_next_json(&mut output, timeout_duration).await;
    assert_eq!(actual, serde_json::json!([{"a": 1}, {"a": 2}]));

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// coverage-covers: source.memory.collection_input
#[tokio::test]
async fn memory_source_collection_topic_with_non_none_decoder_is_rejected() {
    let case_name = "memory_source_collection_topic_with_non_none_decoder_is_rejected";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, _output_topic) =
        make_memory_topics("pipeline_memory_source_collection_decoder", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input collection topic");

    let schema = Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);
    let definition = StreamDefinition::new(
        "stream".to_string(),
        Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    let err = match instance.create_stream(definition, false).await {
        Ok(_) => panic!("collection topic with non-none decoder should fail create_stream"),
        Err(err) => err,
    };
    let err_message = err.to_string();
    assert!(
        err_message.contains("kind mismatch"),
        "unexpected error for test {}: {}",
        case_name,
        err_message
    );
    assert!(
        err_message.contains("expected bytes, got collection"),
        "unexpected error details for test {}: {}",
        case_name,
        err_message
    );
}

// coverage-covers: source.memory.collection_input, source.memory.layout_normalize, sink.memory_collection.materialize, sink.connector.memory_output, parser.select.alias_computing
#[tokio::test]
async fn memory_collection_source_layout_normalize_then_collection_sink_materialize_aliases() {
    let case_name =
        "memory_collection_source_layout_normalize_then_collection_sink_materialize_aliases";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_memory_collection", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input collection topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output collection topic");
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        "stream",
        &[
            ("a".to_string(), vec![Value::Int64(1), Value::Null]),
            ("b".to_string(), vec![Value::Int64(10), Value::Int64(20)]),
        ],
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_collection(&output_topic)
        .expect("subscribe output collection");

    let pipeline_id = format!("pipe_{}", output_topic);
    let sink = SinkDefinition::new(
        "mem_sink",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()));
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT b AS second, a + 1 AS plus_one, a AS first FROM stream",
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let topic_source: Arc<str> = Arc::<str>::from(input_topic.as_str());
    let rows = vec![
        Tuple::new(vec![Arc::new(Message::new_shared_keys(
            Arc::clone(&topic_source),
            Arc::from(vec![Arc::<str>::from("b"), Arc::<str>::from("a")]),
            vec![Arc::new(Value::Int64(10)), Arc::new(Value::Int64(1))],
        ))]),
        Tuple::new(vec![Arc::new(Message::new_shared_keys(
            Arc::clone(&topic_source),
            Arc::from(vec![Arc::<str>::from("b"), Arc::<str>::from("a")]),
            vec![Arc::new(Value::Int64(20)), Arc::new(Value::Null)],
        ))]),
    ];
    let batch = RecordBatch::new(rows).expect("create input record batch");
    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let collection = recv_next_collection(&mut output, timeout_duration).await;
    assert_eq!(collection.num_rows(), 2);

    let expected = [
        vec![
            ("second", Value::Int64(10)),
            ("plus_one", Value::Int64(2)),
            ("first", Value::Int64(1)),
        ],
        vec![
            ("second", Value::Int64(20)),
            ("plus_one", Value::Null),
            ("first", Value::Null),
        ],
    ];

    for (row_idx, tuple) in collection.rows().iter().enumerate() {
        assert!(tuple.affiliate().is_none(), "affiliate should be empty");
        assert_eq!(tuple.messages().len(), 1, "expected a single message");
        let msg = &tuple.messages()[0];
        assert_eq!(msg.source(), "");
        let entries: Vec<(&str, Value)> = msg.entries().map(|(k, v)| (k, v.clone())).collect();
        assert_eq!(entries, expected[row_idx], "collection row mismatch");
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn pipeline_list_element_pruning_sparse_index_json_runtime() {
    let case_name = "pipeline_list_element_pruning_sparse_index_json_runtime";
    let sql =
        "SELECT a, stream_4.b->items[0]->x AS x0, stream_4.b->items[3]->x AS x3 FROM stream_4";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_json_runtime", case_name);
    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");
    install_memory_json_stream_4_schema(&instance, &input_topic).await;

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case_name));

    let input_rows = serde_json::json!([
        {
            "a": 7,
            "b": {
                "items": [
                    {"x": 10},
                    {"x": 20},
                    {"x": 30},
                    {"x": 40}
                ]
            }
        },
        {
            "a": 8,
            "b": {
                "items": [
                    {"x": 11},
                    {"x": 21},
                    {"x": 31},
                    {"x": 41}
                ]
            }
        }
    ]);

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for memory source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");
    publisher
        .publish_bytes(
            serde_json::to_vec(&input_rows).expect("encode sparse list pruning input rows"),
        )
        .expect("publish sparse list pruning input rows");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {"a": 7, "x0": 10, "x3": 40},
        {"a": 8, "x0": 11, "x3": 41}
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "Wrong JSON output for test: {}",
        case_name
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .unwrap_or_else(|_| panic!("Failed to stop pipeline for test: {}", case_name));
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case_name));
}

struct MixedConsumersJsonCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    delta_columns: &'static [&'static str],
    expected_regular: JsonValue,
    expected_delta: JsonValue,
}

async fn run_mixed_consumers_json_case(case: MixedConsumersJsonCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, regular_output_topic) =
        make_memory_topics("pipeline_mixed_consumers_json_regular", case.name);
    let (_, delta_output_topic) =
        make_memory_topics("pipeline_mixed_consumers_json_delta", case.name);
    declare_memory_input_output_topics(&instance, &input_topic, &regular_output_topic);
    instance
        .declare_memory_topic(
            &delta_output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare delta output memory topic");
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.input_data,
    )
    .await;

    let mut regular_output = instance
        .open_memory_subscribe_bytes(&regular_output_topic)
        .expect("subscribe regular output bytes");
    let mut delta_output = instance
        .open_memory_subscribe_bytes(&delta_output_topic)
        .expect("subscribe delta output bytes");

    let pipeline_id = format!("pipe_{}", regular_output_topic);
    let regular_sink = SinkDefinition::new(
        "mem_sink_regular",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(regular_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::json());
    let delta_sink = SinkDefinition::new(
        "mem_sink_delta",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(delta_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::json())
    .with_output(SinkOutputConfig::delta_with_columns(
        case.delta_columns.iter().copied(),
    ));
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![regular_sink, delta_sink],
    );
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
        .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual_regular = recv_next_json(&mut regular_output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual_regular),
        normalize_json(case.expected_regular),
        "Wrong regular JSON for test: {}",
        case.name
    );

    let actual_delta = recv_next_json(&mut delta_output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual_delta),
        normalize_json(case.expected_delta),
        "Wrong delta JSON for test: {}",
        case.name
    );

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

// coverage-covers: planner.physical.by_index_projection_across_mixed_consumers_rewrite, planner.physical.by_index_projection_into_encoder_rewrite, planner.physical.by_index_projection_into_row_diff_rewrite, sink.connector.memory_output, sink.output.row_diff, parser.select.alias_computing
#[tokio::test]
async fn pipeline_mixed_consumers_json_table_driven() {
    let cases = vec![
        MixedConsumersJsonCase {
            name: "shared_project_with_encoder_branch_and_row_diff_branch",
            source_name: "stream_ab",
            sql: "SELECT a, b FROM stream_ab",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(10), Value::Int64(11)],
                ),
            ],
            delta_columns: &["b"],
            expected_regular: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2, "b": 10},
                {"a": 3, "b": 11}
            ]),
            expected_delta: serde_json::json!([
                {"a": 1, "b": 10},
                {"a": 2},
                {"a": 3, "b": 11}
            ]),
        },
        MixedConsumersJsonCase {
            name: "shared_project_with_encoder_branch_and_row_diff_branch_preserves_aliases",
            source_name: "stream_ab",
            sql: "SELECT a AS x, b + 1 AS y FROM stream_ab",
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(1), Value::Int64(1), Value::Int64(2)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(10), Value::Int64(11), Value::Int64(11)],
                ),
            ],
            delta_columns: &["y"],
            expected_regular: serde_json::json!([
                {"x": 1, "y": 11},
                {"x": 1, "y": 12},
                {"x": 2, "y": 12}
            ]),
            expected_delta: serde_json::json!([
                {"x": 1, "y": 11},
                {"x": 1, "y": 12},
                {"x": 2}
            ]),
        },
    ];

    for case in cases {
        run_mixed_consumers_json_case(case).await;
    }
}

struct MixedOutputKindsCase {
    name: &'static str,
    source_name: &'static str,
    sql: &'static str,
    input_data: Vec<(String, Vec<Value>)>,
    expected_json: JsonValue,
    expected_rows: usize,
    expected_message_count: usize,
    expected_affiliate_none: bool,
    expected_collection_columns: Vec<ColumnCheck>,
}

async fn run_mixed_output_kinds_case(case: MixedOutputKindsCase) {
    println!("Running test: {}", case.name);

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, json_output_topic) =
        make_memory_topics("pipeline_mixed_output_kinds_json", case.name);
    let (_, collection_output_topic) =
        make_memory_topics("pipeline_mixed_output_kinds_collection", case.name);

    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input memory topic");
    instance
        .declare_memory_topic(
            &json_output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare json output memory topic");
    instance
        .declare_memory_topic(
            &collection_output_topic,
            MemoryTopicKind::Collection,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare collection output memory topic");
    install_memory_stream_schema_with_name(
        &instance,
        &input_topic,
        case.source_name,
        &case.input_data,
    )
    .await;

    let mut json_output = instance
        .open_memory_subscribe_bytes(&json_output_topic)
        .expect("subscribe json output bytes");
    let mut collection_output = instance
        .open_memory_subscribe_collection(&collection_output_topic)
        .expect("subscribe collection output");

    let pipeline_id = format!("pipe_{}", json_output_topic);
    let json_sink = SinkDefinition::new(
        "mem_sink_json",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(json_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::json());
    let collection_sink = SinkDefinition::new(
        "mem_sink_collection",
        SinkType::Memory,
        SinkProps::Memory(MemorySinkProps::new(collection_output_topic.clone())),
    )
    .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()));
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        case.sql,
        vec![json_sink, collection_sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|_| panic!("Failed to create pipeline for: {}", case.name));

    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to start pipeline for: {}", case.name));

    let columns = case
        .input_data
        .clone()
        .into_iter()
        .map(|(col_name, values)| (case.source_name.to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case.name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let actual_json = recv_next_json(&mut json_output, timeout_duration).await;
    assert_eq!(
        normalize_json(actual_json),
        normalize_json(case.expected_json),
        "Wrong JSON output for test: {}",
        case.name
    );

    let collection = recv_next_collection(&mut collection_output, timeout_duration).await;
    assert_eq!(
        collection.num_rows(),
        case.expected_rows,
        "collection row count mismatch (test: {})",
        case.name
    );
    for (row_idx, tuple) in collection.rows().iter().enumerate() {
        if case.expected_affiliate_none {
            assert!(tuple.affiliate().is_none(), "affiliate should be empty");
        }

        assert_eq!(
            tuple.messages().len(),
            case.expected_message_count,
            "collection message count mismatch (test: {})",
            case.name
        );

        let msg = &tuple.messages()[0];
        assert_eq!(
            msg.source(),
            "",
            "message source should be empty after materialization"
        );

        let entries: Vec<(&str, datatypes::Value)> =
            msg.entries().map(|(k, v)| (k, v.clone())).collect();
        let expected_keys: Vec<&str> = case
            .expected_collection_columns
            .iter()
            .map(|c| c.expected_name.as_str())
            .collect();
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            keys, expected_keys,
            "collection column order mismatch (test: {})",
            case.name
        );

        for (col_idx, check) in case.expected_collection_columns.iter().enumerate() {
            assert_eq!(
                entries[col_idx].0,
                check.expected_name.as_str(),
                "collection column name mismatch (test: {})",
                case.name
            );
            assert_eq!(
                entries[col_idx].1, check.expected_values[row_idx],
                "collection value mismatch (test: {})",
                case.name
            );
        }
    }

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|_| panic!("Failed to delete pipeline for test: {}", case.name));
}

#[tokio::test]
async fn shared_project_with_json_and_collection_sink_preserves_alias_output() {
    run_mixed_output_kinds_case(MixedOutputKindsCase {
        name: "shared_project_with_json_and_collection_sink_preserves_alias_output",
        source_name: "stream",
        sql: "SELECT a AS x FROM stream",
        input_data: vec![(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
        expected_json: serde_json::json!([
            {"x": 1},
            {"x": 2},
            {"x": 3}
        ]),
        expected_rows: 3,
        expected_message_count: 1,
        expected_affiliate_none: true,
        expected_collection_columns: vec![ColumnCheck {
            expected_name: "x".to_string(),
            expected_values: vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        }],
    })
    .await;
}

#[tokio::test]
async fn pipeline_table_driven_memory_collection_sources_layout_normalize() {
    let test_cases = vec![SourceLayoutTestCase {
        name: "layout_normalize_reorders_and_fills_missing_with_null",
        sql: "SELECT a, b FROM stream",
        covers: &[
            "source.memory.collection_input",
            "source.memory.layout_normalize",
        ],
        stream_schema_hint: vec![
            ("a".to_string(), vec![Value::Int64(1)]),
            ("b".to_string(), vec![Value::Int64(2)]),
        ],
        published_rows: vec![
            // Wrong message source + swapped order: [b, a].
            PublishedRowSpec {
                keys_order: vec!["b", "a"],
                values: vec![Value::Int64(2), Value::Int64(1)],
            },
            // Only `a` exists -> `b` should be filled as NULL.
            PublishedRowSpec {
                keys_order: vec!["a"],
                values: vec![Value::Int64(100)],
            },
        ],
        expected_rows: 2,
        expected_columns: 2,
        column_checks: vec![
            ColumnCheck {
                expected_name: "a".to_string(),
                expected_values: vec![Value::Int64(1), Value::Int64(100)],
            },
            ColumnCheck {
                expected_name: "b".to_string(),
                expected_values: vec![Value::Int64(2), Value::Null],
            },
        ],
    }];

    for test_case in test_cases {
        run_source_layout_test_case(test_case).await;
    }
}

// coverage-covers: source.memory.collection_input, sink.memory_collection.materialize, sink.connector.memory_output, parser.select.alias_computing
#[tokio::test]
async fn pipeline_table_driven_collection_sinks() {
    let test_cases = vec![
        CollectionSinkTestCase {
            name: "select_star_with_alias_materializes_single_message",
            sql: "SELECT *, a AS x FROM stream",
            covers: &[
                "sink.memory_collection.materialize",
                "sink.connector.memory_output",
            ],
            input_data: vec![
                (
                    "a".to_string(),
                    vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                ),
                (
                    "b".to_string(),
                    vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                ),
            ],
            expected_rows: 3,
            expected_message_count: 1,
            expected_affiliate_none: true,
            expected_columns: vec![
                ColumnCheck {
                    expected_name: "a".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
                ColumnCheck {
                    expected_name: "b".to_string(),
                    expected_values: vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)],
                },
                ColumnCheck {
                    expected_name: "x".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
                },
            ],
        },
        CollectionSinkTestCase {
            name: "alias_order_and_derived_columns_materialize_from_output_schema",
            sql: "SELECT b AS second, a + 1 AS plus_one, a AS first FROM stream",
            covers: &[
                "parser.select.alias_computing",
                "sink.memory_collection.materialize",
                "sink.connector.memory_output",
            ],
            input_data: vec![
                ("a".to_string(), vec![Value::Int64(10), Value::Int64(20)]),
                ("b".to_string(), vec![Value::Int64(100), Value::Int64(200)]),
            ],
            expected_rows: 2,
            expected_message_count: 1,
            expected_affiliate_none: true,
            expected_columns: vec![
                ColumnCheck {
                    expected_name: "second".to_string(),
                    expected_values: vec![Value::Int64(100), Value::Int64(200)],
                },
                ColumnCheck {
                    expected_name: "plus_one".to_string(),
                    expected_values: vec![Value::Int64(11), Value::Int64(21)],
                },
                ColumnCheck {
                    expected_name: "first".to_string(),
                    expected_values: vec![Value::Int64(10), Value::Int64(20)],
                },
            ],
        },
    ];

    for test_case in test_cases {
        run_collection_sink_test_case(test_case).await;
    }
}

// coverage-covers: sink.connector.memory_output
#[tokio::test]
async fn memory_collection_sink_rejects_duplicate_output_column_names() {
    let case_name = "memory_collection_sink_rejects_duplicate_output_column_names";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_collection_duplicate_columns", case_name);
    declare_memory_input_output_topics_with_output_kind(
        &instance,
        &input_topic,
        &output_topic,
        MemoryTopicKind::Collection,
    );
    install_memory_stream_schema(
        &instance,
        &input_topic,
        &[
            ("a".to_string(), vec![Value::Int64(1)]),
            ("b".to_string(), vec![Value::Int64(2)]),
        ],
    )
    .await;

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id,
        "SELECT a, a FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic)),
        )
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))],
    );
    let err = instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect_err("duplicate output column names should fail for collection sink");
    let err_message = err.to_string();
    assert!(
        err_message.contains("duplicate project field name `a`"),
        "unexpected error for test {}: {}",
        case_name,
        err_message
    );
}

// coverage-covers: sink.connector.memory_output, sink.memory_collection.materialize, sink.output.row_diff
#[tokio::test]
async fn memory_collection_sink_delta_output_preserves_output_mask() {
    let case_name = "memory_collection_sink_delta_output_preserves_output_mask";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_collection_delta_output_mask", case_name);
    let input_data = vec![
        (
            "a".to_string(),
            vec![Value::Int64(1), Value::Null, Value::Null],
        ),
        (
            "b".to_string(),
            vec![Value::Int64(10), Value::Int64(10), Value::Int64(10)],
        ),
    ];
    declare_memory_input_output_topics_with_output_kind(
        &instance,
        &input_topic,
        &output_topic,
        MemoryTopicKind::Collection,
    );
    install_memory_stream_schema(&instance, &input_topic, &input_data).await;

    let mut output = instance
        .open_memory_subscribe_collection(&output_topic)
        .expect("subscribe output collection");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a, b FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))
        .with_output(SinkOutputConfig::delta())],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let columns = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns)
        .unwrap_or_else(|_| panic!("Failed to create test RecordBatch for: {}", case_name));

    let timeout_duration = Duration::from_secs(5);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let collection = recv_next_collection(&mut output, timeout_duration).await;
    assert_eq!(
        collection.num_rows(),
        3,
        "collection row count mismatch (test: {})",
        case_name
    );

    let expected_rows = [
        ([Value::Int64(1), Value::Int64(10)], [true, true]),
        ([Value::Null, Value::Null], [true, false]),
        ([Value::Null, Value::Null], [false, false]),
    ];

    for (row_idx, tuple) in collection.rows().iter().enumerate() {
        assert!(tuple.affiliate().is_none(), "affiliate should be empty");
        assert_eq!(
            tuple.messages().len(),
            1,
            "message count mismatch (test: {})",
            case_name
        );

        let msg = &tuple.messages()[0];
        assert_eq!(
            msg.source(),
            "",
            "message source should be empty after materialization"
        );

        let entries: Vec<(&str, datatypes::Value)> =
            msg.entries().map(|(k, v)| (k, v.clone())).collect();
        let keys: Vec<&str> = entries.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            keys,
            vec!["a", "b"],
            "collection column order mismatch (test: {})",
            case_name
        );

        let (expected_values, expected_mask) = &expected_rows[row_idx];
        let actual_values: Vec<datatypes::Value> =
            entries.into_iter().map(|(_, value)| value).collect();
        assert_eq!(
            actual_values.as_slice(),
            expected_values,
            "collection values mismatch (test: {})",
            case_name
        );

        assert_eq!(
            tuple.output_mask(),
            Some(expected_mask.as_slice()),
            "output mask mismatch (test: {})",
            case_name
        );
    }

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}

// coverage-covers: sink.connector.memory_output, sink.memory_collection.materialize, sink.output.row_diff, sink.output.omit_if_empty
#[tokio::test]
async fn memory_collection_sink_delta_omit_if_empty_suppresses_unchanged_collection() {
    let case_name = "memory_collection_sink_delta_omit_if_empty_suppresses_unchanged_collection";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) =
        make_memory_topics("pipeline_collection_delta_omit_if_empty", case_name);
    declare_memory_input_output_topics_with_output_kind(
        &instance,
        &input_topic,
        &output_topic,
        MemoryTopicKind::Collection,
    );
    install_memory_stream_schema(
        &instance,
        &input_topic,
        &[
            ("a".to_string(), vec![Value::Int64(1)]),
            ("b".to_string(), vec![Value::Int64(10)]),
        ],
    )
    .await;

    let mut output = instance
        .open_memory_subscribe_collection(&output_topic)
        .expect("subscribe output collection");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a, b FROM stream",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))
        .with_output(SinkOutputConfig::delta().with_omit_if_empty(true))],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);

    let first_batch = batch_from_columns_simple(vec![
        ("stream".to_string(), "a".to_string(), vec![Value::Int64(1)]),
        (
            "stream".to_string(),
            "b".to_string(),
            vec![Value::Int64(10)],
        ),
    ])
    .unwrap_or_else(|_| panic!("Failed to create first batch for: {}", case_name));
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(first_batch),
        timeout_duration,
    )
    .await;

    let first_collection = recv_next_collection(&mut output, timeout_duration).await;
    assert_eq!(
        first_collection.num_rows(),
        1,
        "first collection row count mismatch (test: {})",
        case_name
    );
    assert_eq!(
        first_collection.rows()[0].output_mask(),
        Some([true, true].as_slice()),
        "first collection output mask mismatch (test: {})",
        case_name
    );

    let second_batch = batch_from_columns_simple(vec![
        ("stream".to_string(), "a".to_string(), vec![Value::Int64(1)]),
        (
            "stream".to_string(),
            "b".to_string(),
            vec![Value::Int64(10)],
        ),
    ])
    .unwrap_or_else(|_| panic!("Failed to create second batch for: {}", case_name));
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(second_batch),
        timeout_duration,
    )
    .await;

    assert!(
        timeout(Duration::from_millis(300), output.recv())
            .await
            .is_err(),
        "unchanged diff collection should be suppressed for test: {}",
        case_name
    );

    let third_batch = batch_from_columns_simple(vec![
        ("stream".to_string(), "a".to_string(), vec![Value::Int64(2)]),
        (
            "stream".to_string(),
            "b".to_string(),
            vec![Value::Int64(11)],
        ),
    ])
    .unwrap_or_else(|_| panic!("Failed to create third batch for: {}", case_name));
    publish_input_collection(
        &instance,
        &input_topic,
        Box::new(third_batch),
        timeout_duration,
    )
    .await;

    let third_collection = recv_next_collection(&mut output, timeout_duration).await;
    assert_eq!(
        third_collection.num_rows(),
        1,
        "third collection row count mismatch (test: {})",
        case_name
    );
    let third_tuple = &third_collection.rows()[0];
    assert_eq!(
        third_tuple.output_mask(),
        Some([true, true].as_slice()),
        "third collection output mask mismatch (test: {})",
        case_name
    );
    let third_entries: Vec<(&str, datatypes::Value)> = third_tuple.messages()[0]
        .entries()
        .map(|(k, v)| (k, v.clone()))
        .collect();
    assert_eq!(
        third_entries,
        vec![("a", Value::Int64(2)), ("b", Value::Int64(11)),],
        "third collection values mismatch (test: {})",
        case_name
    );

    let _ = instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await;
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to delete pipeline for test {}: {err}", case_name));
}

// ── UDF pipeline test ──────────────────────────────────────────────

/// A test-only UDF that doubles its integer argument.
#[derive(Debug, Clone)]
struct DoubleUdf;

impl flow::CustomFunc for DoubleUdf {
    fn validate_row(&self, args: &[Value]) -> Result<(), flow::expr::func::EvalError> {
        if args.len() != 1 {
            return Err(flow::expr::func::EvalError::TypeMismatch {
                expected: "1 argument".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, flow::expr::func::EvalError> {
        match &args[0] {
            Value::Int64(v) => Ok(Value::Int64(v * 2)),
            other => Err(flow::expr::func::EvalError::TypeMismatch {
                expected: "int64".to_string(),
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "double"
    }
}

// coverage-covers: stream.function.custom_udf, sink.connector.memory_output
#[tokio::test]
async fn udf() {
    let test_name = "udf";
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");

    // Register the mock UDF
    let registry = instance.custom_func_registry();
    let new_registry = registry
        .clone_and_add(Arc::new(DoubleUdf))
        .expect("register double UDF");
    instance.set_custom_func_registry(new_registry);

    let (input_topic, output_topic) =
        make_memory_topics("pipeline_table_driven_queries", test_name);
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
    let sql = "SELECT double(a) AS doubled FROM stream";
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        sql,
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );

    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    let columns: Vec<(String, String, Vec<Value>)> = input_data
        .into_iter()
        .map(|(col_name, values)| ("stream".to_string(), col_name, values))
        .collect();
    let batch = batch_from_columns_simple(columns).expect("build input batch");
    let timeout_duration = Duration::from_secs(10);
    publish_input_collection(&instance, &input_topic, Box::new(batch), timeout_duration).await;

    let first_output = recv_next_json(&mut output, timeout_duration).await;
    println!("first_output: {first_output:?}");
    let rows = first_output
        .as_array()
        .expect("output should be a JSON array");
    assert_eq!(rows.len(), 3, "should have 3 rows");
    assert_eq!(
        rows[0]["doubled"].as_i64(),
        Some(2),
        "1 doubled should be 2"
    );
    assert_eq!(
        rows[1]["doubled"].as_i64(),
        Some(4),
        "2 doubled should be 4"
    );
    assert_eq!(
        rows[2]["doubled"].as_i64(),
        Some(6),
        "3 doubled should be 6"
    );

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

// ── protobuf decoder tests ─────────────────────────────────────────────

#[tokio::test]
async fn memory_source_bytes_topic_with_protobuf_decoder_decodes_expected_rows() {
    use flow::codec::{ProtoDescriptorBundle, ProtoFieldInfo};
    use std::collections::BTreeMap;

    let case_name = "memory_source_bytes_topic_with_protobuf_decoder_decodes_expected_rows";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_proto_decode", case_name);

    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");

    // Build a simple schema and ProtoDescriptorBundle for:
    //   message Simple { int32 a = 1; int32 b = 2; }
    let stream_name = "stream_proto";
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));

    let mut field_map = BTreeMap::new();
    field_map.insert(
        1u32,
        ProtoFieldInfo {
            column_index: 0,
            datatype: ConcreteDatatype::Int64(Int64Type),
            is_repeated: false,
            is_zigzag: false,
            nested_bundle: None,
        },
    );
    field_map.insert(
        2u32,
        ProtoFieldInfo {
            column_index: 1,
            datatype: ConcreteDatatype::Int64(Int64Type),
            is_repeated: false,
            is_zigzag: false,
            nested_bundle: None,
        },
    );
    let mut column_to_field = BTreeMap::new();
    column_to_field.insert("a".to_string(), 1);
    column_to_field.insert("b".to_string(), 2);
    let bundle = Arc::new(ProtoDescriptorBundle::new(
        field_map,
        column_to_field,
        2,
        vec![Arc::from("a"), Arc::from("b")],
    ));

    let decoder_config =
        StreamDecoderConfig::new("protobuf", serde_json::Map::new()).with_proto_bundle(bundle);

    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::clone(&schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        decoder_config,
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a, b FROM stream_proto",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");

    // Encode protobuf message using the generated `Simple` type.
    let proto_payload =
        flow::test_proto::encode_simple(&flow::test_proto::simple::Simple { a: 10, b: 20 });
    publisher
        .publish_bytes(proto_payload)
        .expect("publish protobuf bytes");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    let expected = serde_json::json!([
        {"a": 10, "b": 20}
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "protobuf decoder should decode a=10, b=20"
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            flow::pipeline::PipelineStopMode::Quick,
            timeout_duration,
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}

#[tokio::test]
async fn memory_source_bytes_topic_with_protobuf_decoder_respects_column_pruning() {
    use flow::codec::{ProtoDescriptorBundle, ProtoFieldInfo};
    use std::collections::BTreeMap;

    let case_name = "memory_source_bytes_topic_with_protobuf_decoder_respects_column_pruning";

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let (input_topic, output_topic) = make_memory_topics("pipeline_proto_prune", case_name);

    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input bytes topic");
    instance
        .declare_memory_topic(
            &output_topic,
            MemoryTopicKind::Bytes,
            flow::connector::DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output bytes topic");

    let stream_name = "stream_prune";
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));

    let mut field_map = BTreeMap::new();
    field_map.insert(
        1u32,
        ProtoFieldInfo {
            column_index: 0,
            datatype: ConcreteDatatype::Int64(Int64Type),
            is_repeated: false,
            is_zigzag: false,
            nested_bundle: None,
        },
    );
    field_map.insert(
        2u32,
        ProtoFieldInfo {
            column_index: 1,
            datatype: ConcreteDatatype::Int64(Int64Type),
            is_repeated: false,
            is_zigzag: false,
            nested_bundle: None,
        },
    );
    let mut column_to_field = BTreeMap::new();
    column_to_field.insert("a".to_string(), 1);
    column_to_field.insert("b".to_string(), 2);
    let bundle = Arc::new(ProtoDescriptorBundle::new(
        field_map,
        column_to_field,
        2,
        vec![Arc::from("a"), Arc::from("b")],
    ));

    let decoder_config =
        StreamDecoderConfig::new("protobuf", serde_json::Map::new()).with_proto_bundle(bundle);

    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::clone(&schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        decoder_config,
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");

    let mut output = instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("subscribe output bytes");

    // Select only column 'a' — 'b' should be skipped via column pruning.
    let pipeline_id = format!("pipe_{}", output_topic);
    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        "SELECT a FROM stream_prune",
        vec![SinkDefinition::new(
            "mem_sink",
            SinkType::Memory,
            SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .unwrap_or_else(|err| panic!("Failed to create pipeline for {}: {err}", case_name));
    instance
        .start_pipeline(&pipeline_id)
        .await
        .unwrap_or_else(|err| panic!("Failed to start pipeline for {}: {err}", case_name));

    let timeout_duration = Duration::from_secs(5);
    instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
        .await
        .expect("wait for bytes source subscriber");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input bytes publisher");

    // Encode protobuf message using the generated `Simple` type.
    let proto_payload =
        flow::test_proto::encode_simple(&flow::test_proto::simple::Simple { a: 10, b: 20 });
    publisher
        .publish_bytes(proto_payload)
        .expect("publish protobuf bytes");

    let actual = recv_next_json(&mut output, timeout_duration).await;
    // Only column 'a' should appear; 'b' is pruned.
    let expected = serde_json::json!([
        {"a": 10}
    ]);
    assert_eq!(
        normalize_json(actual),
        normalize_json(expected),
        "column pruning should skip b"
    );

    instance
        .stop_pipeline(
            &pipeline_id,
            flow::pipeline::PipelineStopMode::Quick,
            timeout_duration,
        )
        .await
        .expect("stop pipeline");
    instance
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
}
