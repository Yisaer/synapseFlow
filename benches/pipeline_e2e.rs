use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use datatypes::{ColumnSchema, ConcreteDatatype, Float64Type, Int64Type, Schema};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{MemoryData, MemoryPublisher, MemoryTopicKind};
use flow::pipeline::MemorySinkProps;
use flow::planner::sink::CommonSinkProps;
use flow::{CreatePipelineRequest, PipelineDefinition, SinkDefinition, SinkProps, SinkType};
use flow::{FlowInstance, FlowInstanceOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MESSAGE_COUNT: usize = 2000;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const WARM_UP_TIME: Duration = Duration::from_secs(5);
const MEASUREMENT_TIME: Duration = Duration::from_secs(15);

static TOPIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn sanitize_topic_fragment(fragment: &str) -> String {
    let mut out = String::with_capacity(fragment.len());
    for ch in fragment.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    let out = out.trim_matches('_');
    if out.is_empty() {
        "case".to_string()
    } else {
        out.to_string()
    }
}

fn make_memory_topics(case_name: &str) -> (String, String) {
    let counter = TOPIC_COUNTER.fetch_add(1, Ordering::Relaxed);
    let case_name = sanitize_topic_fragment(case_name);
    let base = format!("bench.pipeline_e2e.{case_name}.{counter}");
    (format!("{base}.input"), format!("{base}.output"))
}

struct PipelineBenchEnv {
    instance: FlowInstance,
    pipeline_id: String,
    input_publisher: MemoryPublisher,
    output_subscriber: tokio::sync::broadcast::Receiver<MemoryData>,
    input_messages: Vec<Bytes>,
    expected_outputs: usize,
    runtime: tokio::runtime::Runtime,
}

impl PipelineBenchEnv {
    fn build(case_name: &str, sql: &str, schema: Schema, messages: Vec<Bytes>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime");

        let expected_outputs = messages.len();
        let capacity = (expected_outputs * 8).max(1024);
        let (instance, pipeline_id, input_publisher, output_subscriber) =
            runtime.block_on(async move {
                let (input_topic, output_topic) = make_memory_topics(case_name);
                let instance = FlowInstance::new(FlowInstanceOptions::shared_current_runtime(
                    format!("bench_instance_{output_topic}"),
                    None,
                ))
                .expect("create bench flow instance");

                instance
                    .declare_memory_topic(&input_topic, MemoryTopicKind::Bytes, capacity)
                    .expect("declare input topic");
                instance
                    .declare_memory_topic(&output_topic, MemoryTopicKind::Bytes, capacity)
                    .expect("declare output topic");

                let stream_id = "source_stream".to_string();
                let definition = StreamDefinition::new(
                    stream_id.clone(),
                    Arc::new(schema),
                    StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
                    StreamDecoderConfig::json(),
                );
                instance
                    .create_stream(definition, false)
                    .await
                    .expect("create source_stream");

                let mut output_subscriber = instance
                    .open_memory_subscribe_bytes(&output_topic)
                    .expect("open output subscriber");

                while output_subscriber.try_recv().is_ok() {}

                let pipeline_id = format!("bench_pipe_{case_name}_{output_topic}");
                let sink = SinkDefinition::new(
                    "mem_sink",
                    SinkType::Memory,
                    SinkProps::Memory(MemorySinkProps::new(output_topic)),
                )
                .with_common_props(CommonSinkProps {
                    batch_count: Some(1),
                    batch_duration: None,
                });

                let pipeline = PipelineDefinition::new(pipeline_id.clone(), sql, vec![sink]);
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
                        IO_TIMEOUT,
                    )
                    .await
                    .expect("wait for memory subscribers");

                let input_publisher = instance
                    .open_memory_publisher_bytes(&input_topic)
                    .expect("open input publisher");

                (instance, pipeline_id, input_publisher, output_subscriber)
            });

        Self {
            instance,
            pipeline_id,
            input_publisher,
            output_subscriber,
            input_messages: messages,
            expected_outputs,
            runtime,
        }
    }

    fn run_iters(&mut self, iters: u64) -> Duration {
        let input_publisher = self.input_publisher.clone();
        let input_messages = self.input_messages.clone();
        let expected_outputs = self.expected_outputs;
        let output_subscriber = &mut self.output_subscriber;

        self.runtime.block_on(async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                while output_subscriber.try_recv().is_ok() {}

                let start = Instant::now();
                for message in &input_messages {
                    input_publisher
                        .publish_bytes(message.clone())
                        .expect("publish input bytes");
                }

                let mut received = 0usize;
                while received < expected_outputs {
                    let item = tokio::time::timeout(IO_TIMEOUT, output_subscriber.recv())
                        .await
                        .expect("timeout waiting for output")
                        .expect("output recv error");

                    match item {
                        MemoryData::Bytes(_) => received += 1,
                        MemoryData::Collection(_) => panic!("unexpected collection on bytes topic"),
                    }
                }

                total += start.elapsed();
            }
            total
        })
    }
}

fn build_projection_env() -> PipelineBenchEnv {
    let schema_columns = vec![
        ColumnSchema::new(
            "source_stream".to_string(),
            "col1".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "source_stream".to_string(),
            "col2".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ];
    let schema = Schema::new(schema_columns);
    let messages = (0..MESSAGE_COUNT)
        .map(|idx| Bytes::from(format!(r#"{{"col1": {idx}, "col2": {}}}"#, idx + 1).into_bytes()))
        .collect();

    PipelineBenchEnv::build(
        "projection",
        "SELECT col1, col2 FROM source_stream",
        schema,
        messages,
    )
}

fn build_filter_env() -> PipelineBenchEnv {
    let schema_columns = vec![
        ColumnSchema::new(
            "source_stream".to_string(),
            "user_id".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "source_stream".to_string(),
            "score".to_string(),
            ConcreteDatatype::Float64(Float64Type),
        ),
    ];
    let schema = Schema::new(schema_columns);
    let messages = (0..MESSAGE_COUNT)
        .map(|idx| {
            let score = (idx % 10) as f64 + 1.0;
            Bytes::from(format!(r#"{{"user_id": {idx}, "score": {score}}}"#).into_bytes())
        })
        .collect();

    PipelineBenchEnv::build(
        "filter",
        "SELECT user_id, score FROM source_stream WHERE score > 0",
        schema,
        messages,
    )
}

fn build_expr_filter_env() -> PipelineBenchEnv {
    let schema_columns = vec![
        ColumnSchema::new(
            "source_stream".to_string(),
            "user_id".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "source_stream".to_string(),
            "score".to_string(),
            ConcreteDatatype::Float64(Float64Type),
        ),
    ];
    let schema = Schema::new(schema_columns);
    let messages = (0..MESSAGE_COUNT)
        .map(|idx| {
            let score = (idx % 10) as f64 + 1.0;
            Bytes::from(format!(r#"{{"user_id": {idx}, "score": {score}}}"#).into_bytes())
        })
        .collect();

    PipelineBenchEnv::build(
        "expr_filter",
        "SELECT user_id, score*1.1 as score2 FROM source_stream WHERE score > 0",
        schema,
        messages,
    )
}

fn pipeline_e2e_benches(c: &mut Criterion) {
    c.bench_function("pipeline_e2e/projection", |b| {
        let mut env = build_projection_env();
        b.iter_custom(move |iters| env.run_iters(iters));
    });

    c.bench_function("pipeline_e2e/filter", |b| {
        let mut env = build_filter_env();
        b.iter_custom(move |iters| env.run_iters(iters));
    });

    c.bench_function("pipeline_e2e/expr_filter", |b| {
        let mut env = build_expr_filter_env();
        b.iter_custom(move |iters| env.run_iters(iters));
    });
}

criterion_group! {
    name = pipeline_e2e;
    config = Criterion::default()
        .warm_up_time(WARM_UP_TIME)
        .measurement_time(MEASUREMENT_TIME);
    targets = pipeline_e2e_benches
}
criterion_main!(pipeline_e2e);
