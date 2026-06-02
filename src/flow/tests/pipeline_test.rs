//! Integration tests for non-table-driven pipeline behaviors.

use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::planner::sink::{
    CommonSinkProps, NopSinkConfig, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig,
};
use flow::processor::{SamplerConfig, StreamData};
use flow::FlowInstance;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

async fn create_stream_with_sampler(
    instance: &FlowInstance,
    stream_name: &str,
    sampler_interval: Duration,
) {
    let schema_columns = vec![ColumnSchema::new(
        stream_name.to_string(),
        "x".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )];
    let schema = Schema::new(schema_columns);
    let mut definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    definition = definition.with_sampler(SamplerConfig::new(sampler_interval));
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream with sampler");
}

async fn create_mock_stream(instance: &FlowInstance, stream_name: &str, columns: &[&str]) {
    let schema = Schema::new(
        columns
            .iter()
            .map(|column| {
                ColumnSchema::new(
                    stream_name.to_string(),
                    (*column).to_string(),
                    ConcreteDatatype::Int64(Int64Type),
                )
            })
            .collect(),
    );
    let definition = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create mock stream");
}

fn build_result_sink(
    sink_id: &str,
    connector_id: &str,
    common_props: CommonSinkProps,
) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        connector_id,
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    PipelineSink::new(sink_id, connector)
        .with_forward_to_result(true)
        .with_common_props(common_props)
}

async fn recv_next_json(
    output: &mut mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::EncodedDelivery { bytes, .. } => {
                return serde_json::from_slice(&bytes).expect("decode pipeline json output")
            }
            StreamData::Control(_) | StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            other => panic!("unexpected pipeline output: {}", other.description()),
        }
    }
}

/// Test execution of the "latest" samler strategy.
/// We send 5 values within a single interval window (200ms).
/// Expected result: Only ONE output value, which is the 5th (latest) value.
#[tokio::test]
async fn test_sampler_execution_latest_strategy() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    let interval = Duration::from_millis(200);
    // Create stream with 200ms sampler interval
    create_stream_with_sampler(&instance, "latest_stream", interval).await;

    let connector = PipelineSinkConnector::new(
        "test_sink",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new("output_sink", connector).with_forward_to_result(true);

    let mut pipeline = instance
        .build_pipeline("SELECT x FROM latest_stream", vec![sink])
        .expect("pipeline creation should succeed");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 5 JSON messages as raw bytes: {"x": 1}, ..., {"x": 5}
    for i in 1..=5 {
        let payload = format!(r#"{{"x": {}}}"#, i).into_bytes();
        pipeline
            .send_stream_data("latest_stream", StreamData::bytes(payload))
            .await
            .expect("send data");
        // Tiny sleep to ensure order but still well within 200ms interval
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    // We expect EXACTLY one output (the sampled value)
    let result = timeout(Duration::from_millis(500), output.recv())
        .await
        .expect("wait for sampled output")
        .expect("sampled output missing");

    // Verify content: should be value 5
    match result {
        StreamData::EncodedDelivery { bytes, .. } => {
            let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
            // Output format depends on sink, typically [{"x": 5}]
            let rows = json.as_array().expect("output is array");
            assert_eq!(rows.len(), 1, "expected 1 row in batch");
            assert_eq!(rows[0]["x"], 5, "Expected latest value 5");
        }
        other => panic!("expected EncodedDelivery, got {}", other.description()),
    }

    // Ensure no more outputs come (sampler should wait for next interval)
    let extra = timeout(Duration::from_millis(100), output.recv()).await;
    assert!(
        extra.is_err(),
        "Expected no extra outputs, sampler should have throttled intermediate values"
    );

    // Verify stats: Sampler should have 5 inputs and 1 output
    let stats = pipeline.processor_stats();
    let _ = stats
        .iter()
        .find(|s| {
            let snap = s.snapshot().stats;
            // Identify sampler by behavior: 5 in, 1 out (and ideally name)
            // Or just check if name contains "sampler"
            s.processor_id.to_lowercase().contains("sampler")
                && snap.records_in == 5
                && snap.records_out == 1
        })
        .expect("Sampler stats not found or incorrect (expected 5 in, 1 out)");

    // Attempt to close pipeline, but don't block indefinitely if it hangs (test artifact)
    let _ = timeout(
        Duration::from_millis(500),
        pipeline.close(Duration::from_secs(1)),
    )
    .await;
}

// coverage-covers: processor.barrier.alignment
#[tokio::test]
async fn shared_tail_barrier_graceful_close_flushes_batched_sibling_before_shutdown() {
    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance");
    create_mock_stream(&instance, "barrier_stream", &["x"]).await;

    let fast_sink = build_result_sink("fast_sink", "fast_conn", CommonSinkProps::default());
    let batched_sink = build_result_sink(
        "batched_sink",
        "batched_conn",
        CommonSinkProps {
            batch_count: Some(8),
            batch_duration: None,
        },
    );

    let mut pipeline = instance
        .build_pipeline(
            "SELECT x FROM barrier_stream",
            vec![fast_sink, batched_sink],
        )
        .expect("build pipeline with shared-tail barrier");
    let mut output = pipeline.take_output().expect("take output receiver");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for value in 1..=3 {
        let payload = format!(r#"{{"x": {value}}}"#).into_bytes();
        pipeline
            .send_stream_data("barrier_stream", StreamData::bytes(payload))
            .await
            .expect("send mock stream data");
    }

    let timeout_duration = Duration::from_secs(5);
    assert_eq!(
        recv_next_json(&mut output, timeout_duration).await,
        serde_json::json!([{"x": 1}]),
        "fast sibling should keep forwarding rows before graceful close"
    );
    assert_eq!(
        recv_next_json(&mut output, timeout_duration).await,
        serde_json::json!([{"x": 2}]),
        "fast sibling should preserve per-row forwarding order"
    );
    assert_eq!(
        recv_next_json(&mut output, timeout_duration).await,
        serde_json::json!([{"x": 3}]),
        "fast sibling should emit every row before barrier alignment"
    );
    assert!(
        timeout(Duration::from_millis(200), output.recv())
            .await
            .is_err(),
        "batched sibling should not flush its partial batch before graceful close"
    );

    timeout(
        Duration::from_secs(2),
        pipeline.close(Duration::from_secs(1)),
    )
    .await
    .expect("close pipeline should not hang")
    .expect("close pipeline");

    assert_eq!(
        recv_next_json(&mut output, timeout_duration).await,
        serde_json::json!([{"x": 1}, {"x": 2}, {"x": 3}]),
        "shared-tail barrier should hold the graceful end until the batched sibling flushes"
    );
}
