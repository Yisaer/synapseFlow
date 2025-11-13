use flow::planner::physical::PhysicalDataSource;
use flow::processor::{
    create_processor_pipeline, create_processor_pipeline_with_sinks, ControlSignal, SinkProcessor,
    StreamData,
};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_create_processor_pipeline_with_datasource() {
    let physical_plan: Arc<dyn flow::planner::physical::PhysicalPlan> =
        Arc::new(PhysicalDataSource::new("test_source".to_string(), 0));

    let mut pipeline =
        create_processor_pipeline(physical_plan).expect("create_processor_pipeline should succeed");

    pipeline.start();

    assert_eq!(
        pipeline.sink_processors.len(),
        1,
        "default pipeline should create one sink processor"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    let control_signal = StreamData::control(ControlSignal::Resume);
    pipeline
        .input
        .send(control_signal.clone())
        .await
        .expect("send control signal");

    let received_signal = timeout(Duration::from_secs(1), pipeline.output.recv())
        .await
        .expect("receive within timeout")
        .expect("output should produce a value");

    assert!(
        received_signal.is_control(),
        "pipeline output should be control signal"
    );
    assert_eq!(
        received_signal.as_control(),
        Some(&ControlSignal::Resume),
        "should receive the same control signal"
    );

    pipeline.close().await.expect("close pipeline");
}

#[tokio::test]
async fn test_create_processor_pipeline_with_multiple_sinks() {
    let physical_plan: Arc<dyn flow::planner::physical::PhysicalPlan> =
        Arc::new(PhysicalDataSource::new("test_source_multi".to_string(), 0));

    let sinks = vec![SinkProcessor::new("sink_a"), SinkProcessor::new("sink_b")];

    let mut pipeline = create_processor_pipeline_with_sinks(physical_plan, sinks)
        .expect("pipeline with sinks should succeed");

    pipeline.start();

    assert_eq!(
        pipeline.sink_processors.len(),
        2,
        "pipeline should retain custom sink processors"
    );

    let control_signal = StreamData::control(ControlSignal::Resume);
    pipeline
        .input
        .send(control_signal.clone())
        .await
        .expect("send control signal");

    let received_signal = timeout(Duration::from_secs(1), pipeline.output.recv())
        .await
        .expect("receive within timeout")
        .expect("output should produce a value");

    assert!(
        received_signal.is_control(),
        "pipeline output should be control signal"
    );
    assert_eq!(
        received_signal.as_control(),
        Some(&ControlSignal::Resume),
        "should receive the same control signal"
    );

    pipeline.close().await.expect("close pipeline");
}
