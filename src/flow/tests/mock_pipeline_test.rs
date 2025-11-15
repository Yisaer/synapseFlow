//! Integration test that feeds the pipeline using DataSource connectors instead
//! of the ControlSource input channel.

use std::sync::Arc;

use datatypes::Value;
use flow::codec::JsonDecoder;
use flow::connector::MockSourceConnector;
use flow::create_pipeline_with_log_sink;
use flow::model::RecordBatch;
use flow::processor::processor_builder::PlanProcessor;
use flow::processor::StreamData;
use flow::Processor;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_mock_pipeline_with_datasource_connector() {
    let mut pipeline = create_pipeline_with_log_sink(
        "SELECT a + 1 AS a_plus_1, b + 2 AS b_plus_2 FROM stream",
        true,
    )
    .expect("pipeline creation failed");

    let mut mock_handle = None;

    for processor in pipeline.middle_processors.iter_mut() {
        if let PlanProcessor::DataSource(ds) = processor {
            let (connector, handle) = MockSourceConnector::new(ds.id().to_string());
            let decoder = Arc::new(JsonDecoder::new(""));
            ds.add_connector(Box::new(connector), decoder);
            mock_handle = Some(handle);
        }
    }

    let mock_handle = mock_handle.expect("no datasource processor found");

    pipeline.start();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let payload = br#"[{"a":10,"b":100},{"a":20,"b":200},{"a":30,"b":300}]"#.to_vec();
    mock_handle
        .send(payload)
        .await
        .expect("failed to send mock payload");

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    let result = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timed out waiting for output")
        .expect("pipeline output closed unexpectedly");

    match result {
        StreamData::Collection(collection) => {
            let batch = RecordBatch::from_rows(collection.rows().to_vec());
            assert_eq!(batch.num_rows(), 3);
            let columns = batch.columns();
            assert_eq!(columns.len(), 2);
            assert_eq!(
                columns[0].values(),
                &[Value::Int64(11), Value::Int64(21), Value::Int64(31)]
            );
            assert_eq!(
                columns[1].values(),
                &[Value::Int64(102), Value::Int64(202), Value::Int64(302)]
            );
        }
        StreamData::Control(sig) => {
            panic!("expected collection, received control signal: {:?}", sig);
        }
        StreamData::Error(err) => {
            panic!("expected collection, received error: {:?}", err);
        }
    }

    pipeline
        .close()
        .await
        .expect("failed to close pipeline after test");
}
