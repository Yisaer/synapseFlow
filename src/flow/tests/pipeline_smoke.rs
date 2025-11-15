use datatypes::Value;
use flow::model::{Column, RecordBatch};
use flow::processor::StreamData;
use std::time::Duration;

#[tokio::test]
async fn pipeline_smoke_receives_output() {
    let sql = "SELECT a + 1 FROM stream";
    let mut pipeline = flow::create_pipeline_with_log_sink(sql, true).expect("pipeline");
    pipeline.start();

    let column = Column::new(
        "".to_string(),
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2)],
    );
    let batch = RecordBatch::new(vec![column]).expect("batch");

    pipeline
        .input
        .send(StreamData::collection(Box::new(batch)))
        .await
        .expect("send");

    let mut output = pipeline
        .take_output()
        .expect("pipeline should expose an output receiver");

    let data = tokio::time::timeout(Duration::from_secs(1), output.recv())
        .await
        .expect("timeout")
        .expect("no data");
    assert!(data.is_data(), "pipeline output should deliver data");
}
