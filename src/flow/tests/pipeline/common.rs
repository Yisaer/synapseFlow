use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::processor::StreamData;
use flow::FlowInstance;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

pub async fn install_stream_schema(instance: &FlowInstance, columns: &[(String, Vec<Value>)]) {
    let schema_columns = columns
        .iter()
        .map(|(name, values)| {
            let datatype = values
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            ColumnSchema::new("stream".to_string(), name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        "stream".to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}

pub async fn recv_next_collection(
    output: &mut tokio::sync::mpsc::Receiver<StreamData>,
    timeout_duration: Duration,
) -> Box<dyn flow::model::Collection> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = timeout(remaining, output.recv())
            .await
            .expect("timeout waiting for pipeline output")
            .expect("pipeline output channel closed");
        match item {
            StreamData::Collection(collection) => return collection,
            StreamData::Control(_) => continue,
            StreamData::Watermark(_) => continue,
            StreamData::Error(err) => panic!("pipeline returned error: {}", err.message),
            StreamData::Encoded { .. } => panic!("unexpected stream data: Encoded"),
            StreamData::Bytes(_) => panic!("unexpected stream data: Bytes"),
        }
    }
}
