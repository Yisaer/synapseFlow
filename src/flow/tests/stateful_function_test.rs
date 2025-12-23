use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::model::batch_from_columns_simple;
use flow::processor::StreamData;
use flow::stateful::{StatefulFunction, StatefulFunctionInstance};
use flow::FlowInstance;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn install_stream_schema(instance: &FlowInstance, columns: &[(String, Vec<Value>)]) {
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

#[tokio::test]
async fn stateful_lag_basic() {
    let instance = FlowInstance::new();
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
    )
    .await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink("SELECT lag(a) AS prev FROM stream", true)
        .expect("create pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )])
    .expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let mut output = pipeline.take_output().expect("output receiver");
    let received = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timeout")
        .expect("missing output");
    match received {
        StreamData::Collection(collection) => {
            let rows = collection.rows();
            assert_eq!(rows.len(), 3);
            let values: Vec<Value> = rows
                .iter()
                .map(|row| row.value_by_name("", "prev").expect("prev").clone())
                .collect();
            assert_eq!(
                values,
                vec![Value::Null, Value::Int64(1), Value::Int64(2)]
            );
        }
        other => panic!("expected collection, got {}", other.description()),
    }

    pipeline.close().await.expect("close");
}

#[tokio::test]
async fn stateful_custom_function_registration() {
    struct AddOneFn;

    struct AddOneInstance;

    impl StatefulFunctionInstance for AddOneInstance {
        fn eval(&mut self, args: &[Value]) -> Result<Value, String> {
            let value = args
                .first()
                .ok_or_else(|| "add_one expects 1 argument".to_string())?;
            match value {
                Value::Null => Ok(Value::Null),
                Value::Int64(v) => Ok(Value::Int64(v + 1)),
                other => Err(format!("add_one expects Int64, got {:?}", other.datatype())),
            }
        }
    }

    impl StatefulFunction for AddOneFn {
        fn name(&self) -> &str {
            "add_one"
        }

        fn return_type(
            &self,
            input_types: &[ConcreteDatatype],
        ) -> Result<ConcreteDatatype, String> {
            match input_types.first() {
                Some(ConcreteDatatype::Int64(_)) => Ok(ConcreteDatatype::Int64(
                    datatypes::types::Int64Type,
                )),
                Some(other) => Err(format!("add_one expects Int64, got {other:?}")),
                None => Err("add_one expects 1 argument".to_string()),
            }
        }

        fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
            Box::new(AddOneInstance)
        }
    }

    let instance = FlowInstance::new();
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
    )
    .await;

    instance
        .register_stateful_function(Arc::new(AddOneFn))
        .expect("register add_one");

    let mut pipeline = instance
        .build_pipeline_with_log_sink("SELECT add_one(a) AS b FROM stream", true)
        .expect("create pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )])
    .expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let mut output = pipeline.take_output().expect("output receiver");
    let received = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timeout")
        .expect("missing output");
    match received {
        StreamData::Collection(collection) => {
            let rows = collection.rows();
            assert_eq!(rows.len(), 3);
            let values: Vec<Value> = rows
                .iter()
                .map(|row| row.value_by_name("", "b").expect("b").clone())
                .collect();
            assert_eq!(values, vec![Value::Int64(2), Value::Int64(3), Value::Int64(4)]);
        }
        other => panic!("expected collection, got {}", other.description()),
    }

    pipeline.close().await.expect("close");
}

#[tokio::test]
async fn stateful_lag_dedup_two_columns() {
    let instance = FlowInstance::new();
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
        )],
    )
    .await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink("SELECT lag(a) AS p1, lag(a) AS p2 FROM stream", true)
        .expect("create pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
    )])
    .expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let mut output = pipeline.take_output().expect("output receiver");
    let received = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timeout")
        .expect("missing output");
    match received {
        StreamData::Collection(collection) => {
            let rows = collection.rows();
            assert_eq!(rows.len(), 3);
            let p1: Vec<Value> = rows
                .iter()
                .map(|row| row.value_by_name("", "p1").expect("p1").clone())
                .collect();
            let p2: Vec<Value> = rows
                .iter()
                .map(|row| row.value_by_name("", "p2").expect("p2").clone())
                .collect();
            let expected = vec![Value::Null, Value::Int64(1), Value::Int64(2)];
            assert_eq!(p1, expected);
            assert_eq!(p2, expected);
        }
        other => panic!("expected collection, got {}", other.description()),
    }

    pipeline.close().await.expect("close");
}

#[tokio::test]
async fn stateful_lag_in_where_filter() {
    let instance = FlowInstance::new();
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
                Value::Int64(4),
            ],
        )],
    )
    .await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink("SELECT a FROM stream WHERE lag(a) > 1", true)
        .expect("create pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3),
            Value::Int64(4),
        ],
    )])
    .expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let mut output = pipeline.take_output().expect("output receiver");
    let received = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timeout")
        .expect("missing output");
    match received {
        StreamData::Collection(collection) => {
            let rows = collection.rows();
            assert_eq!(rows.len(), 2);
            let values: Vec<Value> = rows
                .iter()
                .map(|row| row.value_by_name("stream", "a").expect("a").clone())
                .collect();
            assert_eq!(values, vec![Value::Int64(3), Value::Int64(4)]);
        }
        other => panic!("expected collection, got {}", other.description()),
    }

    pipeline.close().await.expect("close");
}

#[tokio::test]
async fn stateful_nested_inside_aggregate_countwindow() {
    let instance = FlowInstance::new();
    install_stream_schema(
        &instance,
        &[(
            "a".to_string(),
            vec![
                Value::Int64(10),
                Value::Int64(20),
                Value::Int64(30),
                Value::Int64(40),
                Value::Int64(50),
                Value::Int64(60),
                Value::Int64(70),
                Value::Int64(80),
            ],
        )],
    )
    .await;

    let mut pipeline = instance
        .build_pipeline_with_log_sink(
            "SELECT sum(a), last_row(lag(a)) FROM stream GROUP BY countwindow(4)",
            true,
        )
        .expect("create pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let batch = batch_from_columns_simple(vec![(
        "stream".to_string(),
        "a".to_string(),
        vec![
            Value::Int64(10),
            Value::Int64(20),
            Value::Int64(30),
            Value::Int64(40),
            Value::Int64(50),
            Value::Int64(60),
            Value::Int64(70),
            Value::Int64(80),
        ],
    )])
    .expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    tokio::time::sleep(Duration::from_millis(200)).await;
    pipeline.close().await.expect("close");

    let mut output = pipeline.take_output().expect("output receiver");

    let first = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("first timeout")
        .expect("first missing");
    let second = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("second timeout")
        .expect("second missing");

    fn assert_window(data: StreamData, expected_sum: i64, expected_last: Value) {
        match data {
            StreamData::Collection(collection) => {
                assert_eq!(collection.num_rows(), 1);
                let row = &collection.rows()[0];
                let sum = row
                    .value_by_name("", "sum(a)")
                    .expect("sum(a)")
                    .clone();
                assert_eq!(sum, Value::Int64(expected_sum));

                let last = row
                    .value_by_name("", "last_row(lag(a))")
                    .expect("last_row(lag(a))")
                    .clone();
                assert_eq!(last, expected_last);
            }
            other => panic!("expected collection, got {}", other.description()),
        }
    }

    // Window 1: a=[10,20,30,40] => sum=100, lag=[NULL,10,20,30] => last_row=30
    assert_window(first, 100, Value::Int64(30));
    // Window 2: a=[50,60,70,80] and lag continues => lag=[40,50,60,70] => last_row=70, sum=260
    assert_window(second, 260, Value::Int64(70));
}
