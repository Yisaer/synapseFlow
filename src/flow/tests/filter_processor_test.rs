//! FilterProcessor integration tests
//!
//! This module tests the FilterProcessor functionality in the processor pipeline.

use datatypes::types::Int64Type;
use datatypes::{ConcreteDatatype, Value};
use flow::expr::{func::BinaryFunc, ScalarExpr};
use flow::model::{Column, RecordBatch};
use flow::planner::physical::PhysicalFilter;
use flow::processor::{FilterProcessor, Processor, StreamData};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

/// Test FilterProcessor with a simple filter expression
#[tokio::test]
async fn test_filter_processor_basic() {
    // Create test data: a=10,20,30, b=100,200,300
    let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
    let col_b_values = vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)];

    let column_a = Column::new("".to_string(), "a".to_string(), col_a_values);
    let column_b = Column::new("".to_string(), "b".to_string(), col_b_values);

    let batch = RecordBatch::new(vec![column_a, column_b]).expect("Failed to create RecordBatch");

    // Create filter expression: a > 15
    let filter_expr = ScalarExpr::CallBinary {
        func: BinaryFunc::Gt,
        expr1: Box::new(ScalarExpr::column("", "a")),
        expr2: Box::new(ScalarExpr::literal(
            Value::Int64(15),
            ConcreteDatatype::Int64(Int64Type),
        )),
    };

    // Create PhysicalFilter
    let predicate = sqlparser::ast::Expr::BinaryOp {
        left: Box::new(sqlparser::ast::Expr::Identifier(
            sqlparser::ast::Ident::new("a"),
        )),
        op: sqlparser::ast::BinaryOperator::Gt,
        right: Box::new(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
            "15".to_string(),
            false,
        ))),
    };

    let physical_filter = Arc::new(PhysicalFilter::new(predicate, filter_expr, vec![], 0));

    // Create FilterProcessor
    let mut filter_processor = FilterProcessor::new("test_filter", physical_filter);

    // Create channels
    let (input_sender, input_receiver) = broadcast::channel(10);
    let mut output_receiver = filter_processor.subscribe_output().expect("output stream");

    // Connect processor
    filter_processor.add_input(input_receiver);

    // Start processor
    let handle = filter_processor.start();

    // Send test data
    let stream_data = StreamData::collection(Box::new(batch));
    input_sender
        .send(stream_data)
        .map_err(|_| "Failed to send data")
        .expect("Failed to send data");

    // Give processor time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send end signal
    input_sender
        .send(StreamData::stream_end())
        .map_err(|_| "Failed to send end signal")
        .expect("Failed to send end signal");

    // Receive filtered data with timeout
    let filtered_data = timeout(Duration::from_secs(1), output_receiver.recv())
        .await
        .expect("Timeout waiting for filtered data")
        .expect("No data received");

    // Wait for processor to complete
    handle
        .await
        .expect("Processor task failed")
        .expect("Processor error");

    // Extract collection from received data
    let filtered_collection = filtered_data
        .as_collection()
        .expect("Received data should be a collection")
        .clone_box();

    // Verify results

    // Should have 2 rows (a=20, a=30) and 2 columns
    assert_eq!(filtered_collection.num_rows(), 2);
    let batch = RecordBatch::from_rows(filtered_collection.rows().to_vec());
    let columns = batch.columns();
    assert_eq!(columns.len(), 2);
    assert_eq!(
        columns[0].values(),
        &vec![Value::Int64(20), Value::Int64(30)]
    );
    assert_eq!(
        columns[1].values(),
        &vec![Value::Int64(200), Value::Int64(300)]
    );
}

/// Test FilterProcessor with no matching rows
#[tokio::test]
async fn test_filter_processor_no_match() {
    // Create test data
    let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
    let column_a = Column::new("".to_string(), "a".to_string(), col_a_values);
    let batch = RecordBatch::new(vec![column_a]).expect("Failed to create RecordBatch");

    // Create filter expression: a > 100 (no matches)
    let filter_expr = ScalarExpr::CallBinary {
        func: BinaryFunc::Gt,
        expr1: Box::new(ScalarExpr::column("", "a")),
        expr2: Box::new(ScalarExpr::literal(
            Value::Int64(100),
            ConcreteDatatype::Int64(Int64Type),
        )),
    };

    // Create PhysicalFilter
    let predicate = sqlparser::ast::Expr::BinaryOp {
        left: Box::new(sqlparser::ast::Expr::Identifier(
            sqlparser::ast::Ident::new("a"),
        )),
        op: sqlparser::ast::BinaryOperator::Gt,
        right: Box::new(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(
            "100".to_string(),
            false,
        ))),
    };

    let physical_filter = Arc::new(PhysicalFilter::new(predicate, filter_expr, vec![], 0));

    // Create FilterProcessor
    let mut filter_processor = FilterProcessor::new("test_filter_no_match", physical_filter);

    // Create channels
    let (input_sender, input_receiver) = broadcast::channel(10);
    let mut output_receiver = filter_processor.subscribe_output().expect("output stream");

    // Connect processor
    filter_processor.add_input(input_receiver);

    // Start processor
    let handle = filter_processor.start();

    // Send test data
    let stream_data = StreamData::collection(Box::new(batch));
    input_sender
        .send(stream_data)
        .map_err(|_| "Failed to send data")
        .expect("Failed to send data");

    // Give processor time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send end signal
    input_sender
        .send(StreamData::stream_end())
        .map_err(|_| "Failed to send end signal")
        .expect("Failed to send end signal");

    // Receive filtered data with timeout
    let filtered_data = timeout(Duration::from_secs(1), output_receiver.recv())
        .await
        .expect("Timeout waiting for filtered data")
        .expect("No data received");

    // Wait for processor to complete
    handle
        .await
        .expect("Processor task failed")
        .expect("Processor error");

    // Extract collection from received data
    let filtered_collection = filtered_data
        .as_collection()
        .expect("Received data should be a collection")
        .clone_box();

    // Verify results

    // Should have 0 rows but still 1 column
    assert_eq!(filtered_collection.num_rows(), 0);
    let batch = RecordBatch::from_rows(filtered_collection.rows().to_vec());
    assert_eq!(batch.column_pairs().len(), 0);
}
