use datatypes::value::ListValue;
use datatypes::{ConcreteDatatype, Int32Type, StringType, Value};
use flow::expr::scalar::ScalarExpr;
use flow::expr::DataFusionEvaluator;
use flow::model::{Column, RecordBatch};
use std::sync::Arc;

/// Tests basic list index access functionality
/// Test scenario: Accessing the first element of a list
/// Expression: column(0)[0]
/// Expected result: 10
#[test]
fn test_list_index_simple() {
    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression: test_table.list_col[0]
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(0),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression using vectorized evaluation
    let results = list_index_expr
        .eval_with_collection(&evaluator, &collection)
        .unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int32(10));
}

/// Tests list middle element index access functionality
/// Test scenario: Accessing the middle element (index 1) of a list containing [10, 20, 30]
/// Expression: column(0)[1]
/// Expected result: 20
#[test]
fn test_list_index_middle() {
    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression: test_table.list_col[1]
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(1),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression using vectorized evaluation
    let results = list_index_expr
        .eval_with_collection(&evaluator, &collection)
        .unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int32(20));
}

/// Tests list last element index access functionality
/// Test scenario: Accessing the last element (index 2) of a list containing [10, 20, 30]
/// Expression: column(0)[2]
/// Expected result: 30
#[test]
fn test_list_index_last() {
    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression: test_table.list_col[2]
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(2),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression using vectorized evaluation
    let results = list_index_expr
        .eval_with_collection(&evaluator, &collection)
        .unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int32(30));
}

/// Tests string list index access functionality
/// Test scenario: Accessing the second element (index 1) of a string list containing ["hello", "world"]
/// Expression: column(0)[1]
/// Expected result: "world"
#[test]
fn test_list_index_string_list() {
    // Create list value: ["hello", "world"]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ],
        Arc::new(ConcreteDatatype::String(StringType)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression: test_table.list_col[1]
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(1),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression using vectorized evaluation
    let results = list_index_expr
        .eval_with_collection(&evaluator, &collection)
        .unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("world".to_string()));
}

/// Tests list out-of-bounds index access error handling
/// Test scenario: Attempting to access index 3 (out of bounds) of a list containing [10, 20, 30]
/// Expression: column(0)[3]
/// Expected result: ListIndexOutOfBounds error
#[test]
fn test_list_index_out_of_bounds() {
    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression: test_table.list_col[3] (out of bounds)
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(3),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let results = list_index_expr.eval_with_collection(&evaluator, &collection);

    // Verify error
    assert!(results.is_err());
    let error = results.unwrap_err();
    assert!(matches!(
        error,
        flow::expr::func::EvalError::ListIndexOutOfBounds { .. }
    ));
}

/// Tests negative index access error handling
/// Test scenario: Attempting to access index -1 (negative index) of a list containing [10, 20, 30]
/// Expression: column(0)[-1]
/// Expected result: ListIndexOutOfBounds error
#[test]
fn test_list_index_negative_index() {
    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression: test_table.list_col[-1] (negative index)
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(-1),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let results = list_index_expr.eval_with_collection(&evaluator, &collection);

    // Verify error
    assert!(results.is_err());
    let error = results.unwrap_err();
    assert!(matches!(
        error,
        flow::expr::func::EvalError::ListIndexOutOfBounds { .. }
    ));
}

/// Tests error handling for index access on non-list types
/// Test scenario: Attempting list index access on Int32 value 42
/// Expression: column(0)[0]
/// Expected result: TypeMismatch error
#[test]
fn test_list_index_not_list() {
    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "int_col".to_string(),
        vec![Value::Int32(42)],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression on non-list value: test_table.int_col[0]
    let column_expr = ScalarExpr::column("test_table", "int_col");
    let index_expr = ScalarExpr::literal(
        Value::Int64(0),
        ConcreteDatatype::Int64(datatypes::Int64Type),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let results = list_index_expr.eval_with_collection(&evaluator, &collection);

    // Verify error
    assert!(results.is_err());
    let error = results.unwrap_err();
    assert!(matches!(
        error,
        flow::expr::func::EvalError::TypeMismatch { .. }
    ));
}

/// Tests error handling for invalid index types
/// Test scenario: Attempting to use string "hello" as index to access a list containing [10, 20, 30]
/// Expression: column(0)["hello"]
/// Expected result: InvalidIndexType error
#[test]
fn test_list_index_invalid_index_type() {
    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create list index expression with non-integer index: test_table.list_col["hello"]
    let column_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::literal(
        Value::String("hello".to_string()),
        ConcreteDatatype::String(StringType),
    );
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let results = list_index_expr.eval_with_collection(&evaluator, &collection);

    // Verify error
    assert!(results.is_err());
    let error = results.unwrap_err();
    assert!(matches!(
        error,
        flow::expr::func::EvalError::InvalidIndexType { .. }
    ));
}

/// Tests dynamic index access functionality
/// Test scenario: Using another column's value as index to access a list containing [10, 20, 30, 40, 50]
/// Expression: column(0)[column(1)] where column(1) = 3
/// Expected result: 40
#[test]
fn test_list_index_dynamic_index() {
    // Create list value: [10, 20, 30, 40, 50]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
            Value::Int32(40),
            Value::Int32(50),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create a single-row collection for vectorized testing with both list and index columns
    let list_column = Column::new(
        "test_table".to_string(),
        "list_col".to_string(),
        vec![list_value],
    );
    let index_column = Column::new(
        "test_table".to_string(),
        "index_col".to_string(),
        vec![Value::Int64(3)],
    );
    let collection = RecordBatch::new(vec![list_column, index_column]).unwrap();

    // Create list index expression: test_table.list_col[test_table.index_col] where index_col = 3
    let list_expr = ScalarExpr::column("test_table", "list_col");
    let index_expr = ScalarExpr::column("test_table", "index_col");
    let list_index_expr = ScalarExpr::list_index(list_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression using vectorized evaluation
    let results = list_index_expr
        .eval_with_collection(&evaluator, &collection)
        .unwrap();

    // Verify result (index 3 should be 40)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int32(40));
}
