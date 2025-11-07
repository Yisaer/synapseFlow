use datatypes::{ConcreteDatatype, Value, Int32Type, StringType, Schema, ColumnSchema};
use datatypes::value::ListValue;
use datatypes::types::ListType;
use flow::expr::scalar::ScalarExpr;
use flow::expr::evaluator::DataFusionEvaluator;
use flow::tuple::Tuple;
use std::sync::Arc;

/// Tests basic list index access functionality
/// Test scenario: Accessing the first element of a list
/// Expression: column(0)[0]
/// Expected result: 10
#[test]
fn test_list_index_simple() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression: column(0)[0]
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(0), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression
    let result = list_index_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::Int32(10));
}

/// Tests list middle element index access functionality
/// Test scenario: Accessing the middle element (index 1) of a list containing [10, 20, 30]
/// Expression: column(0)[1]
/// Expected result: 20
#[test]
fn test_list_index_middle() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression: column(0)[1]
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression
    let result = list_index_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::Int32(20));
}

/// Tests list last element index access functionality
/// Test scenario: Accessing the last element (index 2) of a list containing [10, 20, 30]
/// Expression: column(0)[2]
/// Expected result: 30
#[test]
fn test_list_index_last() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression: column(0)[2]
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(2), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression
    let result = list_index_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::Int32(30));
}

/// Tests string list index access functionality
/// Test scenario: Accessing the second element (index 1) of a string list containing ["hello", "world"]
/// Expression: column(0)[1]
/// Expected result: "world"
#[test]
fn test_list_index_string_list() {
    // Create list type: List<String>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::String(StringType)));

    // Create list value: ["hello", "world"]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ],
        Arc::new(ConcreteDatatype::String(StringType)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression: column(0)[1]
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression
    let result = list_index_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::String("world".to_string()));
}

/// Tests list out-of-bounds index access error handling
/// Test scenario: Attempting to access index 3 (out of bounds) of a list containing [10, 20, 30]
/// Expression: column(0)[3]
/// Expected result: ListIndexOutOfBounds error
#[test]
fn test_list_index_out_of_bounds() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression: column(0)[3] (out of bounds)
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(3), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let result = list_index_expr.eval(&evaluator, &tuple);
    
    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, flow::expr::func::EvalError::ListIndexOutOfBounds { .. }));
}

/// Tests negative index access error handling
/// Test scenario: Attempting to access index -1 (negative index) of a list containing [10, 20, 30]
/// Expression: column(0)[-1]
/// Expected result: ListIndexOutOfBounds error
#[test]
fn test_list_index_negative_index() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression: column(0)[-1] (negative index)
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(-1), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let result = list_index_expr.eval(&evaluator, &tuple);
    
    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, flow::expr::func::EvalError::ListIndexOutOfBounds { .. }));
}

/// Tests error handling for index access on non-list types
/// Test scenario: Attempting list index access on Int32 value 42
/// Expression: column(0)[0]
/// Expected result: TypeMismatch error
#[test]
fn test_list_index_not_list() {
    // Create schema for the tuple with Int32 value
    let schema = Schema::new(vec![
        ColumnSchema::new("int_col".to_string(), ConcreteDatatype::Int32(Int32Type)),
    ]);

    // Create a tuple with an Int32 value
    let tuple = Tuple::from_values(schema, vec![Value::Int32(42)]);

    // Create list index expression on non-list value: column(0)[0]
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(0), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let result = list_index_expr.eval(&evaluator, &tuple);
    
    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, flow::expr::func::EvalError::TypeMismatch { .. }));
}

/// Tests error handling for invalid index types
/// Test scenario: Attempting to use string "hello" as index to access a list containing [10, 20, 30]
/// Expression: column(0)["hello"]
/// Expected result: InvalidIndexType error
#[test]
fn test_list_index_invalid_index_type() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create list value: [10, 20, 30]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create list index expression with non-integer index: column(0)["hello"]
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::String("hello".to_string()), ConcreteDatatype::String(StringType));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression - should fail
    let result = list_index_expr.eval(&evaluator, &tuple);
    
    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, flow::expr::func::EvalError::InvalidIndexType { .. }));
}

/// Tests dynamic index access functionality
/// Test scenario: Using another column's value as index to access a list containing [10, 20, 30, 40, 50]
/// Expression: column(0)[column(1)] where column(1) = 3
/// Expected result: 40
#[test]
fn test_list_index_dynamic_index() {
    // Create list type: List<Int32>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

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

    // Create schema for the tuple with both list and index
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
        ColumnSchema::new("index_col".to_string(), ConcreteDatatype::Int64(datatypes::Int64Type)),
    ]);

    // Create a tuple with the list value and index value
    let tuple = Tuple::from_values(schema, vec![list_value, Value::Int64(3)]);

    // Create list index expression: column(0)[column(1)] where column(1) = 3
    let list_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::column(1);
    let list_index_expr = ScalarExpr::list_index(list_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the list index expression
    let result = list_index_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result (index 3 should be 40)
    assert_eq!(result, Value::Int32(40));
}