use datatypes::types::{StructField, StructType};
use datatypes::value::StructValue;
use datatypes::{ConcreteDatatype, Int32Type, StringType, Value};
use flow::expr::scalar::ScalarExpr;
use flow::model::{Column, RecordBatch};
use std::sync::Arc;

/// Tests basic struct field access functionality
/// Test scenario: Accessing an integer field from a struct
/// Expression: column(0).x
/// Expected result: 42
#[test]
fn test_field_access_simple() {
    // Create struct type: struct { x: Int32, y: String }
    let fields = Arc::new(vec![
        StructField::new("x".to_string(), ConcreteDatatype::Int32(Int32Type), false),
        StructField::new("y".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let struct_type = StructType::new(fields);

    // Create struct value: { x: 42, y: "hello" }
    let struct_value = Value::Struct(StructValue::new(
        vec![Value::Int32(42), Value::String("hello".to_string())],
        struct_type.clone(),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "struct_col".to_string(),
        vec![struct_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create field access expression: test_table.struct_col.x
    let column_expr = ScalarExpr::column("test_table", "struct_col");
    let field_access_expr = ScalarExpr::field_access(column_expr, "x");

    // Evaluate the field access expression
    let results = field_access_expr.eval_with_collection(&collection).unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int32(42));
}

/// Tests struct string field access functionality
/// Test scenario: Accessing a string field from a struct
/// Expression: column(0).y
/// Expected result: "hello"
#[test]
fn test_field_access_string_field() {
    // Create struct type: struct { x: Int32, y: String }
    let fields = Arc::new(vec![
        StructField::new("x".to_string(), ConcreteDatatype::Int32(Int32Type), false),
        StructField::new("y".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let struct_type = StructType::new(fields);

    // Create struct value: { x: 42, y: "hello" }
    let struct_value = Value::Struct(StructValue::new(
        vec![Value::Int32(42), Value::String("hello".to_string())],
        struct_type.clone(),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "struct_col".to_string(),
        vec![struct_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create field access expression: test_table.struct_col.y
    let column_expr = ScalarExpr::column("test_table", "struct_col");
    let field_access_expr = ScalarExpr::field_access(column_expr, "y");

    // Evaluate the field access expression
    let results = field_access_expr.eval_with_collection(&collection).unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello".to_string()));
}

/// Tests nested struct field access functionality
/// Test scenario: Accessing fields in nested structs (a.b.c syntax)
/// Expression: column(0).inner.a
/// Expected result: 123
#[test]
fn test_field_access_nested() {
    // Create inner struct type: struct { a: Int32 }
    let inner_fields = Arc::new(vec![StructField::new(
        "a".to_string(),
        ConcreteDatatype::Int32(Int32Type),
        false,
    )]);
    let inner_struct_type = StructType::new(inner_fields);

    // Create outer struct type: struct { inner: struct { a: Int32 }, b: String }
    let outer_fields = Arc::new(vec![
        StructField::new(
            "inner".to_string(),
            ConcreteDatatype::Struct(inner_struct_type.clone()),
            false,
        ),
        StructField::new("b".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let outer_struct_type = StructType::new(outer_fields);

    // Create inner struct value: { a: 123 }
    let inner_struct_value =
        Value::Struct(StructValue::new(vec![Value::Int32(123)], inner_struct_type));

    // Create outer struct value: { inner: { a: 123 }, b: "world" }
    let outer_struct_value = Value::Struct(StructValue::new(
        vec![inner_struct_value, Value::String("world".to_string())],
        outer_struct_type.clone(),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "outer_struct".to_string(),
        vec![outer_struct_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create nested field access expression: test_table.outer_struct.inner.a
    let column_expr = ScalarExpr::column("test_table", "outer_struct");
    let inner_access_expr = ScalarExpr::field_access(column_expr, "inner");
    let nested_access_expr = ScalarExpr::field_access(inner_access_expr, "a");

    // Evaluate the nested field access expression
    let results = nested_access_expr
        .eval_with_collection(&collection)
        .unwrap();

    // Verify result
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int32(123));
}

/// Tests error handling when accessing non-existent struct fields
/// Test scenario: Attempting to access a field that doesn't exist in the struct
/// Expression: column(0).y (struct only has x field, not y field)
/// Expected result: FieldNotFound error
#[test]
fn test_field_access_field_not_found() {
    // Create struct type: struct { x: Int32 }
    let fields = Arc::new(vec![StructField::new(
        "x".to_string(),
        ConcreteDatatype::Int32(Int32Type),
        false,
    )]);
    let struct_type = StructType::new(fields);

    // Create struct value: { x: 42 }
    let struct_value = Value::Struct(StructValue::new(
        vec![Value::Int32(42)],
        struct_type.clone(),
    ));

    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "struct_col".to_string(),
        vec![struct_value],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create field access expression for non-existent field: test_table.struct_col.y
    let column_expr = ScalarExpr::column("test_table", "struct_col");
    let field_access_expr = ScalarExpr::field_access(column_expr, "y");

    // Evaluate the field access expression - should fail
    let result = field_access_expr.eval_with_collection(&collection);

    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        flow::expr::func::EvalError::FieldNotFound { .. }
    ));
}

/// Tests error handling for field access on non-struct types
/// Test scenario: Attempting field access on Int32 type
/// Expression: column(0).x (column(0) is Int32 type, not a struct)
/// Expected result: TypeMismatch error
#[test]
fn test_field_access_not_struct() {
    // Create a single-row collection for vectorized testing
    let column = Column::new(
        "test_table".to_string(),
        "int_col".to_string(),
        vec![Value::Int32(42)],
    );
    let collection = RecordBatch::new(vec![column]).unwrap();

    // Create field access expression on non-struct value: test_table.int_col.x
    let column_expr = ScalarExpr::column("test_table", "int_col");
    let field_access_expr = ScalarExpr::field_access(column_expr, "x");

    // Evaluate the field access expression - should fail
    let result = field_access_expr.eval_with_collection(&collection);

    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        flow::expr::func::EvalError::TypeMismatch { .. }
    ));
}
