use datatypes::{ConcreteDatatype, Value, Int32Type, StringType, Schema, ColumnSchema};
use datatypes::types::{StructField, StructType};
use datatypes::value::StructValue;
use flow::expr::scalar::ScalarExpr;
use flow::expr::evaluator::DataFusionEvaluator;
use flow::tuple::Tuple;
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
        vec![
            Value::Int32(42),
            Value::String("hello".to_string()),
        ],
        struct_type.clone(),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("struct_col".to_string(), ConcreteDatatype::Struct(struct_type.clone())),
    ]);

    // Create a tuple with the struct value
    let tuple = Tuple::from_values(schema, vec![struct_value]);

    // Create field access expression: column(0).x
    let column_expr = ScalarExpr::column(0);
    let field_access_expr = ScalarExpr::field_access(column_expr, "x");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the field access expression
    let result = field_access_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::Int32(42));
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
        vec![
            Value::Int32(42),
            Value::String("hello".to_string()),
        ],
        struct_type.clone(),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("struct_col".to_string(), ConcreteDatatype::Struct(struct_type.clone())),
    ]);

    // Create a tuple with the struct value
    let tuple = Tuple::from_values(schema, vec![struct_value]);

    // Create field access expression: column(0).y
    let column_expr = ScalarExpr::column(0);
    let field_access_expr = ScalarExpr::field_access(column_expr, "y");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the field access expression
    let result = field_access_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::String("hello".to_string()));
}

/// Tests nested struct field access functionality
/// Test scenario: Accessing fields in nested structs (a.b.c syntax)
/// Expression: column(0).inner.a
/// Expected result: 123
#[test]
fn test_field_access_nested() {
    // Create inner struct type: struct { a: Int32 }
    let inner_fields = Arc::new(vec![
        StructField::new("a".to_string(), ConcreteDatatype::Int32(Int32Type), false),
    ]);
    let inner_struct_type = StructType::new(inner_fields);

    // Create outer struct type: struct { inner: struct { a: Int32 }, b: String }
    let outer_fields = Arc::new(vec![
        StructField::new("inner".to_string(), ConcreteDatatype::Struct(inner_struct_type.clone()), false),
        StructField::new("b".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let outer_struct_type = StructType::new(outer_fields);

    // Create inner struct value: { a: 123 }
    let inner_struct_value = Value::Struct(StructValue::new(
        vec![Value::Int32(123)],
        inner_struct_type,
    ));

    // Create outer struct value: { inner: { a: 123 }, b: "world" }
    let outer_struct_value = Value::Struct(StructValue::new(
        vec![
            inner_struct_value,
            Value::String("world".to_string()),
        ],
        outer_struct_type.clone(),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("outer_struct".to_string(), ConcreteDatatype::Struct(outer_struct_type)),
    ]);

    // Create a tuple with the outer struct value
    let tuple = Tuple::from_values(schema, vec![outer_struct_value]);

    // Create nested field access expression: column(0).inner.a
    let column_expr = ScalarExpr::column(0);
    let inner_access_expr = ScalarExpr::field_access(column_expr, "inner");
    let nested_access_expr = ScalarExpr::field_access(inner_access_expr, "a");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the nested field access expression
    let result = nested_access_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result
    assert_eq!(result, Value::Int32(123));
}

/// Tests error handling when accessing non-existent struct fields
/// Test scenario: Attempting to access a field that doesn't exist in the struct
/// Expression: column(0).y (struct only has x field, not y field)
/// Expected result: FieldNotFound error
#[test]
fn test_field_access_field_not_found() {
    // Create struct type: struct { x: Int32 }
    let fields = Arc::new(vec![
        StructField::new("x".to_string(), ConcreteDatatype::Int32(Int32Type), false),
    ]);
    let struct_type = StructType::new(fields);

    // Create struct value: { x: 42 }
    let struct_value = Value::Struct(StructValue::new(
        vec![Value::Int32(42)],
        struct_type.clone(),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("struct_col".to_string(), ConcreteDatatype::Struct(struct_type.clone())),
    ]);

    // Create a tuple with the struct value
    let tuple = Tuple::from_values(schema, vec![struct_value]);

    // Create field access expression for non-existent field: column(0).y
    let column_expr = ScalarExpr::column(0);
    let field_access_expr = ScalarExpr::field_access(column_expr, "y");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the field access expression - should fail
    let result = field_access_expr.eval(&evaluator, &tuple);
    
    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, flow::expr::func::EvalError::FieldNotFound { .. }));
}

/// Tests error handling for field access on non-struct types
/// Test scenario: Attempting field access on Int32 type
/// Expression: column(0).x (column(0) is Int32 type, not a struct)
/// Expected result: TypeMismatch error
#[test]
fn test_field_access_not_struct() {
    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("int_col".to_string(), ConcreteDatatype::Int32(Int32Type)),
    ]);

    // Create a tuple with an Int32 value
    let tuple = Tuple::from_values(schema, vec![Value::Int32(42)]);

    // Create field access expression on non-struct value: column(0).x
    let column_expr = ScalarExpr::column(0);
    let field_access_expr = ScalarExpr::field_access(column_expr, "x");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the field access expression - should fail
    let result = field_access_expr.eval(&evaluator, &tuple);
    
    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, flow::expr::func::EvalError::TypeMismatch { .. }));
}