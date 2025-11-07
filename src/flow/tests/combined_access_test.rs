use datatypes::{ConcreteDatatype, Value, Int32Type, StringType, Schema, ColumnSchema};
use datatypes::types::{StructField, StructType, ListType};
use datatypes::value::{StructValue, ListValue};
use flow::expr::scalar::ScalarExpr;
use flow::expr::evaluator::DataFusionEvaluator;
use flow::tuple::Tuple;
use std::sync::Arc;

/// Tests combined struct field access followed by list index access
/// Test scenario: Create a struct containing a numbers field (Int32 list) and a name field (string),
///          where the numbers list contains three elements: [100, 200, 300]
/// Expression: column(0).numbers[1]
/// Expected result: 200 (access the struct's numbers field, then take the element at index 1 of the list)
#[test]
fn test_struct_field_then_list_index() {
    // Create struct type: struct { numbers: List<Int32>, name: String }
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));
    let fields = Arc::new(vec![
        StructField::new("numbers".to_string(), ConcreteDatatype::List(list_type), false),
        StructField::new("name".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let struct_type = StructType::new(fields);

    // Create list value: [100, 200, 300]
    let list_value = Value::List(ListValue::new(
        vec![
            Value::Int32(100),
            Value::Int32(200),
            Value::Int32(300),
        ],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create struct value: { numbers: [100, 200, 300], name: "test" }
    let struct_value = Value::Struct(StructValue::new(
        vec![
            list_value,
            Value::String("test".to_string()),
        ],
        struct_type.clone(),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("struct_col".to_string(), ConcreteDatatype::Struct(struct_type.clone())),
    ]);

    // Create a tuple with the struct value
    let tuple = Tuple::from_values(schema, vec![struct_value]);

    // Create combined access expression: column(0).numbers[1]
    let column_expr = ScalarExpr::column(0);
    let field_access_expr = ScalarExpr::field_access(column_expr, "numbers");
    let index_expr = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(datatypes::Int64Type));
    let combined_expr = ScalarExpr::list_index(field_access_expr, index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the combined expression
    let result = combined_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result (numbers[1] should be 200)
    assert_eq!(result, Value::Int32(200));
}

/// Tests combined list index access followed by struct field access
/// Test scenario: Create a list of structs where each struct contains x (Int32) and y (string) fields,
///          the list contains three elements: [{x: 10, y: "first"}, {x: 20, y: "second"}, {x: 30, y: "third"}]
/// Expression: column(0)[1].y
/// Expected result: "second" (first take the element at index 1 of the list, then access the y field of that struct)
#[test]
fn test_list_index_then_struct_field() {
    // Create struct type: struct { x: Int32, y: String }
    let struct_fields = Arc::new(vec![
        StructField::new("x".to_string(), ConcreteDatatype::Int32(Int32Type), false),
        StructField::new("y".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let struct_type = StructType::new(struct_fields);

    // Create list type: List<struct { x: Int32, y: String }>
    let list_type = ListType::new(Arc::new(ConcreteDatatype::Struct(struct_type.clone())));

    // Create struct values
    let struct_value1 = Value::Struct(StructValue::new(
        vec![
            Value::Int32(10),
            Value::String("first".to_string()),
        ],
        struct_type.clone(),
    ));

    let struct_value2 = Value::Struct(StructValue::new(
        vec![
            Value::Int32(20),
            Value::String("second".to_string()),
        ],
        struct_type.clone(),
    ));

    let struct_value3 = Value::Struct(StructValue::new(
        vec![
            Value::Int32(30),
            Value::String("third".to_string()),
        ],
        struct_type.clone(),
    ));

    // Create list value: [{x: 10, y: "first"}, {x: 20, y: "second"}, {x: 30, y: "third"}]
    let list_value = Value::List(ListValue::new(
        vec![struct_value1, struct_value2, struct_value3],
        Arc::new(ConcreteDatatype::Struct(struct_type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_col".to_string(), ConcreteDatatype::List(list_type)),
    ]);

    // Create a tuple with the list value
    let tuple = Tuple::from_values(schema, vec![list_value]);

    // Create combined access expression: column(0)[1].y
    let column_expr = ScalarExpr::column(0);
    let index_expr = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(column_expr, index_expr);
    let combined_expr = ScalarExpr::field_access(list_index_expr, "y");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the combined expression
    let result = combined_expr.eval(&evaluator, &tuple).unwrap();
    
    // Verify result (list[1].y should be "second")
    assert_eq!(result, Value::String("second".to_string()));
}

/// Tests combined access functionality in complex nested structures
/// Test scenario: Create a three-level nested structure, outer struct contains data field (list of structs) and name field,
///          where each struct in the data list contains a value field (Int32), specific values are:
///          {data: [{value: 100}, {value: 200}, {value: 300}], name: "complex"}
/// Expression: column(0).data[2].value
/// Expected result: 300 (access the outer struct's data field, take the element at index 2 of the list, then access that struct's value field)
#[test]
fn test_complex_nested_access() {
    // Create inner struct type: struct { value: Int32 }
    let inner_struct_fields = Arc::new(vec![
        StructField::new("value".to_string(), ConcreteDatatype::Int32(Int32Type), false),
    ]);
    let inner_struct_type = StructType::new(inner_struct_fields);

    // Create middle list type: List<struct { value: Int32 }>
    let middle_list_type = ListType::new(Arc::new(ConcreteDatatype::Struct(inner_struct_type.clone())));

    // Create outer struct type: struct { data: List<struct { value: Int32 }>, name: String }
    let outer_struct_fields = Arc::new(vec![
        StructField::new("data".to_string(), ConcreteDatatype::List(middle_list_type), false),
        StructField::new("name".to_string(), ConcreteDatatype::String(StringType), false),
    ]);
    let outer_struct_type = StructType::new(outer_struct_fields);

    // Create inner struct values
    let inner_struct1 = Value::Struct(StructValue::new(
        vec![Value::Int32(100)],
        inner_struct_type.clone(),
    ));

    let inner_struct2 = Value::Struct(StructValue::new(
        vec![Value::Int32(200)],
        inner_struct_type.clone(),
    ));

    let inner_struct3 = Value::Struct(StructValue::new(
        vec![Value::Int32(300)],
        inner_struct_type.clone(),
    ));

    // Create middle list value
    let middle_list_value = Value::List(ListValue::new(
        vec![inner_struct1, inner_struct2, inner_struct3],
        Arc::new(ConcreteDatatype::Struct(inner_struct_type)),
    ));

    // Create outer struct value
    let outer_struct_value = Value::Struct(StructValue::new(
        vec![
            middle_list_value,
            Value::String("complex".to_string()),
        ],
        outer_struct_type.clone(),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("complex_col".to_string(), ConcreteDatatype::Struct(outer_struct_type.clone())),
    ]);

    // Create a tuple with the complex value
    let tuple = Tuple::from_values(schema, vec![outer_struct_value]);

    // Create complex access expression: column(0).data[2].value
    let column_expr = ScalarExpr::column(0);
    let field_access_expr = ScalarExpr::field_access(column_expr, "data");
    let index_expr = ScalarExpr::literal(Value::Int64(2), ConcreteDatatype::Int64(datatypes::Int64Type));
    let list_index_expr = ScalarExpr::list_index(field_access_expr, index_expr);
    let final_field_access = ScalarExpr::field_access(list_index_expr, "value");

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the complex expression
    let result = final_field_access.eval(&evaluator, &tuple).unwrap();
    
    // Verify result (data[2].value should be 300)
    assert_eq!(result, Value::Int32(300));
}

/// Tests multi-level index access functionality for nested lists (list of lists)
/// Test scenario: Create a two-dimensional list structure, outer list contains three inner lists:
///          [[1, 2], [10, 20, 30], [100, 200]]
/// Expression: column(0)[1][2]
/// Expected result: 30 (first take the element at index 1 of the outer list [10, 20, 30], then take the element at index 2 of that inner list)
#[test]
fn test_list_of_lists() {
    // Create inner list type: List<Int32>
    let inner_list_type = ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)));

    // Create outer list type: List<List<Int32>>
    let outer_list_type = ListType::new(Arc::new(ConcreteDatatype::List(inner_list_type.clone())));

    // Create inner list values
    let inner_list1 = Value::List(ListValue::new(
        vec![Value::Int32(1), Value::Int32(2)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    let inner_list2 = Value::List(ListValue::new(
        vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    let inner_list3 = Value::List(ListValue::new(
        vec![Value::Int32(100), Value::Int32(200)],
        Arc::new(ConcreteDatatype::Int32(Int32Type)),
    ));

    // Create outer list value: [[1, 2], [10, 20, 30], [100, 200]]
    let outer_list_value = Value::List(ListValue::new(
        vec![inner_list1, inner_list2, inner_list3],
        Arc::new(ConcreteDatatype::List(inner_list_type)),
    ));

    // Create schema for the tuple
    let schema = Schema::new(vec![
        ColumnSchema::new("list_of_lists".to_string(), ConcreteDatatype::List(outer_list_type)),
    ]);

    // Create a tuple with the list of lists
    let tuple = Tuple::from_values(schema, vec![outer_list_value]);

    // Create nested list index expression: column(0)[1][2]
    let column_expr = ScalarExpr::column(0);
    let first_index_expr = ScalarExpr::literal(Value::Int64(1), ConcreteDatatype::Int64(datatypes::Int64Type));
    let first_list_index = ScalarExpr::list_index(column_expr, first_index_expr);
    let second_index_expr = ScalarExpr::literal(Value::Int64(2), ConcreteDatatype::Int64(datatypes::Int64Type));
    let final_list_index = ScalarExpr::list_index(first_list_index, second_index_expr);

    // Create evaluator
    let evaluator = DataFusionEvaluator::new();

    // Evaluate the nested expression
    let result = final_list_index.eval(&evaluator, &tuple).unwrap();
    
    // Verify result (list[1][2] should be 30)
    assert_eq!(result, Value::Int32(30));
}