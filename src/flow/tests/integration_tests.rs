//! Integration tests for DataFusion-based expression evaluation

use datafusion_common::ScalarValue;
use flow::*;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, StringType, Float64Type, BooleanType, Value};

fn create_test_schema() -> datatypes::Schema {
    datatypes::Schema::new(vec![
        ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("first_name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("last_name".to_string(), ConcreteDatatype::String(StringType)),
        ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("score".to_string(), ConcreteDatatype::Float64(Float64Type)),
        ColumnSchema::new("active".to_string(), ConcreteDatatype::Bool(BooleanType)),
    ])
}

fn create_test_tuple() -> Tuple {
    let schema = create_test_schema();
    let values = vec![
        Value::Int64(1),
        Value::String("John".to_string()),
        Value::String("Doe".to_string()),
        Value::Int64(25),
        Value::Float64(95.5),
        Value::Bool(true),
    ];
    Tuple::from_values(schema, values)
}

#[test]
fn test_concat_string_string() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test concat(first_name, last_name) using CallDf
    let first_name_col = ScalarExpr::column(1); // first_name column
    let last_name_col = ScalarExpr::column(2); // last_name column
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![first_name_col, last_name_col],
    };
    
    let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("JohnDoe".to_string()));
}

#[test]
fn test_concat_string_literal() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test concat("Hello, ", first_name) using CallDf
    let hello_lit = ScalarExpr::literal(Value::String("Hello, ".to_string()), ConcreteDatatype::String(StringType));
    let first_name_col = ScalarExpr::column(1); // first_name column
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![hello_lit, first_name_col],
    };
    
    let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("Hello, John".to_string()));
}

#[test]
fn test_concat_int_string() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test concat(age, " years old") using CallDf
    let age_col = ScalarExpr::column(3); // age column
    let years_lit = ScalarExpr::literal(Value::String(" years old".to_string()), ConcreteDatatype::String(StringType));
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![age_col, years_lit],
    };
    
    let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("25 years old".to_string()));
}

#[test]
fn test_concat_float_string() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test concat("Score: ", score) using CallDf
    let score_prefix = ScalarExpr::literal(Value::String("Score: ".to_string()), ConcreteDatatype::String(StringType));
    let score_col = ScalarExpr::column(4); // score column
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![score_prefix, score_col],
    };
    
    let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("Score: 95.5".to_string()));
}

#[test]
fn test_concat_bool_string() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test concat("Active: ", active) using CallDf
    let active_prefix = ScalarExpr::literal(Value::String("Active: ".to_string()), ConcreteDatatype::String(StringType));
    let active_col = ScalarExpr::column(5); // active column
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![active_prefix, active_col],
    };
    
    let result = evaluator.evaluate_expr(&concat_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("Active: true".to_string()));
}

#[test]
fn test_string_functions() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test upper function
    let first_name_col = ScalarExpr::column(1); // first_name column
    let upper_expr = ScalarExpr::CallDf {
        function_name: "upper".to_string(),
        args: vec![first_name_col],
    };
    
    let result = evaluator.evaluate_expr(&upper_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("JOHN".to_string()));

    // Test lower function
    let first_name_col = ScalarExpr::column(1); // first_name column
    let lower_expr = ScalarExpr::CallDf {
        function_name: "lower".to_string(),
        args: vec![first_name_col],
    };
    
    let result = evaluator.evaluate_expr(&lower_expr, &tuple).unwrap();
    assert_eq!(result, Value::String("john".to_string()));

    // Test length function
    let first_name_col = ScalarExpr::column(1); // first_name column
    let length_expr = ScalarExpr::CallDf {
        function_name: "length".to_string(),
        args: vec![first_name_col],
    };
    
    let result = evaluator.evaluate_expr(&length_expr, &tuple).unwrap();
    assert_eq!(result, Value::Int64(4)); // "John" has 4 characters
}

#[test]
fn test_math_functions() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test abs function
    let score_col = ScalarExpr::column(4); // score column
    let abs_expr = ScalarExpr::CallDf {
        function_name: "abs".to_string(),
        args: vec![score_col],
    };
    
    let result = evaluator.evaluate_expr(&abs_expr, &tuple).unwrap();
    assert_eq!(result, Value::Float64(95.5));

    // Test round function
    let score_col = ScalarExpr::column(4); // score column
    let round_expr = ScalarExpr::CallDf {
        function_name: "round".to_string(),
        args: vec![score_col],
    };
    
    let result = evaluator.evaluate_expr(&round_expr, &tuple).unwrap();
    assert_eq!(result, Value::Float64(96.0)); // 95.5 rounded to 96
}

#[test]
fn test_type_conversion() {
    use flow::expr::datafusion_adapter::*;
    
    // Test Value to ScalarValue conversion
    let int_value = Value::Int64(42);
    let scalar = value_to_scalar_value(&int_value).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(Some(42)));

    let string_value = Value::String("test".to_string());
    let scalar = value_to_scalar_value(&string_value).unwrap();
    assert_eq!(scalar, ScalarValue::Utf8(Some("test".to_string())));

    // Test ScalarValue to Value conversion
    let scalar = ScalarValue::Float64(Some(3.14));
    let value = scalar_value_to_value(&scalar).unwrap();
    assert_eq!(value, Value::Float64(3.14));
}

#[test]
fn test_schema_conversion() {
    use flow::expr::datafusion_adapter::*;
    
    let flow_schema = create_test_schema();
    let arrow_schema = flow_schema_to_arrow_schema(&flow_schema).unwrap();
    
    assert_eq!(arrow_schema.fields().len(), 6);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(arrow_schema.field(0).data_type(), &arrow::datatypes::DataType::Int64);
    assert_eq!(arrow_schema.field(1).name(), "first_name");
    assert_eq!(arrow_schema.field(1).data_type(), &arrow::datatypes::DataType::Utf8);
}

#[test]
fn test_tuple_to_record_batch() {
    use flow::expr::datafusion_adapter::*;
    
    let tuple = create_test_tuple();
    let record_batch = tuple_to_record_batch(&tuple).unwrap();
    
    assert_eq!(record_batch.num_columns(), 6);
    assert_eq!(record_batch.num_rows(), 1);
    
    // Check first column (id)
    let id_array = record_batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(id_array.value(0), 1);
    
    // Check second column (first_name)
    let name_array = record_batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(name_array.value(0), "John");
}

#[test]
fn test_error_handling() {
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();

    // Test invalid column index
    let invalid_col_expr = ScalarExpr::column(100); // Invalid index
    let result = evaluator.evaluate_expr(&invalid_col_expr, &tuple);
    assert!(result.is_err());

    // Test unknown function
    let unknown_expr = ScalarExpr::CallDf {
        function_name: "unknown_function".to_string(),
        args: vec![ScalarExpr::column(0)],
    };
    let result = evaluator.evaluate_expr(&unknown_expr, &tuple);
    assert!(result.is_err());
}

#[test]
fn test_eval_error_not_implemented() {
    // Test that CallDf returns NotImplemented error when evaluated with regular eval
    let tuple = create_test_tuple();
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![ScalarExpr::column(1), ScalarExpr::column(2)],
    };
    
    // This should now work with the new eval method that uses DataFusion evaluator
    let evaluator = DataFusionEvaluator::new();
    let result = concat_expr.eval(&evaluator, &tuple);
    
    // The result should now be successful
    assert!(result.is_ok());
    let expected = Value::String("JohnDoe".to_string());
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn test_call_df_with_unified_eval() {
    // Test that CallDf can be evaluated with eval_with_datafusion
    let evaluator = DataFusionEvaluator::new();
    let tuple = create_test_tuple();
    let concat_expr = ScalarExpr::CallDf {
        function_name: "concat".to_string(),
        args: vec![ScalarExpr::column(1), ScalarExpr::column(2)],
    };
    
    // This should work with the unified eval method
    let result = concat_expr.eval(&evaluator, &tuple);
    assert!(result.is_ok());
    
    // The result should be the concatenation of first_name and last_name
    let expected = Value::String("JohnDoe".to_string());
    assert_eq!(result.unwrap(), expected);
}