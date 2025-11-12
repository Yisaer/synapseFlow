//! Core conversion test: verify the complete flow of parser → SelectStmt → ScalarExpr → evaluation results
//! Core demonstration: directly receive SelectStmt, convert to ScalarExpr, verify calculation results
//! Test SQL: SELECT a+b, 42
//! Core flow: directly receive SelectStmt, convert to ScalarExpr, verify calculation results

use flow::{StreamSqlConverter, DataFusionEvaluator, ScalarExpr};
use flow::model::{RecordBatch, Column};
use datatypes::{Value};
use parser::parse_sql;
use std::collections::HashMap;

#[test]
fn test_core_conversion_flow() {
    let sql = "SELECT a+b, 42";
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parsing should succeed");

    // 3. Core conversion: use StreamSqlConverter to convert SelectStmt to ScalarExpr
    println!("Step 3: Core conversion - SelectStmt → ScalarExpr");
    let converter = StreamSqlConverter::new();
    let expressions = converter.convert_select_stmt_to_scalar(&select_stmt)
        .expect("SelectStmt conversion should succeed");
    assert_eq!(expressions.len(), 2, "Should get 2 expressions");
    
    // 5. Detailed validation of each expression
    println!("Step 4: Expression detailed validation");
    
    // First expression: a + b
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            assert_eq!(*func, flow::expr::BinaryFunc::Add, "Should be addition operation");
            
            // Verify operand mapping
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column { source_name: src1, column_name: col1 }, ScalarExpr::Column { source_name: src2, column_name: col2 }) => {
                    assert_eq!(src1, "", "First operand should be from default source");
                    assert_eq!(col1, "a", "First operand should be column 'a'");
                    assert_eq!(src2, "", "Second operand should be from default source");
                    assert_eq!(col2, "b", "Second operand should be column 'b'");
                }
                _ => panic!("Operands should be column references"),
            }
        }
        _ => panic!("First expression should be binary operation"),
    }
    
    // Second expression: 42 (literal)
    match &expressions[1] {
        ScalarExpr::Literal(val, _) => {
            println!("Second expression is literal: {:?}", val);
            assert_eq!(*val, Value::Int64(42), "Should be integer 42");
        }
        _ => panic!("Second expression should be literal"),
    }
    //
    // 6. Create test data for calculation verification
    println!("Step 5: Calculation results verification");
    let evaluator = DataFusionEvaluator::new();

    // Create tuple with HashMap data matching our column references
    let mut data = HashMap::new();
    data.insert(("".to_string(), "a".to_string()), Value::Int64(5));  // a = 5
    data.insert(("".to_string(), "b".to_string()), Value::Int64(3));  // b = 3

    let collection = RecordBatch::new( vec![
        Column::new("a".to_string(), "".to_string(), vec![Value::Int64(5)]),
        Column::new("b".to_string(), "".to_string(), vec![Value::Int64(3)]),
    ]).unwrap();

    let results1 = expressions[0].eval_with_collection(&evaluator, &collection).expect("Calculation should succeed");
    assert_eq!(results1[0], Value::Int64(8), "a+b should equal 8");

    // Calculate second expression: 42 (literal)
    let results2 = expressions[1].eval_with_collection(&evaluator, &collection).expect("Calculation should succeed");
    assert_eq!(results2[0], Value::Int64(42), "Literal 42 should equal 42");
}

#[test]
fn test_mixed_struct_and_list_access() {
    // 1. Use parser module to parse SQL with mixed access patterns
    let sql = "SELECT a->b, c[0]";
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parsing should succeed");
    // 3. Core conversion: convert SelectStmt to ScalarExpr
    let converter = StreamSqlConverter::new();
    let expressions = converter.convert_select_stmt_to_scalar(&select_stmt)
        .expect("SelectStmt conversion should succeed");
    
    // 4. Verify conversion results
    println!("Successfully got {} ScalarExpr", expressions.len());
    assert_eq!(expressions.len(), 2, "Should get 2 expressions");
    
    // 5. Detailed validation of each expression with assertions
    println!("Step 4: Expression detailed validation with assertions");
    
    // First expression: a->b (struct field access)
    println!("Validating first expression: a->b (struct field access)");
    match &expressions[0] {
        ScalarExpr::FieldAccess { expr, field_name } => {
            println!("✓ First expression is FieldAccess: field '{}' from {:?}", field_name, expr);
            
            // Assert field name is correct
            assert_eq!(field_name, "b", "Field name should be 'b'");
            
            // Assert the base expression is a column reference to default.a
            match expr.as_ref() {
                ScalarExpr::Column { source_name, column_name } => {
                    assert_eq!(source_name, "", "Base expression should reference default source");
                    assert_eq!(column_name, "a", "Base expression should reference column 'a'");
                }
                _ => panic!("Base expression should be Column reference"),
            }
        }
        _ => panic!("First expression should be FieldAccess for struct field access"),
    }
    
    // Second expression: c[0] (list indexing)
    println!("Validating second expression: c[0] (list indexing)");
    match &expressions[1] {
        ScalarExpr::ListIndex { expr, index_expr } => {
            // Assert the base expression is a column reference to default.c
            match expr.as_ref() {
                ScalarExpr::Column { source_name, column_name } => {
                    assert_eq!(source_name, "", "Base expression should reference default source");
                    assert_eq!(column_name, "c", "Base expression should reference column 'c'");
                }
                _ => panic!("Base expression should be Column reference"),
            }
            
            // Assert the index expression is a literal with value 0
            match index_expr.as_ref() {
                ScalarExpr::Literal(val, _) => {
                    assert_eq!(*val, Value::Int64(0), "Index should be literal 0");
                    println!("✓ Index expression correctly has literal value 0");
                }
                _ => panic!("Index expression should be Literal"),
            }
        }
        _ => panic!("Second expression should be ListIndex for list indexing"),
    }
    
    // Final comprehensive assertion
    assert!(matches!(expressions[0], ScalarExpr::FieldAccess { .. }), "First expression should be FieldAccess");
    assert!(matches!(expressions[1], ScalarExpr::ListIndex { .. }), "Second expression should be ListIndex");
    
    println!("All assertions passed! Test validates complete functionality.");
}
