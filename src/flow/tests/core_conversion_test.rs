//! Core conversion test: verify the complete flow of parser → SelectStmt → ScalarExpr → evaluation results
//! Core demonstration: directly receive SelectStmt, convert to ScalarExpr, verify calculation results
//! Test SQL: SELECT a+b, 42
//! Core flow: directly receive SelectStmt, convert to ScalarExpr, verify calculation results

use flow::{StreamSqlConverter, DataFusionEvaluator, ScalarExpr};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, StringType, Value};
use parser::parse_sql;

#[test]
fn test_core_conversion_flow() {
    println!("\n=== Core Conversion Flow Test ===");
    println!("Core demonstration: SelectStmt → ScalarExpr → Calculation Results");
    println!("Test SQL: SELECT a+b, 42");
    
    // 1. Create test schema
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // 2. Use parser module to parse SQL and get SelectStmt
    println!("\nStep 1: Use parser module to parse SQL");
    let sql = "SELECT a+b, 42";
    println!("Input SQL: {}", sql);
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parsing should succeed");
    println!("Successfully got SelectStmt with {} fields", select_stmt.select_fields.len());
    
    // 3. View SelectStmt structure (verify input is correct)
    println!("Step 2: SelectStmt structure validation");
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        println!("  Field {}: {:?}", i + 1, field.expr);
        println!("         Alias: {:?}", field.alias);
    }
    
    // 4. Core conversion: use StreamSqlConverter to convert SelectStmt to ScalarExpr
    println!("Step 3: Core conversion - SelectStmt → ScalarExpr");
    let converter = StreamSqlConverter::new();
    let expressions = converter.convert_select_stmt_to_scalar(&select_stmt, &schema)
        .expect("SelectStmt conversion should succeed");
    
    // 5. Verify conversion results
    println!("Successfully got {} ScalarExpr", expressions.len());
    assert_eq!(expressions.len(), 2, "Should get 2 expressions");
    
    // 6. Detailed validation of each expression
    println!("Step 4: Expression detailed validation");
    
    // First expression: a + b
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            println!("First expression is binary operation: {:?}", func);
            assert_eq!(*func, flow::expr::BinaryFunc::Add, "Should be addition operation");
            
            // Verify operand mapping
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    println!("Operands correctly mapped to columns {} + {}", idx1, idx2);
                    assert_eq!(*idx1, 0, "First operand should be column 0 (a)");
                    assert_eq!(*idx2, 1, "Second operand should be column 1 (b)");
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
    
    // 7. Create test data for calculation verification
    println!("Step 5: Calculation results verification");
    let evaluator = DataFusionEvaluator::new();
    let test_data = Row::from(vec![
        Value::Int64(5),  // a = 5
        Value::Int64(3),  // b = 3
    ]);
    let tuple = Tuple::new(schema, test_data);
    
    // Calculate first expression: a + b = 5 + 3 = 8
    let result1 = expressions[0].eval(&evaluator, &tuple).expect("Calculation should succeed");
    println!("Expression 1 (a+b) calculation result: {:?}", result1);
    assert_eq!(result1, Value::Int64(8), "a+b should equal 8");
    
    // Calculate second expression: 42 (literal)
    let result2 = expressions[1].eval(&evaluator, &tuple).expect("Calculation should succeed");
    println!("Expression 2 (42) calculation result: {:?}", result2);
    assert_eq!(result2, Value::Int64(42), "Literal 42 should equal 42");
    
    println!("Core conversion flow test completed!");
    println!("Verification result: parser → SelectStmt → StreamSqlConverter → ScalarExpr → Calculation Results");
    println!("   The entire flow is completely correct!");
}

#[test]
fn test_mixed_struct_and_list_access() {
    println!("\n=== Mixed Struct and List Access Test ===");
    println!("Test SQL: SELECT a->b, c[0]");
    println!("Goal: Verify both struct field access (a->b) and list indexing (c[0]) work correctly");
    
    // 1. Create test schema with struct and list columns
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::String(StringType)), // struct field
        ColumnSchema::new("c".to_string(), ConcreteDatatype::String(StringType)), // list field  
    ]);
    
    // 2. Use parser module to parse SQL with mixed access patterns
    println!("\nStep 1: Parse SQL with mixed struct and list access");
    let sql = "SELECT a->b, c[0]";
    println!("Input SQL: {}", sql);
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parsing should succeed");
    println!("Successfully got SelectStmt with {} fields", select_stmt.select_fields.len());
    
    // 3. View SelectStmt structure (verify parser correctly identified both access types)
    println!("Step 2: SelectStmt structure validation");
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        println!("  Field {}: {:?}", i + 1, field.expr);
        println!("         Alias: {:?}", field.alias);
    }
    
    // 4. Core conversion: convert SelectStmt to ScalarExpr
    println!("Step 3: Core conversion - SelectStmt → ScalarExpr");
    let converter = StreamSqlConverter::new();
    let expressions = converter.convert_select_stmt_to_scalar(&select_stmt, &schema)
        .expect("SelectStmt conversion should succeed");
    
    // 5. Verify conversion results
    println!("Successfully got {} ScalarExpr", expressions.len());
    assert_eq!(expressions.len(), 2, "Should get 2 expressions");
    
    // 6. Detailed validation of each expression with assertions
    println!("Step 4: Expression detailed validation with assertions");
    
    // First expression: a->b (struct field access)
    println!("Validating first expression: a->b (struct field access)");
    match &expressions[0] {
        ScalarExpr::FieldAccess { expr, field_name } => {
            println!("✓ First expression is FieldAccess: field '{}' from {:?}", field_name, expr);
            
            // Assert field name is correct
            assert_eq!(field_name, "b", "Field name should be 'b'");
            
            // Assert the base expression is a column reference to column 0 (struct 'a')
            match expr.as_ref() {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 0, "Base expression should reference column 0 (struct 'a')");
                    println!("✓ Base expression correctly references column 0 (struct 'a')");
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
            println!("✓ Second expression is ListIndex: {:?}[{:?}]", expr, index_expr);
            
            // Assert the base expression is a column reference to column 1 (list 'c')
            match expr.as_ref() {
                ScalarExpr::Column(idx) => {
                    assert_eq!(*idx, 1, "Base expression should reference column 1 (list 'c')");
                    println!("✓ Base expression correctly references column 1 (list 'c')");
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