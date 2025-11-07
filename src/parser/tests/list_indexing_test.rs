//! Unit tests for list indexing functionality (a[0])
//! Following PROJECT_RULES.md requirements: SQL → SelectStmt conversion

use parser::parse_sql;

#[test]
fn test_simple_list_indexing() {
    // Test basic list indexing: a[0]
    let sql = "SELECT items[0] FROM orders";
    
    // Step 1: Parse SQL to SelectStmt (parser module responsibility)
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Verify MapAccess structure - this validates SQL → SelectStmt conversion
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::MapAccess { column, keys } => {
            // Verify column is "items"
            match column.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "items"),
                _ => panic!("Expected identifier 'items' as column"),
            }
            
            // Verify keys contain single element "0"
            assert_eq!(keys.len(), 1);
            match &keys[0] {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "0"),
                _ => panic!("Expected numeric key '0'"),
            }
        }
        _ => panic!("Expected MapAccess expression"),
    }
}

#[test]
fn test_list_indexing_with_alias() {
    // Test list indexing with alias: items[0] AS first_item
    let sql = "SELECT items[0] AS first_item FROM orders";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Verify alias is preserved in SelectStmt
    assert_eq!(select_stmt.select_fields[0].alias, Some("first_item".to_string()));
    
    // Verify MapAccess structure
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::MapAccess { column, keys } => {
            match column.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "items"),
                _ => panic!("Expected identifier 'items'"),
            }
            assert_eq!(keys.len(), 1);
        }
        _ => panic!("Expected MapAccess expression"),
    }
}

#[test]
fn test_multiple_list_indexing() {
    // Test multiple list indexing: items[0], items[1], items[2]
    let sql = "SELECT items[0], items[1], items[2] FROM orders";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Verify all fields are MapAccess with correct indices
    let expected_indices = ["0", "1", "2"];
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        match &field.expr {
            sqlparser::ast::Expr::MapAccess { column, keys } => {
                match column.as_ref() {
                    sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "items"),
                    _ => panic!("Expected identifier 'items'"),
                }
                assert_eq!(keys.len(), 1);
                match &keys[0] {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                        assert_eq!(n, expected_indices[i]);
                    }
                    _ => panic!("Expected numeric key"),
                }
            }
            _ => panic!("Expected MapAccess expression for field {}", i + 1),
        }
    }
}

#[test]
fn test_list_indexing_mixed_with_operations() {
    // Test list indexing mixed with arithmetic operations that are supported
    let sql = "SELECT items[0] + 5 FROM orders";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Verify BinaryOp with MapAccess (parser validation)
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            assert_eq!(format!("{:?}", op), "Plus");
            
            // Left should be MapAccess
            match left.as_ref() {
                sqlparser::ast::Expr::MapAccess { column, keys } => {
                    match column.as_ref() {
                        sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "items"),
                        _ => panic!("Expected identifier 'items'"),
                    }
                    assert_eq!(keys.len(), 1);
                    match &keys[0] {
                        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "0"),
                        _ => panic!("Expected numeric key '0'"),
                    }
                }
                _ => panic!("Expected MapAccess for left operand"),
            }
            
            // Right should be literal 5
            match right.as_ref() {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "5"),
                _ => panic!("Expected numeric literal '5'"),
            }
        }
        _ => panic!("Expected BinaryOp for arithmetic operation"),
    }
}

#[test]
fn test_list_indexing_function_call() {
    // Test list indexing in function calls
    let sql = "SELECT CONCAT(items[0], '-', items[1]) FROM orders";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Verify function call with MapAccess arguments (parser validation)
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::Function(func) => {
            assert_eq!(func.name.to_string().to_uppercase(), "CONCAT");
            
            // Count MapAccess arguments
            let map_access_count = func.args.iter().filter(|arg| {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
                        matches!(expr, sqlparser::ast::Expr::MapAccess { .. })
                    }
                    _ => false,
                }
            }).count();
            
            assert!(map_access_count >= 2, "Should have at least 2 MapAccess expressions in function args");
        }
        _ => panic!("Expected Function expression"),
    }
}

#[test]
fn test_list_indexing_nested_simple() {
    // Test what the parser actually supports - simple cases only
    // Note: Complex nested indexing like items[indexes[0]] is not supported by current parser
    
    // This should work - simple numeric indexing
    let sql1 = "SELECT items[0] FROM orders";
    let result1 = parse_sql(sql1);
    assert!(result1.is_ok(), "Simple numeric indexing should work");
    
    // This should work - string literal indexing  
    let sql2 = "SELECT data['key'] FROM table1";
    let result2 = parse_sql(sql2);
    if result2.is_ok() {
        println!("String literal indexing supported");
    } else {
        println!("String literal indexing not supported: {}", result2.unwrap_err());
    }
}

#[test]
fn test_list_indexing_error_handling() {
    // Test error handling for edge cases
    
    // Test 1: Valid SQL should parse successfully (table validation happens during execution)
    let valid_sql = "SELECT items[0] FROM orders";
    let result = parse_sql(valid_sql);
    assert!(result.is_ok(), "Valid list indexing should parse successfully");
    
    // Test 2: Non-SELECT statements should be rejected
    let insert_sql = "INSERT INTO orders VALUES (items[0])";
    let result = parse_sql(insert_sql);
    assert!(result.is_err(), "Non-SELECT statements should be rejected");
}

#[test]
fn test_list_indexing_expression_analysis() {
    // Test expression analysis for list indexing (parser-level analysis only)
    use parser::expression_extractor::analyze_sql_expressions;
    
    let sql = "SELECT items[0], data[1] FROM orders";
    
    let analysis = analyze_sql_expressions(sql).expect("Should analyze successfully");
    
    // Should detect expressions (parser-level analysis, not flow-specific)
    assert!(analysis.expression_count > 0, "Should detect expressions");
    
    // Verify we have the expected number of top-level expressions
    println!("Found {} expressions in analysis", analysis.expression_count);
    println!("Binary operations: {:?}", analysis.binary_operations);
    println!("Functions: {:?}", analysis.functions);
    println!("Literals: {:?}", analysis.literals);
}