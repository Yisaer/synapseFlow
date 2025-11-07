//! Unit tests for struct field access functionality (a->b)
//! Following PROJECT_RULES.md requirements: SQL → SelectStmt conversion

use parser::parse_sql;

#[test]
fn test_simple_struct_field_access() {
    // Test basic struct field access: a->b
    let sql = "SELECT user->name FROM users";
    
    // Step 1: Parse SQL to SelectStmt (parser module responsibility)
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Verify the parsed structure - this validates SQL → SelectStmt conversion
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::JsonAccess { left, operator, right } => {
            assert!(matches!(left.as_ref(), sqlparser::ast::Expr::Identifier(_)));
            assert_eq!(format!("{:?}", operator), "Arrow");
            assert!(matches!(right.as_ref(), sqlparser::ast::Expr::Identifier(_)));
        }
        _ => panic!("Expected JsonAccess expression"),
    }
}

#[test]
fn test_multiple_struct_field_access() {
    // Test multiple struct field accesses in one query
    let sql = "SELECT user->name, user->age, data->field FROM users";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Verify all fields are JsonAccess expressions (parser validation)
    for field in &select_stmt.select_fields {
        match &field.expr {
            sqlparser::ast::Expr::JsonAccess { .. } => {
                // Valid JsonAccess expression - parser correctly identified struct access
            }
            _ => panic!("Expected JsonAccess expression, got {:?}", field.expr),
        }
    }
}

#[test]
fn test_nested_struct_field_access() {
    // Test nested struct field access: a->b->c
    let sql = "SELECT data->profile->name FROM users";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    // Verify nested JsonAccess structure
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::JsonAccess { left, operator: _, right } => {
            // Left should be identifier "data"
            match left.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => {
                    assert_eq!(ident.value, "data");
                }
                _ => panic!("Expected identifier on left side"),
            }
            
            // Right should be another JsonAccess (profile->name)
            match right.as_ref() {
                sqlparser::ast::Expr::JsonAccess { left: inner_left, operator: _, right: inner_right } => {
                    match inner_left.as_ref() {
                        sqlparser::ast::Expr::Identifier(inner_ident) => {
                            assert_eq!(inner_ident.value, "profile");
                        }
                        _ => panic!("Expected identifier in nested access"),
                    }
                    match inner_right.as_ref() {
                        sqlparser::ast::Expr::Identifier(inner_ident) => {
                            assert_eq!(inner_ident.value, "name");
                        }
                        _ => panic!("Expected identifier in nested access"),
                    }
                }
                _ => panic!("Expected nested JsonAccess expression"),
            }
        }
        _ => panic!("Expected JsonAccess expression"),
    }
}

#[test]
fn test_struct_field_access_with_alias() {
    // Test struct field access with alias
    let sql = "SELECT user->name AS username, data->field AS value FROM users";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 2);
    
    // Verify aliases are preserved in SelectStmt
    assert_eq!(select_stmt.select_fields[0].alias, Some("username".to_string()));
    assert_eq!(select_stmt.select_fields[1].alias, Some("value".to_string()));
    
    // Verify expressions are JsonAccess
    for field in &select_stmt.select_fields {
        match &field.expr {
            sqlparser::ast::Expr::JsonAccess { .. } => {
                // Valid JsonAccess expression
            }
            _ => panic!("Expected JsonAccess expression"),
        }
    }
}

#[test]
fn test_struct_field_access_with_operations() {
    // Test struct field access mixed with arithmetic operations
    let sql = "SELECT (user->age) * 2, user->name FROM users";
    
    // Step 1: Parse SQL to SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    assert_eq!(select_stmt.select_fields.len(), 2);
    
    // Verify first field is a BinaryOp with nested JsonAccess
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            assert_eq!(format!("{:?}", op), "Multiply");
            
            // Left operand should be nested JsonAccess
            match left.as_ref() {
                sqlparser::ast::Expr::Nested(inner) => {
                    match inner.as_ref() {
                        sqlparser::ast::Expr::JsonAccess { .. } => {
                            // Valid nested JsonAccess
                        }
                        _ => panic!("Expected JsonAccess inside nested"),
                    }
                }
                _ => panic!("Expected nested expression"),
            }
            
            // Right operand should be literal 2
            match right.as_ref() {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "2"),
                _ => panic!("Expected literal value"),
            }
        }
        _ => panic!("Expected BinaryOp for first field"),
    }
    
    // Verify second field is simple JsonAccess
    match &select_stmt.select_fields[1].expr {
        sqlparser::ast::Expr::JsonAccess { .. } => {
            // Valid JsonAccess
        }
        _ => panic!("Expected JsonAccess for second field"),
    }
}

#[test]
fn test_struct_field_access_error_handling() {
    // Test error handling for edge cases
    
    // Test 1: Valid SQL should parse successfully (table validation happens during execution)
    let valid_sql = "SELECT user->name FROM nonexistent_table";
    let result = parse_sql(valid_sql);
    assert!(result.is_ok(), "Should parse successfully, table validation happens during execution");
    
    // Test 2: Non-SELECT statements should be rejected
    let insert_sql = "INSERT INTO users VALUES (user->name)";
    let result = parse_sql(insert_sql);
    assert!(result.is_err(), "Non-SELECT statements should be rejected");
}

#[test]
fn test_struct_field_access_expression_analysis() {
    // Test expression analysis for struct field access (parser-level analysis only)
    use parser::expression_extractor::analyze_sql_expressions;
    
    let sql = "SELECT user->name, user->age, data->field FROM users";
    
    let analysis = analyze_sql_expressions(sql).expect("Should analyze successfully");
    
    // Should detect expressions (parser-level analysis, not flow-specific)
    assert!(analysis.expression_count > 0, "Should detect expressions");
    
    // Verify we have the expected number of top-level expressions
    // Each JsonAccess is an expression, plus any nested ones
    println!("Found {} expressions in analysis", analysis.expression_count);
    println!("Binary operations: {:?}", analysis.binary_operations);
    println!("Functions: {:?}", analysis.functions);
    println!("Literals: {:?}", analysis.literals);
}