//! Integration tests for struct field access following PROJECT_RULES.md
//! Tests complete SQL → SelectStmt conversion workflow

use parser::parse_sql;

#[test]
fn test_complete_struct_field_access_workflow() {
    // Test the complete workflow for simple struct field access
    let sql = "SELECT user->name FROM users";
    
    // Step 1: SQL → SelectStmt (Parser module responsibility only)
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify SelectStmt structure per PROJECT_RULES.md - parser validation only
    assert_eq!(select_stmt.select_fields.len(), 1);
    let field = &select_stmt.select_fields[0];
    assert!(field.alias.is_none());
    
    // Verify it's a JsonAccess expression - this confirms parser correctly identified struct access
    match &field.expr {
        sqlparser::ast::Expr::JsonAccess { left, operator, right } => {
            // Validate the structure was correctly parsed
            match left.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "user"),
                _ => panic!("Expected identifier 'user' on left side"),
            }
            assert_eq!(format!("{:?}", operator), "Arrow");
            match right.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "name"),
                _ => panic!("Expected identifier 'name' on right side"),
            }
        }
        _ => panic!("Expected JsonAccess expression in SelectStmt"),
    }
    
    println!("✓ SQL → SelectStmt conversion successful");
    println!("  Parsed SelectStmt with JsonAccess: user->name");
}

#[test]
fn test_struct_field_with_alias_workflow() {
    // Test complete workflow with alias
    let sql = "SELECT data->field AS value FROM table1";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify SelectStmt with alias - parser validation
    assert_eq!(select_stmt.select_fields.len(), 1);
    let field = &select_stmt.select_fields[0];
    assert_eq!(field.alias, Some("value".to_string()));
    
    // Verify JsonAccess expression was correctly parsed
    match &field.expr {
        sqlparser::ast::Expr::JsonAccess { left, operator, right } => {
            match left.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "data"),
                _ => panic!("Expected 'data' identifier"),
            }
            assert_eq!(format!("{:?}", operator), "Arrow");
            match right.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "field"),
                _ => panic!("Expected 'field' identifier"),
            }
        }
        _ => panic!("Expected MapAccess expression"),
    }
    
    println!("✓ SQL → SelectStmt with alias successful");
    println!("  Parsed: data->field AS value");
}

#[test]
fn test_multiple_struct_fields_workflow() {
    // Test multiple struct field accesses
    let sql = "SELECT user->name, user->age, data->status FROM records";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify multiple fields were parsed correctly
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Verify all fields are JsonAccess (parser validation)
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        match &field.expr {
            sqlparser::ast::Expr::JsonAccess { left, operator, right } => {
                assert_eq!(format!("{:?}", operator), "Arrow");
                println!("  Field {}: {}->{} ", i + 1, 
                    match left.as_ref() {
                        sqlparser::ast::Expr::Identifier(ident) => &ident.value,
                        _ => panic!("Expected identifier"),
                    },
                    match right.as_ref() {
                        sqlparser::ast::Expr::Identifier(ident) => &ident.value,
                        _ => panic!("Expected identifier"),
                    }
                );
            }
            _ => panic!("Expected JsonAccess expression for field {}", i + 1),
        }
    }
    
    println!("✓ Multiple struct field accesses parsed successfully");
}

#[test]
fn test_nested_struct_field_workflow() {
    // Test nested struct field access: a->b->c
    let sql = "SELECT config->database->host FROM settings";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify single field with nested structure
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::JsonAccess { left, operator: _, right } => {
            // Left should be "config"
            match left.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "config"),
                _ => panic!("Expected 'config' identifier"),
            }
            
            // Right should be nested JsonAccess (database->host)
            match right.as_ref() {
                sqlparser::ast::Expr::JsonAccess { left: inner_left, operator: _, right: inner_right } => {
                    match inner_left.as_ref() {
                        sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "database"),
                        _ => panic!("Expected 'database' identifier"),
                    }
                    match inner_right.as_ref() {
                        sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "host"),
                        _ => panic!("Expected 'host' identifier"),
                    }
                }
                _ => panic!("Expected nested JsonAccess expression"),
            }
        }
        _ => panic!("Expected JsonAccess expression"),
    }
    
    println!("✓ Nested struct field access parsed: config->database->host");
}

#[test]
fn test_struct_field_with_arithmetic_workflow() {
    // Test struct field access combined with arithmetic
    let sql = "SELECT (user->age) * 2 + 1 FROM users";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify complex expression structure
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            assert_eq!(format!("{:?}", op), "Plus");
            
            // Left side should be multiplication
            match left.as_ref() {
                sqlparser::ast::Expr::BinaryOp { left: mul_left, op: mul_op, right: mul_right } => {
                    assert_eq!(format!("{:?}", mul_op), "Multiply");
                    
                    // Left of multiplication should be nested JsonAccess
                    match mul_left.as_ref() {
                        sqlparser::ast::Expr::Nested(inner) => {
                            match inner.as_ref() {
                                sqlparser::ast::Expr::JsonAccess { .. } => {
                                    // Valid JsonAccess inside nested parentheses
                                }
                                _ => panic!("Expected JsonAccess inside nested parentheses"),
                            }
                        }
                        _ => panic!("Expected nested expression for multiplication left operand"),
                    }
                    
                    // Right of multiplication should be literal 2
                    match mul_right.as_ref() {
                        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "2"),
                        _ => panic!("Expected literal value 2"),
                    }
                }
                _ => panic!("Expected multiplication operation"),
            }
            
            // Right side should be literal 1
            match right.as_ref() {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "1"),
                _ => panic!("Expected literal value 1"),
            }
        }
        _ => panic!("Expected BinaryOp for complex arithmetic expression"),
    }
    
    println!("✓ Complex arithmetic with struct field access parsed: (user->age) * 2 + 1");
}

#[test]
fn test_struct_field_with_function_workflow() {
    // Test struct field access in function calls
    let sql = "SELECT CONCAT(user->first_name, ' ', user->last_name) AS full_name FROM users";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify function call with struct field access
    assert_eq!(select_stmt.select_fields.len(), 1);
    let field = &select_stmt.select_fields[0];
    assert_eq!(field.alias, Some("full_name".to_string()));
    
    match &field.expr {
        sqlparser::ast::Expr::Function(func) => {
            assert_eq!(func.name.to_string().to_uppercase(), "CONCAT");
            assert_eq!(func.args.len(), 3); // first_name, ' ', last_name
            
            // Verify arguments contain JsonAccess expressions
            let json_access_count = func.args.iter().filter(|arg| {
                match arg {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => {
                        matches!(expr, sqlparser::ast::Expr::JsonAccess { .. })
                    }
                    _ => false,
                }
            }).count();
            
            assert_eq!(json_access_count, 2, "Should have 2 JsonAccess expressions in function args");
        }
        _ => panic!("Expected Function expression"),
    }
    
    println!("✓ Function call with struct field access parsed: CONCAT(user->first_name, ' ', user->last_name)");
}

#[test]
fn test_complex_struct_field_workflow() {
    // Test complex query with multiple struct field access patterns
    let sql = "SELECT 
        user->id,
        user->profile->name AS username,
        (user->settings->age) + 5 AS adjusted_age,
        CONCAT('Status: ', data->status) AS status_msg
    FROM users";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify all 4 fields are parsed
    assert_eq!(select_stmt.select_fields.len(), 4);
    
    // Verify field 1: simple struct access
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::JsonAccess { .. } => {
            println!("  Field 1: Simple struct access user->id");
        }
        _ => panic!("Expected JsonAccess for field 1"),
    }
    
    // Verify field 2: nested struct access with alias
    match &select_stmt.select_fields[1].expr {
        sqlparser::ast::Expr::JsonAccess { .. } => {
            assert_eq!(select_stmt.select_fields[1].alias, Some("username".to_string()));
            println!("  Field 2: Nested struct access user->profile->name with alias");
        }
        _ => panic!("Expected JsonAccess for field 2"),
    }
    
    // Verify field 3: arithmetic with nested struct access
    match &select_stmt.select_fields[2].expr {
        sqlparser::ast::Expr::BinaryOp { .. } => {
            assert_eq!(select_stmt.select_fields[2].alias, Some("adjusted_age".to_string()));
            println!("  Field 3: Arithmetic with nested struct access (user->settings->age) + 5");
        }
        _ => panic!("Expected BinaryOp for field 3"),
    }
    
    // Verify field 4: function with struct access
    match &select_stmt.select_fields[3].expr {
        sqlparser::ast::Expr::Function(_) => {
            assert_eq!(select_stmt.select_fields[3].alias, Some("status_msg".to_string()));
            println!("  Field 4: Function call with struct access CONCAT('Status: ', data->status)");
        }
        _ => panic!("Expected Function for field 4"),
    }
    
    println!("✓ Complex multi-field query with various struct access patterns parsed successfully");
}

#[test]
fn test_error_handling_workflow() {
    // Test error scenarios in the workflow
    
    // Test 1: Valid SQL should parse successfully
    let valid_sql = "SELECT user->field FROM table1";
    let result = parse_sql(valid_sql);
    assert!(result.is_ok(), "Valid struct field access should parse successfully");
    
    // Test 2: Non-SELECT statements should be rejected
    let insert_sql = "INSERT INTO table1 VALUES (user->field)";
    let result = parse_sql(insert_sql);
    assert!(result.is_err(), "Non-SELECT statements should be rejected");
    
    // Test 3: Valid SQL with struct field access should work even with complex expressions
    let complex_sql = "SELECT (user->age + user->score) FROM users";
    let result = parse_sql(complex_sql);
    assert!(result.is_ok(), "Valid complex expressions with struct access should parse");
    
    println!("✓ Error handling works correctly for various scenarios");
}