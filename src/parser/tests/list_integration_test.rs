//! Integration tests for list indexing following PROJECT_RULES.md
//! Tests complete SQL → SelectStmt conversion workflow

use parser::parse_sql;

#[test]
fn test_complete_list_indexing_workflow() {
    // Test the complete workflow for simple list indexing
    let sql = "SELECT items[0] FROM orders";
    
    // Step 1: SQL → SelectStmt (Parser module responsibility only)
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify SelectStmt structure per PROJECT_RULES.md - parser validation only
    assert_eq!(select_stmt.select_fields.len(), 1);
    let field = &select_stmt.select_fields[0];
    assert!(field.alias.is_none());
    
    // Verify it's a MapAccess expression - this confirms parser correctly identified list access
    match &field.expr {
        sqlparser::ast::Expr::MapAccess { column, keys } => {
            // Validate the structure was correctly parsed
            match column.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "items"),
                _ => panic!("Expected identifier 'items' on column side"),
            }
            assert_eq!(keys.len(), 1);
            match &keys[0] {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "0"),
                _ => panic!("Expected numeric key '0'"),
            }
        }
        _ => panic!("Expected MapAccess expression in SelectStmt"),
    }
    
    println!("✓ SQL → SelectStmt conversion successful");
    println!("  Parsed SelectStmt with MapAccess: items[0]");
}

#[test]
fn test_list_indexing_with_alias_workflow() {
    // Test complete workflow with alias
    let sql = "SELECT data[1] AS second_item FROM table1";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify SelectStmt with alias - parser validation
    assert_eq!(select_stmt.select_fields.len(), 1);
    let field = &select_stmt.select_fields[0];
    assert_eq!(field.alias, Some("second_item".to_string()));
    
    // Verify MapAccess expression was correctly parsed
    match &field.expr {
        sqlparser::ast::Expr::MapAccess { column, keys } => {
            match column.as_ref() {
                sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "data"),
                _ => panic!("Expected 'data' identifier"),
            }
            assert_eq!(keys.len(), 1);
            match &keys[0] {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "1"),
                _ => panic!("Expected numeric key '1'"),
            }
        }
        _ => panic!("Expected MapAccess expression"),
    }
    
    println!("✓ SQL → SelectStmt with alias successful");
    println!("  Parsed: data[1] AS second_item");
}

#[test]
fn test_multiple_list_indexing_workflow() {
    // Test multiple list indexing
    let sql = "SELECT list[0], list[1], list[2] FROM arrays";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify multiple fields were parsed correctly
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Verify all fields are MapAccess (parser validation)
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        match &field.expr {
            sqlparser::ast::Expr::MapAccess { column, keys } => {
                assert_eq!(keys.len(), 1);
                match column.as_ref() {
                    sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "list"),
                    _ => panic!("Expected 'list' identifier"),
                }
                match &keys[0] {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                        assert_eq!(n, &(i.to_string()));
                        println!("  Field {}: list[{}]", i + 1, n);
                    }
                    _ => panic!("Expected numeric key"),
                }
            }
            _ => panic!("Expected MapAccess expression for field {}", i + 1),
        }
    }
    
    println!("✓ Multiple list indexing parsed successfully");
}

#[test]
fn test_list_indexing_with_arithmetic_workflow() {
    // Test list indexing combined with arithmetic
    let sql = "SELECT (items[0] + 10) * 2 FROM orders";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify complex expression structure
    assert_eq!(select_stmt.select_fields.len(), 1);
    
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            assert_eq!(format!("{:?}", op), "Multiply");
            
            // Left should be nested addition
            match left.as_ref() {
                sqlparser::ast::Expr::Nested(inner) => {
                    match inner.as_ref() {
                        sqlparser::ast::Expr::BinaryOp { left: add_left, op: add_op, right: add_right } => {
                            assert_eq!(format!("{:?}", add_op), "Plus");
                            
                            // Left of addition should be MapAccess
                            match add_left.as_ref() {
                                sqlparser::ast::Expr::MapAccess { column, keys } => {
                                    match column.as_ref() {
                                        sqlparser::ast::Expr::Identifier(ident) => assert_eq!(ident.value, "items"),
                                        _ => panic!("Expected 'items' identifier"),
                                    }
                                    assert_eq!(keys.len(), 1);
                                    match &keys[0] {
                                        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "0"),
                                        _ => panic!("Expected numeric key '0'"),
                                    }
                                }
                                _ => panic!("Expected MapAccess for left operand"),
                            }
                            
                            // Right of addition should be literal 10
                            match add_right.as_ref() {
                                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "10"),
                                _ => panic!("Expected numeric literal '10'"),
                            }
                        }
                        _ => panic!("Expected BinaryOp for addition"),
                    }
                }
                _ => panic!("Expected nested expression"),
            }
            
            // Right should be literal 2
            match right.as_ref() {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => assert_eq!(n, "2"),
                _ => panic!("Expected numeric literal '2'"),
            }
        }
        _ => panic!("Expected BinaryOp for multiplication"),
    }
    
    println!("✓ Complex arithmetic with list indexing parsed: (items[0] + 10) * 2");
}

#[test]
fn test_list_indexing_function_call_workflow() {
    // Test list indexing in function calls
    let sql = "SELECT CONCAT(items[0], '-', items[1]) AS combined FROM orders";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify function call with list indexing
    assert_eq!(select_stmt.select_fields.len(), 1);
    let field = &select_stmt.select_fields[0];
    assert_eq!(field.alias, Some("combined".to_string()));
    
    match &field.expr {
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
    
    println!("✓ Function call with list indexing parsed: CONCAT(items[0], '-', items[1])");
}

#[test]
fn test_mixed_access_types_workflow() {
    // Test mixing different access types in one query
    let sql = "SELECT 
        user->name,
        items[0],
        data.field
    FROM table1";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify all 3 fields are parsed
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Verify field 1: JsonAccess
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::JsonAccess { .. } => {
            println!("  Field 1: JsonAccess user->name");
        }
        _ => panic!("Expected JsonAccess for field 1"),
    }
    
    // Verify field 2: MapAccess
    match &select_stmt.select_fields[1].expr {
        sqlparser::ast::Expr::MapAccess { .. } => {
            println!("  Field 2: MapAccess items[0]");
        }
        _ => panic!("Expected MapAccess for field 2"),
    }
    
    // Verify field 3: CompoundIdentifier
    match &select_stmt.select_fields[2].expr {
        sqlparser::ast::Expr::CompoundIdentifier(_) => {
            println!("  Field 3: CompoundIdentifier data.field");
        }
        _ => panic!("Expected CompoundIdentifier for field 3"),
    }
    
    println!("✓ Mixed access types parsed successfully");
}

#[test]
fn test_simple_complex_list_indexing_workflow() {
    // Test simpler complex query that the parser can handle
    let sql = "SELECT 
        data[0] AS first_element,
        items[1] AS second_element,
        CONCAT(names[0], names[1]) AS combined
    FROM records";
    
    // Step 1: SQL → SelectStmt
    let select_stmt = parse_sql(sql).expect("Should parse successfully");
    
    // Verify all 3 fields
    assert_eq!(select_stmt.select_fields.len(), 3);
    
    // Verify field 1: simple list indexing with alias
    match &select_stmt.select_fields[0].expr {
        sqlparser::ast::Expr::MapAccess { .. } => {
            assert_eq!(select_stmt.select_fields[0].alias, Some("first_element".to_string()));
            println!("  Field 1: data[0] AS first_element");
        }
        _ => panic!("Expected MapAccess for field 1"),
    }
    
    // Verify field 2: simple list indexing with alias
    match &select_stmt.select_fields[1].expr {
        sqlparser::ast::Expr::MapAccess { .. } => {
            assert_eq!(select_stmt.select_fields[1].alias, Some("second_element".to_string()));
            println!("  Field 2: items[1] AS second_element");
        }
        _ => panic!("Expected MapAccess for field 2"),
    }
    
    // Verify field 3: function with list indexing
    match &select_stmt.select_fields[2].expr {
        sqlparser::ast::Expr::Function(_) => {
            assert_eq!(select_stmt.select_fields[2].alias, Some("combined".to_string()));
            println!("  Field 3: CONCAT(names[0], names[1]) AS combined");
        }
        _ => panic!("Expected Function for field 3"),
    }
    
    println!("✓ Complex multi-field query with various list indexing patterns parsed successfully");
}

#[test]
fn test_error_handling_workflow() {
    // Test error scenarios in the workflow
    
    // Test 1: Valid SQL should parse successfully
    let valid_sql = "SELECT items[0] FROM orders";
    let result = parse_sql(valid_sql);
    assert!(result.is_ok(), "Valid list indexing should parse successfully");
    
    // Test 2: Non-SELECT statements should be rejected
    let insert_sql = "INSERT INTO orders (data) VALUES (items[0])";
    let result = parse_sql(insert_sql);
    assert!(result.is_err(), "Non-SELECT statements should be rejected");
    
    // Test 3: Valid SQL with list indexing should work even with complex expressions
    let complex_sql = "SELECT (items[0] + items[1]) FROM orders";
    let result = parse_sql(complex_sql);
    assert!(result.is_ok(), "Valid complex expressions with list indexing should parse");
    
    println!("✓ Error handling works correctly for various scenarios");
}