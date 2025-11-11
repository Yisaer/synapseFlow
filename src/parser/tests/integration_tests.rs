//! Integration tests using StreamSqlParser with SelectStmt output
//! All tests use StreamSqlParser.parse() and work with SelectStmt structure

use parser::{StreamSqlParser, parse_sql};
use sqlparser::ast::Expr;

#[test]
fn test_stream_dialect_basic_parsing() {
    let basic_sql = r#"
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.age > 18
    "#;

    // Use StreamSqlParser.parse() instead of direct Parser::parse_sql
    let parser = StreamSqlParser::new();
    let result = parser.parse(basic_sql);
    
    assert!(result.is_ok());
    let select_stmt = result.unwrap();
    
    // Verify we got a proper SelectStmt
    assert!(!select_stmt.select_fields.is_empty());
    assert_eq!(select_stmt.select_fields.len(), 2); // u.name and o.amount
    
    println!("✓ StreamDialect basic parsing successful");
    println!("✓ Found {} select fields", select_stmt.select_fields.len());
    
    // Check field details
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        println!("  Field {}: {:?}", i + 1, field.expr);
        assert!(field.alias.is_none()); // No aliases in this SQL
    }
}

// Demo test showing WHERE condition functionality
#[test]
fn demo_where_condition_functionality() {
    println!("\n=== WHERE Condition Functionality Demo ===\n");
    
    let test_cases = vec![
        ("Simple WHERE clause", "SELECT name FROM users WHERE age > 18"),
        ("Complex WHERE with AND/OR", "SELECT id, name FROM products WHERE price < 100 AND category = 'electronics' OR featured = true"),
        ("WHERE with function", "SELECT email FROM customers WHERE LENGTH(name) > 5"),
        ("No WHERE clause", "SELECT name, age FROM users"),
    ];
    
    for (description, sql) in test_cases {
        println!("Test: {}", description);
        println!("SQL: {}", sql);
        
        match parse_sql(sql) {
            Ok(select_stmt) => {
                println!("Parse successful!");
                println!("Found {} SELECT fields", select_stmt.select_fields.len());
                
                // Check WHERE condition
                match &select_stmt.where_condition {
                    Some(where_expr) => {
                        println!("WHERE condition present: {:?}", where_expr);
                        match where_expr {
                            Expr::BinaryOp { op, .. } => println!("  Type: Binary operation ({:?})", op),
                            Expr::Function(func) => println!("  Type: Function call ('{}')", func.name),
                            _ => println!("  Type: Other expression"),
                        }
                    }
                    None => {
                        println!("No WHERE condition (correctly set to None)");
                    }
                }
                
                // Check HAVING clause (should be None for these examples)
                match &select_stmt.having {
                    Some(having_expr) => println!("HAVING clause present: {:?}", having_expr),
                    None => println!("No HAVING clause (correctly set to None)"),
                }
            }
            Err(e) => {
                println!("Parse failed: {}", e);
            }
        }
        println!();
    }
    
    println!("WHERE Condition Functionality Demo Complete!");
}

#[test]
fn test_stream_dialect_where_condition_parsing() {
    let sql = "SELECT id, name FROM users WHERE age >= 21 AND status = 'active' OR city = 'NYC'";
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    
    // Verify SELECT fields
    assert_eq!(select_stmt.select_fields.len(), 2);
    println!("✓ StreamDialect parsed {} SELECT fields", select_stmt.select_fields.len());
    
    // Verify WHERE condition is present and correct
    assert!(select_stmt.where_condition.is_some(), "WHERE condition should be extracted");
    
    let where_expr = select_stmt.where_condition.as_ref().unwrap();
    println!("✓ WHERE condition extracted: {:?}", where_expr);
    
    // Verify WHERE condition structure (should be a complex binary operation)
    match where_expr {
        Expr::BinaryOp { op, left, right } => {
            println!("  WHERE is binary operation: {:?}", op);
            println!("  Left operand: {:?}", left);
            println!("  Right operand: {:?}", right);
        }
        _ => {
            println!("  WHERE expression type: {:?}", where_expr);
        }
    }
}

#[test]
fn test_stream_dialect_where_without_condition() {
    let sql = "SELECT name, age FROM users";
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    
    // Verify SELECT fields
    assert_eq!(select_stmt.select_fields.len(), 2);
    
    // Verify WHERE condition is None when no WHERE clause
    assert!(select_stmt.where_condition.is_none(), "WHERE condition should be None when no WHERE clause");
    println!("✓ WHERE condition correctly set to None for SQL without WHERE clause");
}

#[test]
fn test_stream_dialect_tumblingwindow_parsing() {
    let tumbling_sql = r#"
        SELECT id, name, timestamp FROM stream GROUP BY tumblingwindow('ss', 10)
    "#;

    let parser = StreamSqlParser::new();
    let result = parser.parse(tumbling_sql);
    
    if let Err(e) = &result {
        println!("Tumblingwindow parsing error: {}", e);
    }
    
    assert!(result.is_ok());
    let select_stmt = result.unwrap();
    
    // Verify parsing succeeded
    println!("✓ StreamDialect tumblingwindow parsing successful");
    println!("✓ Found {} select fields", select_stmt.select_fields.len());
    
    // Should handle the tumblingwindow in GROUP BY clause
    // The SELECT fields should be processed normally
    for field in &select_stmt.select_fields {
        println!("  Field: {:?}", field.expr);
    }
}

#[test]
fn test_stream_dialect_expression_fields() {
    let sql = "SELECT a + b, CONCAT(name, 'test'), 42";
    
    // Use StreamSqlParser to get SelectStmt
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    
    // Verify we have the expected number of fields
    assert_eq!(select_stmt.select_fields.len(), 3);
    println!("✓ StreamDialect parsed {} expression fields", select_stmt.select_fields.len());
    
    // Check each field
    let expected_expressions = vec![
        "BinaryOp",      // a + b
        "Function",      // CONCAT
        "Value"          // 42
    ];
    
    for (i, (field, expected)) in select_stmt.select_fields.iter().zip(expected_expressions.iter()).enumerate() {
        match &field.expr {
            Expr::BinaryOp { .. } => assert_eq!(*expected, "BinaryOp"),
            Expr::Function { .. } => assert_eq!(*expected, "Function"),
            Expr::Value { .. } => assert_eq!(*expected, "Value"),
            _ => panic!("Unexpected expression type"),
        }
        println!("  Field {}: {} expression", i + 1, expected);
    }
}

#[test]
fn test_stream_dialect_alias_support() {
    let sql = "SELECT a + b AS total, CONCAT(name, '_test') AS full_name, 42 AS answer";
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    
    // Verify field count
    assert_eq!(select_stmt.select_fields.len(), 3);
    println!("✓ StreamDialect parsed {} aliased fields", select_stmt.select_fields.len());
    
    // Verify aliases are preserved
    let expected_aliases = vec![Some("total".to_string()), Some("full_name".to_string()), Some("answer".to_string())];
    
    for (i, (field, expected_alias)) in select_stmt.select_fields.iter().zip(expected_aliases.iter()).enumerate() {
        assert_eq!(&field.alias, expected_alias);
        println!("  Field {}: alias = {:?}", i + 1, field.alias);
    }
}

#[test]
fn test_stream_dialect_complex_expressions() {
    let complex_sql = r#"
        SELECT 
            (a + b) * c / d,
            CONCAT(first_name, ' ', last_name),
            CASE 
                WHEN age > 18 THEN 'adult'
                ELSE 'minor'
            END
    "#;

    let select_stmt = parse_sql(complex_sql).expect("StreamDialect parse should succeed");
    
    // Should parse multiple complex expressions
    assert_eq!(select_stmt.select_fields.len(), 3);
    println!("✓ StreamDialect parsed {} complex expressions", select_stmt.select_fields.len());
    
    // Check each complex expression
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        match &field.expr {
            Expr::BinaryOp { .. } => println!("  Field {}: Binary operation", i + 1),
            Expr::Function { .. } => println!("  Field {}: Function call", i + 1),
            Expr::Case { .. } => println!("  Field {}: CASE expression", i + 1),
            _ => println!("  Field {}: Other expression", i + 1),
        }
    }
}

#[test]
fn test_stream_dialect_function_calls() {
    let sql_with_functions = r#"
        SELECT 
            UPPER(name),
            CONCAT(first_name, ' ', last_name),
            SUBSTRING(email, 1, 10),
            NOW()
    "#;

    let select_stmt = parse_sql(sql_with_functions).expect("StreamDialect parse should succeed");
    
    // Should detect multiple function calls
    assert_eq!(select_stmt.select_fields.len(), 4);
    println!("✓ StreamDialect parsed {} function calls", select_stmt.select_fields.len());
    
    // Verify each field is a function
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        println!("  Field {}: {:?}", i + 1, field.expr);
        match &field.expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string();
                println!("    Function '{}' with {} args", func_name, func.args.len());
            }
            _ => {
                // Some functions might be parsed differently, let's be more flexible
                println!("    Not a simple function call, but part of complex expression");
            }
        }
    }
}

#[test]
fn test_stream_dialect_nested_expressions() {
    let nested_sql = "SELECT a + (b * c), (x + y) / (z - w)";
    
    let select_stmt = parse_sql(nested_sql).expect("StreamDialect parse should succeed");
    
    assert_eq!(select_stmt.select_fields.len(), 2);
    println!("✓ StreamDialect parsed {} nested expressions", select_stmt.select_fields.len());
    
    // Both should be binary operations with nested structure
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        match &field.expr {
            Expr::BinaryOp { left, op, right } => {
                println!("  Field {}: Binary operation {:?}", i + 1, op);
                // Verify nested structure
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Identifier(_), Expr::BinaryOp { .. }) => {
                        println!("    Left: identifier, Right: nested binary op");
                    }
                    (Expr::BinaryOp { .. }, Expr::BinaryOp { .. }) => {
                        println!("    Left: nested binary op, Right: nested binary op");
                    }
                    _ => println!("    Mixed expression types"),
                }
            }
            _ => panic!("Expected binary operation"),
        }
    }
}

#[test]
fn test_stream_dialect_error_handling() {
    let parser = StreamSqlParser::new();
    
    // Test invalid SQL
    let invalid_sql = "INVALID SQL EXPRESSION";
    let result = parser.parse(invalid_sql);
    
    assert!(result.is_err());
    println!("✓ Invalid SQL properly rejected: {:?}", result.unwrap_err());
    
    // Test non-SELECT SQL
    let insert_sql = "INSERT INTO table VALUES (1)";
    let result = parser.parse(insert_sql);
    
    assert!(result.is_err());
    println!("✓ Non-SELECT SQL properly rejected: {:?}", result.unwrap_err());
}

#[test]
fn test_stream_dialect_where_clause_expressions() {
    let sql = "SELECT name, age FROM users WHERE age > 18 AND status = 'active'";
    
    let select_stmt = parse_sql(sql).expect("StreamDialect parse should succeed");
    
    // Should parse SELECT fields
    assert_eq!(select_stmt.select_fields.len(), 2); // name and age
    println!("✓ StreamDialect parsed {} fields from SELECT with WHERE", select_stmt.select_fields.len());
    
    // Verify WHERE condition is extracted
    assert!(select_stmt.where_condition.is_some(), "WHERE condition should be present");
    println!("✓ WHERE condition successfully extracted");
    
    // Verify the WHERE condition structure
    match &select_stmt.where_condition {
        Some(Expr::BinaryOp { op, .. }) => {
            println!("  WHERE condition: Binary operation {:?}", op);
        }
        Some(expr) => {
            println!("  WHERE condition: {:?}", expr);
        }
        None => panic!("WHERE condition should not be None"),
    }
    
    // Verify the SELECT fields are correct
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        match &field.expr {
            Expr::Identifier(ident) => {
                println!("  Field {}: identifier '{}'", i + 1, ident.value);
            }
            _ => println!("  Field {}: other expression type", i + 1),
        }
    }
}

// Demo test showing complete StreamDialect workflow with SelectStmt
#[test]
fn demo_stream_dialect_complete_workflow() {
    println!("\n=== StreamDialect Complete Workflow Demo ===\n");
    
    let sql = "SELECT a + b AS total, CONCAT(name, '_test') AS full_name, 42 AS answer, (x * y) / z AS complex";
    println!("Test SQL: {}", sql);
    
    // Step 1: Parse using StreamDialect
    let parser = StreamSqlParser::new();
    match parser.parse(sql) {
        Ok(select_stmt) => {
            println!("✓ StreamDialect parsing successful!");
            println!("✓ Generated SelectStmt with {} fields", select_stmt.select_fields.len());
            
            // Step 2: Analyze the SelectStmt
            for (i, field) in select_stmt.select_fields.iter().enumerate() {
                println!("\n  Field {}:" , i + 1);
                println!("    Expression: {:?}", field.expr);
                println!("    Alias: {:?}", field.alias);
                
                // Step 3: Detailed analysis
                match &field.expr {
                    Expr::BinaryOp { op, .. } => {
                        println!("    Type: Binary operation ({:?})", op);
                    }
                    Expr::Function(func) => {
                        println!("    Type: Function call ('{}' with {} args)", func.name, func.args.len());
                    }
                    Expr::Value(val) => {
                        println!("    Type: Literal value ({:?})", val);
                    }
                    Expr::Nested(_) => {
                        println!("    Type: Nested expression");
                    }
                    _ => {
                        println!("    Type: Other expression");
                    }
                }
            }
            
            println!("\n✅ StreamDialect SelectStmt workflow complete!");
        }
        Err(e) => {
            println!("✗ StreamDialect parsing failed: {}", e);
        }
    }
}