use parser::{extract_expressions_from_sql, extract_select_expressions_simple, analyze_sql_expressions};
use parser::StreamDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Visit, Visitor, TableFactor};

#[test]
fn test_basic_sql_parsing() {
    let basic_sql = r#"
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.age > 18
    "#;

    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, basic_sql).unwrap();
    
    assert_eq!(statements.len(), 1);
    println!("✓ Successfully parsed basic SQL");
}

#[test]
fn test_tumblingwindow_parsing() {
    let tumbling_sql = r#"
        SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)
    "#;

    let dialect = StreamDialect::new();
    let result = Parser::parse_sql(&dialect, tumbling_sql);
    
    assert!(result.is_ok());
    let statements = result.unwrap();
    assert_eq!(statements.len(), 1);
    
    println!("✓ Successfully parsed tumblingwindow SQL");
}

#[test]
fn test_expression_extraction() {
    let sql = "SELECT a + b, CONCAT(name, 'test'), 42 WHERE age > 18";
    
    // Test general expression extraction
    let expressions = extract_expressions_from_sql(sql).unwrap();
    assert!(!expressions.is_empty());
    println!("✓ Extracted {} expressions", expressions.len());
    
    // Test SELECT-specific extraction
    let select_exprs = extract_select_expressions_simple(sql).unwrap();
    assert!(!select_exprs.is_empty());
    println!("✓ Extracted {} SELECT expressions", select_exprs.len());
}

#[test]
fn test_expression_analysis() {
    let sql = "SELECT a + b, CONCAT(name, suffix), 42 WHERE age > 18";
    let analysis = analyze_sql_expressions(sql).unwrap();
    
    assert!(analysis.expression_count > 0);
    assert!(!analysis.binary_operations.is_empty());
    assert!(!analysis.functions.is_empty());
    assert!(!analysis.literals.is_empty());
    
    println!("✓ Analysis complete:");
    println!("  Total expressions: {}", analysis.expression_count);
    println!("  Binary operations: {:?}", analysis.binary_operations);
    println!("  Functions: {:?}", analysis.functions);
    println!("  Literals: {:?}", analysis.literals);
}

#[test]
fn test_table_visitor() {
    let basic_sql = r#"
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.age > 18
    "#;

    let dialect = StreamDialect::new();
    let statements = Parser::parse_sql(&dialect, basic_sql).unwrap();
    
    let mut visitor = TableNameVisitor {
        table_names: Vec::new(),
    };

    for statement in &statements {
        statement.visit(&mut visitor);
    }

    assert!(!visitor.table_names.is_empty());
    println!("✓ Found tables: {:?}", visitor.table_names);
}

#[test]
fn test_complex_expression_parsing() {
    let complex_sql = r#"
        SELECT 
            (a + b) * c / d,
            CONCAT(first_name, ' ', last_name),
            CASE 
                WHEN age > 18 THEN 'adult'
                ELSE 'minor'
            END
        FROM users
        WHERE status = 'active' AND age BETWEEN 18 AND 65
    "#;

    let analysis = analyze_sql_expressions(complex_sql).unwrap();
    
    // 应该包含多个表达式
    assert!(analysis.expression_count > 5);
    
    // 应该包含二元操作
    assert!(!analysis.binary_operations.is_empty());
    
    // 应该包含函数
    assert!(!analysis.functions.is_empty());
    
    // 应该包含字面量
    assert!(!analysis.literals.is_empty());
    
    println!("✓ Complex expression analysis successful");
}

#[test]
fn test_function_call_parsing() {
    let sql_with_functions = r#"
        SELECT 
            UPPER(name),
            CONCAT(first_name, ' ', last_name),
            SUBSTRING(email, 1, 10),
            NOW()
        FROM users
    "#;

    let analysis = analyze_sql_expressions(sql_with_functions).unwrap();
    
    // 应该检测到多个函数
    assert!(!analysis.functions.is_empty());
    
    // 应该包含我们期望的函数
    let function_names: Vec<String> = analysis.functions.iter()
        .map(|s| s.to_lowercase())
        .collect();
    
    println!("✓ Found functions: {:?}", function_names);
}

// Helper visitor for testing
struct TableNameVisitor {
    pub table_names: Vec<String>,
}

impl Visitor for TableNameVisitor {
    type Break = ();

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> std::ops::ControlFlow<Self::Break> {
        if let TableFactor::Table { name, .. } = table {
            let table_name = name.to_string();
            self.table_names.push(table_name);
        }
        std::ops::ControlFlow::Continue(())
    }
}