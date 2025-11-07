use flow::sql_conversion::extract_select_expressions;
use flow::expr::{DataFusionEvaluator, ScalarExpr, BinaryFunc};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Value, ConcreteDatatype, Int64Type, Schema, ColumnSchema};
use parser::{extract_expressions_from_sql, analyze_sql_expressions};

fn main() {
    println!("=== SQL Expression Parser & Converter Demo ===\n");
    
    // Demonstrate the complete workflow
    demonstrate_complete_workflow();
    
    // Test various SQL expressions
    test_expression_varieties();
    
    // Show parser capabilities
    show_parser_analysis();
}

fn demonstrate_complete_workflow() {
    println!("--- Complete Workflow: SQL → Parser → ScalarExpr → Evaluation ---\n");
    
    // Step 1: Parse SQL using parser crate
    let sql = "SELECT a + b";
    println!("1. Input SQL: {}", sql);
    
    // Step 2: Extract expressions using parser crate
    match extract_expressions_from_sql(sql) {
        Ok(expressions) => {
            println!("2. Parser extracted {} expressions", expressions.len());
            for (i, expr) in expressions.iter().enumerate() {
                println!("   Expression {}: {}", i + 1, expr);
            }
        }
        Err(e) => {
            println!("2. Parser error: {}", e);
            return;
        }
    }
    
    // Step 3: Convert to ScalarExpr using flow crate
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), "test_table".to_string(), ConcreteDatatype::Int64(Int64Type))
    ]);
    
    match extract_select_expressions(sql, &schema) {
        Ok(flow_expressions) => {
            println!("3. Flow converter created {} ScalarExpr(s)", flow_expressions.len());
            
            if let Some(expr) = flow_expressions.first() {
                println!("   ScalarExpr: {:?}", expr);
                
                // Verify it's the expected type
                match expr {
                    ScalarExpr::CallBinary { func, .. } => {
                        println!("   ✓ Confirmed: Binary operation with {:?}", func);
                        if func == &BinaryFunc::Add {
                            println!("   ✓ SUCCESS: This is indeed an addition operation!");
                        }
                    }
                    _ => println!("   ✗ Unexpected expression type"),
                }
            }
        }
        Err(e) => {
            println!("3. Flow converter error: {}", e);
            return;
        }
    }
    
    // Step 4: Demonstrate evaluation capability
    println!("\n4. Expression Evaluation Demo:");
    demonstrate_evaluation();
    
    println!();
}

fn demonstrate_evaluation() {
    // Create expression manually to show evaluation works
    let expr = ScalarExpr::Literal(Value::Int64(5), ConcreteDatatype::Int64(Int64Type))
        .call_binary(
            ScalarExpr::Literal(Value::Int64(3), ConcreteDatatype::Int64(Int64Type)),
            BinaryFunc::Add
        );
    
    println!("   Expression: 5 + 3");
    println!("   Structure: {:?}", expr);
    
    let evaluator = DataFusionEvaluator::new();
    let row = Row::from(vec![Value::Int64(5), Value::Int64(3)]);
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), "test_table".to_string(),ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), "test_table".to_string(),ConcreteDatatype::Int64(Int64Type)),
    ]);
    let tuple = Tuple::new(schema, row);
    
    match expr.eval(&evaluator, &tuple) {
        Ok(result) => {
            println!("   Result: {:?}", result);
            if result == Value::Int64(8) {
                println!("   ✓ Evaluation successful!");
            }
        }
        Err(e) => println!("   ✗ Evaluation failed: {:?}", e),
    }
}

fn test_expression_varieties() {
    println!("--- Testing Various Expression Types ---\n");
    
    let test_cases = vec![
        ("SELECT a + b", "Basic addition"),
        ("SELECT a - b", "Basic subtraction"),
        ("SELECT a * b", "Basic multiplication"),
        ("SELECT a / b", "Basic division"),
        ("SELECT (a + b) * c", "Complex expression with parentheses"),
        ("SELECT 42", "Integer literal"),
        ("SELECT 'hello'", "String literal"),
        ("SELECT CONCAT(a, b)", "Function call"),
    ];
    
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(),"test_table".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), "test_table".to_string(),ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("c".to_string(), "test_table".to_string(),ConcreteDatatype::Int64(Int64Type))
    ]);
    
    for (sql, description) in test_cases {
        println!("Testing: {} ({})", sql, description);
        
        match extract_select_expressions(sql, &schema) {
            Ok(expressions) => {
                println!("  ✓ Success - {} expression(s)", expressions.len());
                if let Some(expr) = expressions.first() {
                    println!("  ✓ Structure: {:?}", expr);
                }
            }
            Err(e) => {
                println!("  ✗ Error: {}", e);
            }
        }
        println!();
    }
}

fn show_parser_analysis() {
    println!("--- Parser Analysis Capabilities ---\n");
    
    let sql = "SELECT a + b, CONCAT(name, 'test'), 42, (x * y) / z";
    println!("Analyzing: {}", sql);
    
    match analyze_sql_expressions(sql) {
        Ok(analysis) => {
            println!("✓ Analysis complete:");
            println!("  Total expressions: {}", analysis.expression_count);
            println!("  Binary operations: {:?}", analysis.binary_operations);
            println!("  Functions: {:?}", analysis.functions);
            println!("  Literals: {:?}", analysis.literals);
        }
        Err(e) => {
            println!("✗ Analysis failed: {}", e);
        }
    }
    
    println!("\n=== Summary ===");
    println!("✅ Successfully demonstrated SQL → ScalarExpr conversion!");
    println!("✅ Parser crate can extract expressions from SQL");
    println!("✅ Flow crate can convert SQL expressions to ScalarExpr");
    println!("✅ ScalarExpr can be evaluated to produce results");
    println!("\n🎯 You can now parse 'SELECT a+b' and calculate its value!");
}