use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;

fn main() {
    println!("=== SQL Expression Parsing Test ===\n");
    
    // Test simple expression
    let sql = "SELECT a + b";
    println!("Testing: {}", sql);
    
    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => {
            println!("✓ Parsed successfully!");
            println!("✓ Statements: {}", statements.len());
            
            // Let's look at the first statement
            if let Some(stmt) = statements.first() {
                println!("✓ Statement: {:?}", stmt);
            }
        }
        Err(e) => {
            println!("✗ Parse error: {}", e);
        }
    }
    
    println!("\n=== More Complex Expression ===");
    let complex_sql = "SELECT (a + b) * c / 2";
    println!("Testing: {}", complex_sql);
    
    match Parser::parse_sql(&dialect, complex_sql) {
        Ok(statements) => {
            println!("✓ Parsed successfully!");
            if let Some(stmt) = statements.first() {
                println!("✓ Statement: {:?}", stmt);
            }
        }
        Err(e) => {
            println!("✗ Parse error: {}", e);
        }
    }
}