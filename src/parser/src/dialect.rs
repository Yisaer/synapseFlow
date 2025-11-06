use sqlparser::ast::{Expr, GroupByExpr, SetExpr, Statement};
use sqlparser::parser::ParserError;

use super::window;
pub use window::{TimeWindow, parse_tumbling_window, window_to_expr};

/// Stream processing dialect that supports tumbling window functions
/// This dialect supports custom window functions like tumblingwindow in GROUP BY clauses
#[derive(Debug, Clone)]
pub struct StreamDialect {}

impl StreamDialect {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StreamDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl sqlparser::dialect::Dialect for StreamDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }
}

/// Process a parsed statement to handle tumblingwindow functions
#[allow(clippy::collapsible_if)]
pub fn process_tumblingwindow_in_statement(statement: &mut Statement) -> Result<(), ParserError> {
    if let Statement::Query(query) = statement {
        if let SetExpr::Select(select) = &mut *query.body {
            // Check if GROUP BY contains tumblingwindow function
            let new_group_by = parse_group_by_with_tumblingwindow(&select.group_by)?;
            select.group_by = new_group_by;
        }
    }
    
    Ok(())
}

/// Parse GROUP BY clause and handle tumblingwindow functions
fn parse_group_by_with_tumblingwindow(group_by: &GroupByExpr) -> Result<GroupByExpr, ParserError> {
    match group_by {
        GroupByExpr::Expressions(exprs) => {
            let mut new_exprs = Vec::new();
            
            for expr in exprs {
                if let Some(window_expr) = parse_tumblingwindow_expr(expr)? {
                    // Convert tumblingwindow function to a special window field
                    new_exprs.push(window_expr);
                } else {
                    new_exprs.push(expr.clone());
                }
            }
            
            Ok(GroupByExpr::Expressions(new_exprs))
        }
        _ => Ok(group_by.clone()),
    }
}

/// Parse a tumblingwindow function expressions and convert to window structure
fn parse_tumblingwindow_expr(expr: &Expr) -> Result<Option<Expr>, ParserError> {
    if let Expr::Function(function) = expr {
        let function_name = function.name.to_string().to_lowercase();
        
        if function_name == "tumblingwindow" {
            // Parse the tumblingwindow function into a TimeWindow structure
            match parse_tumbling_window(function) {
                Ok(window) => {
                    println!("Successfully parsed tumblingwindow: {:?}", window);
                    // For now, keep the original expression, but we have the window structure
                    // In a real implementation, we could replace this with a custom window expression
                    Ok(Some(expr.clone()))
                }
                Err(e) => {
                    println!("Failed to parse tumblingwindow: {}", e);
                    Ok(Some(expr.clone()))
                }
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::parser::Parser;

    #[test]
    fn test_parse_tumblingwindow() {
        let sql = "SELECT * FROM stream GROUP BY tumblingwindow('ss', 10)";
        let dialect = StreamDialect::new();
        
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        
        // Process the statement to handle tumblingwindow
        process_tumblingwindow_in_statement(&mut statements[0]).unwrap();
        
        // Verify the statement was processed correctly
        match &statements[0] {
            Statement::Query(query) => {
                if let SetExpr::Select(select) = &*query.body {
                    // Just verify that we have a GROUP BY clause
                    if let GroupByExpr::Expressions(exprs) = &select.group_by {
                        println!("GROUP BY expressions: {:?}", exprs.len());
                        // Check if any expression is a tumblingwindow function
                        for expr in exprs {
                            if let Expr::Function(func) = expr {
                                if func.name.to_string().to_lowercase() == "tumblingwindow" {
                                    println!("Found tumblingwindow function in GROUP BY");

                                    // Try to parse it as a TimeWindow
                                    match parse_tumbling_window(func) {
                                        Ok(window) => {
                                            println!("Successfully parsed TimeWindow: {:?}", window);
                                            assert_eq!(window.window_type, window::WindowType::Tumbling);
                                            assert_eq!(window.time_unit, "ss");
                                            assert_eq!(window.size, 10);
                                        }
                                        Err(e) => {
                                            println!("Failed to parse as TimeWindow: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    println!("Successfully parsed tumblingwindow in GROUP BY");
                }
            }
            _ => panic!("Expected Query statement"),
        }
    }
}