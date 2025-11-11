//! Table information visitor for parser module
//! Uses sqlparser's visitor pattern to extract table information from SQL statements

use sqlparser::ast::{Visitor, TableFactor};
use std::ops::ControlFlow;
use crate::select_stmt::SourceInfo;

/// Visitor that collects table (source) information from SQL statements
pub struct TableInfoVisitor {
    /// Found table sources: (table_name, alias)
    pub sources: Vec<SourceInfo>,
}

impl TableInfoVisitor {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    /// Get the collected source information
    pub fn get_sources(self) -> Vec<SourceInfo> {
        self.sources
    }
}

impl Visitor for TableInfoVisitor {
    type Break = ();

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                
                self.sources.push(SourceInfo {
                    name: table_name,
                    alias: alias_name,
                });
            }
            _ => {
                // For now, skip non-table sources (subqueries, etc.)
                // Could be extended in the future
            }
        }
        
        ControlFlow::Continue(())
    }
}

impl Default for TableInfoVisitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::parser::Parser;
    use crate::dialect::StreamDialect;
    use sqlparser::ast::{Statement, Visit};

    #[test]
    fn test_extract_single_table() {
        let sql = "SELECT * FROM users";
        let dialect = StreamDialect::new();
        let ast = Parser::parse_sql(&dialect, sql).unwrap();
        
        let mut visitor = TableInfoVisitor::new();
        ast[0].visit(&mut visitor);
        
        let sources = visitor.get_sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "users");
        assert_eq!(sources[0].alias, None);
    }
    
    #[test]
    fn test_extract_table_with_alias() {
        let sql = "SELECT * FROM users AS u";
        let dialect = StreamDialect::new();
        let ast = Parser::parse_sql(&dialect, sql).unwrap();
        
        let mut visitor = TableInfoVisitor::new();
        ast[0].visit(&mut visitor);
        
        let sources = visitor.get_sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "users");
        assert_eq!(sources[0].alias, Some("u".to_string()));
    }
    
    #[test]
    fn test_extract_multiple_tables() {
        let sql = "SELECT * FROM users AS u, orders AS o";
        let dialect = StreamDialect::new();
        let ast = Parser::parse_sql(&dialect, sql).unwrap();
        
        let mut visitor = TableInfoVisitor::new();
        ast[0].visit(&mut visitor);
        
        let sources = visitor.get_sources();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].name, "users");
        assert_eq!(sources[0].alias, Some("u".to_string()));
        assert_eq!(sources[1].name, "orders");
        assert_eq!(sources[1].alias, Some("o".to_string()));
    }
}
