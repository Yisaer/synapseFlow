use sqlparser::ast::Expr;
use std::collections::HashMap;

/// Represents a SELECT statement with its fields, optional WHERE and HAVING clauses, and aggregate mappings
#[derive(Debug, Clone)]
pub struct SelectStmt {
    /// The select fields/expressions
    pub select_fields: Vec<SelectField>,
    /// Optional WHERE clause expression
    pub where_condition: Option<Expr>,
    /// Optional HAVING clause expression
    pub having: Option<Expr>,
    /// Aggregate function mappings: column name -> original aggregate expression
    pub aggregate_mappings: HashMap<String, Expr>,
}

/// Represents a single select field/expression
#[derive(Debug, Clone)]
pub struct SelectField {
    /// The expression for this field (from sqlparser AST)
    pub expr: Expr,
    /// Optional alias for this field
    pub alias: Option<String>,
}

impl SelectStmt {
    /// Create a new SelectStmt with empty fields and no WHERE/HAVING clauses
    pub fn new() -> Self {
        Self {
            select_fields: Vec::new(),
            where_condition: None,
            having: None,
            aggregate_mappings: HashMap::new(),
        }
    }

    /// Create a new SelectStmt with given fields and no WHERE/HAVING clauses
    pub fn with_fields(select_fields: Vec<SelectField>) -> Self {
        Self { 
            select_fields, 
            where_condition: None,
            having: None,
            aggregate_mappings: HashMap::new(),
        }
    }

    /// Create a new SelectStmt with given fields and WHERE/HAVING clauses
    pub fn with_fields_and_conditions(select_fields: Vec<SelectField>, where_condition: Option<Expr>, having: Option<Expr>) -> Self {
        Self { 
            select_fields, 
            where_condition,
            having,
            aggregate_mappings: HashMap::new(),
        }
    }
}

impl Default for SelectStmt {
    fn default() -> Self {
        Self::new()
    }
}

impl SelectField {
    /// Create a new SelectField
    pub fn new(expr: Expr, alias: Option<String>) -> Self {
        Self { expr, alias }
    }
}
