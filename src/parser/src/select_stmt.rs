use sqlparser::ast::Expr;
use std::collections::HashMap;

use crate::window::Window;

/// Represents information about a data source (table)
#[derive(Debug, Clone)]
pub struct SourceInfo {
    /// The name of the source (table name)
    pub name: String,
    /// Optional alias for the source
    pub alias: Option<String>,
}

/// Represents a SELECT statement with its fields, optional WHERE and HAVING clauses, and aggregate mappings
#[derive(Debug, Clone)]
pub struct SelectStmt {
    /// The select fields/expressions
    pub select_fields: Vec<SelectField>,
    /// Optional WHERE clause expression
    pub where_condition: Option<Expr>,
    /// Optional HAVING clause expression
    pub having: Option<Expr>,
    /// GROUP BY expressions, if any
    pub group_by_exprs: Vec<Expr>,
    /// Optional stream window declared in GROUP BY (tumbling/count)
    pub window: Option<Window>,
    /// Aggregate function mappings: column name -> original aggregate expression
    pub aggregate_mappings: HashMap<String, Expr>,
    /// Information about the data sources (tables) accessed
    pub source_infos: Vec<SourceInfo>,
}

/// Represents a single select field/expression
#[derive(Debug, Clone)]
pub struct SelectField {
    /// The expression for this field (from sqlparser AST)
    pub expr: Expr,
    /// Optional alias for this field
    pub alias: Option<String>,
    /// The exposed name for this field (alias or expression string)
    pub field_name: String,
}

impl SelectStmt {
    /// Create a new SelectStmt with empty fields and no WHERE/HAVING clauses
    pub fn new() -> Self {
        Self {
            select_fields: Vec::new(),
            where_condition: None,
            having: None,
            group_by_exprs: Vec::new(),
            window: None,
            aggregate_mappings: HashMap::new(),
            source_infos: Vec::new(),
        }
    }

    /// Create a new SelectStmt with given fields and no WHERE/HAVING clauses
    pub fn with_fields(select_fields: Vec<SelectField>) -> Self {
        Self {
            select_fields,
            where_condition: None,
            having: None,
            group_by_exprs: Vec::new(),
            window: None,
            aggregate_mappings: HashMap::new(),
            source_infos: Vec::new(),
        }
    }

    /// Create a new SelectStmt with given fields and WHERE/HAVING clauses
    pub fn with_fields_and_conditions(
        select_fields: Vec<SelectField>,
        where_condition: Option<Expr>,
        having: Option<Expr>,
    ) -> Self {
        Self {
            select_fields,
            where_condition,
            having,
            group_by_exprs: Vec::new(),
            window: None,
            aggregate_mappings: HashMap::new(),
            source_infos: Vec::new(),
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
    pub fn new(expr: Expr, alias: Option<String>, field_name: String) -> Self {
        Self {
            expr,
            alias,
            field_name,
        }
    }
}
