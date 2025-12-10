//! Aggregate function transformer for parser module
//! Uses visitor pattern for efficient aggregate function detection and transformation
//! Converts SQL like: SELECT sum(a) + 1 FROM t GROUP BY b HAVING sum(c) > 0
//! To: SELECT col_1 + 1 FROM t GROUP BY b HAVING col_2 > 0
//! And records the mapping: sum(a) -> col_1, sum(c) -> col_2

use crate::select_stmt::SelectStmt;
use crate::visitor::extract_aggregates_with_visitor;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Ident};
use std::collections::HashMap;

/// Transform aggregate functions in a SELECT statement and return aggregate mappings
/// Uses visitor pattern for efficient traversal - now with direct in-place replacement!
/// Returns: HashMap where key is replacement column name (e.g., "col_1") and value is original aggregate expression
pub fn transform_aggregate_functions(
    mut select_stmt: SelectStmt,
) -> Result<(SelectStmt, HashMap<String, Expr>), String> {
    let mut all_aggregates = HashMap::new();
    let mut replacement_counter = 0;

    // Process select fields: extract aggregates and replace in one step
    for field in &mut select_stmt.select_fields {
        let (new_expr, field_aggregates) =
            extract_and_replace_aggregates(&field.expr, &mut replacement_counter)?;

        if !field_aggregates.is_empty() && field.alias.is_none() {
            field.alias = Some(field.expr.to_string());
        }
        // Update the field expression
        field.expr = new_expr;

        // Add field aggregates to global collection
        for (replacement_name, original_expr) in field_aggregates {
            all_aggregates.insert(replacement_name, original_expr);
        }
    }

    // Process HAVING clause: extract aggregates and replace in one step
    if let Some(having_expr) = &mut select_stmt.having {
        let (new_having, having_aggregates) =
            extract_and_replace_aggregates(having_expr, &mut replacement_counter)?;

        // Update the having expression
        *having_expr = new_having;

        // Add having aggregates to global collection
        for (replacement_name, original_expr) in having_aggregates {
            all_aggregates.insert(replacement_name, original_expr);
        }
    }

    // Store the aggregate mappings in the SelectStmt
    select_stmt.aggregate_mappings = all_aggregates.clone();

    Ok((select_stmt, all_aggregates))
}

/// Extract aggregates from an expression and replace them in one step
fn extract_and_replace_aggregates(
    expr: &Expr,
    replacement_counter: &mut usize,
) -> Result<(Expr, HashMap<String, Expr>), String> {
    // First, extract all aggregates from this expression
    let aggregates = extract_aggregates_with_visitor(expr);

    if aggregates.is_empty() {
        return Ok((expr.clone(), HashMap::new()));
    }

    // Create replacement mapping for this expression
    let mut replacement_mapping = HashMap::new();
    let mut new_aggregates = HashMap::new();

    for (_old_replacement_name, (_func_name, original_expr)) in aggregates {
        *replacement_counter += 1;
        let new_replacement_name = format!("col_{}", replacement_counter);

        // Build mapping: original_expr -> new_replacement_name
        replacement_mapping.insert(format!("{:?}", original_expr), new_replacement_name.clone());

        // Store the aggregate mapping
        new_aggregates.insert(new_replacement_name, original_expr.clone());
    }

    // Now replace aggregates in the expression using the mapping
    let new_expr = replace_aggregates_in_expression(expr, &replacement_mapping)?;

    Ok((new_expr, new_aggregates))
}

/// Replace aggregates in an expression while preserving structure
fn replace_aggregates_in_expression(
    expr: &Expr,
    mapping: &HashMap<String, String>,
) -> Result<Expr, String> {
    match expr {
        // If this is an aggregate function, replace it
        Expr::Function(func) if is_aggregate_function(&func.name.to_string()) => {
            let expr_str = format!("{:?}", expr);
            if let Some(replacement_name) = mapping.get(&expr_str) {
                Ok(Expr::Identifier(Ident::new(replacement_name)))
            } else {
                // This aggregate was not in our mapping (shouldn't happen)
                Err(format!("No replacement found for aggregate: {}", expr_str))
            }
        }

        // For binary operations, recursively replace in left and right
        Expr::BinaryOp { left, op, right } => {
            let new_left = replace_aggregates_in_expression(left, mapping)?;
            let new_right = replace_aggregates_in_expression(right, mapping)?;
            Ok(Expr::BinaryOp {
                left: Box::new(new_left),
                op: op.clone(),
                right: Box::new(new_right),
            })
        }

        // For unary operations, recursively replace in the inner expression
        Expr::UnaryOp {
            op,
            expr: inner_expr,
        } => {
            let new_inner = replace_aggregates_in_expression(inner_expr, mapping)?;
            Ok(Expr::UnaryOp {
                op: *op,
                expr: Box::new(new_inner),
            })
        }

        // For nested expressions, recursively replace
        Expr::Nested(inner_expr) => {
            let new_inner = replace_aggregates_in_expression(inner_expr, mapping)?;
            Ok(Expr::Nested(Box::new(new_inner)))
        }

        // For BETWEEN expressions, recursively replace in all parts
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let new_expr = replace_aggregates_in_expression(expr, mapping)?;
            let new_low = replace_aggregates_in_expression(low, mapping)?;
            let new_high = replace_aggregates_in_expression(high, mapping)?;
            Ok(Expr::Between {
                expr: Box::new(new_expr),
                negated: *negated,
                low: Box::new(new_low),
                high: Box::new(new_high),
            })
        }

        // For IN expressions, recursively replace in the expression and list
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let new_expr = replace_aggregates_in_expression(expr, mapping)?;
            let new_list: Result<Vec<_>, _> = list
                .iter()
                .map(|item| replace_aggregates_in_expression(item, mapping))
                .collect();
            Ok(Expr::InList {
                expr: Box::new(new_expr),
                list: new_list?,
                negated: *negated,
            })
        }

        // For function calls that are not aggregates, check their arguments
        Expr::Function(func) => {
            let mut new_args = Vec::new();
            for arg in &func.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) = arg {
                    let new_inner = replace_aggregates_in_expression(inner_expr, mapping)?;
                    new_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(new_inner)));
                } else {
                    new_args.push(arg.clone());
                }
            }

            let mut new_func = func.clone();
            new_func.args = new_args;
            Ok(Expr::Function(new_func))
        }

        // For other expressions, return as-is
        _ => Ok(expr.clone()),
    }
}

/// Helper function to identify aggregate functions
fn is_aggregate_function(function_name: &str) -> bool {
    let aggregates = ["sum", "count", "avg", "min", "max", "stddev", "variance"];
    aggregates.contains(&function_name.to_lowercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelectField;
    use sqlparser::ast::FunctionArg;
    use sqlparser::ast::FunctionArgExpr;
    use sqlparser::ast::ObjectName;
    use sqlparser::ast::{Function, Ident};

    #[test]
    fn test_simple_aggregate_transformation() {
        // Create a simple SELECT with aggregate
        let expr = Expr::Function(Function {
            name: ObjectName(vec![Ident::new("sum")]),
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                Expr::Identifier(Ident::new("a")),
            ))],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        });

        let select_field = SelectField::new(expr.clone(), None, expr.to_string());
        let select_stmt = SelectStmt::with_fields(vec![select_field]);

        // Transform aggregate functions (now with direct in-place replacement!)
        let (transformed_stmt, aggregate_mappings) =
            transform_aggregate_functions(select_stmt).expect("Should transform successfully");

        // Verify results
        assert_eq!(aggregate_mappings.len(), 1);
        assert!(aggregate_mappings.contains_key("col_1"));

        match &transformed_stmt.select_fields[0].expr {
            Expr::Identifier(ident) => {
                assert_eq!(ident.value, "col_1", "Should be replaced with col_1");
                println!("✓ Basic aggregate transformation with direct replacement successful");
            }
            _ => panic!("Expected identifier expression after transformation"),
        }

        // Verify SelectStmt contains aggregate mappings
        assert_eq!(transformed_stmt.aggregate_mappings.len(), 1);
        assert!(transformed_stmt.aggregate_mappings.contains_key("col_1"));
    }
}
