use crate::aggregation::AggregateFunctionRegistry;
use crate::expr::sql_conversion::{convert_expr_to_scalar_with_bindings, SchemaBinding};
use crate::expr::ScalarExpr;
use crate::planner::physical::BasePhysicalPlan;
use sqlparser::ast::Expr;
use sqlparser::ast::{FunctionArg, FunctionArgExpr};
use std::collections::HashMap;
use std::sync::Arc;

use super::PhysicalPlan;

#[derive(Debug, Clone)]
pub struct AggregateCall {
    pub output_column: String,
    pub func_name: String,
    pub args: Vec<ScalarExpr>,
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct PhysicalAggregation {
    pub base: BasePhysicalPlan,
    pub aggregate_mappings: HashMap<String, Expr>,
    pub aggregate_calls: Vec<AggregateCall>,
}

impl PhysicalAggregation {
    pub fn new(
        aggregate_mappings: HashMap<String, Expr>,
        children: Vec<Arc<PhysicalPlan>>,
        index: i64,
        bindings: &SchemaBinding,
        aggregate_registry: &AggregateFunctionRegistry,
    ) -> Result<Self, String> {
        let mut aggregate_calls = Vec::new();
        for (output_column, expr) in &aggregate_mappings {
            let call = build_aggregate_call(output_column, expr, bindings, aggregate_registry)?;
            aggregate_calls.push(call);
        }

        Ok(Self {
            base: BasePhysicalPlan::new(children, index),
            aggregate_mappings,
            aggregate_calls,
        })
    }
}

fn build_aggregate_call(
    output_column: &str,
    expr: &Expr,
    bindings: &SchemaBinding,
    aggregate_registry: &AggregateFunctionRegistry,
) -> Result<AggregateCall, String> {
    match expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_lowercase();
            if !aggregate_registry.is_registered(&func_name) {
                return Err(format!("Unsupported aggregate function: {}", func_name));
            }

            let args = extract_aggregate_args(func.args.as_slice(), bindings).map_err(|err| {
                format!(
                    "Failed to compile aggregate argument for {}: {}",
                    output_column, err
                )
            })?;

            Ok(AggregateCall {
                output_column: output_column.to_string(),
                func_name,
                args,
                distinct: func.distinct,
            })
        }
        other => Err(format!(
            "Expected aggregate function expression, got: {:?}",
            other
        )),
    }
}

fn extract_aggregate_args(
    args: &[FunctionArg],
    bindings: &SchemaBinding,
) -> Result<Vec<ScalarExpr>, String> {
    let mut compiled_args = Vec::new();
    for arg in args {
        let expr = function_arg_to_expr(arg)?;
        compiled_args.push(convert_expr_to_scalar_with_bindings(&expr, bindings).map_err(|err| err.to_string())?);
    }
    Ok(compiled_args)
}

fn function_arg_to_expr(arg: &FunctionArg) -> Result<Expr, String> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            // Represent wildcard as an identifier that the scalar converter treats as wildcard.
            Ok(Expr::Identifier(sqlparser::ast::Ident::new("*")))
        }
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
            Err("Qualified wildcard aggregate arguments are not supported".to_string())
        }
        FunctionArg::Named { .. } => Err("Named function arguments are not supported".to_string()),
    }
}
