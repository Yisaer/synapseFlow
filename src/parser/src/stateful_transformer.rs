use crate::aggregate_registry::AggregateRegistry;
use crate::col_placeholder_allocator::ColPlaceholderAllocator;
use crate::select_stmt::SelectStmt;
use crate::stateful_call::{StatefulCallSpec, StatefulMappingEntry};
use crate::stateful_registry::StatefulRegistry;
use crate::stateful_validation::validate_stateful_context_expr;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, Ident, UnaryOperator, Value, WindowSpec,
    WindowType,
};
use std::collections::HashMap;
use std::sync::Arc;

pub fn transform_stateful_functions(
    mut select_stmt: SelectStmt,
    aggregate_registry: Arc<dyn AggregateRegistry>,
    stateful_registry: Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
) -> Result<(SelectStmt, HashMap<String, StatefulCallSpec>), String> {
    let mut mappings: HashMap<String, StatefulCallSpec> = HashMap::new();
    let mut ordered_columns = Vec::new();
    let mut seen: HashMap<String, String> = HashMap::new();

    for field in &mut select_stmt.select_fields {
        field.expr = rewrite_expr_stateful(
            &field.expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
            &mut ordered_columns,
        )?;
    }

    if let Some(where_expr) = &mut select_stmt.where_condition {
        *where_expr = rewrite_expr_stateful(
            where_expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
            &mut ordered_columns,
        )?;
    }

    for item in &mut select_stmt.order_by {
        item.expr = rewrite_expr_stateful(
            &item.expr,
            &aggregate_registry,
            &stateful_registry,
            allocator,
            &mut seen,
            &mut mappings,
            &mut ordered_columns,
        )?;
    }

    select_stmt.stateful_mappings = ordered_columns
        .iter()
        .map(|output_column| {
            mappings
                .get(output_column)
                .ok_or_else(|| {
                    format!("stateful mapping missing for output column '{output_column}'")
                })
                .map(|spec| StatefulMappingEntry {
                    output_column: output_column.clone(),
                    spec: spec.clone(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((select_stmt, mappings))
}

fn rewrite_expr_stateful(
    expr: &Expr,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
    seen: &mut HashMap<String, String>,
    mappings: &mut HashMap<String, StatefulCallSpec>,
    ordered_columns: &mut Vec<String>,
) -> Result<Expr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_expr_stateful(
                left,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_expr_stateful(
                right,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
        }),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr_stateful(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr_stateful(
            inner,
            aggregate_registry,
            stateful_registry,
            allocator,
            seen,
            mappings,
            ordered_columns,
        )?))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(rewrite_expr_stateful(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
            negated: *negated,
            low: Box::new(rewrite_expr_stateful(
                low,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
            high: Box::new(rewrite_expr_stateful(
                high,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let rewritten_expr = rewrite_expr_stateful(
                expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?;
            let rewritten_list = list
                .iter()
                .map(|item| {
                    rewrite_expr_stateful(
                        item,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::InList {
                expr: Box::new(rewritten_expr),
                list: rewritten_list,
                negated: *negated,
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let rewritten_operand = match operand.as_ref() {
                Some(inner) => Some(Box::new(rewrite_expr_stateful(
                    inner,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                    ordered_columns,
                )?)),
                None => None,
            };

            let rewritten_conditions = conditions
                .iter()
                .map(|expr| {
                    rewrite_expr_stateful(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let rewritten_results = results
                .iter()
                .map(|expr| {
                    rewrite_expr_stateful(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let rewritten_else = match else_result.as_ref() {
                Some(inner) => Some(Box::new(rewrite_expr_stateful(
                    inner,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                    ordered_columns,
                )?)),
                None => None,
            };

            Ok(Expr::Case {
                operand: rewritten_operand,
                conditions: rewritten_conditions,
                results: rewritten_results,
                else_result: rewritten_else,
            })
        }
        Expr::Function(func) => {
            let rewritten_func = rewrite_function_stateful_context(
                func,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?;
            let func_name = func
                .name
                .0
                .last()
                .map(|ident| ident.value.to_lowercase())
                .unwrap_or_default();

            if stateful_registry.is_stateful_function(&func_name) {
                let call_spec = parse_stateful_call(
                    &rewritten_func,
                    expr,
                    stateful_registry,
                    aggregate_registry,
                )?;
                let key = call_spec.dedup_key();
                if let Some(col) = seen.get(&key) {
                    return Ok(Expr::Identifier(Ident::new(col)));
                }

                let col = allocator.allocate();
                seen.insert(key, col.clone());
                mappings.insert(col.clone(), call_spec);
                ordered_columns.push(col.clone());
                return Ok(Expr::Identifier(Ident::new(col)));
            }

            Ok(Expr::Function(rewritten_func))
        }
        _ => Ok(expr.clone()),
    }
}

fn rewrite_function_stateful_context(
    func: &Function,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    allocator: &mut ColPlaceholderAllocator,
    seen: &mut HashMap<String, String>,
    mappings: &mut HashMap<String, StatefulCallSpec>,
    ordered_columns: &mut Vec<String>,
) -> Result<Function, String> {
    let mut new_args = Vec::with_capacity(func.args.len());
    for arg in &func.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) => {
                let rewritten = rewrite_expr_stateful(
                    inner_expr,
                    aggregate_registry,
                    stateful_registry,
                    allocator,
                    seen,
                    mappings,
                    ordered_columns,
                )?;
                new_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(rewritten)));
            }
            FunctionArg::Named { name, arg } => {
                let rewritten_arg = match arg {
                    FunctionArgExpr::Expr(inner_expr) => {
                        FunctionArgExpr::Expr(rewrite_expr_stateful(
                            inner_expr,
                            aggregate_registry,
                            stateful_registry,
                            allocator,
                            seen,
                            mappings,
                            ordered_columns,
                        )?)
                    }
                    _ => arg.clone(),
                };
                new_args.push(FunctionArg::Named {
                    name: name.clone(),
                    arg: rewritten_arg,
                });
            }
            _ => new_args.push(arg.clone()),
        }
    }

    let new_filter = match func.filter.as_ref() {
        Some(expr) => Some(Box::new(rewrite_expr_stateful(
            expr,
            aggregate_registry,
            stateful_registry,
            allocator,
            seen,
            mappings,
            ordered_columns,
        )?)),
        None => None,
    };

    let new_over = match func.over.as_ref() {
        Some(WindowType::WindowSpec(spec)) => Some(WindowType::WindowSpec(WindowSpec {
            partition_by: spec
                .partition_by
                .iter()
                .map(|expr| {
                    rewrite_expr_stateful(
                        expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            order_by: spec
                .order_by
                .iter()
                .map(|item| {
                    let mut rewritten = item.clone();
                    rewritten.expr = rewrite_expr_stateful(
                        &item.expr,
                        aggregate_registry,
                        stateful_registry,
                        allocator,
                        seen,
                        mappings,
                        ordered_columns,
                    )?;
                    Ok(rewritten)
                })
                .collect::<Result<Vec<_>, String>>()?,
            window_frame: spec.window_frame.clone(),
        })),
        Some(WindowType::NamedWindow(name)) => Some(WindowType::NamedWindow(name.clone())),
        None => None,
    };

    let new_order_by = func
        .order_by
        .iter()
        .map(|item| {
            let mut rewritten = item.clone();
            rewritten.expr = rewrite_expr_stateful(
                &item.expr,
                aggregate_registry,
                stateful_registry,
                allocator,
                seen,
                mappings,
                ordered_columns,
            )?;
            Ok(rewritten)
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut new_func = func.clone();
    new_func.args = new_args;
    new_func.filter = new_filter;
    new_func.over = new_over;
    new_func.order_by = new_order_by;
    Ok(new_func)
}

fn parse_stateful_call(
    func: &Function,
    original_expr: &Expr,
    stateful_registry: &Arc<dyn StatefulRegistry>,
    aggregate_registry: &Arc<dyn AggregateRegistry>,
) -> Result<StatefulCallSpec, String> {
    let func_name = func
        .name
        .0
        .last()
        .map(|ident| ident.value.to_lowercase())
        .unwrap_or_default();

    if func.distinct {
        return Err(format!(
            "stateful function '{}' does not support DISTINCT",
            func_name
        ));
    }

    if !func.order_by.is_empty() {
        return Err(format!(
            "stateful function '{}' does not support ORDER BY",
            func_name
        ));
    }

    let mut args = Vec::with_capacity(func.args.len());
    for arg in &func.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                validate_stateful_context_expr(
                    expr,
                    stateful_registry,
                    aggregate_registry,
                    "arguments",
                )?;
                args.push(expr.clone());
            }
            _ => {
                return Err(format!(
                    "unsupported stateful function argument for {}: {}",
                    func_name, arg
                ));
            }
        }
    }

    let when = match func.filter.as_ref() {
        Some(expr) => {
            validate_stateful_context_expr(expr, stateful_registry, aggregate_registry, "FILTER")?;
            Some((**expr).clone())
        }
        None => None,
    };

    let partition_by = parse_stateful_over_partition_by(func, &func_name)?;
    for expr in &partition_by {
        validate_stateful_context_expr(
            expr,
            stateful_registry,
            aggregate_registry,
            "OVER PARTITION BY",
        )?;
    }

    validate_stateful_builtin_args(&func_name, &args)?;

    Ok(StatefulCallSpec {
        func_name,
        args,
        when,
        partition_by,
        original_expr: original_expr.clone(),
    })
}

fn parse_stateful_over_partition_by(func: &Function, func_name: &str) -> Result<Vec<Expr>, String> {
    let Some(over) = func.over.as_ref() else {
        return Ok(Vec::new());
    };

    match over {
        WindowType::WindowSpec(spec) => parse_stateful_window_spec_partition_by(spec, func_name),
        WindowType::NamedWindow(name) => Err(format!(
            "stateful function '{}' does not support named windows (got {})",
            func_name, name
        )),
    }
}

fn parse_stateful_window_spec_partition_by(
    spec: &WindowSpec,
    func_name: &str,
) -> Result<Vec<Expr>, String> {
    if !spec.order_by.is_empty() {
        return Err(format!(
            "stateful function '{}' OVER does not support ORDER BY",
            func_name
        ));
    }

    if spec.window_frame.is_some() {
        return Err(format!(
            "stateful function '{}' OVER does not support window frames",
            func_name
        ));
    }

    if spec.partition_by.is_empty() {
        return Err(format!(
            "stateful function '{}' OVER requires PARTITION BY expressions",
            func_name
        ));
    }

    Ok(spec.partition_by.clone())
}

fn validate_stateful_builtin_args(func_name: &str, args: &[Expr]) -> Result<(), String> {
    match func_name {
        "latest" if args.len() != 1 => {
            return Err(format!(
                "stateful function '{}' expects exactly 1 argument, got {}",
                func_name,
                args.len()
            ));
        }
        "latest" => {}
        "changed_col" => {
            if args.len() != 2 {
                return Err(format!(
                    "stateful function '{}' expects exactly 2 arguments, got {}",
                    func_name,
                    args.len()
                ));
            }
            validate_bool_literal(&args[0], "changed_col() first argument")?;
        }
        "had_changed" => {
            if args.len() < 2 {
                return Err(format!(
                    "stateful function '{}' expects at least 2 arguments, got {}",
                    func_name,
                    args.len()
                ));
            }
            validate_bool_literal(&args[0], "had_changed() first argument")?;
        }
        "change_to" => {
            if args.len() != 3 {
                return Err(format!(
                    "stateful function '{}' expects exactly 3 arguments, got {}",
                    func_name,
                    args.len()
                ));
            }
            validate_bool_literal(&args[0], "change_to() first argument")?;
        }
        "change_capture" => {
            if args.len() != 3 && args.len() != 4 {
                return Err(format!(
                    "stateful function '{}' expects 3 or 4 arguments, got {}",
                    func_name,
                    args.len()
                ));
            }
            validate_bool_literal(&args[0], "change_capture() first argument")?;
        }
        "consecutive_count" if args.len() != 1 => {
            return Err(format!(
                "stateful function '{}' expects exactly 1 argument, got {}",
                func_name,
                args.len()
            ));
        }
        "consecutive_count" => {}
        "consecutive_start" if args.len() != 2 => {
            return Err(format!(
                "stateful function '{}' expects exactly 2 arguments, got {}",
                func_name,
                args.len()
            ));
        }
        "consecutive_start" => {}
        "lag" => {
            if args.is_empty() || args.len() > 3 {
                return Err(format!(
                    "stateful function '{}' expects 1 to 3 arguments, got {}",
                    func_name,
                    args.len()
                ));
            }
            if let Some(offset) = args.get(1) {
                validate_positive_integer_literal(offset, "lag() second argument")?;
            }
            if let Some(ignore_null) = args.get(2) {
                validate_bool_literal(ignore_null, "lag() third argument")?;
            }
        }
        "acc_sum" | "acc_max" | "acc_min" | "acc_count" | "acc_avg"
            if args.len() != 1 && args.len() != 3 =>
        {
            return Err(format!(
                "stateful function '{}' expects either 1 argument or 3 arguments: {}(value[, begin_cond, reset_cond]), got {}",
                func_name,
                func_name,
                args.len()
            ));
        }
        "acc_sum" | "acc_max" | "acc_min" | "acc_count" | "acc_avg" => {}
        _ => {}
    }

    Ok(())
}

fn validate_positive_integer_literal(expr: &Expr, context: &str) -> Result<(), String> {
    let value = parse_integer_literal(expr)
        .ok_or_else(|| format!("{context} must be a positive integer literal, got {expr}"))?;
    if value < 1 {
        return Err(format!(
            "{context} must be a positive integer literal, got {expr}"
        ));
    }
    Ok(())
}

fn parse_integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(Value::Number(raw, _)) => raw.parse::<i64>().ok(),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(Value::Number(raw, _)) => raw.parse::<i64>().ok().map(|v| -v),
            _ => None,
        },
        _ => None,
    }
}

fn validate_bool_literal(expr: &Expr, context: &str) -> Result<(), String> {
    match expr {
        Expr::Value(Value::Boolean(_)) => Ok(()),
        _ => Err(format!("{context} must be a boolean literal, got {expr}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelectField;
    use crate::aggregate_registry::default_aggregate_registry;
    use crate::stateful_registry::StaticStatefulRegistry;
    use sqlparser::ast::{Function, ObjectName};

    fn lag_expr(arg: Expr) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("lag")]),
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    #[test]
    fn deduplicates_stateful_calls_in_select() {
        let registry = Arc::new(StaticStatefulRegistry::new(["lag"]));
        let mut allocator = ColPlaceholderAllocator::new();
        let expr = lag_expr(Expr::Identifier(Ident::new("a")));
        let select_stmt = SelectStmt::with_fields(vec![
            SelectField::new(expr.clone(), None, expr.to_string()),
            SelectField::new(expr.clone(), None, expr.to_string()),
        ]);

        let (out, mappings) = transform_stateful_functions(
            select_stmt,
            default_aggregate_registry(),
            registry,
            &mut allocator,
        )
        .unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(out.stateful_mappings.len(), 1);
        assert_eq!(out.select_fields[0].expr.to_string(), "col_1");
        assert_eq!(out.select_fields[1].expr.to_string(), "col_1");
    }

    #[test]
    fn rewrites_stateful_in_where() {
        let registry = Arc::new(StaticStatefulRegistry::new(["lag"]));
        let mut allocator = ColPlaceholderAllocator::new();
        let lag_a = lag_expr(Expr::Identifier(Ident::new("a")));

        let mut select_stmt = SelectStmt::with_fields(vec![SelectField::new(
            Expr::Identifier(Ident::new("a")),
            None,
            "a".to_string(),
        )]);
        select_stmt.where_condition = Some(Expr::BinaryOp {
            left: Box::new(lag_a),
            op: sqlparser::ast::BinaryOperator::Gt,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                "0".to_string(),
                false,
            ))),
        });

        let (out, mappings) = transform_stateful_functions(
            select_stmt,
            default_aggregate_registry(),
            registry,
            &mut allocator,
        )
        .unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(
            out.where_condition.as_ref().unwrap().to_string(),
            "col_1 > 0"
        );
    }
}
