use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Value};
use sqlparser::parser::ParserError;

/// Window specification supported by StreamDialect
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Window {
    /// Fixed-size, non-overlapping window defined by time unit + length
    Tumbling { time_unit: TimeUnit, length: u64 },
    /// Fixed-size window defined by number of rows
    Count { count: u64 },
    /// Sliding window triggered by each received record.
    ///
    /// For a trigger time `t`, the window range is `[t - lookback, t + lookahead]`.
    /// When `lookahead` is `None`, the window is emitted immediately at `t`.
    /// When `lookahead` is `Some(x)`, the window is emitted at `t + x`.
    Sliding {
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
    },
}

/// Supported time units for window definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Seconds,
}

impl Window {
    pub fn tumbling(time_unit: TimeUnit, length: u64) -> Self {
        Window::Tumbling { time_unit, length }
    }

    pub fn count(count: u64) -> Self {
        Window::Count { count }
    }

    pub fn sliding(time_unit: TimeUnit, lookback: u64, lookahead: Option<u64>) -> Self {
        Window::Sliding {
            time_unit,
            lookback,
            lookahead,
        }
    }

    fn function_name(&self) -> &'static str {
        match self {
            Window::Tumbling { .. } => "tumblingwindow",
            Window::Count { .. } => "countwindow",
            Window::Sliding { .. } => "slidingwindow",
        }
    }
}

/// Attempt to parse a window function from an expression
pub fn parse_window_expr(expr: &Expr) -> Result<Option<Window>, ParserError> {
    if let Expr::Function(function) = expr
        && is_supported_window_function(&function.name.to_string())
    {
        return parse_window_function(function).map(Some);
    }

    Ok(None)
}

/// Parse a window function (tumblingwindow/countwindow) into a Window enum
pub fn parse_window_function(function: &Function) -> Result<Window, ParserError> {
    match function.name.to_string().to_lowercase().as_str() {
        "tumblingwindow" => parse_tumbling_window(function),
        "countwindow" => parse_count_window(function),
        "slidingwindow" => parse_sliding_window(function),
        name => Err(ParserError::ParserError(format!(
            "Unsupported window function: {}",
            name
        ))),
    }
}

/// Convert a Window back to a SQL expression
pub fn window_to_expr(window: &Window) -> Expr {
    let args = match window {
        Window::Tumbling { time_unit, length } => {
            vec![
                make_string_arg(time_unit.as_str()),
                make_number_arg(*length),
            ]
        }
        Window::Count { count } => vec![make_number_arg(*count)],
        Window::Sliding {
            time_unit,
            lookback,
            lookahead,
        } => {
            let mut args = vec![
                make_string_arg(time_unit.as_str()),
                make_number_arg(*lookback),
            ];
            if let Some(lookahead) = lookahead {
                args.push(make_number_arg(*lookahead));
            }
            args
        }
    };

    Expr::Function(Function {
        name: ObjectName(vec![Ident::new(window.function_name())]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
        null_treatment: None,
        special: false,
    })
}

fn parse_tumbling_window(function: &Function) -> Result<Window, ParserError> {
    if function.args.len() != 2 {
        return Err(ParserError::ParserError(
            "tumblingwindow requires 2 arguments: (time_unit, length)".to_string(),
        ));
    }

    let time_unit = parse_string_arg(&function.args[0], "tumblingwindow", "time unit")?;
    let length = parse_number_arg(&function.args[1], "tumblingwindow", "length")?;

    let time_unit = TimeUnit::try_from_str(&time_unit)?;

    Ok(Window::tumbling(time_unit, length))
}

fn parse_count_window(function: &Function) -> Result<Window, ParserError> {
    if function.args.len() != 1 {
        return Err(ParserError::ParserError(
            "countwindow requires 1 argument: (count)".to_string(),
        ));
    }

    let count = parse_number_arg(&function.args[0], "countwindow", "count")?;
    Ok(Window::count(count))
}

fn parse_sliding_window(function: &Function) -> Result<Window, ParserError> {
    if function.args.len() != 2 && function.args.len() != 3 {
        return Err(ParserError::ParserError(
            "slidingwindow requires 2 or 3 arguments: (time_unit, lookback [, lookahead])"
                .to_string(),
        ));
    }

    let time_unit = parse_string_arg(&function.args[0], "slidingwindow", "time unit")?;
    let lookback = parse_number_arg(&function.args[1], "slidingwindow", "lookback")?;
    let lookahead = if function.args.len() == 3 {
        Some(parse_number_arg(
            &function.args[2],
            "slidingwindow",
            "lookahead",
        )?)
    } else {
        None
    };

    let time_unit = TimeUnit::try_from_str(&time_unit)?;

    Ok(Window::sliding(time_unit, lookback, lookahead))
}

fn parse_string_arg(
    arg: &FunctionArg,
    func_name: &str,
    field_name: &str,
) -> Result<String, ParserError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(s)))) => {
            Ok(s.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::DoubleQuotedString(s)))) => {
            Ok(s.clone())
        }
        _ => Err(ParserError::ParserError(format!(
            "{} {} must be a string literal",
            func_name, field_name
        ))),
    }
}

fn parse_number_arg(
    arg: &FunctionArg,
    func_name: &str,
    field_name: &str,
) -> Result<u64, ParserError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(v, _)))) => {
            v.parse::<u64>().map_err(|_| {
                ParserError::ParserError(format!(
                    "{} {} must be an unsigned integer",
                    func_name, field_name
                ))
            })
        }
        _ => Err(ParserError::ParserError(format!(
            "{} {} must be a number",
            func_name, field_name
        ))),
    }
}

fn is_supported_window_function(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "tumblingwindow" | "countwindow" | "slidingwindow"
    )
}

impl TimeUnit {
    fn try_from_str(raw: &str) -> Result<Self, ParserError> {
        match raw.to_ascii_lowercase().as_str() {
            "ss" => Ok(TimeUnit::Seconds),
            other => Err(ParserError::ParserError(format!(
                "unsupported time unit `{}` (only `ss` allowed)",
                other
            ))),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            TimeUnit::Seconds => "ss",
        }
    }
}

fn make_string_arg(value: &str) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
        Value::SingleQuotedString(value.to_string()),
    )))
}

fn make_number_arg(value: u64) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(
        value.to_string(),
        false,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tumbling_expr() -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("tumblingwindow")]),
            args: vec![make_string_arg("ss"), make_number_arg(10)],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    fn count_expr() -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("countwindow")]),
            args: vec![make_number_arg(5)],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    fn sliding_expr(lookahead: Option<u64>) -> Expr {
        let mut args = vec![make_string_arg("ss"), make_number_arg(10)];
        if let Some(lookahead) = lookahead {
            args.push(make_number_arg(lookahead));
        }
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("slidingwindow")]),
            args,
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    #[test]
    fn parse_tumbling_window_expr() {
        let parsed = parse_window_expr(&tumbling_expr()).unwrap();
        assert_eq!(parsed, Some(Window::tumbling(TimeUnit::Seconds, 10)));
    }

    #[test]
    fn parse_count_window_expr() {
        let parsed = parse_window_expr(&count_expr()).unwrap();
        assert_eq!(parsed, Some(Window::count(5)));
    }

    #[test]
    fn parse_sliding_window_expr_without_lookahead() {
        let parsed = parse_window_expr(&sliding_expr(None)).unwrap();
        assert_eq!(parsed, Some(Window::sliding(TimeUnit::Seconds, 10, None)));
    }

    #[test]
    fn parse_sliding_window_expr_with_lookahead() {
        let parsed = parse_window_expr(&sliding_expr(Some(15))).unwrap();
        assert_eq!(
            parsed,
            Some(Window::sliding(TimeUnit::Seconds, 10, Some(15)))
        );
    }

    #[test]
    fn parse_window_expr_non_window() {
        let expr = Expr::Identifier(Ident::new("a"));
        let parsed = parse_window_expr(&expr).unwrap();
        assert!(parsed.is_none());
    }

    #[test]
    fn window_round_trip_back_to_expr() {
        let window = Window::tumbling(TimeUnit::Seconds, 25);
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn sliding_window_round_trip_back_to_expr() {
        let window = Window::sliding(TimeUnit::Seconds, 10, None);
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn sliding_window_round_trip_back_to_expr_with_lookahead() {
        let window = Window::sliding(TimeUnit::Seconds, 10, Some(15));
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }
}
