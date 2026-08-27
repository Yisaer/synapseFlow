use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Value, WindowSpec, WindowType,
};
use sqlparser::parser::ParserError;

/// Window specification supported by StreamDialect
#[derive(Debug, Clone, PartialEq)]
pub enum Window {
    /// Fixed-size, non-overlapping window defined by time unit + length
    Tumbling {
        time_unit: TimeUnit,
        length: u64,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
        filter: Option<Box<Expr>>,
    },
    /// Fixed-size window defined by number of rows
    Count {
        count: u64,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
        filter: Option<Box<Expr>>,
    },
    /// Sliding window triggered by each received record.
    ///
    /// For a trigger time `t`, the window range is `[t - lookback, t + lookahead]`.
    /// When `lookahead` is `None`, the window is emitted immediately at `t`.
    /// When `lookahead` is `Some(x)`, the window is emitted at `t + x`.
    Sliding {
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
        /// Optional trigger condition extracted from `OVER (WHEN ...)`.
        /// When absent, every input row triggers a sliding window emission.
        trigger_condition: Option<Box<Expr>>,
        filter: Option<Box<Expr>>,
    },
    /// State window driven by two boolean conditions:
    /// - `open`: when the window starts collecting state
    /// - `emit`: when the window emits its current state
    State {
        open: Box<Expr>,
        emit: Box<Expr>,
        /// Optional partition keys extracted from `OVER (PARTITION BY ...)`.
        /// When empty, the window is global (single partition).
        partition_by: Vec<Expr>,
        filter: Option<Box<Expr>>,
    },
    /// End-of-stream window for bounded table scans.
    ///
    /// The window closes when the bounded source reaches graceful end-of-stream.
    Eos { filter: Option<Box<Expr>> },
}

/// Supported time units for window definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Milliseconds,
    Seconds,
}

impl Window {
    pub fn tumbling(time_unit: TimeUnit, length: u64) -> Self {
        Window::tumbling_partitioned(time_unit, length, Vec::new())
    }

    pub fn tumbling_partitioned(time_unit: TimeUnit, length: u64, partition_by: Vec<Expr>) -> Self {
        Window::Tumbling {
            time_unit,
            length,
            partition_by,
            filter: None,
        }
    }

    pub fn count(count: u64) -> Self {
        Window::count_partitioned(count, Vec::new())
    }

    pub fn count_partitioned(count: u64, partition_by: Vec<Expr>) -> Self {
        Window::Count {
            count,
            partition_by,
            filter: None,
        }
    }

    pub fn sliding(time_unit: TimeUnit, lookback: u64, lookahead: Option<u64>) -> Self {
        Window::sliding_partitioned(time_unit, lookback, lookahead, Vec::new())
    }

    pub fn sliding_partitioned(
        time_unit: TimeUnit,
        lookback: u64,
        lookahead: Option<u64>,
        partition_by: Vec<Expr>,
    ) -> Self {
        Window::Sliding {
            time_unit,
            lookback,
            lookahead,
            partition_by,
            trigger_condition: None,
            filter: None,
        }
    }

    pub fn state(open: Expr, emit: Expr) -> Self {
        Window::state_partitioned(open, emit, Vec::new())
    }

    pub fn state_partitioned(open: Expr, emit: Expr, partition_by: Vec<Expr>) -> Self {
        Window::State {
            open: Box::new(open),
            emit: Box::new(emit),
            partition_by,
            filter: None,
        }
    }

    pub fn eos() -> Self {
        Window::Eos { filter: None }
    }

    pub fn with_filter(self, filter: Option<Box<Expr>>) -> Self {
        match self {
            Window::Tumbling {
                time_unit,
                length,
                partition_by,
                ..
            } => Window::Tumbling {
                time_unit,
                length,
                partition_by,
                filter,
            },
            Window::Count {
                count,
                partition_by,
                ..
            } => Window::Count {
                count,
                partition_by,
                filter,
            },
            Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            } => Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                filter,
            },
            Window::State {
                open,
                emit,
                partition_by,
                ..
            } => Window::State {
                open,
                emit,
                partition_by,
                filter,
            },
            Window::Eos { .. } => Window::Eos { filter },
        }
    }

    pub fn with_sliding_trigger_condition(self, trigger_condition: Option<Box<Expr>>) -> Self {
        match self {
            Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by,
                filter,
                ..
            } => Window::Sliding {
                time_unit,
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                filter,
            },
            other => other,
        }
    }

    pub fn filter(&self) -> Option<&Expr> {
        match self {
            Window::Tumbling { filter, .. }
            | Window::Count { filter, .. }
            | Window::Sliding { filter, .. }
            | Window::State { filter, .. }
            | Window::Eos { filter } => filter.as_deref(),
        }
    }

    fn function_name(&self) -> &'static str {
        match self {
            Window::Tumbling { .. } => "tumblingwindow",
            Window::Count { .. } => "countwindow",
            Window::Sliding { .. } => "slidingwindow",
            Window::State { .. } => "statewindow",
            Window::Eos { .. } => "eoswindow",
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
        "statewindow" => parse_state_window(function),
        "eoswindow" => parse_eos_window(function),
        name => Err(ParserError::ParserError(format!(
            "Unsupported window function: {}",
            name
        ))),
    }
}

/// Convert a Window back to a SQL expression
pub fn window_to_expr(window: &Window) -> Expr {
    let args = match window {
        Window::Tumbling {
            time_unit, length, ..
        } => {
            vec![
                make_string_arg(time_unit.as_str()),
                make_number_arg(*length),
            ]
        }
        Window::Count { count, .. } => vec![make_number_arg(*count)],
        Window::Sliding {
            time_unit,
            lookback,
            lookahead,
            ..
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
        Window::State { open, emit, .. } => vec![
            make_expr_arg(open.as_ref().clone()),
            make_expr_arg(emit.as_ref().clone()),
        ],
        Window::Eos { .. } => Vec::new(),
    };

    let over = match window {
        Window::Tumbling { partition_by, .. }
        | Window::Count { partition_by, .. }
        | Window::Sliding { partition_by, .. }
        | Window::State { partition_by, .. }
            if !partition_by.is_empty() =>
        {
            Some(WindowType::WindowSpec(WindowSpec {
                partition_by: partition_by.clone(),
                order_by: Vec::new(),
                window_frame: None,
            }))
        }
        _ => None,
    };

    Expr::Function(Function {
        name: ObjectName(vec![Ident::new(window.function_name())]),
        args,
        over,
        distinct: false,
        order_by: vec![],
        filter: window.filter().cloned().map(Box::new),
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
    let partition_by = parse_over_partition_by(function, "tumblingwindow")?;

    Ok(
        Window::tumbling_partitioned(time_unit, length, partition_by)
            .with_filter(function.filter.clone()),
    )
}

fn parse_count_window(function: &Function) -> Result<Window, ParserError> {
    if function.args.len() != 1 {
        return Err(ParserError::ParserError(
            "countwindow requires 1 argument: (count)".to_string(),
        ));
    }

    let count = parse_number_arg(&function.args[0], "countwindow", "count")?;
    let partition_by = parse_over_partition_by(function, "countwindow")?;
    Ok(Window::count_partitioned(count, partition_by).with_filter(function.filter.clone()))
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
    let partition_by = parse_over_partition_by(function, "slidingwindow")?;

    Ok(
        Window::sliding_partitioned(time_unit, lookback, lookahead, partition_by)
            .with_filter(function.filter.clone()),
    )
}

fn parse_state_window(function: &Function) -> Result<Window, ParserError> {
    if function.args.len() != 2 {
        return Err(ParserError::ParserError(
            "statewindow requires 2 arguments: (open, emit)".to_string(),
        ));
    }

    let open = parse_expr_arg(&function.args[0], "statewindow", "open")?;
    let emit = parse_expr_arg(&function.args[1], "statewindow", "emit")?;

    let partition_by = parse_over_partition_by(function, "statewindow")?;

    Ok(Window::state_partitioned(open, emit, partition_by).with_filter(function.filter.clone()))
}

fn parse_eos_window(function: &Function) -> Result<Window, ParserError> {
    ensure_no_over(function, "eoswindow")?;
    if !function.args.is_empty() {
        return Err(ParserError::ParserError(
            "eoswindow requires no arguments".to_string(),
        ));
    }

    Ok(Window::eos().with_filter(function.filter.clone()))
}

fn parse_over_partition_by(function: &Function, func_name: &str) -> Result<Vec<Expr>, ParserError> {
    let Some(over) = function.over.as_ref() else {
        return Ok(Vec::new());
    };

    match over {
        WindowType::WindowSpec(spec) => parse_window_spec_partition_by(spec, func_name),
        WindowType::NamedWindow(name) => Err(ParserError::ParserError(format!(
            "{func_name} OVER does not support named windows (got {name})",
        ))),
    }
}

fn ensure_no_over(function: &Function, func_name: &str) -> Result<(), ParserError> {
    if function.over.is_some() {
        return Err(ParserError::ParserError(format!(
            "{func_name} does not support OVER"
        )));
    }
    Ok(())
}

fn parse_window_spec_partition_by(
    spec: &WindowSpec,
    func_name: &str,
) -> Result<Vec<Expr>, ParserError> {
    if !spec.order_by.is_empty() {
        return Err(ParserError::ParserError(format!(
            "{func_name} OVER does not support ORDER BY"
        )));
    }

    if spec.window_frame.is_some() {
        return Err(ParserError::ParserError(format!(
            "{func_name} OVER does not support window frames"
        )));
    }

    if spec.partition_by.is_empty() {
        return Err(ParserError::ParserError(format!(
            "{func_name} OVER requires PARTITION BY expressions"
        )));
    }

    Ok(spec.partition_by.clone())
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

pub(crate) fn is_supported_window_function(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "tumblingwindow" | "countwindow" | "slidingwindow" | "statewindow" | "eoswindow"
    )
}

impl TimeUnit {
    fn try_from_str(raw: &str) -> Result<Self, ParserError> {
        match raw.to_ascii_lowercase().as_str() {
            "ms" => Ok(TimeUnit::Milliseconds),
            "ss" => Ok(TimeUnit::Seconds),
            other => Err(ParserError::ParserError(format!(
                "unsupported time unit `{}` (only `ms` and `ss` allowed)",
                other
            ))),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            TimeUnit::Milliseconds => "ms",
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

fn make_expr_arg(expr: Expr) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
}

fn parse_expr_arg(
    arg: &FunctionArg,
    func_name: &str,
    field_name: &str,
) -> Result<Expr, ParserError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
        _ => Err(ParserError::ParserError(format!(
            "{} {} must be an expression",
            func_name, field_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{WindowSpec, WindowType};

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

    fn state_expr() -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("statewindow")]),
            args: vec![
                make_expr_arg(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("a"))),
                    op: sqlparser::ast::BinaryOperator::Gt,
                    right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
                }),
                make_expr_arg(Expr::Identifier(Ident::new("b"))),
            ],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    fn state_expr_partitioned() -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("statewindow")]),
            args: vec![
                make_expr_arg(Expr::Identifier(Ident::new("open"))),
                make_expr_arg(Expr::Identifier(Ident::new("emit"))),
            ],
            over: Some(WindowType::WindowSpec(WindowSpec {
                partition_by: vec![Expr::Identifier(Ident::new("k1"))],
                order_by: Vec::new(),
                window_frame: None,
            })),
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    fn over_partition_by(keys: Vec<&str>) -> WindowType {
        WindowType::WindowSpec(WindowSpec {
            partition_by: keys
                .into_iter()
                .map(|key| Expr::Identifier(Ident::new(key)))
                .collect(),
            order_by: Vec::new(),
            window_frame: None,
        })
    }

    fn eos_expr() -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("eoswindow")]),
            args: vec![],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        })
    }

    fn eos_expr_with_filter() -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![Ident::new("eoswindow")]),
            args: vec![],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: sqlparser::ast::BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
            })),
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
    fn parse_millisecond_tumbling_window_expr() {
        let mut expr = tumbling_expr();
        let Expr::Function(function) = &mut expr else {
            panic!("expected tumbling window function");
        };
        function.args[0] = make_string_arg("MS");

        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(Window::tumbling(TimeUnit::Milliseconds, 10)));
    }

    #[test]
    fn parse_tumbling_window_expr_partitioned_by() {
        let mut expr = tumbling_expr();
        if let Expr::Function(function) = &mut expr {
            function.over = Some(over_partition_by(vec!["k1", "k2"]));
        }
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(
            parsed,
            Some(Window::tumbling_partitioned(
                TimeUnit::Seconds,
                10,
                vec![
                    Expr::Identifier(Ident::new("k1")),
                    Expr::Identifier(Ident::new("k2")),
                ],
            ))
        );
    }

    #[test]
    fn parse_count_window_expr() {
        let parsed = parse_window_expr(&count_expr()).unwrap();
        assert_eq!(parsed, Some(Window::count(5)));
    }

    #[test]
    fn parse_count_window_expr_partitioned_by() {
        let mut expr = count_expr();
        if let Expr::Function(function) = &mut expr {
            function.over = Some(over_partition_by(vec!["k"]));
        }
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(
            parsed,
            Some(Window::count_partitioned(
                5,
                vec![Expr::Identifier(Ident::new("k"))],
            ))
        );
    }

    #[test]
    fn parse_sliding_window_expr_without_lookahead() {
        let parsed = parse_window_expr(&sliding_expr(None)).unwrap();
        assert_eq!(parsed, Some(Window::sliding(TimeUnit::Seconds, 10, None)));
    }

    #[test]
    fn parse_millisecond_sliding_window_expr() {
        let mut expr = sliding_expr(None);
        let Expr::Function(function) = &mut expr else {
            panic!("expected sliding window function");
        };
        function.args[0] = make_string_arg("ms");

        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(
            parsed,
            Some(Window::sliding(TimeUnit::Milliseconds, 10, None))
        );
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
    fn parse_sliding_window_expr_partitioned_by() {
        let mut expr = sliding_expr(Some(15));
        if let Expr::Function(function) = &mut expr {
            function.over = Some(over_partition_by(vec!["k"]));
        }
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(
            parsed,
            Some(Window::sliding_partitioned(
                TimeUnit::Seconds,
                10,
                Some(15),
                vec![Expr::Identifier(Ident::new("k"))],
            ))
        );
    }

    #[test]
    fn parse_state_window_expr() {
        let parsed = parse_window_expr(&state_expr()).unwrap();
        assert!(matches!(parsed, Some(Window::State { .. })));
    }

    #[test]
    fn parse_state_window_expr_partitioned_by() {
        let parsed = parse_window_expr(&state_expr_partitioned()).unwrap();
        assert_eq!(
            parsed,
            Some(Window::state_partitioned(
                Expr::Identifier(Ident::new("open")),
                Expr::Identifier(Ident::new("emit")),
                vec![Expr::Identifier(Ident::new("k1"))],
            ))
        );
    }

    #[test]
    fn parse_eos_window_expr() {
        let parsed = parse_window_expr(&eos_expr()).unwrap();
        assert_eq!(parsed, Some(Window::eos()));
    }

    #[test]
    fn parse_eos_window_expr_with_filter() {
        let parsed = parse_window_expr(&eos_expr_with_filter()).unwrap();
        let Some(Window::Eos {
            filter: Some(filter),
        }) = parsed
        else {
            panic!("expected eos window with filter");
        };
        assert_eq!(filter.to_string(), "a > 0");
    }

    #[test]
    fn reject_eos_window_arguments() {
        let expr = Expr::Function(Function {
            name: ObjectName(vec![Ident::new("eoswindow")]),
            args: vec![make_number_arg(1)],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            null_treatment: None,
            special: false,
        });

        let err = parse_window_expr(&expr).unwrap_err().to_string();
        assert!(
            err.contains("eoswindow requires no arguments"),
            "unexpected error: {err}",
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
    fn millisecond_sliding_window_round_trip_back_to_expr() {
        let window = Window::sliding(TimeUnit::Milliseconds, 100, Some(150));
        let expr = window_to_expr(&window);
        assert_eq!(expr.to_string(), "slidingwindow('ms', 100, 150)");
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn millisecond_tumbling_window_round_trip_back_to_expr() {
        let window = Window::tumbling(TimeUnit::Milliseconds, 100);
        let expr = window_to_expr(&window);
        assert_eq!(expr.to_string(), "tumblingwindow('ms', 100)");
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn tumbling_window_round_trip_back_to_expr_partitioned_by() {
        let window = Window::tumbling_partitioned(
            TimeUnit::Seconds,
            25,
            vec![Expr::Identifier(Ident::new("k"))],
        );
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn count_window_round_trip_back_to_expr_partitioned_by() {
        let window = Window::count_partitioned(5, vec![Expr::Identifier(Ident::new("k"))]);
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
    fn sliding_window_round_trip_back_to_expr_partitioned_by() {
        let window = Window::sliding_partitioned(
            TimeUnit::Seconds,
            10,
            Some(15),
            vec![Expr::Identifier(Ident::new("k"))],
        );
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

    #[test]
    fn state_window_round_trip_back_to_expr() {
        let window = Window::state(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
        );
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn state_window_round_trip_back_to_expr_partitioned_by() {
        let window = Window::state_partitioned(
            Expr::Identifier(Ident::new("open")),
            Expr::Identifier(Ident::new("emit")),
            vec![
                Expr::Identifier(Ident::new("k1")),
                Expr::Identifier(Ident::new("k2")),
            ],
        );
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn eos_window_round_trip_back_to_expr() {
        let window = Window::eos();
        let expr = window_to_expr(&window);
        let parsed = parse_window_expr(&expr).unwrap();
        assert_eq!(parsed, Some(window));
    }

    #[test]
    fn sliding_window_with_trigger_condition() {
        let window = Window::sliding(TimeUnit::Seconds, 10, None).with_sliding_trigger_condition(
            Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: sqlparser::ast::BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
            })),
        );

        match &window {
            Window::Sliding {
                trigger_condition, ..
            } => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn sliding_window_with_trigger_condition_and_partition_by() {
        let window = Window::sliding_partitioned(
            TimeUnit::Seconds,
            10,
            Some(15),
            vec![Expr::Identifier(Ident::new("k"))],
        )
        .with_sliding_trigger_condition(Some(Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: sqlparser::ast::BinaryOperator::Gt,
            right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
        })));

        match &window {
            Window::Sliding {
                lookback,
                lookahead,
                partition_by,
                trigger_condition,
                ..
            } => {
                assert_eq!(*lookback, 10);
                assert_eq!(*lookahead, Some(15));
                assert_eq!(partition_by.len(), 1);
                assert_eq!(partition_by[0].to_string(), "k");
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }

    #[test]
    fn with_sliding_trigger_condition_noop_on_non_sliding() {
        let window = Window::tumbling(TimeUnit::Seconds, 10)
            .with_sliding_trigger_condition(Some(Box::new(Expr::Identifier(Ident::new("a")))));

        assert!(matches!(window, Window::Tumbling { .. }));
    }

    #[test]
    fn sliding_window_with_filter_and_trigger_condition() {
        let window = Window::sliding(TimeUnit::Seconds, 10, None)
            .with_sliding_trigger_condition(Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: sqlparser::ast::BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
            })))
            .with_filter(Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("x"))),
                op: sqlparser::ast::BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
            })));

        match &window {
            Window::Sliding {
                trigger_condition,
                filter,
                ..
            } => {
                assert_eq!(
                    trigger_condition.as_ref().map(|e| e.to_string()),
                    Some("a > 1".to_string()),
                );
                assert_eq!(
                    filter.as_ref().map(|e| e.to_string()),
                    Some("x > 0".to_string()),
                );
            }
            other => panic!("expected sliding window, got {other:?}"),
        }
    }
}
