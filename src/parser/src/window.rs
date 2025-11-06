use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, Value};
use sqlparser::parser::ParserError;

/// Represents different types of time windows
#[derive(Debug, Clone, PartialEq)]
pub enum WindowType {
    /// Tumbling window - fixed-size, non-overlapping windows
    Tumbling,
    /// Hopping window - fixed-size windows that hop forward
    Hopping,
    /// Sliding window - windows that slide forward continuously
    Sliding,
    /// Session window - windows based on session gaps
    Session,
}

/// Represents a time window specification
#[derive(Debug, Clone, PartialEq)]
pub struct TimeWindow {
    /// Type of window
    pub window_type: WindowType,
    /// Time unit (e.g., 'ss', 'mm', 'hh', 'dd')
    pub time_unit: String,
    /// Window size in the specified time unit
    pub size: u64,
    /// Hop size for hopping windows (optional)
    pub hop_size: Option<u64>,
    /// Session gap for session windows (optional)
    pub session_gap: Option<u64>,
}

impl TimeWindow {
    /// Create a new tumbling window
    pub fn tumbling(time_unit: String, size: u64) -> Self {
        Self {
            window_type: WindowType::Tumbling,
            time_unit,
            size,
            hop_size: None,
            session_gap: None,
        }
    }

    /// Create a new hopping window
    pub fn hopping(time_unit: String, size: u64, hop_size: u64) -> Self {
        Self {
            window_type: WindowType::Hopping,
            time_unit,
            size,
            hop_size: Some(hop_size),
            session_gap: None,
        }
    }

    /// Create a new sliding window
    pub fn sliding(time_unit: String, size: u64) -> Self {
        Self {
            window_type: WindowType::Sliding,
            time_unit,
            size,
            hop_size: None,
            session_gap: None,
        }
    }

    /// Create a new session window
    pub fn session(time_unit: String, session_gap: u64) -> Self {
        Self {
            window_type: WindowType::Session,
            time_unit,
            size: 0, // size is not applicable for session windows
            hop_size: None,
            session_gap: Some(session_gap),
        }
    }
}

/// Parse a tumbling window function from SQL expression
pub fn parse_tumbling_window(function: &Function) -> Result<TimeWindow, ParserError> {
    let function_name = function.name.to_string().to_lowercase();
    
    if function_name != "tumblingwindow" {
        return Err(ParserError::ParserError(
            format!("Expected tumblingwindow function, got {}", function_name)
        ));
    }
    
    if function.args.len() != 2 {
        return Err(ParserError::ParserError(
            "tumblingwindow function requires exactly 2 arguments: time_unit and size".to_string()
        ));
    }
    
    let time_unit = match &function.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(s)))) => s.clone(),
        _ => {
            return Err(ParserError::ParserError(
                "tumblingwindow first argument must be a string literal (time unit)".to_string()
            ));
        }
    };
    
    let size_str = match &function.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(n, _)))) => n.clone(),
        _ => {
            return Err(ParserError::ParserError(
                "tumblingwindow second argument must be a number (window size)".to_string()
            ));
        }
    };
    
    let size = size_str.parse::<u64>().map_err(|_| {
        ParserError::ParserError(
            format!("Invalid window size: {}", size_str)
        )
    })?;
    
    Ok(TimeWindow::tumbling(time_unit, size))
}

/// Convert a TimeWindow back to a SQL expression
pub fn window_to_expr(window: &TimeWindow) -> Expr {
    let window_type_name = match window.window_type {
        WindowType::Tumbling => "tumblingwindow",
        WindowType::Hopping => "hoppingwindow",
        WindowType::Sliding => "slidingwindow",
        WindowType::Session => "sessionwindow",
    };
    
    let args = match window.window_type {
        WindowType::Tumbling => vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(window.time_unit.clone())))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(window.size.to_string(), false)))),
        ],
        WindowType::Hopping => vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(window.time_unit.clone())))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(window.size.to_string(), false)))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(window.hop_size.unwrap_or(0).to_string(), false)))),
        ],
        WindowType::Sliding => vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(window.time_unit.clone())))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(window.size.to_string(), false)))),
        ],
        WindowType::Session => vec![
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(window.time_unit.clone())))),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(window.session_gap.unwrap_or(0).to_string(), false)))),
        ],
    };
    
    Expr::Function(Function {
        name: sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new(window_type_name)]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
        null_treatment: None,
        special: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window_creation() {
        let window = TimeWindow::tumbling("ss".to_string(), 10);
        assert_eq!(window.window_type, WindowType::Tumbling);
        assert_eq!(window.time_unit, "ss");
        assert_eq!(window.size, 10);
        assert_eq!(window.hop_size, None);
        assert_eq!(window.session_gap, None);
    }

    #[test]
    fn test_window_to_expr() {
        let window = TimeWindow::tumbling("ss".to_string(), 10);
        let expr = window_to_expr(&window);
        
        match expr {
            Expr::Function(func) => {
                assert_eq!(func.name.to_string(), "tumblingwindow");
                assert_eq!(func.args.len(), 2);
            }
            _ => panic!("Expected Function expression"),
        }
    }
}