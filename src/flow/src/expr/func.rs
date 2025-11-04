use datatypes::{ConcreteDatatype, Value};

/// Unary function that takes one argument
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryFunc {
    /// Logical NOT
    Not,
    /// Check if value is null
    IsNull,
    /// Check if value is true
    IsTrue,
    /// Check if value is false
    IsFalse,
    /// Cast to a specific type
    Cast(ConcreteDatatype),
}

impl UnaryFunc {
    /// Evaluate a unary function with a pre-evaluated argument
    pub fn eval_unary(&self, arg: Value) -> Result<Value, EvalError> {
        match self {
            Self::Not => {
                if let Value::Bool(bool) = arg {
                    Ok(Value::Bool(!bool))
                } else {
                    Err(EvalError::TypeMismatch {
                        expected: "Bool".to_string(),
                        actual: format!("{:?}", arg),
                    })
                }
            }
            Self::IsNull => {
                // For simplicity, we check if the value is some sentinel value
                // In a real implementation, Value would have an is_null() method
                Ok(Value::Bool(false)) // TODO: implement proper null check
            }
            Self::IsTrue => {
                if let Value::Bool(bool) = arg {
                    Ok(Value::Bool(bool))
                } else {
                    Err(EvalError::TypeMismatch {
                        expected: "Bool".to_string(),
                        actual: format!("{:?}", arg),
                    })
                }
            }
            Self::IsFalse => {
                if let Value::Bool(bool) = arg {
                    Ok(Value::Bool(!bool))
                } else {
                    Err(EvalError::TypeMismatch {
                        expected: "Bool".to_string(),
                        actual: format!("{:?}", arg),
                    })
                }
            }
            Self::Cast(to) => {
                // For simplicity, we'll do basic type conversions
                // In a real implementation, this would use proper type casting
                let arg_clone = arg.clone();
                match (arg, to) {
                    (Value::Int64(v), ConcreteDatatype::Int64(_)) => Ok(Value::Int64(v)),
                    (Value::Float64(v), ConcreteDatatype::Float64(_)) => Ok(Value::Float64(v)),
                    (Value::String(s), ConcreteDatatype::String(_)) => Ok(Value::String(s)),
                    (Value::Bool(b), ConcreteDatatype::Bool(_)) => Ok(Value::Bool(b)),
                    (Value::Int64(v), ConcreteDatatype::Float64(_)) => Ok(Value::Float64(v as f64)),
                    (Value::Float64(v), ConcreteDatatype::Int64(_)) => Ok(Value::Int64(v as i64)),
                    (Value::Int64(v), ConcreteDatatype::String(_)) => Ok(Value::String(v.to_string())),
                    (Value::Float64(v), ConcreteDatatype::String(_)) => Ok(Value::String(v.to_string())),
                    (Value::Bool(v), ConcreteDatatype::String(_)) => Ok(Value::String(v.to_string())),
                    (Value::String(s), ConcreteDatatype::Int64(_)) => {
                        s.parse::<i64>()
                            .map(Value::Int64)
                            .map_err(|_| EvalError::CastFailed {
                                from: format!("{:?}", arg_clone),
                                to: format!("{:?}", to),
                            })
                    }
                    (Value::String(s), ConcreteDatatype::Float64(_)) => {
                        s.parse::<f64>()
                            .map(Value::Float64)
                            .map_err(|_| EvalError::CastFailed {
                                from: format!("{:?}", arg_clone),
                                to: format!("{:?}", to),
                            })
                    }
                    _ => Err(EvalError::CastFailed {
                        from: format!("{:?}", arg_clone),
                        to: format!("{:?}", to),
                    }),
                }
            }
        }
    }
}

/// Binary function that takes two arguments
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinaryFunc {
    /// Equality
    Eq,
    /// Not equal
    NotEq,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Addition
    Add,
    /// Subtraction
    Sub,
    /// Multiplication
    Mul,
    /// Division
    Div,
    /// Modulo
    Mod,
}

impl BinaryFunc {
    /// Compare two values
    fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        match (left, right) {
            (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Evaluate a binary function with pre-evaluated arguments
    pub fn eval_binary(&self, left: Value, right: Value) -> Result<Value, EvalError> {
        match self {
            Self::Eq => Ok(Value::Bool(left == right)),
            Self::NotEq => Ok(Value::Bool(left != right)),
            Self::Lt => {
                Self::compare_values(&left, &right)
                    .map(|ord| Value::Bool(ord == std::cmp::Ordering::Less))
                    .ok_or_else(|| {
                        let left_debug = format!("{:?}", &left);
                        let right_debug = format!("{:?}", &right);
                        EvalError::TypeMismatch {
                            expected: "Comparable types (Int64, Float64, String, Bool)".to_string(),
                            actual: format!("{} and {}", left_debug, right_debug),
                        }
                    })
            }
            Self::Lte => {
                Self::compare_values(&left, &right)
                    .map(|ord| Value::Bool(ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal))
                    .ok_or_else(|| {
                        let left_debug = format!("{:?}", &left);
                        let right_debug = format!("{:?}", &right);
                        EvalError::TypeMismatch {
                            expected: "Comparable types (Int64, Float64, String, Bool)".to_string(),
                            actual: format!("{} and {}", left_debug, right_debug),
                        }
                    })
            }
            Self::Gt => {
                Self::compare_values(&left, &right)
                    .map(|ord| Value::Bool(ord == std::cmp::Ordering::Greater))
                    .ok_or_else(|| {
                        let left_debug = format!("{:?}", &left);
                        let right_debug = format!("{:?}", &right);
                        EvalError::TypeMismatch {
                            expected: "Comparable types (Int64, Float64, String, Bool)".to_string(),
                            actual: format!("{} and {}", left_debug, right_debug),
                        }
                    })
            }
            Self::Gte => {
                Self::compare_values(&left, &right)
                    .map(|ord| Value::Bool(ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal))
                    .ok_or_else(|| {
                        let left_debug = format!("{:?}", &left);
                        let right_debug = format!("{:?}", &right);
                        EvalError::TypeMismatch {
                            expected: "Comparable types (Int64, Float64, String, Bool)".to_string(),
                            actual: format!("{} and {}", left_debug, right_debug),
                        }
                    })
            }
            Self::Add => {
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                match (left, right) {
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a + b)),
                    (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(a as f64 + b)),
                    (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a + b as f64)),
                    _ => Err(EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{} and {}", left_debug, right_debug),
                    }),
                }
            }
            Self::Sub => {
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                match (left, right) {
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
                    (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(a as f64 - b)),
                    (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a - b as f64)),
                    _ => Err(EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{} and {}", left_debug, right_debug),
                    }),
                }
            }
            Self::Mul => {
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                match (left, right) {
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a * b)),
                    (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(a as f64 * b)),
                    (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(a * b as f64)),
                    _ => Err(EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{} and {}", left_debug, right_debug),
                    }),
                }
            }
            Self::Div => {
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                match (left, right) {
                    (Value::Int64(a), Value::Int64(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a as f64 / b as f64))
                        }
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a / b))
                        }
                    }
                    (Value::Int64(a), Value::Float64(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a as f64 / b))
                        }
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a / b as f64))
                        }
                    }
                    _ => Err(EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{} and {}", left_debug, right_debug),
                    }),
                }
            }
            Self::Mod => {
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                match (left, right) {
                    (Value::Int64(a), Value::Int64(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Int64(a % b))
                        }
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a % b))
                        }
                    }
                    (Value::Int64(a), Value::Float64(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a as f64 % b))
                        }
                    }
                    (Value::Float64(a), Value::Int64(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a % b as f64))
                        }
                    }
                    _ => Err(EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{} and {}", left_debug, right_debug),
                    }),
                }
            }
        }
    }
}

/// Error type for expression evaluation
#[derive(Debug, Clone, PartialEq)]
pub enum EvalError {
    /// Type mismatch error
    TypeMismatch {
        expected: String,
        actual: String,
    },
    /// Cast failed error
    CastFailed {
        from: String,
        to: String,
    },
    /// Division by zero error
    DivisionByZero,
    /// Index out of bounds
    IndexOutOfBounds {
        index: usize,
        length: usize,
    },
    /// Feature not implemented
    NotImplemented {
        feature: String,
    },
    /// DataFusion error
    DataFusionError {
        message: String,
    },
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            EvalError::CastFailed { from, to } => {
                write!(f, "Failed to cast from {} to {}", from, to)
            }
            EvalError::DivisionByZero => write!(f, "Division by zero"),
            EvalError::IndexOutOfBounds { index, length } => {
                write!(f, "Index {} out of bounds for length {}", index, length)
            }
            EvalError::NotImplemented { feature } => {
                write!(f, "Feature not implemented: {}", feature)
            }
            EvalError::DataFusionError { message } => {
                write!(f, "DataFusion error: {}", message)
            }
        }
    }
}

impl std::error::Error for EvalError {}
