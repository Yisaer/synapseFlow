use datatypes::{ConcreteDatatype, DataType, Value, Int64Type, Float64Type, StringType};

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
    /// Try to cast a value to Int64
    fn try_cast_to_int64(value: &Value) -> Option<i64> {
        let int64_type = Int64Type;
        int64_type.try_cast(value.clone()).and_then(|v| match v {
            Value::Int64(i) => Some(i),
            _ => None,
        })
    }

    /// Try to cast a value to Float64
    fn try_cast_to_float64(value: &Value) -> Option<f64> {
        let float64_type = Float64Type;
        float64_type.try_cast(value.clone()).and_then(|v| match v {
            Value::Float64(f) => Some(f),
            _ => None,
        })
    }

    /// Try to cast a value to String
    fn try_cast_to_string(value: &Value) -> Option<String> {
        let string_type = StringType;
        string_type.try_cast(value.clone()).and_then(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
    }

    /// Compare two values by trying to cast them to comparable types
    fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        // If types match, compare directly
        match (left, right) {
            (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            // If types don't match, try to cast to a common type
            _ => {
                None
            }
        }
    }

    /// Try to cast both values to a numeric type for arithmetic operations
    /// Returns (left, right) as either (Int64, Int64) or (Float64, Float64)
    fn try_cast_to_numeric(left: Value, right: Value) -> Result<(Value, Value), EvalError> {
        // If types match, return directly
        match (&left, &right) {
            (Value::Int64(_), Value::Int64(_)) => return Ok((left, right)),
            (Value::Float64(_), Value::Float64(_)) => return Ok((left, right)),
            _ => {}
        }
        
        // Try Float64
        if let (Some(left_f), Some(right_f)) = (
            Self::try_cast_to_float64(&left),
            Self::try_cast_to_float64(&right),
        ) {
            return Ok((Value::Float64(left_f), Value::Float64(right_f)));
        }

        let left_debug = format!("{:?}", &left);
        let right_debug = format!("{:?}", &right);
        Err(EvalError::TypeMismatch {
            expected: "Int64 or Float64".to_string(),
            actual: format!("{} and {}", left_debug, right_debug),
        })
    }

    /// Evaluate a binary function with pre-evaluated arguments
    pub fn eval_binary(&self, left: Value, right: Value) -> Result<Value, EvalError> {
        match self {
            Self::Eq => Ok(Value::Bool(left == right)),
            Self::NotEq => Ok(Value::Bool(left != right)),
            Self::Lt => {
                Ok(Value::Bool(
                    Self::compare_values(&left, &right)
                        .map(|ord| ord == std::cmp::Ordering::Less)
                        .unwrap_or(false)
                ))
            }
            Self::Lte => {
                Ok(Value::Bool(
                    Self::compare_values(&left, &right)
                        .map(|ord| ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal)
                        .unwrap_or(false)
                ))
            }
            Self::Gt => {
                Ok(Value::Bool(
                    Self::compare_values(&left, &right)
                        .map(|ord| ord == std::cmp::Ordering::Greater)
                        .unwrap_or(false)
                ))
            }
            Self::Gte => {
                Ok(Value::Bool(
                    Self::compare_values(&left, &right)
                        .map(|ord| ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal)
                        .unwrap_or(false)
                ))
            }
            Self::Add => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a + b)),
                    (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a + b)),
                    (Value::String(left_str), Value::String(right_str)) => {
                        return Ok(Value::String(format!("{}{}", left_str, right_str)));
                    }
                    _ => {}
                }

                // If types don't match, try numeric addition first
                if let Ok((left_num, right_num)) = Self::try_cast_to_numeric(left.clone(), right.clone()) {
                    match (left_num, right_num) {
                        (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a + b)),
                        (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a + b)),
                        _ => unreachable!(),
                    }
                }
                // If both fail, return error
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                Err(EvalError::TypeMismatch {
                    expected: "Int64, Float64, or String".to_string(),
                    actual: format!("{} and {}", left_debug, right_debug),
                })
            }
            Self::Sub => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a - b)),
                    (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a - b)),
                    _ => {}
                }

                // If types don't match, try numeric conversion
                let (left_num, right_num) = Self::try_cast_to_numeric(left, right)?;
                match (left_num, right_num) {
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
                    _ => unreachable!(),
                }
            }
            Self::Mul => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a * b)),
                    (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a * b)),
                    _ => {}
                }

                // If types don't match, try numeric conversion
                let (left_num, right_num) = Self::try_cast_to_numeric(left, right)?;
                match (left_num, right_num) {
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a * b)),
                    _ => unreachable!(),
                }
            }
            Self::Div => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int64(a), Value::Int64(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if *b == 0.0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(a / b));
                    }
                    _ => {}
                }

                // If types don't match, convert to Float64 for precise results
                let left_f = Self::try_cast_to_float64(&left)
                    .ok_or_else(|| EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{:?}", left),
                    })?;
                let right_f = Self::try_cast_to_float64(&right)
                    .ok_or_else(|| EvalError::TypeMismatch {
                        expected: "Int64 or Float64".to_string(),
                        actual: format!("{:?}", right),
                    })?;

                if right_f == 0.0 {
                    Err(EvalError::DivisionByZero)
                } else {
                    Ok(Value::Float64(left_f / right_f))
                }
            }
            Self::Mod => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int64(a), Value::Int64(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Int64(a % b));
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if *b == 0.0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(a % b));
                    }
                    _ => {}
                }

                // If types don't match, try numeric conversion
                let (left_num, right_num) = Self::try_cast_to_numeric(left, right)?;
                match (left_num, right_num) {
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
                    _ => unreachable!(),
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
