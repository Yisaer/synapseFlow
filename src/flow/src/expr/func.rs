use datatypes::{
    ConcreteDatatype, DataType, Float32Type, Float64Type, Int64Type, StringType, Value,
};

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
            Self::IsNull => Ok(Value::Bool(arg.is_null())),
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
                // Use the DataType's try_cast method for proper type casting
                match to {
                    ConcreteDatatype::Int8(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Int16(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Int32(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Int64(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Float32(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Float64(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Uint8(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Uint16(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Uint32(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Uint64(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::String(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    ConcreteDatatype::Bool(t) => {
                        t.try_cast(arg.clone())
                            .ok_or_else(|| EvalError::CastFailed {
                                from: format!("{:?}", arg),
                                to: format!("{:?}", to),
                            })
                    }
                    _ => {
                        // For unsupported types like Struct and List, fall back to basic casting
                        let arg_clone = arg.clone();
                        match (arg, to) {
                            (Value::Int64(v), ConcreteDatatype::Int64(_)) => Ok(Value::Int64(v)),
                            (Value::Float64(v), ConcreteDatatype::Float64(_)) => {
                                Ok(Value::Float64(v))
                            }
                            (Value::String(s), ConcreteDatatype::String(_)) => Ok(Value::String(s)),
                            (Value::Bool(b), ConcreteDatatype::Bool(_)) => Ok(Value::Bool(b)),
                            (Value::Int64(v), ConcreteDatatype::Float64(_)) => {
                                Ok(Value::Float64(v as f64))
                            }
                            (Value::Float64(v), ConcreteDatatype::Int64(_)) => {
                                Ok(Value::Int64(v as i64))
                            }
                            (Value::Int64(v), ConcreteDatatype::String(_)) => {
                                Ok(Value::String(v.to_string()))
                            }
                            (Value::Float64(v), ConcreteDatatype::String(_)) => {
                                Ok(Value::String(v.to_string()))
                            }
                            (Value::Bool(v), ConcreteDatatype::String(_)) => {
                                Ok(Value::String(v.to_string()))
                            }
                            (Value::String(s), ConcreteDatatype::Int64(_)) => s
                                .parse::<i64>()
                                .map(Value::Int64)
                                .map_err(|_| EvalError::CastFailed {
                                    from: format!("{:?}", arg_clone),
                                    to: format!("{:?}", to),
                                }),
                            (Value::String(s), ConcreteDatatype::Float64(_)) => s
                                .parse::<f64>()
                                .map(Value::Float64)
                                .map_err(|_| EvalError::CastFailed {
                                    from: format!("{:?}", arg_clone),
                                    to: format!("{:?}", to),
                                }),
                            _ => Err(EvalError::CastFailed {
                                from: format!("{:?}", arg_clone),
                                to: format!("{:?}", to),
                            }),
                        }
                    }
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

    /// Try to cast a value to Float32
    fn try_cast_to_float32(value: &Value) -> Option<f32> {
        let float32_type = Float32Type;
        float32_type.try_cast(value.clone()).and_then(|v| match v {
            Value::Float32(f) => Some(f),
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
        // Null values are not comparable
        if left.is_null() || right.is_null() {
            return None;
        }

        // If types match, compare directly
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Int8(a), Value::Int8(b)) => Some(a.cmp(b)),
            (Value::Int16(a), Value::Int16(b)) => Some(a.cmp(b)),
            (Value::Int32(a), Value::Int32(b)) => Some(a.cmp(b)),
            (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
            (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::Uint8(a), Value::Uint8(b)) => Some(a.cmp(b)),
            (Value::Uint16(a), Value::Uint16(b)) => Some(a.cmp(b)),
            (Value::Uint32(a), Value::Uint32(b)) => Some(a.cmp(b)),
            (Value::Uint64(a), Value::Uint64(b)) => Some(a.cmp(b)),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            // If types don't match, try to cast to a common type
            _ => {
                // Try Int64 first
                if let (Some(a), Some(b)) = (
                    Self::try_cast_to_int64(left),
                    Self::try_cast_to_int64(right),
                ) {
                    return Some(a.cmp(&b));
                }

                // Try Float32
                if let (Some(a), Some(b)) = (
                    Self::try_cast_to_float32(left),
                    Self::try_cast_to_float32(right),
                ) {
                    return a.partial_cmp(&b);
                }

                // Try Float64
                if let (Some(a), Some(b)) = (
                    Self::try_cast_to_float64(left),
                    Self::try_cast_to_float64(right),
                ) {
                    return a.partial_cmp(&b);
                }

                // Try String
                if let (Some(a), Some(b)) = (
                    Self::try_cast_to_string(left),
                    Self::try_cast_to_string(right),
                ) {
                    return Some(a.cmp(&b));
                }

                None
            }
        }
    }

    /// Try to cast both values to a numeric type for arithmetic operations
    /// Returns (left, right) as either (Int64, Int64), (Float64, Float64), or (Uint8, Uint8)
    fn try_cast_to_numeric(left: Value, right: Value) -> Result<(Value, Value), EvalError> {
        // If types match, return directly
        match (&left, &right) {
            (Value::Int8(_), Value::Int8(_)) => return Ok((left, right)),
            (Value::Int16(_), Value::Int16(_)) => return Ok((left, right)),
            (Value::Int32(_), Value::Int32(_)) => return Ok((left, right)),
            (Value::Int64(_), Value::Int64(_)) => return Ok((left, right)),
            (Value::Float32(_), Value::Float32(_)) => return Ok((left, right)),
            (Value::Float64(_), Value::Float64(_)) => return Ok((left, right)),
            (Value::Uint8(_), Value::Uint8(_)) => return Ok((left, right)),
            (Value::Uint16(_), Value::Uint16(_)) => return Ok((left, right)),
            (Value::Uint32(_), Value::Uint32(_)) => return Ok((left, right)),
            (Value::Uint64(_), Value::Uint64(_)) => return Ok((left, right)),
            _ => {}
        }

        // Try Int64 first (preferred for integer operations)
        if let (Some(left_i), Some(right_i)) = (
            Self::try_cast_to_int64(&left),
            Self::try_cast_to_int64(&right),
        ) {
            return Ok((Value::Int64(left_i), Value::Int64(right_i)));
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
            expected: "Int64, Float64, or Uint8".to_string(),
            actual: format!("{} and {}", left_debug, right_debug),
        })
    }

    /// Evaluate a binary function with pre-evaluated arguments
    pub fn eval_binary(&self, left: Value, right: Value) -> Result<Value, EvalError> {
        match self {
            Self::Eq => {
                // Null == Null is true, Null == anything else is false
                if left.is_null() && right.is_null() {
                    Ok(Value::Bool(true))
                } else if left.is_null() || right.is_null() {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Bool(left == right))
                }
            }
            Self::NotEq => {
                // Null != Null is false, Null != anything else is true
                if left.is_null() && right.is_null() {
                    Ok(Value::Bool(false))
                } else if left.is_null() || right.is_null() {
                    Ok(Value::Bool(true))
                } else {
                    Ok(Value::Bool(left != right))
                }
            }
            Self::Lt => Ok(Value::Bool(
                Self::compare_values(&left, &right)
                    .map(|ord| ord == std::cmp::Ordering::Less)
                    .unwrap_or(false),
            )),
            Self::Lte => Ok(Value::Bool(
                Self::compare_values(&left, &right)
                    .map(|ord| ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal)
                    .unwrap_or(false),
            )),
            Self::Gt => Ok(Value::Bool(
                Self::compare_values(&left, &right)
                    .map(|ord| ord == std::cmp::Ordering::Greater)
                    .unwrap_or(false),
            )),
            Self::Gte => Ok(Value::Bool(
                Self::compare_values(&left, &right)
                    .map(|ord| {
                        ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal
                    })
                    .unwrap_or(false),
            )),
            Self::Add => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int8(a), Value::Int8(b)) => {
                        return Ok(Value::Int8(a.saturating_add(*b)))
                    }
                    (Value::Int16(a), Value::Int16(b)) => {
                        return Ok(Value::Int16(a.saturating_add(*b)))
                    }
                    (Value::Int32(a), Value::Int32(b)) => {
                        return Ok(Value::Int32(a.saturating_add(*b)))
                    }
                    (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a + b)),
                    (Value::Float32(a), Value::Float32(b)) => return Ok(Value::Float32(a + b)),
                    (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a + b)),
                    (Value::Uint8(a), Value::Uint8(b)) => {
                        return Ok(Value::Uint8(a.saturating_add(*b)))
                    }
                    (Value::Uint16(a), Value::Uint16(b)) => {
                        return Ok(Value::Uint16(a.saturating_add(*b)))
                    }
                    (Value::Uint32(a), Value::Uint32(b)) => {
                        return Ok(Value::Uint32(a.saturating_add(*b)))
                    }
                    (Value::Uint64(a), Value::Uint64(b)) => {
                        return Ok(Value::Uint64(a.saturating_add(*b)))
                    }
                    (Value::String(left_str), Value::String(right_str)) => {
                        return Ok(Value::String(format!("{}{}", left_str, right_str)));
                    }
                    _ => {}
                }

                // If types don't match, try numeric addition first
                if let Ok((left_num, right_num)) =
                    Self::try_cast_to_numeric(left.clone(), right.clone())
                {
                    match (left_num, right_num) {
                        (Value::Int8(a), Value::Int8(b)) => {
                            return Ok(Value::Int8(a.saturating_add(b)))
                        }
                        (Value::Int16(a), Value::Int16(b)) => {
                            return Ok(Value::Int16(a.saturating_add(b)))
                        }
                        (Value::Int32(a), Value::Int32(b)) => {
                            return Ok(Value::Int32(a.saturating_add(b)))
                        }
                        (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a + b)),
                        (Value::Float32(a), Value::Float32(b)) => return Ok(Value::Float32(a + b)),
                        (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a + b)),
                        (Value::Uint8(a), Value::Uint8(b)) => {
                            return Ok(Value::Uint8(a.saturating_add(b)))
                        }
                        (Value::Uint16(a), Value::Uint16(b)) => {
                            return Ok(Value::Uint16(a.saturating_add(b)))
                        }
                        (Value::Uint32(a), Value::Uint32(b)) => {
                            return Ok(Value::Uint32(a.saturating_add(b)))
                        }
                        (Value::Uint64(a), Value::Uint64(b)) => {
                            return Ok(Value::Uint64(a.saturating_add(b)))
                        }
                        _ => unreachable!(),
                    }
                }
                // If both fail, return error
                let left_debug = format!("{:?}", &left);
                let right_debug = format!("{:?}", &right);
                Err(EvalError::TypeMismatch {
                    expected: "Int64, Float32, Float64, or String".to_string(),
                    actual: format!("{} and {}", left_debug, right_debug),
                })
            }
            Self::Sub => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int8(a), Value::Int8(b)) => {
                        return Ok(Value::Int8(a.saturating_sub(*b)))
                    }
                    (Value::Int16(a), Value::Int16(b)) => {
                        return Ok(Value::Int16(a.saturating_sub(*b)))
                    }
                    (Value::Int32(a), Value::Int32(b)) => {
                        return Ok(Value::Int32(a.saturating_sub(*b)))
                    }
                    (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a - b)),
                    (Value::Float32(a), Value::Float32(b)) => return Ok(Value::Float32(a - b)),
                    (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a - b)),
                    (Value::Uint8(a), Value::Uint8(b)) => {
                        return Ok(Value::Int64(*a as i64 - *b as i64))
                    }
                    (Value::Uint16(a), Value::Uint16(b)) => {
                        return Ok(Value::Int64(*a as i64 - *b as i64))
                    }
                    (Value::Uint32(a), Value::Uint32(b)) => {
                        return Ok(Value::Int64(*a as i64 - *b as i64))
                    }
                    (Value::Uint64(a), Value::Uint64(b)) => {
                        return Ok(Value::Int64(*a as i64 - *b as i64))
                    }
                    _ => {}
                }

                // If types don't match, try numeric conversion
                let (left_num, right_num) = Self::try_cast_to_numeric(left, right)?;
                match (left_num, right_num) {
                    (Value::Int8(a), Value::Int8(b)) => Ok(Value::Int8(a.saturating_sub(b))),
                    (Value::Int16(a), Value::Int16(b)) => Ok(Value::Int16(a.saturating_sub(b))),
                    (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a.saturating_sub(b))),
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
                    (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a - b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
                    (Value::Uint8(a), Value::Uint8(b)) => Ok(Value::Int64(a as i64 - b as i64)),
                    (Value::Uint16(a), Value::Uint16(b)) => Ok(Value::Int64(a as i64 - b as i64)),
                    (Value::Uint32(a), Value::Uint32(b)) => Ok(Value::Int64(a as i64 - b as i64)),
                    (Value::Uint64(a), Value::Uint64(b)) => Ok(Value::Int64(a as i64 - b as i64)),
                    _ => unreachable!(),
                }
            }
            Self::Mul => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int8(a), Value::Int8(b)) => {
                        return Ok(Value::Int8(a.saturating_mul(*b)))
                    }
                    (Value::Int16(a), Value::Int16(b)) => {
                        return Ok(Value::Int16(a.saturating_mul(*b)))
                    }
                    (Value::Int32(a), Value::Int32(b)) => {
                        return Ok(Value::Int32(a.saturating_mul(*b)))
                    }
                    (Value::Int64(a), Value::Int64(b)) => return Ok(Value::Int64(a * b)),
                    (Value::Float32(a), Value::Float32(b)) => return Ok(Value::Float32(a * b)),
                    (Value::Float64(a), Value::Float64(b)) => return Ok(Value::Float64(a * b)),
                    (Value::Uint8(a), Value::Uint8(b)) => {
                        return Ok(Value::Uint8(a.saturating_mul(*b)))
                    }
                    (Value::Uint16(a), Value::Uint16(b)) => {
                        return Ok(Value::Uint16(a.saturating_mul(*b)))
                    }
                    (Value::Uint32(a), Value::Uint32(b)) => {
                        return Ok(Value::Uint32(a.saturating_mul(*b)))
                    }
                    (Value::Uint64(a), Value::Uint64(b)) => {
                        return Ok(Value::Uint64(a.saturating_mul(*b)))
                    }
                    _ => {}
                }

                // If types don't match, try numeric conversion
                let (left_num, right_num) = Self::try_cast_to_numeric(left, right)?;
                match (left_num, right_num) {
                    (Value::Int8(a), Value::Int8(b)) => Ok(Value::Int8(a.saturating_mul(b))),
                    (Value::Int16(a), Value::Int16(b)) => Ok(Value::Int16(a.saturating_mul(b))),
                    (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a.saturating_mul(b))),
                    (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
                    (Value::Float32(a), Value::Float32(b)) => Ok(Value::Float32(a * b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a * b)),
                    (Value::Uint8(a), Value::Uint8(b)) => Ok(Value::Uint8(a.saturating_mul(b))),
                    (Value::Uint16(a), Value::Uint16(b)) => Ok(Value::Uint16(a.saturating_mul(b))),
                    (Value::Uint32(a), Value::Uint32(b)) => Ok(Value::Uint32(a.saturating_mul(b))),
                    (Value::Uint64(a), Value::Uint64(b)) => Ok(Value::Uint64(a.saturating_mul(b))),
                    _ => unreachable!(),
                }
            }
            Self::Div => {
                // If types match, handle directly
                match (&left, &right) {
                    (Value::Int8(a), Value::Int8(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Int16(a), Value::Int16(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Int32(a), Value::Int32(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Int64(a), Value::Int64(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Float32(a), Value::Float32(b)) => {
                        if *b == 0.0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float32(a / b));
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if *b == 0.0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(a / b));
                    }
                    (Value::Uint8(a), Value::Uint8(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Uint16(a), Value::Uint16(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Uint32(a), Value::Uint32(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    (Value::Uint64(a), Value::Uint64(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(*a as f64 / *b as f64));
                    }
                    _ => {}
                }

                // If types don't match, convert to Float64 for precise results
                let left_f =
                    Self::try_cast_to_float64(&left).ok_or_else(|| EvalError::TypeMismatch {
                        expected: "Int64, Float64, or Uint8".to_string(),
                        actual: format!("{:?}", left),
                    })?;
                let right_f =
                    Self::try_cast_to_float64(&right).ok_or_else(|| EvalError::TypeMismatch {
                        expected: "Int64, Float64, or Uint8".to_string(),
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
                    (Value::Int8(a), Value::Int8(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Int8(a % b));
                    }
                    (Value::Int16(a), Value::Int16(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Int16(a % b));
                    }
                    (Value::Int32(a), Value::Int32(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Int32(a % b));
                    }
                    (Value::Int64(a), Value::Int64(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Int64(a % b));
                    }
                    (Value::Float32(a), Value::Float32(b)) => {
                        if *b == 0.0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float32(a % b));
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if *b == 0.0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Float64(a % b));
                    }
                    (Value::Uint8(a), Value::Uint8(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Uint8(a % b));
                    }
                    (Value::Uint16(a), Value::Uint16(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Uint16(a % b));
                    }
                    (Value::Uint32(a), Value::Uint32(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Uint32(a % b));
                    }
                    (Value::Uint64(a), Value::Uint64(b)) => {
                        if *b == 0 {
                            return Err(EvalError::DivisionByZero);
                        }
                        return Ok(Value::Uint64(a % b));
                    }
                    _ => {}
                }

                // If types don't match, try numeric conversion
                let (left_num, right_num) = Self::try_cast_to_numeric(left, right)?;
                match (left_num, right_num) {
                    (Value::Int8(a), Value::Int8(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Int8(a % b))
                        }
                    }
                    (Value::Int16(a), Value::Int16(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Int16(a % b))
                        }
                    }
                    (Value::Int32(a), Value::Int32(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Int32(a % b))
                        }
                    }
                    (Value::Int64(a), Value::Int64(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Int64(a % b))
                        }
                    }
                    (Value::Float32(a), Value::Float32(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float32(a % b))
                        }
                    }
                    (Value::Float64(a), Value::Float64(b)) => {
                        if b == 0.0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Float64(a % b))
                        }
                    }
                    (Value::Uint8(a), Value::Uint8(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Uint8(a % b))
                        }
                    }
                    (Value::Uint16(a), Value::Uint16(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Uint16(a % b))
                        }
                    }
                    (Value::Uint32(a), Value::Uint32(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Uint32(a % b))
                        }
                    }
                    (Value::Uint64(a), Value::Uint64(b)) => {
                        if b == 0 {
                            Err(EvalError::DivisionByZero)
                        } else {
                            Ok(Value::Uint64(a % b))
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
    TypeMismatch { expected: String, actual: String },
    /// Cast failed error
    CastFailed { from: String, to: String },
    /// Division by zero error
    DivisionByZero,
    /// Index out of bounds
    IndexOutOfBounds { index: usize, length: usize },
    /// Field not found error
    FieldNotFound {
        field_name: String,
        struct_type: String,
    },
    /// Invalid index type (for list indexing)
    InvalidIndexType { expected: String, actual: String },
    /// List index out of bounds
    ListIndexOutOfBounds { index: usize, list_length: usize },
    /// Feature not implemented
    NotImplemented { feature: String },
    /// Column not found error
    ColumnNotFound { source: String, column: String },
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
            EvalError::FieldNotFound {
                field_name,
                struct_type,
            } => {
                write!(
                    f,
                    "Field '{}' not found in struct type {}",
                    field_name, struct_type
                )
            }
            EvalError::InvalidIndexType { expected, actual } => {
                write!(
                    f,
                    "Invalid index type: expected {}, got {}",
                    expected, actual
                )
            }
            EvalError::ListIndexOutOfBounds { index, list_length } => {
                write!(
                    f,
                    "List index {} out of bounds for length {}",
                    index, list_length
                )
            }
            EvalError::NotImplemented { feature } => {
                write!(f, "Feature not implemented: {}", feature)
            }
            EvalError::ColumnNotFound { source, column } => {
                write!(f, "Column not found: {}.{}", source, column)
            }
        }
    }
}

impl std::error::Error for EvalError {}
