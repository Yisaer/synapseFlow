use datatypes::{ConcreteDatatype, DataType, Float64Type, Int64Type, Value};

/// Unary function that takes one argument
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryFunc {
    /// Logical NOT
    Not,
    /// Arithmetic negation (unary `-`)
    Neg,
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
            Self::Neg => {
                if arg.is_null() {
                    return Ok(Value::Null);
                }
                match arg {
                    Value::Int8(v) => Ok(Value::Int8(v.wrapping_neg())),
                    Value::Int16(v) => Ok(Value::Int16(v.wrapping_neg())),
                    Value::Int32(v) => Ok(Value::Int32(v.wrapping_neg())),
                    Value::Int64(v) => Ok(Value::Int64(v.wrapping_neg())),
                    Value::Float32(v) => Ok(Value::Float32(-v)),
                    Value::Float64(v) => Ok(Value::Float64(-v)),
                    other => Err(EvalError::TypeMismatch {
                        expected: "Int* or Float*".to_string(),
                        actual: format!("{:?}", other),
                    }),
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
                    ConcreteDatatype::Timestamp(t) => {
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
    /// Logical AND
    And,
    /// Logical OR
    Or,
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

    fn compare_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
        super::value_compare::compare_values(left, right)
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
                    Ok(Value::Bool(super::value_compare::values_equal(
                        &left, &right,
                    )))
                }
            }
            Self::NotEq => {
                // Null != Null is false, Null != anything else is true
                if left.is_null() && right.is_null() {
                    Ok(Value::Bool(false))
                } else if left.is_null() || right.is_null() {
                    Ok(Value::Bool(true))
                } else {
                    Ok(Value::Bool(!super::value_compare::values_equal(
                        &left, &right,
                    )))
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
                if left.is_null() || right.is_null() {
                    return Ok(Value::Null);
                }
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
                        (l, r) => {
                            return Err(EvalError::TypeMismatch {
                                expected: "numeric pair".to_string(),
                                actual: format!("{:?} and {:?}", l, r),
                            })
                        }
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
                if left.is_null() || right.is_null() {
                    return Ok(Value::Null);
                }
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
                    (l, r) => Err(EvalError::TypeMismatch {
                        expected: "numeric pair".to_string(),
                        actual: format!("{:?} and {:?}", l, r),
                    }),
                }
            }
            Self::Mul => {
                if left.is_null() || right.is_null() {
                    return Ok(Value::Null);
                }
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
                    (l, r) => Err(EvalError::TypeMismatch {
                        expected: "numeric pair".to_string(),
                        actual: format!("{:?} and {:?}", l, r),
                    }),
                }
            }
            Self::Div => {
                if left.is_null() || right.is_null() {
                    return Ok(Value::Null);
                }
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
                if left.is_null() || right.is_null() {
                    return Ok(Value::Null);
                }
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
                    (l, r) => Err(EvalError::TypeMismatch {
                        expected: "numeric pair".to_string(),
                        actual: format!("{:?} and {:?}", l, r),
                    }),
                }
            }
            Self::And => {
                let left_bool = match left {
                    Value::Bool(v) => v,
                    Value::Null => false,
                    other => {
                        return Err(EvalError::TypeMismatch {
                            expected: "Bool".to_string(),
                            actual: format!("{:?}", other),
                        });
                    }
                };
                let right_bool = match right {
                    Value::Bool(v) => v,
                    Value::Null => false,
                    other => {
                        return Err(EvalError::TypeMismatch {
                            expected: "Bool".to_string(),
                            actual: format!("{:?}", other),
                        });
                    }
                };
                Ok(Value::Bool(left_bool && right_bool))
            }
            Self::Or => {
                let left_bool = match left {
                    Value::Bool(v) => v,
                    Value::Null => false,
                    other => {
                        return Err(EvalError::TypeMismatch {
                            expected: "Bool".to_string(),
                            actual: format!("{:?}", other),
                        });
                    }
                };
                let right_bool = match right {
                    Value::Bool(v) => v,
                    Value::Null => false,
                    other => {
                        return Err(EvalError::TypeMismatch {
                            expected: "Bool".to_string(),
                            actual: format!("{:?}", other),
                        });
                    }
                };
                Ok(Value::Bool(left_bool || right_bool))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BinaryFunc;
    use datatypes::Value;

    #[test]
    fn arithmetic_ops_propagate_null() {
        let cases = [
            BinaryFunc::Add,
            BinaryFunc::Sub,
            BinaryFunc::Mul,
            BinaryFunc::Div,
            BinaryFunc::Mod,
        ];

        for func in cases {
            assert_eq!(
                func.eval_binary(Value::Null, Value::Int64(1)).unwrap(),
                Value::Null
            );
            assert_eq!(
                func.eval_binary(Value::Int64(1), Value::Null).unwrap(),
                Value::Null
            );
        }
    }

    #[test]
    fn null_division_does_not_error() {
        assert_eq!(
            BinaryFunc::Div
                .eval_binary(Value::Null, Value::Int64(0))
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            BinaryFunc::Div
                .eval_binary(Value::Int64(1), Value::Null)
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn null_string_concat_returns_null() {
        assert_eq!(
            BinaryFunc::Add
                .eval_binary(Value::Null, Value::String("x".to_string()))
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            BinaryFunc::Add
                .eval_binary(Value::String("x".to_string()), Value::Null)
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn null_equality_semantics() {
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Null, Value::Null)
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Null, Value::Int64(1))
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Int64(1), Value::Null)
                .unwrap(),
            Value::Bool(false)
        );

        assert_eq!(
            BinaryFunc::NotEq
                .eval_binary(Value::Null, Value::Null)
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            BinaryFunc::NotEq
                .eval_binary(Value::Null, Value::Int64(1))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::NotEq
                .eval_binary(Value::Int64(1), Value::Null)
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn null_ordering_comparisons_are_false() {
        for func in [
            BinaryFunc::Lt,
            BinaryFunc::Lte,
            BinaryFunc::Gt,
            BinaryFunc::Gte,
        ] {
            assert_eq!(
                func.eval_binary(Value::Null, Value::Int64(1)).unwrap(),
                Value::Bool(false)
            );
            assert_eq!(
                func.eval_binary(Value::Int64(1), Value::Null).unwrap(),
                Value::Bool(false)
            );
            assert_eq!(
                func.eval_binary(Value::Null, Value::Null).unwrap(),
                Value::Bool(false)
            );
        }
    }

    // ========== UnaryFunc Tests ==========

    #[test]
    fn unary_not_on_bool() {
        use super::UnaryFunc;
        assert_eq!(
            UnaryFunc::Not.eval_unary(Value::Bool(true)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            UnaryFunc::Not.eval_unary(Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn unary_not_type_error() {
        use super::UnaryFunc;
        let result = UnaryFunc::Not.eval_unary(Value::Int64(1));
        assert!(result.is_err());
    }

    #[test]
    fn unary_neg_integers() {
        use super::UnaryFunc;
        assert_eq!(
            UnaryFunc::Neg.eval_unary(Value::Int64(42)).unwrap(),
            Value::Int64(-42)
        );
        assert_eq!(
            UnaryFunc::Neg.eval_unary(Value::Int32(10)).unwrap(),
            Value::Int32(-10)
        );
        assert_eq!(
            UnaryFunc::Neg.eval_unary(Value::Int16(5)).unwrap(),
            Value::Int16(-5)
        );
        assert_eq!(
            UnaryFunc::Neg.eval_unary(Value::Int8(3)).unwrap(),
            Value::Int8(-3)
        );
    }

    #[test]
    fn unary_neg_floats() {
        use super::UnaryFunc;
        assert_eq!(
            UnaryFunc::Neg.eval_unary(Value::Float64(3.14)).unwrap(),
            Value::Float64(-3.14)
        );
        assert_eq!(
            UnaryFunc::Neg.eval_unary(Value::Float32(2.5)).unwrap(),
            Value::Float32(-2.5)
        );
    }

    #[test]
    fn unary_neg_null() {
        use super::UnaryFunc;
        assert_eq!(UnaryFunc::Neg.eval_unary(Value::Null).unwrap(), Value::Null);
    }

    #[test]
    fn unary_neg_type_error() {
        use super::UnaryFunc;
        let result = UnaryFunc::Neg.eval_unary(Value::String("hello".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn unary_is_null() {
        use super::UnaryFunc;
        assert_eq!(
            UnaryFunc::IsNull.eval_unary(Value::Null).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            UnaryFunc::IsNull.eval_unary(Value::Int64(1)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn unary_is_true_is_false() {
        use super::UnaryFunc;
        assert_eq!(
            UnaryFunc::IsTrue.eval_unary(Value::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            UnaryFunc::IsTrue.eval_unary(Value::Bool(false)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            UnaryFunc::IsFalse.eval_unary(Value::Bool(true)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            UnaryFunc::IsFalse.eval_unary(Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn unary_is_true_type_error() {
        use super::UnaryFunc;
        assert!(UnaryFunc::IsTrue.eval_unary(Value::Int64(1)).is_err());
        assert!(UnaryFunc::IsFalse.eval_unary(Value::Int64(0)).is_err());
    }

    #[test]
    fn unary_cast_int64() {
        use super::UnaryFunc;
        use datatypes::{ConcreteDatatype, Int64Type};
        let cast = UnaryFunc::Cast(ConcreteDatatype::Int64(Int64Type));
        // Same type cast should work
        assert_eq!(cast.eval_unary(Value::Int64(42)).unwrap(), Value::Int64(42));
    }

    #[test]
    fn unary_cast_float64() {
        use super::UnaryFunc;
        use datatypes::{ConcreteDatatype, Float64Type};
        let cast = UnaryFunc::Cast(ConcreteDatatype::Float64(Float64Type));
        assert_eq!(
            cast.eval_unary(Value::Int64(42)).unwrap(),
            Value::Float64(42.0)
        );
    }

    #[test]
    fn unary_cast_string() {
        use super::UnaryFunc;
        use datatypes::{ConcreteDatatype, StringType};
        let cast = UnaryFunc::Cast(ConcreteDatatype::String(StringType));
        // Same type cast should work
        assert_eq!(
            cast.eval_unary(Value::String("hello".to_string())).unwrap(),
            Value::String("hello".to_string())
        );
    }

    #[test]
    fn unary_cast_fails_incompatible() {
        use super::UnaryFunc;
        use datatypes::{ConcreteDatatype, StringType};
        let cast = UnaryFunc::Cast(ConcreteDatatype::String(StringType));
        // Incompatible cast should fail
        let result = cast.eval_unary(Value::Int64(123));
        assert!(result.is_err());
    }

    // ========== BinaryFunc Arithmetic Tests ==========

    #[test]
    fn binary_add_integers() {
        assert_eq!(
            BinaryFunc::Add
                .eval_binary(Value::Int64(10), Value::Int64(5))
                .unwrap(),
            Value::Int64(15)
        );
        assert_eq!(
            BinaryFunc::Add
                .eval_binary(Value::Int32(10), Value::Int32(5))
                .unwrap(),
            Value::Int32(15)
        );
    }

    #[test]
    fn binary_add_floats() {
        assert_eq!(
            BinaryFunc::Add
                .eval_binary(Value::Float64(1.5), Value::Float64(2.5))
                .unwrap(),
            Value::Float64(4.0)
        );
    }

    #[test]
    fn binary_add_strings() {
        assert_eq!(
            BinaryFunc::Add
                .eval_binary(
                    Value::String("hello".to_string()),
                    Value::String(" world".to_string())
                )
                .unwrap(),
            Value::String("hello world".to_string())
        );
    }

    #[test]
    fn binary_sub_integers() {
        assert_eq!(
            BinaryFunc::Sub
                .eval_binary(Value::Int64(10), Value::Int64(3))
                .unwrap(),
            Value::Int64(7)
        );
    }

    #[test]
    fn binary_mul_integers() {
        assert_eq!(
            BinaryFunc::Mul
                .eval_binary(Value::Int64(6), Value::Int64(7))
                .unwrap(),
            Value::Int64(42)
        );
    }

    #[test]
    fn binary_div_integers() {
        assert_eq!(
            BinaryFunc::Div
                .eval_binary(Value::Int64(10), Value::Int64(4))
                .unwrap(),
            Value::Float64(2.5)
        );
    }

    #[test]
    fn binary_div_by_zero_error() {
        let result = BinaryFunc::Div.eval_binary(Value::Int64(10), Value::Int64(0));
        assert!(matches!(result, Err(super::EvalError::DivisionByZero)));
    }

    #[test]
    fn binary_mod_integers() {
        assert_eq!(
            BinaryFunc::Mod
                .eval_binary(Value::Int64(10), Value::Int64(3))
                .unwrap(),
            Value::Int64(1)
        );
    }

    #[test]
    fn binary_mod_by_zero_error() {
        let result = BinaryFunc::Mod.eval_binary(Value::Int64(10), Value::Int64(0));
        assert!(matches!(result, Err(super::EvalError::DivisionByZero)));
    }

    // ========== BinaryFunc Comparison Tests ==========

    #[test]
    fn binary_comparisons_int64() {
        assert_eq!(
            BinaryFunc::Lt
                .eval_binary(Value::Int64(5), Value::Int64(10))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::Lte
                .eval_binary(Value::Int64(5), Value::Int64(5))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::Gt
                .eval_binary(Value::Int64(10), Value::Int64(5))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::Gte
                .eval_binary(Value::Int64(10), Value::Int64(10))
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn binary_equality_int64() {
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Int64(42), Value::Int64(42))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Int64(42), Value::Int64(43))
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            BinaryFunc::NotEq
                .eval_binary(Value::Int64(42), Value::Int64(43))
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn eq_coerces_across_numeric_types() {
        // Float vs int literal must compare by value (regression: a Float64
        // signal compared with an integer literal, e.g. `speed = 0`).
        let t = Value::Bool(true);
        let f = Value::Bool(false);
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Float64(0.0), Value::Int64(0))
                .unwrap(),
            t
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Int64(1), Value::Float64(1.0))
                .unwrap(),
            t
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Float64(0.5), Value::Int64(0))
                .unwrap(),
            f
        );
        assert_eq!(
            BinaryFunc::NotEq
                .eval_binary(Value::Float64(0.0), Value::Int64(0))
                .unwrap(),
            f
        );
        // same-type equality still works
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::Int64(3), Value::Int64(3))
                .unwrap(),
            t
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::String("a".into()), Value::String("a".into()))
                .unwrap(),
            t
        );
        assert_eq!(
            BinaryFunc::Eq
                .eval_binary(Value::String("a".into()), Value::String("b".into()))
                .unwrap(),
            f
        );
    }

    // ========== BinaryFunc Logical Tests ==========

    #[test]
    fn binary_and_or() {
        assert_eq!(
            BinaryFunc::And
                .eval_binary(Value::Bool(true), Value::Bool(true))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::And
                .eval_binary(Value::Bool(true), Value::Bool(false))
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            BinaryFunc::Or
                .eval_binary(Value::Bool(false), Value::Bool(true))
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            BinaryFunc::Or
                .eval_binary(Value::Bool(false), Value::Bool(false))
                .unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn binary_and_or_with_null() {
        assert_eq!(
            BinaryFunc::And
                .eval_binary(Value::Null, Value::Bool(true))
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            BinaryFunc::Or
                .eval_binary(Value::Null, Value::Bool(true))
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn binary_and_or_type_error() {
        let result = BinaryFunc::And.eval_binary(Value::Int64(1), Value::Bool(true));
        assert!(result.is_err());
        let result = BinaryFunc::Or.eval_binary(Value::Bool(true), Value::Int64(1));
        assert!(result.is_err());
    }

    // ========== EvalError Display Tests ==========

    #[test]
    fn eval_error_display() {
        use super::EvalError;
        let err = EvalError::TypeMismatch {
            expected: "Bool".to_string(),
            actual: "Int64".to_string(),
        };
        assert!(err.to_string().contains("Type mismatch"));

        let err = EvalError::CastFailed {
            from: "String".to_string(),
            to: "Int64".to_string(),
        };
        assert!(err.to_string().contains("cast"));

        let err = EvalError::DivisionByZero;
        assert!(err.to_string().contains("Division by zero"));

        let err = EvalError::IndexOutOfBounds {
            index: 5,
            length: 3,
        };
        assert!(err.to_string().contains("out of bounds"));

        let err = EvalError::FieldNotFound {
            field_name: "foo".to_string(),
            struct_type: "Bar".to_string(),
        };
        assert!(err.to_string().contains("not found"));

        let err = EvalError::InvalidIndexType {
            expected: "Int64".to_string(),
            actual: "String".to_string(),
        };
        assert!(err.to_string().contains("Invalid index type"));

        let err = EvalError::ListIndexOutOfBounds {
            index: 10,
            list_length: 5,
        };
        assert!(err.to_string().contains("List index"));

        let err = EvalError::NotImplemented {
            feature: "test".to_string(),
        };
        assert!(err.to_string().contains("not implemented"));

        let err = EvalError::ColumnNotFound {
            source: "src".to_string(),
            column: "col".to_string(),
        };
        assert!(err.to_string().contains("Column not found"));
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
