use datatypes::Value;

pub fn bool_arg(name: &str, value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(v) => Ok(*v),
        other => Err(format!("{name} must be bool, got {other:?}")),
    }
}

/// Resolve a per-row boolean *condition* argument.
///
/// Unlike [`bool_arg`] (used for static `ignore_null` config literals), the
/// `condition` of the `consecutive_*` functions is an arbitrary expression
/// evaluated every row. A `NULL` condition means "the condition does not hold"
/// and is treated as `false`; any non-boolean value is a hard error.
pub fn bool_condition(name: &str, value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(v) => Ok(*v),
        Value::Null => Ok(false),
        other => Err(format!("{name} must be bool, got {other:?}")),
    }
}

pub fn normalize_state_value(value: &Value) -> Option<Value> {
    if value.is_null() {
        None
    } else {
        Some(value.clone())
    }
}

pub fn lag_offset(value: &Value) -> Result<usize, String> {
    let offset = match value {
        Value::Int8(v) => i64::from(*v),
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        Value::Uint8(v) => i64::from(*v),
        Value::Uint16(v) => i64::from(*v),
        Value::Uint32(v) => i64::from(*v),
        Value::Uint64(v) => i64::try_from(*v)
            .map_err(|_| format!("lag() offset is too large for i64: {value:?}"))?,
        other => {
            return Err(format!(
                "lag() second argument must be an integer, got {other:?}"
            ))
        }
    };

    usize::try_from(offset)
        .ok()
        .filter(|offset| *offset >= 1)
        .ok_or_else(|| format!("lag() offset must be >= 1, got {offset}"))
}
