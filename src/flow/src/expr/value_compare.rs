use datatypes::{DataType, Float32Type, Float64Type, Int64Type, StringType, Value};
use std::cmp::Ordering;

fn try_cast_to_int64(value: &Value) -> Option<i64> {
    let int64_type = Int64Type;
    int64_type.try_cast(value.clone()).and_then(|v| match v {
        Value::Int64(i) => Some(i),
        _ => None,
    })
}

fn try_cast_to_float32(value: &Value) -> Option<f32> {
    let float32_type = Float32Type;
    float32_type.try_cast(value.clone()).and_then(|v| match v {
        Value::Float32(f) => Some(f),
        _ => None,
    })
}

fn try_cast_to_float64(value: &Value) -> Option<f64> {
    let float64_type = Float64Type;
    float64_type.try_cast(value.clone()).and_then(|v| match v {
        Value::Float64(f) => Some(f),
        _ => None,
    })
}

fn try_cast_to_string(value: &Value) -> Option<String> {
    let string_type = StringType;
    string_type.try_cast(value.clone()).and_then(|v| match v {
        Value::String(s) => Some(s),
        _ => None,
    })
}

/// Returns true when both values are non-NULL and compare as equal.
///
/// Equality is type-strict for same-datatype values, but cross-type
/// numeric coercion is supported (e.g. `Int64(0) == Float64(0.0)`).
/// String or other cross-type fallback is NOT performed for equality.
/// NULL is never equal to anything (including another NULL). Callers
/// that need SQL-style NULL = NULL semantics must handle NULL before
/// calling this function.
pub fn values_equal(left: &Value, right: &Value) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    if left == right {
        return true;
    }
    if is_numeric_value(left) && is_numeric_value(right) {
        return compare_values(left, right) == Some(Ordering::Equal);
    }
    false
}

fn is_numeric_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Float32(_)
            | Value::Float64(_)
            | Value::Int8(_)
            | Value::Int16(_)
            | Value::Int32(_)
            | Value::Int64(_)
            | Value::Uint8(_)
            | Value::Uint16(_)
            | Value::Uint32(_)
            | Value::Uint64(_)
    )
}

/// Compare two values by trying to cast them to comparable types.
///
/// - `NULL` is not comparable and yields `None`.
/// - If types match, compare directly.
/// - If types don't match, try to cast both sides to a common type using this order:
///   `Int64` -> `Float32` -> `Float64` -> `String`.
pub fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    if left.is_null() || right.is_null() {
        return None;
    }

    match (left, right) {
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
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        _ => {
            if let (Some(a), Some(b)) = (try_cast_to_int64(left), try_cast_to_int64(right)) {
                return Some(a.cmp(&b));
            }
            if let (Some(a), Some(b)) = (try_cast_to_float32(left), try_cast_to_float32(right)) {
                return a.partial_cmp(&b);
            }
            if let (Some(a), Some(b)) = (try_cast_to_float64(left), try_cast_to_float64(right)) {
                return a.partial_cmp(&b);
            }
            if let (Some(a), Some(b)) = (try_cast_to_string(left), try_cast_to_string(right)) {
                return Some(a.cmp(&b));
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_same_int_types() {
        assert_eq!(
            compare_values(&Value::Int64(5), &Value::Int64(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Int64(10), &Value::Int64(5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare_values(&Value::Int64(5), &Value::Int64(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Int32(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Int16(5), &Value::Int16(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Int8(5), &Value::Int8(10)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn compare_same_uint_types() {
        assert_eq!(
            compare_values(&Value::Uint64(5), &Value::Uint64(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Uint32(5), &Value::Uint32(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Uint16(5), &Value::Uint16(10)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Uint8(5), &Value::Uint8(10)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn compare_same_float_types() {
        assert_eq!(
            compare_values(&Value::Float64(1.5), &Value::Float64(2.5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Float32(1.5), &Value::Float32(2.5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Float64(2.5), &Value::Float64(2.5)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn compare_strings() {
        assert_eq!(
            compare_values(
                &Value::String("apple".to_string()),
                &Value::String("banana".to_string())
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(
                &Value::String("zebra".to_string()),
                &Value::String("apple".to_string())
            ),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn compare_bools() {
        assert_eq!(
            compare_values(&Value::Bool(false), &Value::Bool(true)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_values(&Value::Bool(true), &Value::Bool(true)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn compare_null_returns_none() {
        assert_eq!(compare_values(&Value::Null, &Value::Int64(5)), None);
        assert_eq!(compare_values(&Value::Int64(5), &Value::Null), None);
        assert_eq!(compare_values(&Value::Null, &Value::Null), None);
    }

    #[test]
    fn compare_cross_type_int_to_int64() {
        // Int32 vs Int64 should be cast to Int64
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Int64(10)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn compare_cross_type_int_to_float64() {
        // Int64 vs Float64 should be cast to Float64
        assert_eq!(
            compare_values(&Value::Int64(5), &Value::Float64(5.5)),
            Some(Ordering::Less)
        );
    }

    // ========== values_equal tests ==========

    #[test]
    fn values_equal_same_type_int() {
        assert!(values_equal(&Value::Int64(5), &Value::Int64(5)));
        assert!(!values_equal(&Value::Int64(5), &Value::Int64(10)));
    }

    #[test]
    fn values_equal_same_type_float() {
        assert!(values_equal(&Value::Float64(1.5), &Value::Float64(1.5)));
        assert!(!values_equal(&Value::Float64(1.5), &Value::Float64(2.5)));
    }

    #[test]
    fn values_equal_same_type_string() {
        assert!(values_equal(
            &Value::String("a".to_string()),
            &Value::String("a".to_string())
        ));
        assert!(!values_equal(
            &Value::String("a".to_string()),
            &Value::String("b".to_string())
        ));
    }

    #[test]
    fn values_equal_same_type_bool() {
        assert!(values_equal(&Value::Bool(true), &Value::Bool(true)));
        assert!(!values_equal(&Value::Bool(true), &Value::Bool(false)));
    }

    #[test]
    fn values_equal_cross_type_numeric() {
        // Int vs Float across different integer sizes
        assert!(values_equal(&Value::Int64(0), &Value::Float64(0.0)));
        assert!(values_equal(&Value::Float64(0.0), &Value::Int64(0)));
        assert!(values_equal(&Value::Int64(1), &Value::Float64(1.0)));
        assert!(values_equal(&Value::Float32(1.0), &Value::Int64(1)));
        assert!(values_equal(&Value::Int32(1), &Value::Int64(1)));
        assert!(!values_equal(&Value::Float64(0.5), &Value::Int64(0)));
    }

    #[test]
    fn values_equal_rejects_non_numeric_cross_type() {
        // String "1" vs Int64(1) must NOT compare equal — no String fallback.
        assert!(!values_equal(
            &Value::String("1".to_string()),
            &Value::Int64(1)
        ));
        // Bool vs Int is not numeric cross-type.
        assert!(!values_equal(&Value::Bool(true), &Value::Int64(1)));
    }

    #[test]
    fn values_equal_null_never_equal() {
        assert!(!values_equal(&Value::Null, &Value::Null));
        assert!(!values_equal(&Value::Null, &Value::Int64(1)));
        assert!(!values_equal(&Value::Int64(1), &Value::Null));
    }
}
