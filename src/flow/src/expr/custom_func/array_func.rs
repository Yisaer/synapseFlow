use super::helpers::*;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use crate::expr::value_compare;
use datatypes::{ConcreteDatatype, ListValue, Value};
use std::collections::HashSet;
use std::sync::Arc;

const MAX_ARRAY_OUTPUT_LEN: usize = 100_000;
const MAX_CONTAINS_ANY_FALLBACK_COMPARISONS: usize = 1_000_000;

type ArrayRefAndDatatype<'a> = (&'a [Value], &'a ConcreteDatatype);

#[derive(Debug, Clone)]
pub struct ArrayPositionFunc;

#[derive(Debug, Clone)]
pub struct ArrayLastPositionFunc;

#[derive(Debug, Clone)]
pub struct ElementAtFunc;

#[derive(Debug, Clone)]
pub struct ArrayContainsFunc;

#[derive(Debug, Clone)]
pub struct ArrayContainsAnyFunc;

#[derive(Debug, Clone)]
pub struct ArrayCreateFunc;

#[derive(Debug, Clone)]
pub struct ArrayRemoveFunc;

#[derive(Debug, Clone)]
pub struct RepeatFunc;

#[derive(Debug, Clone)]
pub struct SequenceFunc;

#[derive(Debug, Clone)]
pub struct ArrayConcatFunc;

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![
        array_position_function_def(),
        array_last_position_function_def(),
        element_at_function_def(),
        array_contains_function_def(),
        array_contains_any_function_def(),
        array_create_function_def(),
        array_remove_function_def(),
        repeat_function_def(),
        sequence_function_def(),
        array_concat_function_def(),
    ]
}

pub fn array_position_function_def() -> FunctionDef {
    scalar_function_def(
        "array_position",
        vec![req_arg("array", array_type()), req_arg("value", any_type())],
        int_type(),
        "Return the first zero-based position of a value in an array, or -1 if not found.",
        vec![
            "Requires exactly 2 arguments.",
            "Returns -1 if the array is NULL.",
            "Numeric values compare by numeric value across numeric datatypes.",
        ],
        vec!["SELECT array_position(a, 1)"],
    )
}

pub fn array_last_position_function_def() -> FunctionDef {
    scalar_function_def(
        "array_last_position",
        vec![req_arg("array", array_type()), req_arg("value", any_type())],
        int_type(),
        "Return the last zero-based position of a value in an array, or -1 if not found.",
        vec![
            "Requires exactly 2 arguments.",
            "Returns -1 if the array is NULL.",
            "Numeric values compare by numeric value across numeric datatypes.",
        ],
        vec!["SELECT array_last_position(a, 1)"],
    )
}

pub fn element_at_function_def() -> FunctionDef {
    scalar_function_def(
        "element_at",
        vec![req_arg("array", array_type()), req_arg("index", int_type())],
        any_type(),
        "Return the array element at a zero-based index, supporting negative indexes from the end.",
        vec![
            "Requires exactly 2 arguments.",
            "Returns NULL if the array is NULL.",
            "Returns NULL if the index is out of bounds.",
        ],
        vec!["SELECT element_at(a, 0)", "SELECT element_at(a, -1)"],
    )
}

pub fn array_contains_function_def() -> FunctionDef {
    scalar_function_def(
        "array_contains",
        vec![req_arg("array", array_type()), req_arg("value", any_type())],
        bool_type(),
        "Return whether an array contains a value.",
        vec![
            "Requires exactly 2 arguments.",
            "Returns false if the array is NULL.",
            "Numeric values compare by numeric value across numeric datatypes.",
        ],
        vec!["SELECT array_contains(a, 1)"],
    )
}

pub fn array_contains_any_function_def() -> FunctionDef {
    scalar_function_def(
        "array_contains_any",
        vec![
            req_arg("array1", array_type()),
            req_arg("array2", array_type()),
        ],
        bool_type(),
        "Return whether the first array contains any value from the second array.",
        vec![
            "Requires exactly 2 array arguments.",
            "Returns false if either array is NULL.",
            "Numeric values compare by numeric value across numeric datatypes.",
            "Mixed-datatype fallback scanning returns true on the first match and errors only after more than 1000000 comparisons without a match.",
        ],
        vec!["SELECT array_contains_any(a, b)"],
    )
}

pub fn array_create_function_def() -> FunctionDef {
    scalar_function_def(
        "array_create",
        vec![variadic_arg("value", any_type())],
        array_type(),
        "Create an array from the provided values.",
        vec![
            "Accepts any number of arguments.",
            "All non-NULL values must have the same datatype.",
        ],
        vec!["SELECT array_create(1, 2, 3)"],
    )
}

pub fn array_remove_function_def() -> FunctionDef {
    scalar_function_def(
        "array_remove",
        vec![req_arg("array", array_type()), req_arg("value", any_type())],
        array_type(),
        "Return a copy of the array with all matching values removed.",
        vec![
            "Requires exactly 2 arguments.",
            "Returns NULL if the array is NULL.",
            "Numeric values compare by numeric value across numeric datatypes.",
        ],
        vec!["SELECT array_remove(a, 1)"],
    )
}

pub fn repeat_function_def() -> FunctionDef {
    scalar_function_def(
        "repeat",
        vec![req_arg("value", any_type()), req_arg("count", int_type())],
        array_type(),
        "Return an array containing the value repeated count times.",
        vec![
            "Requires exactly 2 arguments.",
            "Returns an empty array when count is less than or equal to 0.",
            "The output length is bounded.",
        ],
        vec!["SELECT repeat(a, 3)"],
    )
}

pub fn sequence_function_def() -> FunctionDef {
    scalar_function_def(
        "sequence",
        vec![
            req_arg("start", int_type()),
            req_arg("stop", int_type()),
            opt_arg("step", int_type()),
        ],
        array_type(),
        "Return an Int64 array from start to stop using an optional step.",
        vec![
            "Accepts 2 or 3 integer arguments.",
            "The default step is 1 for ascending ranges and -1 for descending ranges.",
            "Step 0 is invalid.",
            "The output length is bounded.",
        ],
        vec!["SELECT sequence(1, 3)", "SELECT sequence(3, 1, -1)"],
    )
}

pub fn array_concat_function_def() -> FunctionDef {
    scalar_function_def(
        "array_concat",
        vec![variadic_arg("array", array_type())],
        array_type(),
        "Concatenate arrays with the same element datatype.",
        vec![
            "Requires at least 1 argument.",
            "Each argument must be an array or NULL.",
            "NULL arrays are skipped; if all arrays are NULL, returns NULL.",
            "All non-NULL arrays must have the same element datatype.",
            "The output length is bounded.",
        ],
        vec!["SELECT array_concat(a, b)"],
    )
}

fn values_equal(left: &Value, right: &Value) -> bool {
    if left.is_null() || right.is_null() {
        return left.is_null() && right.is_null();
    }
    value_compare::values_equal(left, right)
}

fn array_value_with_datatype(items: Vec<Value>, datatype: Arc<ConcreteDatatype>) -> Value {
    Value::List(ListValue::new(items, datatype))
}

fn empty_array_for_value(value: &Value) -> Value {
    array_value_with_datatype(Vec::new(), Arc::new(value.datatype()))
}

fn array_ref_or_null(args: &[Value], idx: usize) -> Result<Option<&[Value]>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(Value::List(list)) => Ok(Some(list.items())),
        Some(other) => Err(EvalError::TypeMismatch {
            expected: format!("array argument {}", idx),
            actual: format!("{:?}", other),
        }),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

fn array_ref_and_datatype(
    args: &[Value],
    idx: usize,
) -> Result<Option<ArrayRefAndDatatype<'_>>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(Value::List(list)) => Ok(Some((list.items(), list.datatype()))),
        Some(other) => Err(EvalError::TypeMismatch {
            expected: format!("array argument {}", idx),
            actual: format!("{:?}", other),
        }),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

fn ensure_array_output_len(len: usize) -> Result<(), EvalError> {
    if len > MAX_ARRAY_OUTPUT_LEN {
        return Err(EvalError::TypeMismatch {
            expected: format!("array output length <= {MAX_ARRAY_OUTPUT_LEN}"),
            actual: len.to_string(),
        });
    }
    Ok(())
}

fn value_to_index(value: &Value) -> Result<i64, EvalError> {
    match value {
        Value::Int8(v) => Ok(*v as i64),
        Value::Int16(v) => Ok(*v as i64),
        Value::Int32(v) => Ok(*v as i64),
        Value::Int64(v) => Ok(*v),
        Value::Uint8(v) => Ok(*v as i64),
        Value::Uint16(v) => Ok(*v as i64),
        Value::Uint32(v) => Ok(*v as i64),
        Value::Uint64(v) => i64::try_from(*v).map_err(|_| EvalError::TypeMismatch {
            expected: "int64-compatible index".to_string(),
            actual: format!("{:?}", value),
        }),
        other => Err(EvalError::TypeMismatch {
            expected: "integer index".to_string(),
            actual: format!("{:?}", other),
        }),
    }
}

fn value_to_count(value: &Value) -> Result<i64, EvalError> {
    value_to_index(value)
}

fn position(values: &[Value], needle: &Value) -> i64 {
    values
        .iter()
        .position(|value| values_equal(value, needle))
        .map(|idx| idx as i64)
        .unwrap_or(-1)
}

fn last_position(values: &[Value], needle: &Value) -> i64 {
    values
        .iter()
        .rposition(|value| values_equal(value, needle))
        .map(|idx| idx as i64)
        .unwrap_or(-1)
}

fn contains_any_same_type(left: &[Value], right: &[Value]) -> bool {
    let (smaller, larger) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    let lookup: HashSet<&Value> = smaller.iter().collect();
    larger.iter().any(|value| lookup.contains(value))
}

fn contains_any_relaxed(left: &[Value], right: &[Value]) -> Result<bool, EvalError> {
    let mut comparisons = 0usize;

    for left_value in left {
        for right_value in right {
            if values_equal(left_value, right_value) {
                return Ok(true);
            }

            comparisons = comparisons.checked_add(1).ok_or_else(|| {
                EvalError::TypeMismatch {
                    expected: format!(
                        "array_contains_any fallback comparisons <= {MAX_CONTAINS_ANY_FALLBACK_COMPARISONS}"
                    ),
                    actual: "overflow".to_string(),
                }
            })?;

            if comparisons > MAX_CONTAINS_ANY_FALLBACK_COMPARISONS {
                return Err(EvalError::TypeMismatch {
                    expected: format!(
                        "array_contains_any fallback comparisons <= {MAX_CONTAINS_ANY_FALLBACK_COMPARISONS}"
                    ),
                    actual: comparisons.to_string(),
                });
            }
        }
    }

    Ok(false)
}

impl CustomFunc for ArrayPositionFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        array_ref_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(values) = array_ref_or_null(args, 0)? else {
            return Ok(Value::Int64(-1));
        };
        Ok(Value::Int64(position(values, &args[1])))
    }

    fn name(&self) -> &str {
        "array_position"
    }
}

impl CustomFunc for ArrayLastPositionFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        array_ref_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(values) = array_ref_or_null(args, 0)? else {
            return Ok(Value::Int64(-1));
        };
        Ok(Value::Int64(last_position(values, &args[1])))
    }

    fn name(&self) -> &str {
        "array_last_position"
    }
}

impl CustomFunc for ElementAtFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        array_ref_or_null(args, 0)?;
        value_to_index(&args[1])?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(values) = array_ref_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let len = values.len() as i64;
        let index = value_to_index(&args[1])?;
        let normalized = if index < 0 { len + index } else { index };

        if normalized < 0 || normalized >= len {
            return Ok(Value::Null);
        }

        Ok(values[normalized as usize].clone())
    }

    fn name(&self) -> &str {
        "element_at"
    }
}

impl CustomFunc for ArrayContainsFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        array_ref_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(values) = array_ref_or_null(args, 0)? else {
            return Ok(Value::Bool(false));
        };
        Ok(Value::Bool(position(values, &args[1]) >= 0))
    }

    fn name(&self) -> &str {
        "array_contains"
    }
}

impl CustomFunc for ArrayContainsAnyFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        array_ref_and_datatype(args, 0)?;
        array_ref_and_datatype(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some((left, left_type)) = array_ref_and_datatype(args, 0)? else {
            return Ok(Value::Bool(false));
        };
        let Some((right, right_type)) = array_ref_and_datatype(args, 1)? else {
            return Ok(Value::Bool(false));
        };

        if left_type == right_type {
            return Ok(Value::Bool(contains_any_same_type(left, right)));
        }

        Ok(Value::Bool(contains_any_relaxed(left, right)?))
    }

    fn name(&self) -> &str {
        "array_contains_any"
    }
}

impl CustomFunc for ArrayCreateFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        ensure_array_output_len(args.len())?;
        infer_list_datatype(args)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        ensure_array_output_len(args.len())?;
        let datatype = infer_list_datatype(args)?;
        Ok(array_value_with_datatype(args.to_vec(), datatype))
    }

    fn name(&self) -> &str {
        "array_create"
    }
}

impl CustomFunc for ArrayRemoveFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        array_ref_and_datatype(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some((values, datatype)) = array_ref_and_datatype(args, 0)? else {
            return Ok(Value::Null);
        };

        let out: Vec<Value> = values
            .iter()
            .filter(|value| !values_equal(value, &args[1]))
            .cloned()
            .collect();

        Ok(array_value_with_datatype(out, Arc::new(datatype.clone())))
    }

    fn name(&self) -> &str {
        "array_remove"
    }
}

impl CustomFunc for RepeatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        let count = value_to_count(&args[1])?;
        if count > 0 {
            ensure_array_output_len(count as usize)?;
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        self.validate_row(args)?;
        let count = value_to_count(&args[1])?;
        if count <= 0 {
            return Ok(empty_array_for_value(&args[0]));
        }

        let items = vec![args[0].clone(); count as usize];
        Ok(array_value_with_datatype(
            items,
            Arc::new(args[0].datatype()),
        ))
    }

    fn name(&self) -> &str {
        "repeat"
    }
}

impl CustomFunc for SequenceFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2, 3])?;
        let start = value_to_index(&args[0])?;
        let stop = value_to_index(&args[1])?;
        let step = match args.get(2) {
            Some(value) => value_to_index(value)?,
            None if start <= stop => 1,
            None => -1,
        };

        if step == 0 {
            return Err(EvalError::TypeMismatch {
                expected: "non-zero sequence step".to_string(),
                actual: "0".to_string(),
            });
        }

        ensure_array_output_len(sequence_len(start, stop, step)?)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        self.validate_row(args)?;
        let start = value_to_index(&args[0])?;
        let stop = value_to_index(&args[1])?;
        let step = match args.get(2) {
            Some(value) => value_to_index(value)?,
            None if start <= stop => 1,
            None => -1,
        };

        let len = sequence_len(start, stop, step)?;
        let mut items = Vec::with_capacity(len);
        let mut current = start;
        for idx in 0..len {
            items.push(Value::Int64(current));
            if idx + 1 == len {
                break;
            }
            current = current
                .checked_add(step)
                .ok_or_else(|| EvalError::TypeMismatch {
                    expected: "sequence value without integer overflow".to_string(),
                    actual: format!("{current} + {step}"),
                })?;
        }

        array_to_value(items)
    }

    fn name(&self) -> &str {
        "sequence"
    }
}

impl CustomFunc for ArrayConcatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.is_empty() {
            return Err(EvalError::TypeMismatch {
                expected: "at least 1 argument".to_string(),
                actual: "0 arguments".to_string(),
            });
        }

        let mut datatype: Option<Arc<ConcreteDatatype>> = None;
        let mut len = 0usize;
        for (idx, _) in args.iter().enumerate() {
            let Some((values, current_type)) = array_ref_and_datatype(args, idx)? else {
                continue;
            };

            if let Some(expected_type) = &datatype {
                if expected_type.as_ref() != current_type {
                    return Err(EvalError::TypeMismatch {
                        expected: format!("array element datatype {:?}", expected_type),
                        actual: format!("{:?}", current_type),
                    });
                }
            } else {
                datatype = Some(Arc::new(current_type.clone()));
            }

            len = len
                .checked_add(values.len())
                .ok_or_else(|| EvalError::TypeMismatch {
                    expected: format!("array output length <= {MAX_ARRAY_OUTPUT_LEN}"),
                    actual: "overflow".to_string(),
                })?;
            ensure_array_output_len(len)?;
        }

        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        if args.is_empty() {
            return Err(EvalError::TypeMismatch {
                expected: "at least 1 argument".to_string(),
                actual: "0 arguments".to_string(),
            });
        }

        let mut datatype: Option<Arc<ConcreteDatatype>> = None;
        let mut out = Vec::new();
        for (idx, _) in args.iter().enumerate() {
            let Some((values, current_type)) = array_ref_and_datatype(args, idx)? else {
                continue;
            };

            if let Some(expected_type) = &datatype {
                if expected_type.as_ref() != current_type {
                    return Err(EvalError::TypeMismatch {
                        expected: format!("array element datatype {:?}", expected_type),
                        actual: format!("{:?}", current_type),
                    });
                }
            } else {
                datatype = Some(Arc::new(current_type.clone()));
            }

            let new_len =
                out.len()
                    .checked_add(values.len())
                    .ok_or_else(|| EvalError::TypeMismatch {
                        expected: format!("array output length <= {MAX_ARRAY_OUTPUT_LEN}"),
                        actual: "overflow".to_string(),
                    })?;
            ensure_array_output_len(new_len)?;
            out.extend(values.iter().cloned());
        }

        let Some(datatype) = datatype else {
            return Ok(Value::Null);
        };

        Ok(array_value_with_datatype(out, datatype))
    }

    fn name(&self) -> &str {
        "array_concat"
    }
}

fn sequence_len(start: i64, stop: i64, step: i64) -> Result<usize, EvalError> {
    if step > 0 && start > stop {
        return Ok(0);
    }
    if step < 0 && start < stop {
        return Ok(0);
    }

    let distance = if step > 0 {
        (stop as i128) - (start as i128)
    } else {
        (start as i128) - (stop as i128)
    };
    let step_abs = (step as i128).abs();
    let len = distance / step_abs + 1;

    usize::try_from(len).map_err(|_| EvalError::TypeMismatch {
        expected: format!("array output length <= {MAX_ARRAY_OUTPUT_LEN}"),
        actual: len.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::custom_func::CustomFunc;

    fn int8_array(values: &[i8]) -> Value {
        array_to_value(values.iter().map(|v| Value::Int8(*v)).collect()).unwrap()
    }

    fn assert_int8(actual: Value, expected: i8) {
        match actual {
            Value::Int8(v) => assert_eq!(v, expected),
            other => panic!("expected Int8({expected}), got {:?}", other),
        }
    }

    #[test]
    fn array_position_matches_cross_numeric_values() {
        let func = ArrayPositionFunc;

        assert_int(
            func.eval_row(&[int8_array(&[0, 1, 2]), Value::Int64(1)])
                .unwrap(),
            1,
        );
        assert_int(
            func.eval_row(&[int8_array(&[0, 1, 2]), Value::Float64(1.0)])
                .unwrap(),
            1,
        );
    }

    #[test]
    fn array_position_returns_minus_one_for_null_array_or_missing_value() {
        let func = ArrayPositionFunc;

        assert_int(func.eval_row(&[Value::Null, Value::Int64(1)]).unwrap(), -1);
        assert_int(
            func.eval_row(&[int8_array(&[0, 1, 2]), Value::Int64(3)])
                .unwrap(),
            -1,
        );
    }

    #[test]
    fn array_last_position_scans_from_right() {
        let func = ArrayLastPositionFunc;

        assert_int(
            func.eval_row(&[int8_array(&[1, 2, 1]), Value::Int64(1)])
                .unwrap(),
            2,
        );
    }

    #[test]
    fn element_at_supports_negative_indexes_and_returns_null_out_of_bounds() {
        let func = ElementAtFunc;

        assert_int8(func.eval_row(&[int8_array(&[1, 2, 3]), i(0)]).unwrap(), 1);
        assert_int8(func.eval_row(&[int8_array(&[1, 2, 3]), i(-1)]).unwrap(), 3);
        assert_null(func.eval_row(&[int8_array(&[1, 2, 3]), i(3)]).unwrap());
        assert_null(func.eval_row(&[int8_array(&[1, 2, 3]), i(-4)]).unwrap());
        assert_null(func.eval_row(&[Value::Null, i(0)]).unwrap());
    }

    #[test]
    fn array_contains_matches_cross_numeric_values() {
        let func = ArrayContainsFunc;

        assert_bool(
            func.eval_row(&[int8_array(&[0, 1, 2]), Value::Int64(1)])
                .unwrap(),
            true,
        );
        assert_bool(
            func.eval_row(&[int8_array(&[0, 1, 2]), s("1")]).unwrap(),
            false,
        );
        assert_bool(
            func.eval_row(&[Value::Null, Value::Int64(1)]).unwrap(),
            false,
        );
    }

    #[test]
    fn array_contains_any_matches_cross_numeric_values() {
        let func = ArrayContainsAnyFunc;
        let left = int8_array(&[1, 2, 3]);
        let right = array_to_value(vec![Value::Int64(9), Value::Int64(2)]).unwrap();

        assert_bool(func.eval_row(&[left, right]).unwrap(), true);
        assert_bool(
            func.eval_row(&[Value::Null, int8_array(&[1])]).unwrap(),
            false,
        );
        assert_bool(
            func.eval_row(&[int8_array(&[1]), Value::Null]).unwrap(),
            false,
        );
    }

    #[test]
    fn array_contains_any_relaxed_fallback_preserves_early_match() {
        let func = ArrayContainsAnyFunc;
        let mut left = vec![Value::Int8(7)];
        left.extend(std::iter::repeat_n(Value::Int8(1), 1000));
        let mut right = vec![Value::Int64(7)];
        right.extend(std::iter::repeat_n(Value::Int64(2), 999));

        assert_bool(
            func.eval_row(&[
                array_to_value(left).unwrap(),
                array_to_value(right).unwrap(),
            ])
            .unwrap(),
            true,
        );
    }

    #[test]
    fn array_contains_any_relaxed_fallback_errors_after_comparison_cap_without_match() {
        let func = ArrayContainsAnyFunc;
        let left = vec![Value::Int8(1); 1001];
        let right = vec![Value::Int64(2); 1000];

        let err = func
            .eval_row(&[
                array_to_value(left).unwrap(),
                array_to_value(right).unwrap(),
            ])
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("array_contains_any fallback comparisons"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn array_create_builds_strictly_typed_arrays() {
        let func = ArrayCreateFunc;

        assert_array(
            func.eval_row(&[Value::Int8(1), Value::Int8(2)]).unwrap(),
            vec![Value::Int8(1), Value::Int8(2)],
        );
        assert_array(func.eval_row(&[]).unwrap(), vec![]);
        assert_array(
            func.eval_row(&[Value::Null, Value::Int8(1)]).unwrap(),
            vec![Value::Null, Value::Int8(1)],
        );
        assert!(func.eval_row(&[Value::Int8(1), Value::Int64(2)]).is_err());
    }

    #[test]
    fn array_remove_filters_cross_numeric_matches_and_preserves_array_type() {
        let func = ArrayRemoveFunc;

        assert_array(
            func.eval_row(&[int8_array(&[1, 2, 1]), Value::Int64(1)])
                .unwrap(),
            vec![Value::Int8(2)],
        );

        let actual = func.eval_row(&[int8_array(&[1]), Value::Int64(1)]).unwrap();
        match actual {
            Value::List(list) => {
                assert!(list.items().is_empty());
                assert_eq!(list.datatype(), &Value::Int8(0).datatype());
            }
            other => panic!("expected array, got {:?}", other),
        }

        assert_null(func.eval_row(&[Value::Null, Value::Int64(1)]).unwrap());
    }

    #[test]
    fn repeat_returns_typed_arrays_and_rejects_large_outputs() {
        let func = RepeatFunc;

        assert_array(
            func.eval_row(&[Value::Int8(7), i(3)]).unwrap(),
            vec![Value::Int8(7), Value::Int8(7), Value::Int8(7)],
        );

        let empty = func.eval_row(&[Value::Int8(7), i(0)]).unwrap();
        match empty {
            Value::List(list) => {
                assert!(list.items().is_empty());
                assert_eq!(list.datatype(), &Value::Int8(0).datatype());
            }
            other => panic!("expected array, got {:?}", other),
        }

        assert!(func
            .eval_row(&[
                Value::Int64(1),
                Value::Int64(MAX_ARRAY_OUTPUT_LEN as i64 + 1)
            ])
            .is_err());
    }

    #[test]
    fn sequence_builds_int64_ranges() {
        let func = SequenceFunc;

        assert_array(
            func.eval_row(&[i(1), i(3)]).unwrap(),
            vec![i(1), i(2), i(3)],
        );
        assert_array(
            func.eval_row(&[i(3), i(1)]).unwrap(),
            vec![i(3), i(2), i(1)],
        );
        assert_array(
            func.eval_row(&[i(1), i(5), i(2)]).unwrap(),
            vec![i(1), i(3), i(5)],
        );
        assert_array(func.eval_row(&[i(1), i(5), i(-1)]).unwrap(), vec![]);
        assert!(func.eval_row(&[i(1), i(5), i(0)]).is_err());
    }

    #[test]
    fn array_concat_skips_null_arrays_and_requires_matching_element_types() {
        let func = ArrayConcatFunc;

        assert_array(
            func.eval_row(&[int8_array(&[1, 2]), Value::Null, int8_array(&[3])])
                .unwrap(),
            vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)],
        );
        assert_null(func.eval_row(&[Value::Null, Value::Null]).unwrap());
        assert!(func
            .eval_row(&[
                int8_array(&[1]),
                array_to_value(vec![Value::Int64(2)]).unwrap()
            ])
            .is_err());
    }
}
