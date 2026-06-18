use super::helpers::*;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![
        acos_function_def(),
        asin_function_def(),
        atan_function_def(),
        cos_function_def(),
        cosh_function_def(),
        exp_function_def(),
        floor_function_def(),
        ln_function_def(),
        sin_function_def(),
        sinh_function_def(),
        sqrt_function_def(),
        tan_function_def(),
        tanh_function_def(),
        ceiling_function_def(),
        ceil_function_def(),
        radians_function_def(),
        degrees_function_def(),
        atan2_function_def(),
        mod_function_def(),
        power_function_def(),
        pow_function_def(),
        abs_function_def(),
        bitand_function_def(),
        bitor_function_def(),
        bitxor_function_def(),
        bitnot_function_def(),
        sign_function_def(),
        pi_function_def(),
        rand_function_def(),
        cot_function_def(),
        log_function_def(),
        round_function_def(),
        conv_function_def(),
    ]
}

pub fn acos_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "acos",
        "Return the arc cosine of a numeric value.",
        vec!["SELECT acos(x)", "SELECT acos(0.5)"],
    )
}

pub fn asin_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "asin",
        "Return the arc sine of a numeric value.",
        vec!["SELECT asin(x)", "SELECT asin(0.5)"],
    )
}

pub fn atan_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "atan",
        "Return the arc tangent of a numeric value.",
        vec!["SELECT atan(x)", "SELECT atan(1.0)"],
    )
}

pub fn cos_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "cos",
        "Return the cosine of a numeric value.",
        vec!["SELECT cos(x)", "SELECT cos(0.0)"],
    )
}

pub fn cosh_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "cosh",
        "Return the hyperbolic cosine of a numeric value.",
        vec!["SELECT cosh(x)", "SELECT cosh(1.0)"],
    )
}

pub fn exp_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "exp",
        "Return e raised to the given numeric value.",
        vec!["SELECT exp(x)", "SELECT exp(1.0)"],
    )
}

pub fn floor_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "floor",
        "Round a numeric value down to the nearest integer value in float form.",
        vec!["SELECT floor(x)", "SELECT floor(3.7)"],
    )
}

pub fn ln_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "ln",
        "Return the natural logarithm of a numeric value.",
        vec!["SELECT ln(x)", "SELECT ln(10.0)"],
    )
}

pub fn sin_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "sin",
        "Return the sine of a numeric value.",
        vec!["SELECT sin(x)", "SELECT sin(0.0)"],
    )
}

pub fn sinh_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "sinh",
        "Return the hyperbolic sine of a numeric value.",
        vec!["SELECT sinh(x)", "SELECT sinh(1.0)"],
    )
}

pub fn sqrt_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "sqrt",
        "Return the square root of a numeric value.",
        vec!["SELECT sqrt(x)", "SELECT sqrt(9.0)"],
    )
}

pub fn tan_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "tan",
        "Return the tangent of a numeric value.",
        vec!["SELECT tan(x)", "SELECT tan(1.0)"],
    )
}

pub fn tanh_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "tanh",
        "Return the hyperbolic tangent of a numeric value.",
        vec!["SELECT tanh(x)", "SELECT tanh(1.0)"],
    )
}

pub fn ceiling_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "ceiling",
        "Round a numeric value up to the nearest integer value in float form.",
        vec!["SELECT ceiling(x)", "SELECT ceiling(3.2)"],
    )
}

pub fn ceil_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "ceil",
        vec!["ceiling"],
        vec![req_arg("x", float_type())],
        float_type(),
        "Round a numeric value up to the nearest integer value in float form.",
        vec![
            "Requires exactly 1 numeric argument.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT ceil(x)", "SELECT ceil(3.2)"],
    )
}

pub fn radians_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "radians",
        "Convert degrees to radians.",
        vec!["SELECT radians(x)", "SELECT radians(180.0)"],
    )
}

pub fn degrees_function_def() -> FunctionDef {
    unary_numeric_fn_def(
        "degrees",
        "Convert radians to degrees.",
        vec!["SELECT degrees(x)", "SELECT degrees(3.1415926)"],
    )
}

pub fn atan2_function_def() -> FunctionDef {
    binary_numeric_fn_def(
        "atan2",
        "Return the arc tangent of y/x using the signs of both arguments.",
        vec!["SELECT atan2(y, x)", "SELECT atan2(1.0, 1.0)"],
    )
}

pub fn mod_function_def() -> FunctionDef {
    binary_numeric_fn_def(
        "mod",
        "Return the remainder of dividing the first numeric argument by the second.",
        vec!["SELECT mod(a, b)", "SELECT mod(10, 3)"],
    )
}

pub fn power_function_def() -> FunctionDef {
    binary_numeric_fn_def(
        "power",
        "Raise the first numeric argument to the power of the second.",
        vec!["SELECT power(a, b)", "SELECT power(2, 3)"],
    )
}

pub fn pow_function_def() -> FunctionDef {
    scalar_function_def_with_aliases(
        "pow",
        vec!["power"],
        vec![req_arg("a", float_type()), req_arg("b", float_type())],
        float_type(),
        "Raise the first numeric argument to the power of the second.",
        vec![
            "Requires exactly 2 numeric arguments.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT pow(a, b)", "SELECT pow(2, 3)"],
    )
}

pub fn abs_function_def() -> FunctionDef {
    scalar_function_def(
        "abs",
        vec![req_arg("x", float_type())],
        float_type(),
        "Return the absolute value of a numeric argument.",
        vec![
            "Requires exactly 1 numeric argument.",
            "Returns NULL if the argument is NULL.",
            "Overflow on signed integer absolute value can return an evaluation error.",
        ],
        vec!["SELECT abs(x)", "SELECT abs(-42)"],
    )
}

pub fn bitand_function_def() -> FunctionDef {
    scalar_function_def(
        "bitand",
        vec![req_arg("a", int_type()), req_arg("b", int_type())],
        int_type(),
        "Return the bitwise AND of two integers.",
        vec![
            "Requires exactly 2 integer arguments.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT bitand(a, b)", "SELECT bitand(6, 3)"],
    )
}

pub fn bitor_function_def() -> FunctionDef {
    scalar_function_def(
        "bitor",
        vec![req_arg("a", int_type()), req_arg("b", int_type())],
        int_type(),
        "Return the bitwise OR of two integers.",
        vec![
            "Requires exactly 2 integer arguments.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT bitor(a, b)", "SELECT bitor(6, 3)"],
    )
}

pub fn bitxor_function_def() -> FunctionDef {
    scalar_function_def(
        "bitxor",
        vec![req_arg("a", int_type()), req_arg("b", int_type())],
        int_type(),
        "Return the bitwise XOR of two integers.",
        vec![
            "Requires exactly 2 integer arguments.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT bitxor(a, b)", "SELECT bitxor(6, 3)"],
    )
}

pub fn bitnot_function_def() -> FunctionDef {
    scalar_function_def(
        "bitnot",
        vec![req_arg("x", int_type())],
        int_type(),
        "Return the bitwise NOT of an integer.",
        vec![
            "Requires exactly 1 integer argument.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT bitnot(x)", "SELECT bitnot(6)"],
    )
}

pub fn sign_function_def() -> FunctionDef {
    scalar_function_def(
        "sign",
        vec![req_arg("x", float_type())],
        int_type(),
        "Return the sign of a numeric value as -1, 0, or 1.",
        vec![
            "Requires exactly 1 numeric argument.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT sign(x)", "SELECT sign(-12.3)"],
    )
}

pub fn pi_function_def() -> FunctionDef {
    scalar_function_def(
        "pi",
        vec![],
        float_type(),
        "Return the mathematical constant pi.",
        vec!["Requires exactly 0 arguments."],
        vec!["SELECT pi()", "SELECT round(pi(), 2)"],
    )
}

pub fn rand_function_def() -> FunctionDef {
    scalar_function_def(
        "rand",
        vec![],
        float_type(),
        "Return a random floating-point value.",
        vec![
            "Requires exactly 0 arguments.",
            "This function is non-deterministic.",
        ],
        vec!["SELECT rand()", "SELECT rand() AS r"],
    )
}

pub fn cot_function_def() -> FunctionDef {
    scalar_function_def(
        "cot",
        vec![req_arg("x", float_type())],
        float_type(),
        "Return the cotangent of a numeric value.",
        vec![
            "Requires exactly 1 numeric argument.",
            "Returns NULL if the argument is NULL.",
            "Division by zero situations can return an evaluation error.",
        ],
        vec!["SELECT cot(x)", "SELECT cot(1.0)"],
    )
}

pub fn log_function_def() -> FunctionDef {
    scalar_function_def(
        "log",
        vec![opt_arg("base", float_type()), req_arg("x", float_type())],
        float_type(),
        "Return the common logarithm of x, or the logarithm of x in the given base.",
        vec![
            "Accepts 1 or 2 numeric arguments.",
            "With 1 argument, computes log10(x).",
            "With 2 arguments, computes log base `base` of `x`.",
            "Returns NULL if any provided argument is NULL.",
        ],
        vec!["SELECT log(100)", "SELECT log(2, 8)"],
    )
}

pub fn round_function_def() -> FunctionDef {
    scalar_function_def(
        "round",
        vec![req_arg("x", float_type()), opt_arg("precision", int_type())],
        float_type(),
        "Round a numeric value to the given decimal precision.",
        vec![
            "Accepts 1 or 2 arguments.",
            "The first argument must be numeric.",
            "The optional second argument must be an integer precision.",
            "Returns NULL if any provided argument is NULL.",
        ],
        vec!["SELECT round(3.14159)", "SELECT round(3.14159, 2)"],
    )
}

pub fn conv_function_def() -> FunctionDef {
    scalar_function_def(
        "conv",
        vec![
            req_arg("value", string_type()),
            req_arg("from_base", int_type()),
            req_arg("to_base", int_type()),
        ],
        string_type(),
        "Convert a string representation of an integer from one base to another.",
        vec![
            "Requires exactly 3 arguments.",
            "The first argument must be a string.",
            "The second and third arguments must be integers.",
            "Supported bases are in the range 2 to 36.",
            "Returns NULL if any argument is NULL.",
        ],
        vec!["SELECT conv('15', 10, 16)", "SELECT conv('FF', 16, 10)"],
    )
}

macro_rules! unary_f64_func {
    ($struct:ident,$name:expr,$op:expr) => {
        #[derive(Debug, Clone)]
        pub struct $struct;

        impl CustomFunc for $struct {
            fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
                validate_arity(args, &[1])
            }

            fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
                unary_numeric_to_f64(args, $op)
            }

            fn name(&self) -> &str {
                $name
            }
        }
    };
}

macro_rules! binary_f64_func {
    ($struct:ident,$name:expr,$op:expr) => {
        #[derive(Debug, Clone)]
        pub struct $struct;

        impl CustomFunc for $struct {
            fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
                validate_arity(args, &[2])
            }

            fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
                binary_numeric_to_f64(args, $op)
            }

            fn name(&self) -> &str {
                $name
            }
        }
    };
}

unary_f64_func!(AcosFunc, "acos", |x: f64| x.acos());
unary_f64_func!(AsinFunc, "asin", |x: f64| x.asin());
unary_f64_func!(AtanFunc, "atan", |x: f64| x.atan());
unary_f64_func!(CosFunc, "cos", |x: f64| x.cos());
unary_f64_func!(CoshFunc, "cosh", |x: f64| x.cosh());
unary_f64_func!(ExpFunc, "exp", |x: f64| x.exp());
unary_f64_func!(FloorFunc, "floor", |x: f64| x.floor());
unary_f64_func!(LnFunc, "ln", |x: f64| x.ln());
unary_f64_func!(SinFunc, "sin", |x: f64| x.sin());
unary_f64_func!(SinhFunc, "sinh", |x: f64| x.sinh());
unary_f64_func!(SqrtFunc, "sqrt", |x: f64| x.sqrt());
unary_f64_func!(TanFunc, "tan", |x: f64| x.tan());
unary_f64_func!(TanhFunc, "tanh", |x: f64| x.tanh());
unary_f64_func!(CeilingFunc, "ceiling", |x: f64| x.ceil());
unary_f64_func!(CeilFunc, "ceil", |x: f64| x.ceil());
unary_f64_func!(RadiansFunc, "radians", |x: f64| x.to_radians());
unary_f64_func!(DegreesFunc, "degrees", |x: f64| x.to_degrees());

binary_f64_func!(Atan2Func, "atan2", |a: f64, b: f64| a.atan2(b));
binary_f64_func!(ModFunc, "mod", |a: f64, b: f64| a % b);
binary_f64_func!(PowerFunc, "power", |a: f64, b: f64| a.powf(b));
binary_f64_func!(PowFunc, "pow", |a: f64, b: f64| a.powf(b));

#[derive(Debug, Clone)]
pub struct AbsFunc;

impl CustomFunc for AbsFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        abs_value(&args[0])
    }

    fn name(&self) -> &str {
        "abs"
    }
}

pub fn abs_value(v: &Value) -> Result<Value, EvalError> {
    map_numeric_value(
        v,
        |x| {
            x.checked_abs().ok_or(EvalError::TypeMismatch {
                expected: "integer whose abs is representable".to_string(),
                actual: format!("{x}"),
            })
        },
        Ok,
        |x| Ok(x.abs()),
    )
}

#[derive(Debug, Clone)]
pub struct BitAndFunc;

impl CustomFunc for BitAndFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_i64(args, |a, b| a & b)
    }

    fn name(&self) -> &str {
        "bitand"
    }
}

#[derive(Debug, Clone)]
pub struct BitOrFunc;

impl CustomFunc for BitOrFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_i64(args, |a, b| a | b)
    }

    fn name(&self) -> &str {
        "bitor"
    }
}

#[derive(Debug, Clone)]
pub struct BitXorFunc;

impl CustomFunc for BitXorFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        binary_i64(args, |a, b| a ^ b)
    }

    fn name(&self) -> &str {
        "bitxor"
    }
}

#[derive(Debug, Clone)]
pub struct BitNotFunc;

impl CustomFunc for BitNotFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_i64(args, |a| !a)
    }

    fn name(&self) -> &str {
        "bitnot"
    }
}

#[derive(Debug, Clone)]
pub struct SignFunc;

impl CustomFunc for SignFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        sign_numeric(args)
    }

    fn name(&self) -> &str {
        "sign"
    }
}

#[derive(Debug, Clone)]
pub struct PiFunc;

impl CustomFunc for PiFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        nullary_f64(args, || std::f64::consts::PI)
    }

    fn name(&self) -> &str {
        "pi"
    }
}

#[derive(Debug, Clone)]
pub struct RandFunc;

impl CustomFunc for RandFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[0])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        nullary_f64(args, rand::random::<f64>)
    }

    fn name(&self) -> &str {
        "rand"
    }
}

#[derive(Debug, Clone)]
pub struct CotFunc;

impl CustomFunc for CotFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        unary_numeric_to_f64_inf_err(args, |x| 1.0 / x.tan())
    }

    fn name(&self) -> &str {
        "cot"
    }
}

#[derive(Debug, Clone)]
pub struct LogFunc;

impl CustomFunc for LogFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1, 2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1, 2])?;

        match args.len() {
            1 => {
                let Some(x) = nth_f64_or_null(args, 0)? else {
                    return Ok(Value::Null);
                };
                Ok(f64_to_value_nan_null(x.log10()))
            }
            2 => {
                let Some(base) = nth_f64_or_null(args, 0)? else {
                    return Ok(Value::Null);
                };
                let Some(x) = nth_f64_or_null(args, 1)? else {
                    return Ok(Value::Null);
                };
                Ok(f64_to_value_nan_null(x.ln() / base.ln()))
            }
            _ => Err(EvalError::TypeMismatch {
                expected: "1 or 2 arguments for log".to_string(),
                actual: format!("{} arguments", args.len()),
            }),
        }
    }

    fn name(&self) -> &str {
        "log"
    }
}

#[derive(Debug, Clone)]
pub struct RoundFunc;

impl CustomFunc for RoundFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1, 2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1, 2])?;

        let Some(x) = nth_f64_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let precision = precision_arg_or_default(args, 1, 0)?;

        // Non-finite inputs are passed through unchanged — Inf/NaN round to themselves.
        if !x.is_finite() {
            return Ok(Value::Float64(x));
        }

        let factor = 10_f64.powi(precision);
        let scaled = x * factor;

        let result = if factor.is_infinite() || scaled.is_infinite() || factor == 0.0 {
            x
        } else {
            scaled.round() / factor
        };

        Ok(Value::Float64(result))
    }

    fn name(&self) -> &str {
        "round"
    }
}

#[derive(Debug, Clone)]
pub struct ConvFunc;

impl CustomFunc for ConvFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[3])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[3])?;

        if any_null(args) {
            return Ok(Value::Null);
        }

        let s = value_to_string(&args[0])?;
        let from_base = value_to_i64(&args[1])?;
        let to_base = value_to_i64(&args[2])?;

        conv_impl(&s, from_base, to_base)
    }

    fn name(&self) -> &str {
        "conv"
    }
}

fn conv_impl(input: &str, mut from_base: i64, mut to_base: i64) -> Result<Value, EvalError> {
    let mut signed = false;
    let mut ignore_sign = false;

    if from_base < 0 {
        from_base = -from_base;
        signed = true;
    }

    if to_base < 0 {
        to_base = -to_base;
        ignore_sign = true;
    }

    if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
        return Ok(Value::Null);
    }

    let trimmed = input.trim();
    let prefix = get_valid_prefix(trimmed, from_base as u32);

    if prefix.is_empty() {
        return Ok(Value::String("0".to_string()));
    }

    let mut negative_input = false;
    let digits = if let Some(rest) = prefix.strip_prefix('-') {
        negative_input = true;
        rest
    } else {
        prefix.as_str()
    };

    let mut val =
        u64::from_str_radix(digits, from_base as u32).map_err(|e| EvalError::TypeMismatch {
            expected: "valid number string for base conversion".to_string(),
            actual: e.to_string(),
        })?;

    if signed {
        let min_int64_abs = (i64::MIN as i128).unsigned_abs() as u64;
        if negative_input && val > min_int64_abs {
            val = min_int64_abs;
        }
        if !negative_input && val > i64::MAX as u64 {
            val = i64::MAX as u64;
        }
    }

    if negative_input {
        val = 0u64.wrapping_sub(val);
    }

    let signed_val = val as i64;
    let negative_result = signed_val < 0;

    if ignore_sign && negative_result {
        val = 0u64.wrapping_sub(val);
    }

    let mut s = to_base_string(val, to_base as u32);
    if negative_result && ignore_sign {
        s = format!("-{}", s);
    }

    Ok(Value::String(s.to_uppercase()))
}

fn get_valid_prefix(s: &str, base: u32) -> String {
    let mut end = 0usize;

    for (i, ch) in s.char_indices() {
        if i == 0 && (ch == '+' || ch == '-') {
            end = i + ch.len_utf8();
            continue;
        }

        match char_to_digit(ch) {
            Some(d) if d < base => {
                end = i + ch.len_utf8();
            }
            _ => break,
        }
    }

    let out = &s[..end];
    if out.len() > 1 && out.starts_with('+') {
        out[1..].to_string()
    } else {
        out.to_string()
    }
}

fn char_to_digit(c: char) -> Option<u32> {
    c.to_digit(36)
}

fn to_base_string(mut value: u64, base: u32) -> String {
    debug_assert!((2..=36).contains(&base));

    if value == 0 {
        return "0".to_string();
    }

    let mut buf = Vec::new();
    while value > 0 {
        let digit = (value % base as u64) as u8;
        let ch = if digit <= 9 {
            (b'0' + digit) as char
        } else {
            (b'A' + (digit - 10)) as char
        };
        buf.push(ch);
        value /= base as u64;
    }

    buf.iter().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::Value;

    fn eval<F: CustomFunc>(f: &F, args: &[Value]) -> Value {
        f.eval_row(args).unwrap()
    }

    fn eval_err<F: CustomFunc>(f: &F, args: &[Value]) {
        assert!(f.eval_row(args).is_err(), "expected error, got ok");
    }

    #[test]
    fn test_unary_math_correctness() {
        assert_float(eval(&AcosFunc, &[Value::Float64(1.0)]), 0.0);
        assert_float(eval(&AsinFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(
            eval(&AtanFunc, &[Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert_float(eval(&CosFunc, &[Value::Float64(0.0)]), 1.0);
        assert_float(eval(&CoshFunc, &[Value::Float64(0.0)]), 1.0);
        assert_float(eval(&ExpFunc, &[Value::Float64(1.0)]), std::f64::consts::E);
        assert_float(eval(&FloorFunc, &[Value::Float64(3.8)]), 3.0);
        assert_float(eval(&LnFunc, &[Value::Float64(1.0)]), 0.0);
        assert_float(eval(&SinFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&SinhFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&SqrtFunc, &[Value::Float64(9.0)]), 3.0);
        assert_float(eval(&TanFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&TanhFunc, &[Value::Float64(0.0)]), 0.0);
        assert_float(eval(&CeilingFunc, &[Value::Float64(3.2)]), 4.0);
        assert_float(eval(&CeilFunc, &[Value::Float64(3.2)]), 4.0);
        assert_float(
            eval(&RadiansFunc, &[Value::Float64(180.0)]),
            std::f64::consts::PI,
        );
        assert_float(
            eval(&DegreesFunc, &[Value::Float64(std::f64::consts::PI)]),
            180.0,
        );
        assert_float(
            eval(&CotFunc, &[Value::Float64(std::f64::consts::FRAC_PI_4)]),
            1.0,
        );
    }

    #[test]
    fn test_binary_math_correctness() {
        assert_float(
            eval(&Atan2Func, &[Value::Float64(1.0), Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert_float(
            eval(&ModFunc, &[Value::Float64(10.5), Value::Float64(3.0)]),
            10.5 % 3.0,
        );
        assert_float(
            eval(&PowerFunc, &[Value::Float64(2.0), Value::Float64(3.0)]),
            8.0,
        );
        assert_float(
            eval(&PowFunc, &[Value::Float64(2.0), Value::Float64(4.0)]),
            16.0,
        );
    }

    #[test]
    fn test_abs_bit_sign_pi_rand_correctness() {
        assert_int(eval(&AbsFunc, &[Value::Int64(-12)]), 12);
        assert_float(eval(&AbsFunc, &[Value::Float64(-12.5)]), 12.5);

        assert_int(eval(&BitAndFunc, &[Value::Int64(6), Value::Int64(3)]), 2);
        assert_int(eval(&BitOrFunc, &[Value::Int64(6), Value::Int64(3)]), 7);
        assert_int(eval(&BitXorFunc, &[Value::Int64(6), Value::Int64(3)]), 5);
        assert_int(eval(&BitNotFunc, &[Value::Int64(6)]), !6);

        assert_int(eval(&SignFunc, &[Value::Int64(9)]), 1);
        assert_int(eval(&SignFunc, &[Value::Int64(-9)]), -1);
        assert_int(eval(&SignFunc, &[Value::Int64(0)]), 0);

        assert_float(eval(&PiFunc, &[]), std::f64::consts::PI);

        let rand_v = eval(&RandFunc, &[]);
        match rand_v {
            Value::Float64(v) => assert!((0.0..1.0).contains(&v)),
            other => panic!("expected Float64 in [0,1), got {:?}", other),
        }
    }

    #[test]
    fn test_round_large_precision_matches_bigfloat_behavior() {
        let f = RoundFunc;
        let v = f.eval_row(&[Value::Float64(1.2345), Value::Int64(400)]);

        assert!(v.is_ok(), "expected Ok result, got {:?}", v);

        match v.unwrap() {
            Value::Float64(x) => {
                assert!(x.is_finite(), "expected finite float result, got {}", x);
                assert_eq!(x, 1.2345);
            }
            other => panic!("expected Float64 result, got {:?}", other),
        }
    }

    #[test]
    fn test_log_round_conv_correctness() {
        assert_float(eval(&LogFunc, &[Value::Float64(1000.0)]), 3.0);

        assert_float(
            eval(&LogFunc, &[Value::Float64(2.0), Value::Float64(8.0)]),
            3.0,
        );

        assert_float(eval(&RoundFunc, &[Value::Float64(3.6)]), 4.0);
        assert_float(
            eval(&RoundFunc, &[Value::Float64(3.14159), Value::Int64(2)]),
            3.14,
        );
        assert_float(
            eval(&RoundFunc, &[Value::Float64(1234.56), Value::Int64(-2)]),
            1200.0,
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("A".to_string()),
                    Value::Int64(16),
                    Value::Int64(10),
                ],
            ),
            "10",
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("1010".to_string()),
                    Value::Int64(2),
                    Value::Int64(16),
                ],
            ),
            "A",
        );
    }

    #[test]
    fn test_semantics_for_null_and_domain_cases() {
        assert_null(eval(&SqrtFunc, &[Value::Float64(-1.0)]));
        assert_null(eval(&LnFunc, &[Value::Float64(-1.0)]));
        assert_null(eval(&AcosFunc, &[Value::Float64(2.0)]));
        assert_null(eval(&AsinFunc, &[Value::Float64(2.0)]));

        assert_null(eval(&LogFunc, &[Value::Float64(-10.0)]));
        assert_null(eval(
            &LogFunc,
            &[Value::Float64(10.0), Value::Float64(-1.0)],
        ));

        assert_null(eval(
            &ConvFunc,
            &[
                Value::String("10".to_string()),
                Value::Int64(1),
                Value::Int64(10),
            ],
        ));

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("XYZ".to_string()),
                    Value::Int64(10),
                    Value::Int64(16),
                ],
            ),
            "0",
        );

        eval_err(&CotFunc, &[Value::Float64(0.0)]);
    }

    #[test]
    fn test_conv_edge_cases() {
        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("   +1GZ".to_string()),
                    Value::Int64(17),
                    Value::Int64(10),
                ],
            ),
            "33",
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("-10".to_string()),
                    Value::Int64(-10),
                    Value::Int64(16),
                ],
            ),
            "FFFFFFFFFFFFFFF6",
        );

        assert_string(
            eval(
                &ConvFunc,
                &[
                    Value::String("-10".to_string()),
                    Value::Int64(-10),
                    Value::Int64(-16),
                ],
            ),
            "-A",
        );
    }

    #[test]
    fn test_abs_overflow_errors() {
        eval_err(&AbsFunc, &[Value::Int64(i64::MIN)]);
    }

    #[test]
    fn test_nullary_arity_validation() {
        assert!(PiFunc.validate_row(&[]).is_ok());
        assert!(RandFunc.validate_row(&[]).is_ok());

        assert!(PiFunc.validate_row(&[Value::Int64(1)]).is_err());
        assert!(RandFunc.validate_row(&[Value::Int64(1)]).is_err());
    }

    #[test]
    fn test_unary_arity_validation() {
        assert!(AcosFunc.validate_row(&[Value::Float64(1.0)]).is_ok());
        assert!(AbsFunc.validate_row(&[Value::Int64(1)]).is_ok());
        assert!(BitNotFunc.validate_row(&[Value::Int64(1)]).is_ok());
        assert!(SignFunc.validate_row(&[Value::Int64(1)]).is_ok());
        assert!(CotFunc.validate_row(&[Value::Float64(1.0)]).is_ok());

        assert!(AcosFunc.validate_row(&[]).is_err());
        assert!(AcosFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(2.0)])
            .is_err());
    }

    #[test]
    fn test_binary_arity_validation() {
        assert!(Atan2Func
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(ModFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(PowerFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(PowFunc
            .validate_row(&[Value::Float64(1.0), Value::Float64(1.0)])
            .is_ok());
        assert!(BitAndFunc
            .validate_row(&[Value::Int64(1), Value::Int64(1)])
            .is_ok());
        assert!(BitOrFunc
            .validate_row(&[Value::Int64(1), Value::Int64(1)])
            .is_ok());
        assert!(BitXorFunc
            .validate_row(&[Value::Int64(1), Value::Int64(1)])
            .is_ok());

        assert!(Atan2Func.validate_row(&[Value::Float64(1.0)]).is_err());
        assert!(Atan2Func
            .validate_row(&[
                Value::Float64(1.0),
                Value::Float64(1.0),
                Value::Float64(1.0)
            ])
            .is_err());
    }

    #[test]
    fn test_variable_arity_functions() {
        assert!(LogFunc.validate_row(&[Value::Float64(10.0)]).is_ok());
        assert!(LogFunc
            .validate_row(&[Value::Float64(2.0), Value::Float64(8.0)])
            .is_ok());
        assert!(LogFunc.validate_row(&[]).is_err());
        assert!(LogFunc
            .validate_row(&[
                Value::Float64(1.0),
                Value::Float64(2.0),
                Value::Float64(3.0)
            ])
            .is_err());

        assert!(RoundFunc.validate_row(&[Value::Float64(3.14)]).is_ok());
        assert!(RoundFunc
            .validate_row(&[Value::Float64(3.14), Value::Int64(2)])
            .is_ok());
        assert!(RoundFunc.validate_row(&[]).is_err());
        assert!(RoundFunc
            .validate_row(&[Value::Float64(3.14), Value::Int64(2), Value::Int64(3)])
            .is_err());

        assert!(ConvFunc
            .validate_row(&[
                Value::String("10".to_string()),
                Value::Int64(10),
                Value::Int64(2)
            ])
            .is_ok());
        assert!(ConvFunc.validate_row(&[]).is_err());
        assert!(ConvFunc
            .validate_row(&[Value::String("10".to_string())])
            .is_err());
        assert!(ConvFunc
            .validate_row(&[
                Value::String("10".to_string()),
                Value::Int64(10),
                Value::Int64(2),
                Value::Int64(8),
            ])
            .is_err());
    }

    #[test]
    fn test_mixed_numeric_types_for_unary_funcs() {
        assert_float(eval(&SinFunc, &[Value::Int64(0)]), 0.0);
        assert_float(eval(&SqrtFunc, &[Value::Int64(16)]), 4.0);
        assert_float(eval(&CeilingFunc, &[Value::Int64(3)]), 3.0);

        assert_int(eval(&AbsFunc, &[Value::Int64(-7)]), 7);
        assert_float(eval(&AbsFunc, &[Value::Float64(-7.25)]), 7.25);
    }

    #[test]
    fn test_mixed_numeric_types_for_binary_funcs() {
        assert_float(
            eval(&Atan2Func, &[Value::Int64(1), Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert_float(
            eval(&ModFunc, &[Value::Int64(10), Value::Float64(4.0)]),
            10.0 % 4.0,
        );
        assert_float(
            eval(&PowerFunc, &[Value::Float64(2.5), Value::Int64(2)]),
            6.25,
        );
        assert_float(eval(&PowFunc, &[Value::Int64(9), Value::Float64(0.5)]), 3.0);
    }

    #[test]
    fn test_mixed_numeric_types_for_variable_arity_funcs() {
        assert_float(eval(&LogFunc, &[Value::Int64(10)]), 1.0);
        assert_float(eval(&LogFunc, &[Value::Int64(2), Value::Float64(8.0)]), 3.0);

        assert_float(
            eval(&RoundFunc, &[Value::Int64(1234), Value::Int64(-2)]),
            1200.0,
        );
        assert_float(
            eval(&RoundFunc, &[Value::Float64(12.345), Value::Int64(2)]),
            12.35,
        );
    }

    #[test]
    fn test_type_errors_for_non_numeric_inputs() {
        eval_err(&SinFunc, &[Value::String("abc".to_string())]);
        eval_err(
            &Atan2Func,
            &[Value::String("1".to_string()), Value::Float64(1.0)],
        );
        eval_err(
            &RoundFunc,
            &[Value::Float64(1.23), Value::String("2".to_string())],
        );
        eval_err(&BitAndFunc, &[Value::Int64(1), Value::Float64(2.0)]);
        eval_err(
            &BitOrFunc,
            &[Value::String("1".to_string()), Value::Int64(2)],
        );
        eval_err(&BitXorFunc, &[Value::Float64(1.0), Value::Int64(2)]);
        eval_err(&BitNotFunc, &[Value::Float64(1.0)]);
        eval_err(
            &ConvFunc,
            &[
                Value::String("10".to_string()),
                Value::Float64(10.0),
                Value::Int64(2),
            ],
        );
    }

    #[test]
    fn test_null_propagation_behavior() {
        assert_null(eval(&SinFunc, &[Value::Null]));
        assert_null(eval(&Atan2Func, &[Value::Null, Value::Float64(1.0)]));
        assert_null(eval(&Atan2Func, &[Value::Float64(1.0), Value::Null]));
        assert_null(eval(&AbsFunc, &[Value::Null]));
        assert_null(eval(&BitAndFunc, &[Value::Null, Value::Int64(1)]));
        assert_null(eval(&BitNotFunc, &[Value::Null]));
        assert_null(eval(&SignFunc, &[Value::Null]));
        assert_null(eval(&LogFunc, &[Value::Null]));
        assert_null(eval(&LogFunc, &[Value::Float64(10.0), Value::Null]));
        assert_null(eval(&RoundFunc, &[Value::Null]));
        assert_null(eval(
            &ConvFunc,
            &[Value::Null, Value::Int64(10), Value::Int64(2)],
        ));
    }

    #[test]
    fn test_round_half_away_from_zero() {
        assert_float(eval(&RoundFunc, &[Value::Float64(2.5)]), 3.0);
        assert_float(eval(&RoundFunc, &[Value::Float64(-2.5)]), -3.0);
    }

    #[test]
    fn test_mod_zero_returns_null() {
        assert_null(eval(&ModFunc, &[Value::Float64(10.0), Value::Float64(0.0)]));
    }

    #[test]
    fn test_inf_behavior() {
        match eval(&LnFunc, &[Value::Float64(0.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }

        match eval(&LogFunc, &[Value::Float64(0.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }

        match eval(&ExpFunc, &[Value::Float64(1000.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_positive()),
            other => panic!("expected +Inf, got {:?}", other),
        }
    }

    #[test]
    fn test_round_non_finite() {
        // round(Inf) should preserve Inf, not return 0
        match eval(&RoundFunc, &[Value::Float64(f64::INFINITY)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_positive()),
            other => panic!("expected +Inf, got {:?}", other),
        }
        match eval(&RoundFunc, &[Value::Float64(f64::NEG_INFINITY)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }
        match eval(&RoundFunc, &[Value::Float64(f64::NAN)]) {
            Value::Float64(v) => assert!(v.is_nan()),
            other => panic!("expected NaN, got {:?}", other),
        }
        // round(Inf, 2) via the big-float path
        match eval(
            &RoundFunc,
            &[Value::Float64(f64::INFINITY), Value::Int64(2)],
        ) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_positive()),
            other => panic!("expected +Inf, got {:?}", other),
        }
    }

    #[test]
    fn test_log_edge_cases() {
        match eval(&LogFunc, &[Value::Float64(1.0), Value::Float64(10.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_positive()),
            other => panic!("expected +Inf, got {:?}", other),
        }

        match eval(&LogFunc, &[Value::Float64(10.0), Value::Float64(0.0)]) {
            Value::Float64(v) => assert!(v.is_infinite() && v.is_sign_negative()),
            other => panic!("expected -Inf, got {:?}", other),
        }
    }
}
