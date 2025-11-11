use crate::expr::func::EvalError;
use datatypes::Value;
use crate::expr::custom_func::CustomFunc;

/// Custom implementation of the concat function
/// This function concatenates exactly 2 String arguments
#[derive(Debug, Clone)]
pub struct ConcatFunc;

impl CustomFunc for ConcatFunc {
    /// Validate the concat function arguments for vectorized evaluation
    ///
    /// # Arguments
    ///
    /// * `args` - A vector of argument vectors, where args[i][j] is the i-th argument's j-th row
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if exactly 2 String argument vectors are provided with same length.
    fn validate_vectorized(&self, args: &[Vec<Value>]) -> Result<(), EvalError> {
        // Check that exactly 2 arguments are provided
        if args.len() != 2 {
            return Err(EvalError::TypeMismatch {
                expected: "2 argument vectors".to_string(),
                actual: format!("{} argument vectors", args.len()),
            });
        }

        // Check that both argument vectors have the same length
        let num_rows = args[0].len();
        if args[1].len() != num_rows {
            return Err(EvalError::NotImplemented {
                feature: format!("Argument vector length mismatch: {} vs {}", args[0].len(), args[1].len())
            });
        }

        // Check that both argument vectors contain String values (validate first element for efficiency)
        if num_rows > 0 {
            for (i, arg_vec) in args.iter().enumerate() {
                if let Some(first_val) = arg_vec.first() {
                    if !matches!(first_val, Value::String(_)) {
                        return Err(EvalError::TypeMismatch {
                            expected: "String vector".to_string(),
                            actual: format!("{:?} at argument {}", first_val, i),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Evaluate the concat function with vectorized arguments
    ///
    /// # Arguments
    ///
    /// * `args` - A vector of exactly 2 String argument vectors to concatenate.
    ///   args[0][j] and args[1][j] represent the two strings to concatenate at row j
    ///   This method assumes arguments have been validated by validate_vectorized().
    ///
    /// # Returns
    ///
    /// Returns a vector of String values containing the concatenation of corresponding rows.
    fn eval_vectorized(&self, args: &[Vec<Value>]) -> Result<Vec<Value>, EvalError> {
        self.validate_vectorized(args)?;

        let num_rows = args[0].len();
        let mut results = Vec::with_capacity(num_rows);

        // True vectorized evaluation - process all rows in one call
        for i in 0..num_rows {
            let first = match &args[0][i] {
                Value::String(s) => s,
                other => {
                    return Err(EvalError::TypeMismatch {
                        expected: "String".to_string(),
                        actual: format!("{:?}", other),
                    });
                }
            };

            let second = match &args[1][i] {
                Value::String(s) => s,
                other => {
                    return Err(EvalError::TypeMismatch {
                        expected: "String".to_string(),
                        actual: format!("{:?}", other),
                    });
                }
            };

            // Concatenate the strings at row i
            results.push(Value::String(format!("{}{}", first, second)));
        }

        Ok(results)
    }

    fn name(&self) -> &str {
        "concat"
    }
}