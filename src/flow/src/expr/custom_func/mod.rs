pub mod string_func;

use datatypes::Value;
pub use string_func::ConcatFunc;
use crate::expr::func::EvalError;

/// List of functions that can be called through CallFunc (custom functions)
pub const CUSTOM_FUNCTIONS: &[&str] = &[
    "concat",
    // Add more custom functions here as they are implemented
];

/// Custom function that can be implemented by users
/// This trait allows users to define their own functions for evaluation
pub trait CustomFunc: Send + Sync + std::fmt::Debug {
    /// Validate function arguments for vectorized evaluation (multiple rows)
    ///
    /// # Arguments
    ///
    /// * `args` - A vector of argument vectors, where each inner vector contains values for all rows
    ///   args[i][j] represents the i-th argument's j-th row value
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if arguments are valid for vectorized processing, or an error if validation fails.
    /// Implementors should validate the entire multi-row structure and data consistency.
    fn validate_vectorized(&self, args: &[Vec<Value>]) -> Result<(), EvalError>;

    /// Evaluate the function with vectorized arguments (multiple rows in one call)
    ///
    /// # Arguments
    ///
    /// * `args` - A vector of argument vectors, where each inner vector contains values for all rows
    ///   args[i][j] represents the i-th argument's j-th row value
    ///
    /// # Returns
    ///
    /// Returns a vector of evaluated results, one for each row.
    /// Implementors should implement true vectorized evaluation that processes multiple rows efficiently.
    fn eval_vectorized(&self, args: &[Vec<Value>]) -> Result<Vec<Value>, EvalError>;

    /// Get the function name for debugging purposes
    fn name(&self) -> &str;
}
