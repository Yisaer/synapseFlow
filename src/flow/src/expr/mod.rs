pub mod context;
pub mod custom_func;
pub mod func;
pub mod scalar;
pub mod sql_conversion;

pub use context::EvalContext;
pub use custom_func::ConcatFunc;
pub use func::{BinaryFunc, UnaryFunc};
pub use scalar::ScalarExpr;
pub use sql_conversion::{
    convert_expr_to_scalar, convert_select_stmt_to_scalar, extract_select_expressions,
    ConversionError, StreamSqlConverter,
};
