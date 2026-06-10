pub mod acc;
pub mod change_capture;
pub mod change_to;
pub mod changed_col;
pub mod consecutive_count;
pub mod consecutive_start;
pub mod had_changed;
pub mod lag;
pub mod latest;
pub mod registry;
pub(crate) mod util;

pub use acc::{
    acc_avg_function_def, acc_count_function_def, acc_max_function_def, acc_min_function_def,
    acc_sum_function_def, AccAvgFunction, AccCountFunction, AccMaxFunction, AccMinFunction,
    AccSumFunction,
};
pub use change_capture::change_capture_function_def;
pub use change_capture::ChangeCaptureFunction;
pub use change_to::change_to_function_def;
pub use change_to::ChangeToFunction;
pub use changed_col::changed_col_function_def;
pub use changed_col::ChangedColFunction;
pub use consecutive_count::consecutive_count_function_def;
pub use consecutive_count::ConsecutiveCountFunction;
pub use consecutive_start::consecutive_start_function_def;
pub use consecutive_start::ConsecutiveStartFunction;
pub use had_changed::had_changed_function_def;
pub use had_changed::HadChangedFunction;
pub use lag::lag_function_def;
pub use lag::LagFunction;
pub use latest::latest_function_def;
pub use latest::LatestFunction;
pub use registry::{
    StatefulEvalInput, StatefulFunction, StatefulFunctionInstance, StatefulFunctionRegistry,
    StatefulRegistryError,
};

use crate::catalog::FunctionDef;

pub fn builtin_stateful_function_defs() -> Vec<FunctionDef> {
    vec![
        acc_avg_function_def(),
        acc_count_function_def(),
        acc_max_function_def(),
        acc_min_function_def(),
        acc_sum_function_def(),
        change_capture_function_def(),
        change_to_function_def(),
        changed_col_function_def(),
        consecutive_count_function_def(),
        consecutive_start_function_def(),
        had_changed_function_def(),
        lag_function_def(),
        latest_function_def(),
    ]
}
