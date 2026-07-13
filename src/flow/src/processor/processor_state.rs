use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// The set of SQL-visible function names that read pipeline state.
///
/// When the parser or expression converter encounters one of these
/// function names, it produces a `ScalarExpr::PipelineState` that is
/// resolved to `ScalarExpr::ProcessorState` during physical plan building.
pub const BUILTIN_PIPELINE_STATE_FUNCTIONS: &[&str] = &["last_hit_count"];

/// Returns `true` if `name` (case-insensitive) is a built-in pipeline state function.
pub fn is_pipeline_state_function(name: &str) -> bool {
    BUILTIN_PIPELINE_STATE_FUNCTIONS
        .iter()
        .any(|f| f.eq_ignore_ascii_case(name))
}

/// Processor-local state for tracking pipeline-level runtime counters.
///
/// Each processor that needs to observe pipeline state (Filter for WHERE,
/// Project for SELECT) holds its own `ProcessorState` instance. The same
/// `Arc<AtomicU64>` backing the counter is embedded in `ScalarExpr` via the
/// `ScalarExpr::ProcessorState` variant, enabling lock-free reads during
/// expression evaluation without signature changes.
///
/// See `docs/syntax/language/pipeline_state.md` for the full design.
#[derive(Debug, Clone)]
pub struct ProcessorState {
    pub last_hit_count: Arc<AtomicU64>,
}

impl ProcessorState {
    pub fn new() -> Self {
        Self {
            last_hit_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for ProcessorState {
    fn default() -> Self {
        Self::new()
    }
}
