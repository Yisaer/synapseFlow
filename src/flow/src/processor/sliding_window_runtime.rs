use crate::expr::ScalarExpr;
use crate::model::Tuple;
use crate::processor::ProcessorError;
use datatypes::Value;

pub(crate) fn evaluate_trigger_condition(
    trigger_condition: Option<&ScalarExpr>,
    tuple: &Tuple,
    context: &str,
) -> Result<bool, ProcessorError> {
    let Some(trigger_condition) = trigger_condition else {
        return Ok(true);
    };

    match trigger_condition.eval_with_tuple(tuple).map_err(|err| {
        ProcessorError::ProcessingError(format!("failed to evaluate {context}: {err}"))
    })? {
        Value::Bool(value) => Ok(value),
        Value::Null => Ok(false),
        other => Err(ProcessorError::ProcessingError(format!(
            "{context} must be bool or null, got {other:?}",
        ))),
    }
}
