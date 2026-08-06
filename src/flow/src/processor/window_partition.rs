use crate::expr::ScalarExpr;
use crate::model::Tuple;
use datatypes::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum PartitionKey {
    Global,
    Values(Vec<Value>),
}

pub(crate) fn eval_partition_key(
    scalars: &[ScalarExpr],
    tuple: &Tuple,
    context: &str,
) -> Result<PartitionKey, String> {
    if scalars.is_empty() {
        return Ok(PartitionKey::Global);
    }

    let mut values = Vec::with_capacity(scalars.len());
    for scalar in scalars {
        values.push(
            scalar
                .eval_with_tuple(tuple)
                .map_err(|err| format!("failed to evaluate {context} partition key: {err}"))?,
        );
    }
    Ok(PartitionKey::Values(values))
}
