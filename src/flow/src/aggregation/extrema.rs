use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use crate::expr::value_compare::compare_values;
use datatypes::{ConcreteDatatype, Value};
use std::any::Any;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct MaxFunction;

#[derive(Debug)]
pub struct MinFunction;

pub fn max_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "max".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Any,
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Any,
        },
        description: "Maximum non-NULL value in the group/window.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be comparable scalar data.".to_string(),
            "Ignores NULL inputs; returns NULL if all inputs are NULL.".to_string(),
            "Return type matches the input type.".to_string(),
        ],
        examples: vec![
            "SELECT max(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT max(score) FROM leaderboard GROUP BY user_id".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

pub fn min_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "min".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Any,
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Any,
        },
        description: "Minimum non-NULL value in the group/window.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be comparable scalar data.".to_string(),
            "Ignores NULL inputs; returns NULL if all inputs are NULL.".to_string(),
            "Return type matches the input type.".to_string(),
        ],
        examples: vec![
            "SELECT min(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT min(score) FROM leaderboard GROUP BY user_id".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

impl MaxFunction {
    pub fn new() -> Self {
        Self
    }
}

impl MinFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MaxFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for MinFunction {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_extrema_input(
    name: &str,
    input_types: &[ConcreteDatatype],
) -> Result<ConcreteDatatype, String> {
    if input_types.len() != 1 {
        return Err(format!(
            "{name} expects exactly 1 argument, got {}",
            input_types.len()
        ));
    }

    match &input_types[0] {
        ConcreteDatatype::Null | ConcreteDatatype::Struct(_) | ConcreteDatatype::List(_) => {
            Err(format!("{name} does not support type {:?}", input_types[0]))
        }
        other => Ok(other.clone()),
    }
}

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_extrema_input("max", input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(MaxAccumulator::default())
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_extrema_input("min", input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(MinAccumulator::default())
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Clone)]
struct MaxAccumulator {
    value: Option<Value>,
}

#[derive(Debug, Default, Clone)]
struct MinAccumulator {
    value: Option<Value>,
}

impl AggregateAccumulator for MaxAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("max expects one argument".to_string());
        };

        if value.is_null() {
            return Ok(Box::new(ExtremaUpdate { value: None }));
        }

        let next = match self.value.as_ref() {
            None => Some(value.clone()),
            Some(current) => {
                let ordering = compare_values(value, current)
                    .ok_or_else(|| format!("max cannot compare {:?} and {:?}", value, current))?;
                if ordering == Ordering::Greater {
                    Some(value.clone())
                } else {
                    None
                }
            }
        };

        Ok(Box::new(ExtremaUpdate { value: next }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<ExtremaUpdate>()
            .expect("max accumulator received incompatible update");
        if let Some(value) = &update.value {
            self.value = Some(value.clone());
        }
    }

    fn finalize(&self) -> Value {
        self.value.clone().unwrap_or(Value::Null)
    }
}

impl AggregateAccumulator for MinAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("min expects one argument".to_string());
        };

        if value.is_null() {
            return Ok(Box::new(ExtremaUpdate { value: None }));
        }

        let next = match self.value.as_ref() {
            None => Some(value.clone()),
            Some(current) => {
                let ordering = compare_values(value, current)
                    .ok_or_else(|| format!("min cannot compare {:?} and {:?}", value, current))?;
                if ordering == Ordering::Less {
                    Some(value.clone())
                } else {
                    None
                }
            }
        };

        Ok(Box::new(ExtremaUpdate { value: next }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<ExtremaUpdate>()
            .expect("min accumulator received incompatible update");
        if let Some(value) = &update.value {
            self.value = Some(value.clone());
        }
    }

    fn finalize(&self) -> Value {
        self.value.clone().unwrap_or(Value::Null)
    }
}

struct ExtremaUpdate {
    value: Option<Value>,
}

impl AggregateUpdate for ExtremaUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_accumulator_returns_largest_non_null_value() {
        let func = MaxFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(10)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(30)]).unwrap();
        acc.update(&[Value::Int64(20)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(30));
    }

    #[test]
    fn min_accumulator_returns_smallest_non_null_value() {
        let func = MinFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(10)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(30)]).unwrap();
        acc.update(&[Value::Int64(20)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(10));
    }
}
