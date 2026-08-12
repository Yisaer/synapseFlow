use super::{
    PreparedStatefulEval, StatefulEvalInput, StatefulEvalUpdate, StatefulFunction,
    StatefulFunctionInstance,
};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Value};
use std::any::Any;

pub struct LatestFunction;

pub fn latest_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "latest".to_string(),
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
        description: "Return the latest accepted non-NULL value of the argument.".to_string(),
        allowed_contexts: vec![FunctionContext::Select, FunctionContext::Where],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Ignores NULL input values and keeps the previously accepted value.".to_string(),
            "If no value has been accepted yet, returns NULL.".to_string(),
        ],
        examples: vec!["SELECT latest(status) AS latest_status FROM stream".to_string()],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics: "Maintains the latest accepted non-NULL value per partition."
                .to_string(),
        }),
    }
}

impl LatestFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LatestFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct LatestInstance {
    latest: Option<Value>,
}

impl StatefulFunctionInstance for LatestInstance {
    fn prepare_eval(&self, input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        if input.args.len() != 1 {
            return Err(format!(
                "latest() expects exactly 1 argument, got {}",
                input.args.len()
            ));
        }

        let current = &input.args[0];
        if input.should_apply && !current.is_null() {
            return Ok(PreparedStatefulEval::new(
                current.clone(),
                Box::new(LatestUpdate {
                    latest: Some(current.clone()),
                }),
            ));
        }

        let output = self.latest.clone().unwrap_or(Value::Null);
        Ok(PreparedStatefulEval::new(
            output,
            Box::new(LatestUpdate { latest: None }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<LatestUpdate>()
            .expect("latest received incompatible update");
        if let Some(value) = &update.latest {
            self.latest = Some(value.clone());
        }
    }
}

struct LatestUpdate {
    latest: Option<Value>,
}

impl StatefulEvalUpdate for LatestUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for LatestFunction {
    fn name(&self) -> &str {
        "latest"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "latest() expects exactly 1 argument type, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[0].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(LatestInstance::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    #[test]
    fn latest_keeps_last_non_null_value() {
        let function = LatestFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null],
                    should_apply: true,
                })
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Int64(10)],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(10)
        );
        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null],
                    should_apply: true,
                })
                .unwrap(),
            Value::Int64(10)
        );
    }

    #[test]
    fn latest_returns_null_before_state_exists() {
        let function = LatestFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(
            instance
                .eval(StatefulEvalInput {
                    args: &[Value::Null],
                    should_apply: false,
                })
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn latest_type_matches_input() {
        let function = LatestFunction::new();
        let ty = function
            .return_type(&[ConcreteDatatype::Int64(types::Int64Type)])
            .unwrap();
        assert!(matches!(ty, ConcreteDatatype::Int64(_)));
    }
}
