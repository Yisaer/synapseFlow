use super::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Value};
use std::any::Any;

pub struct LastRowFunction;

pub fn last_row_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "last_row".to_string(),
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
        description: "Return the most recent value in the group/window.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Return type matches the input type.".to_string(),
            "Returns NULL if the group/window is empty.".to_string(),
        ],
        examples: vec![
            "SELECT last_row(a) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT user_id, last_row(status) FROM s GROUP BY user_id, tumblingwindow('ss', 10)"
                .to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

impl LastRowFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LastRowFunction {
    fn default() -> Self {
        Self::new()
    }
}

struct LastRowAccumulator {
    last: Option<Value>,
}

impl AggregateAccumulator for LastRowAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let value = args
            .first()
            .ok_or_else(|| "last_row expects exactly 1 argument".to_string())?;
        Ok(Box::new(LastRowUpdate {
            value: value.clone(),
        }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<LastRowUpdate>()
            .expect("last_row accumulator received incompatible update");
        self.last = Some(update.value.clone());
    }

    fn finalize(&self) -> Value {
        self.last.clone().unwrap_or(Value::Null)
    }
}

struct LastRowUpdate {
    value: Value,
}

impl AggregateUpdate for LastRowUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl AggregateFunction for LastRowFunction {
    fn name(&self) -> &str {
        "last_row"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "last_row expects exactly 1 argument, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[0].clone())
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(LastRowAccumulator { last: None })
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::Int64Type;

    #[test]
    fn last_row_function_name() {
        let func = LastRowFunction::new();
        assert_eq!(func.name(), "last_row");
    }

    #[test]
    fn last_row_function_default() {
        let func = LastRowFunction::default();
        assert_eq!(func.name(), "last_row");
    }

    #[test]
    fn last_row_function_supports_incremental() {
        let func = LastRowFunction::new();
        assert!(func.supports_incremental());
    }

    #[test]
    fn last_row_function_return_type_valid() {
        let func = LastRowFunction::new();
        let result = func.return_type(&[ConcreteDatatype::Int64(Int64Type)]);
        assert!(result.is_ok());
    }

    #[test]
    fn last_row_function_return_type_no_args() {
        let func = LastRowFunction::new();
        let result = func.return_type(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly 1 argument"));
    }

    #[test]
    fn last_row_accumulator_returns_last() {
        let func = LastRowFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(1)]).unwrap();
        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Int64(3)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(3));
    }

    #[test]
    fn last_row_accumulator_empty_returns_null() {
        let func = LastRowFunction::new();
        let acc = func.create_accumulator();
        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn last_row_accumulator_no_args_error() {
        let func = LastRowFunction::new();
        let mut acc = func.create_accumulator();
        let result = acc.update(&[]);
        assert!(result.is_err());
    }
}
