use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Int64Type, Value};
use std::any::Any;
use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct NdvFunction;

pub fn ndv_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "ndv".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Any,
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Named {
                name: "int64".to_string(),
            },
        },
        description: "Number of distinct non-NULL values.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Ignores NULL inputs.".to_string(),
            "Returns 0 if all inputs are NULL or the group/window is empty.".to_string(),
            "Does not support incremental (streaming) updates.".to_string(),
        ],
        examples: vec![
            "SELECT ndv(user_id) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT ndv(tag) FROM s GROUP BY b".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: false,
        }),
        stateful: None,
    }
}

impl NdvFunction {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for NdvFunction {
    fn name(&self) -> &str {
        "ndv"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "NDV expects exactly one argument, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(NdvAccumulator::default())
    }
}

#[derive(Debug, Default, Clone)]
struct NdvAccumulator {
    distinct_values: HashSet<Value>,
}

impl AggregateAccumulator for NdvAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("NDV expects one argument".to_string());
        };
        Ok(Box::new(NdvUpdate {
            value: if value.is_null() {
                None
            } else {
                Some(value.clone())
            },
        }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<NdvUpdate>()
            .expect("ndv accumulator received incompatible update");
        if let Some(value) = &update.value {
            self.distinct_values.insert(value.clone());
        }
    }

    fn finalize(&self) -> Value {
        Value::Int64(i64::try_from(self.distinct_values.len()).unwrap_or(i64::MAX))
    }
}

struct NdvUpdate {
    value: Option<Value>,
}

impl AggregateUpdate for NdvUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndv_function_name() {
        let func = NdvFunction::new();
        assert_eq!(func.name(), "ndv");
    }

    #[test]
    fn ndv_function_default() {
        let func = NdvFunction::default();
        assert_eq!(func.name(), "ndv");
    }

    #[test]
    fn ndv_function_return_type_valid() {
        let func = NdvFunction::new();
        let result = func.return_type(&[ConcreteDatatype::Int64(Int64Type)]);
        assert!(result.is_ok());
        // NDV always returns Int64
        assert!(matches!(result.unwrap(), ConcreteDatatype::Int64(_)));
    }

    #[test]
    fn ndv_function_return_type_no_args() {
        let func = NdvFunction::new();
        let result = func.return_type(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly one argument"));
    }

    #[test]
    fn ndv_accumulator_counts_distinct() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(1)]).unwrap();
        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Int64(1)]).unwrap(); // duplicate
        acc.update(&[Value::Int64(3)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(3)); // 3 distinct values
    }

    #[test]
    fn ndv_accumulator_ignores_nulls() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(1)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(2)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(2)); // nulls not counted
    }

    #[test]
    fn ndv_accumulator_empty_returns_zero() {
        let func = NdvFunction::new();
        let acc = func.create_accumulator();
        assert_eq!(acc.finalize(), Value::Int64(0));
    }

    #[test]
    fn ndv_accumulator_no_args_error() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();
        let result = acc.update(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn ndv_accumulator_strings() {
        let func = NdvFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::String("a".to_string())]).unwrap();
        acc.update(&[Value::String("b".to_string())]).unwrap();
        acc.update(&[Value::String("a".to_string())]).unwrap(); // duplicate

        assert_eq!(acc.finalize(), Value::Int64(2)); // 2 distinct values
    }
}
