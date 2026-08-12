use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use crate::expr::func::BinaryFunc;
use datatypes::{ConcreteDatatype, Value};
use std::any::Any;

#[derive(Debug)]
pub struct SumFunction;

pub fn sum_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "sum".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Category {
                    name: "numeric".to_string(),
                },
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Category {
                name: "numeric".to_string(),
            },
        },
        description: "Sum of numeric values.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be numeric (int/uint/float).".to_string(),
            "Ignores NULL inputs; returns NULL if all inputs are NULL.".to_string(),
            "Return type matches the input numeric type.".to_string(),
        ],
        examples: vec![
            "SELECT sum(x) AS total".to_string(),
            "SELECT sum(amount) FROM orders GROUP BY user_id".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

impl Default for SumFunction {
    fn default() -> Self {
        Self::new()
    }
}
impl SumFunction {
    pub fn new() -> Self {
        Self
    }

    fn validate_numeric_type(
        &self,
        input_types: &[ConcreteDatatype],
    ) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "SUM expects exactly one argument, got {}",
                input_types.len()
            ));
        }
        match &input_types[0] {
            ConcreteDatatype::Int8(_)
            | ConcreteDatatype::Int16(_)
            | ConcreteDatatype::Int32(_)
            | ConcreteDatatype::Int64(_)
            | ConcreteDatatype::Uint8(_)
            | ConcreteDatatype::Uint16(_)
            | ConcreteDatatype::Uint32(_)
            | ConcreteDatatype::Uint64(_)
            | ConcreteDatatype::Float32(_)
            | ConcreteDatatype::Float64(_) => Ok(input_types[0].clone()),
            other => Err(format!("SUM does not support type {:?}", other)),
        }
    }
}

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        self.validate_numeric_type(input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(SumAccumulator::default())
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Clone)]
struct SumAccumulator {
    acc: Option<Value>,
}

impl SumAccumulator {
    fn add_values(current: Option<Value>, next: Value) -> Result<Option<Value>, String> {
        if next.is_null() {
            return Ok(current);
        }
        match current {
            Some(existing) => BinaryFunc::Add
                .eval_binary(existing, next)
                .map(Some)
                .map_err(|err| err.to_string()),
            None => Ok(Some(next)),
        }
    }
}

impl AggregateAccumulator for SumAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("SUM expects one argument".to_string());
        };
        Ok(Box::new(SumUpdate {
            next: Self::add_values(self.acc.clone(), value.clone())?,
        }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<SumUpdate>()
            .expect("sum accumulator received incompatible update");
        self.acc = update.next.clone();
    }

    fn finalize(&self) -> Value {
        self.acc.clone().unwrap_or(Value::Null)
    }
}

struct SumUpdate {
    next: Option<Value>,
}

impl AggregateUpdate for SumUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{Float64Type, Int64Type, StringType};

    #[test]
    fn sum_function_name() {
        let sum = SumFunction::new();
        assert_eq!(sum.name(), "sum");
    }

    #[test]
    fn sum_function_supports_incremental() {
        let sum = SumFunction::new();
        assert!(sum.supports_incremental());
    }

    #[test]
    fn sum_function_return_type_int64() {
        let sum = SumFunction::new();
        let result = sum.return_type(&[ConcreteDatatype::Int64(Int64Type)]);
        assert!(result.is_ok());
    }

    #[test]
    fn sum_function_return_type_float64() {
        let sum = SumFunction::new();
        let result = sum.return_type(&[ConcreteDatatype::Float64(Float64Type)]);
        assert!(result.is_ok());
    }

    #[test]
    fn sum_function_return_type_error_no_args() {
        let sum = SumFunction::new();
        let result = sum.return_type(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly one argument"));
    }

    #[test]
    fn sum_function_return_type_error_non_numeric() {
        let sum = SumFunction::new();
        let result = sum.return_type(&[ConcreteDatatype::String(StringType)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not support"));
    }

    #[test]
    fn sum_accumulator_sum_integers() {
        let sum = SumFunction::new();
        let mut acc = sum.create_accumulator();

        acc.update(&[Value::Int64(10)]).unwrap();
        acc.update(&[Value::Int64(20)]).unwrap();
        acc.update(&[Value::Int64(30)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(60));
    }

    #[test]
    fn sum_accumulator_sum_floats() {
        let sum = SumFunction::new();
        let mut acc = sum.create_accumulator();

        acc.update(&[Value::Float64(1.5)]).unwrap();
        acc.update(&[Value::Float64(2.5)]).unwrap();

        assert_eq!(acc.finalize(), Value::Float64(4.0));
    }

    #[test]
    fn sum_accumulator_ignores_nulls() {
        let sum = SumFunction::new();
        let mut acc = sum.create_accumulator();

        acc.update(&[Value::Int64(10)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(20)]).unwrap();

        assert_eq!(acc.finalize(), Value::Int64(30));
    }

    #[test]
    fn sum_accumulator_all_nulls_returns_null() {
        let sum = SumFunction::new();
        let mut acc = sum.create_accumulator();

        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Null]).unwrap();

        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn sum_accumulator_empty_returns_null() {
        let sum = SumFunction::new();
        let acc = sum.create_accumulator();
        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn sum_accumulator_update_no_args_error() {
        let sum = SumFunction::new();
        let mut acc = sum.create_accumulator();
        let result = acc.update(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn sum_function_def_returns_valid_def() {
        let def = sum_function_def();
        assert_eq!(def.name, "sum");
        assert_eq!(def.kind, FunctionKind::Aggregate);
        assert!(def.aggregate.is_some());
    }

    #[test]
    fn sum_function_default() {
        let sum = SumFunction::default();
        assert_eq!(sum.name(), "sum");
    }
}
