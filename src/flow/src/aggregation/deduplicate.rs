use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, ListType, ListValue, Value};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug)]
pub struct DeduplicateFunction;

pub fn deduplicate_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "deduplicate".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "x".to_string(),
                r#type: TypeSpec::Any,
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::List {
                element: Box::new(TypeSpec::Any),
            },
        },
        description: "Collect distinct non-NULL values in first-seen order.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Ignores NULL inputs.".to_string(),
            "Returns values in first-seen order.".to_string(),
            "Returns NULL if all inputs are NULL or the group/window is empty.".to_string(),
            "Does not support incremental (streaming) updates.".to_string(),
        ],
        examples: vec![
            "SELECT deduplicate(tag) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT deduplicate(user_id) FROM s GROUP BY region".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: false,
        }),
        stateful: None,
    }
}

impl DeduplicateFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DeduplicateFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for DeduplicateFunction {
    fn name(&self) -> &str {
        "deduplicate"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "deduplicate expects exactly 1 argument, got {}",
                input_types.len()
            ));
        }

        Ok(ConcreteDatatype::List(ListType::new(Arc::new(
            input_types[0].clone(),
        ))))
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(DeduplicateAccumulator::default())
    }
}

#[derive(Debug, Default, Clone)]
struct DeduplicateAccumulator {
    values: Vec<Value>,
    seen: HashSet<Value>,
    datatype: Option<Arc<ConcreteDatatype>>,
}

impl AggregateAccumulator for DeduplicateAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("deduplicate expects one argument".to_string());
        };

        let value = if value.is_null() || self.seen.contains(value) {
            None
        } else {
            Some(value.clone())
        };
        Ok(Box::new(DeduplicateUpdate { value }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<DeduplicateUpdate>()
            .expect("deduplicate accumulator received incompatible update");
        if let Some(value) = &update.value {
            self.seen.insert(value.clone());
            if self.datatype.is_none() {
                self.datatype = Some(Arc::new(value.datatype()));
            }
            self.values.push(value.clone());
        }
    }

    fn finalize(&self) -> Value {
        let Some(datatype) = self.datatype.clone() else {
            return Value::Null;
        };

        Value::List(ListValue::new(self.values.clone(), datatype))
    }
}

struct DeduplicateUpdate {
    value: Option<Value>,
}

impl AggregateUpdate for DeduplicateUpdate {
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
    fn deduplicate_accumulator_preserves_first_seen_order() {
        let func = DeduplicateFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Int64(1)]).unwrap();
        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Null]).unwrap();
        acc.update(&[Value::Int64(3)]).unwrap();

        let expected = Value::List(ListValue::new(
            vec![Value::Int64(2), Value::Int64(1), Value::Int64(3)],
            Arc::new(ConcreteDatatype::Int64(datatypes::Int64Type)),
        ));

        assert_eq!(acc.finalize(), expected);
    }
}
