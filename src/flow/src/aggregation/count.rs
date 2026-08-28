use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Int64Type, Value};
use std::any::Any;

#[derive(Debug)]
pub struct CountFunction;

pub fn count_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "count".to_string(),
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
        description: "Count rows or non-NULL values.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "COUNT(*) counts all rows in the group/window.".to_string(),
            "COUNT(expr) ignores NULL values.".to_string(),
            "COUNT(DISTINCT ...) is not supported.".to_string(),
        ],
        examples: vec![
            "SELECT count(*) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT count(user_id) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

impl Default for CountFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl CountFunction {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "count"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "COUNT expects exactly one argument, got {}",
                input_types.len()
            ));
        }
        Ok(ConcreteDatatype::Int64(Int64Type))
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(CountAccumulator::default())
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Clone)]
struct CountAccumulator {
    count: i64,
}

impl AggregateAccumulator for CountAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("COUNT expects one argument".to_string());
        };
        Ok(Box::new(CountUpdate {
            increment: !value.is_null(),
        }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<CountUpdate>()
            .expect("count accumulator received incompatible update");
        if update.increment {
            self.count = self.count.saturating_add(1);
        }
    }

    fn finalize(&self) -> Value {
        Value::Int64(self.count)
    }
}

struct CountUpdate {
    increment: bool,
}

impl AggregateUpdate for CountUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
