use crate::aggregation::{AggregateAccumulator, AggregateFunction};
use crate::expr::func::BinaryFunc;
use datatypes::{ConcreteDatatype, Value};

#[derive(Debug)]
pub struct SumFunction;

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
    fn update(&mut self, args: &[Value]) -> Result<(), String> {
        let Some(value) = args.first() else {
            return Err("SUM expects one argument".to_string());
        };
        self.acc = Self::add_values(self.acc.take(), value.clone())?;
        Ok(())
    }

    fn finalize(&self) -> Value {
        self.acc.clone().unwrap_or(Value::Null)
    }
}
