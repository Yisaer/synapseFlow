use super::{AggregateAccumulator, AggregateFunction};
use datatypes::{ConcreteDatatype, Value};

pub struct LastRowFunction;

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
    fn update(&mut self, args: &[Value]) -> Result<(), String> {
        let value = args
            .first()
            .ok_or_else(|| "last_row expects exactly 1 argument".to_string())?;
        self.last = Some(value.clone());
        Ok(())
    }

    fn finalize(&self) -> Value {
        self.last.clone().unwrap_or(Value::Null)
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
