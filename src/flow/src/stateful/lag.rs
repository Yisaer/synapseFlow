use super::{StatefulFunction, StatefulFunctionInstance};
use datatypes::{ConcreteDatatype, Value};

pub struct LagFunction;

impl LagFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LagFunction {
    fn default() -> Self {
        Self::new()
    }
}

struct LagInstance {
    prev: Option<Value>,
}

impl StatefulFunctionInstance for LagInstance {
    fn eval(&mut self, args: &[Value]) -> Result<Value, String> {
        if args.len() != 1 {
            return Err(format!("lag() expects exactly 1 argument, got {}", args.len()));
        }
        let out = self.prev.clone().unwrap_or(Value::Null);
        self.prev = Some(args[0].clone());
        Ok(out)
    }
}

impl StatefulFunction for LagFunction {
    fn name(&self) -> &str {
        "lag"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "lag() expects exactly 1 argument type, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[0].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(LagInstance { prev: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    #[test]
    fn lag_emits_previous_value() {
        let function = LagFunction::new();
        let mut instance = function.create_instance();

        assert_eq!(instance.eval(&[Value::Int64(1)]).unwrap(), Value::Null);
        assert_eq!(instance.eval(&[Value::Int64(2)]).unwrap(), Value::Int64(1));
        assert_eq!(instance.eval(&[Value::Int64(3)]).unwrap(), Value::Int64(2));
    }

    #[test]
    fn lag_type_matches_input() {
        let function = LagFunction::new();
        let ty = function
            .return_type(&[ConcreteDatatype::Int64(types::Int64Type)])
            .unwrap();
        assert!(matches!(ty, ConcreteDatatype::Int64(_)));
    }
}

