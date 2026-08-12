use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, DataType, Float64Type, Value};
use std::any::Any;

#[derive(Debug)]
pub struct StddevFunction;

#[derive(Debug)]
pub struct StddevsFunction;

#[derive(Debug)]
pub struct VarFunction;

#[derive(Debug)]
pub struct VarsFunction;

pub fn stddev_function_def() -> FunctionDef {
    numeric_stat_function_def(
        "stddev",
        "Population standard deviation of numeric values.",
        vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be numeric (int/uint/float).".to_string(),
            "Ignores NULL inputs; returns NULL if all inputs are NULL.".to_string(),
            "Returns 0.0 when exactly one non-NULL value is present.".to_string(),
            "Returns float64.".to_string(),
        ],
        vec![
            "SELECT stddev(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT stddev(latency) FROM requests GROUP BY endpoint".to_string(),
        ],
        true,
    )
}

pub fn stddevs_function_def() -> FunctionDef {
    numeric_stat_function_def(
        "stddevs",
        "Sample standard deviation of numeric values.",
        vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be numeric (int/uint/float).".to_string(),
            "Ignores NULL inputs; returns NULL if fewer than 2 non-NULL values are present."
                .to_string(),
            "Returns float64.".to_string(),
        ],
        vec![
            "SELECT stddevs(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT stddevs(latency) FROM requests GROUP BY endpoint".to_string(),
        ],
        true,
    )
}

pub fn var_function_def() -> FunctionDef {
    numeric_stat_function_def(
        "var",
        "Population variance of numeric values.",
        vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be numeric (int/uint/float).".to_string(),
            "Ignores NULL inputs; returns NULL if all inputs are NULL.".to_string(),
            "Returns 0.0 when exactly one non-NULL value is present.".to_string(),
            "Returns float64.".to_string(),
        ],
        vec![
            "SELECT var(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT var(latency) FROM requests GROUP BY endpoint".to_string(),
        ],
        true,
    )
}

pub fn vars_function_def() -> FunctionDef {
    numeric_stat_function_def(
        "vars",
        "Sample variance of numeric values.",
        vec![
            "Requires exactly 1 argument.".to_string(),
            "Argument type must be numeric (int/uint/float).".to_string(),
            "Ignores NULL inputs; returns NULL if fewer than 2 non-NULL values are present."
                .to_string(),
            "Returns float64.".to_string(),
        ],
        vec![
            "SELECT vars(x) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT vars(latency) FROM requests GROUP BY endpoint".to_string(),
        ],
        true,
    )
}

fn numeric_stat_function_def(
    name: &str,
    description: &str,
    constraints: Vec<String>,
    examples: Vec<String>,
    supports_incremental: bool,
) -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: name.to_string(),
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
            return_type: TypeSpec::Named {
                name: "float64".to_string(),
            },
        },
        description: description.to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints,
        examples,
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental,
        }),
        stateful: None,
    }
}

impl StddevFunction {
    pub fn new() -> Self {
        Self
    }
}

impl StddevsFunction {
    pub fn new() -> Self {
        Self
    }
}

impl VarFunction {
    pub fn new() -> Self {
        Self
    }
}

impl VarsFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StddevFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for StddevsFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for VarFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for VarsFunction {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_numeric_input(
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
        ConcreteDatatype::Int8(_)
        | ConcreteDatatype::Int16(_)
        | ConcreteDatatype::Int32(_)
        | ConcreteDatatype::Int64(_)
        | ConcreteDatatype::Uint8(_)
        | ConcreteDatatype::Uint16(_)
        | ConcreteDatatype::Uint32(_)
        | ConcreteDatatype::Uint64(_)
        | ConcreteDatatype::Float32(_)
        | ConcreteDatatype::Float64(_) => Ok(ConcreteDatatype::Float64(Float64Type)),
        other => Err(format!("{name} does not support type {:?}", other)),
    }
}

impl AggregateFunction for StddevFunction {
    fn name(&self) -> &str {
        "stddev"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_input("stddev", input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(VarianceAccumulator::new(VarianceMode::PopulationStddev))
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

impl AggregateFunction for StddevsFunction {
    fn name(&self) -> &str {
        "stddevs"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_input("stddevs", input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(VarianceAccumulator::new(VarianceMode::SampleStddev))
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

impl AggregateFunction for VarFunction {
    fn name(&self) -> &str {
        "var"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_input("var", input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(VarianceAccumulator::new(VarianceMode::PopulationVariance))
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

impl AggregateFunction for VarsFunction {
    fn name(&self) -> &str {
        "vars"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        validate_numeric_input("vars", input_types)
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(VarianceAccumulator::new(VarianceMode::SampleVariance))
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy)]
enum VarianceMode {
    PopulationStddev,
    SampleStddev,
    PopulationVariance,
    SampleVariance,
}

#[derive(Debug, Clone)]
struct VarianceAccumulator {
    mode: VarianceMode,
    count: u64,
    mean: f64,
    m2: f64,
}

impl VarianceAccumulator {
    fn new(mode: VarianceMode) -> Self {
        Self {
            mode,
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    fn cast_to_f64(value: &Value) -> Result<Option<f64>, String> {
        if value.is_null() {
            return Ok(None);
        }

        let float64 = Float64Type;
        match float64.try_cast(value.clone()) {
            Some(Value::Float64(v)) => Ok(Some(v)),
            _ => Err(format!("stat aggregate does not support value {:?}", value)),
        }
    }
}

impl AggregateAccumulator for VarianceAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        let Some(value) = args.first() else {
            return Err("stat aggregate expects one argument".to_string());
        };

        let next = match Self::cast_to_f64(value)? {
            Some(x) => {
                let count = self.count.saturating_add(1);
                let delta = x - self.mean;
                let mean = self.mean + delta / count as f64;
                let delta2 = x - mean;
                let m2 = self.m2 + delta * delta2;
                Some((count, mean, m2))
            }
            None => None,
        };

        Ok(Box::new(VarianceUpdate { next }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<VarianceUpdate>()
            .expect("variance accumulator received incompatible update");
        if let Some((count, mean, m2)) = update.next {
            self.count = count;
            self.mean = mean;
            self.m2 = m2;
        }
    }

    fn finalize(&self) -> Value {
        match self.mode {
            VarianceMode::PopulationVariance => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float64(self.m2 / self.count as f64)
                }
            }
            VarianceMode::SampleVariance => {
                if self.count < 2 {
                    Value::Null
                } else {
                    Value::Float64(self.m2 / (self.count - 1) as f64)
                }
            }
            VarianceMode::PopulationStddev => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float64((self.m2 / self.count as f64).sqrt())
                }
            }
            VarianceMode::SampleStddev => {
                if self.count < 2 {
                    Value::Null
                } else {
                    Value::Float64((self.m2 / (self.count - 1) as f64).sqrt())
                }
            }
        }
    }
}

struct VarianceUpdate {
    next: Option<(u64, f64, f64)>,
}

impl AggregateUpdate for VarianceUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stddev_accumulator_returns_population_standard_deviation() {
        let func = StddevFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Int64(4)]).unwrap();
        acc.update(&[Value::Int64(4)]).unwrap();
        acc.update(&[Value::Int64(4)]).unwrap();
        acc.update(&[Value::Int64(5)]).unwrap();
        acc.update(&[Value::Int64(5)]).unwrap();
        acc.update(&[Value::Int64(7)]).unwrap();
        acc.update(&[Value::Int64(9)]).unwrap();

        assert_eq!(acc.finalize(), Value::Float64(2.0));
    }

    #[test]
    fn stddevs_accumulator_returns_sample_standard_deviation() {
        let func = StddevsFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(2)]).unwrap();
        acc.update(&[Value::Int64(4)]).unwrap();
        acc.update(&[Value::Int64(4)]).unwrap();
        acc.update(&[Value::Int64(4)]).unwrap();
        acc.update(&[Value::Int64(5)]).unwrap();
        acc.update(&[Value::Int64(5)]).unwrap();
        acc.update(&[Value::Int64(7)]).unwrap();
        acc.update(&[Value::Int64(9)]).unwrap();

        match acc.finalize() {
            Value::Float64(v) => assert!((v - 2.138_089_935_299_395).abs() < 1e-12),
            other => panic!("expected float64, got {:?}", other),
        }
    }

    #[test]
    fn var_accumulator_returns_population_variance() {
        let func = VarFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(600)]).unwrap();
        acc.update(&[Value::Int64(470)]).unwrap();
        acc.update(&[Value::Int64(170)]).unwrap();
        acc.update(&[Value::Int64(430)]).unwrap();
        acc.update(&[Value::Int64(300)]).unwrap();

        match acc.finalize() {
            Value::Float64(v) => assert!((v - 21_704.0).abs() < 1e-9),
            other => panic!("expected float64, got {:?}", other),
        }
    }

    #[test]
    fn vars_accumulator_returns_sample_variance() {
        let func = VarsFunction::new();
        let mut acc = func.create_accumulator();

        acc.update(&[Value::Int64(600)]).unwrap();
        acc.update(&[Value::Int64(470)]).unwrap();
        acc.update(&[Value::Int64(170)]).unwrap();
        acc.update(&[Value::Int64(430)]).unwrap();
        acc.update(&[Value::Int64(300)]).unwrap();

        match acc.finalize() {
            Value::Float64(v) => assert!((v - 27_130.0).abs() < 1e-9),
            other => panic!("expected float64, got {:?}", other),
        }
    }
}
