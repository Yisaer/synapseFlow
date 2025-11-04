//! DataFusion function implementation for CallDf expressions

use std::sync::Arc;
use datatypes::{Value, Schema as FlowSchema, ColumnSchema, ConcreteDatatype};

use crate::expr::evaluator::DataFusionEvaluator;
use crate::expr::ScalarExpr;

/// A DataFusion scalar function that can be evaluated
#[derive(Clone)]
pub struct DfScalarFunction {
    /// The name of the DataFusion function
    pub function_name: String,
    /// The arguments to the function
    pub args: Vec<ScalarExpr>,
    /// Cached evaluator for performance
    #[allow(dead_code)]
    evaluator: Arc<DataFusionEvaluator>,
}

impl std::fmt::Debug for DfScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfScalarFunction")
            .field("function_name", &self.function_name)
            .field("args", &self.args)
            .field("evaluator", &"<DataFusionEvaluator>")
            .finish()
    }
}

impl DfScalarFunction {
    /// Create a new DataFusion scalar function
    pub fn new(function_name: String, args: Vec<ScalarExpr>) -> Self {
        Self {
            function_name,
            args,
            evaluator: Arc::new(DataFusionEvaluator::new()),
        }
    }

    /// Evaluate the DataFusion function with given values
    /// This method now requires a DataFusionEvaluator and tuple for proper evaluation
    pub fn eval(&self, evaluator: &crate::expr::evaluator::DataFusionEvaluator, tuple: &crate::tuple::Tuple) -> Result<Value, crate::expr::func::EvalError> {
        // Create a CallDf expression and evaluate it using DataFusionEvaluator
        let call_df_expr = crate::expr::ScalarExpr::CallDf {
            function_name: self.function_name.clone(),
            args: self.args.clone(),
        };
        
        match evaluator.evaluate_expr(&call_df_expr, tuple) {
            Ok(value) => Ok(value),
            Err(df_error) => Err(crate::expr::func::EvalError::DataFusionError { 
                message: df_error.to_string() 
            }),
        }
    }

    /// Legacy eval method that returns error (for backward compatibility during transition)
    pub fn eval_legacy(&self, _values: &[Value]) -> Result<Value, crate::expr::func::EvalError> {
        Err(crate::expr::func::EvalError::NotImplemented {
            feature: format!("CallDf function '{}' must be evaluated through DataFusionEvaluator", self.function_name),
        })
    }
}

/// Create a simple schema based on the values
#[allow(dead_code)]
fn create_simple_schema(values: &[Value]) -> FlowSchema {
    let columns: Vec<ColumnSchema> = values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let data_type = value_to_concrete_datatype(value);
            ColumnSchema::new(format!("col_{}", i), data_type)
        })
        .collect();
    
    FlowSchema::new(columns)
}

/// Convert Value to ConcreteDatatype
#[allow(dead_code)]
fn value_to_concrete_datatype(value: &Value) -> ConcreteDatatype {
    match value {
        Value::Int64(_) => ConcreteDatatype::Int64(datatypes::Int64Type),
        Value::Float64(_) => ConcreteDatatype::Float64(datatypes::Float64Type),
        Value::String(_) => ConcreteDatatype::String(datatypes::StringType),
        Value::Bool(_) => ConcreteDatatype::Bool(datatypes::BooleanType),
        Value::Struct(_) => panic!("Struct type not supported in simple schema"),
        Value::List(_) => panic!("List type not supported in simple schema"),
    }
}

