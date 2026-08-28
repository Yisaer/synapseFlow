use crate::aggregation::{AggregateAccumulator, AggregateFunction, AggregateUpdate};
use crate::catalog::{
    AggregateFunctionSpec, FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind,
    FunctionRequirement, FunctionSignatureSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, StructField, StructType, StructValue, Value};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct MergeAggFunction;

pub fn merge_agg_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Aggregate,
        name: "merge_agg".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![FunctionArgSpec {
                name: "object".to_string(),
                r#type: TypeSpec::Any,
                optional: false,
                variadic: false,
            }],
            return_type: TypeSpec::Any,
        },
        description: "Shallow-merge object fields across a group/window.".to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::AggregateContext],
        constraints: vec![
            "Requires exactly 1 argument.".to_string(),
            "Accepts merge_agg(*) or a struct-valued expression.".to_string(),
            "Later values overwrite earlier values for duplicate top-level fields.".to_string(),
            "NULL also overwrites an earlier value when it is a struct field.".to_string(),
            "Ignores arguments that are not structs.".to_string(),
            "Returns NULL if no struct input is observed.".to_string(),
        ],
        examples: vec![
            "SELECT merge_agg(*) FROM s GROUP BY tumblingwindow('ss', 10)".to_string(),
            "SELECT merge_agg(properties) FROM s GROUP BY device_id".to_string(),
        ],
        aggregate: Some(AggregateFunctionSpec {
            supports_incremental: true,
        }),
        stateful: None,
    }
}

impl MergeAggFunction {
    pub fn new() -> Self {
        Self
    }
}

impl AggregateFunction for MergeAggFunction {
    fn name(&self) -> &str {
        "merge_agg"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.len() != 1 {
            return Err(format!(
                "merge_agg expects exactly 1 argument, got {}",
                input_types.len()
            ));
        }

        match &input_types[0] {
            ConcreteDatatype::Struct(struct_type) => {
                Ok(ConcreteDatatype::Struct(struct_type.clone()))
            }
            _ => Ok(ConcreteDatatype::Null),
        }
    }

    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(MergeAggAccumulator::default())
    }

    fn supports_incremental(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct MergeAggAccumulator {
    field_indexes: HashMap<String, usize>,
    fields: Vec<(StructField, Value)>,
    observed_struct: bool,
}

impl AggregateAccumulator for MergeAggAccumulator {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String> {
        if args.len() != 1 {
            return Err(format!(
                "merge_agg expects exactly 1 argument, got {}",
                args.len()
            ));
        }

        let value = match &args[0] {
            Value::Struct(value) => Some(value.clone()),
            _ => None,
        };

        Ok(Box::new(MergeAggUpdate { value }))
    }

    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>) {
        let update = update
            .into_any()
            .downcast::<MergeAggUpdate>()
            .expect("merge_agg accumulator received incompatible update");
        let Some(struct_value) = update.value else {
            return;
        };
        self.observed_struct = true;
        let (values, struct_type) = struct_value.into_parts();

        for (field, value) in struct_type.fields().iter().zip(values) {
            if let Some(index) = self.field_indexes.get(field.name()).copied() {
                self.fields[index].1 = value;
            } else {
                let index = self.fields.len();
                self.field_indexes.insert(field.name().to_string(), index);
                self.fields.push((field.clone(), value));
            }
        }
    }

    fn finalize(&self) -> Value {
        if !self.observed_struct {
            return Value::Null;
        }

        let fields = self.fields.iter().map(|(field, _)| field.clone()).collect();
        let values = self.fields.iter().map(|(_, value)| value.clone()).collect();

        Value::Struct(StructValue::new(values, StructType::new(Arc::new(fields))))
    }
}

struct MergeAggUpdate {
    value: Option<StructValue>,
}

impl AggregateUpdate for MergeAggUpdate {
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
    use datatypes::{Int64Type, StringType};

    fn struct_value(fields: Vec<(&str, Value)>) -> Value {
        let struct_fields = fields
            .iter()
            .map(|(name, value)| StructField::new((*name).to_string(), value.datatype(), true))
            .collect();
        let values = fields.into_iter().map(|(_, value)| value).collect();
        Value::Struct(StructValue::new(
            values,
            StructType::new(Arc::new(struct_fields)),
        ))
    }

    #[test]
    fn merge_agg_unions_fields_and_overwrites_in_first_seen_order() {
        let mut accumulator = MergeAggFunction::new().create_accumulator();
        accumulator
            .update(&[struct_value(vec![
                ("a", Value::Int64(1)),
                ("b", Value::Int64(2)),
            ])])
            .unwrap();
        accumulator
            .update(&[struct_value(vec![
                ("b", Value::Int64(3)),
                ("c", Value::String("value".to_string())),
            ])])
            .unwrap();

        let Value::Struct(result) = accumulator.finalize() else {
            panic!("expected struct result");
        };
        let names = result
            .fields()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a", "b", "c"]);
        assert_eq!(
            result.items(),
            &[
                Value::Int64(1),
                Value::Int64(3),
                Value::String("value".to_string()),
            ]
        );
    }

    #[test]
    fn merge_agg_replaces_nested_struct_and_preserves_null_field() {
        let mut accumulator = MergeAggFunction::new().create_accumulator();
        accumulator
            .update(&[struct_value(vec![(
                "nested",
                struct_value(vec![("x", Value::Int64(1)), ("y", Value::Int64(2))]),
            )])])
            .unwrap();
        let replacement = struct_value(vec![("x", Value::Null)]);
        accumulator
            .update(&[struct_value(vec![("nested", replacement.clone())])])
            .unwrap();

        let Value::Struct(result) = accumulator.finalize() else {
            panic!("expected struct result");
        };
        assert_eq!(result.get_field("nested"), Some(&replacement));
    }

    #[test]
    fn merge_agg_distinguishes_empty_struct_from_no_struct_input() {
        let mut accumulator = MergeAggFunction::new().create_accumulator();
        accumulator.update(&[Value::Null]).unwrap();
        accumulator.update(&[Value::Int64(1)]).unwrap();

        assert_eq!(accumulator.finalize(), Value::Null);

        accumulator.update(&[struct_value(vec![])]).unwrap();

        assert_eq!(accumulator.finalize(), struct_value(vec![]));
    }

    #[test]
    fn merge_agg_preserves_declared_field_type_when_null_overwrites_value() {
        let mut accumulator = MergeAggFunction::new().create_accumulator();
        let struct_type = StructType::new(Arc::new(vec![StructField::new(
            "x".to_string(),
            ConcreteDatatype::Int64(Int64Type),
            true,
        )]));
        accumulator
            .update(&[Value::Struct(StructValue::new(
                vec![Value::Int64(1)],
                struct_type.clone(),
            ))])
            .unwrap();
        accumulator
            .update(&[Value::Struct(StructValue::new(
                vec![Value::Null],
                struct_type.clone(),
            ))])
            .unwrap();

        assert_eq!(
            accumulator.finalize(),
            Value::Struct(StructValue::new(vec![Value::Null], struct_type))
        );
    }

    #[test]
    fn merge_agg_validates_signature_and_incremental_capability() {
        let function = MergeAggFunction::new();
        let struct_type = StructType::new(Arc::new(vec![StructField::new(
            "x".to_string(),
            ConcreteDatatype::Int64(Int64Type),
            true,
        )]));

        assert_eq!(
            function.return_type(&[ConcreteDatatype::Struct(struct_type.clone())]),
            Ok(ConcreteDatatype::Struct(struct_type))
        );
        assert_eq!(
            function.return_type(&[ConcreteDatatype::String(StringType)]),
            Ok(ConcreteDatatype::Null)
        );
        assert!(function.return_type(&[]).is_err());
        assert!(function.supports_incremental());

        let mut accumulator = function.create_accumulator();
        assert!(accumulator.update(&[]).is_err());
        assert!(accumulator
            .update(&[Value::Int64(1), Value::Int64(2)])
            .is_err());
    }
}
