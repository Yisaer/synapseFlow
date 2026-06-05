use crate::expr::func::{BinaryFunc, UnaryFunc};
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::planner::physical::PhysicalPlan;
use datatypes::{BooleanType, ConcreteDatatype};
use std::sync::Arc;

/// How to fetch a column's value from a runtime [`crate::model::Tuple`].
///
/// This is a planning-time contract that allows sinks/sources to materialize a stable
/// column layout without having to re-interpret SQL expressions at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputValueGetter {
    /// Fetch a value from a source message by column name.
    ///
    /// Note: a tuple may contain multiple messages with the same `source_name` (for example when
    /// a `Project` includes `*` plus extra projected columns). Callers should search all matching
    /// messages, not just the first one.
    MessageByName {
        source_name: Arc<str>,
        column_name: Arc<str>,
    },
    /// Fetch a value from the tuple affiliate (derived columns) by name.
    Affiliate { column_name: Arc<str> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumn {
    pub name: Arc<str>,
    pub data_type: ConcreteDatatype,
    pub getter: OutputValueGetter,
}

/// Ordered output columns produced by a physical plan node.
///
/// This is distinct from `datatypes::Schema`:
/// - It preserves output ordering (including `SELECT *` expansion order).
/// - It can represent duplicate column names (some sinks may reject duplicates).
/// - It includes value access strategy (`getter`) for materialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSchema {
    pub columns: Arc<[OutputColumn]>,
}

impl OutputSchema {
    pub fn new(columns: Vec<OutputColumn>) -> Self {
        Self {
            columns: Arc::from(columns),
        }
    }
}

impl PhysicalPlan {
    pub fn output_schema(&self) -> Result<OutputSchema, String> {
        match self {
            PhysicalPlan::DataSource(plan) => Ok(output_from_datatype_schema(plan.schema.as_ref())),
            PhysicalPlan::Decoder(plan) => Ok(output_from_datatype_schema(plan.schema().as_ref())),
            PhysicalPlan::SharedStream(plan) => {
                Ok(output_from_datatype_schema(plan.schema().as_ref()))
            }
            PhysicalPlan::CollectionLayoutNormalize(plan) => {
                Ok(output_from_datatype_schema(plan.schema.as_ref()))
            }
            PhysicalPlan::MemoryCollectionMaterialize(plan) => Ok(plan.output_schema.clone()),

            PhysicalPlan::Filter(_)
            | PhysicalPlan::Order(_)
            | PhysicalPlan::EmptySuppress(_)
            | PhysicalPlan::Batch(_)
            | PhysicalPlan::SinkEncoder(_)
            | PhysicalPlan::IncSinkEncoder(_)
            | PhysicalPlan::SinkCompress(_)
            | PhysicalPlan::ResultCollect(_)
            | PhysicalPlan::TumblingWindow(_)
            | PhysicalPlan::CountWindow(_)
            | PhysicalPlan::SlidingWindow(_)
            | PhysicalPlan::StateWindow(_)
            | PhysicalPlan::ProcessTimeWatermark(_)
            | PhysicalPlan::EventtimeWatermark(_)
            | PhysicalPlan::Watermark(_)
            | PhysicalPlan::Sampler(_)
            | PhysicalPlan::SourceChangeGate(_) => passthrough_single_child(self),

            PhysicalPlan::Barrier(_) => passthrough_fan_in(self),
            PhysicalPlan::DataSink(_) | PhysicalPlan::SinkConnector(_) => {
                passthrough_single_child(self)
            }

            PhysicalPlan::Compute(plan) => {
                let mut columns = passthrough_single_child(self)?.columns.to_vec();
                for field in &plan.fields {
                    let name: Arc<str> = Arc::from(field.field_name.as_str());
                    let data_type = infer_scalar_type(&field.compiled_expr, columns.as_slice())
                        .unwrap_or(ConcreteDatatype::Null);
                    columns.push(OutputColumn {
                        name: Arc::clone(&name),
                        data_type,
                        getter: OutputValueGetter::Affiliate { column_name: name },
                    });
                }
                Ok(OutputSchema::new(columns))
            }

            PhysicalPlan::StatefulFunction(plan) => {
                let mut columns = passthrough_single_child(self)?.columns.to_vec();
                for call in &plan.calls {
                    let name: Arc<str> = Arc::from(call.output_column.as_str());
                    columns.push(OutputColumn {
                        name: Arc::clone(&name),
                        data_type: ConcreteDatatype::Null,
                        getter: OutputValueGetter::Affiliate { column_name: name },
                    });
                }
                Ok(OutputSchema::new(columns))
            }

            PhysicalPlan::Aggregation(plan) => {
                let mut columns = passthrough_single_child(self)?.columns.to_vec();

                // Aggregate results are materialized into affiliate columns.
                for call in &plan.aggregate_calls {
                    let name: Arc<str> = Arc::from(call.output_column.as_str());
                    columns.push(OutputColumn {
                        name: Arc::clone(&name),
                        data_type: ConcreteDatatype::Null,
                        getter: OutputValueGetter::Affiliate { column_name: name },
                    });
                }

                // Non-simple group-by expressions are also materialized into affiliate columns.
                for expr in &plan.group_by_exprs {
                    if matches!(expr, sqlparser::ast::Expr::Identifier(_)) {
                        continue;
                    }
                    let name = expr.to_string();
                    let name: Arc<str> = Arc::from(name.as_str());
                    columns.push(OutputColumn {
                        name: Arc::clone(&name),
                        data_type: ConcreteDatatype::Null,
                        getter: OutputValueGetter::Affiliate { column_name: name },
                    });
                }

                Ok(OutputSchema::new(columns))
            }

            PhysicalPlan::StreamingAggregation(plan) => {
                // Streaming aggregation emits output columns via a downstream Project in current
                // plans. Keep it as a pass-through for Stage A.
                let _ = plan;
                passthrough_single_child(self)
            }

            PhysicalPlan::RowDiff(plan) => Ok(plan.output_schema.as_ref().clone()),

            PhysicalPlan::Project(plan) => {
                let input = passthrough_single_child(self)?;
                let mut out = Vec::new();

                for field in plan.fields.iter() {
                    match &field.compiled_expr {
                        ScalarExpr::Wildcard { source_name } => {
                            let prefix = source_name.as_deref();
                            for col in input.columns.iter() {
                                let OutputValueGetter::MessageByName { source_name, .. } =
                                    &col.getter
                                else {
                                    continue;
                                };
                                if let Some(prefix) = prefix {
                                    if source_name.as_ref() != prefix {
                                        continue;
                                    }
                                }
                                out.push(col.clone());
                            }
                        }
                        ScalarExpr::Column(ColumnRef::ByIndex { source_name, .. }) => {
                            let name: Arc<str> = Arc::clone(&field.field_name);
                            let data_type =
                                infer_scalar_type(&field.compiled_expr, input.columns.as_ref())
                                    .unwrap_or(ConcreteDatatype::Null);
                            out.push(OutputColumn {
                                name: Arc::clone(&name),
                                data_type,
                                getter: OutputValueGetter::MessageByName {
                                    source_name: Arc::from(source_name.as_str()),
                                    column_name: name,
                                },
                            });
                        }
                        _ => {
                            let name: Arc<str> = Arc::clone(&field.field_name);
                            let data_type =
                                infer_scalar_type(&field.compiled_expr, input.columns.as_ref())
                                    .unwrap_or(ConcreteDatatype::Null);
                            out.push(OutputColumn {
                                name: Arc::clone(&name),
                                data_type,
                                getter: OutputValueGetter::Affiliate { column_name: name },
                            });
                        }
                    }
                }

                Ok(OutputSchema::new(out))
            }
        }
    }
}

fn output_from_datatype_schema(schema: &datatypes::Schema) -> OutputSchema {
    let columns = schema
        .column_schemas()
        .iter()
        .map(|col| {
            let source_name: Arc<str> = Arc::from(col.source_name.as_str());
            let name: Arc<str> = Arc::from(col.name.as_str());
            OutputColumn {
                name: Arc::clone(&name),
                data_type: col.data_type.clone(),
                getter: OutputValueGetter::MessageByName {
                    source_name,
                    column_name: name,
                },
            }
        })
        .collect();
    OutputSchema::new(columns)
}

fn passthrough_single_child(plan: &PhysicalPlan) -> Result<OutputSchema, String> {
    let child = plan
        .children()
        .first()
        .ok_or_else(|| format!("{} expects 1 child", plan.get_plan_type()))?;
    if plan.children().len() != 1 {
        return Err(format!("{} expects 1 child", plan.get_plan_type()));
    }
    child.output_schema()
}

fn passthrough_fan_in(plan: &PhysicalPlan) -> Result<OutputSchema, String> {
    let mut iter = plan.children().iter();
    let first = iter
        .next()
        .ok_or_else(|| format!("{} expects at least 1 child", plan.get_plan_type()))?
        .output_schema()?;

    for child in iter {
        let schema = child.output_schema()?;
        if schema != first {
            return Err(format!(
                "{} fan-in children output schemas mismatch",
                plan.get_plan_type()
            ));
        }
    }
    Ok(first)
}

fn infer_scalar_type(expr: &ScalarExpr, input: &[OutputColumn]) -> Option<ConcreteDatatype> {
    match expr {
        ScalarExpr::Literal(_, typ) => Some(typ.clone()),
        ScalarExpr::Column(ColumnRef::ByIndex { column_index, .. }) => input
            .iter()
            .filter(|col| matches!(col.getter, OutputValueGetter::MessageByName { .. }))
            .nth(*column_index)
            .map(|col| col.data_type.clone()),
        ScalarExpr::Column(ColumnRef::ByName { column_name }) => input
            .iter()
            .find(|col| col.name.as_ref() == column_name.as_str())
            .map(|col| col.data_type.clone()),
        ScalarExpr::CallUnary { func, .. } => match func {
            UnaryFunc::Not | UnaryFunc::IsNull | UnaryFunc::IsTrue | UnaryFunc::IsFalse => {
                Some(ConcreteDatatype::Bool(BooleanType))
            }
            UnaryFunc::Neg => None,
            UnaryFunc::Cast(to) => Some(to.clone()),
        },
        ScalarExpr::CallBinary { func, .. } => match func {
            BinaryFunc::Eq
            | BinaryFunc::NotEq
            | BinaryFunc::Lt
            | BinaryFunc::Lte
            | BinaryFunc::Gt
            | BinaryFunc::Gte
            | BinaryFunc::And
            | BinaryFunc::Or => Some(ConcreteDatatype::Bool(BooleanType)),
            BinaryFunc::Add
            | BinaryFunc::Sub
            | BinaryFunc::Mul
            | BinaryFunc::Div
            | BinaryFunc::Mod => None,
        },
        ScalarExpr::Case { .. } => None,
        ScalarExpr::Wildcard { .. }
        | ScalarExpr::FieldAccess { .. }
        | ScalarExpr::ListIndex { .. }
        | ScalarExpr::CallFunc { .. } => None,
    }
}
