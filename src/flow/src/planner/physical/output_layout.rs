use crate::expr::func::{BinaryFunc, UnaryFunc};
use crate::expr::scalar::ColumnRef;
use crate::expr::ScalarExpr;
use crate::model::Tuple;
use crate::planner::physical::PhysicalPlan;
use datatypes::{BooleanType, ConcreteDatatype, Value};
use std::sync::Arc;

static NULL_VALUE: Value = Value::Null;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputValueRef {
    Message {
        message_index: usize,
        value_index: usize,
    },
    Affiliate {
        affiliate_index: usize,
    },
    Null,
}

impl OutputValueRef {
    pub fn resolve<'a>(&self, tuple: &'a Tuple) -> Result<&'a Value, String> {
        match self {
            Self::Message {
                message_index,
                value_index,
            } => tuple
                .messages()
                .get(*message_index)
                .and_then(|message| message.value_by_index(*value_index))
                .ok_or_else(|| {
                    debug_assert!(false, "planned message value reference is out of bounds");
                    format!(
                        "output layout message reference [{message_index}][{value_index}] is out of bounds"
                    )
                }),
            Self::Affiliate { affiliate_index } => tuple
                .affiliate()
                .and_then(|affiliate| affiliate.value_by_index(*affiliate_index))
                .ok_or_else(|| {
                    debug_assert!(false, "planned affiliate value reference is out of bounds");
                    format!(
                        "output layout affiliate reference [{affiliate_index}] is out of bounds"
                    )
                }),
            Self::Null => Ok(&NULL_VALUE),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumnLayout {
    pub name: Arc<str>,
    pub data_type: ConcreteDatatype,
    pub value_ref: OutputValueRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputLayout {
    pub columns: Arc<[OutputColumnLayout]>,
}

impl OutputLayout {
    pub fn new(columns: Vec<OutputColumnLayout>) -> Self {
        Self {
            columns: Arc::from(columns),
        }
    }

    pub fn materialized(&self) -> Self {
        let columns = self
            .columns
            .iter()
            .enumerate()
            .map(|(value_index, column)| OutputColumnLayout {
                name: Arc::clone(&column.name),
                data_type: column.data_type.clone(),
                value_ref: OutputValueRef::Message {
                    message_index: 0,
                    value_index,
                },
            })
            .collect();
        Self::new(columns)
    }
}

#[derive(Clone)]
struct MessageColumnLayout {
    name: Arc<str>,
    data_type: ConcreteDatatype,
    logical_index: usize,
}

#[derive(Clone)]
struct MessageLayout {
    source_name: Arc<str>,
    columns: Vec<MessageColumnLayout>,
}

#[derive(Clone)]
struct PlanLayout {
    output: OutputLayout,
    messages: Vec<MessageLayout>,
    affiliate_len: usize,
}

impl PhysicalPlan {
    pub fn output_layout(&self) -> Result<OutputLayout, String> {
        Ok(self.derive_output_layout()?.output)
    }

    fn derive_output_layout(&self) -> Result<PlanLayout, String> {
        match self {
            PhysicalPlan::DataSource(plan) => layout_from_schema(
                plan.schema.as_ref(),
                plan.source_name(),
                plan.decode_projection()
                    .and_then(|projection| projection.output_slots()),
            ),
            PhysicalPlan::TableScan(plan) => {
                layout_from_schema(plan.schema().as_ref(), plan.table_name(), None)
            }
            PhysicalPlan::Decoder(plan) => layout_from_schema(
                plan.schema().as_ref(),
                plan.source_name(),
                plan.decode_projection()
                    .and_then(|projection| projection.output_slots()),
            ),
            PhysicalPlan::SharedStream(plan) => {
                layout_from_schema(plan.schema().as_ref(), plan.stream_name(), None)
            }
            PhysicalPlan::CollectionLayoutNormalize(plan) => {
                layout_from_schema(plan.schema.as_ref(), plan.output_source_name(), None)
            }
            PhysicalPlan::MemoryCollectionMaterialize(plan) => {
                Ok(plan_layout_from_materialized_output(&plan.output_layout))
            }

            PhysicalPlan::Filter(_)
            | PhysicalPlan::Order(_)
            | PhysicalPlan::EmptySuppress(_)
            | PhysicalPlan::Batch(_)
            | PhysicalPlan::SinkCompress(_)
            | PhysicalPlan::SinkEncrypt(_)
            | PhysicalPlan::ResultCollect(_)
            | PhysicalPlan::TumblingWindow(_)
            | PhysicalPlan::CountWindow(_)
            | PhysicalPlan::SlidingWindow(_)
            | PhysicalPlan::StateWindow(_)
            | PhysicalPlan::EosWindow(_)
            | PhysicalPlan::ProcessTimeWatermark(_)
            | PhysicalPlan::EventtimeWatermark(_)
            | PhysicalPlan::Watermark(_)
            | PhysicalPlan::Sampler(_)
            | PhysicalPlan::SourceChangeGate(_)
            | PhysicalPlan::DataSink(_)
            | PhysicalPlan::SinkConnector(_) => passthrough_single_child(self),

            PhysicalPlan::Barrier(_) => passthrough_fan_in(self),

            PhysicalPlan::Compute(plan) => {
                let mut layout = passthrough_single_child(self)?;
                for field in &plan.fields {
                    let name: Arc<str> = Arc::from(field.field_name.as_str());
                    let data_type = infer_scalar_type(&field.compiled_expr, &layout)
                        .unwrap_or(ConcreteDatatype::Null);
                    let affiliate_index = layout.affiliate_len;
                    layout.affiliate_len += 1;
                    layout.output.columns = append_column(
                        &layout.output.columns,
                        OutputColumnLayout {
                            name,
                            data_type,
                            value_ref: OutputValueRef::Affiliate { affiliate_index },
                        },
                    );
                }
                Ok(layout)
            }

            PhysicalPlan::StatefulFunction(plan) => {
                let mut layout = passthrough_single_child(self)?;
                for call in &plan.calls {
                    let affiliate_index = layout.affiliate_len;
                    layout.affiliate_len += 1;
                    layout.output.columns = append_column(
                        &layout.output.columns,
                        OutputColumnLayout {
                            name: Arc::from(call.output_column.as_str()),
                            data_type: ConcreteDatatype::Null,
                            value_ref: OutputValueRef::Affiliate { affiliate_index },
                        },
                    );
                }
                Ok(layout)
            }

            PhysicalPlan::Aggregation(plan) => {
                let child = passthrough_single_child(self)?;
                let mut columns = child
                    .output
                    .columns
                    .iter()
                    .filter(|column| matches!(column.value_ref, OutputValueRef::Message { .. }))
                    .cloned()
                    .collect::<Vec<_>>();
                let mut affiliate_len = 0;
                for call in &plan.aggregate_calls {
                    columns.push(OutputColumnLayout {
                        name: Arc::from(call.output_column.as_str()),
                        data_type: ConcreteDatatype::Null,
                        value_ref: OutputValueRef::Affiliate {
                            affiliate_index: affiliate_len,
                        },
                    });
                    affiliate_len += 1;
                }
                for expr in &plan.group_by_exprs {
                    if matches!(expr, sqlparser::ast::Expr::Identifier(_)) {
                        continue;
                    }
                    columns.push(OutputColumnLayout {
                        name: Arc::from(expr.to_string()),
                        data_type: ConcreteDatatype::Null,
                        value_ref: OutputValueRef::Affiliate {
                            affiliate_index: affiliate_len,
                        },
                    });
                    affiliate_len += 1;
                }
                Ok(PlanLayout {
                    output: OutputLayout::new(columns),
                    messages: child.messages,
                    affiliate_len,
                })
            }

            PhysicalPlan::SinkEncoder(plan) => {
                if let Some(output_layout) = plan.output_layout.as_ref() {
                    return Ok(plan_layout_from_materialized_output(output_layout.as_ref()));
                }
                passthrough_single_child(self)
            }
            PhysicalPlan::IncSinkEncoder(plan) => {
                if let Some(output_layout) = plan.output_layout.as_ref() {
                    return Ok(plan_layout_from_materialized_output(output_layout.as_ref()));
                }
                passthrough_single_child(self)
            }

            PhysicalPlan::StreamingAggregation(_) => passthrough_single_child(self),
            PhysicalPlan::RowDiff(plan) => Ok(plan_layout_from_materialized_output(
                plan.output_layout.as_ref(),
            )),
            PhysicalPlan::ColumnFilter(plan) => {
                let input = passthrough_single_child(self)?;
                apply_column_filter_layout(
                    input,
                    plan.include_columns.as_deref(),
                    plan.exclude_columns.as_deref(),
                )
            }
            PhysicalPlan::Project(plan) => derive_project_layout(plan, self),
        }
    }
}

fn plan_layout_from_materialized_output(output_layout: &OutputLayout) -> PlanLayout {
    let output = output_layout.materialized();
    let message_columns = output
        .columns
        .iter()
        .enumerate()
        .map(|(logical_index, column)| MessageColumnLayout {
            name: Arc::clone(&column.name),
            data_type: column.data_type.clone(),
            logical_index,
        })
        .collect();
    PlanLayout {
        output,
        messages: vec![MessageLayout {
            source_name: Arc::from(""),
            columns: message_columns,
        }],
        affiliate_len: 0,
    }
}

fn apply_column_filter_layout(
    mut input: PlanLayout,
    include_columns: Option<&[String]>,
    exclude_columns: Option<&[String]>,
) -> Result<PlanLayout, String> {
    let columns = match (include_columns, exclude_columns) {
        (Some(include), None) => include
            .iter()
            .map(|name| {
                input
                    .output
                    .columns
                    .iter()
                    .find(|column| column.name.as_ref() == name)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "column filter include_columns: column `{name}` not found in output layout [{}]",
                            format_column_names(&input.output)
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        (None, Some(exclude)) => {
            for name in exclude {
                if !input
                    .output
                    .columns
                    .iter()
                    .any(|column| column.name.as_ref() == name)
                {
                    return Err(format!(
                        "column filter exclude_columns: column `{name}` not found in output layout [{}]",
                        format_column_names(&input.output)
                    ));
                }
            }
            let excluded = exclude
                .iter()
                .map(String::as_str)
                .collect::<std::collections::HashSet<_>>();
            input
                .output
                .columns
                .iter()
                .filter(|column| !excluded.contains(column.name.as_ref()))
                .cloned()
                .collect::<Vec<_>>()
        }
        (None, None) => input.output.columns.to_vec(),
        (Some(_), Some(_)) => {
            return Err("include_columns and exclude_columns are mutually exclusive".to_string());
        }
    };
    if columns.is_empty() {
        return Err("column filter must retain at least one output column".to_string());
    }
    input.output = OutputLayout::new(columns);
    Ok(input)
}

fn format_column_names(layout: &OutputLayout) -> String {
    let names = layout
        .columns
        .iter()
        .map(|column| column.name.as_ref())
        .collect::<Vec<_>>();
    if names.is_empty() {
        "<empty>".to_string()
    } else {
        names.join(", ")
    }
}

fn layout_from_schema(
    schema: &datatypes::Schema,
    source_name: &str,
    output_slots: Option<&Arc<[Arc<str>]>>,
) -> Result<PlanLayout, String> {
    let schema_columns = schema.column_schemas();
    let selected = match output_slots {
        Some(slots) => slots
            .iter()
            .map(|slot| {
                schema_columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == slot.as_ref())
                    .ok_or_else(|| {
                        format!(
                            "runtime output slot `{}` is not present in source schema",
                            slot.as_ref()
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        None => schema_columns.iter().enumerate().collect(),
    };

    let message_columns = selected
        .iter()
        .map(|(logical_index, column)| MessageColumnLayout {
            name: Arc::from(column.name.as_str()),
            data_type: column.data_type.clone(),
            logical_index: *logical_index,
        })
        .collect::<Vec<_>>();
    let columns = message_columns
        .iter()
        .enumerate()
        .map(|(value_index, column)| OutputColumnLayout {
            name: Arc::clone(&column.name),
            data_type: column.data_type.clone(),
            value_ref: OutputValueRef::Message {
                message_index: 0,
                value_index,
            },
        })
        .collect();

    Ok(PlanLayout {
        output: OutputLayout::new(columns),
        messages: vec![MessageLayout {
            source_name: Arc::from(source_name),
            columns: message_columns,
        }],
        affiliate_len: 0,
    })
}

fn derive_project_layout(
    plan: &crate::planner::physical::PhysicalProject,
    node: &PhysicalPlan,
) -> Result<PlanLayout, String> {
    let input = passthrough_single_child(node)?;
    let mut output = Vec::new();
    let mut affiliate_len = 0;

    for field in plan.fields.iter() {
        match &field.compiled_expr {
            ScalarExpr::Wildcard { source_name } => {
                for column in input.output.columns.iter() {
                    let OutputValueRef::Message { message_index, .. } = column.value_ref else {
                        continue;
                    };
                    if let Some(source_name) = source_name {
                        if input
                            .messages
                            .get(message_index)
                            .is_none_or(|message| message.source_name.as_ref() != source_name)
                        {
                            continue;
                        }
                    }
                    output.push(column.clone());
                }
            }
            ScalarExpr::Column(ColumnRef::ByIndex {
                source_name,
                column_index,
            }) => {
                let (value_ref, data_type) =
                    resolve_message_column(&input.messages, source_name.as_str(), *column_index)?;
                output.push(OutputColumnLayout {
                    name: Arc::clone(&field.field_name),
                    data_type,
                    value_ref,
                });
            }
            _ => {
                let data_type = infer_scalar_type(&field.compiled_expr, &input)
                    .unwrap_or(ConcreteDatatype::Null);
                output.push(OutputColumnLayout {
                    name: Arc::clone(&field.field_name),
                    data_type,
                    value_ref: OutputValueRef::Affiliate {
                        affiliate_index: affiliate_len,
                    },
                });
                affiliate_len += 1;
            }
        }
    }

    Ok(PlanLayout {
        output: OutputLayout::new(output),
        messages: input.messages,
        affiliate_len,
    })
}

fn resolve_message_column(
    messages: &[MessageLayout],
    source_name: &str,
    logical_index: usize,
) -> Result<(OutputValueRef, ConcreteDatatype), String> {
    for (message_index, message) in messages.iter().enumerate() {
        if !source_name.is_empty() && message.source_name.as_ref() != source_name {
            continue;
        }
        if let Some((value_index, column)) = message
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.logical_index == logical_index)
        {
            return Ok((
                OutputValueRef::Message {
                    message_index,
                    value_index,
                },
                column.data_type.clone(),
            ));
        }
    }
    Err(format!(
        "cannot resolve source `{source_name}` logical column index {logical_index} to a fixed runtime slot"
    ))
}

fn passthrough_single_child(plan: &PhysicalPlan) -> Result<PlanLayout, String> {
    let child = plan
        .children()
        .first()
        .ok_or_else(|| format!("{} expects 1 child", plan.get_plan_type()))?;
    if plan.children().len() != 1 {
        return Err(format!("{} expects 1 child", plan.get_plan_type()));
    }
    child.derive_output_layout()
}

fn passthrough_fan_in(plan: &PhysicalPlan) -> Result<PlanLayout, String> {
    let mut iter = plan.children().iter();
    let first = iter
        .next()
        .ok_or_else(|| format!("{} expects at least 1 child", plan.get_plan_type()))?
        .derive_output_layout()?;
    for child in iter {
        let layout = child.derive_output_layout()?;
        if layout.output != first.output {
            return Err(format!(
                "{} fan-in children output layouts mismatch",
                plan.get_plan_type()
            ));
        }
    }
    Ok(first)
}

fn append_column(
    columns: &[OutputColumnLayout],
    column: OutputColumnLayout,
) -> Arc<[OutputColumnLayout]> {
    let mut columns = columns.to_vec();
    columns.push(column);
    columns.into()
}

fn infer_scalar_type(expr: &ScalarExpr, input: &PlanLayout) -> Option<ConcreteDatatype> {
    match expr {
        ScalarExpr::Literal(_, typ) => Some(typ.clone()),
        ScalarExpr::Column(ColumnRef::ByIndex {
            source_name,
            column_index,
        }) => resolve_message_column(&input.messages, source_name, *column_index)
            .ok()
            .map(|(_, data_type)| data_type),
        ScalarExpr::Column(ColumnRef::ByName { column_name }) => input
            .output
            .columns
            .iter()
            .find(|column| column.name.as_ref() == column_name.as_str())
            .map(|column| column.data_type.clone()),
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
        ScalarExpr::Case { .. }
        | ScalarExpr::Wildcard { .. }
        | ScalarExpr::FieldAccess { .. }
        | ScalarExpr::ListIndex { .. }
        | ScalarExpr::CallFunc { .. }
        | ScalarExpr::PipelineState { .. }
        | ScalarExpr::ProcessorState { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::func::BinaryFunc;
    use crate::planner::physical::{
        PhysicalDataSource, PhysicalProject, PhysicalProjectField, PhysicalRowDiff,
        PhysicalSinkEncoder,
    };
    use crate::planner::sink::{CommonSinkProps, SinkEncoderConfig, SinkOutputConfig};
    use datatypes::{ColumnSchema, Int64Type, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "stream".to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream".to_string(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "stream".to_string(),
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]))
    }

    #[test]
    fn decoder_slots_define_runtime_value_indexes() {
        let slots: Arc<[Arc<str>]> = Arc::from(vec![Arc::from("c"), Arc::from("a")]);
        let layout = layout_from_schema(test_schema().as_ref(), "stream", Some(&slots))
            .expect("derive slot layout")
            .output;

        assert_eq!(
            layout
                .columns
                .iter()
                .map(|column| (column.name.as_ref(), column.value_ref.clone()))
                .collect::<Vec<_>>(),
            vec![
                (
                    "c",
                    OutputValueRef::Message {
                        message_index: 0,
                        value_index: 0,
                    },
                ),
                (
                    "a",
                    OutputValueRef::Message {
                        message_index: 0,
                        value_index: 1,
                    },
                ),
            ]
        );
    }

    #[test]
    fn project_inherits_direct_refs_and_assigns_expression_affiliate_refs() {
        let source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "stream".to_string(),
            test_schema(),
            None,
            0,
        )));
        let project = PhysicalPlan::Project(PhysicalProject::new(
            vec![
                PhysicalProjectField::new(
                    "renamed_b",
                    sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("b")),
                    ScalarExpr::Column(ColumnRef::ByIndex {
                        source_name: "stream".to_string(),
                        column_index: 1,
                    }),
                ),
                PhysicalProjectField::new(
                    "computed",
                    sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("computed")),
                    ScalarExpr::CallBinary {
                        func: BinaryFunc::Add,
                        expr1: Box::new(ScalarExpr::Column(ColumnRef::ByIndex {
                            source_name: "stream".to_string(),
                            column_index: 0,
                        })),
                        expr2: Box::new(ScalarExpr::Literal(
                            Value::Int64(1),
                            ConcreteDatatype::Int64(Int64Type),
                        )),
                    },
                ),
            ],
            vec![source],
            1,
        ));

        let layout = project.output_layout().expect("derive project layout");
        assert_eq!(
            layout.columns[0].value_ref,
            OutputValueRef::Message {
                message_index: 0,
                value_index: 1,
            }
        );
        assert_eq!(
            layout.columns[1].value_ref,
            OutputValueRef::Affiliate { affiliate_index: 0 }
        );
    }

    #[test]
    fn column_filter_reorders_visible_columns_without_readdressing_tuple_values() {
        let int64 = ConcreteDatatype::Int64(Int64Type);
        let input = PlanLayout {
            output: OutputLayout::new(vec![
                OutputColumnLayout {
                    name: Arc::from("a"),
                    data_type: int64.clone(),
                    value_ref: OutputValueRef::Message {
                        message_index: 1,
                        value_index: 2,
                    },
                },
                OutputColumnLayout {
                    name: Arc::from("computed"),
                    data_type: int64,
                    value_ref: OutputValueRef::Affiliate { affiliate_index: 3 },
                },
            ]),
            messages: Vec::new(),
            affiliate_len: 4,
        };

        let filtered = apply_column_filter_layout(
            input,
            Some(&["computed".to_string(), "a".to_string()]),
            None,
        )
        .expect("filter output layout");

        assert_eq!(filtered.output.columns[0].name.as_ref(), "computed");
        assert_eq!(
            filtered.output.columns[0].value_ref,
            OutputValueRef::Affiliate { affiliate_index: 3 }
        );
        assert_eq!(filtered.output.columns[1].name.as_ref(), "a");
        assert_eq!(
            filtered.output.columns[1].value_ref,
            OutputValueRef::Message {
                message_index: 1,
                value_index: 2,
            }
        );
    }

    #[test]
    fn row_diff_uses_captured_materialized_layout_after_filter_removal() {
        let source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "stream".to_string(),
            test_schema(),
            None,
            0,
        )));
        let captured = OutputLayout::new(vec![OutputColumnLayout {
            name: Arc::from("b"),
            data_type: ConcreteDatatype::Int64(Int64Type),
            value_ref: OutputValueRef::Message {
                message_index: 0,
                value_index: 1,
            },
        }]);
        let row_diff = PhysicalPlan::RowDiff(PhysicalRowDiff::new(
            vec![source],
            1,
            "sink".to_string(),
            SinkOutputConfig::delta(),
            captured,
            vec![Arc::from("b")],
            vec![0],
        ));

        let layout = row_diff.output_layout().expect("derive row diff layout");

        assert_eq!(layout.columns.len(), 1);
        assert_eq!(layout.columns[0].name.as_ref(), "b");
        assert_eq!(
            layout.columns[0].value_ref,
            OutputValueRef::Message {
                message_index: 0,
                value_index: 0,
            }
        );
    }

    #[test]
    fn sink_encoder_prefers_attached_layout_after_filter_removal() {
        let source = Arc::new(PhysicalPlan::DataSource(PhysicalDataSource::new(
            "stream".to_string(),
            test_schema(),
            None,
            0,
        )));
        let captured = OutputLayout::new(vec![OutputColumnLayout {
            name: Arc::from("b"),
            data_type: ConcreteDatatype::Int64(Int64Type),
            value_ref: OutputValueRef::Message {
                message_index: 0,
                value_index: 1,
            },
        }]);
        let mut encoder = PhysicalSinkEncoder::new(
            vec![source],
            1,
            "sink".to_string(),
            SinkEncoderConfig::json(),
            CommonSinkProps::default(),
        );
        encoder.output_layout = Some(Arc::new(captured));
        let encoder = PhysicalPlan::SinkEncoder(encoder);

        let layout = encoder.output_layout().expect("derive encoder layout");

        assert_eq!(layout.columns.len(), 1);
        assert_eq!(layout.columns[0].name.as_ref(), "b");
        assert_eq!(
            layout.columns[0].value_ref,
            OutputValueRef::Message {
                message_index: 0,
                value_index: 0,
            }
        );
    }

    #[test]
    fn null_ref_resolves_to_null() {
        assert_eq!(
            OutputValueRef::Null
                .resolve(&Tuple::new(Vec::new()))
                .expect("resolve planned null"),
            &Value::Null
        );
    }
}
