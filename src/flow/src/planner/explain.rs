use super::{logical::LogicalPlan, physical::PhysicalPlan};
use crate::planner::decode_projection::{DecodeProjection, ListIndexSelection, ProjectionNode};
use crate::planner::logical::{DataSinkPlan, LogicalWindowSpec};
use crate::planner::physical::{WatermarkConfig, WatermarkStrategy};
use datatypes::{ConcreteDatatype, ListType, Schema, StructField, StructType};
use parser::StatefulCallSpec;
use serde::Serialize;
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRow {
    pub id: String,
    pub info: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PipelineExplainOptions {
    pub eventtime_enabled: bool,
    pub eventtime_late_tolerance_ms: u128,
}

#[derive(Debug, Clone, Default)]
pub struct PipelineExplainConfig {
    pub pipeline_options: Option<PipelineExplainOptions>,
    pub shared_stream_decode_applied: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ExplainReport {
    pub root: ExplainNode,
}

impl ExplainReport {
    pub fn rows(&self) -> Vec<ExplainRow> {
        self.root.collect_rows()
    }

    /// Build a report from a logical plan only (no physical needed).
    pub fn from_logical(plan: Arc<LogicalPlan>) -> Self {
        ExplainReport {
            root: build_logical_node(&plan),
        }
    }

    /// Build a report from a physical plan only (no logical needed).
    pub fn from_physical(plan: Arc<PhysicalPlan>) -> Self {
        ExplainReport {
            root: build_physical_node(&plan),
        }
    }

    pub fn topology_string(&self) -> String {
        self.root.topology_string()
    }

    pub fn table_string(&self) -> String {
        let mut rows = self.rows();
        rows.insert(
            0,
            ExplainRow {
                id: "id".to_string(),
                info: "info".to_string(),
            },
        );

        // Truncate the info field if it's too long
        for row in &mut rows {
            if row.info.len() > 2048 {
                row.info.truncate(2048);
                row.info.push_str("...");
            }
        }

        let id_width = rows.iter().map(|r| r.id.len()).max().unwrap_or(2);
        let info_width = rows.iter().map(|r| r.info.len()).max().unwrap_or(4);

        rows.into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let sep = if idx == 0 { "-" } else { " " };
                format!(
                    "{} {:<id_width$} | {:<info_width$}",
                    sep,
                    row.id,
                    row.info,
                    id_width = id_width,
                    info_width = info_width
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.root).unwrap_or(serde_json::Value::Null)
    }
}

#[derive(Debug, Clone)]
pub struct PipelineExplain {
    pub options: Option<PipelineExplainOptions>,
    pub logical: ExplainReport,
    pub physical: ExplainReport,
}

impl PipelineExplain {
    pub fn new(
        logical_plan: Arc<LogicalPlan>,
        physical_plan: Arc<PhysicalPlan>,
        config: PipelineExplainConfig,
    ) -> Self {
        let logical = ExplainReport {
            root: build_logical_node(&logical_plan),
        };

        let physical_root = if config.shared_stream_decode_applied.is_empty() {
            build_physical_node(&physical_plan)
        } else {
            build_physical_node_with_shared_stream_decode_applied(
                &physical_plan,
                Some(&config.shared_stream_decode_applied),
            )
        };
        let physical = ExplainReport {
            root: physical_root,
        };

        Self {
            options: config.pipeline_options,
            logical,
            physical,
        }
    }

    pub fn to_pretty_string(&self) -> String {
        format!(
            "Logical Plan Explain:\n{}\n\nPhysical Plan Explain:\n{}",
            self.logical.table_string(),
            self.physical.table_string()
        )
    }

    /// Structured JSON view containing both logical and physical explains.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "options": self.options,
            "logical": self.logical.to_json(),
            "physical": self.physical.to_json(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainNode {
    pub id: String,
    pub operator: String,
    pub info: Vec<String>,
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    fn topology_string(&self) -> String {
        let mut lines = Vec::new();
        self.collect_topology(0, &mut lines);
        lines.join("\n")
    }

    fn collect_topology(&self, indent: usize, lines: &mut Vec<String>) {
        let spacing = "  ".repeat(indent);
        let info = if self.info.is_empty() {
            "".to_string()
        } else {
            format!(" [{}]", self.info.join(", "))
        };
        lines.push(format!(
            "{}{} ({}){}",
            spacing, self.operator, self.id, info
        ));
        for child in &self.children {
            child.collect_topology(indent + 1, lines);
        }
    }

    fn collect_rows(&self) -> Vec<ExplainRow> {
        let mut rows = Vec::new();
        self.collect_rows_inner(0, &[], true, &mut rows);
        rows
    }

    fn collect_rows_inner(
        &self,
        depth: usize,
        ancestors_last: &[bool],
        is_last: bool,
        rows: &mut Vec<ExplainRow>,
    ) {
        let mut prefix = String::new();
        for ancestor_last in ancestors_last {
            prefix.push_str(if *ancestor_last { "  " } else { "│ " });
        }
        if depth > 0 {
            prefix.push_str(if is_last { "└─" } else { "├─" });
        }

        rows.push(ExplainRow {
            id: format!("{}{}", prefix, self.id),
            info: self.info.join(", "),
        });

        let child_count = self.children.len();
        for (idx, child) in self.children.iter().enumerate() {
            let mut next_ancestors = ancestors_last.to_vec();
            if depth > 0 {
                next_ancestors.push(is_last);
            }
            let child_is_last = idx + 1 == child_count;
            child.collect_rows_inner(depth + 1, &next_ancestors, child_is_last, rows);
        }
    }
}

fn build_logical_node(plan: &Arc<LogicalPlan>) -> ExplainNode {
    let mut info = Vec::new();
    match plan.as_ref() {
        LogicalPlan::DataSource(ds) => {
            info.push(format!("source={}", ds.source_name));
            if let Some(alias) = &ds.alias {
                info.push(format!("alias={}", alias));
            }
            info.push(format!("decoder={}", ds.decoder().kind()));
            if let Some(required) = ds.shared_required_schema() {
                info.push(format!("schema=[{}]", required.join(", ")));
            } else {
                info.push(format_schema_with_decode_projection(
                    ds.schema.as_ref(),
                    ds.decode_projection(),
                ));
            }
            if let Some(sampler) = ds.sampler.as_ref() {
                info.push(format!(
                    "sampler.strategy={}",
                    sampling_strategy_name(&sampler.strategy)
                ));
            }
            if ds.source_input().is_on_change() {
                info.push(format!("input.mode={}", ds.source_input().mode.as_str()));
                let columns = ds
                    .source_input()
                    .on_change_columns()
                    .map(|cols| cols.join(", "))
                    .unwrap_or_else(|| "ALL".to_string());
                info.push(format!("input.columns=[{}]", columns));
            }
        }
        LogicalPlan::StatefulFunction(stateful) => {
            let mappings = stateful
                .calls
                .iter()
                .map(|call| {
                    format!(
                        "{} -> {}",
                        format_stateful_call_spec(&call.spec),
                        call.output_column
                    )
                })
                .collect::<Vec<_>>();
            info.push(format!("calls=[{}]", mappings.join("; ")));
        }
        LogicalPlan::Filter(filter) => {
            info.push(format!("predicate={}", filter.predicate));
        }
        LogicalPlan::Aggregation(agg) => {
            let mut mappings = agg
                .aggregate_mappings
                .iter()
                .map(|(out, expr)| format!("{} -> {}", expr, out))
                .collect::<Vec<_>>();
            mappings.sort();
            info.push(format!("aggregates=[{}]", mappings.join("; ")));
            if !agg.group_by_exprs.is_empty() {
                let group_exprs = agg
                    .group_by_exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>();
                info.push(format!("group_by=[{}]", group_exprs.join(", ")));
            }
        }
        LogicalPlan::Compute(compute) => {
            // Keep compute fields in order; it reflects evaluation order (later fields may depend on earlier ones).
            let temps = compute
                .fields
                .iter()
                .map(|f| format!("{} = {}", f.field_name, f.expr))
                .collect::<Vec<_>>();
            info.push(format!("temps=[{}]", temps.join("; ")));
        }
        LogicalPlan::Order(order) => {
            let keys = order
                .items
                .iter()
                .map(|item| format!("{} {}", item.expr, if item.asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>();
            info.push(format!("keys=[{}]", keys.join("; ")));
        }
        LogicalPlan::Project(project) => {
            let fields = project
                .fields
                .iter()
                .map(|f| format_project_field(&f.expr, &f.field_name))
                .collect::<Vec<_>>();
            info.push(format!("fields=[{}]", fields.join("; ")));
        }
        LogicalPlan::DataSink(DataSinkPlan { sink, .. }) => {
            info.push(format!("sink_id={}", sink.sink_id));
            info.push(format!("connector={}", sink.connector.connector.kind()));
            info.push(format!("encoder={}", sink.connector.encoder.kind_str()));
            if sink.output.is_delta() {
                info.push(format!("output.mode={}", sink.output.mode.as_str()));
                if let Some(columns) = sink.output.delta_columns() {
                    info.push(format!("output.columns=[{}]", columns.join(", ")));
                }
            }
            if sink.output.omit_if_empty() {
                info.push("output.omit_if_empty=true".to_string());
            }
            if let Some(transform_kind) = sink.connector.encoder.transform_kind() {
                info.push(format!("transform={}", transform_kind));
            }
            if sink.common.is_batching_enabled() {
                info.push("batching=true".to_string());
            }
        }
        LogicalPlan::Tail(tail) => {
            info.push(format!("sink_count={}", tail.base.children.len()));
        }
        LogicalPlan::Window(window) => match &window.spec {
            LogicalWindowSpec::Tumbling { time_unit, length } => {
                info.push("kind=tumbling".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("length={}", length));
            }
            LogicalWindowSpec::Count { count } => {
                info.push("kind=count".to_string());
                info.push(format!("count={}", count));
            }
            LogicalWindowSpec::Sliding {
                time_unit,
                lookback,
                lookahead,
            } => {
                info.push("kind=sliding".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("lookback={}", lookback));
                match lookahead {
                    Some(lookahead) => info.push(format!("lookahead={}", lookahead)),
                    None => info.push("lookahead=none".to_string()),
                }
            }
            LogicalWindowSpec::State {
                open,
                emit,
                partition_by,
            } => {
                info.push("kind=state".to_string());
                info.push(format!("open={}", open.as_ref()));
                info.push(format!("emit={}", emit.as_ref()));
                if !partition_by.is_empty() {
                    info.push(format!(
                        "partition_by={}",
                        partition_by
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    ));
                }
            }
        },
    }

    let children = plan.children().iter().map(build_logical_node).collect();

    ExplainNode {
        id: plan.get_plan_name(),
        operator: plan.get_plan_type().to_string(),
        info,
        children,
    }
}

fn format_schema(schema: &Schema) -> String {
    let cols: Vec<String> = schema
        .column_schemas()
        .iter()
        .map(format_column_projection)
        .collect();
    format!("schema=[{}]", cols.join(", "))
}

fn format_schema_with_decode_projection(
    schema: &Schema,
    decode_projection: Option<&DecodeProjection>,
) -> String {
    let Some(decode_projection) = decode_projection else {
        return format_schema(schema);
    };

    let cols: Vec<String> = schema
        .column_schemas()
        .iter()
        .map(|col| format_column_projection_with_decode_projection(col, decode_projection))
        .collect();
    format!("schema=[{}]", cols.join(", "))
}

fn format_column_projection(column: &datatypes::ColumnSchema) -> String {
    match &column.data_type {
        ConcreteDatatype::Struct(struct_type) => format!(
            "{}{{{}}}",
            column.name,
            format_struct_fields_projection(struct_type)
        ),
        ConcreteDatatype::List(list_type) => {
            format!(
                "{}[{}]",
                column.name,
                format_list_item_projection(list_type)
            )
        }
        _ => column.name.clone(),
    }
}

fn format_column_projection_with_decode_projection(
    column: &datatypes::ColumnSchema,
    decode_projection: &DecodeProjection,
) -> String {
    let projection = decode_projection.column(column.name.as_str());
    match &column.data_type {
        ConcreteDatatype::Struct(struct_type) => {
            let projection_fields = match projection {
                Some(ProjectionNode::Struct(fields)) => Some(fields),
                _ => None,
            };
            format!(
                "{}{{{}}}",
                column.name,
                format_struct_fields_projection_with_decode_projection(
                    struct_type,
                    projection_fields,
                )
            )
        }
        ConcreteDatatype::List(list_type) => {
            let list_proj = match projection {
                Some(ProjectionNode::List { indexes, element }) => {
                    Some((indexes, element.as_ref()))
                }
                _ => None,
            };
            format!(
                "{}{}[{}]",
                column.name,
                format_list_index_selection(list_proj.map(|(indexes, _)| indexes)),
                format_list_item_projection_with_decode_projection(
                    list_type,
                    list_proj.map(|(_, element)| element),
                )
            )
        }
        _ => column.name.clone(),
    }
}

fn format_list_item_projection(list_type: &ListType) -> String {
    match list_type.item_type() {
        ConcreteDatatype::Struct(struct_type) => {
            format!("struct{{{}}}", format_struct_fields_projection(struct_type))
        }
        ConcreteDatatype::List(inner) => format!("list[{}]", format_list_item_projection(inner)),
        other => format!("{:?}", other),
    }
}

fn format_list_item_projection_with_decode_projection(
    list_type: &ListType,
    projection: Option<&ProjectionNode>,
) -> String {
    match list_type.item_type() {
        ConcreteDatatype::Struct(struct_type) => {
            let projection_fields = match projection {
                Some(ProjectionNode::Struct(fields)) => Some(fields),
                _ => None,
            };
            format!(
                "struct{{{}}}",
                format_struct_fields_projection_with_decode_projection(
                    struct_type,
                    projection_fields,
                )
            )
        }
        ConcreteDatatype::List(inner) => {
            let list_proj = match projection {
                Some(ProjectionNode::List { indexes, element }) => {
                    Some((indexes, element.as_ref()))
                }
                _ => None,
            };
            format!(
                "list{}[{}]",
                format_list_index_selection(list_proj.map(|(indexes, _)| indexes)),
                format_list_item_projection_with_decode_projection(
                    inner,
                    list_proj.map(|(_, element)| element),
                )
            )
        }
        other => format!("{:?}", other),
    }
}

fn format_list_index_selection(indexes: Option<&ListIndexSelection>) -> String {
    let Some(indexes) = indexes else {
        return String::new();
    };

    match indexes {
        ListIndexSelection::All => "[*]".to_string(),
        ListIndexSelection::Indexes(values) => {
            let joined = values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("[{joined}]")
        }
    }
}

fn format_struct_fields_projection(struct_type: &StructType) -> String {
    struct_type
        .fields()
        .iter()
        .map(format_struct_field_projection)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_struct_fields_projection_with_decode_projection(
    struct_type: &StructType,
    projection_fields: Option<&std::collections::BTreeMap<String, ProjectionNode>>,
) -> String {
    struct_type
        .fields()
        .iter()
        .map(|field| {
            let projection = projection_fields.and_then(|fields| fields.get(field.name()));
            format_struct_field_projection_with_decode_projection(field, projection)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_struct_field_projection(field: &StructField) -> String {
    match field.data_type() {
        ConcreteDatatype::Struct(struct_type) => format!(
            "{}{{{}}}",
            field.name(),
            format_struct_fields_projection(struct_type)
        ),
        ConcreteDatatype::List(list_type) => {
            format!(
                "{}[{}]",
                field.name(),
                format_list_item_projection(list_type)
            )
        }
        _ => field.name().to_string(),
    }
}

fn format_struct_field_projection_with_decode_projection(
    field: &StructField,
    projection: Option<&ProjectionNode>,
) -> String {
    match field.data_type() {
        ConcreteDatatype::Struct(struct_type) => {
            let projection_fields = match projection {
                Some(ProjectionNode::Struct(fields)) => Some(fields),
                _ => None,
            };
            format!(
                "{}{{{}}}",
                field.name(),
                format_struct_fields_projection_with_decode_projection(
                    struct_type,
                    projection_fields,
                )
            )
        }
        ConcreteDatatype::List(list_type) => {
            let list_proj = match projection {
                Some(ProjectionNode::List { indexes, element }) => {
                    Some((indexes, element.as_ref()))
                }
                _ => None,
            };
            format!(
                "{}{}[{}]",
                field.name(),
                format_list_index_selection(list_proj.map(|(indexes, _)| indexes)),
                format_list_item_projection_with_decode_projection(
                    list_type,
                    list_proj.map(|(_, element)| element),
                )
            )
        }
        _ => field.name().to_string(),
    }
}

fn format_project_field(expr: &Expr, field_name: &str) -> String {
    // Keep legacy formatting for internal "mapping" display strings like `stream_struct.b -> c`.
    if field_name.contains("->") {
        return field_name.to_string();
    }

    let expr_str = expr.to_string();
    if expr_str == field_name {
        expr_str
    } else {
        // Always show `expr as <output_name>` when they differ so EXPLAIN reveals placeholder
        // rewrites (e.g., `sum(a)` -> `col_1`) while still keeping user-facing names.
        format!("{expr_str} as {field_name}")
    }
}

fn format_stateful_call_spec(spec: &StatefulCallSpec) -> String {
    let args = spec
        .args
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let mut rendered = format!("{}({})", spec.func_name, args);

    if let Some(when) = &spec.when {
        rendered.push_str(&format!(" FILTER (WHERE {})", when));
    }

    if !spec.partition_by.is_empty() {
        let partition_by = spec
            .partition_by
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        rendered.push_str(&format!(" OVER (PARTITION BY {})", partition_by));
    }

    rendered
}

fn build_physical_node(plan: &Arc<PhysicalPlan>) -> ExplainNode {
    build_physical_node_with_shared_stream_decode_applied(plan, None)
}

fn build_physical_node_with_shared_stream_decode_applied(
    plan: &Arc<PhysicalPlan>,
    shared_stream_decode_applied: Option<&HashMap<String, Vec<String>>>,
) -> ExplainNode {
    build_physical_node_with_prefix(plan, None, None, shared_stream_decode_applied)
}

fn build_physical_node_with_prefix(
    plan: &Arc<PhysicalPlan>,
    id_prefix: Option<&str>,
    scope_info: Option<&str>,
    shared_stream_decode_applied: Option<&HashMap<String, Vec<String>>>,
) -> ExplainNode {
    let mut info = Vec::new();
    if let Some(scope_info) = scope_info {
        info.push(scope_info.to_string());
    }
    match plan.as_ref() {
        PhysicalPlan::DataSource(ds) => {
            info.push(format!("source={}", ds.source_name()));
            if let Some(alias) = ds.alias() {
                info.push(format!("alias={}", alias));
            }
            info.push(format_schema_with_decode_projection(
                ds.schema().as_ref(),
                ds.decode_projection(),
            ));
        }
        PhysicalPlan::Decoder(decoder) => {
            info.push(format!("decoder={}", decoder.decoder().kind()));
            if scope_info == Some("scope=shared_stream") {
                if let Some(shared_stream_decode_applied) = shared_stream_decode_applied {
                    if let Some(applied) = shared_stream_decode_applied.get(decoder.source_name()) {
                        info.push(format!("shared.decode_applied=[{}]", applied.join(", ")));
                    }
                }
            } else {
                info.push(format_schema_with_decode_projection(
                    decoder.schema().as_ref(),
                    decoder.decode_projection(),
                ));
            }
            if let Some(eventtime) = decoder.eventtime() {
                info.push(format!("eventtime.column={}", eventtime.column_name));
                info.push(format!("eventtime.type={}", eventtime.type_key));
                info.push(format!("eventtime.index={}", eventtime.column_index));
            }
        }
        PhysicalPlan::SharedStream(ds) => {
            info.push(format!("source={}", ds.stream_name()));
            if let Some(alias) = ds.alias() {
                info.push(format!("alias={}", alias));
            }
            info.push(format!("schema=[{}]", ds.required_columns().join(", ")));
        }
        PhysicalPlan::SourceChangeGate(gate) => {
            info.push(format!("source={}", gate.source_name));
            info.push(format!("mode={}", gate.input.mode.as_str()));
            info.push(format!(
                "columns=[{}]",
                gate.tracked_columns
                    .iter()
                    .map(|col| col.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        PhysicalPlan::CollectionLayoutNormalize(normalize) => {
            info.push(format!("source={}", normalize.output_source_name()));
            info.push(format_schema(normalize.schema().as_ref()));
        }
        PhysicalPlan::MemoryCollectionMaterialize(_) => {
            // Intentionally keep this node opaque in EXPLAIN (no column layout dumped).
        }
        PhysicalPlan::StatefulFunction(stateful) => {
            let calls = stateful
                .calls
                .iter()
                .map(|call| {
                    format!(
                        "{} -> {}",
                        format_stateful_call_spec(&call.spec),
                        call.output_column
                    )
                })
                .collect::<Vec<_>>();
            info.push(format!("calls=[{}]", calls.join("; ")));
        }
        PhysicalPlan::Filter(filter) => {
            info.push(format!("predicate={}", filter.predicate));
        }
        PhysicalPlan::Compute(compute) => {
            // Keep compute fields in order; it reflects evaluation order (later fields may depend on earlier ones).
            let temps = compute
                .fields
                .iter()
                .map(|f| format!("{} = {}", f.field_name, f.original_expr))
                .collect::<Vec<_>>();
            info.push(format!("temps=[{}]", temps.join("; ")));
        }
        PhysicalPlan::Order(order) => {
            let keys = order
                .keys
                .iter()
                .map(|key| {
                    format!(
                        "{} {}",
                        key.original_expr,
                        if key.asc { "ASC" } else { "DESC" }
                    )
                })
                .collect::<Vec<_>>();
            info.push(format!("keys=[{}]", keys.join("; ")));
        }
        PhysicalPlan::Project(project) => {
            let fields = project
                .fields
                .iter()
                .map(|f| format_project_field(&f.original_expr, f.field_name.as_ref()))
                .collect::<Vec<_>>();
            info.push(format!("fields=[{}]", fields.join("; ")));
            if project.passthrough_messages {
                info.push("passthrough_messages=true".to_string());
            }
        }
        PhysicalPlan::RowDiff(row_diff) => {
            info.push(format!("sink_id={}", row_diff.sink_id));
            info.push(format!("mode={}", row_diff.output.mode.as_str()));
            info.push(format!(
                "columns=[{}]",
                row_diff
                    .tracked_columns
                    .iter()
                    .map(|column| column.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            if let Some(spec) = &row_diff.late_projection {
                if !spec.is_empty() {
                    let cols = spec
                        .columns()
                        .iter()
                        .map(|c| {
                            if c.source_column_display.as_ref() == c.output_name.as_ref() {
                                format!(
                                    "{}.{}",
                                    c.source_name.as_ref(),
                                    c.source_column_display.as_ref()
                                )
                            } else {
                                format!(
                                    "{}.{} as {}",
                                    c.source_name.as_ref(),
                                    c.source_column_display.as_ref(),
                                    c.output_name.as_ref()
                                )
                            }
                        })
                        .collect::<Vec<_>>();
                    info.push(format!("by_index_projection=[{}]", cols.join("; ")));
                }
            }
        }
        PhysicalPlan::EmptySuppress(empty_suppress) => {
            info.push(format!("sink_id={}", empty_suppress.sink_id));
            info.push(format!("omit_if_empty={}", empty_suppress.omit_if_empty));
        }
        PhysicalPlan::Aggregation(aggregation) => {
            info.push(format!(
                "calls=[{}]",
                format_aggregation_calls(&aggregation.aggregate_mappings)
            ));
            if !aggregation.group_by_exprs.is_empty() {
                let group_exprs = aggregation
                    .group_by_exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>();
                info.push(format!("group_by=[{}]", group_exprs.join(", ")));
            }
        }
        PhysicalPlan::StreamingAggregation(aggregation) => {
            info.push(format!(
                "calls=[{}]",
                format_aggregation_calls(&aggregation.aggregate_mappings)
            ));
            if !aggregation.group_by_exprs.is_empty() {
                let group_exprs = aggregation
                    .group_by_exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>();
                info.push(format!("group_by=[{}]", group_exprs.join(", ")));
            }
            match &aggregation.window {
                crate::planner::physical::StreamingWindowSpec::Tumbling { time_unit, length } => {
                    info.push("window=tumbling".to_string());
                    info.push(format!("unit={:?}", time_unit));
                    info.push(format!("length={}", length));
                }
                crate::planner::physical::StreamingWindowSpec::Count { count } => {
                    info.push("window=count".to_string());
                    info.push(format!("count={}", count));
                }
                crate::planner::physical::StreamingWindowSpec::Sliding {
                    time_unit,
                    lookback,
                    lookahead,
                } => {
                    info.push("window=sliding".to_string());
                    info.push(format!("unit={:?}", time_unit));
                    info.push(format!("lookback={}", lookback));
                    match lookahead {
                        Some(lookahead) => info.push(format!("lookahead={}", lookahead)),
                        None => info.push("lookahead=none".to_string()),
                    }
                }
                crate::planner::physical::StreamingWindowSpec::State {
                    open_expr,
                    emit_expr,
                    partition_by_exprs,
                    ..
                } => {
                    info.push("window=state".to_string());
                    info.push(format!("open={}", open_expr));
                    info.push(format!("emit={}", emit_expr));
                    if !partition_by_exprs.is_empty() {
                        info.push(format!(
                            "partition_by={}",
                            partition_by_exprs
                                .iter()
                                .map(|e| e.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        ));
                    }
                }
            }
        }
        PhysicalPlan::Batch(batch) => {
            info.push(format!("sink_id={}", batch.sink_id));
            if let Some(count) = batch.common.batch_count {
                info.push(format!("batch_count={}", count));
            }
            if let Some(duration) = batch.common.batch_duration {
                info.push(format!("batch_duration_ms={}", duration.as_millis()));
            }
        }
        PhysicalPlan::DataSink(sink) | PhysicalPlan::SinkConnector(sink) => {
            info.push(format!("sink_id={}", sink.connector.sink_id));
            info.push(format!("connector={}", sink.connector.connector.kind()));
            if let crate::planner::sink::SinkConnectorConfig::Memory(cfg) =
                &sink.connector.connector
            {
                info.push(format!("topic={}", cfg.topic));
                info.push(format!("kind={}", cfg.kind));
            }
        }
        PhysicalPlan::SinkCompress(compress) => {
            info.push(format!("codec={}", compress.codec.kind_str()));
            if let Some(level) = compress.codec.level_display() {
                info.push(format!("level={}", level));
            }
        }
        PhysicalPlan::SinkEncoder(encoder) => {
            info.push(format!("sink_id={}", encoder.sink_id));
            info.push(format!("encoder={}", encoder.encoder.kind_str()));
            if let Some(transform_kind) = encoder.encoder.transform_kind() {
                info.push(format!("transform={}", transform_kind));
            }
            if encoder.common.is_batching_enabled() {
                if let Some(count) = encoder.common.batch_count {
                    info.push(format!("batch_count={}", count));
                }
                if let Some(duration) = encoder.common.batch_duration {
                    info.push(format!("batch_duration_ms={}", duration.as_millis()));
                }
            }
            if let Some(spec) = &encoder.by_index_projection {
                if !spec.is_empty() {
                    let cols = spec
                        .columns()
                        .iter()
                        .map(|c| {
                            format!(
                                "{}#{}->{}",
                                c.source_name.as_ref(),
                                c.column_index,
                                c.output_name.as_ref()
                            )
                        })
                        .collect::<Vec<_>>();
                    info.push(format!("by_index_projection=[{}]", cols.join("; ")));
                }
            }
        }
        PhysicalPlan::IncSinkEncoder(encoder) => {
            info.push(format!("sink_id={}", encoder.sink_id));
            info.push(format!("encoder={}", encoder.encoder.kind_str()));
            if let Some(transform_kind) = encoder.encoder.transform_kind() {
                info.push(format!("transform={}", transform_kind));
            }
            if encoder.common.is_batching_enabled() {
                if let Some(count) = encoder.common.batch_count {
                    info.push(format!("batch_count={}", count));
                }
                if let Some(duration) = encoder.common.batch_duration {
                    info.push(format!("batch_duration_ms={}", duration.as_millis()));
                }
            }
            if let Some(spec) = &encoder.by_index_projection {
                if !spec.is_empty() {
                    let cols = spec
                        .columns()
                        .iter()
                        .map(|c| {
                            format!(
                                "{}#{}->{}",
                                c.source_name.as_ref(),
                                c.column_index,
                                c.output_name.as_ref()
                            )
                        })
                        .collect::<Vec<_>>();
                    info.push(format!("by_index_projection=[{}]", cols.join("; ")));
                }
            }
        }
        PhysicalPlan::ResultCollect(rc) => {
            let _ = rc;
        }
        PhysicalPlan::Barrier(barrier) => {
            info.push(format!("upstream_count={}", barrier.base.children.len()));
        }
        PhysicalPlan::ProcessTimeWatermark(watermark) => match &watermark.config {
            WatermarkConfig::Tumbling {
                time_unit,
                length,
                strategy,
            } => {
                info.push("window=tumbling".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("length={}", length));
                match strategy {
                    WatermarkStrategy::ProcessingTime { interval, .. } => {
                        info.push("mode=processing_time".to_string());
                        info.push(format!("interval={}", interval));
                    }
                    WatermarkStrategy::EventTime { late_tolerance } => {
                        info.push("mode=event_time".to_string());
                        info.push(format!("late_tolerance_ms={}", late_tolerance.as_millis()));
                    }
                }
            }
            WatermarkConfig::Sliding {
                time_unit,
                lookback,
                lookahead,
                strategy,
            } => {
                info.push("window=sliding".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("lookback={}", lookback));
                match lookahead {
                    Some(lookahead) => info.push(format!("lookahead={}", lookahead)),
                    None => info.push("lookahead=none".to_string()),
                }
                match strategy {
                    WatermarkStrategy::ProcessingTime { interval, .. } => {
                        info.push("mode=processing_time".to_string());
                        info.push(format!("interval={}", interval));
                    }
                    WatermarkStrategy::EventTime { late_tolerance } => {
                        info.push("mode=event_time".to_string());
                        info.push(format!("late_tolerance_ms={}", late_tolerance.as_millis()));
                    }
                }
            }
        },
        PhysicalPlan::EventtimeWatermark(watermark) => match &watermark.config {
            WatermarkConfig::Tumbling {
                time_unit,
                length,
                strategy,
            } => {
                info.push("window=tumbling".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("length={}", length));
                match strategy {
                    WatermarkStrategy::ProcessingTime { interval, .. } => {
                        info.push("mode=processing_time".to_string());
                        info.push(format!("interval={}", interval));
                    }
                    WatermarkStrategy::EventTime { late_tolerance } => {
                        info.push("mode=event_time".to_string());
                        info.push(format!("late_tolerance_ms={}", late_tolerance.as_millis()));
                    }
                }
            }
            WatermarkConfig::Sliding {
                time_unit,
                lookback,
                lookahead,
                strategy,
            } => {
                info.push("window=sliding".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("lookback={}", lookback));
                match lookahead {
                    Some(lookahead) => info.push(format!("lookahead={}", lookahead)),
                    None => info.push("lookahead=none".to_string()),
                }
                match strategy {
                    WatermarkStrategy::ProcessingTime { interval, .. } => {
                        info.push("mode=processing_time".to_string());
                        info.push(format!("interval={}", interval));
                    }
                    WatermarkStrategy::EventTime { late_tolerance } => {
                        info.push("mode=event_time".to_string());
                        info.push(format!("late_tolerance_ms={}", late_tolerance.as_millis()));
                    }
                }
            }
        },
        PhysicalPlan::Watermark(watermark) => match &watermark.config {
            WatermarkConfig::Tumbling {
                time_unit,
                length,
                strategy,
            } => {
                info.push("window=tumbling".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("length={}", length));
                match strategy {
                    WatermarkStrategy::ProcessingTime { interval, .. } => {
                        info.push("mode=processing_time".to_string());
                        info.push(format!("interval={}", interval));
                    }
                    WatermarkStrategy::EventTime { late_tolerance } => {
                        info.push("mode=event_time".to_string());
                        info.push(format!("late_tolerance_ms={}", late_tolerance.as_millis()));
                    }
                }
            }
            WatermarkConfig::Sliding {
                time_unit,
                lookback,
                lookahead,
                strategy,
            } => {
                info.push("window=sliding".to_string());
                info.push(format!("unit={:?}", time_unit));
                info.push(format!("lookback={}", lookback));
                match lookahead {
                    Some(lookahead) => info.push(format!("lookahead={}", lookahead)),
                    None => info.push("lookahead=none".to_string()),
                }
                match strategy {
                    WatermarkStrategy::ProcessingTime { interval, .. } => {
                        info.push("mode=processing_time".to_string());
                        info.push(format!("interval={}", interval));
                    }
                    WatermarkStrategy::EventTime { late_tolerance } => {
                        info.push("mode=event_time".to_string());
                        info.push(format!("late_tolerance_ms={}", late_tolerance.as_millis()));
                    }
                }
            }
        },
        PhysicalPlan::TumblingWindow(window) => {
            info.push("kind=tumbling".to_string());
            info.push(format!("unit={:?}", window.time_unit));
            info.push(format!("length={}", window.length));
        }
        PhysicalPlan::CountWindow(window) => {
            info.push("kind=count".to_string());
            info.push(format!("count={}", window.count));
        }
        PhysicalPlan::SlidingWindow(window) => {
            info.push("kind=sliding".to_string());
            info.push(format!("unit={:?}", window.time_unit));
            info.push(format!("lookback={}", window.lookback));
            match window.lookahead {
                Some(lookahead) => info.push(format!("lookahead={}", lookahead)),
                None => info.push("lookahead=none".to_string()),
            }
        }
        PhysicalPlan::StateWindow(window) => {
            info.push("kind=state".to_string());
            info.push(format!("open={}", window.open_expr));
            info.push(format!("emit={}", window.emit_expr));
            if !window.partition_by_exprs.is_empty() {
                info.push(format!(
                    "partition_by={}",
                    window
                        .partition_by_exprs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
        }
        PhysicalPlan::Sampler(sampler) => {
            info.push(format!("interval={:?}", sampler.interval));
            info.push(format!(
                "strategy={}",
                sampling_strategy_name(&sampler.strategy)
            ));
        }
    }

    let mut children: Vec<ExplainNode> = plan
        .children()
        .iter()
        .map(|child| {
            build_physical_node_with_prefix(
                child,
                id_prefix,
                scope_info,
                shared_stream_decode_applied,
            )
        })
        .collect();

    if let PhysicalPlan::SharedStream(shared) = plan.as_ref() {
        if let Some(ingest_plan) = shared.explain_ingest_plan() {
            let prefix = format!("shared/{}/", shared.stream_name());
            children.push(build_physical_node_with_prefix(
                &ingest_plan,
                Some(prefix.as_str()),
                Some("scope=shared_stream"),
                shared_stream_decode_applied,
            ));
        }
    }

    ExplainNode {
        id: match id_prefix {
            Some(prefix) => format!("{}{}", prefix, plan.get_plan_name()),
            None => plan.get_plan_name(),
        },
        operator: plan.get_plan_type().to_string(),
        info,
        children,
    }
}

fn sampling_strategy_name(strategy: &crate::processor::SamplingStrategy) -> &'static str {
    match strategy {
        crate::processor::SamplingStrategy::Latest => "latest",
        crate::processor::SamplingStrategy::Packer { .. } => "packer",
    }
}

fn format_aggregation_calls(mappings: &std::collections::HashMap<String, Expr>) -> String {
    let mut out = mappings
        .iter()
        .map(|(out, expr)| format!("{} -> {}", expr, out))
        .collect::<Vec<_>>();
    out.sort();
    out.join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_string_simple() {
        let node = ExplainNode {
            id: "root".to_string(),
            operator: "DataSource".to_string(),
            info: vec!["source=test".to_string()],
            children: vec![],
        };
        let report = ExplainReport { root: node };
        let output = report.table_string();
        assert!(output.contains("root"));
        assert!(output.contains("source=test"));
    }

    #[test]
    fn test_table_string_with_children() {
        let node = ExplainNode {
            id: "root".to_string(),
            operator: "Project".to_string(),
            info: vec!["cols=[a, b]".to_string()],
            children: vec![ExplainNode {
                id: "child".to_string(),
                operator: "Filter".to_string(),
                info: vec!["predicate=x > 0".to_string()],
                children: vec![],
            }],
        };
        let report = ExplainReport { root: node };
        let output = report.table_string();
        assert!(output.contains("root"));
        assert!(output.contains("child"));
    }

    /// Regression test: format! panics when width >= 65536 (u16 limit).
    /// This test ensures manual padding works for very long info strings.
    #[test]
    fn test_table_string_info_exceeds_u16_width() {
        // Create info string longer than 65536 characters
        let long_info = "x".repeat(70000);
        let node = ExplainNode {
            id: "test".to_string(),
            operator: "DataSource".to_string(),
            info: vec![long_info.clone()],
            children: vec![],
        };
        let report = ExplainReport { root: node };

        // This would panic with the old format! implementation
        let output = report.table_string();

        assert!(output.contains("test"));
        assert!(output.contains("..."));
        assert!(output.len() < 5000);
    }

    /// Test with many columns (simulates DBC schema with thousands of signals)
    #[test]
    fn test_table_string_many_columns() {
        let schema_info = format!(
            "schema=[{}]",
            (0..5000)
                .map(|i| format!("Signal{}", i))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let node = ExplainNode {
            id: "DataSource".to_string(),
            operator: "DataSource".to_string(),
            info: vec!["source=spiStream".to_string(), schema_info],
            children: vec![],
        };
        let report = ExplainReport { root: node };
        let output = report.table_string();
        assert!(output.contains("DataSource"));
        assert!(output.contains("Signal0"));
        assert!(output.contains("..."));
        assert!(!output.contains("Signal4999"));
    }

    #[test]
    fn test_shared_stream_ingest_decoder_shows_decode_applied_snapshot() {
        use crate::catalog::StreamDecoderConfig;
        use crate::planner::physical::{PhysicalPlan, PhysicalSharedStream};
        use crate::planner::shared_stream_plan::create_physical_plan_for_shared_stream;
        use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
        use std::collections::HashMap;

        let stream_name = "shared_stream";
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                stream_name.to_string(),
                "a".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                stream_name.to_string(),
                "b".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
        ]));

        let ingest_plan = create_physical_plan_for_shared_stream(
            stream_name,
            Arc::clone(&schema),
            StreamDecoderConfig::json(),
            None,
        );

        let plan = Arc::new(PhysicalPlan::SharedStream(PhysicalSharedStream::new(
            stream_name.to_string(),
            None,
            Arc::clone(&schema),
            vec!["a".to_string()],
            StreamDecoderConfig::json(),
            Some(ingest_plan),
            0,
        )));

        let mut snapshot = HashMap::new();
        snapshot.insert(stream_name.to_string(), vec!["a".to_string()]);

        let report = ExplainReport {
            root: build_physical_node_with_shared_stream_decode_applied(&plan, Some(&snapshot)),
        };
        let output = report.table_string();
        assert!(output.contains("shared/shared_stream/PhysicalDecoder"));
        assert!(output.contains("shared.decode_applied=[a]"));

        let decoder_row = report
            .rows()
            .into_iter()
            .find(|row| row.id.contains("shared/shared_stream/PhysicalDecoder"))
            .expect("decoder row should exist");
        assert!(
            !decoder_row.info.contains("schema=["),
            "shared stream ingest decoder should omit schema info"
        );
    }

    #[test]
    fn test_shared_stream_ingest_decoder_wraps_sampler() {
        use crate::catalog::StreamDecoderConfig;
        use crate::planner::create_physical_plan;
        use crate::planner::logical::{DataSource, LogicalPlan};
        use crate::planner::physical::PhysicalPlan;
        use crate::planner::shared_stream_plan::create_physical_plan_for_shared_stream;
        use crate::processor::SamplerConfig;
        use crate::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
        use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
        use std::time::Duration;

        let stream_name = "shared_sampler_stream";
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));

        let plan = create_physical_plan_for_shared_stream(
            stream_name,
            Arc::clone(&schema),
            StreamDecoderConfig::json(),
            Some(SamplerConfig::new(Duration::from_millis(100))),
        );

        let report = ExplainReport::from_physical(plan);
        let rows = report.rows();
        let sampler_pos = rows
            .iter()
            .position(|row| row.id.contains("PhysicalSampler"))
            .expect("shared stream ingest sampler should exist");
        let sampler_row = &rows[sampler_pos];
        let decoder_pos = rows
            .iter()
            .position(|row| row.id.contains("PhysicalDecoder"))
            .expect("shared stream ingest decoder should exist");

        assert!(
            decoder_pos < sampler_pos,
            "decoder should wrap the sampler in the ingest explain tree: {}",
            report.table_string()
        );
        assert!(
            sampler_row.info.contains("interval=100ms")
                && sampler_row.info.contains("strategy=latest"),
            "shared ingest sampler info mismatch: {}",
            sampler_row.info
        );

        let logical_plan = Arc::new(LogicalPlan::DataSource(DataSource::new(
            stream_name.to_string(),
            None,
            StreamDecoderConfig::json(),
            0,
            Arc::clone(&schema),
            None,
            Some(SamplerConfig::new(Duration::from_millis(100))),
        )));
        let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
            source_name: stream_name.to_string(),
            alias: None,
            schema: Arc::clone(&schema),
            kind: SourceBindingKind::Shared,
        }]);
        let physical_plan = create_physical_plan(
            Arc::clone(&logical_plan),
            &bindings,
            &crate::PipelineRegistries::new_with_builtin(),
        )
        .expect("shared physical plan should build");
        let shared_stream = match physical_plan.as_ref() {
            PhysicalPlan::SharedStream(plan) => plan,
            other => panic!(
                "expected shared stream physical plan, got {}",
                other.get_plan_type()
            ),
        };
        let builder_report = ExplainReport::from_physical(
            shared_stream
                .explain_ingest_plan()
                .expect("shared stream explain ingest plan should exist"),
        );
        let builder_rows = builder_report.rows();
        let builder_sampler_pos = builder_rows
            .iter()
            .position(|row| row.id.contains("PhysicalSampler"))
            .expect("shared stream explain ingest sampler should exist");
        let builder_sampler_row = &builder_rows[builder_sampler_pos];
        let builder_decoder_pos = builder_rows
            .iter()
            .position(|row| row.id.contains("PhysicalDecoder"))
            .expect("shared stream explain ingest decoder should exist");

        assert!(
            builder_decoder_pos < builder_sampler_pos,
            "shared stream explain ingest decoder should wrap sampler: {}",
            builder_report.table_string()
        );
        assert!(
            builder_sampler_row.info.contains("interval=100ms")
                && builder_sampler_row.info.contains("strategy=latest"),
            "shared stream explain ingest sampler info mismatch: {}",
            builder_sampler_row.info
        );
    }
}
