use super::{logical::LogicalPlan, physical::PhysicalPlan};
use crate::planner::logical::{DataSinkPlan, LogicalWindowSpec};
use crate::planner::physical::{WatermarkConfig, WatermarkStrategy};
use serde::Serialize;
use sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRow {
    pub id: String,
    pub info: String,
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
        let id_width = rows.iter().map(|r| r.id.len()).max().unwrap_or(2);
        let info_width = rows.iter().map(|r| r.info.len()).max().unwrap_or(4);

        rows.into_iter()
            .enumerate()
            .map(|(idx, row)| {
                let sep = if idx == 0 { "-" } else { " " };
                format!(
                    "{} {:<id_w$} | {:<info_w$}",
                    sep,
                    row.id,
                    row.info,
                    id_w = id_width,
                    info_w = info_width
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
    pub logical: ExplainReport,
    pub physical: ExplainReport,
}

impl PipelineExplain {
    pub fn new(logical_plan: Arc<LogicalPlan>, physical_plan: Arc<PhysicalPlan>) -> Self {
        let logical = ExplainReport {
            root: build_logical_node(&logical_plan),
        };
        let physical = ExplainReport {
            root: build_physical_node(&physical_plan),
        };
        Self { logical, physical }
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
            let cols: Vec<String> = ds
                .schema
                .column_schemas()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            info.push(format!("schema=[{}]", cols.join(", ")));
        }
        LogicalPlan::Filter(filter) => {
            info.push(format!("predicate={}", filter.predicate));
        }
        LogicalPlan::Aggregation(agg) => {
            let mappings = agg
                .aggregate_mappings
                .iter()
                .map(|(out, expr)| format!("{} -> {}", expr, out))
                .collect::<Vec<_>>();
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
        LogicalPlan::Project(project) => {
            let fields = project
                .fields
                .iter()
                .map(|f| f.expr.to_string())
                .collect::<Vec<_>>();
            info.push(format!("fields=[{}]", fields.join("; ")));
        }
        LogicalPlan::DataSink(DataSinkPlan { sink, .. }) => {
            info.push(format!("sink_id={}", sink.sink_id));
            info.push(format!("connector={}", sink.connector.connector.kind()));
            info.push(format!("encoder={}", sink.connector.encoder.kind()));
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
            LogicalWindowSpec::State { open, emit } => {
                info.push("kind=state".to_string());
                info.push(format!("open={}", open.as_ref()));
                info.push(format!("emit={}", emit.as_ref()));
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

fn build_physical_node(plan: &Arc<PhysicalPlan>) -> ExplainNode {
    build_physical_node_with_prefix(plan, None, None)
}

fn build_physical_node_with_prefix(
    plan: &Arc<PhysicalPlan>,
    id_prefix: Option<&str>,
    scope_info: Option<&str>,
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
            let cols: Vec<String> = ds
                .schema()
                .column_schemas()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            info.push(format!("schema=[{}]", cols.join(", ")));
        }
        PhysicalPlan::Decoder(decoder) => {
            info.push(format!("decoder={}", decoder.decoder().kind()));
            let cols: Vec<String> = decoder
                .schema()
                .column_schemas()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            info.push(format!("schema=[{}]", cols.join(", ")));
        }
        PhysicalPlan::SharedStream(ds) => {
            info.push(format!("source={}", ds.stream_name()));
            if let Some(alias) = ds.alias() {
                info.push(format!("alias={}", alias));
            }
            let cols: Vec<String> = ds
                .schema()
                .column_schemas()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            info.push(format!("schema=[{}]", cols.join(", ")));
        }
        PhysicalPlan::Filter(filter) => {
            info.push(format!("predicate={}", filter.predicate));
        }
        PhysicalPlan::Project(project) => {
            let fields = project
                .fields
                .iter()
                .map(|f| f.original_expr.to_string())
                .collect::<Vec<_>>();
            info.push(format!("fields=[{}]", fields.join("; ")));
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
            match aggregation.window {
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
        PhysicalPlan::DataSink(sink) => {
            info.push(format!("sink_id={}", sink.connector.sink_id));
            info.push(format!("connector={}", sink.connector.connector.kind()));
        }
        PhysicalPlan::Encoder(encoder) => {
            info.push(format!("sink_id={}", encoder.sink_id));
            info.push(format!("encoder={}", encoder.encoder.kind()));
        }
        PhysicalPlan::StreamingEncoder(streaming) => {
            info.push(format!("sink_id={}", streaming.sink_id));
            info.push(format!("encoder={}", streaming.encoder.kind()));
            if streaming.common.is_batching_enabled() {
                info.push("batching=true".to_string());
            }
        }
        PhysicalPlan::ResultCollect(rc) => {
            info.push(format!("sink_count={}", rc.base.children.len()));
        }
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
                    WatermarkStrategy::External => {
                        info.push("mode=external".to_string());
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
                    WatermarkStrategy::External => {
                        info.push("mode=external".to_string());
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
        }
    }

    let mut children: Vec<ExplainNode> = plan
        .children()
        .iter()
        .map(|child| build_physical_node_with_prefix(child, id_prefix, scope_info))
        .collect();

    if let PhysicalPlan::SharedStream(shared) = plan.as_ref() {
        if let Some(ingest_plan) = shared.explain_ingest_plan() {
            let prefix = format!("shared/{}/", shared.stream_name());
            children.push(build_physical_node_with_prefix(
                &ingest_plan,
                Some(prefix.as_str()),
                Some("scope=shared_stream"),
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

fn format_aggregation_calls(mappings: &std::collections::HashMap<String, Expr>) -> String {
    mappings
        .iter()
        .map(|(out, expr)| format!("{} -> {}", expr, out))
        .collect::<Vec<_>>()
        .join("; ")
}
