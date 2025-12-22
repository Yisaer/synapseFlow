use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::connector::sink::mqtt::MqttSinkConfig;
use crate::planner::logical::LogicalPlan;
use crate::planner::physical::PhysicalPlan;
use crate::planner::sink::{
    CommonSinkProps, CustomSinkConnectorConfig, PipelineSink, PipelineSinkConnector,
    SinkConnectorConfig, SinkEncoderConfig,
};

#[derive(Debug, Error)]
pub enum PlanCacheCodecError {
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("deserialize error: {0}")]
    Deserialize(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommonSinkPropsIR {
    pub batch_count: Option<usize>,
    pub batch_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkIR {
    pub sink_id: String,
    pub forward_to_result: bool,
    pub common: Option<CommonSinkPropsIR>,
    pub connector_kind: String,
    pub connector_settings: JsonValue,
    pub encoder_kind: Option<String>,
    pub encoder_props: Option<JsonMap<String, JsonValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectFieldIR {
    pub field_name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AggregateExprIR {
    pub output_name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeUnitIR {
    Seconds,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WindowIR {
    Tumbling { time_unit: TimeUnitIR, length: u64 },
    Count { count: u64 },
    Sliding {
        time_unit: TimeUnitIR,
        lookback: u64,
        lookahead: Option<u64>,
    },
    State {
        open: Box<Expr>,
        emit: Box<Expr>,
        partition_by: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalPlanIR {
    pub root: i64,
    pub nodes: Vec<LogicalPlanNodeIR>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalPlanNodeIR {
    pub index: i64,
    pub kind: LogicalPlanNodeKindIR,
    pub children: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalPlanNodeKindIR {
    DataSource { stream: String, alias: Option<String> },
    Window { window: WindowIR },
    Aggregation {
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExprIR>,
    },
    Filter { predicate: Expr },
    Project { fields: Vec<ProjectFieldIR> },
    Tail,
    DataSink { sinks: Vec<SinkIR> },
    Opaque { plan_type: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalPlanIR {
    pub root: i64,
    pub nodes: Vec<PhysicalPlanNodeIR>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhysicalPlanNodeIR {
    pub index: i64,
    pub kind: PhysicalPlanNodeKindIR,
    pub children: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PhysicalPlanNodeKindIR {
    DataSource { stream: String, alias: Option<String> },
    Decoder {
        decoder_kind: String,
        decoder_props: JsonMap<String, JsonValue>,
    },
    Filter { predicate: Expr },
    Project { fields: Vec<ProjectFieldIR> },
    Encoder {
        encoder_kind: String,
        encoder_props: JsonMap<String, JsonValue>,
    },
    DataSink {
        sink: SinkIR,
        encoder_plan_index: i64,
    },
    ResultCollect,
    Opaque { plan_type: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanSnapshotBytes {
    pub fingerprint: String,
    pub flow_build_id: String,
    pub logical_plan_ir: Vec<u8>,
}

impl PlanSnapshotBytes {
    pub fn new(
        fingerprint: String,
        flow_build_id: String,
        logical: &LogicalPlanIR,
    ) -> Result<Self, PlanCacheCodecError> {
        Ok(Self {
            fingerprint,
            flow_build_id,
            logical_plan_ir: encode_ir(logical)?,
        })
    }

    pub fn decode_logical(&self) -> Result<LogicalPlanIR, PlanCacheCodecError> {
        decode_ir(&self.logical_plan_ir)
    }
}

fn encode_ir<T: Serialize>(value: &T) -> Result<Vec<u8>, PlanCacheCodecError> {
    bincode::serialize(value).map_err(|err| PlanCacheCodecError::Serialize(err.to_string()))
}

fn decode_ir<T: for<'de> Deserialize<'de>>(raw: &[u8]) -> Result<T, PlanCacheCodecError> {
    bincode::deserialize(raw).map_err(|err| PlanCacheCodecError::Deserialize(err.to_string()))
}

pub fn logical_plan_from_ir(
    ir: &LogicalPlanIR,
    streams: &HashMap<String, (crate::catalog::StreamDecoderConfig, Arc<datatypes::Schema>)>,
) -> Result<Arc<LogicalPlan>, String> {
    let nodes: HashMap<i64, &LogicalPlanNodeIR> = ir.nodes.iter().map(|n| (n.index, n)).collect();
    let mut cache: HashMap<i64, Arc<LogicalPlan>> = HashMap::new();
    build_logical_plan_node(ir.root, &nodes, &mut cache, streams)
}

pub fn sources_from_logical_ir(ir: &LogicalPlanIR) -> Vec<(String, Option<String>)> {
    let mut sources = Vec::new();
    for node in &ir.nodes {
        if let LogicalPlanNodeKindIR::DataSource { stream, alias } = &node.kind {
            sources.push((stream.clone(), alias.clone()));
        }
    }
    sources
}

fn build_logical_plan_node(
    index: i64,
    nodes: &HashMap<i64, &LogicalPlanNodeIR>,
    cache: &mut HashMap<i64, Arc<LogicalPlan>>,
    streams: &HashMap<String, (crate::catalog::StreamDecoderConfig, Arc<datatypes::Schema>)>,
) -> Result<Arc<LogicalPlan>, String> {
    if let Some(found) = cache.get(&index) {
        return Ok(Arc::clone(found));
    }
    let node = nodes
        .get(&index)
        .ok_or_else(|| format!("logical plan IR missing node index {index}"))?;

    let mut children = Vec::with_capacity(node.children.len());
    for child_index in &node.children {
        children.push(build_logical_plan_node(*child_index, nodes, cache, streams)?);
    }

    let plan = match &node.kind {
        LogicalPlanNodeKindIR::DataSource { stream, alias } => {
            let (decoder, schema) = streams
                .get(stream)
                .ok_or_else(|| format!("missing stream definition for {stream}"))?
                .clone();
            let datasource = crate::planner::logical::DataSource::new(
                stream.clone(),
                alias.clone(),
                decoder,
                node.index,
                schema,
            );
            Arc::new(LogicalPlan::DataSource(datasource))
        }
        LogicalPlanNodeKindIR::Window { window } => {
            let spec = window_ir_to_spec(window)?;
            let plan = crate::planner::logical::LogicalWindow::new(spec, children, node.index);
            Arc::new(LogicalPlan::Window(plan))
        }
        LogicalPlanNodeKindIR::Aggregation {
            group_by,
            aggregates,
        } => {
            let aggregate_mappings = aggregates
                .iter()
                .map(|agg| (agg.output_name.clone(), agg.expr.clone()))
                .collect();
            let plan = crate::planner::logical::Aggregation::new(
                aggregate_mappings,
                group_by.clone(),
                children,
                node.index,
            );
            Arc::new(LogicalPlan::Aggregation(plan))
        }
        LogicalPlanNodeKindIR::Filter { predicate } => {
            let plan = crate::planner::logical::Filter::new(
                predicate.clone(),
                children,
                node.index,
            );
            Arc::new(LogicalPlan::Filter(plan))
        }
        LogicalPlanNodeKindIR::Project { fields } => {
            let fields = fields
                .iter()
                .map(|f| crate::planner::logical::project::ProjectField {
                    field_name: f.field_name.clone(),
                    expr: f.expr.clone(),
                })
                .collect();
            let plan = crate::planner::logical::Project::new(fields, children, node.index);
            Arc::new(LogicalPlan::Project(plan))
        }
        LogicalPlanNodeKindIR::Tail => {
            let plan = crate::planner::logical::TailPlan::new(children, node.index);
            Arc::new(LogicalPlan::Tail(plan))
        }
        LogicalPlanNodeKindIR::DataSink { sinks } => {
            let sink = sinks
                .first()
                .ok_or_else(|| "DataSink IR requires at least one sink".to_string())?;
            let sink = sink_ir_to_pipeline_sink(sink)?;
            let child = children
                .into_iter()
                .next()
                .ok_or_else(|| "DataSink IR requires exactly one child".to_string())?;
            let plan = crate::planner::logical::DataSinkPlan::new(child, node.index, sink);
            Arc::new(LogicalPlan::DataSink(plan))
        }
        LogicalPlanNodeKindIR::Opaque { plan_type } => {
            return Err(format!("unsupported logical plan IR node kind: {plan_type}"));
        }
    };

    cache.insert(index, Arc::clone(&plan));
    Ok(plan)
}

fn sink_ir_to_pipeline_sink(sink: &SinkIR) -> Result<PipelineSink, String> {
    let encoder_kind = sink
        .encoder_kind
        .as_deref()
        .ok_or_else(|| "sink IR missing encoder_kind".to_string())?;
    let encoder_props = sink
        .encoder_props
        .as_ref()
        .ok_or_else(|| "sink IR missing encoder_props".to_string())?;
    let encoder = SinkEncoderConfig::new(encoder_kind.to_string(), encoder_props.clone());

    let connector = match sink.connector_kind.as_str() {
        "mqtt" => SinkConnectorConfig::Mqtt(mqtt_sink_from_ir_settings(&sink.connector_settings)?),
        "nop" => SinkConnectorConfig::Nop(crate::planner::sink::NopSinkConfig),
        other => SinkConnectorConfig::Custom(CustomSinkConnectorConfig {
            kind: other.to_string(),
            settings: sink.connector_settings.clone(),
        }),
    };
    let connector = PipelineSinkConnector::new(sink.sink_id.clone(), connector, encoder);

    let mut pipeline_sink = PipelineSink::new(sink.sink_id.clone(), connector)
        .with_forward_to_result(sink.forward_to_result);
    if let Some(common) = &sink.common {
        pipeline_sink = pipeline_sink.with_common_props(common_sink_props_from_ir(common));
    }
    Ok(pipeline_sink)
}

fn mqtt_sink_from_ir_settings(settings: &JsonValue) -> Result<MqttSinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "mqtt sink settings must be an object".to_string())?;

    let sink_name = obj
        .get("sink_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "mqtt sink settings missing sink_name".to_string())?
        .to_string();
    let broker_url = obj
        .get("broker_url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "mqtt sink settings missing broker_url".to_string())?
        .to_string();
    let topic = obj
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "mqtt sink settings missing topic".to_string())?
        .to_string();
    let qos = obj
        .get("qos")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| "mqtt sink settings missing qos".to_string())? as u8;
    let retain = obj.get("retain").and_then(|v| v.as_bool()).unwrap_or(false);
    let client_id = obj.get("client_id").and_then(|v| v.as_str()).map(|s| s.to_string());
    let connector_key = obj
        .get("connector_key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut config = MqttSinkConfig::new(sink_name.clone(), broker_url, topic, qos)
        .with_retain(retain);
    if let Some(client_id) = client_id {
        config = config.with_client_id(client_id);
    }
    if let Some(connector_key) = connector_key {
        config = config.with_connector_key(connector_key);
    }
    Ok(config)
}

fn common_sink_props_from_ir(common: &CommonSinkPropsIR) -> CommonSinkProps {
    CommonSinkProps {
        batch_count: common.batch_count,
        batch_duration: common.batch_duration_ms.map(Duration::from_millis),
    }
}

fn window_ir_to_spec(window: &WindowIR) -> Result<crate::planner::logical::LogicalWindowSpec, String> {
    Ok(match window {
        WindowIR::Tumbling { time_unit, length } => {
            crate::planner::logical::LogicalWindowSpec::Tumbling {
                time_unit: time_unit_ir_to_time_unit(*time_unit),
                length: *length,
            }
        }
        WindowIR::Count { count } => crate::planner::logical::LogicalWindowSpec::Count { count: *count },
        WindowIR::Sliding {
            time_unit,
            lookback,
            lookahead,
        } => crate::planner::logical::LogicalWindowSpec::Sliding {
            time_unit: time_unit_ir_to_time_unit(*time_unit),
            lookback: *lookback,
            lookahead: *lookahead,
        },
        WindowIR::State {
            open,
            emit,
            partition_by,
        } => crate::planner::logical::LogicalWindowSpec::State {
            open: open.clone(),
            emit: emit.clone(),
            partition_by: partition_by.clone(),
        },
    })
}

fn time_unit_ir_to_time_unit(unit: TimeUnitIR) -> crate::planner::logical::TimeUnit {
    match unit {
        TimeUnitIR::Seconds => crate::planner::logical::TimeUnit::Seconds,
    }
}

impl LogicalPlanIR {
    pub fn from_plan(root: &Arc<LogicalPlan>) -> Self {
        let mut nodes = Vec::new();
        let mut visited = HashMap::<i64, ()>::new();
        build_logical_ir(root, &mut nodes, &mut visited);
        Self {
            root: root.get_plan_index(),
            nodes,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, PlanCacheCodecError> {
        encode_ir(self)
    }

    pub fn decode(raw: &[u8]) -> Result<Self, PlanCacheCodecError> {
        decode_ir(raw)
    }
}

impl PhysicalPlanIR {
    pub fn from_plan(root: &Arc<PhysicalPlan>) -> Self {
        let mut nodes = Vec::new();
        let mut visited = HashMap::<i64, ()>::new();
        build_physical_ir(root, &mut nodes, &mut visited);
        Self {
            root: root.get_plan_index(),
            nodes,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, PlanCacheCodecError> {
        encode_ir(self)
    }

    pub fn decode(raw: &[u8]) -> Result<Self, PlanCacheCodecError> {
        decode_ir(raw)
    }
}

fn build_logical_ir(
    node: &Arc<LogicalPlan>,
    out: &mut Vec<LogicalPlanNodeIR>,
    visited: &mut HashMap<i64, ()>,
) {
    let index = node.get_plan_index();
    if visited.insert(index, ()).is_some() {
        return;
    }

    for child in node.children() {
        build_logical_ir(child, out, visited);
    }

    let children = node.children().iter().map(|c| c.get_plan_index()).collect();
    let kind = match node.as_ref() {
        LogicalPlan::DataSource(plan) => LogicalPlanNodeKindIR::DataSource {
            stream: plan.source_name.clone(),
            alias: plan.alias.clone(),
        },
        LogicalPlan::Window(plan) => LogicalPlanNodeKindIR::Window {
            window: window_spec_to_ir(&plan.spec),
        },
        LogicalPlan::Aggregation(plan) => {
            let aggregates = plan
                .aggregate_mappings
                .iter()
                .map(|(name, expr)| AggregateExprIR {
                    output_name: name.clone(),
                    expr: expr.clone(),
                })
                .collect();
            LogicalPlanNodeKindIR::Aggregation {
                group_by: plan.group_by_exprs.clone(),
                aggregates,
            }
        }
        LogicalPlan::Filter(plan) => LogicalPlanNodeKindIR::Filter {
            predicate: plan.predicate.clone(),
        },
        LogicalPlan::Project(plan) => LogicalPlanNodeKindIR::Project {
            fields: plan
                .fields
                .iter()
                .map(|f| ProjectFieldIR {
                    field_name: f.field_name.clone(),
                    expr: f.expr.clone(),
                })
                .collect(),
        },
        LogicalPlan::Tail(_) => LogicalPlanNodeKindIR::Tail,
        LogicalPlan::DataSink(plan) => LogicalPlanNodeKindIR::DataSink {
            sinks: vec![sink_to_ir(&plan.sink)],
        },
    };

    out.push(LogicalPlanNodeIR {
        index,
        kind,
        children,
    });
}

fn build_physical_ir(
    node: &Arc<PhysicalPlan>,
    out: &mut Vec<PhysicalPlanNodeIR>,
    visited: &mut HashMap<i64, ()>,
) {
    let index = node.get_plan_index();
    if visited.insert(index, ()).is_some() {
        return;
    }

    for child in node.children() {
        build_physical_ir(child, out, visited);
    }

    let children = node.children().iter().map(|c| c.get_plan_index()).collect();
    let kind = match node.as_ref() {
        PhysicalPlan::DataSource(plan) => PhysicalPlanNodeKindIR::DataSource {
            stream: plan.source_name.clone(),
            alias: plan.alias.clone(),
        },
        PhysicalPlan::Decoder(plan) => PhysicalPlanNodeKindIR::Decoder {
            decoder_kind: plan.decoder().kind().to_string(),
            decoder_props: plan.decoder().props().clone(),
        },
        PhysicalPlan::Filter(plan) => PhysicalPlanNodeKindIR::Filter {
            predicate: plan.predicate.clone(),
        },
        PhysicalPlan::Project(plan) => PhysicalPlanNodeKindIR::Project {
            fields: plan
                .fields
                .iter()
                .map(|f| ProjectFieldIR {
                    field_name: f.field_name.clone(),
                    expr: f.original_expr.clone(),
                })
                .collect(),
        },
        PhysicalPlan::Encoder(plan) => PhysicalPlanNodeKindIR::Encoder {
            encoder_kind: plan.encoder.kind().to_string(),
            encoder_props: plan.encoder.props().clone(),
        },
        PhysicalPlan::DataSink(plan) => PhysicalPlanNodeKindIR::DataSink {
            sink: physical_sink_to_ir(&plan.connector),
            encoder_plan_index: plan.connector.encoder_plan_index,
        },
        PhysicalPlan::ResultCollect(_) => PhysicalPlanNodeKindIR::ResultCollect,
        other => PhysicalPlanNodeKindIR::Opaque {
            plan_type: other.get_plan_type().to_string(),
        },
    };

    out.push(PhysicalPlanNodeIR {
        index,
        kind,
        children,
    });
}

fn sink_to_ir(sink: &PipelineSink) -> SinkIR {
    let (connector_kind, connector_settings) = connector_to_ir(&sink.connector.connector);
    SinkIR {
        sink_id: sink.sink_id.clone(),
        forward_to_result: sink.forward_to_result,
        common: Some(common_sink_props_to_ir(&sink.common)),
        connector_kind,
        connector_settings,
        encoder_kind: Some(sink.connector.encoder.kind().to_string()),
        encoder_props: Some(sink.connector.encoder.props().clone()),
    }
}

fn physical_sink_to_ir(connector: &crate::planner::physical::PhysicalSinkConnector) -> SinkIR {
    let (connector_kind, connector_settings) = connector_to_ir(&connector.connector);
    SinkIR {
        sink_id: connector.sink_id.clone(),
        forward_to_result: connector.forward_to_result,
        common: None,
        connector_kind,
        connector_settings,
        encoder_kind: None,
        encoder_props: None,
    }
}

fn common_sink_props_to_ir(common: &CommonSinkProps) -> CommonSinkPropsIR {
    CommonSinkPropsIR {
        batch_count: common.batch_count,
        batch_duration_ms: common.batch_duration.map(|d| d.as_millis() as u64),
    }
}

fn connector_to_ir(connector: &SinkConnectorConfig) -> (String, JsonValue) {
    match connector {
        SinkConnectorConfig::Mqtt(cfg) => (
            "mqtt".to_string(),
            serde_json::json!({
                "sink_name": cfg.sink_name,
                "broker_url": cfg.broker_url,
                "topic": cfg.topic,
                "qos": cfg.qos,
                "retain": cfg.retain,
                "client_id": cfg.client_id,
                "connector_key": cfg.connector_key,
            }),
        ),
        SinkConnectorConfig::Nop(_) => ("nop".to_string(), JsonValue::Object(JsonMap::new())),
        SinkConnectorConfig::Custom(custom) => (
            custom.kind.clone(),
            custom.settings.clone(),
        ),
    }
}

fn window_spec_to_ir(spec: &crate::planner::logical::LogicalWindowSpec) -> WindowIR {
    match spec {
        crate::planner::logical::LogicalWindowSpec::Tumbling { time_unit, length } => {
            WindowIR::Tumbling {
                time_unit: match time_unit {
                    crate::planner::logical::TimeUnit::Seconds => TimeUnitIR::Seconds,
                },
                length: *length,
            }
        }
        crate::planner::logical::LogicalWindowSpec::Count { count } => WindowIR::Count { count: *count },
        crate::planner::logical::LogicalWindowSpec::Sliding {
            time_unit,
            lookback,
            lookahead,
        } => WindowIR::Sliding {
            time_unit: match time_unit {
                crate::planner::logical::TimeUnit::Seconds => TimeUnitIR::Seconds,
            },
            lookback: *lookback,
            lookahead: *lookahead,
        },
        crate::planner::logical::LogicalWindowSpec::State {
            open,
            emit,
            partition_by,
        } => WindowIR::State {
            open: open.clone(),
            emit: emit.clone(),
            partition_by: partition_by.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_expr() -> Expr {
        Expr::Identifier(sqlparser::ast::Ident::new("col_a"))
    }

    #[test]
    fn snapshot_bytes_roundtrip() {
        let logical = LogicalPlanIR {
            root: 0,
            nodes: vec![LogicalPlanNodeIR {
                index: 0,
                kind: LogicalPlanNodeKindIR::Filter {
                    predicate: sample_expr(),
                },
                children: vec![],
            }],
        };

        let snapshot = PlanSnapshotBytes::new(
            "fp".to_string(),
            "sha:deadbeef tag:v0.0.0".to_string(),
            &logical,
        )
        .unwrap();

        assert_eq!(snapshot.decode_logical().unwrap(), logical);
    }
}
