//! Logical plan IR codec.
//!
//! This module provides a JSON-serializable intermediate representation (IR) for `LogicalPlan`
//! along with helpers to rebuild a `LogicalPlan` from that IR.

use parser::StatefulCallSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlparser::ast::Expr;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::connector::sink::mqtt::MqttSinkConfig;
use crate::planner::logical::LogicalPlan;
use crate::planner::sink::{
    CommonSinkProps, CustomSinkConnectorConfig, PipelineSink, PipelineSinkConnector,
    SinkConnectorConfig, SinkDeltaOutputConfig, SinkEncoderConfig, SinkEncoderTransformConfig,
    SinkOutputConfig, SinkOutputMode,
};

#[derive(Debug, Error)]
pub enum LogicalPlanIrCodecError {
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
    pub output: Option<SinkOutputIR>,
    pub connector_kind: String,
    pub connector_settings: JsonValue,
    pub encoder_kind: Option<String>,
    pub encoder_props: Option<JsonMap<String, JsonValue>>,
    pub encoder_transform: Option<SinkEncoderTransformIR>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkOutputIR {
    pub mode: SinkOutputModeIR,
    pub delta: Option<SinkDeltaOutputIR>,
    #[serde(default)]
    pub omit_if_empty: bool,
    #[serde(default)]
    pub include_columns: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SinkOutputModeIR {
    Full,
    Delta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkDeltaOutputIR {
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SinkEncoderTransformIR {
    Template { template: String },
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
    Tumbling {
        time_unit: TimeUnitIR,
        length: u64,
    },
    Count {
        count: u64,
    },
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
    DataSource {
        stream: String,
        alias: Option<String>,
    },
    StatefulFunction {
        calls: Vec<StatefulExprIR>,
    },
    Order {
        keys: Vec<OrderKeyIR>,
    },
    Window {
        window: WindowIR,
    },
    Aggregation {
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExprIR>,
    },
    Filter {
        predicate: Expr,
    },
    Compute {
        fields: Vec<ProjectFieldIR>,
    },
    Project {
        fields: Vec<ProjectFieldIR>,
    },
    Tail,
    DataSink {
        sinks: Vec<SinkIR>,
    },
    Opaque {
        plan_type: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatefulExprIR {
    pub output_name: String,
    pub call: StatefulCallSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderKeyIR {
    pub expr: Expr,
    pub asc: bool,
}

fn encode_ir<T: Serialize>(value: &T) -> Result<Vec<u8>, LogicalPlanIrCodecError> {
    serde_json::to_vec(value).map_err(|err| LogicalPlanIrCodecError::Serialize(err.to_string()))
}

fn decode_ir<T: for<'de> Deserialize<'de>>(raw: &[u8]) -> Result<T, LogicalPlanIrCodecError> {
    serde_json::from_slice(raw).map_err(|err| LogicalPlanIrCodecError::Deserialize(err.to_string()))
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
        children.push(build_logical_plan_node(
            *child_index,
            nodes,
            cache,
            streams,
        )?);
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
                None, // eventtime
                None, // sampler
            );
            Arc::new(LogicalPlan::DataSource(datasource))
        }
        LogicalPlanNodeKindIR::StatefulFunction { calls } => {
            let logical_calls = calls
                .iter()
                .map(|call| crate::planner::logical::LogicalStatefulCall {
                    output_column: call.output_name.clone(),
                    spec: call.call.clone(),
                })
                .collect();
            let plan = crate::planner::logical::StatefulFunctionPlan::new(
                logical_calls,
                children,
                node.index,
            );
            Arc::new(LogicalPlan::StatefulFunction(plan))
        }
        LogicalPlanNodeKindIR::Order { keys } => {
            let items = keys
                .iter()
                .map(|key| crate::planner::logical::OrderItem {
                    expr: key.expr.clone(),
                    asc: key.asc,
                })
                .collect();
            let plan = crate::planner::logical::Order::new(items, children, node.index);
            Arc::new(LogicalPlan::Order(plan))
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
            let plan =
                crate::planner::logical::Filter::new(predicate.clone(), children, node.index);
            Arc::new(LogicalPlan::Filter(plan))
        }
        LogicalPlanNodeKindIR::Compute { fields } => {
            let fields = fields
                .iter()
                .map(|f| crate::planner::logical::compute::ComputeField {
                    field_name: f.field_name.clone(),
                    expr: f.expr.clone(),
                })
                .collect();
            let plan = crate::planner::logical::Compute::new(fields, children, node.index);
            Arc::new(LogicalPlan::Compute(plan))
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
            return Err(format!(
                "unsupported logical plan IR node kind: {plan_type}"
            ));
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
    let mut encoder_props = sink.encoder_props.clone().unwrap_or_default();
    let transform = sink
        .encoder_transform
        .as_ref()
        .cloned()
        .or_else(|| extract_legacy_transform_from_props(&mut encoder_props));
    let mut encoder = SinkEncoderConfig::new(encoder_kind.to_string(), encoder_props);
    if let Some(transform) = transform.as_ref() {
        encoder = encoder.with_transform(transform_ir_to_config(transform));
    }

    let connector = match sink.connector_kind.as_str() {
        "mqtt" => SinkConnectorConfig::Mqtt(mqtt_sink_from_ir_settings(&sink.connector_settings)?),
        "kuksa" => {
            SinkConnectorConfig::Kuksa(kuksa_sink_from_ir_settings(&sink.connector_settings)?)
        }
        "kura" => SinkConnectorConfig::Kura(kura_sink_from_ir_settings(&sink.connector_settings)?),
        "nop" => {
            let log = sink
                .connector_settings
                .as_object()
                .and_then(|obj| obj.get("log"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            SinkConnectorConfig::Nop(crate::planner::sink::NopSinkConfig { log })
        }
        "memory" => {
            SinkConnectorConfig::Memory(memory_sink_from_ir_settings(&sink.connector_settings)?)
        }
        "file" => SinkConnectorConfig::File(file_sink_from_ir_settings(&sink.connector_settings)?),
        "nng_pubsub" => SinkConnectorConfig::NngPubSub(nng_pubsub_sink_from_ir_settings(
            &sink.connector_settings,
        )?),
        "video" => {
            SinkConnectorConfig::Video(video_sink_from_ir_settings(&sink.connector_settings)?)
        }
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
    if let Some(output) = &sink.output {
        pipeline_sink = pipeline_sink.with_output(sink_output_from_ir(output));
    }
    Ok(pipeline_sink)
}

fn memory_sink_from_ir_settings(
    settings: &JsonValue,
) -> Result<crate::connector::MemorySinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "memory sink settings must be an object".to_string())?;

    let sink_name = obj
        .get("sink_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "memory sink settings missing sink_name".to_string())?
        .to_string();
    let topic = obj
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "memory sink settings missing topic".to_string())?
        .to_string();
    let kind = obj.get("kind").and_then(|v| v.as_str()).unwrap_or("bytes");
    let kind = match kind {
        "bytes" => crate::connector::MemoryTopicKind::Bytes,
        "collection" => crate::connector::MemoryTopicKind::Collection,
        other => {
            return Err(format!(
                "memory sink settings invalid kind `{other}` (expected bytes|collection)"
            ));
        }
    };

    Ok(crate::connector::MemorySinkConfig::new(
        sink_name, topic, kind,
    ))
}

fn file_sink_from_ir_settings(
    settings: &JsonValue,
) -> Result<crate::connector::sink::file::FileSinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "file sink settings must be an object".to_string())?;
    let sink_name = obj
        .get("sink_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "file sink settings missing sink_name".to_string())?
        .to_string();
    if sink_name.trim().is_empty() {
        return Err("file sink settings missing sink_name".to_string());
    }
    let pipeline_id = obj
        .get("pipeline_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "file sink settings missing pipeline_id".to_string())?
        .to_string();
    if pipeline_id.trim().is_empty() {
        return Err("file sink settings missing pipeline_id".to_string());
    }
    let path = obj
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "file sink settings missing path".to_string())?
        .to_string();
    if path.trim().is_empty() {
        return Err("file sink settings missing path".to_string());
    }
    let filename_prefix = obj
        .get("filename_prefix")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let filename_suffix = obj
        .get("filename_suffix")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    crate::connector::sink::file::validate_file_name_affixes(&filename_prefix, &filename_suffix)?;
    let retention = obj.get("retention").and_then(|v| v.as_object());
    let max_file_count = retention
        .and_then(|retention| retention.get("max_file_count"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let max_file_age_days = retention
        .and_then(|retention| retention.get("max_file_age_days"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    Ok(crate::connector::sink::file::FileSinkConfig {
        sink_name,
        pipeline_id,
        path,
        filename_prefix,
        filename_suffix,
        retention: crate::connector::sink::file::FileRetentionConfig {
            max_file_count,
            max_file_age_days,
        },
    })
}

fn nng_pubsub_sink_from_ir_settings(
    settings: &JsonValue,
) -> Result<crate::connector::NngPubSubSinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "nng_pubsub sink settings must be an object".to_string())?;
    let sink_name = obj
        .get("sink_name")
        .and_then(|v| v.as_str())
        .unwrap_or("nng_pubsub_sink")
        .to_string();
    let url = obj
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "nng_pubsub sink settings require url".to_string())?
        .to_string();
    let topic = obj
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "nng_pubsub sink settings require topic".to_string())?
        .to_string();
    let topic_delimiter = obj
        .get("topic_delimiter")
        .or_else(|| obj.get("topicDelimiter"))
        .and_then(|v| v.as_str())
        .unwrap_or(crate::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER)
        .to_string();
    let config = crate::connector::NngPubSubSinkConfig::new(sink_name, url, topic)
        .with_topic_delimiter(topic_delimiter);
    config.validate()?;
    Ok(config)
}

fn video_sink_from_ir_settings(
    settings: &JsonValue,
) -> Result<crate::connector::sink::video::VideoSinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "video sink settings must be an object".to_string())?;
    let path = obj
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "video sink settings missing path".to_string())?
        .to_string();
    let filename_prefix = obj
        .get("filename_prefix")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "video sink settings missing filename_prefix".to_string())?
        .to_string();
    let codec = match obj.get("codec").and_then(|v| v.as_str()).unwrap_or("h264") {
        "h264" => crate::connector::sink::video::VideoCodecConfig::H264,
        "h265" => crate::connector::sink::video::VideoCodecConfig::H265,
        other => return Err(format!("video sink settings invalid codec `{other}`")),
    };
    let container = match obj
        .get("container")
        .and_then(|v| v.as_str())
        .unwrap_or("mp4")
    {
        "mp4" => crate::connector::sink::video::VideoContainerConfig::Mp4,
        other => return Err(format!("video sink settings invalid container `{other}`")),
    };
    let rolling = obj
        .get("rolling")
        .and_then(|v| v.as_object())
        .ok_or_else(|| "video sink settings missing rolling".to_string())?;
    let rolling = match rolling
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("duration")
    {
        "duration" => crate::connector::sink::video::VideoRollingConfig::Duration {
            duration: std::time::Duration::from_secs(
                rolling
                    .get("seconds")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "video sink rolling duration missing seconds".to_string())?,
            ),
        },
        other => {
            return Err(format!(
                "video sink settings invalid rolling type `{other}`"
            ))
        }
    };

    Ok(crate::connector::sink::video::VideoSinkConfig {
        target: crate::connector::sink::video::VideoSinkTargetConfig::File(
            crate::connector::sink::video::VideoFileSinkConfig {
                path,
                filename_prefix,
            },
        ),
        codec,
        container,
        rolling,
    })
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
    let connector_key = obj
        .get("connector_key")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|s| s.to_string());
    let broker_url = obj
        .get("broker_url")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|s| s.to_string());
    if connector_key.is_none() && broker_url.is_none() {
        return Err("mqtt sink settings missing broker_url".to_string());
    }
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
    let client_id = obj
        .get("client_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let max_packet_size = obj
        .get("max_packet_size")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);

    let mut config = MqttSinkConfig::new(
        sink_name.clone(),
        broker_url.unwrap_or_default(),
        topic,
        qos,
    )
    .with_retain(retain);
    if let Some(client_id) = client_id {
        config = config.with_client_id(client_id);
    }
    if let Some(connector_key) = connector_key {
        config = config.with_connector_key(connector_key);
    }
    if let Some(max_packet_size) = max_packet_size {
        config = config.with_max_packet_size(max_packet_size);
    }
    Ok(config)
}

fn kuksa_sink_from_ir_settings(
    settings: &JsonValue,
) -> Result<crate::connector::KuksaSinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "kuksa sink settings must be an object".to_string())?;

    let sink_name = obj
        .get("sink_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "kuksa sink settings missing sink_name".to_string())?
        .to_string();
    let addr = obj
        .get("addr")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "kuksa sink settings missing addr".to_string())?
        .to_string();
    let vss_path = obj
        .get("vss_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "kuksa sink settings missing vss_path".to_string())?
        .to_string();

    Ok(crate::connector::KuksaSinkConfig {
        sink_name,
        addr,
        vss_path,
    })
}

fn kura_sink_from_ir_settings(
    settings: &JsonValue,
) -> Result<crate::connector::KuraSinkConfig, String> {
    let obj = settings
        .as_object()
        .ok_or_else(|| "kura sink settings must be an object".to_string())?;

    let sink_name = obj
        .get("sink_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "kura sink settings missing sink_name".to_string())?
        .to_string();
    let addr = obj
        .get("addr")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "kura sink settings missing addr".to_string())?
        .to_string();
    let mapping_path = obj
        .get("mapping_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "kura sink settings missing mapping_path".to_string())?
        .to_string();

    Ok(crate::connector::KuraSinkConfig {
        sink_name,
        addr,
        mapping_path,
    })
}

fn common_sink_props_from_ir(common: &CommonSinkPropsIR) -> CommonSinkProps {
    CommonSinkProps {
        batch_count: common.batch_count,
        batch_duration: common.batch_duration_ms.map(Duration::from_millis),
    }
}

fn window_ir_to_spec(
    window: &WindowIR,
) -> Result<crate::planner::logical::LogicalWindowSpec, String> {
    Ok(match window {
        WindowIR::Tumbling { time_unit, length } => {
            crate::planner::logical::LogicalWindowSpec::Tumbling {
                time_unit: time_unit_ir_to_time_unit(*time_unit),
                length: *length,
            }
        }
        WindowIR::Count { count } => {
            crate::planner::logical::LogicalWindowSpec::Count { count: *count }
        }
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

    pub fn encode(&self) -> Result<Vec<u8>, LogicalPlanIrCodecError> {
        encode_ir(self)
    }

    pub fn decode(raw: &[u8]) -> Result<Self, LogicalPlanIrCodecError> {
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
        LogicalPlan::TableScan(_) => LogicalPlanNodeKindIR::Opaque {
            plan_type: "TableScan".to_string(),
        },
        LogicalPlan::StatefulFunction(plan) => {
            let calls = plan
                .calls
                .iter()
                .map(|call| StatefulExprIR {
                    output_name: call.output_column.clone(),
                    call: call.spec.clone(),
                })
                .collect();
            LogicalPlanNodeKindIR::StatefulFunction { calls }
        }
        LogicalPlan::Order(plan) => LogicalPlanNodeKindIR::Order {
            keys: plan
                .items
                .iter()
                .map(|item| OrderKeyIR {
                    expr: item.expr.clone(),
                    asc: item.asc,
                })
                .collect(),
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
        LogicalPlan::Compute(plan) => LogicalPlanNodeKindIR::Compute {
            fields: plan
                .fields
                .iter()
                .map(|f| ProjectFieldIR {
                    field_name: f.field_name.clone(),
                    expr: f.expr.clone(),
                })
                .collect(),
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

fn sink_to_ir(sink: &PipelineSink) -> SinkIR {
    let (connector_kind, connector_settings) = connector_to_ir(&sink.connector.connector);
    SinkIR {
        sink_id: sink.sink_id.clone(),
        forward_to_result: sink.forward_to_result,
        common: Some(common_sink_props_to_ir(&sink.common)),
        output: Some(sink_output_to_ir(&sink.output)),
        connector_kind,
        connector_settings,
        encoder_kind: Some(sink.connector.encoder.kind_str().to_string()),
        encoder_props: Some(sink.connector.encoder.props().clone()),
        encoder_transform: sink
            .connector
            .encoder
            .transform()
            .map(transform_config_to_ir),
    }
}

fn transform_ir_to_config(transform: &SinkEncoderTransformIR) -> SinkEncoderTransformConfig {
    match transform {
        SinkEncoderTransformIR::Template { template } => SinkEncoderTransformConfig::Template {
            template: template.clone(),
        },
    }
}

fn sink_output_from_ir(output: &SinkOutputIR) -> SinkOutputConfig {
    SinkOutputConfig {
        mode: match output.mode {
            SinkOutputModeIR::Full => SinkOutputMode::Full,
            SinkOutputModeIR::Delta => SinkOutputMode::Delta,
        },
        delta: output.delta.as_ref().map(|delta| SinkDeltaOutputConfig {
            columns: delta.columns.clone(),
        }),
        omit_if_empty: output.omit_if_empty,
        include_columns: output.include_columns.clone(),
        exclude_columns: output.exclude_columns.clone(),
    }
}

fn sink_output_to_ir(output: &SinkOutputConfig) -> SinkOutputIR {
    SinkOutputIR {
        mode: match output.mode {
            SinkOutputMode::Full => SinkOutputModeIR::Full,
            SinkOutputMode::Delta => SinkOutputModeIR::Delta,
        },
        delta: output.delta.as_ref().map(|delta| SinkDeltaOutputIR {
            columns: delta.columns.clone(),
        }),
        omit_if_empty: output.omit_if_empty,
        include_columns: output.include_columns.clone(),
        exclude_columns: output.exclude_columns.clone(),
    }
}

fn transform_config_to_ir(transform: &SinkEncoderTransformConfig) -> SinkEncoderTransformIR {
    match transform {
        SinkEncoderTransformConfig::Template { template } => SinkEncoderTransformIR::Template {
            template: template.clone(),
        },
    }
}

fn extract_legacy_transform_from_props(
    encoder_props: &mut JsonMap<String, JsonValue>,
) -> Option<SinkEncoderTransformIR> {
    let transform = encoder_props.remove("transform")?;
    let template = transform
        .as_object()
        .and_then(|obj| obj.get("template"))
        .and_then(|value| value.as_str())?;
    Some(SinkEncoderTransformIR::Template {
        template: template.to_string(),
    })
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
                "max_packet_size": cfg.max_packet_size,
            }),
        ),
        SinkConnectorConfig::Kuksa(cfg) => (
            "kuksa".to_string(),
            serde_json::json!({
                "sink_name": cfg.sink_name,
                "addr": cfg.addr,
                "vss_path": cfg.vss_path,
            }),
        ),
        SinkConnectorConfig::Kura(cfg) => (
            "kura".to_string(),
            serde_json::json!({
                "sink_name": cfg.sink_name,
                "addr": cfg.addr,
                "mapping_path": cfg.mapping_path,
            }),
        ),
        SinkConnectorConfig::Nop(cfg) => {
            if cfg.log {
                ("nop".to_string(), serde_json::json!({ "log": true }))
            } else {
                ("nop".to_string(), JsonValue::Object(JsonMap::new()))
            }
        }
        SinkConnectorConfig::Memory(cfg) => (
            "memory".to_string(),
            serde_json::json!({
                "sink_name": cfg.sink_name,
                "topic": cfg.topic,
                "kind": cfg.kind.to_string(),
            }),
        ),
        SinkConnectorConfig::File(cfg) => (
            "file".to_string(),
            serde_json::json!({
                "sink_name": cfg.sink_name,
                "pipeline_id": cfg.pipeline_id,
                "path": cfg.path,
                "filename_prefix": cfg.filename_prefix,
                "filename_suffix": cfg.filename_suffix,
                "retention": {
                    "max_file_count": cfg.retention.max_file_count,
                    "max_file_age_days": cfg.retention.max_file_age_days,
                },
            }),
        ),
        SinkConnectorConfig::NngPubSub(cfg) => (
            "nng_pubsub".to_string(),
            serde_json::json!({
                "sink_name": cfg.sink_name,
                "url": cfg.url,
                "topic": cfg.topic,
                "topic_delimiter": cfg.topic_delimiter,
            }),
        ),
        SinkConnectorConfig::Http(cfg) => (
            "http".to_string(),
            serde_json::json!({
                "url": cfg.url,
                "method": match cfg.method {
                    crate::connector::sink::http::HttpMethod::Get => "GET",
                    crate::connector::sink::http::HttpMethod::Post => "POST",
                    crate::connector::sink::http::HttpMethod::Put => "PUT",
                    crate::connector::sink::http::HttpMethod::Patch => "PATCH",
                    crate::connector::sink::http::HttpMethod::Delete => "DELETE",
                },
                "timeout_ms": cfg.timeout.as_millis(),
                "max_body_size": cfg.max_body_size,
                "content_type": cfg.content_type,
                "headers": cfg.headers,
            }),
        ),
        SinkConnectorConfig::Video(cfg) => {
            let crate::connector::sink::video::VideoSinkTargetConfig::File(file) = &cfg.target;
            (
                "video".to_string(),
                serde_json::json!({
                    "path": file.path,
                    "filename_prefix": file.filename_prefix,
                    "codec": match cfg.codec {
                        crate::connector::sink::video::VideoCodecConfig::H264 => "h264",
                        crate::connector::sink::video::VideoCodecConfig::H265 => "h265",
                    },
                    "container": match cfg.container {
                        crate::connector::sink::video::VideoContainerConfig::Mp4 => "mp4",
                    },
                    "rolling": match &cfg.rolling {
                        crate::connector::sink::video::VideoRollingConfig::Duration { duration } => {
                            serde_json::json!({
                                "type": "duration",
                                "seconds": duration.as_secs(),
                            })
                        }
                    },
                }),
            )
        }
        SinkConnectorConfig::Custom(custom) => (custom.kind.clone(), custom.settings.clone()),
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
        crate::planner::logical::LogicalWindowSpec::Count { count } => {
            WindowIR::Count { count: *count }
        }
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
    fn logical_plan_ir_roundtrip() {
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

        let raw = logical.encode().unwrap();
        assert_eq!(LogicalPlanIR::decode(&raw).unwrap(), logical);
    }

    #[test]
    fn mqtt_sink_ir_allows_missing_broker_url_when_connector_key_is_present() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "topic": "out/topic",
            "qos": 1,
            "connector_key": "shared_mqtt"
        });

        let config = mqtt_sink_from_ir_settings(&settings).expect("decode mqtt sink config");
        assert_eq!(config.broker_url, "");
        assert_eq!(config.topic, "out/topic");
        assert_eq!(config.connector_key.as_deref(), Some("shared_mqtt"));
    }

    #[test]
    fn nng_pubsub_sink_ir_decodes_topic_delimiter() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "url": "inproc://vf-nng-ir",
            "topic": "topic/can",
            "topicDelimiter": "\u{0}"
        });

        let config =
            nng_pubsub_sink_from_ir_settings(&settings).expect("decode nng pubsub sink config");
        assert_eq!(config.url, "inproc://vf-nng-ir");
        assert_eq!(config.topic, "topic/can");
        assert_eq!(config.topic_delimiter, "\0");
    }

    #[test]
    fn file_sink_ir_roundtrip_preserves_settings() {
        let connector = SinkConnectorConfig::File(crate::connector::sink::file::FileSinkConfig {
            sink_name: "sink_1".to_string(),
            pipeline_id: "pipe_1".to_string(),
            path: "/tmp/vf-file".to_string(),
            filename_prefix: "speed_".to_string(),
            filename_suffix: ".json".to_string(),
            retention: crate::connector::sink::file::FileRetentionConfig {
                max_file_count: 10,
                max_file_age_days: 7,
            },
        });

        let (kind, settings) = connector_to_ir(&connector);
        assert_eq!(kind, "file");
        let decoded = file_sink_from_ir_settings(&settings).expect("decode file sink config");

        assert_eq!(decoded.sink_name, "sink_1");
        assert_eq!(decoded.pipeline_id, "pipe_1");
        assert_eq!(decoded.path, "/tmp/vf-file");
        assert_eq!(decoded.filename_prefix, "speed_");
        assert_eq!(decoded.filename_suffix, ".json");
        assert_eq!(decoded.retention.max_file_count, 10);
        assert_eq!(decoded.retention.max_file_age_days, 7);
    }

    #[test]
    fn file_sink_ir_rejects_missing_pipeline_id() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "path": "/tmp/vf-file",
            "filename_prefix": "speed_",
            "filename_suffix": ".json"
        });

        let err = file_sink_from_ir_settings(&settings).expect_err("missing pipeline_id");

        assert!(
            err.contains("missing pipeline_id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_sink_ir_rejects_empty_pipeline_id() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "pipeline_id": " ",
            "path": "/tmp/vf-file",
            "filename_prefix": "speed_",
            "filename_suffix": ".json"
        });

        let err = file_sink_from_ir_settings(&settings).expect_err("empty pipeline_id");

        assert!(
            err.contains("missing pipeline_id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_sink_ir_rejects_empty_path() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "pipeline_id": "pipe_1",
            "path": " ",
            "filename_prefix": "speed_",
            "filename_suffix": ".json"
        });

        let err = file_sink_from_ir_settings(&settings).expect_err("empty path");

        assert!(err.contains("missing path"), "unexpected error: {err}");
    }

    #[test]
    fn file_sink_ir_rejects_filename_prefix_path_separator() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "pipeline_id": "pipe_1",
            "path": "/tmp/vf-file",
            "filename_prefix": "speed/",
            "filename_suffix": ".json"
        });

        let err = file_sink_from_ir_settings(&settings).expect_err("invalid filename_prefix");

        assert!(
            err.contains("filename_prefix must not contain path separators"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_sink_ir_rejects_filename_suffix_path_separator() {
        let settings = serde_json::json!({
            "sink_name": "sink_1",
            "pipeline_id": "pipe_1",
            "path": "/tmp/vf-file",
            "filename_prefix": "speed_",
            "filename_suffix": "/data.json"
        });

        let err = file_sink_from_ir_settings(&settings).expect_err("invalid filename_suffix");

        assert!(
            err.contains("filename_suffix must not contain path separators"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_sink_ir_rejects_reserved_filename_suffix() {
        for suffix in [".", ".."] {
            let settings = serde_json::json!({
                "sink_name": "sink_1",
                "pipeline_id": "pipe_1",
                "path": "/tmp/vf-file",
                "filename_prefix": "speed_",
                "filename_suffix": suffix
            });

            let err = file_sink_from_ir_settings(&settings).expect_err("invalid filename_suffix");

            assert!(
                err.contains("filename_suffix must not be `.` or `..`"),
                "unexpected error for suffix {suffix}: {err}"
            );
        }
    }
}
