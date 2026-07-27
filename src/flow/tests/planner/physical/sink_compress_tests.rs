use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::CompressionCodec;
use flow::planner::logical::create_logical_plan;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::{
    NopSinkConfig, PipelineRegistries, PipelineSink, PipelineSinkConnector, SinkConnectorConfig,
    SinkEncoderConfig,
};
use parser::parse_sql;
use std::collections::HashMap;
use std::sync::Arc;

fn setup_registry() -> PipelineRegistries {
    PipelineRegistries::new_with_builtin()
}

fn stream_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "v".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]))
}

fn build_sink_with_compression(compression: Option<CompressionCodec>) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    )
    .with_compression(compression);
    PipelineSink::new("test_sink", connector)
}

fn explain_json(sink: PipelineSink) -> serde_json::Value {
    let schema = stream_schema();
    let stream = StreamDefinition::new(
        "stream",
        Arc::clone(&schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    let stream_defs = HashMap::from([("stream".to_string(), Arc::new(stream))]);
    let sql = "SELECT v FROM stream";
    let select_stmt = parse_sql(sql).expect("parse sql");
    let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
        source_name: "stream".to_string(),
        alias: None,
        schema: Arc::clone(&schema),
        kind: SourceBindingKind::Regular,
    }]);
    let registries = setup_registry();

    let logical = create_logical_plan(select_stmt, vec![sink], &stream_defs).expect("logical plan");
    let (logical, bindings) = flow::optimize_logical_plan(logical, &bindings);
    let physical = flow::create_physical_plan(Arc::clone(&logical), &bindings, &registries)
        .expect("physical plan");
    let physical = flow::optimize_physical_plan(
        physical,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let report = flow::ExplainReport::from_physical(physical);
    report.to_json()
}

/// Collect all operator IDs from an explain JSON tree.
fn collect_operators(v: &serde_json::Value, out: &mut Vec<String>) {
    if let Some(op) = v.get("operator").and_then(|o| o.as_str()) {
        out.push(op.to_string());
    }
    if let Some(children) = v.get("children").and_then(|c| c.as_array()) {
        for child in children {
            collect_operators(child, out);
        }
    }
}

// coverage-covers: planner.physical.sink_compress_insertion
#[test]
fn no_compression_does_not_insert_sink_compress() {
    let json = explain_json(build_sink_with_compression(None));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        !operators.contains(&"PhysicalSinkCompress".to_string()),
        "unexpected PhysicalSinkCompress in explain: {operators:?}"
    );
    assert!(operators.contains(&"PhysicalSinkEncoder".to_string()));
}

// coverage-covers: planner.physical.sink_compress_insertion
#[test]
fn gzip_compression_inserts_sink_compress_with_codec_gzip() {
    let json = explain_json(build_sink_with_compression(Some(CompressionCodec::gzip())));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        operators.contains(&"PhysicalSinkCompress".to_string()),
        "expected PhysicalSinkCompress in explain: {operators:?}"
    );
    let json_str = serde_json::to_string(&json).unwrap();
    assert!(
        json_str.contains("codec=gzip"),
        "expected codec=gzip info: {json_str}"
    );
}

// coverage-covers: planner.physical.sink_compress_insertion
#[test]
fn zstd_compression_with_level_shows_level_in_explain() {
    let json = explain_json(build_sink_with_compression(Some(
        CompressionCodec::zstd_with_level(3),
    )));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(
        operators.contains(&"PhysicalSinkCompress".to_string()),
        "expected PhysicalSinkCompress in explain: {operators:?}"
    );
    let json_str = serde_json::to_string(&json).unwrap();
    assert!(
        json_str.contains("codec=zstd"),
        "expected codec=zstd info: {json_str}"
    );
    assert!(
        json_str.contains("level=3"),
        "expected level=3 info: {json_str}"
    );
}

// coverage-covers: planner.physical.sink_compress_insertion
#[test]
fn sink_compress_is_placed_between_encoder_and_connector() {
    let json = explain_json(build_sink_with_compression(Some(CompressionCodec::gzip())));

    fn find_compress_child(v: &serde_json::Value) -> Option<String> {
        if let Some(op) = v.get("operator").and_then(|o| o.as_str()) {
            if op == "PhysicalSinkCompress" {
                if let Some(children) = v.get("children").and_then(|c| c.as_array()) {
                    if let Some(first) = children.first() {
                        return first
                            .get("operator")
                            .and_then(|o| o.as_str())
                            .map(str::to_string);
                    }
                }
            }
        }
        if let Some(children) = v.get("children").and_then(|c| c.as_array()) {
            for child in children {
                if let Some(result) = find_compress_child(child) {
                    return Some(result);
                }
            }
        }
        None
    }

    let child_of_compress = find_compress_child(&json);
    assert_eq!(
        child_of_compress.as_deref(),
        Some("PhysicalSinkEncoder"),
        "PhysicalSinkCompress should have PhysicalSinkEncoder as child, got: {child_of_compress:?}"
    );
}
