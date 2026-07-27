use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::{CompressionCodec, InlineEncryptionKey, SecretEncoding, SinkEncryptionConfig};
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

fn encryption_config() -> SinkEncryptionConfig {
    SinkEncryptionConfig::aes_gcm(
        "sink-aes-v1",
        InlineEncryptionKey::new(hex::encode([7u8; 32]), SecretEncoding::Hex),
    )
    .expect("encryption config")
}

fn build_sink(
    compression: Option<CompressionCodec>,
    encryption: Option<SinkEncryptionConfig>,
) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    )
    .with_compression(compression)
    .with_encryption(encryption);
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
    flow::ExplainReport::from_physical(physical).to_json()
}

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

fn find_operator_child(v: &serde_json::Value, operator: &str) -> Option<String> {
    if v.get("operator").and_then(|o| o.as_str()) == Some(operator) {
        return v
            .get("children")
            .and_then(|c| c.as_array())
            .and_then(|children| children.first())
            .and_then(|first| first.get("operator"))
            .and_then(|o| o.as_str())
            .map(str::to_string);
    }
    v.get("children")
        .and_then(|c| c.as_array())
        .and_then(|children| {
            children
                .iter()
                .find_map(|child| find_operator_child(child, operator))
        })
}

// coverage-covers: planner.physical.sink_encrypt_insertion
#[test]
fn no_encryption_does_not_insert_sink_encrypt() {
    let json = explain_json(build_sink(None, None));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(!operators.contains(&"PhysicalSinkEncrypt".to_string()));
    assert!(operators.contains(&"PhysicalSinkEncoder".to_string()));
}

// coverage-covers: planner.physical.sink_encrypt_insertion
#[test]
fn aes_encryption_inserts_sink_encrypt_with_redacted_explain() {
    let raw_key = hex::encode([7u8; 32]);
    let json = explain_json(build_sink(None, Some(encryption_config())));
    let mut operators = Vec::new();
    collect_operators(&json, &mut operators);
    assert!(operators.contains(&"PhysicalSinkEncrypt".to_string()));

    let json_str = serde_json::to_string(&json).unwrap();
    assert!(json_str.contains("algorithm=aes-gcm"));
    assert!(json_str.contains("key_bits=256"));
    assert!(json_str.contains("key_id=sink-aes-v1"));
    assert!(!json_str.contains(&raw_key), "explain leaked raw key");
    assert!(
        !json_str.contains("encoding"),
        "explain leaked key encoding"
    );
}

// coverage-covers: planner.physical.sink_encrypt_insertion
#[test]
fn gzip_and_aes_tree_is_connector_encrypt_compress_encoder() {
    let json = explain_json(build_sink(
        Some(CompressionCodec::gzip()),
        Some(encryption_config()),
    ));

    assert_eq!(
        find_operator_child(&json, "PhysicalSinkConnector").as_deref(),
        Some("PhysicalSinkEncrypt")
    );
    assert_eq!(
        find_operator_child(&json, "PhysicalSinkEncrypt").as_deref(),
        Some("PhysicalSinkCompress")
    );
    assert_eq!(
        find_operator_child(&json, "PhysicalSinkCompress").as_deref(),
        Some("PhysicalSinkEncoder")
    );
}
