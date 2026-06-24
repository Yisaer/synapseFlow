#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

use serde_json::{Map, Value};
use std::sync::Arc;

pub mod codec;
pub mod decoder;
pub mod encoder;
pub mod schema;

/// Register all SDV-specific codecs, decoders, mergers, and schema parsers
/// on the given FlowInstance. Called from both normal startup and worker mode.
pub fn register(instance: &flow::FlowInstance) {
    schema::register_dbc_schema();

    let encoder_registry = instance.encoder_registry();
    encoder_registry.register_encoder(
        "yajson",
        Arc::new(|config| {
            Ok(Arc::new(
                flow::JsonEncoder::new(config.kind_str().to_string(), config)
                    .map_err(|err| flow::codec::CodecError::Other(err.to_string()))?,
            ) as Arc<_>)
        }),
    );
    encoder_registry.register_encoder(
        "columnar_json",
        Arc::new(|_config| Ok(Arc::new(encoder::ColumnarJsonEncoder::new()) as Arc<_>)),
    );
    encoder_registry.register_encoder_with_caps(
        "columnar_csv_json",
        Arc::new(|_config| {
            Ok(Arc::new(encoder::ColumnarCsvJsonEncoder::new("columnar_csv_json")) as Arc<_>)
        }),
        true,
    );

    let decoder_registry = instance.decoder_registry();
    decoder::register_gbf_decoder(&decoder_registry);

    let merger_registry = instance.merger_registry();
    merger_registry.register(
        "gbf",
        |props: &Map<String, Value>, schema: Arc<datatypes::Schema>| {
            let schema_file = props
                .get("schema")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    flow::codec::CodecError::Other("missing schema property".to_string())
                })?;

            let format_schema_path = props
                .get("format_schema_path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    flow::codec::CodecError::Other(
                        "gbf packer merger requires `format_schema_path` prop".to_string(),
                    )
                })?;
            build_fused_gbf_merger(props, schema, schema_file, format_schema_path)
        },
    );
}

/// Build a fused (decode-capable) GBF sampler from merger props + the stream's
/// output schema. Requires the same CAN format inputs as the `gbf` decoder.
fn build_fused_gbf_merger(
    props: &Map<String, Value>,
    schema: Arc<datatypes::Schema>,
    schema_file: &str,
    format_schema_path: &str,
) -> Result<Box<dyn flow::Merger>, flow::codec::CodecError> {
    use flow::codec::CodecError;

    let format_type = props
        .get("format_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CodecError::Other("gbf packer merger requires `format_type` prop".into()))?;
    if format_type != "can" {
        return Err(CodecError::Other(format!(
            "unsupported merger format_type: {format_type}"
        )));
    }

    let gbf_schema = schema::gbf::GbfSchema::load(schema_file)
        .map_err(|e| CodecError::Other(format!("failed to load gbf schema: {e}")))?;
    let dbc = schema::dbc::load_can_schema(format_schema_path).map_err(|e| {
        CodecError::Other(format!(
            "failed to load can schema from {format_schema_path}: {e}"
        ))
    })?;

    let pattern = props
        .get("signal_name_pattern")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let clamp_to_range = props
        .get("clamp_to_range")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    // The decoded tuple is namespaced by the stream's source name; derive it
    // from the schema so downstream column resolution matches the `gbf` decoder.
    let source_name = schema
        .column_schemas()
        .first()
        .map(|col| col.source_name.clone())
        .ok_or_else(|| CodecError::Other("output schema has no columns".to_string()))?;

    let fused = decoder::GbfFusedMerger::new(
        source_name,
        schema,
        gbf_schema,
        dbc,
        pattern,
        clamp_to_range,
    )?;
    Ok(Box::new(fused) as Box<dyn flow::Merger>)
}
