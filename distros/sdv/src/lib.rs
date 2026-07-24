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
    schema::register_arxml_schema();
    schema::register_busmirror_schema();
    schema::register_gbf_schema();

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
    encoder_registry.register_encoder(
        "columnar_csv_json",
        Arc::new(|_config| {
            Ok(Arc::new(encoder::ColumnarCsvJsonEncoder::new("columnar_csv_json")) as Arc<_>)
        }),
    );

    let decoder_registry = instance.decoder_registry();
    decoder::register_busmirror_decoder(&decoder_registry);
    decoder::register_gbf_decoder(&decoder_registry);

    let merger_registry = instance.merger_registry();
    merger_registry.register_with_schema_artifact(
        "busmirror",
        |_props: &Map<String, Value>, schema: Arc<datatypes::Schema>, artifact| {
            let artifact = artifact
                .and_then(|artifact| {
                    artifact
                        .downcast::<schema::busmirror::CompiledBusMirrorSchema>()
                        .ok()
                })
                .ok_or_else(|| {
                    flow::codec::CodecError::Other(
                        "busmirror packer merger requires a resolved BusMirror schema".to_string(),
                    )
                })?;
            build_fused_busmirror_merger(schema, &artifact)
        },
    );
    merger_registry.register_with_schema_artifact(
        "gbf",
        |_props: &Map<String, Value>, schema: Arc<datatypes::Schema>, artifact| {
            let artifact = artifact
                .and_then(|artifact| artifact.downcast::<schema::gbf::CompiledGbfSchema>().ok())
                .ok_or_else(|| {
                    flow::codec::CodecError::Other(
                        "gbf packer merger requires a resolved GBF schema".to_string(),
                    )
                })?;
            build_fused_gbf_merger(schema, &artifact)
        },
    );
}

fn build_fused_busmirror_merger(
    schema: Arc<datatypes::Schema>,
    compiled: &schema::busmirror::CompiledBusMirrorSchema,
) -> Result<Box<dyn flow::Merger>, flow::codec::CodecError> {
    use flow::codec::CodecError;

    let source_name = schema
        .column_schemas()
        .first()
        .map(|column| column.source_name.clone())
        .ok_or_else(|| CodecError::Other("output schema has no columns".to_string()))?;
    let fused = decoder::BusMirrorFusedMerger::from_compiled(source_name, schema, compiled)?;
    Ok(Box::new(fused) as Box<dyn flow::Merger>)
}

/// Build a fused (decode-capable) GBF sampler from merger props + the stream's
/// output schema. Requires the same CAN format inputs as the `gbf` decoder.
fn build_fused_gbf_merger(
    schema: Arc<datatypes::Schema>,
    compiled: &schema::gbf::CompiledGbfSchema,
) -> Result<Box<dyn flow::Merger>, flow::codec::CodecError> {
    use flow::codec::CodecError;

    // The decoded tuple is namespaced by the stream's source name; derive it
    // from the schema so downstream column resolution matches the `gbf` decoder.
    let source_name = schema
        .column_schemas()
        .first()
        .map(|col| col.source_name.clone())
        .ok_or_else(|| CodecError::Other("output schema has no columns".to_string()))?;

    let fused = decoder::GbfFusedMerger::from_compiled_gbf(source_name, schema, compiled)?;
    Ok(Box::new(fused) as Box<dyn flow::Merger>)
}
