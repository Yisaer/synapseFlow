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
    merger_registry.register("gbf", |props: &Map<String, Value>| {
        let schema_file = props
            .get("schema")
            .and_then(|v| v.as_str())
            .ok_or_else(|| flow::codec::CodecError::Other("missing schema property".to_string()))?;
        Ok(Box::new(codec::GbfMerger::from_schema_file(schema_file)?) as Box<dyn flow::Merger>)
    });
}
