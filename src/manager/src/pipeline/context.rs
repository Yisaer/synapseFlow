use axum::http::StatusCode;
use axum::response::IntoResponse;
use flow::FlowInstance;
use parser::SelectStmt;
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

use storage::StorageManager;

use super::types::{CreatePipelineRequest, CreatePipelineSinkRequest, MqttSinkPropsRequest};

fn normalized_optional_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn select_stmt_from_sql(
    instance: &FlowInstance,
    pipeline_id: &str,
    sql: &str,
) -> Result<SelectStmt, String> {
    parser::parse_sql_with_registries(
        sql,
        instance.aggregate_registry(),
        instance.stateful_registry(),
    )
    .map_err(|err| format!("parse pipeline {pipeline_id} sql: {err}"))
}

fn connector_keys_from_pipeline_sinks(
    pipeline_id: &str,
    sinks: &[CreatePipelineSinkRequest],
) -> Result<BTreeSet<String>, String> {
    let mut keys = BTreeSet::new();
    for sink in sinks {
        if !sink.sink_type.eq_ignore_ascii_case("mqtt") {
            continue;
        }
        let mqtt_props: MqttSinkPropsRequest = serde_json::from_value(sink.props.to_value())
            .map_err(|err| format!("decode pipeline {pipeline_id} mqtt sink props: {err}"))?;
        if let Some(key) = mqtt_props.connector_key
            && let Some(key) = normalized_optional_string(key)
        {
            keys.insert(key);
        }
    }
    Ok(keys)
}

fn connector_key_from_stream(
    req: &crate::stream::CreateStreamRequest,
) -> Result<Option<String>, String> {
    if !req.stream_type.eq_ignore_ascii_case("mqtt") {
        return Ok(None);
    }
    let mqtt_props: crate::stream::MqttStreamPropsRequest =
        serde_json::from_value(JsonValue::Object(req.props.fields.clone()))
            .map_err(|err| format!("decode mqtt stream {} props: {err}", req.name))?;
    Ok(mqtt_props
        .connector_key
        .and_then(normalized_optional_string))
}

pub(super) type PipelineContextError = Box<axum::response::Response>;

pub(super) fn shared_mqtt_connector_keys_from_pipeline_request(
    instance: &FlowInstance,
    storage: &StorageManager,
    pipeline_id: &str,
    pipeline_req: &CreatePipelineRequest,
) -> Result<BTreeSet<String>, PipelineContextError> {
    let select_stmt = select_stmt_from_sql(instance, pipeline_id, &pipeline_req.sql)
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;

    let mut stream_names = select_stmt
        .source_infos
        .iter()
        .map(|source| source.name.clone())
        .collect::<Vec<_>>();
    stream_names.sort();
    stream_names.dedup();

    let mut connector_keys = connector_keys_from_pipeline_sinks(pipeline_id, &pipeline_req.sinks)
        .map_err(|err| {
        Box::new(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to inspect pipeline {pipeline_id} sink props: {err}"),
            )
                .into_response(),
        )
    })?;

    for stream_name in stream_names {
        let stored_stream = match storage.get_stream(&stream_name) {
            Ok(Some(stream)) => stream,
            Ok(None) => {
                return Err(Box::new(
                    (
                        StatusCode::BAD_REQUEST,
                        format!("stream {stream_name} missing from storage"),
                    )
                        .into_response(),
                ));
            }
            Err(err) => {
                return Err(Box::new(
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to read stream {stream_name} from storage: {err}"),
                    )
                        .into_response(),
                ));
            }
        };

        let stream_req: crate::stream::CreateStreamRequest =
            match serde_json::from_str(&stored_stream.raw_json) {
                Ok(req) => req,
                Err(err) => {
                    return Err(Box::new(
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored stream {stream_name}: {err}"),
                        )
                            .into_response(),
                    ));
                }
            };

        match connector_key_from_stream(&stream_req) {
            Ok(Some(key)) => {
                connector_keys.insert(key);
            }
            Ok(None) => {}
            Err(err) => {
                return Err(Box::new(
                    (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
                ));
            }
        }
    }

    Ok(connector_keys)
}
