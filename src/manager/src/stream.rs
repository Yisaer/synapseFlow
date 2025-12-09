use crate::pipeline::AppState;
use crate::storage_bridge;
use crate::{DEFAULT_BROKER_URL, MQTT_QOS, SOURCE_TOPIC};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::catalog::{CatalogError, MqttStreamProps, StreamDecoderConfig};
use flow::shared_stream::{SharedStreamError, SharedStreamInfo, SharedStreamStatus};
use flow::{FlowInstanceError, Schema, StreamDefinition, StreamProps, StreamRuntimeInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use flow::{
    BooleanType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, ListType, StringType, StructField, StructType, Uint8Type, Uint16Type,
    Uint32Type, Uint64Type,
};
use storage::StorageError;

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateStreamRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub stream_type: String,
    pub schema: StreamSchemaRequest,
    #[serde(default)]
    pub props: StreamPropsRequest,
    #[serde(default)]
    pub shared: bool,
    #[serde(default)]
    pub decoder: Option<StreamDecoderConfigRequest>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct StreamSchemaRequest {
    pub columns: Vec<StreamColumnRequest>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct StreamColumnRequest {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub fields: Option<Vec<StreamColumnRequest>>,
    #[serde(default)]
    pub element: Option<Box<StreamColumnRequest>>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct StreamPropsRequest {
    #[serde(flatten)]
    pub fields: JsonMap<String, JsonValue>,
}

impl StreamPropsRequest {
    fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct StreamDecoderConfigRequest {
    pub decoder_type: Option<String>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MqttStreamPropsRequest {
    pub broker_url: Option<String>,
    pub topic: Option<String>,
    pub qos: Option<u8>,
    pub client_id: Option<String>,
    pub connector_key: Option<String>,
}

#[derive(Serialize)]
pub struct StreamInfo {
    pub name: String,
    pub shared: bool,
    pub schema: StreamSchemaInfo,
    pub shared_stream: Option<SharedStreamItem>,
}

#[derive(Serialize)]
pub struct StreamSchemaInfo {
    pub columns: Vec<StreamColumnInfo>,
}

#[derive(Serialize)]
pub struct StreamColumnInfo {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<StreamColumnInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<Box<StreamColumnInfo>>,
}

#[derive(Serialize)]
pub struct SharedStreamItem {
    pub id: String,
    pub status: String,
    pub status_message: Option<String>,
    pub connector_id: String,
    pub subscribers: usize,
    pub created_at_secs: u64,
}

pub async fn create_stream_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateStreamRequest>,
) -> impl IntoResponse {
    if req.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "stream name must not be empty".to_string(),
        )
            .into_response();
    }
    if req.schema.columns.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "schema must contain at least one column".to_string(),
        )
            .into_response();
    }

    let schema = match build_schema_from_request(&req) {
        Ok(schema) => schema,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let stream_props = match build_stream_props(&req.stream_type, &req.props) {
        Ok(props) => props,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let decoder = match build_stream_decoder(&req) {
        Ok(config) => config,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let stored = match storage_bridge::stored_stream_from_request(&req) {
        Ok(stored) => stored,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.storage.create_stream(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("stream {} already exists", req.name),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist stream {}: {err}", req.name),
            )
                .into_response();
        }
    }

    let definition =
        StreamDefinition::new(req.name.clone(), Arc::new(schema), stream_props, decoder);

    match state.instance.create_stream(definition, req.shared).await {
        Ok(info) => {
            println!("[manager] stream {} created", req.name);
            (StatusCode::CREATED, Json(build_stream_info(info))).into_response()
        }
        Err(err) => {
            let _ = state.storage.delete_stream(&req.name);
            map_flow_instance_error(err)
        }
    }
}

pub async fn list_streams(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_streams() {
        Ok(entries) => {
            let mut result = Vec::new();
            for entry in entries {
                let req: CreateStreamRequest = match serde_json::from_str(&entry.raw_json) {
                    Ok(req) => req,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored stream {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let schema = match build_schema_from_request(&req) {
                    Ok(s) => s,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("build schema for stream {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let columns = schema
                    .column_schemas()
                    .iter()
                    .map(|col| stream_column_info(&col.name, &col.data_type))
                    .collect();
                result.push(StreamInfo {
                    name: req.name,
                    shared: req.shared,
                    schema: StreamSchemaInfo { columns },
                    shared_stream: None,
                });
            }
            Json(result).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list streams: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_stream_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pipelines_using_stream = state
        .instance
        .list_pipelines()
        .into_iter()
        .filter(|snapshot| snapshot.streams.iter().any(|stream| stream == &name))
        .map(|snapshot| snapshot.definition.id().to_string())
        .collect::<Vec<_>>();
    if !pipelines_using_stream.is_empty() {
        return (
            StatusCode::CONFLICT,
            format!(
                "stream {name} still referenced by pipelines: {}",
                pipelines_using_stream.join(", ")
            ),
        )
            .into_response();
    }

    match state.instance.delete_stream(&name).await {
        Ok(_) => {
            if let Err(err) = state.storage.delete_stream(&name) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "stream {name} deleted in runtime but failed to remove from storage: {err}"
                    ),
                )
                    .into_response();
            }
            (StatusCode::OK, format!("stream {name} deleted")).into_response()
        }
        Err(err) => map_flow_instance_error(err),
    }
}

fn build_stream_info(info: StreamRuntimeInfo) -> StreamInfo {
    let schema = info.definition.schema();
    let shared_item = info.shared_info.map(into_shared_stream_item);
    StreamInfo {
        name: info.definition.id().to_string(),
        shared: shared_item.is_some(),
        schema: StreamSchemaInfo {
            columns: schema
                .column_schemas()
                .iter()
                .map(|col| stream_column_info(&col.name, &col.data_type))
                .collect(),
        },
        shared_stream: shared_item,
    }
}

fn map_flow_instance_error(err: FlowInstanceError) -> axum::response::Response {
    let (status, message) = match err {
        FlowInstanceError::Catalog(CatalogError::AlreadyExists(name)) => (
            StatusCode::CONFLICT,
            format!("stream {name} already exists"),
        ),
        FlowInstanceError::Catalog(CatalogError::NotFound(name)) => {
            (StatusCode::NOT_FOUND, format!("stream {name} not found"))
        }
        FlowInstanceError::SharedStream(SharedStreamError::InUse(consumers)) => (
            StatusCode::CONFLICT,
            format!(
                "shared stream still referenced by pipelines: {}",
                consumers.join(", ")
            ),
        ),
        FlowInstanceError::SharedStream(SharedStreamError::NotFound(name)) => (
            StatusCode::NOT_FOUND,
            format!("shared stream {name} not found"),
        ),
        other => (StatusCode::BAD_REQUEST, other.to_string()),
    };

    (status, message).into_response()
}

fn stream_column_info(name: &str, datatype: &ConcreteDatatype) -> StreamColumnInfo {
    let mut info = StreamColumnInfo {
        name: name.to_string(),
        data_type: datatype_name(datatype),
        fields: None,
        element: None,
    };

    match datatype {
        ConcreteDatatype::Struct(struct_type) => {
            let field_infos = struct_type
                .fields()
                .iter()
                .map(|field| stream_column_info(field.name(), field.data_type()))
                .collect();
            info.fields = Some(field_infos);
        }
        ConcreteDatatype::List(list_type) => {
            let element_info = stream_column_info("element", list_type.item_type());
            info.element = Some(Box::new(element_info));
        }
        _ => {}
    }

    info
}

pub(crate) fn build_schema_from_request(req: &CreateStreamRequest) -> Result<Schema, String> {
    let columns: Result<Vec<ColumnSchema>, String> = req
        .schema
        .columns
        .iter()
        .map(|col| column_schema_from_request(req.name.clone(), col))
        .collect();
    columns.map(Schema::new)
}

pub(crate) fn build_stream_props(
    stream_type: &str,
    props: &StreamPropsRequest,
) -> Result<StreamProps, String> {
    match stream_type.to_ascii_lowercase().as_str() {
        "mqtt" => {
            let mqtt_props: MqttStreamPropsRequest = serde_json::from_value(props.to_value())
                .map_err(|err| format!("invalid mqtt props: {}", err))?;
            let broker = mqtt_props
                .broker_url
                .unwrap_or_else(|| DEFAULT_BROKER_URL.to_string());
            let topic = mqtt_props.topic.unwrap_or_else(|| SOURCE_TOPIC.to_string());
            let qos = mqtt_props.qos.unwrap_or(MQTT_QOS);
            Ok(StreamProps::Mqtt(MqttStreamProps {
                broker_url: broker,
                topic,
                qos,
                client_id: mqtt_props.client_id,
                connector_key: mqtt_props.connector_key,
            }))
        }
        other => Err(format!("unsupported stream type: {other}")),
    }
}

pub(crate) fn build_stream_decoder(
    req: &CreateStreamRequest,
) -> Result<StreamDecoderConfig, String> {
    let config = req.decoder.clone().unwrap_or_default();
    Ok(StreamDecoderConfig::new(
        config
            .decoder_type
            .unwrap_or_else(|| format!("{}_decoder", req.name)),
    ))
}

fn column_schema_from_request(
    source: String,
    column: &StreamColumnRequest,
) -> Result<ColumnSchema, String> {
    parse_datatype(column).map(|datatype| ColumnSchema::new(source, column.name.clone(), datatype))
}

fn parse_datatype(column: &StreamColumnRequest) -> Result<ConcreteDatatype, String> {
    match column.data_type.to_ascii_lowercase().as_str() {
        "null" => Ok(ConcreteDatatype::Null),
        "bool" | "boolean" => Ok(ConcreteDatatype::Bool(BooleanType)),
        "int8" => Ok(ConcreteDatatype::Int8(Int8Type)),
        "int16" => Ok(ConcreteDatatype::Int16(Int16Type)),
        "int32" => Ok(ConcreteDatatype::Int32(Int32Type)),
        "int64" => Ok(ConcreteDatatype::Int64(Int64Type)),
        "uint8" => Ok(ConcreteDatatype::Uint8(Uint8Type)),
        "uint16" => Ok(ConcreteDatatype::Uint16(Uint16Type)),
        "uint32" => Ok(ConcreteDatatype::Uint32(Uint32Type)),
        "uint64" => Ok(ConcreteDatatype::Uint64(Uint64Type)),
        "float32" => Ok(ConcreteDatatype::Float32(Float32Type)),
        "float64" => Ok(ConcreteDatatype::Float64(Float64Type)),
        "string" => Ok(ConcreteDatatype::String(StringType)),
        "list" => {
            let element = column.element.as_deref().ok_or_else(|| {
                format!("list column {} requires element definition", column.name)
            })?;
            let element_type = parse_datatype(element)?;
            Ok(ConcreteDatatype::List(ListType::new(Arc::new(
                element_type,
            ))))
        }
        "struct" => {
            let fields = column.fields.as_deref().ok_or_else(|| {
                format!("struct column {} requires fields definition", column.name)
            })?;
            let struct_fields: Result<Vec<StructField>, String> = fields
                .iter()
                .map(|field| {
                    let field_type = parse_datatype(field)?;
                    Ok(StructField::new(field.name.clone(), field_type, false))
                })
                .collect();
            Ok(ConcreteDatatype::Struct(StructType::new(Arc::new(
                struct_fields?,
            ))))
        }
        other => Err(format!("unsupported data type: {}", other)),
    }
}

fn datatype_name(datatype: &ConcreteDatatype) -> String {
    match datatype {
        ConcreteDatatype::Null => "null",
        ConcreteDatatype::Float32(_) => "float32",
        ConcreteDatatype::Float64(_) => "float64",
        ConcreteDatatype::Int8(_) => "int8",
        ConcreteDatatype::Int16(_) => "int16",
        ConcreteDatatype::Int32(_) => "int32",
        ConcreteDatatype::Int64(_) => "int64",
        ConcreteDatatype::Uint8(_) => "uint8",
        ConcreteDatatype::Uint16(_) => "uint16",
        ConcreteDatatype::Uint32(_) => "uint32",
        ConcreteDatatype::Uint64(_) => "uint64",
        ConcreteDatatype::String(_) => "string",
        ConcreteDatatype::Struct(_) => "struct",
        ConcreteDatatype::List(_) => "list",
        ConcreteDatatype::Bool(_) => "bool",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn struct_request() -> StreamColumnRequest {
        StreamColumnRequest {
            name: "user".to_string(),
            data_type: "struct".to_string(),
            fields: Some(vec![
                StreamColumnRequest {
                    name: "id".to_string(),
                    data_type: "int64".to_string(),
                    fields: None,
                    element: None,
                },
                StreamColumnRequest {
                    name: "sessions".to_string(),
                    data_type: "list".to_string(),
                    fields: None,
                    element: Some(Box::new(StreamColumnRequest {
                        name: "session".to_string(),
                        data_type: "struct".to_string(),
                        fields: Some(vec![
                            StreamColumnRequest {
                                name: "session_id".to_string(),
                                data_type: "string".to_string(),
                                fields: None,
                                element: None,
                            },
                            StreamColumnRequest {
                                name: "events".to_string(),
                                data_type: "list".to_string(),
                                fields: None,
                                element: Some(Box::new(StreamColumnRequest {
                                    name: "event".to_string(),
                                    data_type: "string".to_string(),
                                    fields: None,
                                    element: None,
                                })),
                            },
                        ]),
                        element: None,
                    })),
                },
            ]),
            element: None,
        }
    }

    #[test]
    fn parse_datatype_nested_struct_list() {
        let column = struct_request();
        let datatype = parse_datatype(&column).expect("should parse nested struct");

        let expected = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
            StructField::new("id".to_string(), ConcreteDatatype::Int64(Int64Type), false),
            StructField::new(
                "sessions".to_string(),
                ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Struct(
                    StructType::new(Arc::new(vec![
                        StructField::new(
                            "session_id".to_string(),
                            ConcreteDatatype::String(StringType),
                            false,
                        ),
                        StructField::new(
                            "events".to_string(),
                            ConcreteDatatype::List(ListType::new(Arc::new(
                                ConcreteDatatype::String(StringType),
                            ))),
                            false,
                        ),
                    ])),
                )))),
                false,
            ),
        ])));

        assert_eq!(datatype, expected);
    }

    #[test]
    fn parse_datatype_errors_without_nested_payload() {
        let missing_fields = StreamColumnRequest {
            name: "bad_struct".to_string(),
            data_type: "struct".to_string(),
            fields: None,
            element: None,
        };
        assert!(parse_datatype(&missing_fields).is_err());

        let missing_element = StreamColumnRequest {
            name: "bad_list".to_string(),
            data_type: "list".to_string(),
            fields: None,
            element: None,
        };
        assert!(parse_datatype(&missing_element).is_err());
    }
}

fn into_shared_stream_item(info: SharedStreamInfo) -> SharedStreamItem {
    let (status, status_message) = shared_stream_status_label(&info.status);
    SharedStreamItem {
        id: info.name,
        status,
        status_message,
        connector_id: info.connector_id,
        subscribers: info.subscriber_count,
        created_at_secs: unix_timestamp_secs(info.created_at),
    }
}

fn shared_stream_status_label(status: &SharedStreamStatus) -> (String, Option<String>) {
    match status {
        SharedStreamStatus::Starting => ("starting".to_string(), None),
        SharedStreamStatus::Running => ("running".to_string(), None),
        SharedStreamStatus::Stopped => ("stopped".to_string(), None),
        SharedStreamStatus::Failed(msg) => ("failed".to_string(), Some(msg.clone())),
    }
}

fn unix_timestamp_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
