use crate::pipeline::AppState;
use crate::{DEFAULT_BROKER_URL, MQTT_QOS, SOURCE_TOPIC};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::catalog::global_catalog;
use flow::shared_stream::{
    SharedStreamConfig, SharedStreamError, SharedStreamInfo, SharedStreamStatus,
    registry as shared_stream_registry,
};
use flow::{
    JsonDecoder, Schema,
    connector::{MqttSourceConfig, MqttSourceConnector},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use flow::{
    BooleanType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, ListType, StringType, StructField, StructType, Uint8Type, Uint16Type,
    Uint32Type, Uint64Type,
};

#[derive(Deserialize)]
pub struct CreateStreamRequest {
    pub name: String,
    pub schema: StreamSchemaRequest,
    #[serde(default)]
    pub shared: SharedStreamOptions,
}

#[derive(Deserialize, Clone)]
pub struct StreamSchemaRequest {
    pub columns: Vec<StreamColumnRequest>,
}

#[derive(Deserialize, Clone)]
pub struct StreamColumnRequest {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub fields: Option<Vec<StreamColumnRequest>>,
    #[serde(default)]
    pub element: Option<Box<StreamColumnRequest>>,
}

#[derive(Deserialize, Default, Clone)]
#[serde(default)]
pub struct SharedStreamOptions {
    pub enabled: bool,
    pub connector: Option<String>,
    pub source_broker: Option<String>,
    pub source_topic: Option<String>,
    pub qos: Option<u8>,
}

impl SharedStreamOptions {
    fn is_enabled(&self) -> bool {
        self.enabled
            || self.connector.is_some()
            || self.source_broker.is_some()
            || self.source_topic.is_some()
            || self.qos.is_some()
    }
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

pub async fn create_stream_handler(Json(req): Json<CreateStreamRequest>) -> impl IntoResponse {
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
    let schema_clone = schema.clone();

    let catalog = global_catalog();
    if let Err(err) = catalog.insert(req.name.clone(), schema) {
        return (
            StatusCode::CONFLICT,
            format!("failed to create stream: {}", err),
        )
            .into_response();
    }

    let mut shared_stream_runtime = None;
    if req.shared.is_enabled() {
        let schema_arc = Arc::new(schema_clone.clone());
        match create_shared_stream(&req.name, schema_arc, &req.shared).await {
            Ok(info) => shared_stream_runtime = Some(info),
            Err(err) => {
                let _ = catalog.remove(&req.name);
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to enable shared stream: {err}"),
                )
                    .into_response();
            }
        }
    }

    println!("[manager] stream {} created", req.name);
    let response = build_stream_info(
        req.name.clone(),
        Arc::new(schema_clone),
        shared_stream_runtime,
    );
    (StatusCode::CREATED, Json(response)).into_response()
}

pub async fn list_streams() -> impl IntoResponse {
    let catalog = global_catalog();
    let schemas = catalog.list();
    let shared_infos = shared_stream_registry().list_streams().await;
    let shared_map: HashMap<String, SharedStreamInfo> = shared_infos
        .into_iter()
        .map(|info| (info.name.clone(), info))
        .collect();

    let mut payload = Vec::with_capacity(schemas.len());
    for (name, schema) in schemas {
        let shared_item = shared_map
            .get(&name)
            .map(|info| into_shared_stream_item(info.clone()));
        payload.push(build_stream_info(name, schema, shared_item));
    }
    Json(payload)
}

pub async fn delete_stream_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pipelines_using_stream = {
        let pipelines = state.pipelines.lock().await;
        pipelines
            .iter()
            .filter(|(_, entry)| entry.streams.iter().any(|stream| stream == &name))
            .map(|(id, _)| id.clone())
            .collect::<Vec<String>>()
    };
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

    match shared_stream_registry().drop_stream(&name).await {
        Ok(_) | Err(SharedStreamError::NotFound(_)) => {}
        Err(SharedStreamError::InUse(consumers)) => {
            return (
                StatusCode::CONFLICT,
                format!(
                    "shared stream {name} still referenced by pipelines: {}",
                    consumers.join(", ")
                ),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to delete shared stream {name}: {err}"),
            )
                .into_response();
        }
    }

    let catalog = global_catalog();
    match catalog.remove(&name) {
        Ok(_) => (StatusCode::OK, format!("stream {name} deleted")).into_response(),
        Err(_) => (StatusCode::NOT_FOUND, format!("stream {name} not found")).into_response(),
    }
}

async fn create_shared_stream(
    name: &str,
    schema: Arc<Schema>,
    options: &SharedStreamOptions,
) -> Result<SharedStreamItem, String> {
    let config = build_shared_stream_config(name, schema, options)?;
    shared_stream_registry()
        .create_stream(config)
        .await
        .map(into_shared_stream_item)
        .map_err(|err| err.to_string())
}

fn build_stream_info(
    name: String,
    schema: Arc<Schema>,
    shared_item: Option<SharedStreamItem>,
) -> StreamInfo {
    StreamInfo {
        name: name.clone(),
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

fn build_shared_stream_config(
    name: &str,
    schema: Arc<Schema>,
    options: &SharedStreamOptions,
) -> Result<SharedStreamConfig, String> {
    if !options.is_enabled() {
        return Err("shared stream disabled".into());
    }
    let mut config = SharedStreamConfig::new(name.to_string(), schema);
    let connector_kind = options
        .connector
        .as_deref()
        .unwrap_or("mqtt")
        .to_ascii_lowercase();
    match connector_kind.as_str() {
        "mqtt" => {
            let broker = options
                .source_broker
                .as_deref()
                .unwrap_or(DEFAULT_BROKER_URL);
            let topic = options.source_topic.as_deref().unwrap_or(SOURCE_TOPIC);
            let qos = options.qos.unwrap_or(MQTT_QOS);
            let source_config =
                MqttSourceConfig::new(name.to_string(), broker.to_string(), topic.to_string(), qos);
            let connector =
                MqttSourceConnector::new(format!("{name}_shared_source_connector"), source_config);
            let decoder = Arc::new(JsonDecoder::new(name.to_string(), config.schema.clone()));
            config.set_connector(Box::new(connector), decoder);
            Ok(config)
        }
        other => Err(format!("unsupported shared stream connector: {other}")),
    }
}

fn build_schema_from_request(req: &CreateStreamRequest) -> Result<Schema, String> {
    let columns: Result<Vec<ColumnSchema>, String> = req
        .schema
        .columns
        .iter()
        .map(|col| column_schema_from_request(req.name.clone(), col))
        .collect();
    columns.map(Schema::new)
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
