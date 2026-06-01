use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::audit::ResourceMutationLog;
use crate::pipeline::AppState;
use crate::stream::{named_schema_store, schema_registry};
use storage::{StorageError, StoredSchema};

#[derive(Deserialize)]
pub struct CreateSchemaRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub schema_type: String,
    #[serde(default)]
    pub props: JsonMap<String, JsonValue>,
}

#[derive(Serialize)]
pub struct SchemaInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub schema_type: String,
    pub props: JsonMap<String, JsonValue>,
    pub columns: Vec<SchemaColumnInfo>,
}

#[derive(Serialize)]
pub struct SchemaColumnInfo {
    pub name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<SchemaColumnInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<Box<SchemaColumnInfo>>,
}

pub async fn create_schema_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateSchemaRequest>,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("schema", "create", req.name.as_str(), None);
    let name = req.name.trim().to_string();
    if name.is_empty() {
        let err = "schema name must not be empty".to_string();
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    if req.schema_type.trim().is_empty() {
        let err = "schema type must not be empty".to_string();
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let schema = match schema_registry().parse(&req.schema_type, &name, &req.props) {
        Ok(s) => s,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let props_json = match serde_json::to_string(&req.props) {
        Ok(s) => s,
        Err(e) => {
            let err = format!("serialize schema props: {e}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };
    let stored = StoredSchema {
        name: name.clone(),
        schema_type: req.schema_type.clone(),
        props_json,
    };

    match state.storage.create_schema(stored) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("schema {name} already exists"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist schema {name}: {err}"),
            )
                .into_response();
        }
    }

    named_schema_store().insert(name, schema);
    audit.log_success();
    (
        StatusCode::CREATED,
        Json(serde_json::json!({ "name": req.name })),
    )
        .into_response()
}

pub async fn list_schemas_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_schemas() {
        Ok(entries) => {
            let result: Vec<SchemaInfo> = entries
                .into_iter()
                .map(|stored| stored_to_info(&stored))
                .collect();
            Json(result).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list schemas: {err}"),
        )
            .into_response(),
    }
}

pub async fn get_schema_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.storage.get_schema(&name) {
        Ok(Some(stored)) => (StatusCode::OK, Json(stored_to_info(&stored))).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("schema {name} not found")).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to get schema {name}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_schema_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("schema", "delete", name.as_str(), None);

    // Check if any stream references this schema
    let referencing = match find_streams_referencing_schema(&state, &name) {
        Ok(streams) => streams,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };
    if !referencing.is_empty() {
        let err = format!(
            "schema {name} is still referenced by streams: {}",
            referencing.join(", ")
        );
        audit.log_failure(&err);
        return (StatusCode::CONFLICT, err).into_response();
    }

    match state.storage.delete_schema(&name) {
        Ok(()) => {
            named_schema_store().remove(&name);
            audit.log_success();
            (StatusCode::OK, format!("schema {name} deleted")).into_response()
        }
        Err(StorageError::NotFound(_)) => {
            let err = format!("schema {name} not found");
            audit.log_failure(&err);
            (StatusCode::NOT_FOUND, err).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to delete schema {name}: {err}"),
        )
            .into_response(),
    }
}

fn stored_to_info(stored: &StoredSchema) -> SchemaInfo {
    let props: JsonMap<String, JsonValue> =
        serde_json::from_str(&stored.props_json).unwrap_or_default();
    let columns = match named_schema_store().get(&stored.name) {
        Some(schema) => schema
            .column_schemas()
            .iter()
            .map(|col| schema_column_info(&col.name, &col.data_type))
            .collect(),
        None => Vec::new(),
    };
    SchemaInfo {
        name: stored.name.clone(),
        schema_type: stored.schema_type.clone(),
        props,
        columns,
    }
}

fn schema_column_info(name: &str, datatype: &flow::ConcreteDatatype) -> SchemaColumnInfo {
    let mut info = SchemaColumnInfo {
        name: name.to_string(),
        data_type: datatype_name(datatype),
        fields: None,
        element: None,
    };
    match datatype {
        flow::ConcreteDatatype::Struct(struct_type) => {
            info.fields = Some(
                struct_type
                    .fields()
                    .iter()
                    .map(|field| schema_column_info(field.name(), field.data_type()))
                    .collect(),
            );
        }
        flow::ConcreteDatatype::List(list_type) => {
            info.element = Some(Box::new(schema_column_info(
                "element",
                list_type.item_type(),
            )));
        }
        _ => {}
    }
    info
}

fn datatype_name(dt: &flow::ConcreteDatatype) -> String {
    match dt {
        flow::ConcreteDatatype::Null => "null",
        flow::ConcreteDatatype::Float32(_) => "float32",
        flow::ConcreteDatatype::Float64(_) => "float64",
        flow::ConcreteDatatype::Int8(_) => "int8",
        flow::ConcreteDatatype::Int16(_) => "int16",
        flow::ConcreteDatatype::Int32(_) => "int32",
        flow::ConcreteDatatype::Int64(_) => "int64",
        flow::ConcreteDatatype::Uint8(_) => "uint8",
        flow::ConcreteDatatype::Uint16(_) => "uint16",
        flow::ConcreteDatatype::Uint32(_) => "uint32",
        flow::ConcreteDatatype::Uint64(_) => "uint64",
        flow::ConcreteDatatype::String(_) => "string",
        flow::ConcreteDatatype::Bytes(_) => "bytes",
        flow::ConcreteDatatype::Timestamp(_) => "timestamp",
        flow::ConcreteDatatype::Struct(_) => "struct",
        flow::ConcreteDatatype::List(_) => "list",
        flow::ConcreteDatatype::Bool(_) => "bool",
    }
    .to_string()
}

fn find_streams_referencing_schema(
    state: &AppState,
    schema_name: &str,
) -> Result<Vec<String>, String> {
    let streams = state
        .storage
        .list_streams()
        .map_err(|e| format!("list streams: {e}"))?;
    let mut referencing = Vec::new();
    for stream in streams {
        // Deserialize CreateStreamRequest to check the schema.ref field
        #[derive(Deserialize)]
        struct SchemaRef {
            #[serde(rename = "ref")]
            r#ref: Option<String>,
        }
        #[derive(Deserialize)]
        struct StreamSchemaCheck {
            schema: SchemaRef,
            name: String,
        }
        match serde_json::from_str::<StreamSchemaCheck>(&stream.raw_json) {
            Ok(check) if check.schema.r#ref.as_deref() == Some(schema_name) => {
                referencing.push(check.name);
            }
            _ => {}
        }
    }
    Ok(referencing)
}
