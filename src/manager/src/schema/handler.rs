use axum::{
    Json,
    extract::{Multipart, Path, Query, State, multipart::MultipartRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::audit::ResourceMutationLog;
use crate::pipeline::AppState;
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use crate::stream::{
    PipelineRestartResult, named_schema_store, refresh_stream_and_restart_pipelines,
    schema_registry,
};
use crate::streaming_upload::{
    TemporaryUpload, is_zip_filename, read_text_field, required_multipart,
};
use crate::table::refresh_table_runtime;
use storage::{StorageError, StoredSchema};

use super::source::{PreparedSchemaSource, delete_installed_source};

#[derive(Deserialize)]
pub struct CreateSchemaRequest {
    pub name: String,
    #[serde(deserialize_with = "crate::revision::deserialize_revision")]
    pub revision: u64,
    #[serde(rename = "type")]
    pub schema_type: String,
    #[serde(default)]
    pub props: JsonMap<String, JsonValue>,
}

#[derive(Deserialize)]
pub struct UpdateSchemaQuery {
    #[serde(default)]
    pub restart_pipelines: bool,
}

#[derive(Serialize)]
pub struct SchemaInfo {
    pub name: String,
    pub revision: u64,
    #[serde(rename = "type")]
    pub schema_type: String,
    pub props: JsonMap<String, JsonValue>,
    pub columns: Vec<SchemaColumnInfo>,
}

#[derive(Serialize)]
struct ResourceRefreshResult {
    id: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
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
) -> Response {
    let _permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another import/export/upload operation is in progress",
            )
                .into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "operation guard closed").into_response();
        }
    };
    create_schema_locked(&state, req, false).await
}

pub async fn update_schema_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<UpdateSchemaQuery>,
    Json(req): Json<CreateSchemaRequest>,
) -> Response {
    let _storage_permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another storage operation is in progress",
            )
                .into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation guard closed",
            )
                .into_response();
        }
    };
    if name != req.name {
        return (
            StatusCode::BAD_REQUEST,
            "schema name in path and body differ",
        )
            .into_response();
    }
    let current = match state.storage.get_schema(&name) {
        Ok(Some(schema)) => schema,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("schema {name} not found")).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read schema {name}: {err}"),
            )
                .into_response();
        }
    };
    if req.revision < current.revision {
        return (
            StatusCode::CONFLICT,
            format!(
                "schema {name} older_revision: incoming revision {}, current revision {}",
                req.revision, current.revision
            ),
        )
            .into_response();
    }
    let current_props: JsonValue = serde_json::from_str(&current.props_json).unwrap_or_default();
    let incoming_props = JsonValue::Object(req.props.clone());
    if req.revision == current.revision {
        if current.schema_type == req.schema_type && current_props == incoming_props {
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "name": name,
                    "revision": current.revision
                })),
            )
                .into_response();
        }
        return (StatusCode::CONFLICT, format!(
            "schema {name} same_revision_different_spec: incoming revision {}, current revision {}",
            req.revision, current.revision
        )).into_response();
    }

    let (streams, tables) = match find_resources_referencing_schema(&state, &name) {
        Ok(references) => references,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    };
    let mut resource_permits = Vec::with_capacity(streams.len() + tables.len());
    for resource in streams.iter().chain(tables.iter()) {
        match state.try_acquire_stream_op(resource).await {
            Ok(permit) => resource_permits.push(permit),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return (
                    StatusCode::CONFLICT,
                    format!("resource {resource} is busy processing another command"),
                )
                    .into_response();
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "resource operation guard closed",
                )
                    .into_response();
            }
        }
    }

    let mut source = match PreparedSchemaSource::prepare_for_update(
        &state.storage,
        &name,
        &req.schema_type,
        &req.props,
    ) {
        Ok(source) => source,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (schema, proto_bundle, artifact) =
        match schema_registry().parse(&req.schema_type, &name, source.parse_props()) {
            Ok(parsed) => parsed,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
    let props_json = match serde_json::to_string(source.stored_props()) {
        Ok(props) => props,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("serialize schema props: {err}"),
            )
                .into_response();
        }
    };
    if let Err(err) = source.commit() {
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }
    if let Err(err) = state.storage.replace_schema(StoredSchema {
        name: name.clone(),
        revision: req.revision,
        schema_type: req.schema_type,
        props_json,
    }) {
        let _ = source.rollback();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist schema {name}: {err}"),
        )
            .into_response();
    }
    named_schema_store().insert_resolved(
        name.clone(),
        req.revision,
        schema,
        proto_bundle,
        artifact,
    );
    source.finish();

    let mut pipeline_results = Vec::<PipelineRestartResult>::new();
    let mut stream_results = Vec::<ResourceRefreshResult>::new();
    for stream in &streams {
        match refresh_stream_and_restart_pipelines(&state, stream, query.restart_pipelines).await {
            Ok(results) => {
                pipeline_results.extend(results);
                stream_results.push(ResourceRefreshResult {
                    id: stream.clone(),
                    status: "refreshed",
                    error: None,
                });
            }
            Err(err) => stream_results.push(ResourceRefreshResult {
                id: stream.clone(),
                status: "failed",
                error: Some(err),
            }),
        }
    }
    let mut table_results = Vec::<ResourceRefreshResult>::new();
    for table in &tables {
        if let Err(err) = refresh_table_runtime(&state, table).await {
            table_results.push(ResourceRefreshResult {
                id: table.clone(),
                status: "failed",
                error: Some(err),
            });
        } else {
            table_results.push(ResourceRefreshResult {
                id: table.clone(),
                status: "refreshed",
                error: None,
            });
        }
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "name": name,
            "revision": req.revision,
            "pipeline_restart": {
                "requested": query.restart_pipelines,
                "results": pipeline_results
            },
            "stream_refresh": stream_results,
            "table_refresh": table_results
        })),
    )
        .into_response()
}

async fn create_schema_locked(
    state: &AppState,
    req: CreateSchemaRequest,
    include_type: bool,
) -> Response {
    let audit = ResourceMutationLog::new("schema", "create", req.name.as_str(), Some(req.revision));
    if let Err(err) = validate_resource_id(ResourceIdKind::SchemaName, &req.name) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let name = req.name.clone();
    if req.schema_type.trim().is_empty() {
        let err = "schema type must not be empty".to_string();
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    match state.storage.get_schema(&name) {
        Ok(Some(_)) => {
            let err = format!("schema {name} already exists");
            audit.log_failure(&err);
            return (StatusCode::CONFLICT, err).into_response();
        }
        Ok(None) => {}
        Err(err) => {
            let err = format!("failed to check schema {name}: {err}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    }
    let mut source =
        match PreparedSchemaSource::prepare(&state.storage, &name, &req.schema_type, &req.props) {
            Ok(source) => source,
            Err(err) => {
                audit.log_failure(&err);
                return (StatusCode::BAD_REQUEST, err).into_response();
            }
        };

    let (schema, proto_bundle, artifact) =
        match schema_registry().parse(&req.schema_type, &name, source.parse_props()) {
            Ok(s) => s,
            Err(err) => {
                audit.log_failure(&err);
                return (StatusCode::BAD_REQUEST, err).into_response();
            }
        };

    let props_json = match serde_json::to_string(source.stored_props()) {
        Ok(s) => s,
        Err(e) => {
            let err = format!("serialize schema props: {e}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };
    let stored = StoredSchema {
        name: name.clone(),
        revision: req.revision,
        schema_type: req.schema_type.clone(),
        props_json,
    };

    if let Err(err) = source.commit() {
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }

    match state.storage.create_schema(stored) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            let mut err = format!("schema {name} already exists");
            if let Err(cleanup_err) = source.rollback() {
                err = format!("{err}; rollback failed: {cleanup_err}");
            }
            audit.log_failure(&err);
            return (StatusCode::CONFLICT, err).into_response();
        }
        Err(storage_err) => {
            let mut err = format!("failed to persist schema {name}: {storage_err}");
            if let Err(cleanup_err) = source.rollback() {
                err = format!("{err}; rollback failed: {cleanup_err}");
            }
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    }

    named_schema_store().insert_resolved(name, req.revision, schema, proto_bundle, artifact);
    audit.log_success();
    let body = if include_type {
        serde_json::json!({
            "name": req.name,
            "revision": req.revision,
            "type": req.schema_type
        })
    } else {
        serde_json::json!({ "name": req.name, "revision": req.revision })
    };
    (StatusCode::CREATED, Json(body)).into_response()
}

pub async fn upload_create_schema_handler(
    State(state): State<AppState>,
    Path((schema_type, name)): Path<(String, String)>,
    headers: HeaderMap,
    multipart: Result<Multipart, MultipartRejection>,
) -> Response {
    if let Err(err) = validate_resource_id(ResourceIdKind::SchemaName, &name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    if schema_type.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "schema type must not be empty").into_response();
    }
    if schema_type == "json" {
        return (
            StatusCode::BAD_REQUEST,
            "schema type 'json' does not support file upload",
        )
            .into_response();
    }
    let mut multipart = match required_multipart(&headers, multipart) {
        Ok(multipart) => multipart,
        Err(response) => return *response,
    };

    let mut upload: Option<TemporaryUpload> = None;
    let mut props: Option<JsonMap<String, JsonValue>> = None;
    let mut form_name: Option<String> = None;
    let mut revision: Option<u64> = None;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(err) => {
                let err = crate::streaming_upload::UploadFailure::multipart(
                    "read multipart request",
                    err,
                );
                return (err.status, err.message).into_response();
            }
        };
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "file" => {
                if upload.is_some() {
                    return (StatusCode::BAD_REQUEST, "field 'file' must not be repeated")
                        .into_response();
                }
                if !is_zip_filename(field.file_name()) {
                    return (
                        StatusCode::BAD_REQUEST,
                        "field 'file' must have a .zip filename",
                    )
                        .into_response();
                }
                upload = match TemporaryUpload::receive(&state.storage, field, Some("zip")).await {
                    Ok(upload) => Some(upload),
                    Err(err) => return (err.status, err.message).into_response(),
                };
            }
            "props" => {
                if props.is_some() {
                    return (
                        StatusCode::BAD_REQUEST,
                        "field 'props' must not be repeated",
                    )
                        .into_response();
                }
                let text = match read_text_field(field, "props").await {
                    Ok(text) => text,
                    Err(err) => return (err.status, err.message).into_response(),
                };
                let value: JsonValue = match serde_json::from_str(&text) {
                    Ok(value) => value,
                    Err(err) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            format!("field 'props' must be valid JSON: {err}"),
                        )
                            .into_response();
                    }
                };
                props = match value {
                    JsonValue::Object(props) => Some(props),
                    _ => {
                        return (
                            StatusCode::BAD_REQUEST,
                            "field 'props' must be a JSON object",
                        )
                            .into_response();
                    }
                };
            }
            "name" => {
                if form_name.is_some() {
                    return (StatusCode::BAD_REQUEST, "field 'name' must not be repeated")
                        .into_response();
                }
                form_name = match read_text_field(field, "name").await {
                    Ok(value) => Some(value),
                    Err(err) => return (err.status, err.message).into_response(),
                };
            }
            "revision" => {
                if revision.is_some() {
                    return (
                        StatusCode::BAD_REQUEST,
                        "field 'revision' must not be repeated",
                    )
                        .into_response();
                }
                let text = match read_text_field(field, "revision").await {
                    Ok(value) => value,
                    Err(err) => return (err.status, err.message).into_response(),
                };
                revision = match text.parse::<u64>() {
                    Ok(value) => match crate::revision::validate_revision(value) {
                        Ok(()) => Some(value),
                        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
                    },
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            "field 'revision' must be a positive JSON safe integer",
                        )
                            .into_response();
                    }
                };
            }
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("unknown multipart field '{field_name}'"),
                )
                    .into_response();
            }
        }
    }

    if form_name.as_deref().is_some_and(|value| value != name) {
        return (
            StatusCode::BAD_REQUEST,
            format!("field 'name' must match path schema name '{name}'"),
        )
            .into_response();
    }
    let upload = match upload {
        Some(upload) => upload,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "field 'file' is required and must not be empty",
            )
                .into_response();
        }
    };
    let revision = match revision {
        Some(revision) => revision,
        None => {
            return (StatusCode::BAD_REQUEST, "field 'revision' is required").into_response();
        }
    };
    let mut props = props.unwrap_or_default();
    if props.contains_key("proto_path") || props.contains_key("schema_path") {
        return (
            StatusCode::BAD_REQUEST,
            "field 'props' must not contain 'proto_path' or 'schema_path'",
        )
            .into_response();
    }
    let path_key = if schema_type == "proto" {
        "proto_path"
    } else {
        "schema_path"
    };
    props.insert(
        path_key.to_string(),
        JsonValue::String(upload.path().to_string_lossy().into_owned()),
    );

    let _permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another import/export/upload operation is in progress",
            )
                .into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "operation guard closed").into_response();
        }
    };
    create_schema_locked(
        &state,
        CreateSchemaRequest {
            name,
            revision,
            schema_type,
            props,
        },
        true,
    )
    .await
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
    if let Err(err) = validate_resource_id(ResourceIdKind::SchemaName, &name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let name = name.as_str();
    match state.storage.get_schema(name) {
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
    if let Err(err) = validate_resource_id(ResourceIdKind::SchemaName, &name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let _permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another import/export/upload operation is in progress",
            )
                .into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "operation guard closed").into_response();
        }
    };
    let mut audit = ResourceMutationLog::new("schema", "delete", &name, None);
    let stored_schema = match state.storage.get_schema(&name) {
        Ok(stored) => stored,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to get schema {name}: {err}"),
            )
                .into_response();
        }
    };
    if let Some(stored) = &stored_schema {
        audit.set_revision(Some(stored.revision));
    }

    // Check if any persisted stream or table references this schema.
    let (referencing_streams, referencing_tables) =
        match find_resources_referencing_schema(&state, &name) {
            Ok(references) => references,
            Err(err) => {
                audit.log_failure(&err);
                return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
            }
        };
    if !referencing_streams.is_empty() || !referencing_tables.is_empty() {
        let mut references = Vec::new();
        if !referencing_streams.is_empty() {
            references.push(format!("streams: {}", referencing_streams.join(", ")));
        }
        if !referencing_tables.is_empty() {
            references.push(format!("tables: {}", referencing_tables.join(", ")));
        }
        let err = format!(
            "schema {name} is still referenced by {}",
            references.join("; ")
        );
        audit.log_failure(&err);
        return (StatusCode::CONFLICT, err).into_response();
    }

    match state.storage.delete_schema(&name) {
        Ok(()) => {
            if let Some(stored) = stored_schema {
                delete_installed_source(&state.storage, &name, &stored.schema_type);
            }
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
        revision: stored.revision,
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

fn find_resources_referencing_schema(
    state: &AppState,
    schema_name: &str,
) -> Result<(Vec<String>, Vec<String>), String> {
    #[derive(Deserialize)]
    struct SchemaRef {
        #[serde(rename = "ref")]
        r#ref: Option<String>,
    }
    #[derive(Deserialize)]
    struct ResourceSchemaCheck {
        schema: SchemaRef,
        name: String,
    }

    let streams = state
        .storage
        .list_streams()
        .map_err(|e| format!("list streams: {e}"))?;
    let mut referencing_streams = Vec::new();
    for stream in streams {
        match serde_json::from_str::<ResourceSchemaCheck>(&stream.raw_json) {
            Ok(check) if check.schema.r#ref.as_deref().map(|s| s.trim()) == Some(schema_name) => {
                referencing_streams.push(check.name);
            }
            _ => {}
        }
    }

    let tables = state
        .storage
        .list_tables()
        .map_err(|e| format!("list tables: {e}"))?;
    let mut referencing_tables = Vec::new();
    for table in tables {
        match serde_json::from_str::<ResourceSchemaCheck>(&table.raw_json) {
            Ok(check) if check.schema.r#ref.as_deref().map(|s| s.trim()) == Some(schema_name) => {
                referencing_tables.push(check.name);
            }
            _ => {}
        }
    }
    Ok((referencing_streams, referencing_tables))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::extract::Path;
    use std::io::{Cursor, Write};
    use storage::{StorageManager, StoredSchema, StoredTable};
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn build_state(temp_dir: &TempDir) -> AppState {
        let storage = StorageManager::new(temp_dir.path()).expect("create storage");
        AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![crate::FlowInstanceSpec {
                id: "default".to_string(),
                ..crate::FlowInstanceSpec::default()
            }],
            0,
        )
        .expect("build app state")
    }

    fn proto_zip() -> Vec<u8> {
        proto_zip_with_field("value")
    }

    fn proto_zip_with_field(field_name: &str) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "sensor.proto",
                zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Deflated),
            )
            .expect("start proto entry");
        writer
            .write_all(
                format!("syntax = \"proto3\"; message Sensor {{ int64 {field_name} = 1; }}")
                    .as_bytes(),
            )
            .expect("write proto entry");
        writer.finish().expect("finish ZIP").into_inner()
    }

    fn schema_upload_request(name: &str) -> axum::http::Request<axum::body::Body> {
        schema_upload_request_with_zip(name, 1, proto_zip())
    }

    fn schema_upload_request_with_zip(
        name: &str,
        revision: u64,
        zip: Vec<u8>,
    ) -> axum::http::Request<axum::body::Body> {
        let boundary = "schema_upload_test_boundary";
        let mut body = Vec::new();
        write!(
            &mut body,
            "--{boundary}\r\nContent-Disposition: form-data; name=\"props\"\r\n\r\n{{\"message_type\":\"Sensor\"}}\r\n"
        )
        .expect("write props field");
        write!(
            &mut body,
            "--{boundary}\r\nContent-Disposition: form-data; name=\"revision\"\r\n\r\n{revision}\r\n"
        )
        .expect("write revision field");
        write!(
            &mut body,
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"sensor.zip\"\r\nContent-Type: application/zip\r\n\r\n"
        )
        .expect("write file header");
        body.extend_from_slice(&zip);
        write!(&mut body, "\r\n--{boundary}--\r\n").expect("write multipart trailer");
        axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri(format!("/schemas/proto/{name}/upload"))
            .header(
                axum::http::header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .expect("build schema upload request")
    }

    #[tokio::test]
    async fn upload_schema_creates_from_zip_and_cleans_temporary_file() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);
        let name = "UploadedSchema";

        let response = crate::build_app(state.clone())
            .oneshot(schema_upload_request(name))
            .await
            .expect("send schema upload request");
        assert_eq!(response.status(), StatusCode::CREATED);
        let stored = state
            .storage
            .get_schema(name)
            .expect("read stored schema")
            .expect("schema exists");
        assert_eq!(stored.schema_type, "proto");
        assert!(
            state
                .storage
                .schemas_dir()
                .join("proto")
                .join(name)
                .join("sensor.proto")
                .is_file()
        );
        assert!(
            std::fs::read_dir(state.storage.uploads_tmp_dir())
                .expect("read temporary upload directory")
                .next()
                .is_none()
        );

        let conflict = crate::build_app(state.clone())
            .oneshot(schema_upload_request(name))
            .await
            .expect("send conflicting schema upload request");
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert!(
            std::fs::read_dir(state.storage.uploads_tmp_dir())
                .expect("read temporary upload directory")
                .next()
                .is_none()
        );
        named_schema_store().remove(name);
    }

    #[tokio::test]
    async fn create_schema_rejects_invalid_name() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = create_schema_handler(
            State(state.clone()),
            Json(CreateSchemaRequest {
                name: "bad-schema".to_string(),
                revision: 1,
                schema_type: "json".to_string(),
                props: JsonMap::new(),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let message = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(message.contains("schema name"), "got: {message}");
        assert!(
            state
                .storage
                .get_schema("bad-schema")
                .expect("read schema")
                .is_none(),
            "invalid name must not persist",
        );
    }

    #[tokio::test]
    async fn update_schema_refreshes_stream_and_restarts_dependent_pipeline() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);
        let schema_name = "CascadeSchema";
        let stream_name = "cascade_schema_stream";
        let pipeline_id = "cascade_schema_pipeline";

        create_schema_handler(
            State(state.clone()),
            Json(CreateSchemaRequest {
                name: schema_name.to_string(),
                revision: 1,
                schema_type: "json".to_string(),
                props: serde_json::json!({
                    "columns": [{ "name": "old_value", "data_type": "int64" }]
                })
                .as_object()
                .expect("schema props object")
                .clone(),
            }),
        )
        .await;

        let stream_request = crate::stream::CreateStreamRequest {
            name: stream_name.to_string(),
            revision: 1,
            stream_type: "mock".to_string(),
            schema: crate::stream::SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: JsonMap::new(),
                r#ref: Some(schema_name.to_string()),
            },
            props: crate::stream::StreamPropsRequest::default(),
            shared: false,
            decoder: crate::stream::DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        };
        let stream_response =
            crate::stream::create_stream_handler(State(state.clone()), Json(stream_request))
                .await
                .into_response();
        assert_eq!(stream_response.status(), StatusCode::CREATED);

        let pipeline_request: crate::pipeline::CreatePipelineRequest =
            serde_json::from_value(serde_json::json!({
                "id": pipeline_id,
                "revision": 1,
                "flow_instance_id": "default",
                "sql": format!("SELECT * FROM {stream_name}"),
                "sinks": [{
                    "id": "sink",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": { "type": "json", "props": {} }
                }]
            }))
            .expect("decode pipeline request");
        let instance = state.local_instance("default").expect("default instance");
        let definition = crate::pipeline::build_pipeline_definition(
            &pipeline_request,
            instance.encoder_registry().as_ref(),
            instance.as_ref(),
        )
        .expect("build pipeline definition");
        state
            .storage
            .create_pipeline(
                crate::storage_bridge::stored_pipeline_from_request(&pipeline_request)
                    .expect("serialize pipeline"),
            )
            .expect("store pipeline");
        instance
            .create_pipeline(flow::CreatePipelineRequest::new(definition))
            .expect("create pipeline runtime");
        instance
            .start_pipeline(pipeline_id)
            .await
            .expect("start pipeline runtime");
        state
            .storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: pipeline_id.to_string(),
                desired_state: storage::StoredPipelineDesiredState::Running,
            })
            .expect("persist running desired state");

        let response = update_schema_handler(
            State(state.clone()),
            Path(schema_name.to_string()),
            Query(UpdateSchemaQuery {
                restart_pipelines: true,
            }),
            Json(CreateSchemaRequest {
                name: schema_name.to_string(),
                revision: 2,
                schema_type: "json".to_string(),
                props: serde_json::json!({
                    "columns": [{ "name": "new_value", "data_type": "float64" }]
                })
                .as_object()
                .expect("schema props object")
                .clone(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read update response body");
        let body: JsonValue = serde_json::from_slice(&body).expect("decode update response");
        assert_eq!(body["revision"], 2);
        assert_eq!(body["pipeline_restart"]["requested"], true);
        assert_eq!(body["pipeline_restart"]["results"][0]["id"], pipeline_id);
        assert_eq!(
            body["pipeline_restart"]["results"][0]["status"],
            "restarted"
        );

        let schema = state
            .storage
            .get_schema(schema_name)
            .expect("read schema")
            .expect("schema exists");
        assert_eq!(schema.revision, 2);
        let stream = instance
            .get_stream(stream_name)
            .await
            .expect("read refreshed stream");
        assert_eq!(
            stream.definition.schema().column_schemas()[0].name,
            "new_value"
        );
        assert!(matches!(
            stream.definition.schema().column_schemas()[0].data_type,
            flow::ConcreteDatatype::Float64(_)
        ));
        assert_eq!(
            instance
                .list_pipelines()
                .into_iter()
                .find(|snapshot| snapshot.definition.id() == pipeline_id)
                .map(|snapshot| snapshot.status),
            Some(flow::pipeline::PipelineStatus::Running)
        );

        named_schema_store().remove(schema_name);
    }

    #[tokio::test]
    async fn update_file_backed_proto_schema_refreshes_stream_and_restarts_pipeline() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);
        let schema_name = "CascadeProtoSchema";
        let stream_name = "cascade_proto_stream";
        let pipeline_id = "cascade_proto_pipeline";

        let create_response = crate::build_app(state.clone())
            .oneshot(schema_upload_request_with_zip(
                schema_name,
                1,
                proto_zip_with_field("old_value"),
            ))
            .await
            .expect("send proto schema upload request");
        assert_eq!(create_response.status(), StatusCode::CREATED);

        let stream_request = crate::stream::CreateStreamRequest {
            name: stream_name.to_string(),
            revision: 1,
            stream_type: "mock".to_string(),
            schema: crate::stream::SchemaConfigRequest {
                schema_type: "proto".to_string(),
                props: JsonMap::new(),
                r#ref: Some(schema_name.to_string()),
            },
            props: crate::stream::StreamPropsRequest::default(),
            shared: false,
            decoder: crate::stream::DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        };
        let stream_response =
            crate::stream::create_stream_handler(State(state.clone()), Json(stream_request))
                .await
                .into_response();
        assert_eq!(stream_response.status(), StatusCode::CREATED);

        let pipeline_request: crate::pipeline::CreatePipelineRequest =
            serde_json::from_value(serde_json::json!({
                "id": pipeline_id,
                "revision": 1,
                "flow_instance_id": "default",
                "sql": format!("SELECT * FROM {stream_name}"),
                "sinks": [{
                    "id": "sink",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": { "type": "json", "props": {} }
                }]
            }))
            .expect("decode pipeline request");
        let instance = state.local_instance("default").expect("default instance");
        let definition = crate::pipeline::build_pipeline_definition(
            &pipeline_request,
            instance.encoder_registry().as_ref(),
            instance.as_ref(),
        )
        .expect("build pipeline definition");
        state
            .storage
            .create_pipeline(
                crate::storage_bridge::stored_pipeline_from_request(&pipeline_request)
                    .expect("serialize pipeline"),
            )
            .expect("store pipeline");
        instance
            .create_pipeline(flow::CreatePipelineRequest::new(definition))
            .expect("create pipeline runtime");
        instance
            .start_pipeline(pipeline_id)
            .await
            .expect("start pipeline runtime");
        state
            .storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: pipeline_id.to_string(),
                desired_state: storage::StoredPipelineDesiredState::Running,
            })
            .expect("persist running desired state");

        let updated_archive = temp_dir.path().join("updated.proto.zip");
        std::fs::write(&updated_archive, {
            let zip = proto_zip_with_field("new_value");
            zip
        })
        .expect("write updated proto archive");
        let response = update_schema_handler(
            State(state.clone()),
            Path(schema_name.to_string()),
            Query(UpdateSchemaQuery {
                restart_pipelines: true,
            }),
            Json(CreateSchemaRequest {
                name: schema_name.to_string(),
                revision: 2,
                schema_type: "proto".to_string(),
                props: serde_json::json!({
                    "proto_path": updated_archive,
                    "message_type": "Sensor"
                })
                .as_object()
                .expect("schema props object")
                .clone(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read update response body");
        let body: JsonValue = serde_json::from_slice(&body).expect("decode update response");
        assert_eq!(body["revision"], 2);
        assert_eq!(body["pipeline_restart"]["results"][0]["id"], pipeline_id);
        assert_eq!(
            body["pipeline_restart"]["results"][0]["status"],
            "restarted"
        );

        let stream = instance
            .get_stream(stream_name)
            .await
            .expect("read refreshed proto stream");
        assert_eq!(
            stream.definition.schema().column_schemas()[0].name,
            "new_value"
        );
        assert_eq!(
            instance
                .list_pipelines()
                .into_iter()
                .find(|snapshot| snapshot.definition.id() == pipeline_id)
                .map(|snapshot| snapshot.status),
            Some(flow::pipeline::PipelineStatus::Running)
        );

        named_schema_store().remove(schema_name);
    }

    #[tokio::test]
    async fn get_schema_rejects_invalid_path_name() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = get_schema_handler(State(state), Path("bad-schema".to_string()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let message = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(message.contains("schema name"), "got: {message}");
    }

    #[tokio::test]
    async fn delete_schema_rejects_table_reference() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);
        let schema_name = "ReferencedSchema";
        state
            .storage
            .create_schema(StoredSchema {
                name: schema_name.to_string(),
                revision: 1,
                schema_type: "json".to_string(),
                props_json: "{}".to_string(),
            })
            .expect("persist schema");
        state
            .storage
            .create_table(StoredTable {
                id: "referencing_table".to_string(),
                revision: 1,
                raw_json: serde_json::json!({
                    "name": "referencing_table",
                    "type": "history",
                    "schema": { "ref": schema_name },
                    "props": {
                        "datasource": "datasource",
                        "topic": "topic"
                    }
                })
                .to_string(),
            })
            .expect("persist table");

        let response = delete_schema_handler(State(state.clone()), Path(schema_name.to_string()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let message = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(
            message.contains("tables: referencing_table"),
            "got: {message}"
        );
        assert!(
            state
                .storage
                .get_schema(schema_name)
                .expect("read schema")
                .is_some()
        );
    }
}
