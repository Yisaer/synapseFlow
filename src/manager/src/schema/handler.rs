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
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use crate::stream::{named_schema_store, schema_registry};
use storage::{StorageError, StoredSchema};

/// Schema props keys that may reference a local file path.
const PROTO_PATH_KEY: &str = "proto_path";
const DBC_SCHEMA_PATH_KEY: &str = "schema_path";

/// Resolve file-path props through the uploads directory before falling back to
/// the raw value. For any non-empty value, try `data_dir/uploads/<value>` first;
/// if that file exists, replace with the full path. Otherwise leave the value
/// unchanged (backward compatible with local filesystem paths).
fn resolve_prop_paths(props: &mut serde_json::Map<String, serde_json::Value>, state: &AppState) {
    let keys: &[&str] = &[PROTO_PATH_KEY, DBC_SCHEMA_PATH_KEY];
    for key in keys {
        let Some(val) = props.get(*key).and_then(|v| v.as_str()) else {
            continue;
        };
        let trimmed = val.trim();
        if trimmed.is_empty() {
            continue;
        }
        let uploads_path = state.storage.uploads_dir().join(trimmed);
        if uploads_path.exists() {
            props.insert(
                (*key).to_string(),
                serde_json::Value::String(uploads_path.to_string_lossy().into_owned()),
            );
        }
    }
}

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

    let mut props = req.props.clone();
    resolve_prop_paths(&mut props, &state);

    let (schema, proto_bundle) = match schema_registry().parse(&req.schema_type, &name, &props) {
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

    if let Some(bundle) = proto_bundle {
        named_schema_store().insert_with_bundle(name, schema, (*bundle).clone());
    } else {
        named_schema_store().insert(name, schema);
    }
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
    let audit = ResourceMutationLog::new("schema", "delete", &name, None);

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
            Ok(check) if check.schema.r#ref.as_deref().map(|s| s.trim()) == Some(schema_name) => {
                referencing.push(check.name);
            }
            _ => {}
        }
    }
    Ok(referencing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::extract::Path;
    use storage::StorageManager;
    use tempfile::TempDir;

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

    #[tokio::test]
    async fn create_schema_rejects_invalid_name() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = create_schema_handler(
            State(state.clone()),
            Json(CreateSchemaRequest {
                name: "bad-schema".to_string(),
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

    #[test]
    fn resolve_prop_path_replaces_when_upload_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = build_state(&temp_dir);
        state
            .storage
            .save_upload("sensor.proto", b"syntax = \"proto3\";")
            .unwrap();

        let mut props = serde_json::Map::new();
        props.insert(
            "proto_path".to_string(),
            serde_json::Value::String("sensor.proto".to_string()),
        );
        resolve_prop_paths(&mut props, &state);

        let resolved = props["proto_path"].as_str().unwrap();
        assert!(
            resolved.contains("uploads/sensor.proto"),
            "expected path to contain uploads/sensor.proto, got: {resolved}"
        );
    }

    #[test]
    fn resolve_prop_path_keeps_original_when_upload_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = build_state(&temp_dir);

        let mut props = serde_json::Map::new();
        props.insert(
            "proto_path".to_string(),
            serde_json::Value::String("not_uploaded.proto".to_string()),
        );
        resolve_prop_paths(&mut props, &state);

        assert_eq!(props["proto_path"].as_str().unwrap(), "not_uploaded.proto");
    }

    #[test]
    fn resolve_prop_path_falls_back_when_upload_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = build_state(&temp_dir);

        let mut props = serde_json::Map::new();
        props.insert(
            "proto_path".to_string(),
            serde_json::Value::String("/etc/veloflux/sensor.proto".to_string()),
        );
        resolve_prop_paths(&mut props, &state);

        // Neither uploads/etc/veloflux/sensor.proto nor /etc/veloflux/sensor.proto exists,
        // so the value stays unchanged for the proto parser to try as a local path.
        assert_eq!(
            props["proto_path"].as_str().unwrap(),
            "/etc/veloflux/sensor.proto"
        );
    }

    #[test]
    fn resolve_prop_path_works_with_subdirectory_upload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = build_state(&temp_dir);
        state
            .storage
            .save_upload("proto/sensor.proto", b"syntax = \"proto3\";")
            .unwrap();

        let mut props = serde_json::Map::new();
        props.insert(
            "proto_path".to_string(),
            serde_json::Value::String("proto/sensor.proto".to_string()),
        );
        resolve_prop_paths(&mut props, &state);

        let resolved = props["proto_path"].as_str().unwrap();
        assert!(
            resolved.contains("uploads/proto/sensor.proto"),
            "expected path to contain uploads/proto/sensor.proto, got: {resolved}"
        );
    }
}
