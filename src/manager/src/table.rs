use crate::audit::ResourceMutationLog;
use crate::pipeline::AppState;
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use crate::stream::{
    DecoderConfigRequest, ResolvedSchema, SchemaConfigRequest, stream_column_info,
};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::DecoderRegistry;
use flow::catalog::{HistoryTableProps, StreamDecoderConfig, TableDefinition, TableProps};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;
use storage::StorageError;
use tokio::sync::TryAcquireError;

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateTableRequest {
    pub name: String,
    #[serde(deserialize_with = "crate::revision::deserialize_revision")]
    pub revision: u64,
    #[serde(rename = "type")]
    pub table_type: String,
    #[serde(default)]
    pub schema: SchemaConfigRequest,
    #[serde(default)]
    pub props: TablePropsRequest,
    #[serde(default)]
    pub decoder: DecoderConfigRequest,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct TablePropsRequest {
    #[serde(flatten)]
    pub fields: JsonMap<String, JsonValue>,
}

impl TablePropsRequest {
    fn to_value(&self) -> JsonValue {
        JsonValue::Object(self.fields.clone())
    }
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct HistoryTablePropsRequest {
    pub datasource: Option<String>,
    pub topic: Option<String>,
    pub time_column: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Serialize)]
pub struct TableInfo {
    pub name: String,
    pub revision: u64,
    #[serde(rename = "type")]
    pub table_type: String,
    pub schema: crate::stream::StreamSchemaInfo,
}

fn table_busy_response(name: &str) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        format!("table {name} is busy processing another command"),
    )
        .into_response()
}

pub(crate) fn build_table_props(
    table_type: &str,
    props: &TablePropsRequest,
) -> Result<TableProps, String> {
    match table_type.to_ascii_lowercase().as_str() {
        "history" => {
            let history_props: HistoryTablePropsRequest = serde_json::from_value(props.to_value())
                .map_err(|err| format!("invalid history table props: {err}"))?;
            let datasource = history_props
                .datasource
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "history table requires datasource".to_string())?;
            let topic = history_props
                .topic
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "history table requires topic".to_string())?;
            let time_column = history_props
                .time_column
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "ts".to_string());
            if let Some(batch_size) = history_props.batch_size
                && batch_size == 0
            {
                return Err("history table batch_size must be greater than 0".to_string());
            }
            Ok(TableProps::History(HistoryTableProps {
                datasource,
                topic,
                time_column,
                batch_size: history_props.batch_size,
            }))
        }
        other => Err(format!("unsupported table type: {other}")),
    }
}

pub(crate) fn build_table_decoder(
    req: &CreateTableRequest,
    decoder_registry: &DecoderRegistry,
    resolved_schema: &ResolvedSchema,
) -> Result<StreamDecoderConfig, String> {
    let decoder_config = req.decoder.clone();
    if decoder_config.decode_type == "none" {
        return Ok(StreamDecoderConfig::new(
            decoder_config.decode_type,
            decoder_config.props,
        ));
    }
    if !decoder_registry.is_registered(&decoder_config.decode_type) {
        return Err(format!(
            "decoder kind `{}` not registered",
            decoder_config.decode_type
        ));
    }
    let mut config =
        StreamDecoderConfig::new(decoder_config.decode_type.clone(), decoder_config.props);
    if config.kind().eq_ignore_ascii_case("protobuf")
        && let Some(bundle) = &resolved_schema.proto_bundle
    {
        config = config.with_proto_bundle(Arc::clone(bundle));
    }
    if let Some(artifact) = &resolved_schema.artifact {
        config = config.with_schema_artifact(Arc::clone(artifact));
    }
    Ok(config)
}

pub(crate) async fn refresh_table_runtime(
    state: &AppState,
    table_name: &str,
) -> Result<(), String> {
    let stored = state
        .storage
        .get_table(table_name)
        .map_err(|err| format!("failed to read table {table_name}: {err}"))?
        .ok_or_else(|| format!("table {table_name} not found"))?;
    let definition = crate::storage_bridge::table_definition_from_stored(
        &stored,
        state
            .instances
            .default_instance()
            .decoder_registry()
            .as_ref(),
    )?;
    for (_, instance) in state.instances.instances_snapshot() {
        match instance.delete_table(table_name).await {
            Ok(())
            | Err(flow::FlowInstanceError::Catalog(flow::catalog::CatalogError::TableNotFound(
                _,
            ))) => {}
            Err(err) => return Err(format!("failed to remove table {table_name}: {err}")),
        }
        instance
            .create_table(definition.clone())
            .await
            .map_err(|err| format!("failed to refresh table {table_name}: {err}"))?;
    }
    Ok(())
}

pub async fn create_table_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateTableRequest>,
) -> impl IntoResponse {
    let audit = ResourceMutationLog::new("table", "create", req.name.as_str(), Some(req.revision));
    if let Err(err) = validate_resource_id(ResourceIdKind::StreamName, &req.name) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let _storage_permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another storage operation is in progress",
            )
                .into_response();
        }
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation guard closed",
            )
                .into_response();
        }
    };
    let _table_permit = match state.try_acquire_stream_op(&req.name).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return table_busy_response(&req.name),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "table operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let resolved_schema = match resolve_schema_from_request_for_table(&req) {
        Ok(schema) => schema,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let table_props = match build_table_props(&req.table_type, &req.props) {
        Ok(props) => props,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let decoder_registry = state.instances.default_instance().decoder_registry();
    let decoder = match build_table_decoder(&req, decoder_registry.as_ref(), &resolved_schema) {
        Ok(config) => config,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let stored = match crate::storage_bridge::stored_table_from_request(&req) {
        Ok(stored) => stored,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    match state.storage.create_table(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("table {} already exists", req.name),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist table {}: {err}", req.name),
            )
                .into_response();
        }
    }

    let definition = TableDefinition::new(
        req.name.clone(),
        Arc::clone(&resolved_schema.logical_schema),
        table_props,
        decoder,
    );

    for (_, instance) in state.instances.instances_snapshot() {
        if let Err(err) = instance.create_table(definition.clone()).await {
            let _ = state.storage.delete_table(&req.name);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to create table {}: {err}", req.name),
            )
                .into_response();
        }
    }

    audit.log_success();
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "name": req.name,
            "revision": req.revision,
        })),
    )
        .into_response()
}

pub async fn list_tables(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_tables() {
        Ok(entries) => {
            let mut result = Vec::new();
            for entry in entries {
                let req = match crate::storage_bridge::table_request_from_stored(&entry) {
                    Ok(req) => req,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored table {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let schema = match resolve_schema_from_request_for_table(&req) {
                    Ok(resolved) => resolved.logical_schema,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("build schema for table {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let columns = schema
                    .column_schemas()
                    .iter()
                    .map(|col| stream_column_info(&col.name, &col.data_type))
                    .collect();
                result.push(TableInfo {
                    name: req.name,
                    revision: req.revision,
                    table_type: req.table_type,
                    schema: crate::stream::StreamSchemaInfo { columns },
                });
            }
            Json(result).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list tables: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_table_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_resource_id(ResourceIdKind::StreamName, &name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let mut audit = ResourceMutationLog::new("table", "delete", name.as_str(), None);
    let _storage_permit = match state.try_acquire_storage_operation() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another storage operation is in progress",
            )
                .into_response();
        }
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation guard closed",
            )
                .into_response();
        }
    };
    let _table_permit = match state.try_acquire_stream_op(&name).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return table_busy_response(&name),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "table operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let stored = match state.storage.get_table(&name) {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            let err = format!("table {name} not found");
            audit.log_failure(&err);
            return (StatusCode::NOT_FOUND, err).into_response();
        }
        Err(err) => {
            let err = format!("failed to read table {name} from storage: {err}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };
    audit.set_revision(Some(stored.revision));

    for (_, instance) in state.instances.instances_snapshot() {
        match instance.delete_table(&name).await {
            Ok(()) => {}
            Err(flow::FlowInstanceError::Catalog(flow::catalog::CatalogError::TableNotFound(
                _,
            ))) => {}
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to delete table {name}: {err}"),
                )
                    .into_response();
            }
        }
    }

    match state.storage.delete_table(&name) {
        Ok(()) => {}
        Err(StorageError::NotFound(_)) => {
            let err = format!("table {name} not found");
            audit.log_failure(&err);
            return (StatusCode::NOT_FOUND, err).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("table {name} deleted in runtime but failed to remove from storage: {err}"),
            )
                .into_response();
        }
    }
    audit.log_success();
    (StatusCode::OK, format!("table {name} deleted")).into_response()
}

fn resolve_schema_from_request_for_table(
    req: &CreateTableRequest,
) -> Result<ResolvedSchema, String> {
    if let Some(ref_name) = &req.schema.r#ref {
        validate_resource_id(ResourceIdKind::SchemaName, ref_name)
            .map_err(|err| format!("invalid schema ref: {err}"))?;
        return crate::stream::named_schema_store()
            .get_resolved(ref_name)
            .ok_or_else(|| format!("referenced schema '{}' not found", ref_name));
    }
    if req.schema.props.contains_key("schema_path") || req.schema.props.contains_key("proto_path") {
        return Err(
            "file-backed schemas must be installed with POST /schemas and referenced by schema ID"
                .to_string(),
        );
    }
    let (schema, bundle, artifact) = crate::stream::schema_registry().parse(
        &req.schema.schema_type,
        &req.name,
        &req.schema.props,
    )?;
    Ok(ResolvedSchema::new(schema, bundle, artifact))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_FLOW_INSTANCE_ID;
    use crate::instances::FlowInstanceSpec;
    use crate::pipeline::AppState;
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use serde_json::{Value as JsonValue, json};
    use storage::StorageManager;
    use tempfile::TempDir;

    fn sample_default_instance_spec() -> FlowInstanceSpec {
        FlowInstanceSpec {
            id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..FlowInstanceSpec::default()
        }
    }

    fn build_state(temp_dir: &TempDir, flow_instances: Vec<FlowInstanceSpec>) -> AppState {
        let storage = StorageManager::new(temp_dir.path()).expect("create storage");
        AppState::new(
            crate::new_default_flow_instance(),
            storage,
            flow_instances,
            0,
        )
        .expect("build app state")
    }

    fn history_table_request(name: &str) -> CreateTableRequest {
        let schema_props: JsonMap<String, JsonValue> = json!({
            "columns": [
                { "name": "value", "data_type": "int64" },
                { "name": "ts", "data_type": "timestamp" }
            ]
        })
        .as_object()
        .expect("schema props object")
        .clone();

        let props_fields: JsonMap<String, JsonValue> = json!({
            "datasource": "my_datasource",
            "topic": "my_topic",
            "time_column": "ts",
            "batch_size": 100
        })
        .as_object()
        .expect("props object")
        .clone();

        CreateTableRequest {
            name: name.to_string(),
            revision: 1,
            table_type: "history".to_string(),
            schema: SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: schema_props,
                r#ref: None,
            },
            props: TablePropsRequest {
                fields: props_fields,
            },
            decoder: DecoderConfigRequest::default(),
        }
    }

    #[tokio::test]
    async fn create_table_handler_persists_and_creates() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let req = history_table_request("test_history_table");

        let response = create_table_handler(State(state.clone()), Json(req.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode response");
        assert_eq!(json["name"], "test_history_table");
        assert_eq!(json["revision"], 1);

        // Verify stored
        let stored = state
            .storage
            .get_table("test_history_table")
            .unwrap()
            .expect("stored table");
        assert_eq!(stored.revision, 1);
        assert!(!stored.raw_json.contains("\"revision\""));

        // Verify in catalog
        let instance = state.instances.default_instance();
        let table = instance.get_table("test_history_table").await.unwrap();
        assert!(table.is_some());
    }

    #[tokio::test]
    async fn create_table_rejects_duplicate() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let req = history_table_request("dup_table");

        let first = create_table_handler(State(state.clone()), Json(req.clone()))
            .await
            .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let second = create_table_handler(State(state.clone()), Json(req.clone()))
            .await
            .into_response();
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn table_mutations_reject_active_storage_operation() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let existing = history_table_request("existing_table");
        let response = create_table_handler(State(state.clone()), Json(existing))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);

        let _storage_permit = state
            .try_acquire_storage_operation()
            .expect("acquire storage operation");

        let create_response = create_table_handler(
            State(state.clone()),
            Json(history_table_request("blocked_table")),
        )
        .await
        .into_response();
        assert_eq!(create_response.status(), StatusCode::CONFLICT);

        let delete_response =
            delete_table_handler(State(state.clone()), Path("existing_table".to_string()))
                .await
                .into_response();
        assert_eq!(delete_response.status(), StatusCode::CONFLICT);
        assert!(
            state
                .storage
                .get_table("existing_table")
                .expect("read existing table")
                .is_some()
        );
    }

    #[tokio::test]
    async fn list_tables_returns_created_tables() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);

        let req_a = history_table_request("table_a");
        let req_b = history_table_request("table_b");

        let _ = create_table_handler(State(state.clone()), Json(req_a))
            .await
            .into_response();
        let _ = create_table_handler(State(state.clone()), Json(req_b))
            .await
            .into_response();

        let response = list_tables(State(state.clone())).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: Vec<JsonValue> = serde_json::from_slice(&body).expect("decode response");
        assert_eq!(json.len(), 2);
        assert_eq!(json[0]["name"], "table_a");
        assert_eq!(json[0]["revision"], 1);
        assert_eq!(json[1]["name"], "table_b");
    }

    #[tokio::test]
    async fn delete_table_removes_from_storage_and_catalog() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![sample_default_instance_spec()]);
        let req = history_table_request("table_to_delete");

        let _ = create_table_handler(State(state.clone()), Json(req))
            .await
            .into_response();

        let response =
            delete_table_handler(State(state.clone()), Path("table_to_delete".to_string()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        assert!(
            state
                .storage
                .get_table("table_to_delete")
                .unwrap()
                .is_none()
        );
        let instance = state.instances.default_instance();
        assert!(
            instance
                .get_table("table_to_delete")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn build_table_props_history_defaults() {
        let props = TablePropsRequest {
            fields: json!({
                "datasource": "ds1",
                "topic": "t1"
            })
            .as_object()
            .expect("props object")
            .clone(),
        };
        let built = build_table_props("history", &props).expect("build history table props");
        let TableProps::History(h) = built;
        assert_eq!(h.datasource, "ds1");
        assert_eq!(h.topic, "t1");
        assert_eq!(h.time_column, "ts");
        assert_eq!(h.batch_size, None);
    }

    #[test]
    fn build_table_props_history_custom_time_column() {
        let props = TablePropsRequest {
            fields: json!({
                "datasource": "ds1",
                "topic": "t1",
                "time_column": "event_time"
            })
            .as_object()
            .expect("props object")
            .clone(),
        };
        let built = build_table_props("history", &props).expect("build history table props");
        let TableProps::History(h) = built;
        assert_eq!(h.time_column, "event_time");
    }

    #[test]
    fn build_table_props_rejects_zero_batch_size() {
        let props = TablePropsRequest {
            fields: json!({
                "datasource": "ds1",
                "topic": "t1",
                "batch_size": 0
            })
            .as_object()
            .expect("props object")
            .clone(),
        };
        let err = build_table_props("history", &props).unwrap_err();
        assert_eq!(err, "history table batch_size must be greater than 0");
    }

    #[test]
    fn build_table_props_rejects_missing_datasource() {
        let props = TablePropsRequest {
            fields: json!({
                "topic": "t1"
            })
            .as_object()
            .expect("props object")
            .clone(),
        };
        let err = build_table_props("history", &props).unwrap_err();
        assert_eq!(err, "history table requires datasource");
    }

    #[test]
    fn build_table_props_rejects_missing_topic() {
        let props = TablePropsRequest {
            fields: json!({
                "datasource": "ds1"
            })
            .as_object()
            .expect("props object")
            .clone(),
        };
        let err = build_table_props("history", &props).unwrap_err();
        assert_eq!(err, "history table requires topic");
    }
}
