use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use flow::connector::{ConnectorError, SharedMqttClientConfig};
use serde::{Deserialize, Serialize};
use storage::StorageError;
use tokio::sync::TryAcquireError;

use crate::audit::ResourceMutationLog;
use crate::pipeline::AppState;
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use crate::storage_bridge::{mqtt_config_from_stored, stored_mqtt_from_config};

#[derive(Clone, Deserialize, Serialize)]
pub struct SharedMqttClientResource {
    #[serde(deserialize_with = "crate::revision::deserialize_revision")]
    pub revision: u64,
    #[serde(flatten)]
    pub definition: SharedMqttClientConfig,
}

fn validate_shared_mqtt_config(cfg: &SharedMqttClientConfig) -> Result<(), String> {
    validate_resource_id(ResourceIdKind::SharedMqttClientKey, &cfg.key)?;
    if cfg.broker_url.trim().is_empty() {
        return Err(format!(
            "shared mqtt client {} broker_url must not be empty",
            cfg.key
        ));
    }
    // Reject credentials embedded in the URL (VF-51 §7.3): they would otherwise
    // land in resource manifests/redb, a scannable surface. Use `username`/`password`.
    if flow::connector::url_has_userinfo(cfg.broker_url.trim()) {
        return Err(format!(
            "shared mqtt client {} broker_url must not embed credentials; use `username`/`password`",
            cfg.key
        ));
    }
    if cfg.topic.trim().is_empty() {
        return Err(format!(
            "shared mqtt client {} topic must not be empty",
            cfg.key
        ));
    }
    if cfg.client_id.trim().is_empty() {
        return Err(format!(
            "shared mqtt client {} client_id must not be empty",
            cfg.key
        ));
    }
    Ok(())
}

pub(crate) fn shared_mqtt_config_eq(
    left: &SharedMqttClientConfig,
    right: &SharedMqttClientConfig,
) -> bool {
    left.key == right.key
        && left.broker_url == right.broker_url
        && left.topic == right.topic
        && left.client_id == right.client_id
        && left.qos == right.qos
        && left.max_packet_size == right.max_packet_size
        && left.protocol_version == right.protocol_version
        && left.username == right.username
        && left.password == right.password
}

fn shared_mqtt_busy_response(key: &str) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        format!("shared mqtt client {key} is busy processing another command"),
    )
        .into_response()
}

pub async fn create_shared_mqtt_client_handler(
    State(state): State<AppState>,
    Json(resource): Json<SharedMqttClientResource>,
) -> impl IntoResponse {
    let revision = resource.revision;
    let req = resource.definition;
    let audit = ResourceMutationLog::new(
        "shared_mqtt_client",
        "create",
        req.key.as_str(),
        Some(revision),
    );
    if let Err(err) = validate_shared_mqtt_config(&req) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let key = req.key.trim().to_string();
    let _permit = match state
        .try_acquire_shared_mqtt_ops(std::iter::once(key.clone()))
        .await
    {
        Ok(permits) => permits,
        Err(TryAcquireError::NoPermits) => return shared_mqtt_busy_response(&key),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "shared mqtt operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    match state.storage.get_mqtt_config(&key) {
        Ok(Some(_)) => {
            let err = format!("shared mqtt client {key} already exists");
            audit.log_failure(&err);
            return (StatusCode::CONFLICT, err).into_response();
        }
        Ok(None) => match state
            .storage
            .create_mqtt_config(stored_mqtt_from_config(&req, revision))
        {
            Ok(()) => {}
            Err(StorageError::AlreadyExists(_)) => {
                let err = format!("shared mqtt client {key} already exists");
                audit.log_failure(&err);
                return (StatusCode::CONFLICT, err).into_response();
            }
            Err(err) => {
                let err = format!("failed to persist shared mqtt client {key}: {err}");
                audit.log_failure(&err);
                return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
            }
        },
        Err(err) => {
            let err = format!("failed to read shared mqtt client {key}: {err}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    }

    let mut created_instances: Vec<std::sync::Arc<flow::FlowInstance>> = Vec::new();
    for (instance_id, instance) in state.instances.instances_snapshot() {
        if let Some(existing) = instance.get_shared_mqtt_client(&key) {
            if !shared_mqtt_config_eq(&existing, &req) {
                for created in &created_instances {
                    let _ = created.drop_shared_mqtt_client(&key);
                }
                let _ = state.storage.delete_mqtt_config(&key);
                let err = format!(
                    "shared mqtt client {key} already exists in runtime instance {instance_id} with different config"
                );
                audit.log_failure(&err);
                return (StatusCode::CONFLICT, err).into_response();
            }
            continue;
        }
        if let Err(err) = instance.create_shared_mqtt_client(req.clone()).await {
            for created in &created_instances {
                let _ = created.drop_shared_mqtt_client(&key);
            }
            let _ = state.storage.delete_mqtt_config(&key);
            let err =
                format!("create shared mqtt client {key} in runtime instance {instance_id}: {err}");
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
        created_instances.push(instance);
    }

    audit.log_success();
    (
        StatusCode::CREATED,
        Json(SharedMqttClientResource {
            revision,
            definition: req,
        }),
    )
        .into_response()
}

pub async fn list_shared_mqtt_clients_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_mqtt_configs() {
        Ok(configs) => {
            let mut items = configs
                .into_iter()
                .map(|stored| SharedMqttClientResource {
                    revision: stored.revision,
                    definition: mqtt_config_from_stored(&stored),
                })
                .collect::<Vec<_>>();
            items.sort_by(|a, b| a.definition.key.cmp(&b.definition.key));
            Json(items).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list shared mqtt clients: {err}"),
        )
            .into_response(),
    }
}

pub async fn get_shared_mqtt_client_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_resource_id(ResourceIdKind::SharedMqttClientKey, &key) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    match state.storage.get_mqtt_config(&key) {
        Ok(Some(stored)) => (
            StatusCode::OK,
            Json(SharedMqttClientResource {
                revision: stored.revision,
                definition: mqtt_config_from_stored(&stored),
            }),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("shared mqtt client {key} not found"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read shared mqtt client {key}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_shared_mqtt_client_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_resource_id(ResourceIdKind::SharedMqttClientKey, &key) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let _permit = match state
        .try_acquire_shared_mqtt_ops(std::iter::once(key.clone()))
        .await
    {
        Ok(permits) => permits,
        Err(TryAcquireError::NoPermits) => return shared_mqtt_busy_response(&key),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "shared mqtt operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let stored = match state.storage.get_mqtt_config(&key) {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                format!("shared mqtt client {key} not found"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read shared mqtt client {key}: {err}"),
            )
                .into_response();
        }
    };
    let audit = ResourceMutationLog::new(
        "shared_mqtt_client",
        "delete",
        key.as_str(),
        Some(stored.revision),
    );

    match state.storage.delete_mqtt_config(&key) {
        Ok(()) => {}
        Err(StorageError::NotFound(_)) => {
            let err = format!("shared mqtt client {key} not found");
            audit.log_failure(&err);
            return (StatusCode::NOT_FOUND, err).into_response();
        }
        Err(err) => {
            let err = format!("failed to delete shared mqtt client {key}: {err}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    }

    // Storage is authoritative; runtime cleanup is best-effort.
    for (instance_id, instance) in state.instances.instances_snapshot() {
        if let Err(err) = instance.drop_shared_mqtt_client(&key) {
            match err {
                flow::FlowInstanceError::Connector(ConnectorError::NotFound(_)) => {}
                other => {
                    tracing::warn!(
                        shared_mqtt_key = %key,
                        flow_instance_id = %instance_id,
                        error = %other,
                        "best-effort delete of shared mqtt client in local runtime failed"
                    );
                }
            }
        }
    }

    audit.log_success();
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::{
        create_shared_mqtt_client_handler, delete_shared_mqtt_client_handler,
        get_shared_mqtt_client_handler, list_shared_mqtt_clients_handler,
    };
    use crate::pipeline::AppState;
    use axum::{
        Json,
        body::to_bytes,
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use flow::connector::SharedMqttClientConfig;
    use serde_json::Value as JsonValue;
    use tempfile::TempDir;

    fn default_flow_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: "default".to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn local_flow_instance_spec(id: &str) -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: id.to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn shared_mqtt_cfg(key: &str) -> SharedMqttClientConfig {
        SharedMqttClientConfig {
            key: key.to_string(),
            broker_url: "tcp://127.0.0.1:1883".to_string(),
            topic: "fleet/+/telemetry".to_string(),
            client_id: format!("client_{key}"),
            qos: 0,
            max_packet_size: None,
            protocol_version: Default::default(),
            username: None,
            password: None,
            resolved_password: None,
        }
    }

    fn resource(definition: SharedMqttClientConfig) -> super::SharedMqttClientResource {
        super::SharedMqttClientResource {
            revision: 1,
            definition,
        }
    }

    fn build_state(temp_dir: &TempDir, flow_instances: Vec<crate::FlowInstanceSpec>) -> AppState {
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        AppState::new(
            crate::new_default_flow_instance(),
            storage,
            flow_instances,
            0,
        )
        .expect("build app state")
    }

    #[tokio::test]
    async fn create_shared_mqtt_client_rejects_blank_required_fields() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);

        let cases = [
            (
                SharedMqttClientConfig {
                    key: "   ".to_string(),
                    ..shared_mqtt_cfg("shared")
                },
                "shared mqtt client key must not be whitespace (expected [A-Za-z][A-Za-z0-9_]{0,127})",
            ),
            (
                SharedMqttClientConfig {
                    broker_url: "   ".to_string(),
                    ..shared_mqtt_cfg("shared")
                },
                "shared mqtt client shared broker_url must not be empty",
            ),
            (
                SharedMqttClientConfig {
                    topic: "   ".to_string(),
                    ..shared_mqtt_cfg("shared")
                },
                "shared mqtt client shared topic must not be empty",
            ),
            (
                SharedMqttClientConfig {
                    client_id: "   ".to_string(),
                    ..shared_mqtt_cfg("shared")
                },
                "shared mqtt client shared client_id must not be empty",
            ),
        ];

        for (cfg, expected) in cases {
            let response =
                create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg)))
                    .await
                    .into_response();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let body = to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("read response body");
            assert_eq!(
                String::from_utf8(body.to_vec()).expect("utf8 body"),
                expected
            );
        }

        assert!(
            state
                .storage
                .list_mqtt_configs()
                .expect("list shared mqtt configs")
                .is_empty(),
            "invalid creates must not persist shared mqtt configs",
        );
    }

    #[tokio::test]
    async fn create_shared_mqtt_client_rejects_invalid_key() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);

        let cfg = SharedMqttClientConfig {
            key: "bad-key".to_string(),
            ..shared_mqtt_cfg("shared")
        };
        let response = create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg)))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let message = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(message.contains("shared mqtt client key"), "got: {message}");
        assert!(
            state
                .storage
                .list_mqtt_configs()
                .expect("list shared mqtt configs")
                .is_empty(),
            "invalid key must not persist",
        );
    }

    // coverage-covers: source.shared_mqtt_client.management
    #[tokio::test]
    async fn create_shared_mqtt_client_conflicts_for_identical_existing_config() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);
        let cfg = shared_mqtt_cfg("shared");

        let first =
            create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg.clone())))
                .await
                .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let second =
            create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg.clone())))
                .await
                .into_response();
        assert_eq!(second.status(), StatusCode::CONFLICT);

        let stored = state
            .storage
            .list_mqtt_configs()
            .expect("list shared mqtt configs");
        assert_eq!(stored.len(), 1);

        let local_instance = state
            .local_instance("default")
            .expect("default local runtime instance");
        let runtime_items = local_instance.list_shared_mqtt_clients();
        assert_eq!(runtime_items.len(), 1);
        assert!(super::shared_mqtt_config_eq(&runtime_items[0], &cfg));
    }

    #[tokio::test]
    async fn create_shared_mqtt_client_conflicts_for_different_config() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);
        let cfg = shared_mqtt_cfg("shared");

        let first =
            create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg.clone())))
                .await
                .into_response();
        assert_eq!(first.status(), StatusCode::CREATED);

        let mut updated = cfg.clone();
        updated.topic = "fleet/+/status".to_string();
        updated.client_id = "client_shared_v2".to_string();
        updated.qos = 1;

        let response = create_shared_mqtt_client_handler(State(state), Json(resource(updated)))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "shared mqtt client shared already exists"
        );
    }

    #[tokio::test]
    async fn create_shared_mqtt_client_rolls_back_storage_on_runtime_install_failure() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);
        let cfg = SharedMqttClientConfig {
            broker_url: "://invalid-url".to_string(),
            ..shared_mqtt_cfg("shared")
        };

        let response = create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg)))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let message = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(
            message.starts_with("create shared mqtt client shared in runtime instance default:"),
            "unexpected runtime failure message: {message}",
        );
        assert!(
            state
                .storage
                .get_mqtt_config("shared")
                .expect("read shared mqtt config")
                .is_none(),
            "runtime install failure must roll back persisted shared mqtt config",
        );
        let local_instance = state
            .local_instance("default")
            .expect("default local runtime instance");
        assert!(
            local_instance.get_shared_mqtt_client("shared").is_none(),
            "runtime install failure must not leave local shared mqtt client behind",
        );
    }

    #[tokio::test]
    async fn create_shared_mqtt_client_rolls_back_earlier_runtime_instances_on_later_failure() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(
            &temp_dir,
            vec![
                default_flow_instance_spec(),
                local_flow_instance_spec("local_b"),
            ],
        );

        let conflicting_cfg = shared_mqtt_cfg("shared");
        let later_instance = state
            .local_instance("local_b")
            .expect("local_b runtime instance");
        later_instance
            .create_shared_mqtt_client(conflicting_cfg.clone())
            .await
            .expect("seed local_b shared mqtt client");

        let mut req = shared_mqtt_cfg("shared");
        req.topic = "fleet/+/status".to_string();

        let response = create_shared_mqtt_client_handler(State(state.clone()), Json(resource(req)))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "shared mqtt client shared already exists in runtime instance local_b with different config"
        );
        assert!(
            state
                .storage
                .get_mqtt_config("shared")
                .expect("read shared mqtt config")
                .is_none(),
            "late runtime conflict must roll back storage write",
        );

        let default_instance = state
            .local_instance("default")
            .expect("default local runtime instance");
        assert!(
            default_instance.get_shared_mqtt_client("shared").is_none(),
            "late runtime conflict must roll back earlier runtime installs",
        );
        let runtime_cfg = later_instance
            .get_shared_mqtt_client("shared")
            .expect("conflicting runtime config still present");
        assert!(super::shared_mqtt_config_eq(&runtime_cfg, &conflicting_cfg));
    }

    #[tokio::test]
    async fn get_shared_mqtt_client_returns_not_found_for_unknown_key() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);

        let response = get_shared_mqtt_client_handler(State(state), Path("missing".to_string()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "shared mqtt client missing not found"
        );
    }

    #[tokio::test]
    async fn list_shared_mqtt_clients_returns_sorted_keys() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);

        for cfg in [shared_mqtt_cfg("shared_b"), shared_mqtt_cfg("shared_a")] {
            state
                .storage
                .create_mqtt_config(crate::storage_bridge::stored_mqtt_from_config(&cfg, 1))
                .expect("seed shared mqtt config");
        }

        let response = list_shared_mqtt_clients_handler(State(state))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode response json");
        let keys = json
            .as_array()
            .expect("shared mqtt client array")
            .iter()
            .map(|item| item["key"].as_str().expect("shared mqtt client key"))
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["shared_a", "shared_b"]);
    }

    #[tokio::test]
    async fn delete_shared_mqtt_client_returns_not_found_when_missing() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);

        let response = delete_shared_mqtt_client_handler(State(state), Path("missing".to_string()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "shared mqtt client missing not found"
        );
    }

    #[tokio::test]
    async fn create_shared_mqtt_client_returns_conflict_when_key_operation_is_busy() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);
        let _permit = state
            .try_acquire_shared_mqtt_ops(std::iter::once("shared".to_string()))
            .await
            .expect("acquire shared mqtt op");

        let response = create_shared_mqtt_client_handler(
            State(state.clone()),
            Json(resource(shared_mqtt_cfg("shared"))),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 body"),
            "shared mqtt client shared is busy processing another command"
        );
        assert!(
            state
                .storage
                .get_mqtt_config("shared")
                .expect("read shared mqtt config")
                .is_none(),
            "busy-key rejection must happen before any storage mutation",
        );
    }

    #[tokio::test]
    async fn delete_shared_mqtt_client_returns_conflict_when_key_operation_is_busy() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir, vec![default_flow_instance_spec()]);

        let cfg = shared_mqtt_cfg("shared");
        let create_resp =
            create_shared_mqtt_client_handler(State(state.clone()), Json(resource(cfg)))
                .await
                .into_response();
        assert_eq!(create_resp.status(), StatusCode::CREATED);

        let local_instance = state
            .local_instance("default")
            .expect("default local runtime instance");
        let _permit = state
            .try_acquire_shared_mqtt_ops(std::iter::once("shared".to_string()))
            .await
            .expect("acquire shared mqtt op");

        let delete_resp =
            delete_shared_mqtt_client_handler(State(state), Path("shared".to_string()))
                .await
                .into_response();
        assert_eq!(delete_resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(delete_resp.into_body(), 64 * 1024)
            .await
            .expect("read delete body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 delete body"),
            "shared mqtt client shared is busy processing another command"
        );
        assert!(
            local_instance.get_shared_mqtt_client("shared").is_some(),
            "busy key conflict must not drop the local shared mqtt client"
        );
    }
}
