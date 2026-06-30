use crate::pipeline::AppState;
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use flow::connector::{DEFAULT_MEMORY_PUBSUB_CAPACITY, MemoryPubSubError, MemoryTopicKind};
use serde::{Deserialize, Serialize};
use storage::{StorageError, StoredMemoryTopic, StoredMemoryTopicKind};

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MemoryTopicKindRequest {
    Bytes,
    Collection,
}

impl MemoryTopicKindRequest {
    fn as_flow_kind(&self) -> MemoryTopicKind {
        match self {
            MemoryTopicKindRequest::Bytes => MemoryTopicKind::Bytes,
            MemoryTopicKindRequest::Collection => MemoryTopicKind::Collection,
        }
    }

    fn as_storage_kind(&self) -> StoredMemoryTopicKind {
        match self {
            MemoryTopicKindRequest::Bytes => StoredMemoryTopicKind::Bytes,
            MemoryTopicKindRequest::Collection => StoredMemoryTopicKind::Collection,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CreateMemoryTopicRequest {
    pub topic: String,
    pub kind: MemoryTopicKindRequest,
    #[serde(default)]
    pub capacity: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct MemoryTopicInfo {
    pub topic: String,
    pub kind: StoredMemoryTopicKind,
    pub capacity: usize,
}

pub async fn create_memory_topic_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateMemoryTopicRequest>,
) -> impl IntoResponse {
    if let Err(err) = validate_resource_id(ResourceIdKind::MemoryTopic, &req.topic) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let topic = req.topic.clone();
    let capacity = req
        .capacity
        .unwrap_or(DEFAULT_MEMORY_PUBSUB_CAPACITY)
        .max(1);

    for (_, instance) in state.instances.instances_snapshot() {
        if let Some(kind) = instance.memory_topic_kind(&topic) {
            if kind != req.kind.as_flow_kind() {
                return (
                    StatusCode::CONFLICT,
                    format!(
                        "memory topic {topic} kind mismatch: expected {}, got {}",
                        kind,
                        req.kind.as_flow_kind()
                    ),
                )
                    .into_response();
            }
            if let Some(actual_capacity) = instance.memory_topic_capacity(&topic)
                && actual_capacity != capacity
            {
                return (
                    StatusCode::CONFLICT,
                    format!(
                        "memory topic {topic} capacity mismatch: expected {actual_capacity}, got {capacity}"
                    ),
                )
                    .into_response();
            }
        }
    }

    let stored = StoredMemoryTopic {
        topic: topic.clone(),
        kind: req.kind.as_storage_kind(),
        capacity,
    };

    match state.storage.create_memory_topic(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("memory topic {topic} already exists"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist memory topic {topic}: {err}"),
            )
                .into_response();
        }
    }

    for (_, instance) in state.instances.instances_snapshot() {
        if let Err(err) = instance.declare_memory_topic(&topic, req.kind.as_flow_kind(), capacity) {
            let _ = state.storage.delete_memory_topic(&topic);
            let (status, message) = match err {
                MemoryPubSubError::InvalidTopic => {
                    (StatusCode::BAD_REQUEST, format!("invalid topic {topic}"))
                }
                MemoryPubSubError::NotFound { .. } => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unexpected memory pubsub error: {err}"),
                ),
                MemoryPubSubError::TimeoutWaitingForSubscribers { .. } => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unexpected memory pubsub error: {err}"),
                ),
                MemoryPubSubError::TopicKindMismatch { .. }
                | MemoryPubSubError::TopicCapacityMismatch { .. } => {
                    (StatusCode::CONFLICT, err.to_string())
                }
            };
            return (status, message).into_response();
        }
    }

    (
        StatusCode::CREATED,
        Json(MemoryTopicInfo {
            topic,
            kind: stored.kind,
            capacity,
        }),
    )
        .into_response()
}

pub async fn list_memory_topics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_memory_topics() {
        Ok(topics) => {
            let mut items = topics
                .into_iter()
                .map(|topic| MemoryTopicInfo {
                    topic: topic.topic,
                    kind: topic.kind,
                    capacity: topic.capacity,
                })
                .collect::<Vec<_>>();
            items.sort_by(|a, b| a.topic.cmp(&b.topic));
            Json(items).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list memory topics: {err}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use serde_json::Value as JsonValue;
    use tempfile::TempDir;

    fn sample_default_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: "default".to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn build_state(temp_dir: &TempDir) -> AppState {
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![sample_default_instance_spec()],
            0,
        )
        .expect("build app state")
    }

    // coverage-covers: stream.memory_topic.lifecycle
    #[tokio::test]
    async fn create_memory_topic_defaults_capacity_when_omitted() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = create_memory_topic_handler(
            State(state.clone()),
            Json(CreateMemoryTopicRequest {
                topic: "topic_a".to_string(),
                kind: MemoryTopicKindRequest::Bytes,
                capacity: None,
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode response json");
        assert_eq!(
            json["capacity"].as_u64().expect("capacity as u64"),
            DEFAULT_MEMORY_PUBSUB_CAPACITY as u64
        );

        let stored = state
            .storage
            .get_memory_topic("topic_a")
            .expect("read memory topic")
            .expect("memory topic exists");
        assert_eq!(stored.capacity, DEFAULT_MEMORY_PUBSUB_CAPACITY);
    }

    #[tokio::test]
    async fn create_memory_topic_rejects_blank_topic_name() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = create_memory_topic_handler(
            State(state),
            Json(CreateMemoryTopicRequest {
                topic: "   ".to_string(),
                kind: MemoryTopicKindRequest::Bytes,
                capacity: Some(8),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 response"),
            "memory topic must not be whitespace (expected [A-Za-z][A-Za-z0-9_]{0,127})"
        );
    }

    #[tokio::test]
    async fn create_memory_topic_rejects_invalid_topic_id() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = create_memory_topic_handler(
            State(state.clone()),
            Json(CreateMemoryTopicRequest {
                topic: "bad-topic".to_string(),
                kind: MemoryTopicKindRequest::Bytes,
                capacity: Some(8),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let message = String::from_utf8(body.to_vec()).expect("utf8 response");
        assert!(message.contains("memory topic"), "got: {message}");
        assert!(message.contains("invalid character"), "got: {message}");
        assert!(
            state
                .storage
                .get_memory_topic("bad-topic")
                .expect("read memory topic")
                .is_none(),
            "invalid topic must not persist",
        );
    }

    #[tokio::test]
    async fn create_memory_topic_normalizes_zero_capacity_to_one() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        let response = create_memory_topic_handler(
            State(state.clone()),
            Json(CreateMemoryTopicRequest {
                topic: "topic_zero".to_string(),
                kind: MemoryTopicKindRequest::Collection,
                capacity: Some(0),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CREATED);
        let stored = state
            .storage
            .get_memory_topic("topic_zero")
            .expect("read memory topic")
            .expect("memory topic exists");
        assert_eq!(stored.capacity, 1);
    }

    #[tokio::test]
    async fn create_memory_topic_conflicts_on_kind_mismatch_with_existing_runtime_topic() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);
        let instance = state
            .local_instance("default")
            .expect("default runtime instance");
        instance
            .declare_memory_topic("topic_existing", MemoryTopicKind::Bytes, 16)
            .expect("declare runtime memory topic");

        let response = create_memory_topic_handler(
            State(state.clone()),
            Json(CreateMemoryTopicRequest {
                topic: "topic_existing".to_string(),
                kind: MemoryTopicKindRequest::Collection,
                capacity: Some(16),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 response"),
            "memory topic topic_existing kind mismatch: expected bytes, got collection"
        );
        assert!(
            state
                .storage
                .get_memory_topic("topic_existing")
                .expect("read memory topic")
                .is_none(),
            "runtime kind mismatch must reject before storage mutation",
        );
    }

    #[tokio::test]
    async fn create_memory_topic_conflicts_on_capacity_mismatch_with_existing_runtime_topic() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);
        let instance = state
            .local_instance("default")
            .expect("default runtime instance");
        instance
            .declare_memory_topic("topic_existing", MemoryTopicKind::Bytes, 16)
            .expect("declare runtime memory topic");

        let response = create_memory_topic_handler(
            State(state.clone()),
            Json(CreateMemoryTopicRequest {
                topic: "topic_existing".to_string(),
                kind: MemoryTopicKindRequest::Bytes,
                capacity: Some(32),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 response"),
            "memory topic topic_existing capacity mismatch: expected 16, got 32"
        );
        assert!(
            state
                .storage
                .get_memory_topic("topic_existing")
                .expect("read memory topic")
                .is_none(),
            "runtime capacity mismatch must reject before storage mutation",
        );
    }

    #[tokio::test]
    async fn list_memory_topics_returns_stably_sorted_topics() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state = build_state(&temp_dir);

        for topic in [
            StoredMemoryTopic {
                topic: "topic_b".to_string(),
                kind: StoredMemoryTopicKind::Bytes,
                capacity: 16,
            },
            StoredMemoryTopic {
                topic: "topic_a".to_string(),
                kind: StoredMemoryTopicKind::Collection,
                capacity: 32,
            },
        ] {
            state
                .storage
                .create_memory_topic(topic)
                .expect("seed memory topic");
        }

        let response = list_memory_topics_handler(State(state))
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response body");
        let json: JsonValue = serde_json::from_slice(&body).expect("decode list response");
        let names = json
            .as_array()
            .expect("topics array")
            .iter()
            .map(|item| item["topic"].as_str().expect("topic name"))
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["topic_a", "topic_b"]);
    }
}
