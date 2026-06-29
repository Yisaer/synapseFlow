use crate::audit::ResourceMutationLog;
use crate::instances::DEFAULT_FLOW_INSTANCE_ID;
use crate::storage_bridge;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use flow::pipeline::{PipelineError, PipelineStopMode};
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;
use storage::{StorageError, StoredPipelineDesiredState, StoredPipelineRunState};
use tokio::sync::{OwnedSemaphorePermit, TryAcquireError};

use super::context::{
    build_pipeline_context_payload, shared_mqtt_connector_keys_from_pipeline_request,
};
use super::spec::{
    build_pipeline_definition, referenced_streams_from_pipeline_sql, status_label,
    validate_create_request,
};
use super::state::AppState;
use super::types::{
    BuildPipelineContextResponse, CollectStatsQuery, CreatePipelineRequest, CreatePipelineResponse,
    GetPipelineResponse, ListPipelineItem, StopPipelineQuery, UpsertPipelineRequest,
};

fn parse_stop_mode(mode: &str) -> Result<PipelineStopMode, String> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "" | "quick" => Ok(PipelineStopMode::Quick),
        "graceful" => Ok(PipelineStopMode::Graceful),
        other => Err(format!("unsupported stop mode: {other}")),
    }
}

fn busy_response(id: &str) -> axum::response::Response {
    (
        StatusCode::CONFLICT,
        format!("pipeline {id} is busy processing another command"),
    )
        .into_response()
}

fn shared_mqtt_busy_response(keys: &BTreeSet<String>) -> axum::response::Response {
    if let Some(key) = keys.iter().next().filter(|_| keys.len() == 1) {
        return (
            StatusCode::CONFLICT,
            format!(
                "shared mqtt client {} is busy processing another command",
                key
            ),
        )
            .into_response();
    }

    (
        StatusCode::CONFLICT,
        format!(
            "shared mqtt clients {} are busy processing another command",
            keys.iter().cloned().collect::<Vec<_>>().join(", ")
        ),
    )
        .into_response()
}

fn stream_refs_busy_response(streams: &BTreeSet<String>) -> axum::response::Response {
    if let Some(stream) = streams.iter().next().filter(|_| streams.len() == 1) {
        return (
            StatusCode::CONFLICT,
            format!("stream {stream} is busy processing another command"),
        )
            .into_response();
    }

    (
        StatusCode::CONFLICT,
        format!(
            "streams {} are busy processing another command",
            streams.iter().cloned().collect::<Vec<_>>().join(", ")
        ),
    )
        .into_response()
}

fn stored_state_label(state: Option<StoredPipelineRunState>) -> String {
    match state.map(|s| s.desired_state) {
        Some(
            StoredPipelineDesiredState::Running | StoredPipelineDesiredState::RunningScheduled(_),
        ) => "running".to_string(),
        _ => "stopped".to_string(),
    }
}

fn canonical_flow_instance_id(value: Option<&str>) -> Result<String, String> {
    let id = value.unwrap_or(DEFAULT_FLOW_INSTANCE_ID).trim();
    if id.is_empty() {
        return Err("flow_instance_id must not be empty".to_string());
    }
    Ok(id.to_string())
}

fn local_instance_response(
    state: &AppState,
    flow_instance_id: &str,
) -> Result<std::sync::Arc<flow::FlowInstance>, Box<axum::response::Response>> {
    state.local_instance(flow_instance_id).ok_or_else(|| {
        Box::new(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("flow instance {flow_instance_id} is not available in runtime"),
            )
                .into_response(),
        )
    })
}

async fn resolve_pipeline_spec(
    state: &AppState,
    pipeline_id: &str,
) -> Result<(String, CreatePipelineRequest), axum::response::Response> {
    let stored = match state.storage.get_pipeline(pipeline_id) {
        Ok(Some(pipeline)) => pipeline,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("pipeline {pipeline_id} not found"),
            )
                .into_response());
        }
        Err(err) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {pipeline_id} from storage: {err}"),
            )
                .into_response());
        }
    };

    let mut req = match storage_bridge::pipeline_request_from_stored(&stored) {
        Ok(req) => req,
        Err(err) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode stored pipeline {pipeline_id}: {err}"),
            )
                .into_response());
        }
    };
    let flow_instance_id = canonical_flow_instance_id(req.flow_instance_id.as_deref())
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err).into_response())?;
    req.flow_instance_id = Some(flow_instance_id.clone());

    if !state.is_declared_instance(&flow_instance_id) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "pipeline {pipeline_id} references undeclared flow instance {flow_instance_id}"
            ),
        )
            .into_response());
    }
    Ok((flow_instance_id, req))
}

async fn try_acquire_shared_mqtt_pipeline_ops(
    state: &AppState,
    pipeline_id: &str,
    pipeline_req: &CreatePipelineRequest,
) -> Result<Vec<OwnedSemaphorePermit>, axum::response::Response> {
    let keys = match shared_mqtt_connector_keys_from_pipeline_request(
        state.instances.default_instance().as_ref(),
        state.storage.as_ref(),
        pipeline_id,
        pipeline_req,
    ) {
        Ok(keys) => keys,
        Err(resp) => return Err(*resp),
    };

    match state
        .try_acquire_shared_mqtt_ops(keys.iter().cloned())
        .await
    {
        Ok(permits) => Ok(permits),
        Err(TryAcquireError::NoPermits) => Err(shared_mqtt_busy_response(&keys)),
        Err(TryAcquireError::Closed) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "shared mqtt operation guard closed".to_string(),
        )
            .into_response()),
    }
}

async fn try_acquire_referenced_stream_ops(
    state: &AppState,
    pipeline_req: &CreatePipelineRequest,
    instance: &flow::FlowInstance,
) -> Result<Vec<OwnedSemaphorePermit>, axum::response::Response> {
    let streams = referenced_streams_from_pipeline_sql(pipeline_req, instance)
        .map_err(|err| (StatusCode::BAD_REQUEST, err).into_response())?;
    match state
        .try_acquire_stream_ref_ops(streams.iter().cloned())
        .await
    {
        Ok(permits) => Ok(permits),
        Err(TryAcquireError::NoPermits) => Err(stream_refs_busy_response(&streams)),
        Err(TryAcquireError::Closed) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "stream operation guard closed".to_string(),
        )
            .into_response()),
    }
}

pub async fn create_pipeline_handler(
    State(state): State<AppState>,
    Json(req): Json<CreatePipelineRequest>,
) -> impl IntoResponse {
    let mut req = req;
    req.normalize();
    let flow_instance_id = match canonical_flow_instance_id(req.flow_instance_id.as_deref()) {
        Ok(id) => id,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    req.flow_instance_id = Some(flow_instance_id.clone());
    let audit = ResourceMutationLog::new(
        "pipeline",
        "create",
        req.id.as_str(),
        Some(&flow_instance_id),
    );

    if let Err(err) = validate_create_request(&req) {
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }
    let _permit = match state.try_acquire_pipeline_op(&req.id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&req.id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    if !state.is_declared_instance(&flow_instance_id) {
        return (
            StatusCode::BAD_REQUEST,
            format!("flow instance {flow_instance_id} is not declared by config"),
        )
            .into_response();
    }

    let _shared_mqtt_permits =
        match try_acquire_shared_mqtt_pipeline_ops(&state, &req.id, &req).await {
            Ok(permits) => permits,
            Err(resp) => return resp,
        };

    let instance = match local_instance_response(&state, &flow_instance_id) {
        Ok(instance) => instance,
        Err(resp) => return *resp,
    };

    let _stream_permits =
        match try_acquire_referenced_stream_ops(&state, &req, instance.as_ref()).await {
            Ok(permits) => permits,
            Err(resp) => return resp,
        };

    let encoder_registry = instance.encoder_registry();
    let definition =
        match build_pipeline_definition(&req, encoder_registry.as_ref(), instance.as_ref()) {
            Ok(def) => def,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };

    let stored = match storage_bridge::stored_pipeline_from_request(&req) {
        Ok(stored) => stored,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.storage.create_pipeline(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    }

    let build_result = match instance.create_pipeline(flow::CreatePipelineRequest::new(definition))
    {
        Ok(result) => result,
        Err(PipelineError::AlreadyExists(_)) => {
            let _ = state.storage.delete_pipeline(&stored.id);
            return (
                StatusCode::CONFLICT,
                format!("pipeline {} already exists", req.id),
            )
                .into_response();
        }
        Err(err) => {
            let _ = state.storage.delete_pipeline(&stored.id);
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to create pipeline {}: {err}", req.id),
            )
                .into_response();
        }
    };

    let snapshot = build_result.snapshot;
    audit.log_success();
    (
        StatusCode::CREATED,
        Json(CreatePipelineResponse {
            id: snapshot.definition.id().to_string(),
            status: status_label(snapshot.status),
        }),
    )
        .into_response()
}

pub async fn upsert_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpsertPipelineRequest>,
) -> impl IntoResponse {
    let id = id.trim().to_string();
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let old_pipeline = match state.storage.get_pipeline(&id) {
        Ok(pipeline) => pipeline,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} from storage: {err}"),
            )
                .into_response();
        }
    };

    let flow_instance_id = match old_pipeline.as_ref() {
        Some(stored) => match storage_bridge::pipeline_request_from_stored(stored) {
            Ok(req) => match canonical_flow_instance_id(req.flow_instance_id.as_deref()) {
                Ok(id) => id,
                Err(err) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("invalid stored flow_instance_id for pipeline {id}: {err}"),
                    )
                        .into_response();
                }
            },
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to decode stored pipeline {id}: {err}"),
                )
                    .into_response();
            }
        },
        None => DEFAULT_FLOW_INSTANCE_ID.to_string(),
    };

    let mut create_req = CreatePipelineRequest {
        id: id.clone(),
        flow_instance_id: Some(flow_instance_id),
        sql: req.sql,
        sources: req.sources,
        sinks: req.sinks,
        options: req.options,
    };
    create_req.normalize();
    if let Err(err) = validate_create_request(&create_req) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let old_desired_state = match state.storage.get_pipeline_run_state(&id) {
        Ok(Some(state)) => state.desired_state,
        Ok(None) => StoredPipelineDesiredState::Stopped,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} run state from storage: {err}"),
            )
                .into_response();
        }
    };

    let flow_instance_id = create_req
        .flow_instance_id
        .clone()
        .unwrap_or_else(|| DEFAULT_FLOW_INSTANCE_ID.to_string());
    let audit =
        ResourceMutationLog::new("pipeline", "update", id.as_str(), Some(&flow_instance_id));
    if !state.is_declared_instance(&flow_instance_id) {
        let err = format!("flow instance {flow_instance_id} is not declared by config");
        audit.log_failure(&err);
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let _shared_mqtt_permits =
        match try_acquire_shared_mqtt_pipeline_ops(&state, &id, &create_req).await {
            Ok(permits) => permits,
            Err(resp) => return resp,
        };

    let instance = match local_instance_response(&state, &flow_instance_id) {
        Ok(instance) => instance,
        Err(resp) => return *resp,
    };

    let _stream_permits =
        match try_acquire_referenced_stream_ops(&state, &create_req, instance.as_ref()).await {
            Ok(permits) => permits,
            Err(resp) => return resp,
        };

    let encoder_registry = instance.encoder_registry();
    let definition = match build_pipeline_definition(
        &create_req,
        encoder_registry.as_ref(),
        instance.as_ref(),
    ) {
        Ok(definition) => definition,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    if let Err(err) =
        instance.explain_pipeline(flow::ExplainPipelineTarget::Definition(&definition))
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("invalid pipeline spec: {err}"),
        )
            .into_response();
    }

    if old_pipeline.is_some() {
        match instance.delete_pipeline(&id).await {
            Ok(_) | Err(PipelineError::NotFound(_)) => {}
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to delete pipeline {id} in runtime: {err}"),
                )
                    .into_response();
            }
        }
        match state.storage.delete_pipeline(&id) {
            Ok(_) | Err(StorageError::NotFound(_)) => {}
            Err(err) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to delete pipeline {id} from storage: {err}"),
                )
                    .into_response();
            }
        }
    }

    let stored = match storage_bridge::stored_pipeline_from_request(&create_req) {
        Ok(stored) => stored,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match state.storage.create_pipeline(stored.clone()) {
        Ok(()) => {}
        Err(StorageError::AlreadyExists(_)) => {
            return (
                StatusCode::CONFLICT,
                format!("pipeline {id} already exists"),
            )
                .into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {id}: {err}"),
            )
                .into_response();
        }
    }

    if let Err(err) = instance.create_pipeline(flow::CreatePipelineRequest::new(definition)) {
        let _ = state.storage.delete_pipeline(&id);
        return (
            StatusCode::BAD_REQUEST,
            format!("failed to create pipeline {id}: {err}"),
        )
            .into_response();
    }

    if matches!(
        old_desired_state,
        StoredPipelineDesiredState::Running | StoredPipelineDesiredState::RunningScheduled(_)
    ) {
        if let Err(err) = state
            .storage
            .put_pipeline_run_state(StoredPipelineRunState {
                pipeline_id: id.clone(),
                desired_state: StoredPipelineDesiredState::Running,
            })
        {
            let _ = instance.delete_pipeline(&id).await;
            let _ = state.storage.delete_pipeline(&id);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist pipeline {id} desired state: {err}"),
            )
                .into_response();
        }

        if let Err(err) = instance.start_pipeline(&id).await {
            tracing::error!(
                pipeline_id = %id,
                error = %err,
                "failed to start pipeline after upsert, leaving stopped"
            );
            let _ = state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                });
        }
    }

    let status = stored_state_label(state.storage.get_pipeline_run_state(&id).unwrap_or(None));
    audit.log_success();
    Json(CreatePipelineResponse { id, status }).into_response()
}

pub async fn get_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let stored = match state.storage.get_pipeline(&id) {
        Ok(Some(pipeline)) => pipeline,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response();
        }
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} from storage: {err}"),
            )
                .into_response();
        }
    };

    let spec = match storage_bridge::pipeline_request_from_stored(&stored) {
        Ok(spec) => spec,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode stored pipeline {id}: {err}"),
            )
                .into_response();
        }
    };
    let mut spec = spec;
    let flow_instance_id = match canonical_flow_instance_id(spec.flow_instance_id.as_deref()) {
        Ok(id) => id,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid stored flow_instance_id for pipeline {id}: {err}"),
            )
                .into_response();
        }
    };
    spec.flow_instance_id = Some(flow_instance_id);

    let run_state = match state.storage.get_pipeline_run_state(&id) {
        Ok(state) => state,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read pipeline {id} run state from storage: {err}"),
            )
                .into_response();
        }
    };

    let schedule_status = spec.options.schedule.as_ref().map(|s| {
        let scheduled_until_ms = run_state.as_ref().and_then(|rs| match rs.desired_state {
            storage::StoredPipelineDesiredState::RunningScheduled(until_ms) => Some(until_ms),
            _ => None,
        });
        super::scheduler::compute_schedule_status(s, scheduled_until_ms)
    });

    Json(GetPipelineResponse {
        id: id.clone(),
        status: stored_state_label(run_state),
        spec,
        schedule_status,
    })
    .into_response()
}

pub async fn build_pipeline_context_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let (_flow_instance_id, pipeline_req) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let (streams, shared_mqtt_clients, memory_topics) = match build_pipeline_context_payload(
        &state.instances,
        state.storage.as_ref(),
        &id,
        &pipeline_req,
    ) {
        Ok(payload) => payload,
        Err(resp) => return *resp,
    };

    (
        StatusCode::OK,
        Json(BuildPipelineContextResponse {
            pipeline: pipeline_req,
            streams,
            shared_mqtt_clients,
            memory_topics,
        }),
    )
        .into_response()
}

pub async fn explain_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let instance = match local_instance_response(&state, &flow_instance_id) {
        Ok(instance) => instance,
        Err(resp) => return *resp,
    };
    let explain = match instance.explain_pipeline(flow::ExplainPipelineTarget::Id(&id)) {
        Ok(explain) => explain,
        Err(PipelineError::NotFound(_)) => {
            return (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response();
        }
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to explain pipeline {id}: {err}"),
            )
                .into_response();
        }
    };

    let mut response = explain.to_pretty_string().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

pub async fn collect_pipeline_stats_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<CollectStatsQuery>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };

    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };

    let timeout = Duration::from_millis(query.timeout_ms);
    let instance = match local_instance_response(&state, &flow_instance_id) {
        Ok(instance) => instance,
        Err(resp) => return *resp,
    };
    match instance.collect_pipeline_stats(&id, timeout).await {
        Ok(stats) => {
            let stats = stats
                .into_iter()
                .filter(|entry| {
                    entry.processor_id != "control_source"
                        && !entry.processor_id.starts_with("PhysicalResultCollect_")
                })
                .collect::<Vec<_>>();
            (StatusCode::OK, Json(stats)).into_response()
        }
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(PipelineError::Runtime(err)) if err == flow::ProcessorError::Timeout.to_string() => (
            StatusCode::GATEWAY_TIMEOUT,
            format!("collect stats timeout for pipeline {id}"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to collect pipeline {id} stats: {err}"),
        )
            .into_response(),
    }
}

pub async fn start_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let (flow_instance_id, pipeline_req) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    let audit = ResourceMutationLog::new("pipeline", "start", id.as_str(), Some(&flow_instance_id));

    let _shared_mqtt_permits =
        match try_acquire_shared_mqtt_pipeline_ops(&state, &id, &pipeline_req).await {
            Ok(permits) => permits,
            Err(resp) => return resp,
        };

    if let Err(err) = state
        .storage
        .put_pipeline_run_state(StoredPipelineRunState {
            pipeline_id: id.clone(),
            desired_state: StoredPipelineDesiredState::Running,
        })
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist pipeline {id} desired state: {err}"),
        )
            .into_response();
    }

    let instance = match local_instance_response(&state, &flow_instance_id) {
        Ok(instance) => instance,
        Err(resp) => return *resp,
    };
    match instance.start_pipeline(&id).await {
        Ok(_) => {
            audit.log_success();
            (StatusCode::OK, format!("pipeline {id} started")).into_response()
        }
        Err(PipelineError::NotFound(_)) => {
            let _ = state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                });
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => {
            let _ = state
                .storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                });
            (
                StatusCode::BAD_REQUEST,
                format!("failed to start pipeline {id}: {err}"),
            )
                .into_response()
        }
    }
}

pub async fn stop_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<StopPipelineQuery>,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let (flow_instance_id, _) = match resolve_pipeline_spec(&state, &id).await {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    let audit = ResourceMutationLog::new("pipeline", "stop", id.as_str(), Some(&flow_instance_id));

    let mode = match parse_stop_mode(&query.mode) {
        Ok(mode) => mode,
        Err(err) => {
            audit.log_failure(&err);
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    let timeout = Duration::from_millis(query.timeout_ms);

    // Set desired_state to Stopped (not RunningScheduled) on manual stop
    // so the patrol scheduler does not re-start the pipeline within the
    // same scheduling window.
    if let Err(err) = state
        .storage
        .put_pipeline_run_state(StoredPipelineRunState {
            pipeline_id: id.clone(),
            desired_state: StoredPipelineDesiredState::Stopped,
        })
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist pipeline {id} desired state: {err}"),
        )
            .into_response();
    }

    let instance = match local_instance_response(&state, &flow_instance_id) {
        Ok(instance) => instance,
        Err(resp) => return *resp,
    };
    match instance.stop_pipeline(&id, mode, timeout).await {
        Ok(_) => {
            audit.log_success();
            (StatusCode::OK, format!("pipeline {id} stopped")).into_response()
        }
        Err(PipelineError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, format!("pipeline {id} not found")).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            format!("failed to stop pipeline {id}: {err}"),
        )
            .into_response(),
    }
}

pub async fn delete_pipeline_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut audit = ResourceMutationLog::new("pipeline", "delete", id.as_str(), None);
    let _permit = match state.try_acquire_pipeline_op(&id).await {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => return busy_response(&id),
        Err(TryAcquireError::Closed) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "pipeline operation guard closed".to_string(),
            )
                .into_response();
        }
    };
    let stored = match state.storage.get_pipeline(&id) {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            let err = format!("pipeline {id} not found");
            audit.log_failure(&err);
            return (StatusCode::NOT_FOUND, err).into_response();
        }
        Err(err) => {
            let err = format!("failed to read pipeline {id} from storage: {err}");
            audit.log_failure(&err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
        }
    };
    let flow_instance_id = match storage_bridge::pipeline_request_from_stored(&stored) {
        Ok(req) => match canonical_flow_instance_id(req.flow_instance_id.as_deref()) {
            Ok(id) => Some(id),
            Err(err) => {
                tracing::warn!(pipeline_id = %id, error = %err, "invalid stored flow_instance_id while deleting pipeline");
                None
            }
        },
        Err(err) => {
            tracing::warn!(pipeline_id = %id, error = %err, "failed to decode stored pipeline while deleting");
            None
        }
    };
    audit.set_flow_instance_id(flow_instance_id.as_deref());

    if let Some(flow_instance_id) = flow_instance_id.as_deref() {
        if let Some(instance) = state.local_instance(flow_instance_id) {
            match instance.delete_pipeline(&id).await {
                Ok(_) | Err(PipelineError::NotFound(_)) => {}
                Err(err) => {
                    tracing::warn!(
                        pipeline_id = %id,
                        flow_instance_id = %flow_instance_id,
                        error = %err,
                        "failed to delete pipeline from in-process runtime"
                    );
                }
            }
        } else {
            tracing::warn!(
                pipeline_id = %id,
                flow_instance_id = %flow_instance_id,
                "local flow instance unavailable while deleting pipeline"
            );
        }
    }

    if let Err(err) = state.storage.delete_pipeline(&id) {
        let err = format!("failed to remove pipeline {id} from storage: {err}");
        audit.log_failure(&err);
        return (StatusCode::INTERNAL_SERVER_ERROR, err).into_response();
    }
    audit.log_success();
    (StatusCode::OK, format!("pipeline {id} deleted")).into_response()
}

pub async fn list_pipelines(State(state): State<AppState>) -> impl IntoResponse {
    let mut runtime_status = HashMap::new();
    for (_, instance) in state.instances.instances_snapshot() {
        for snapshot in instance.list_pipelines() {
            runtime_status.insert(
                snapshot.definition.id().to_string(),
                status_label(snapshot.status),
            );
        }
    }

    match state.storage.list_pipelines() {
        Ok(entries) => {
            let mut list = Vec::with_capacity(entries.len());
            for entry in entries {
                let mut spec = match storage_bridge::pipeline_request_from_stored(&entry) {
                    Ok(req) => req,
                    Err(err) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("decode stored pipeline {}: {err}", entry.id),
                        )
                            .into_response();
                    }
                };
                let flow_instance_id =
                    match canonical_flow_instance_id(spec.flow_instance_id.as_deref()) {
                        Ok(id) => id,
                        Err(err) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!(
                                    "invalid stored flow_instance_id for pipeline {}: {err}",
                                    entry.id
                                ),
                            )
                                .into_response();
                        }
                    };
                spec.flow_instance_id = Some(flow_instance_id.clone());

                let status = runtime_status
                    .get(&entry.id)
                    .cloned()
                    .unwrap_or_else(|| "stopped".to_string());
                list.push(ListPipelineItem {
                    id: entry.id,
                    status,
                    flow_instance_id,
                });
            }
            list.sort_by(|a, b| a.id.cmp(&b.id));
            Json(list).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list pipelines: {err}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_pipeline_context_handler, start_pipeline_handler};
    use crate::pipeline::{AppState, CreatePipelineRequest, types};
    use crate::storage_bridge::{
        stored_mqtt_from_config, stored_pipeline_from_request, stored_stream_from_request,
    };
    use crate::stream::{
        CreateStreamRequest, MqttStreamPropsRequest, SchemaConfigRequest, StreamPropsRequest,
    };
    use axum::{
        body::to_bytes,
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use flow::connector::SharedMqttClientConfig;
    use serde_json::{Map as JsonMap, Value as JsonValue};

    fn default_flow_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: "default".to_string(),
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
            username: None,
            password: None,
            resolved_password: None,
        }
    }

    fn mqtt_stream_request(name: &str, connector_key: &str) -> CreateStreamRequest {
        let props = serde_json::to_value(MqttStreamPropsRequest {
            connector_key: Some(connector_key.to_string()),
            ..MqttStreamPropsRequest::default()
        })
        .expect("encode mqtt stream props");
        let JsonValue::Object(fields) = props else {
            panic!("mqtt stream props should encode as object");
        };

        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: JsonMap::new(),
                r#ref: None,
            },
            props: StreamPropsRequest { fields },
            shared: false,
            decoder: crate::stream::DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn mqtt_stream_request_without_connector_key(name: &str) -> CreateStreamRequest {
        CreateStreamRequest {
            name: name.to_string(),
            stream_type: "mqtt".to_string(),
            schema: SchemaConfigRequest {
                schema_type: "json".to_string(),
                props: JsonMap::new(),
                r#ref: None,
            },
            props: StreamPropsRequest {
                fields: JsonMap::new(),
            },
            shared: false,
            decoder: crate::stream::DecoderConfigRequest::default(),
            eventtime: None,
            sampler: None,
        }
    }

    fn mqtt_sink_request(id: &str, connector_key: &str) -> types::CreatePipelineSinkRequest {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "type": "mqtt",
            "props": {
                "connector_key": connector_key
            }
        }))
        .expect("decode mqtt sink request")
    }

    #[tokio::test]
    async fn start_pipeline_returns_conflict_when_shared_mqtt_key_operation_is_busy() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![default_flow_instance_spec()],
            0,
        )
        .expect("build app state");

        let connector_key = "shared".to_string();
        state
            .storage
            .create_mqtt_config(stored_mqtt_from_config(&shared_mqtt_cfg(&connector_key)))
            .expect("persist shared mqtt config");

        let stream_req = mqtt_stream_request("src", &connector_key);
        state
            .storage
            .create_stream(
                stored_stream_from_request(&stream_req).expect("serialize stored stream request"),
            )
            .expect("persist stream");

        let pipeline_req = CreatePipelineRequest {
            id: "pipe_busy".to_string(),
            flow_instance_id: Some("default".to_string()),
            sql: "select * from src".to_string(),
            sources: Vec::new(),
            sinks: Vec::new(),
            options: Default::default(),
        };
        state
            .storage
            .create_pipeline(
                stored_pipeline_from_request(&pipeline_req)
                    .expect("serialize stored pipeline request"),
            )
            .expect("persist pipeline");

        let _permit = state
            .try_acquire_shared_mqtt_ops(std::iter::once(connector_key.clone()))
            .await
            .expect("acquire shared mqtt op");

        let start_resp =
            start_pipeline_handler(State(state.clone()), Path("pipe_busy".to_string()))
                .await
                .into_response();
        assert_eq!(start_resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(start_resp.into_body(), 64 * 1024)
            .await
            .expect("read start body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 start body"),
            "shared mqtt client shared is busy processing another command"
        );
        assert!(
            state
                .storage
                .get_pipeline_run_state("pipe_busy")
                .expect("read pipeline run state")
                .is_none(),
            "busy shared mqtt key must reject start before mutating desired state"
        );
    }

    #[tokio::test]
    async fn start_pipeline_returns_conflict_when_shared_mqtt_sink_key_operation_is_busy() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![default_flow_instance_spec()],
            0,
        )
        .expect("build app state");

        let stream_req = mqtt_stream_request_without_connector_key("src");
        state
            .storage
            .create_stream(
                stored_stream_from_request(&stream_req).expect("serialize stored stream request"),
            )
            .expect("persist stream");

        let connector_key = "shared_sink".to_string();
        state
            .storage
            .create_mqtt_config(stored_mqtt_from_config(&shared_mqtt_cfg(&connector_key)))
            .expect("persist shared mqtt config");

        let pipeline_req = CreatePipelineRequest {
            id: "pipe_busy_sink".to_string(),
            flow_instance_id: Some("default".to_string()),
            sql: "select * from src".to_string(),
            sources: Vec::new(),
            sinks: vec![mqtt_sink_request("sink", &connector_key)],
            options: Default::default(),
        };
        state
            .storage
            .create_pipeline(
                stored_pipeline_from_request(&pipeline_req)
                    .expect("serialize stored pipeline request"),
            )
            .expect("persist pipeline");

        let _permit = state
            .try_acquire_shared_mqtt_ops(std::iter::once(connector_key.clone()))
            .await
            .expect("acquire shared mqtt op");

        let start_resp =
            start_pipeline_handler(State(state.clone()), Path("pipe_busy_sink".to_string()))
                .await
                .into_response();
        assert_eq!(start_resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(start_resp.into_body(), 64 * 1024)
            .await
            .expect("read start body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 start body"),
            "shared mqtt client shared_sink is busy processing another command"
        );
        assert!(
            state
                .storage
                .get_pipeline_run_state("pipe_busy_sink")
                .expect("read pipeline run state")
                .is_none(),
            "busy shared mqtt sink key must reject start before mutating desired state"
        );
    }

    #[tokio::test]
    async fn build_pipeline_context_returns_bad_request_when_shared_mqtt_config_is_missing() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![default_flow_instance_spec()],
            0,
        )
        .expect("build app state");

        let connector_key = "missing_shared".to_string();
        let stream_req = mqtt_stream_request("src", &connector_key);
        state
            .storage
            .create_stream(
                stored_stream_from_request(&stream_req).expect("serialize stored stream request"),
            )
            .expect("persist stream");

        let pipeline_req = CreatePipelineRequest {
            id: "pipe_missing_shared".to_string(),
            flow_instance_id: Some("default".to_string()),
            sql: "select * from src".to_string(),
            sources: Vec::new(),
            sinks: Vec::new(),
            options: Default::default(),
        };
        state
            .storage
            .create_pipeline(
                stored_pipeline_from_request(&pipeline_req)
                    .expect("serialize stored pipeline request"),
            )
            .expect("persist pipeline");

        let response =
            build_pipeline_context_handler(State(state), Path("pipe_missing_shared".to_string()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read build context body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 build context body"),
            "shared mqtt client config missing_shared missing from storage"
        );
    }

    #[tokio::test]
    async fn build_pipeline_context_returns_bad_request_when_shared_mqtt_sink_config_is_missing() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        let state = AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![default_flow_instance_spec()],
            0,
        )
        .expect("build app state");

        let stream_req = mqtt_stream_request_without_connector_key("src");
        state
            .storage
            .create_stream(
                stored_stream_from_request(&stream_req).expect("serialize stored stream request"),
            )
            .expect("persist stream");

        let connector_key = "missing_shared_sink".to_string();
        let pipeline_req = CreatePipelineRequest {
            id: "pipe_missing_shared_sink".to_string(),
            flow_instance_id: Some("default".to_string()),
            sql: "select * from src".to_string(),
            sources: Vec::new(),
            sinks: vec![mqtt_sink_request("sink", &connector_key)],
            options: Default::default(),
        };
        state
            .storage
            .create_pipeline(
                stored_pipeline_from_request(&pipeline_req)
                    .expect("serialize stored pipeline request"),
            )
            .expect("persist pipeline");

        let response = build_pipeline_context_handler(
            State(state),
            Path("pipe_missing_shared_sink".to_string()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read build context body");
        assert_eq!(
            String::from_utf8(body.to_vec()).expect("utf8 build context body"),
            "shared mqtt client config missing_shared_sink missing from storage"
        );
    }
}
