use chrono::Utc;
use cron::Schedule;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use storage::{StoredPipelineDesiredState, StoredPipelineRunState};

use super::runtime_failure::persist_generic_runtime_failure_marker;
use super::types::{PipelineDatetimeRangeRequest, PipelineScheduleRequest, ScheduleStatus};
use crate::storage_bridge;

/// Validate a 5-field cron expression.
pub(crate) fn validate_cron_expression(expr: &str) -> Result<(), String> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err("cron expression must not be empty".to_string());
    }
    // cron crate uses 7-field (sec min hour dom month dow year) by default.
    // For 5-field (min hour dom month dow), prepend "0 " for seconds and append " *" for year.
    let seven_field = format!("0 {} *", trimmed);
    Schedule::from_str(&seven_field).map_err(|err| format!("invalid cron expression: {err}"))?;
    Ok(())
}

/// Parse a 5-field cron expression into a `cron::Schedule`.
/// Returns None if the expression is empty or invalid.
fn parse_cron_schedule(expr: &str) -> Option<Schedule> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return None;
    }
    let seven_field = format!("0 {} *", trimmed);
    Schedule::from_str(&seven_field).ok()
}

/// Check whether `now` falls within a scheduling window and return the most
/// recent cron fire-time that defines the current window.
///
/// A window is [fire, fire + duration_secs). `now` is in-window iff there
/// exists a cron fire such that fire <= now < fire + duration_secs.
fn find_active_window(
    schedule: &Schedule,
    now: chrono::DateTime<Utc>,
    duration_secs: u64,
) -> Option<chrono::DateTime<Utc>> {
    // Search backwards from `now` up to `duration_secs + 60` seconds.
    let search_start = now - chrono::Duration::seconds(duration_secs as i64 + 60);
    let upcoming = schedule.after(&search_start);
    let mut last_fire: Option<chrono::DateTime<Utc>> = None;
    for fire in upcoming {
        if fire > now {
            break;
        }
        last_fire = Some(fire);
    }
    last_fire.filter(|fire| {
        let window_end = *fire + chrono::Duration::seconds(duration_secs as i64);
        now < window_end
    })
}

fn datetime_range_contains(range: &PipelineDatetimeRangeRequest, timestamp_ms: i64) -> bool {
    range.begin_timestamp_ms <= timestamp_ms && timestamp_ms < range.end_timestamp_ms
}

fn effective_window_end_ms(
    schedule_config: &PipelineScheduleRequest,
    active_fire: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> Option<i64> {
    let cron_window_end_ms =
        active_fire.timestamp_millis() + (schedule_config.duration_secs as i64) * 1000;
    if schedule_config.datetime_ranges.is_empty() {
        return Some(cron_window_end_ms);
    }

    let now_ms = now.timestamp_millis();
    schedule_config
        .datetime_ranges
        .iter()
        .filter(|range| datetime_range_contains(range, now_ms))
        .map(|range| cron_window_end_ms.min(range.end_timestamp_ms))
        .max()
}

/// Find the most recent cron fire that is <= `reference`.
/// Searches backwards up to 2 years; worst case ~1M iterations for minutely
/// cron but only called from the GET handler, not the hot patrol loop.
fn find_previous_fire(
    schedule: &Schedule,
    reference: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    for days_back in 0..(365 * 2) {
        let search_from = reference - chrono::Duration::days(days_back as i64);
        let mut upcoming = schedule.after(&search_from);
        if let Some(first) = upcoming.next() {
            if first > reference {
                continue;
            }
            let mut last_fire = first;
            for fire in upcoming {
                if fire > reference {
                    return Some(last_fire);
                }
                last_fire = fire;
            }
            return Some(last_fire);
        }
    }
    None
}

/// Compute the `ScheduleStatus` for a pipeline at the current time.
pub(crate) fn compute_schedule_status(schedule_config: &PipelineScheduleRequest) -> ScheduleStatus {
    compute_schedule_status_at(schedule_config, Utc::now())
}

fn compute_schedule_status_at(
    schedule_config: &PipelineScheduleRequest,
    now: chrono::DateTime<Utc>,
) -> ScheduleStatus {
    let cron_schedule = parse_cron_schedule(&schedule_config.cron);

    let next_fire_at = cron_schedule
        .as_ref()
        .and_then(|s| s.after(&now).next())
        .map(|t| t.to_rfc3339());

    let previous_fire_at = cron_schedule
        .as_ref()
        .and_then(|s| find_previous_fire(s, now))
        .map(|t| t.to_rfc3339());

    let active_window_end_ms = cron_schedule
        .as_ref()
        .and_then(|s| find_active_window(s, now, schedule_config.duration_secs))
        .and_then(|fire| effective_window_end_ms(schedule_config, fire, now));

    let in_window = active_window_end_ms.is_some();

    let auto_stop_at = active_window_end_ms
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|t| t.to_rfc3339());

    ScheduleStatus {
        cron: schedule_config.cron.clone(),
        duration_secs: schedule_config.duration_secs,
        datetime_ranges: schedule_config.datetime_ranges.clone(),
        in_window,
        previous_fire_at,
        next_fire_at,
        auto_stop_at,
    }
}

/// The patrol loop that periodically checks all pipelines with schedules
/// and reconciles their actual state with the expected schedule windows.
pub(crate) async fn run_patrol(
    storage: Arc<storage::StorageManager>,
    instances: crate::instances::FlowInstances,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.tick().await;

    loop {
        tick.tick().await;
        let pipelines = match storage.list_pipelines() {
            Ok(p) => p,
            Err(err) => {
                tracing::error!(%err, "patrol: failed to list pipelines");
                continue;
            }
        };

        for stored in &pipelines {
            patrol_pipeline(stored, storage.as_ref(), &instances).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::extract::{Path, Query, State};
    use axum::response::IntoResponse;
    use chrono::TimeZone;
    use serde_json::Value as JsonValue;

    fn default_flow_instance_spec() -> crate::FlowInstanceSpec {
        crate::FlowInstanceSpec {
            id: crate::instances::DEFAULT_FLOW_INSTANCE_ID.to_string(),
            ..crate::FlowInstanceSpec::default()
        }
    }

    fn schedule(
        cron: &str,
        duration_secs: u64,
        datetime_ranges: Vec<PipelineDatetimeRangeRequest>,
    ) -> PipelineScheduleRequest {
        PipelineScheduleRequest {
            cron: cron.to_string(),
            duration_secs,
            datetime_ranges,
        }
    }

    fn scheduled_pipeline_request(id: &str) -> super::super::types::CreatePipelineRequest {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "revision": 7,
            "flow_instance_id": crate::instances::DEFAULT_FLOW_INSTANCE_ID,
            "sql": "select * from src",
            "sinks": [
                {
                    "id": "sink",
                    "type": "nop",
                    "props": { "log": false }
                }
            ],
            "options": {
                "schedule": {
                    "cron": "* * * * *",
                    "duration_secs": 60
                }
            }
        }))
        .expect("decode scheduled pipeline request")
    }

    #[test]
    fn schedule_status_requires_datetime_range_match() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 10).unwrap();
        let req = schedule(
            "* * * * *",
            30,
            vec![PipelineDatetimeRangeRequest {
                begin_timestamp_ms: now.timestamp_millis() + 60_000,
                end_timestamp_ms: now.timestamp_millis() + 120_000,
            }],
        );

        let status = compute_schedule_status_at(&req, now);

        assert!(!status.in_window);
        assert_eq!(status.auto_stop_at, None);
    }

    #[test]
    fn schedule_status_clips_auto_stop_at_to_datetime_range_end() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 1, 0).unwrap();
        let range_end = Utc.with_ymd_and_hms(2026, 1, 1, 10, 2, 0).unwrap();
        let req = schedule(
            "0 10 * * *",
            300,
            vec![PipelineDatetimeRangeRequest {
                begin_timestamp_ms: Utc
                    .with_ymd_and_hms(2026, 1, 1, 10, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
                end_timestamp_ms: range_end.timestamp_millis(),
            }],
        );

        let status = compute_schedule_status_at(&req, now);

        assert!(status.in_window);
        assert_eq!(status.auto_stop_at, Some(range_end.to_rfc3339()));
    }

    #[test]
    fn schedule_status_without_datetime_ranges_uses_cron_window() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 10).unwrap();
        let req = schedule("* * * * *", 30, Vec::new());

        let status = compute_schedule_status_at(&req, now);

        assert!(status.in_window);
        assert_eq!(
            status.auto_stop_at,
            Some(
                Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 30)
                    .unwrap()
                    .to_rfc3339()
            )
        );
    }

    #[tokio::test]
    async fn patrol_auto_start_failure_marks_failed_without_retrying() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
        let state = super::super::state::AppState::new(
            crate::new_default_flow_instance(),
            storage,
            vec![default_flow_instance_spec()],
            0,
        )
        .expect("build app state");
        let pipeline_req = scheduled_pipeline_request("scheduled_fail_pipe");
        let stored = storage_bridge::stored_pipeline_from_request(&pipeline_req)
            .expect("serialize scheduled pipeline");
        state
            .storage
            .create_pipeline(stored.clone())
            .expect("persist scheduled pipeline");

        patrol_pipeline(&stored, state.storage.as_ref(), &state.instances).await;

        let run_state = state
            .storage
            .get_pipeline_run_state("scheduled_fail_pipe")
            .expect("read run state")
            .expect("run state exists");
        assert_eq!(
            run_state.desired_state,
            StoredPipelineDesiredState::ScheduledRunning
        );
        let marker = state
            .storage
            .get_pipeline_runtime_failure("scheduled_fail_pipe")
            .expect("read failure marker")
            .expect("failure marker exists");
        assert_eq!(marker.revision, 7);
        assert_eq!(marker.processor_id, "pipeline_runtime");
        assert_eq!(marker.processor_kind, "scheduler_auto_start");

        let response = super::super::handlers::list_pipelines(State(state.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read list body");
        let payload: JsonValue = serde_json::from_slice(&body).expect("decode list body");
        let item = payload
            .as_array()
            .and_then(|items| items.first())
            .expect("one list item");
        assert_eq!(item["status"], "failed");
        assert_eq!(item["desired_status"], "scheduled_running");
        assert_eq!(
            item["last_runtime_error"]["processor_kind"],
            "scheduler_auto_start"
        );

        let first_failed_at_ms = marker.failed_at_ms;
        patrol_pipeline(&stored, state.storage.as_ref(), &state.instances).await;
        let marker = state
            .storage
            .get_pipeline_runtime_failure("scheduled_fail_pipe")
            .expect("read failure marker")
            .expect("failure marker exists");
        assert_eq!(marker.failed_at_ms, first_failed_at_ms);
        assert_eq!(marker.processor_kind, "scheduler_auto_start");

        let response = super::super::handlers::stop_pipeline_handler(
            State(state.clone()),
            Path("scheduled_fail_pipe".to_string()),
            Query(super::super::types::StopPipelineQuery::default()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert!(
            state
                .storage
                .get_pipeline_runtime_failure("scheduled_fail_pipe")
                .expect("read failure marker")
                .is_none()
        );
        let run_state = state
            .storage
            .get_pipeline_run_state("scheduled_fail_pipe")
            .expect("read run state")
            .expect("run state exists");
        assert_eq!(
            run_state.desired_state,
            StoredPipelineDesiredState::ScheduledStopped
        );

        patrol_pipeline(&stored, state.storage.as_ref(), &state.instances).await;
        let marker = state
            .storage
            .get_pipeline_runtime_failure("scheduled_fail_pipe")
            .expect("read failure marker")
            .expect("failure marker exists");
        assert_eq!(marker.revision, 7);
        assert_eq!(marker.processor_kind, "scheduler_auto_start");
    }
}

async fn patrol_pipeline(
    stored: &storage::StoredPipeline,
    storage: &storage::StorageManager,
    instances: &crate::instances::FlowInstances,
) {
    let pipeline_id = &stored.id;

    let req = match storage_bridge::pipeline_request_from_stored(stored) {
        Ok(r) => r,
        Err(err) => {
            tracing::warn!(pipeline_id, %err, "patrol: failed to decode stored pipeline");
            return;
        }
    };

    let Some(schedule_config) = &req.options.schedule else {
        return;
    };

    let cron_schedule = match parse_cron_schedule(&schedule_config.cron) {
        Some(s) => s,
        None => {
            tracing::warn!(
                pipeline_id,
                cron = %schedule_config.cron,
                "patrol: invalid cron expression"
            );
            return;
        }
    };

    let now = Utc::now();
    let active_window_end_ms =
        find_active_window(&cron_schedule, now, schedule_config.duration_secs)
            .and_then(|fire| effective_window_end_ms(schedule_config, fire, now));
    let in_window = active_window_end_ms.is_some();

    let expected_desired_state = if in_window {
        StoredPipelineDesiredState::ScheduledRunning
    } else {
        StoredPipelineDesiredState::ScheduledStopped
    };

    let current_desired_state = match storage.get_pipeline_run_state(pipeline_id) {
        Ok(Some(state)) => state.desired_state,
        Ok(None) => StoredPipelineDesiredState::ScheduledStopped,
        Err(err) => {
            tracing::warn!(pipeline_id, %err, "patrol: failed to read run state");
            return;
        }
    };

    let flow_instance_id = req
        .flow_instance_id
        .as_deref()
        .unwrap_or(crate::instances::DEFAULT_FLOW_INSTANCE_ID);

    let Some(instance) = instances.get(flow_instance_id) else {
        tracing::debug!(
            pipeline_id,
            flow_instance_id,
            "patrol: flow instance not available"
        );
        return;
    };

    let is_running = instance.list_pipelines().iter().any(|s| {
        s.definition.id() == pipeline_id
            && matches!(s.status, flow::pipeline::PipelineStatus::Running)
    });

    if current_desired_state != expected_desired_state
        && storage
            .put_pipeline_run_state(StoredPipelineRunState {
                pipeline_id: pipeline_id.clone(),
                desired_state: expected_desired_state.clone(),
            })
            .is_err()
    {
        tracing::error!(
            pipeline_id,
            "patrol: failed to persist scheduled desired state"
        );
        return;
    }

    if in_window && !is_running {
        match storage.get_pipeline_runtime_failure(pipeline_id) {
            Ok(Some(failure)) if failure.revision == stored.revision => {
                tracing::warn!(
                    pipeline_id,
                    revision = stored.revision,
                    processor_id = %failure.processor_id,
                    processor_kind = %failure.processor_kind,
                    "patrol: skipping auto-start for failed pipeline"
                );
                return;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    pipeline_id,
                    %err,
                    "patrol: failed to read pipeline runtime failure marker"
                );
                return;
            }
        }

        tracing::info!(
            pipeline_id,
            cron = %schedule_config.cron,
            "patrol: auto-starting pipeline"
        );

        if let Err(err) = instance.start_pipeline(pipeline_id).await {
            tracing::error!(pipeline_id, %err, "patrol: failed to auto-start pipeline");
            persist_generic_runtime_failure_marker(
                storage,
                pipeline_id,
                stored.revision,
                "scheduler_auto_start",
                err.to_string(),
            );
        }
    } else if !in_window && is_running {
        tracing::info!(
            pipeline_id,
            cron = %schedule_config.cron,
            "patrol: auto-stopping pipeline (scheduled window ended)"
        );

        let timeout = Duration::from_secs(30);
        if let Err(err) = instance
            .stop_pipeline(
                pipeline_id,
                flow::pipeline::PipelineStopMode::Quick,
                timeout,
            )
            .await
        {
            tracing::error!(pipeline_id, %err, "patrol: failed to auto-stop pipeline");
        }
    }
}
