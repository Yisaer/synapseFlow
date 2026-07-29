use chrono::Utc;
use cron::Schedule;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use storage::{StoredPipelineDesiredState, StoredPipelineRunState};

use super::types::{PipelineScheduleRequest, ScheduleStatus};
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
pub(crate) fn compute_schedule_status(
    schedule_config: &PipelineScheduleRequest,
    scheduled_until_ms: Option<i64>,
) -> ScheduleStatus {
    let cron_schedule = parse_cron_schedule(&schedule_config.cron);
    let now = Utc::now();

    let next_fire_at = cron_schedule
        .as_ref()
        .and_then(|s| s.after(&now).next())
        .map(|t| t.to_rfc3339());

    let previous_fire_at = cron_schedule
        .as_ref()
        .and_then(|s| find_previous_fire(s, now))
        .map(|t| t.to_rfc3339());

    let active_fire = cron_schedule
        .as_ref()
        .and_then(|s| find_active_window(s, now, schedule_config.duration_secs));

    let in_window = active_fire.is_some();

    let auto_stop_at = scheduled_until_ms.filter(|_| in_window).map(|ms| {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH)
            .to_rfc3339()
    });

    ScheduleStatus {
        cron: schedule_config.cron.clone(),
        duration_secs: schedule_config.duration_secs,
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
    let _now_ms = now.timestamp_millis();

    let active_fire = find_active_window(&cron_schedule, now, schedule_config.duration_secs);
    let in_window = active_fire.is_some();

    let window_end_ms = active_fire
        .map(|fire| fire.timestamp_millis() + (schedule_config.duration_secs as i64) * 1000);

    let run_state = match storage.get_pipeline_run_state(pipeline_id) {
        Ok(Some(state)) => state,
        Ok(None) => StoredPipelineRunState {
            pipeline_id: pipeline_id.clone(),
            desired_state: StoredPipelineDesiredState::Stopped,
        },
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

    let is_scheduled_run = matches!(
        run_state.desired_state,
        StoredPipelineDesiredState::RunningScheduled(_)
    );

    if in_window && !is_running {
        let window_end = window_end_ms.expect("window_end_ms must be set when in_window is true");
        tracing::info!(
            pipeline_id,
            cron = %schedule_config.cron,
            window_end_ms = window_end,
            "patrol: auto-starting pipeline"
        );

        if let Err(err) = storage.put_pipeline_run_state(StoredPipelineRunState {
            pipeline_id: pipeline_id.clone(),
            desired_state: StoredPipelineDesiredState::RunningScheduled(window_end),
        }) {
            tracing::error!(pipeline_id, %err, "patrol: failed to persist scheduled run state");
            return;
        }

        if let Err(err) = instance.start_pipeline(pipeline_id).await {
            tracing::error!(pipeline_id, %err, "patrol: failed to auto-start pipeline");
            let _ = storage.put_pipeline_run_state(StoredPipelineRunState {
                pipeline_id: pipeline_id.clone(),
                desired_state: StoredPipelineDesiredState::Stopped,
            });
        }
    } else if !in_window && is_running && is_scheduled_run {
        tracing::info!(
            pipeline_id,
            cron = %schedule_config.cron,
            "patrol: auto-stopping pipeline (scheduled window ended)"
        );

        if let Err(err) = storage.put_pipeline_run_state(StoredPipelineRunState {
            pipeline_id: pipeline_id.clone(),
            desired_state: StoredPipelineDesiredState::Stopped,
        }) {
            tracing::error!(pipeline_id, %err, "patrol: failed to clear scheduled run state");
            return;
        }

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
    } else if in_window && is_running && is_scheduled_run {
        let window_end = window_end_ms.expect("window_end_ms must be set when in_window is true");
        let current_window = match run_state.desired_state {
            StoredPipelineDesiredState::RunningScheduled(ms) => Some(ms),
            _ => None,
        };
        if current_window != Some(window_end)
            && storage
                .put_pipeline_run_state(StoredPipelineRunState {
                    pipeline_id: pipeline_id.clone(),
                    desired_state: StoredPipelineDesiredState::RunningScheduled(window_end),
                })
                .is_err()
        {
            tracing::error!(pipeline_id, "patrol: failed to update scheduled window end");
        }
    }
}
