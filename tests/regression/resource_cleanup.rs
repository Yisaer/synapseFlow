use super::{random_suffix, wait_for_shared_stream_status, ManagerHarness};
use reqwest::StatusCode;
use sdk::{PipelineCreateRequest, StopOptions, StreamCreateRequest};
use std::time::Duration;

fn stress_rounds(default: usize) -> usize {
    std::env::var("VELOFLUX_CONCURRENCY_STRESS_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_pipeline_create_does_not_leave_shared_stream_runtime_dirty() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("cleanup_failed_create_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let bad_pipeline_id = format!("cleanup_bad_pipe_{}", random_suffix());
    let bad_resp = harness
        .http
        .post(format!("{}/pipelines", harness.base))
        .json(&PipelineCreateRequest::nop(
            bad_pipeline_id.clone(),
            format!("SELECT missing_column FROM {stream_name}"),
        ))
        .send()
        .await
        .expect("create invalid pipeline request");
    let bad_status = bad_resp.status();
    let bad_body = bad_resp.text().await.unwrap_or_default();
    assert_eq!(
        bad_status,
        StatusCode::BAD_REQUEST,
        "invalid pipeline should fail cleanly: {bad_body}"
    );

    let get_bad_resp = harness
        .http
        .get(format!("{}/pipelines/{bad_pipeline_id}", harness.base))
        .send()
        .await
        .expect("get failed pipeline request");
    assert_eq!(
        get_bad_resp.status(),
        StatusCode::NOT_FOUND,
        "failed create must not leave a stored pipeline"
    );

    let stopped = wait_for_shared_stream_status(
        &harness.client,
        &stream_name,
        "stopped",
        Duration::from_secs(5),
    )
    .await;
    assert!(
        stopped["processors"]
            .as_array()
            .is_some_and(|processors| processors.is_empty()),
        "failed create should not leave shared stream processors: {stopped}"
    );

    let good_pipeline_id = format!("cleanup_good_pipe_{}", random_suffix());
    harness
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            good_pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        ))
        .await
        .expect("create valid pipeline after failed create");
    harness
        .client
        .start_pipeline(&good_pipeline_id)
        .await
        .expect("valid pipeline should start after failed create");
    wait_for_shared_stream_status(
        &harness.client,
        &stream_name,
        "running",
        Duration::from_secs(5),
    )
    .await;

    harness
        .client
        .stop_pipeline(&good_pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("stop valid pipeline");
    harness
        .client
        .delete_pipeline(&good_pipeline_id)
        .await
        .expect("delete valid pipeline");
    wait_for_shared_stream_status(
        &harness.client,
        &stream_name,
        "stopped",
        Duration::from_secs(5),
    )
    .await;
    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_pipeline_spec_build_does_not_leave_stored_pipeline() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("cleanup_failed_spec_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let pipeline_id = format!("cleanup_failed_spec_pipe_{}", random_suffix());
    let invalid_req = serde_json::json!({
        "id": pipeline_id,
        "sql": format!("SELECT value FROM {stream_name}"),
        "sinks": [
            {
                "type": "memory",
                "props": {
                    "topic": format!("missing_topic_{}", random_suffix())
                }
            }
        ]
    });

    let create_resp = harness
        .http
        .post(format!("{}/pipelines", harness.base))
        .json(&invalid_req)
        .send()
        .await
        .expect("create invalid memory-sink pipeline request");
    let create_status = create_resp.status();
    let create_body = create_resp.text().await.unwrap_or_default();
    assert_eq!(
        create_status,
        StatusCode::BAD_REQUEST,
        "invalid pipeline spec should fail cleanly: {create_body}"
    );
    assert!(
        create_body.contains("memory topic") && create_body.contains("not declared"),
        "unexpected invalid pipeline error body: {create_body}"
    );

    let get_resp = harness
        .http
        .get(format!("{}/pipelines/{pipeline_id}", harness.base))
        .send()
        .await
        .expect("get failed pipeline request");
    let get_status = get_resp.status();
    let get_body = get_resp.text().await.unwrap_or_default();
    assert_eq!(
        get_status,
        StatusCode::NOT_FOUND,
        "failed build must not leave stored pipeline: {get_body}"
    );

    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long-running shared stream lifecycle stress; run for release qualification"]
async fn create_delete_shared_pipeline_loop_reclaims_runtime() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("cleanup_loop_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let rounds = stress_rounds(1000);
    for round in 0..rounds {
        let pipeline_id = format!("cleanup_loop_pipe_{round}_{}", random_suffix());
        harness
            .client
            .create_pipeline(&PipelineCreateRequest::nop(
                pipeline_id.clone(),
                format!("SELECT value FROM {stream_name}"),
            ))
            .await
            .unwrap_or_else(|err| panic!("create pipeline at round {round}: {err}"));
        harness
            .client
            .start_pipeline(&pipeline_id)
            .await
            .unwrap_or_else(|err| panic!("start pipeline at round {round}: {err}"));
        harness
            .client
            .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
            .await
            .unwrap_or_else(|err| panic!("stop pipeline at round {round}: {err}"));
        harness
            .client
            .delete_pipeline(&pipeline_id)
            .await
            .unwrap_or_else(|err| panic!("delete pipeline at round {round}: {err}"));

        if round % 25 == 0 {
            wait_for_shared_stream_status(
                &harness.client,
                &stream_name,
                "stopped",
                Duration::from_secs(5),
            )
            .await;
        }
    }

    let stopped = wait_for_shared_stream_status(
        &harness.client,
        &stream_name,
        "stopped",
        Duration::from_secs(5),
    )
    .await;
    assert!(
        stopped["processors"]
            .as_array()
            .is_some_and(|processors| processors.is_empty()),
        "shared stream processors should be reclaimed after loop: {stopped}"
    );
    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");
}
