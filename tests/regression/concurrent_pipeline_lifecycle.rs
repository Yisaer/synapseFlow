use super::{random_suffix, ManagerHarness};
use reqwest::StatusCode;
use sdk::types::PipelineUpsertRequest;
use sdk::{PipelineCreateRequest, StopOptions, StreamCreateRequest};
use std::time::Duration;

async fn response_status_and_body(resp: reqwest::Response) -> (StatusCode, String) {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_create_same_pipeline_id_returns_conflict_without_corruption() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("crud_create_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let pipeline_id = format!("crud_create_pipe_{}", random_suffix());
    let req = PipelineCreateRequest::nop(
        pipeline_id.clone(),
        format!("SELECT value FROM {stream_name}"),
    );

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let http = harness.http.clone();
        let base = harness.base.clone();
        let req = req.clone();
        tasks.push(tokio::spawn(async move {
            let resp = http
                .post(format!("{base}/pipelines"))
                .json(&req)
                .send()
                .await
                .expect("create pipeline request");
            response_status_and_body(resp).await
        }));
    }

    let results = tokio::time::timeout(Duration::from_secs(10), async {
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.expect("create task panicked"));
        }
        results
    })
    .await
    .expect("concurrent create did not finish");

    let created = results
        .iter()
        .filter(|(status, _)| *status == StatusCode::CREATED)
        .count();
    assert_eq!(
        created, 1,
        "exactly one create should win for a duplicate pipeline id: {results:?}"
    );
    assert!(
        results
            .iter()
            .all(|(status, _)| matches!(*status, StatusCode::CREATED | StatusCode::CONFLICT)),
        "duplicate creates should either succeed once or return conflict: {results:?}"
    );

    harness
        .client
        .start_pipeline(&pipeline_id)
        .await
        .expect("created pipeline should still be startable");
    harness
        .client
        .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("created pipeline should still be stoppable");
    harness
        .client
        .delete_pipeline(&pipeline_id)
        .await
        .expect("created pipeline should still be deletable");
    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upsert_same_pipeline_id_leaves_valid_pipeline() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("crud_upsert_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let pipeline_id = format!("crud_upsert_pipe_{}", random_suffix());
    harness
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        ))
        .await
        .expect("create pipeline");

    let sql_variants = [
        format!("SELECT value FROM {stream_name}"),
        format!("SELECT value FROM {stream_name} WHERE value >= 0"),
        format!("SELECT value + 1 AS shifted_value FROM {stream_name}"),
        format!("SELECT value + 2 AS shifted_value_2 FROM {stream_name}"),
    ];

    let mut tasks = Vec::new();
    for idx in 0..12 {
        let http = harness.http.clone();
        let base = harness.base.clone();
        let pipeline_id = pipeline_id.clone();
        let req = PipelineUpsertRequest::nop(sql_variants[idx % sql_variants.len()].clone());
        tasks.push(tokio::spawn(async move {
            let resp = http
                .put(format!("{base}/pipelines/{pipeline_id}"))
                .json(&req)
                .send()
                .await
                .expect("upsert pipeline request");
            response_status_and_body(resp).await
        }));
    }

    let results = tokio::time::timeout(Duration::from_secs(15), async {
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.expect("upsert task panicked"));
        }
        results
    })
    .await
    .expect("concurrent upsert did not finish");

    assert!(
        results.iter().any(|(status, _)| *status == StatusCode::OK),
        "at least one upsert should succeed: {results:?}"
    );
    assert!(
        results
            .iter()
            .all(|(status, _)| matches!(*status, StatusCode::OK | StatusCode::CONFLICT)),
        "concurrent upserts should succeed or hit the per-pipeline busy guard: {results:?}"
    );

    harness
        .client
        .get_pipeline(&pipeline_id)
        .await
        .expect("pipeline should remain readable after concurrent upserts");
    harness
        .client
        .start_pipeline(&pipeline_id)
        .await
        .expect("pipeline should remain startable after concurrent upserts");
    harness
        .client
        .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("pipeline should remain stoppable after concurrent upserts");
    harness
        .client
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_start_stop_same_pipeline_does_not_hang_or_corrupt_state() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("crud_start_stop_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let pipeline_id = format!("crud_start_stop_pipe_{}", random_suffix());
    harness
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        ))
        .await
        .expect("create pipeline");

    let mut tasks = Vec::new();
    for idx in 0..24 {
        let http = harness.http.clone();
        let base = harness.base.clone();
        let pipeline_id = pipeline_id.clone();
        tasks.push(tokio::spawn(async move {
            let url = if idx % 2 == 0 {
                format!("{base}/pipelines/{pipeline_id}/start")
            } else {
                format!("{base}/pipelines/{pipeline_id}/stop?mode=graceful&timeout_ms=5000")
            };
            let resp = http.post(url).send().await.expect("lifecycle request");
            response_status_and_body(resp).await
        }));
    }

    let results = tokio::time::timeout(Duration::from_secs(20), async {
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.expect("lifecycle task panicked"));
        }
        results
    })
    .await
    .expect("concurrent start/stop did not finish");

    assert!(
        results.iter().all(|(status, _)| {
            matches!(
                *status,
                StatusCode::OK | StatusCode::CONFLICT | StatusCode::BAD_REQUEST
            )
        }),
        "start/stop race should not produce server errors: {results:?}"
    );

    harness
        .client
        .start_pipeline(&pipeline_id)
        .await
        .expect("pipeline should remain startable after start/stop race");
    harness
        .client
        .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("pipeline should remain stoppable after start/stop race");
    harness
        .client
        .delete_pipeline(&pipeline_id)
        .await
        .expect("delete pipeline");
    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_create_delete_same_stream_leaves_storage_and_runtime_consistent() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    for round in 0..30 {
        let stream_name = format!("crud_stream_race_{round}_{}", random_suffix());
        let stream_req = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());

        let mut tasks = Vec::new();
        for idx in 0..12 {
            let http = harness.http.clone();
            let base = harness.base.clone();
            let stream_name = stream_name.clone();
            let stream_req = stream_req.clone();
            tasks.push(tokio::spawn(async move {
                let resp = if idx % 2 == 0 {
                    http.post(format!("{base}/streams"))
                        .json(&stream_req)
                        .send()
                        .await
                        .expect("create stream request")
                } else {
                    http.delete(format!("{base}/streams/{stream_name}"))
                        .send()
                        .await
                        .expect("delete stream request")
                };
                response_status_and_body(resp).await
            }));
        }

        let results = tokio::time::timeout(Duration::from_secs(10), async {
            let mut results = Vec::new();
            for task in tasks {
                results.push(task.await.expect("stream lifecycle task panicked"));
            }
            results
        })
        .await
        .unwrap_or_else(|_| panic!("stream create/delete race did not finish at round {round}"));

        assert!(
            results.iter().all(|(status, _)| {
                matches!(
                    *status,
                    StatusCode::CREATED
                        | StatusCode::OK
                        | StatusCode::CONFLICT
                        | StatusCode::NOT_FOUND
                )
            }),
            "stream create/delete race should not produce server errors at round {round}: {results:?}"
        );

        let describe_resp = harness
            .http
            .get(format!("{}/streams/describe/{stream_name}", harness.base))
            .send()
            .await
            .expect("describe stream after race");
        match describe_resp.status() {
            StatusCode::OK => {
                let stats_resp = harness
                    .http
                    .get(format!(
                        "{}/streams/{stream_name}/shared/stats?flow_instance_id=default",
                        harness.base
                    ))
                    .send()
                    .await
                    .expect("shared stream stats after race");
                let stats_status = stats_resp.status();
                let stats_body = stats_resp.text().await.unwrap_or_default();
                assert_eq!(
                    stats_status,
                    StatusCode::OK,
                    "stored shared stream should also exist in runtime at round {round}: {stats_body}"
                );
                harness
                    .client
                    .delete_stream(&stream_name)
                    .await
                    .unwrap_or_else(|err| {
                        panic!("cleanup existing stream at round {round}: {err}")
                    });
            }
            StatusCode::NOT_FOUND => {
                let recreate_resp = harness
                    .http
                    .post(format!("{}/streams", harness.base))
                    .json(&stream_req)
                    .send()
                    .await
                    .expect("recreate stream after race");
                let recreate_status = recreate_resp.status();
                let recreate_body = recreate_resp.text().await.unwrap_or_default();
                assert_eq!(
                    recreate_status,
                    StatusCode::CREATED,
                    "missing stored stream should be cleanly recreatable at round {round}: {recreate_body}"
                );
                harness
                    .client
                    .delete_stream(&stream_name)
                    .await
                    .unwrap_or_else(|err| {
                        panic!("cleanup recreated stream at round {round}: {err}")
                    });
            }
            status => {
                let body = describe_resp.text().await.unwrap_or_default();
                panic!("unexpected describe status after stream race at round {round}: {status} {body}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_create_pipeline_delete_stream_keeps_references_consistent() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    for round in 0..40 {
        let stream_name = format!("crud_pipe_stream_race_{round}_{}", random_suffix());
        let pipeline_id = format!("crud_pipe_stream_race_pipe_{round}_{}", random_suffix());
        harness
            .client
            .create_stream(&StreamCreateRequest::mock_shared_i64_value(
                stream_name.clone(),
            ))
            .await
            .unwrap_or_else(|err| panic!("create stream at round {round}: {err}"));

        let pipeline_req = PipelineCreateRequest::nop(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        );

        let mut tasks = Vec::new();
        for idx in 0..16 {
            let http = harness.http.clone();
            let base = harness.base.clone();
            let pipeline_req = pipeline_req.clone();
            let stream_name = stream_name.clone();
            tasks.push(tokio::spawn(async move {
                let resp = if idx % 2 == 0 {
                    http.post(format!("{base}/pipelines"))
                        .json(&pipeline_req)
                        .send()
                        .await
                        .expect("create pipeline request")
                } else {
                    http.delete(format!("{base}/streams/{stream_name}"))
                        .send()
                        .await
                        .expect("delete stream request")
                };
                response_status_and_body(resp).await
            }));
        }

        let results = tokio::time::timeout(Duration::from_secs(10), async {
            let mut results = Vec::new();
            for task in tasks {
                results.push(task.await.expect("pipeline/stream race task panicked"));
            }
            results
        })
        .await
        .unwrap_or_else(|_| {
            panic!("pipeline create/delete stream race timed out at round {round}")
        });

        assert!(
            results.iter().all(|(status, _)| {
                matches!(
                    *status,
                    StatusCode::CREATED
                        | StatusCode::OK
                        | StatusCode::CONFLICT
                        | StatusCode::BAD_REQUEST
                        | StatusCode::NOT_FOUND
                )
            }),
            "pipeline create/delete stream race should not produce server errors at round {round}: {results:?}"
        );

        let pipeline_resp = harness
            .http
            .get(format!("{}/pipelines/{pipeline_id}", harness.base))
            .send()
            .await
            .expect("get pipeline after race");
        let pipeline_exists = match pipeline_resp.status() {
            StatusCode::OK => true,
            StatusCode::NOT_FOUND => false,
            status => {
                let body = pipeline_resp.text().await.unwrap_or_default();
                panic!("unexpected pipeline status after race at round {round}: {status} {body}");
            }
        };

        let stream_resp = harness
            .http
            .get(format!("{}/streams/describe/{stream_name}", harness.base))
            .send()
            .await
            .expect("describe stream after race");
        let stream_exists = match stream_resp.status() {
            StatusCode::OK => true,
            StatusCode::NOT_FOUND => false,
            status => {
                let body = stream_resp.text().await.unwrap_or_default();
                panic!("unexpected stream status after race at round {round}: {status} {body}");
            }
        };

        assert!(
            !pipeline_exists || stream_exists,
            "pipeline {pipeline_id} must not reference deleted stream {stream_name} at round {round}: {results:?}"
        );

        if pipeline_exists {
            harness
                .client
                .delete_pipeline(&pipeline_id)
                .await
                .unwrap_or_else(|err| panic!("cleanup pipeline at round {round}: {err}"));
        }
        if stream_exists {
            harness
                .client
                .delete_stream(&stream_name)
                .await
                .unwrap_or_else(|err| panic!("cleanup stream at round {round}: {err}"));
        }
    }
}
