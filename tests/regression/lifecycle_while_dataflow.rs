use super::{random_suffix, records_value_or_zero, ManagerHarness};
use sdk::{PipelineCreateRequest, StopOptions, StreamCreateRequest};
use serde_json::Value as JsonValue;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

fn stress_rounds(default: usize) -> usize {
    std::env::var("VELOFLUX_CONCURRENCY_STRESS_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn pipeline_stats(
    http: &reqwest::Client,
    base: &str,
    pipeline_id: &str,
) -> Option<Vec<JsonValue>> {
    let resp = http
        .get(format!("{base}/pipelines/{pipeline_id}/stats"))
        .send()
        .await
        .expect("pipeline stats request");
    if !resp.status().is_success() {
        return None;
    }
    Some(resp.json().await.expect("decode pipeline stats"))
}

fn total_records(stats: &[JsonValue]) -> u64 {
    stats
        .iter()
        .map(|entry| {
            records_value_or_zero(entry, "records_in") + records_value_or_zero(entry, "records_out")
        })
        .sum()
}

async fn wait_for_pipeline_total_records_above(
    http: &reqwest::Client,
    base: &str,
    pipeline_id: &str,
    baseline: u64,
    timeout_duration: Duration,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        if let Some(stats) = pipeline_stats(http, base, pipeline_id).await {
            let total = total_records(&stats);
            if total > baseline {
                return total;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pipeline {pipeline_id} records did not grow beyond {baseline}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_running_stats(
    http: &reqwest::Client,
    base: &str,
    pipeline_id: &str,
    timeout_duration: Duration,
) -> Vec<JsonValue> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        if let Some(stats) = pipeline_stats(http, base, pipeline_id).await {
            return stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pipeline {pipeline_id} did not expose running stats"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn inject_until_success(instance: &flow::FlowInstance, stream_name: &str, value: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let payload = format!(r#"{{"value":{value}}}"#);
    loop {
        match instance
            .send_shared_mock_stream_payload(stream_name, payload.as_bytes())
            .await
        {
            Ok(()) => return,
            Err(err) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "shared stream {stream_name} did not accept payload before timeout: {err}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_stop_loop_while_shared_stream_receives_data() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("dataflow_lifecycle_stream_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let pipeline_id = format!("dataflow_lifecycle_pipe_{}", random_suffix());
    harness
        .client
        .create_pipeline(&PipelineCreateRequest::nop(
            pipeline_id.clone(),
            format!("SELECT value FROM {stream_name}"),
        ))
        .await
        .expect("create pipeline");

    let keep_sending = Arc::new(AtomicBool::new(true));
    let sender_flag = Arc::clone(&keep_sending);
    let sender_instance = harness.injector.clone();
    let sender_stream = stream_name.clone();
    let background_sender = tokio::spawn(async move {
        let mut value = 10_000i64;
        while sender_flag.load(Ordering::Relaxed) {
            let payload = format!(r#"{{"value":{value}}}"#);
            let _ = sender_instance
                .send_shared_mock_stream_payload(&sender_stream, payload.as_bytes())
                .await;
            value += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let rounds = stress_rounds(30);
    for round in 0..rounds {
        harness
            .client
            .start_pipeline(&pipeline_id)
            .await
            .unwrap_or_else(|err| panic!("start pipeline at round {round}: {err}"));

        let baseline_stats = wait_for_running_stats(
            &harness.http,
            &harness.base,
            &pipeline_id,
            Duration::from_secs(5),
        )
        .await;
        let baseline = total_records(&baseline_stats);
        inject_until_success(&harness.injector, &stream_name, round as i64).await;
        wait_for_pipeline_total_records_above(
            &harness.http,
            &harness.base,
            &pipeline_id,
            baseline,
            Duration::from_secs(5),
        )
        .await;

        harness
            .client
            .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
            .await
            .unwrap_or_else(|err| panic!("stop pipeline at round {round}: {err}"));
    }

    keep_sending.store(false, Ordering::Relaxed);
    background_sender.await.expect("background sender panicked");

    harness
        .client
        .start_pipeline(&pipeline_id)
        .await
        .expect("pipeline should remain startable after lifecycle loop");
    inject_until_success(&harness.injector, &stream_name, 99_999).await;
    wait_for_pipeline_total_records_above(
        &harness.http,
        &harness.base,
        &pipeline_id,
        0,
        Duration::from_secs(5),
    )
    .await;
    harness
        .client
        .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
        .await
        .expect("final stop pipeline");
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
