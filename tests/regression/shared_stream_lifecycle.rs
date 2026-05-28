use super::{
    bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix,
    records_value, wait_for_pipeline_activity, wait_for_shared_stream_status, ManagerHarness,
};
use sdk::PipelineCreateRequest;
use sdk::StopOptions;
use sdk::StreamCreateRequest;
use serde_json::Value as JsonValue;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

fn total_records(stats: &[JsonValue]) -> u64 {
    stats
        .iter()
        .map(|entry| records_value(entry, "records_in") + records_value(entry, "records_out"))
        .sum()
}

async fn pipeline_total_records(http: &reqwest::Client, base: &str, pipeline_id: &str) -> u64 {
    let resp = http
        .get(format!("{base}/pipelines/{pipeline_id}/stats"))
        .send()
        .await
        .expect("pipeline stats request");
    assert!(
        resp.status().is_success(),
        "pipeline {pipeline_id} stats should be readable: status={} body={}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    let stats = resp
        .json::<Vec<JsonValue>>()
        .await
        .expect("decode pipeline stats");
    total_records(&stats)
}

async fn wait_for_pipeline_total_records_above(
    http: &reqwest::Client,
    base: &str,
    pipeline_id: &str,
    baseline: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let total = pipeline_total_records(http, base, pipeline_id).await;
        if total > baseline {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pipeline {pipeline_id} records did not grow beyond {baseline}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_rapid_start_stop_cycles_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let client = make_client(addr);

    let stream_name = format!("reg_shared_stream_{}", random_suffix());
    let stream_req = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());
    client
        .create_stream(&stream_req)
        .await
        .expect("create stream");

    let sql = format!("SELECT value FROM {stream_name}");

    for cycle in 0..5usize {
        let pipeline_a = format!("pipe_a_{cycle}_{}", random_suffix());
        let pipeline_b = format!("pipe_b_{cycle}_{}", random_suffix());

        let req_a = PipelineCreateRequest::nop(pipeline_a.clone(), sql.clone());
        let req_b = PipelineCreateRequest::nop(pipeline_b.clone(), sql.clone());

        client
            .create_pipeline(&req_a)
            .await
            .expect("create pipeline_a");
        client
            .create_pipeline(&req_b)
            .await
            .expect("create pipeline_b");

        client
            .start_pipeline(&pipeline_a)
            .await
            .expect("start pipeline_a");
        client
            .start_pipeline(&pipeline_b)
            .await
            .expect("start pipeline_b");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let opt = StopOptions::graceful(5000);
        let stop_a = client.stop_pipeline(&pipeline_a, opt.clone());
        let stop_b = client.stop_pipeline(&pipeline_b, opt.clone());
        let (ra, rb) = tokio::join!(stop_a, stop_b);

        ra.expect("stop pipeline_a");
        rb.expect("stop pipeline_b");

        client
            .delete_pipeline(&pipeline_a)
            .await
            .expect("delete pipeline_a");
        client
            .delete_pipeline(&pipeline_b)
            .await
            .expect("delete pipeline_b");
    }

    client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_slow_unsubscribe_during_restart_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start manager server");
    });

    let client = make_client(addr);

    let stream_name = format!("reg_shared_stream_slow_unsub_{}", random_suffix());
    let stream_req = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());
    client
        .create_stream(&stream_req)
        .await
        .expect("create stream");

    let sql = format!("SELECT value FROM {stream_name}");
    let pipeline_a = format!("pipe_a_slow_unsub_{}", random_suffix());
    let pipeline_b_v1 = format!("pipe_b_v1_slow_unsub_{}", random_suffix());
    let pipeline_b_v2 = format!("pipe_b_v2_slow_unsub_{}", random_suffix());

    let req_a = PipelineCreateRequest::nop(pipeline_a.clone(), sql.clone());
    let req_b_v1 = PipelineCreateRequest::nop(pipeline_b_v1.clone(), sql.clone());

    client
        .create_pipeline(&req_a)
        .await
        .expect("create pipeline_a");
    client
        .create_pipeline(&req_b_v1)
        .await
        .expect("create pipeline_b_v1");

    client
        .start_pipeline(&pipeline_a)
        .await
        .expect("start pipeline_a");
    client
        .start_pipeline(&pipeline_b_v1)
        .await
        .expect("start pipeline_b_v1");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let client_for_slow = client.clone();
    let pipeline_a_for_slow = pipeline_a.clone();
    let slow_stop_a = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        client_for_slow
            .stop_pipeline(&pipeline_a_for_slow, StopOptions::graceful(5000))
            .await
    });

    client
        .stop_pipeline(&pipeline_b_v1, StopOptions::graceful(5000))
        .await
        .expect("stop pipeline_b_v1");
    client
        .delete_pipeline(&pipeline_b_v1)
        .await
        .expect("delete pipeline_b_v1");

    let req_b_v2 = PipelineCreateRequest::nop(pipeline_b_v2.clone(), sql.clone());
    client
        .create_pipeline(&req_b_v2)
        .await
        .expect("create pipeline_b_v2");
    client
        .start_pipeline(&pipeline_b_v2)
        .await
        .expect("start pipeline_b_v2");

    slow_stop_a
        .await
        .expect("slow stop task panicked")
        .expect("stop pipeline_a");

    client
        .stop_pipeline(&pipeline_b_v2, StopOptions::graceful(5000))
        .await
        .expect("stop pipeline_b_v2");
    client
        .delete_pipeline(&pipeline_b_v2)
        .await
        .expect("delete pipeline_b_v2");

    client
        .delete_pipeline(&pipeline_a)
        .await
        .expect("delete pipeline_a");

    client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_stream_many_consumers_concurrent_lifecycle_via_rest() {
    let Some(harness) = ManagerHarness::new().await else {
        return;
    };

    let stream_name = format!("reg_shared_stream_many_{}", random_suffix());
    harness
        .client
        .create_stream(&StreamCreateRequest::mock_shared_i64_value(
            stream_name.clone(),
        ))
        .await
        .expect("create shared stream");

    let pipeline_ids: Vec<String> = (0..5)
        .map(|idx| format!("reg_shared_stream_many_pipe_{idx}_{}", random_suffix()))
        .collect();
    let sql = format!("SELECT value FROM {stream_name}");

    let mut create_tasks = Vec::new();
    for pipeline_id in &pipeline_ids {
        let client = harness.client.clone();
        let req = PipelineCreateRequest::nop(pipeline_id.clone(), sql.clone());
        create_tasks.push(tokio::spawn(
            async move { client.create_pipeline(&req).await },
        ));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        for task in create_tasks {
            task.await
                .expect("create task panicked")
                .expect("create pipeline");
        }
    })
    .await
    .expect("concurrent creates did not finish");

    let mut start_tasks = Vec::new();
    for pipeline_id in &pipeline_ids {
        let client = harness.client.clone();
        let pipeline_id = pipeline_id.clone();
        start_tasks.push(tokio::spawn(async move {
            client.start_pipeline(&pipeline_id).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        for task in start_tasks {
            task.await
                .expect("start task panicked")
                .expect("start pipeline");
        }
    })
    .await
    .expect("concurrent starts did not finish");

    for value in 0..5 {
        harness
            .injector
            .send_shared_mock_stream_payload(
                &stream_name,
                format!(r#"{{"value":{value}}}"#).as_bytes(),
            )
            .await
            .expect("inject warm-up payload");
    }
    for pipeline_id in &pipeline_ids {
        wait_for_pipeline_activity(
            &harness.http,
            &harness.base,
            pipeline_id,
            Duration::from_secs(5),
        )
        .await;
    }

    let survivors = &pipeline_ids[2..];
    let mut baselines = Vec::new();
    for pipeline_id in survivors {
        baselines.push((
            pipeline_id.clone(),
            pipeline_total_records(&harness.http, &harness.base, pipeline_id).await,
        ));
    }

    let keep_sending = Arc::new(AtomicBool::new(true));
    let sender_flag = Arc::clone(&keep_sending);
    let sender_instance = harness.injector.clone();
    let sender_stream = stream_name.clone();
    let sender = tokio::spawn(async move {
        let mut value = 100i64;
        while sender_flag.load(Ordering::Relaxed) {
            let payload = format!(r#"{{"value":{value}}}"#);
            let _ = sender_instance
                .send_shared_mock_stream_payload(&sender_stream, payload.as_bytes())
                .await;
            value += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let mut remove_tasks = Vec::new();
    for pipeline_id in &pipeline_ids[..2] {
        let client = harness.client.clone();
        let pipeline_id = pipeline_id.clone();
        remove_tasks.push(tokio::spawn(async move {
            client
                .stop_pipeline(&pipeline_id, StopOptions::graceful(5000))
                .await?;
            client.delete_pipeline(&pipeline_id).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        for task in remove_tasks {
            task.await
                .expect("remove task panicked")
                .expect("stop/delete pipeline");
        }
    })
    .await
    .expect("concurrent stop/delete did not finish");
    keep_sending.store(false, Ordering::Relaxed);
    sender.await.expect("sender task panicked");

    for value in 200..205 {
        harness
            .injector
            .send_shared_mock_stream_payload(
                &stream_name,
                format!(r#"{{"value":{value}}}"#).as_bytes(),
            )
            .await
            .expect("inject survivor payload");
    }
    for (pipeline_id, baseline) in baselines {
        wait_for_pipeline_total_records_above(&harness.http, &harness.base, &pipeline_id, baseline)
            .await;
    }

    for pipeline_id in &pipeline_ids[..2] {
        let resp = harness
            .http
            .get(format!("{}/pipelines/{pipeline_id}", harness.base))
            .send()
            .await
            .expect("get deleted pipeline");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::NOT_FOUND,
            "deleted pipeline {pipeline_id} should not remain visible"
        );
    }

    for pipeline_id in survivors {
        harness
            .client
            .stop_pipeline(pipeline_id, StopOptions::graceful(5000))
            .await
            .expect("stop survivor pipeline");
        harness
            .client
            .delete_pipeline(pipeline_id)
            .await
            .expect("delete survivor pipeline");
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
        "shared stream runtime should be reclaimed after all consumers leave: {stopped}"
    );
    harness
        .client
        .delete_stream(&stream_name)
        .await
        .expect("delete shared stream");
}
