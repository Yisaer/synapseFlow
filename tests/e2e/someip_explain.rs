use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use reqwest::StatusCode;
use std::path::PathBuf;
use std::time::Duration;

fn arxml_test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("distros/sdv/tests/test_data")
}

/// Compare two explain strings line-by-line, trimming trailing whitespace.
fn assert_explain_eq(actual: &str, expected: &str, context: &str) {
    let actual_lines: Vec<&str> = actual.lines().map(|l| l.trim_end()).collect();
    let expected_lines: Vec<&str> = expected.lines().map(|l| l.trim_end()).collect();
    assert_eq!(
        actual_lines, expected_lines,
        "[{context}] explain mismatch\nactual:\n{actual}\nexpected:\n{expected}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn someip_explain() {
    let arxml_path = arxml_test_data_dir()
        .join("baq.arxml")
        .to_str()
        .unwrap()
        .to_string();
    let packet_path = arxml_test_data_dir()
        .join("someip_packet.json")
        .to_str()
        .unwrap()
        .to_string();

    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage");
    let instance = manager::new_default_flow_instance();
    veloflux_sdv::register(&instance);

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        manager::start_server_with_listener(listener, instance, storage, default_flow_instances())
            .await
            .expect("start server");
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let http = http_client();
    let base = format!("http://{addr}");
    let suffix = random_suffix();

    // Case 1: default (raw field names).
    {
        let name = format!("sip1_{suffix}");
        create_stream(&http, &base, &name, &arxml_path, &packet_path, None).await;
        let pipe_id = format!("pipe_{name}");
        let sql = format!("SELECT ts, DTE_SlotID, DTE_SlotType, DTE_SlotStatus from {name}");
        let sink_id = format!("sink_{name}");
        create_pipeline(&http, &base, &pipe_id, &sql, &sink_id).await;

        let resp = http
            .get(format!("{base}/pipelines/{pipe_id}/explain"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let explain = resp.text().await.unwrap();

        let expected = [
            "Logical Plan Explain:",
            "- id                     | info",
            &format!("  Tail_3                 | sink_count=1"),
            &format!("  └─DataSink_2           | sink_id={sink_id}, connector=nop, encoder=json"),
            "    └─Project_1          | fields=[ts; DTE_SlotID; DTE_SlotType; DTE_SlotStatus]",
            &format!("      └─DataSource_0     | source={name}, decoder=gbf, schema=[ts, DTE_SlotID, DTE_SlotType, DTE_SlotStatus]"),
            "",
            "Physical Plan Explain:",
            "- id                                 | info",
            "  PhysicalResultCollect_5            |",
            &format!("  └─PhysicalSinkConnector_3          | sink_id={sink_id}, connector=nop"),
            &format!("    └─PhysicalSinkEncoder_4          | sink_id={sink_id}, encoder=json, by_index_projection=[{name}#0->ts; {name}#1->DTE_SlotID; {name}#2->DTE_SlotType; {name}#3->DTE_SlotStatus]"),
            "      └─PhysicalProject_2            | fields=[], passthrough_messages=true",
            "        └─PhysicalDecoder_1          | decoder=gbf, schema=[ts, DTE_SlotID, DTE_SlotType, DTE_SlotStatus]",
            &format!("          └─PhysicalDataSource_0     | source={name}, schema=[ts, DTE_SlotID, DTE_SlotType, DTE_SlotStatus]"),
        ].join("\n");
        assert_explain_eq(&explain, &expected, "case1");
    }

    // Case 2: with signal_name_pattern.
    {
        let name = format!("sip2_{suffix}");
        create_stream(
            &http,
            &base,
            &name,
            &arxml_path,
            &packet_path,
            Some("{service}.{method}.{field}"),
        )
        .await;

        let svc = "PSI_ADCC_ParkingHmiEnv_1_VLAN62_ADCC";
        let entry = "ADT_ADAS_arr_ParkingSlot";
        let col = format!("`{svc}.{entry}.DTE_SlotID`");
        let col_flat = format!("{svc}.{entry}.DTE_SlotID");
        let pipe_id = format!("pipe_{name}");
        let sql = format!("SELECT ts, {col} from {name}");
        let sink_id = format!("sink_{name}");
        create_pipeline(&http, &base, &pipe_id, &sql, &sink_id).await;

        let resp = http
            .get(format!("{base}/pipelines/{pipe_id}/explain"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let explain = resp.text().await.unwrap();

        let expected = [
            "Logical Plan Explain:",
            "- id                     | info",
            &format!("  Tail_3                 | sink_count=1"),
            &format!("  └─DataSink_2           | sink_id={sink_id}, connector=nop, encoder=json"),
            &format!("    └─Project_1          | fields=[ts; `{col_flat}` as {col_flat}]"),
            &format!("      └─DataSource_0     | source={name}, decoder=gbf, schema=[ts, {col_flat}]"),
            "",
            "Physical Plan Explain:",
            "- id                                 | info",
            "  PhysicalResultCollect_5            |",
            &format!("  └─PhysicalSinkConnector_3          | sink_id={sink_id}, connector=nop"),
            &format!("    └─PhysicalSinkEncoder_4          | sink_id={sink_id}, encoder=json, by_index_projection=[{name}#0->ts; {name}#1->{col_flat}]"),
            "      └─PhysicalProject_2            | fields=[], passthrough_messages=true",
            &format!("        └─PhysicalDecoder_1          | decoder=gbf, schema=[ts, {col_flat}]"),
            &format!("          └─PhysicalDataSource_0     | source={name}, schema=[ts, {col_flat}]"),
        ].join("\n");
        assert_explain_eq(&explain, &expected, "case2");
    }

    server.abort();
}

async fn create_stream(
    http: &reqwest::Client,
    base: &str,
    name: &str,
    arxml_path: &str,
    packet_path: &str,
    signal_name_pattern: Option<&str>,
) {
    let mut props = serde_json::json!({"schema_path": arxml_path});
    if let Some(p) = signal_name_pattern {
        props["signal_name_pattern"] = serde_json::json!(p);
    }
    let resp = http
        .post(format!("{base}/streams"))
        .json(&serde_json::json!({
            "name": name, "type": "mqtt",
            "schema": {"type": "arxml", "props": props},
            "props": {"broker_url": "tcp://127.0.0.1:1883", "topic": "x", "qos": 0},
            "decoder": {"type": "gbf", "props": {
                "schema_path": packet_path,
                "format_type": "someip",
                "format_schema_path": arxml_path,
            }}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "stream {name}: {}",
        resp.text().await.unwrap_or_default()
    );
}

async fn create_pipeline(
    http: &reqwest::Client,
    base: &str,
    pipe_id: &str,
    sql: &str,
    sink_id: &str,
) {
    let resp = http
        .post(format!("{base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipe_id,
            "sql": sql,
            "sinks": [{
                "id": sink_id,
                "type": "nop", "props": {"log": false},
                "common_sink_props": {},
                "encoder": {"type": "json", "props": {}}
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "pipeline {pipe_id}: {}",
        resp.text().await.unwrap_or_default()
    );
}
