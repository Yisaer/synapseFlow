use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix,
    write_schema_zip,
};
use flow::connector::{MemoryData, MemoryTopicKind};
use reqwest::StatusCode;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// First line from the eKuiper SOME/IP sample.
const SIP_HEX: &str = concat!(
    "00001a873e9f005800000355ab0480030000034d0200000348",
    "00460301c4f8ca99c0779029c4d0ddc9c1b251b3c50b6a9bc3f942f4c4eee865c4013c78",
    "0000000000000000000000000000000000000000470201c4cdc0e7c1ebdab4c4b291d5c2b72463c4ea231ac4035640c4cef408c412dbf700000000",
    "000000000000000000000000000000000000480301c4a62e03441f349cc4cdea9d44281f53c4bf2cf5448d9e3bc4e6e9d844921451000000000000",
    "000000000000000000000000000000000000490301c4a7a34ec228b0a9c4823692c2c82bcac4cc10b5c415735fc4a6a3f9c423edcd000000000000",
    "0000000000000000000000000000000000004a0301c47ca3d3441933d8c4a45d1d44201cf9c4960490448b20b4c4bc0fc4448e9544000000000000",
    "0000000000000000000000000000000000004b0303c42571a44415ed1fc47749b1441c6827c453596c4489c30fc49298d4448d008a010000000000",
    "0000000000000000000000000000000000004c0301c47bd983c2762bacc428d9afc2bbea59c49ff2f0c40eebfac46ce60ec417068a000000000000",
    "0000000000000000000000000000000000004d0303c3a15140440fe1f4c41df36b44167c8fc3fe9e5e44869b09c44c99fa4489e856020000000000",
    "0000000000000000000000000000000000004e0203c420b330c2c7b1cec3d1e8a5c31289dbc45e6f93c41287dac426b112c41e32e5090000000000",
    "0000000000000000000000000000000000004f0303c38ff31ac2e47ffdc288c21cc32999fdc401da3fc41803b4c395f2bbc425d8fe080000000000",
    "00000000000000000000000000000000000050030341ea0384440a6cf8c39688e044103bd2c3323fa84482d783c3fe488f4485bef9030000000000",
    "00000000000000000000000000000000000051030342045b5fc30403d9439ea9e1c3389791c38cbbbc430db1c40315d4bc43e000a0700000000000",
    "00000000000000000000000000000000000052030343a4bd6844079dde423a8897440c3d5e430ffedb44828655c30ada884484d60c040000000000",
    "00000000000000000000000000000000000053030343c5931cc316249d441a44fdc340671543222f30c420d11243c00f17c42b62f7060000000000",
    "00000000000000000000000000000000000054030344276687440267d343b654da4406b29243f41bc644801b18433747214482407805000000000000",
    "0000000000000000000000000000000000"
);

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
            &format!("    └─PhysicalSinkEncoder_4          | sink_id={sink_id}, encoder=json"),
            "      └─PhysicalProject_2            | fields=[ts; DTE_SlotID; DTE_SlotType; DTE_SlotStatus]",
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
        let slot_id = format!("{svc}.{entry}.DTE_SlotID");
        let slot_type = format!("{svc}.{entry}.DTE_SlotType");
        let slot_status = format!("{svc}.{entry}.DTE_SlotStatus");
        let columns = format!("`{slot_id}`, `{slot_type}`, `{slot_status}`");
        let col_flat = format!("{slot_id}, {slot_type}, {slot_status}");
        let project_fields = format!(
            "`{slot_id}` as {slot_id}; `{slot_type}` as {slot_type}; `{slot_status}` as {slot_status}"
        );
        let pipe_id = format!("pipe_{name}");
        let sql = format!("SELECT ts, {columns} from {name}");
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
            &format!("    └─Project_1          | fields=[ts; {project_fields}]"),
            &format!(
                "      └─DataSource_0     | source={name}, decoder=gbf, schema=[ts, {col_flat}]"
            ),
            "",
            "Physical Plan Explain:",
            "- id                                 | info",
            "  PhysicalResultCollect_5            |",
            &format!("  └─PhysicalSinkConnector_3          | sink_id={sink_id}, connector=nop"),
            &format!("    └─PhysicalSinkEncoder_4          | sink_id={sink_id}, encoder=json"),
            &format!("      └─PhysicalProject_2            | fields=[ts; {project_fields}]"),
            &format!("        └─PhysicalDecoder_1          | decoder=gbf, schema=[ts, {col_flat}]"),
            &format!(
                "          └─PhysicalDataSource_0     | source={name}, schema=[ts, {col_flat}]"
            ),
        ]
        .join("\n");
        assert_explain_eq(&explain, &expected, "case2");
    }

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn someip_pattern_decodes_runtime_output() {
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
    let runtime_instance = instance.clone();

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
    let stream_name = format!("sip_runtime_{suffix}");
    let input_topic = format!("someip_input_{suffix}");
    let output_topic = format!("someip_output_{suffix}");

    create_memory_topic(&http, &base, &input_topic).await;
    create_memory_topic(&http, &base, &output_topic).await;
    create_memory_stream(
        &http,
        &base,
        &stream_name,
        &input_topic,
        &arxml_path,
        &packet_path,
        Some("{service}.{method}.{field}"),
    )
    .await;

    let svc = "PSI_ADCC_ParkingHmiEnv_1_VLAN62_ADCC";
    let entry = "ADT_ADAS_arr_ParkingSlot";
    let col_prefix = format!("{svc}.{entry}");
    let sql = format!(
        "SELECT ts, `{col_prefix}.DTE_SlotID`, `{col_prefix}.DTE_SlotType`, `{col_prefix}.DTE_SlotStatus` from {stream_name}"
    );
    let pipeline_id = format!("pipe_{stream_name}");
    create_memory_pipeline(&http, &base, &pipeline_id, &sql, &output_topic).await;

    let mut output = runtime_instance
        .open_memory_subscribe_bytes(&output_topic)
        .expect("open output subscription");
    start_pipeline(&http, &base, &pipeline_id).await;
    runtime_instance
        .wait_for_memory_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, TEST_TIMEOUT)
        .await
        .expect("wait for memory source");

    let publisher = runtime_instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open input publisher");
    publisher
        .publish_bytes(hex::decode(SIP_HEX).expect("decode SOME/IP sample"))
        .expect("publish SOME/IP sample");

    let payload = timeout(TEST_TIMEOUT, async {
        loop {
            match output.recv().await.expect("receive memory sink output") {
                MemoryData::Bytes(payload) => break payload,
                MemoryData::Collection(_) => continue,
            }
        }
    })
    .await
    .expect("timeout waiting for SOME/IP output");
    let actual: serde_json::Value =
        serde_json::from_slice(&payload).expect("decode memory sink JSON");
    let mut expected = serde_json::json!([{ "ts": 29168173514840i64 }]);
    expected[0][format!("{col_prefix}.DTE_SlotID")] = serde_json::json!(70);
    expected[0][format!("{col_prefix}.DTE_SlotType")] = serde_json::json!(3);
    expected[0][format!("{col_prefix}.DTE_SlotStatus")] = serde_json::json!(1);
    assert_eq!(actual, expected);

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
    let schema_path = complete_gbf_schema_path(name, arxml_path, packet_path, signal_name_pattern);
    let schema_name = format!("{name}_schema");
    install_gbf_schema(http, base, &schema_name, &schema_path).await;
    let resp = http
        .post(format!("{base}/streams"))
        .json(&serde_json::json!({
            "name": name, "revision": 1, "type": "mqtt",
            "schema": {"ref": schema_name},
            "props": {"broker_url": "tcp://127.0.0.1:1883", "topic": "x", "qos": 0},
            "decoder": {"type": "gbf", "props": {}}
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
            "revision": 1,
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

async fn create_memory_topic(http: &reqwest::Client, base: &str, topic: &str) {
    let resp = http
        .post(format!("{base}/memory/topics"))
        .json(&serde_json::json!({
            "topic": topic,
            "revision": 1,
            "kind": "bytes",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "memory topic {topic}: {}",
        resp.text().await.unwrap_or_default()
    );
}

async fn create_memory_stream(
    http: &reqwest::Client,
    base: &str,
    name: &str,
    input_topic: &str,
    arxml_path: &str,
    packet_path: &str,
    signal_name_pattern: Option<&str>,
) {
    let schema_path = complete_gbf_schema_path(name, arxml_path, packet_path, signal_name_pattern);
    let schema_name = format!("{name}_schema");
    install_gbf_schema(http, base, &schema_name, &schema_path).await;
    let resp = http
        .post(format!("{base}/streams"))
        .json(&serde_json::json!({
            "name": name, "revision": 1, "type": "memory",
            "schema": {"ref": schema_name},
            "props": {"topic": input_topic},
            "decoder": {"type": "gbf", "props": {}}
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

fn complete_gbf_schema_path(
    name: &str,
    arxml_path: &str,
    packet_path: &str,
    signal_name_pattern: Option<&str>,
) -> String {
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(packet_path).expect("read GBF packet layout"))
            .expect("parse GBF packet layout");
    let format_props = serde_json::json!({"arxml_path": "format/system.arxml"});
    if let Some(pattern) = signal_name_pattern {
        document["signal_name_pattern"] = serde_json::json!(pattern);
    }
    document["format"] = serde_json::json!({"type": "someip", "props": format_props});
    let entry = serde_json::to_vec_pretty(&document).expect("encode complete GBF schema");
    let arxml = std::fs::read(arxml_path).expect("read private ARXML source");
    let archive = std::env::temp_dir().join(format!("veloflux-{name}.zip"));
    write_schema_zip(
        &archive,
        &[
            ("someip_packet.json", &entry),
            ("someip_packet/format/system.arxml", &arxml),
        ],
    );
    archive.to_string_lossy().into_owned()
}

async fn install_gbf_schema(http: &reqwest::Client, base: &str, name: &str, archive_path: &str) {
    let resp = http
        .post(format!("{base}/schemas"))
        .json(&serde_json::json!({
            "name": name,
            "revision": 1,
            "type": "gbf",
            "props": {"schema_path": archive_path}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "schema {name}: {}",
        resp.text().await.unwrap_or_default()
    );
}

async fn create_memory_pipeline(
    http: &reqwest::Client,
    base: &str,
    pipe_id: &str,
    sql: &str,
    output_topic: &str,
) {
    let resp = http
        .post(format!("{base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipe_id,
            "revision": 1,
            "sql": sql,
            "sinks": [{
                "id": format!("{pipe_id}_sink"),
                "type": "memory", "props": {"topic": output_topic},
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

async fn start_pipeline(http: &reqwest::Client, base: &str, pipe_id: &str) {
    let resp = http
        .post(format!("{base}/pipelines/{pipe_id}/start"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "start pipeline {pipe_id}: {}",
        resp.text().await.unwrap_or_default()
    );
}
