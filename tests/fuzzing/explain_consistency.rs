use sdk::{ManagerClient, PipelineCreateRequest, SdkError, StreamCreateRequest};
use serde_json::json;
use std::net::SocketAddr;

use super::{
    bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix,
    wait_for_server,
};

struct TestHarness {
    _temp_dir: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    client: ManagerClient,
    http: reqwest::Client,
}

impl TestHarness {
    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn new() -> Option<Self> {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage =
            storage::StorageManager::new(temp_dir.path()).expect("create storage manager");

        let instance = manager::new_default_flow_instance();

        let Some(listener) = bind_manager_listener_or_skip().await else {
            return None;
        };

        let addr = listener.local_addr().expect("read listener addr");

        let server = tokio::spawn(async move {
            manager::start_server_with_listener(
                listener,
                instance,
                storage,
                default_flow_instances(),
            )
            .await
            .expect("start manager server");
        });

        let client = make_client(addr);
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build reqwest client");

        wait_for_server(&client).await;

        Some(Self {
            _temp_dir: temp_dir,
            server,
            addr,
            client,
            http,
        })
    }

    async fn explain_pipeline(&self, pipeline_id: &str) -> (reqwest::StatusCode, String) {
        let resp = self
            .http
            .get(format!("{}/pipelines/{}/explain", self.base(), pipeline_id))
            .send()
            .await
            .expect("send explain request");

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        (status, body)
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn basic_mock_stream_req(name: String) -> StreamCreateRequest {
    StreamCreateRequest {
        name,
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" },
                    { "name": "flag", "data_type": "bool" },
                    { "name": "user_id", "data_type": "string" }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    }
}

fn assert_http_status(err: SdkError, expected: u16) -> String {
    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), expected, "unexpected body: {body}");
            body
        }
        other => panic!("expected HTTP {expected}, got: {other:?}"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorClass {
    Parser,
    ResolveStream,
    Logical,
    Physical,
}

fn classify_error(body: &str) -> ErrorClass {
    let lower = body.to_lowercase();

    if lower.contains("parse") || lower.contains("syntax") {
        ErrorClass::Parser
    } else if lower.contains("missing from storage")
        || (lower.contains("stream") && lower.contains("missing"))
        || lower.contains("not found in catalog")
    {
        ErrorClass::ResolveStream
    } else if lower.contains("sink")
        || lower.contains("connector")
        || lower.contains("encoder")
        || lower.contains("not registered")
    {
        ErrorClass::Physical
    } else if lower.contains("column")
        || lower.contains("schema")
        || lower.contains("no schema bindings")
        || lower.contains("failed to compile expression")
        || lower.contains("failed to convert")
        || lower.contains("having")
        || lower.contains("logical")
    {
        ErrorClass::Logical
    } else {
        panic!("unclassified error body: {body}");
    }
}

fn plan_lines(body: &str, section_header: &str) -> Vec<String> {
    let mut in_section = false;
    let mut lines = Vec::new();

    for line in body.lines() {
        let lower = line.to_lowercase();

        if lower.contains(&section_header.to_lowercase()) {
            in_section = true;
            continue;
        }

        if in_section
            && (lower.contains("logical plan explain") || lower.contains("physical plan explain"))
            && !lower.contains(&section_header.to_lowercase())
        {
            break;
        }

        if in_section {
            lines.push(line.to_string());
        }
    }

    lines
}

fn normalize_plan_topology(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| {
            let lower = line.to_lowercase();

            let node = [
                "physicalresultcollect",
                "physicalsinkconnector",
                "physicalsinkencoder",
                "physicalproject",
                "physicalfilter",
                "physicaldecoder",
                "physicaldatasource",
                "tail",
                "datasink",
                "project",
                "filter",
                "datasource",
            ]
            .iter()
            .find(|node| lower.contains(**node))?;

            Some((*node).to_string())
        })
        .collect()
}

fn logical_topology(body: &str) -> Vec<String> {
    normalize_plan_topology(&plan_lines(body, "logical plan explain"))
}

fn physical_topology(body: &str) -> Vec<String> {
    normalize_plan_topology(&plan_lines(body, "physical plan explain"))
}

async fn create_and_explain(
    h: &TestHarness,
    pipeline_id: String,
    sql: String,
) -> (reqwest::StatusCode, String) {
    h.client
        .create_pipeline(&PipelineCreateRequest::nop(pipeline_id.clone(), sql))
        .await
        .expect("create pipeline");

    h.explain_pipeline(&pipeline_id).await
}

async fn create_pipeline_with_bad_encoder(
    h: &TestHarness,
    pipeline_id: String,
    sql: String,
) -> (reqwest::StatusCode, String) {
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&json!({
            "id": pipeline_id,
            "sql": sql,
            "sinks": [
                {
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "not_registered_encoder",
                        "props": {}
                    }
                }
            ]
        }))
        .send()
        .await
        .expect("send raw create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

#[test]
fn supported_sql_fragments_match_expected_logical_explain_shape() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("explain_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");

        let sql_variants = vec![
            format!("SELECT value FROM {stream_name} WHERE value > 0"),
            format!("SELECT value FROM {stream_name} WHERE value > 1"),
            format!("SELECT value FROM {stream_name} WHERE value = 1"),
        ];

        let mut topologies = Vec::new();

        for sql in sql_variants {
            let pipeline_id = format!("explain_logical_{}", random_suffix());
            let (status, body) = create_and_explain(&h, pipeline_id, sql).await;

            assert_eq!(status.as_u16(), 200, "unexpected explain body: {body}");
            assert!(!body.trim().is_empty(), "explain body should not be empty");

            let topology = logical_topology(&body);

            assert_eq!(
                topology,
                vec!["tail", "datasink", "project", "filter", "datasource"],
                "unexpected logical topology:\n{body}"
            );

            topologies.push(topology);
        }

        assert!(
            topologies.windows(2).all(|pair| pair[0] == pair[1]),
            "logical topology should be stable across equivalent supported SQL"
        );
    });
}

#[test]
fn supported_sql_fragments_match_expected_physical_explain_shape() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("explain_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");

        let sql_variants = vec![
            format!("SELECT value FROM {stream_name} WHERE value > 0"),
            format!("SELECT value FROM {stream_name} WHERE value > 1"),
            format!("SELECT value FROM {stream_name} WHERE value = 1"),
        ];

        let mut topologies = Vec::new();

        for sql in sql_variants {
            let pipeline_id = format!("explain_physical_{}", random_suffix());
            let (status, body) = create_and_explain(&h, pipeline_id, sql).await;

            assert_eq!(status.as_u16(), 200, "unexpected explain body: {body}");
            assert!(!body.trim().is_empty(), "explain body should not be empty");

            let topology = physical_topology(&body);

            assert_eq!(
                topology,
                vec![
                    "physicalresultcollect",
                    "physicalsinkconnector",
                    "physicalsinkencoder",
                    "physicalproject",
                    "physicalfilter",
                    "physicaldecoder",
                    "physicaldatasource",
                ],
                "unexpected physical topology:\n{body}"
            );

            topologies.push(topology);
        }

        assert!(
            topologies.windows(2).all(|pair| pair[0] == pair[1]),
            "physical topology should be stable across equivalent supported SQL"
        );
    });
}

#[test]
fn unsupported_sql_fragments_fail_with_stable_errors() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("explain_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");

        for _ in 0..3 {
            let pipeline_id = format!("stable_logical_fail_{}", random_suffix());

            let err = h
                .client
                .create_pipeline(&PipelineCreateRequest::nop(
                    pipeline_id,
                    format!("SELECT missing_col FROM {stream_name}"),
                ))
                .await
                .expect_err("logical failure should return error");

            let body = assert_http_status(err, 400);

            assert_eq!(
                classify_error(&body),
                ErrorClass::Logical,
                "expected stable logical error classification, body: {body}"
            );
        }

        for _ in 0..3 {
            let pipeline_id = format!("stable_physical_fail_{}", random_suffix());

            let (status, body) = create_pipeline_with_bad_encoder(
                &h,
                pipeline_id,
                format!("SELECT value FROM {stream_name}"),
            )
            .await;

            assert_eq!(
                status.as_u16(),
                400,
                "expected physical failure HTTP 400, body: {body}"
            );

            assert_eq!(
                classify_error(&body),
                ErrorClass::Physical,
                "expected stable physical error classification, body: {body}"
            );
        }
    });
}
