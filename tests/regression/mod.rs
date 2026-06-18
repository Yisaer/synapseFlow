mod concurrent_pipeline_lifecycle;
mod flow_instance_binding;
mod lifecycle_while_dataflow;
mod multi_in_process_flow_instances;
mod pipeline_build_context;
mod resource_cleanup;
mod shared_stream_lifecycle;
mod shared_stream_stats;
mod status;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use sdk::{ClientConfig, ManagerClient};
use serde_json::Value as JsonValue;

pub fn http_client() -> reqwest::Client {
    sdk::install_default_crypto_provider();
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build test http client")
}

pub fn make_client(addr: SocketAddr) -> ManagerClient {
    let base_url = format!("http://{}", addr).parse().expect("base_url");
    ManagerClient::new(ClientConfig::new(base_url)).expect("create client")
}

pub fn random_suffix() -> String {
    use rand::{distributions::Alphanumeric, Rng};
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

pub async fn bind_manager_listener_or_skip() -> Option<TcpListener> {
    match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping test: binding a local listener is not permitted: {err}");
            None
        }
        Err(err) => panic!("bind manager listener: {err}"),
    }
}

pub fn default_flow_instances() -> Vec<manager::FlowInstanceSpec> {
    vec![manager::FlowInstanceSpec {
        id: "default".to_string(),
        ..manager::FlowInstanceSpec::default()
    }]
}

pub async fn wait_for_server(client: &sdk::ManagerClient) {
    let mut last_err = None;
    for _ in 0..300 {
        match client.list_pipelines().await {
            Ok(_) => return,
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not start within 30s: {:?}", last_err.unwrap());
}

pub struct ManagerHarness {
    _temp_dir: tempfile::TempDir,
    server: JoinHandle<()>,
    pub client: ManagerClient,
    pub http: reqwest::Client,
    pub base: String,
    pub injector: flow::FlowInstance,
}

impl ManagerHarness {
    pub async fn new() -> Option<Self> {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage =
            storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
        let instance = manager::new_default_flow_instance();
        let injector = instance.clone();

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
        wait_for_server(&client).await;
        let http = http_client();

        Some(Self {
            _temp_dir: temp_dir,
            server,
            client,
            http,
            base: format!("http://{addr}"),
            injector,
        })
    }
}

impl Drop for ManagerHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub fn records_value(entry: &JsonValue, field: &str) -> u64 {
    entry["stats"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("missing numeric stats field {field} in {entry}"))
}

pub async fn wait_for_pipeline_activity(
    http: &reqwest::Client,
    base: &str,
    pipeline_id: &str,
    timeout_duration: Duration,
) -> Vec<JsonValue> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let resp = http
            .get(format!("{base}/pipelines/{pipeline_id}/stats"))
            .send()
            .await
            .expect("collect pipeline stats request");
        if resp.status().is_success() {
            let stats = resp
                .json::<Vec<JsonValue>>()
                .await
                .expect("decode pipeline stats");
            if stats.iter().any(|entry| {
                records_value(entry, "records_in") > 0 || records_value(entry, "records_out") > 0
            }) {
                return stats;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pipeline {pipeline_id} did not report runtime activity before timeout"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for_shared_stream_status(
    client: &ManagerClient,
    stream_name: &str,
    expected: &str,
    timeout_duration: Duration,
) -> JsonValue {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let stats = client
            .shared_stream_stats_in_instance(stream_name, Some("default"))
            .await
            .expect("shared stream stats");
        if stats["status"] == expected {
            return stats;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shared stream {stream_name} did not become {expected}: {stats}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
