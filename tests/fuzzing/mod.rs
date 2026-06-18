mod explain_consistency;
mod import_export;
mod property_fuzz;
mod semantic_contract;
mod source_input_mode;
mod sql_fuzz;

use std::net::SocketAddr;
use tokio::net::TcpListener;

use sdk::{ClientConfig, ManagerClient};

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
    use std::time::Duration;
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
