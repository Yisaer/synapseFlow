mod http_sink_e2e;
mod pipeline_stats;
mod proto_decoder_e2e;
mod proto_encoder_e2e;
mod proto_schema_integration;
mod shared_stream_stats;
mod someip_explain;
mod stream_upsert;

use tokio::net::TcpListener;

pub fn http_client() -> reqwest::Client {
    sdk::install_default_crypto_provider();
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build test http client")
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
