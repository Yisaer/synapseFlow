use reqwest::StatusCode;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::TcpListener;

struct CurrentDirGuard(PathBuf);

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.0).expect("restore current directory");
    }
}

fn write_schema_zip(path: &Path, files: &[(&str, &[u8])]) {
    use std::io::Write;

    let file = std::fs::File::create(path).expect("create schema zip");
    let mut archive = zip::ZipWriter::new(file);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    for (name, content) in files {
        archive
            .start_file(*name, options)
            .expect("start schema zip entry");
        archive.write_all(content).expect("write schema zip entry");
    }
    archive.finish().expect("finish schema zip");
}

fn build_gbf_schema_archive(temp_dir: &Path) -> PathBuf {
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("distros/sdv/src/tests");
    let mut document: Value = serde_json::from_slice(
        &std::fs::read(source_root.join("spi_packet.json")).expect("read GBF packet layout"),
    )
    .expect("parse GBF packet layout");
    document["format"] = json!({
        "type": "can",
        "props": {
            "dbc_path": "format/sim.json",
            "can_id_mapping": {"mode": "bus_shift", "bits": 12}
        }
    });
    let entry = serde_json::to_vec(&document).expect("encode GBF schema");
    let dbc = std::fs::read(source_root.join("sim.json")).expect("read CAN schema");
    let archive = temp_dir.join("vehicle.zip");
    write_schema_zip(
        &archive,
        &[("vehicle.json", &entry), ("vehicle/format/sim.json", &dbc)],
    );
    archive
}

async fn start_manager(storage: storage::StorageManager) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind manager listener");
    let addr = listener.local_addr().expect("manager listener address");
    let instance = manager::new_default_flow_instance();
    veloflux_sdv::register(&instance);
    let server = tokio::spawn(async move {
        manager::start_server_with_listener(
            listener,
            instance,
            storage,
            vec![manager::FlowInstanceSpec {
                id: manager::DEFAULT_FLOW_INSTANCE_ID.to_string(),
                ..manager::FlowInstanceSpec::default()
            }],
        )
        .await
        .expect("start manager");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (format!("http://{addr}"), server)
}

async fn create_gbf_stream(
    http: &reqwest::Client,
    base: &str,
    stream_name: &str,
    schema_name: &str,
) {
    let response = http
        .post(format!("{base}/streams"))
        .json(&json!({
            "name": stream_name,
            "revision": 1,
            "type": "mqtt",
            "schema": {"ref": schema_name},
            "props": {
                "broker_url": "tcp://127.0.0.1:1883",
                "topic": stream_name,
                "qos": 0
            },
            "decoder": {"type": "gbf", "props": {}}
        }))
        .send()
        .await
        .expect("create GBF stream request");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "create stream {stream_name}: {}",
        response.text().await.unwrap_or_default()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_import_restores_file_backed_gbf_schema_artifact() {
    sdk::install_default_crypto_provider();
    let original_current_dir = std::env::current_dir().expect("read current directory");
    let decoy_dir = tempfile::tempdir().expect("decoy current directory");
    std::env::set_current_dir(decoy_dir.path()).expect("set isolated current directory");
    let _current_dir_guard = CurrentDirGuard(original_current_dir);
    std::fs::write(
        decoy_dir.path().join("vehicle.json"),
        b"not the archived GBF schema",
    )
    .expect("write CWD schema decoy");

    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build HTTP client");
    let source_dir = tempfile::tempdir().expect("source data dir");
    let source_storage =
        storage::StorageManager::new(source_dir.path()).expect("source storage manager");
    let (source_base, source_server) = start_manager(source_storage).await;
    let schema_archive = build_gbf_schema_archive(source_dir.path());
    let schema_name = "vehicle_schema";

    let response = http
        .post(format!("{source_base}/schemas"))
        .json(&json!({
            "name": schema_name,
            "revision": 1,
            "type": "gbf",
            "props": {"schema_path": schema_archive}
        }))
        .send()
        .await
        .expect("create GBF schema request");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "create GBF schema: {}",
        response.text().await.unwrap_or_default()
    );
    create_gbf_stream(&http, &source_base, "vehicle_stream", schema_name).await;

    let response = http
        .get(format!(
            "{source_base}/storage/export?bundle_version=test-bundle-1"
        ))
        .send()
        .await
        .expect("export storage");
    assert_eq!(response.status(), StatusCode::OK);
    let archive = response.bytes().await.expect("read export archive");
    source_server.abort();
    let _ = source_server.await;

    let target_dir = tempfile::tempdir().expect("target data dir");
    let target_storage =
        storage::StorageManager::new(target_dir.path()).expect("target storage manager");
    let (target_base, target_server) = start_manager(target_storage).await;
    let response = http
        .post(format!("{target_base}/import"))
        .multipart(
            reqwest::multipart::Form::new().part(
                "file",
                reqwest::multipart::Part::bytes(archive.to_vec())
                    .file_name("veloflux-export.zip")
                    .mime_str("application/zip")
                    .expect("valid ZIP MIME type"),
            ),
        )
        .send()
        .await
        .expect("import storage");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "import GBF schema: {}",
        response.text().await.unwrap_or_default()
    );

    assert!(target_dir
        .path()
        .join("schemas/gbf/vehicle_schema/vehicle.json")
        .is_file());
    create_gbf_stream(&http, &target_base, "restored_vehicle_stream", schema_name).await;

    target_server.abort();
    let _ = target_server.await;
}
