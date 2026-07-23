use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Once;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

pub fn write_schema_zip(path: &std::path::Path, files: &[(&str, &[u8])]) {
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

/// Port for integration tests (avoid conflicts with local services)
const TEST_PORT: u16 = 19082;

/// MQTT broker port (external broker required)
pub const MQTT_PORT: u16 = 1883;

/// Global singleton test environment instance
static TEST_ENV: OnceLock<TestEnvironment> = OnceLock::new();
static INSTALL_PROVIDER: Once = Once::new();

fn install_default_crypto_provider() {
    INSTALL_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Check if MQTT broker is available, panic with clear message if not.
fn check_mqtt_available() {
    let addr = format!("127.0.0.1:{}", MQTT_PORT);
    match TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(2)) {
        Ok(_) => {}
        Err(e) => {
            panic!(
                "MQTT broker not available at {}. Please start an MQTT broker (e.g., `docker run -d -p 1883:1883 emqx/nanomq`) before running integration tests. Error: {}",
                addr, e
            );
        }
    }
}

/// Get the singleton test environment, starting it if necessary.
pub fn get_server() -> &'static TestEnvironment {
    TEST_ENV.get_or_init(|| {
        check_mqtt_available();
        TestEnvironment::start()
    })
}

use std::sync::Mutex;

/// Test environment that manages the application server.
/// Uses singleton pattern - all tests share one environment instance.
/// Requires external MQTT broker on port 1883.
#[allow(dead_code)]
pub struct TestEnvironment {
    server_process: Mutex<Child>,
    pub base_url: String,
    stopped: AtomicBool,
}

#[allow(dead_code)]
impl TestEnvironment {
    fn config_path() -> PathBuf {
        static CONFIG_PATH: OnceLock<PathBuf> = OnceLock::new();
        CONFIG_PATH
            .get_or_init(|| {
                let path = std::env::temp_dir().join(format!(
                    "veloflux-sdv-test-config-{}.yaml",
                    std::process::id()
                ));
                let config = r#"server:
  flow_instances:
    - id: default
      backend: in_process
"#;
                std::fs::write(&path, config).expect("write sdv test config");
                path
            })
            .clone()
    }

    fn binary_path() -> std::path::PathBuf {
        if let Ok(path) = std::env::var("CARGO_BIN_EXE_veloflux") {
            return std::path::PathBuf::from(path);
        }

        let current_exe = std::env::current_exe().expect("resolve current test binary");
        let profile_dir = current_exe
            .parent()
            .and_then(|deps| deps.parent())
            .expect("resolve target profile directory");
        let candidate = profile_dir.join("veloflux");
        if candidate.exists() {
            return candidate;
        }

        #[cfg(windows)]
        {
            let candidate = profile_dir.join("veloflux.exe");
            if candidate.exists() {
                return candidate;
            }
        }

        panic!(
            "root veloflux binary not found under {}. Build it before running distro tests.",
            profile_dir.display()
        );
    }

    /// Start the test environment.
    fn start() -> Self {
        let binary_path = Self::binary_path();
        let config_path = Self::config_path();

        // Start the server process
        let process = Command::new(&binary_path)
            .arg("--config")
            .arg(&config_path)
            .env(
                "VELOFLUX_SERVER__MANAGER_ADDR",
                format!("0.0.0.0:{}", TEST_PORT),
            )
            .env(
                "VELOFLUX_METRICS__ADDR",
                format!("0.0.0.0:{}", TEST_PORT + 1),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start server");

        let base_url = format!("http://127.0.0.1:{}", TEST_PORT);

        // Wait for server to be ready
        install_default_crypto_provider();
        let client = reqwest::blocking::Client::new();
        let health_url = format!("{}/ping", base_url);
        for _ in 0..300 {
            thread::sleep(Duration::from_millis(100));
            if client.get(&health_url).send().is_ok() {
                return Self {
                    server_process: Mutex::new(process),
                    base_url,
                    stopped: AtomicBool::new(false),
                };
            }
        }

        panic!("server did not become ready within 30s — check binary path or port conflict");
    }

    /// Get the MQTT broker address
    pub fn mqtt_addr() -> String {
        format!("tcp://127.0.0.1:{}", MQTT_PORT)
    }

    /// Stop the server instance gracefully to allow coverage data to be flushed.
    /// This is idempotent - can be called multiple times safely.
    /// Note: Must be called explicitly at the end of tests since static singletons
    /// don't get dropped at program exit.
    pub fn stop(&self) {
        // Only stop once using compare_exchange
        if self
            .stopped
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // Already stopped
        }

        let mut process = self.server_process.lock().unwrap();
        // Send SIGTERM to allow graceful shutdown
        // Note: Child::kill() sends SIGKILL which prevents coverage flush
        let _ = Command::new("kill").arg(process.id().to_string()).status();

        let _ = process.wait();
    }
}

/// HTTP client wrapper for API testing
pub struct ApiClient {
    client: reqwest::blocking::Client,
    base_url: String,
}

#[allow(dead_code)]
impl ApiClient {
    pub fn new(base_url: &str) -> Self {
        install_default_crypto_provider();
        Self {
            client: reqwest::blocking::Client::new(),
            base_url: base_url.to_string(),
        }
    }

    pub fn post_json(&self, path: &str, body: &serde_json::Value) -> reqwest::blocking::Response {
        self.client
            .post(format!("{}{}", self.base_url, path))
            .json(body)
            .send()
            .expect("request failed")
    }

    pub fn delete(&self, path: &str) -> reqwest::blocking::Response {
        self.client
            .delete(format!("{}{}", self.base_url, path))
            .send()
            .expect("request failed")
    }

    pub fn get(&self, path: &str) -> reqwest::blocking::Response {
        self.client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .expect("request failed")
    }

    /// Fetch pipeline stats.
    pub fn get_pipeline_stats(&self, pipeline_id: &str) -> Option<serde_json::Value> {
        let resp = self.get(&format!("/pipelines/{}/stats?timeout_ms=5000", pipeline_id));
        if resp.status().is_success() {
            Some(resp.json().expect("invalid stats JSON"))
        } else {
            None
        }
    }

    /// Fetch pipeline stats and verify data was processed correctly.
    /// Returns the stats response if successful, panics if:
    /// - Stats API call fails
    /// - Any processor has non-zero error
    /// - No records were processed (records_in == 0 for all processors)
    pub fn verify_pipeline_stats(&self, pipeline_id: &str) -> serde_json::Value {
        let stats = self
            .get_pipeline_stats(pipeline_id)
            .unwrap_or_else(|| panic!("stats API failed for pipeline {pipeline_id}"));
        if let Some(processors) = stats.as_array() {
            let mut total_records_in = 0u64;
            for proc in processors {
                // Check for errors
                if let Some(error) = proc
                    .get("stats")
                    .and_then(|s| s.get("error"))
                    .and_then(|e| e.as_str())
                {
                    panic!(
                        "Processor {} has error: {}",
                        proc.get("processor_id").unwrap_or(&serde_json::Value::Null),
                        error
                    );
                }
                // Accumulate records_in
                if let Some(records_in) = proc
                    .get("stats")
                    .and_then(|s| s.get("records_in"))
                    .and_then(|r| r.as_u64())
                {
                    total_records_in += records_in;
                }
            }
            assert!(
                total_records_in > 0,
                "No records processed - check pipeline configuration"
            );
        }
        stats
    }
}
