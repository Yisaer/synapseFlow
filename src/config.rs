use crate::server::ServerOptions;
use flow::secret::SecretString;
use flow::{validate_property_key, PropertyContext};
use manager::FlowInstanceSpec;
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::Path;

mod env;

type ConfigResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub properties: PropertiesConfig,
    pub logging: LoggingConfig,
    pub profiling: ProfilingConfig,
    pub metrics: MetricsConfig,
    pub server: ServerConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            properties: PropertiesConfig::default(),
            logging: LoggingConfig::default(),
            profiling: ProfilingConfig {
                enabled: None,
                addr: None,
                cpu_profile_freq_hz: None,
            },
            metrics: MetricsConfig {
                poll_interval_secs: None,
            },
            server: ServerConfig::default(),
        }
    }
}

/// Plaintext-on-input static properties with redacted in-memory diagnostics.
#[derive(Clone, Default)]
pub struct PropertiesConfig {
    values: BTreeMap<String, SecretString>,
}

impl PropertiesConfig {
    pub(crate) fn insert_env(&mut self, key: String, value: String) {
        self.values.insert(key, SecretString::new(value));
    }

    fn context(&self) -> PropertyContext {
        PropertyContext::new(self.values.clone())
    }
}

impl fmt::Debug for PropertiesConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertiesConfig")
            .field("keys", &self.values.keys().collect::<Vec<_>>())
            .field("count", &self.values.len())
            .finish()
    }
}

impl<'de> Deserialize<'de> for PropertiesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = BTreeMap::<String, String>::deserialize(deserializer)?;
        let mut properties = BTreeMap::new();
        for (key, value) in values {
            validate_property_key(&key).map_err(serde::de::Error::custom)?;
            properties.insert(key, SecretString::new(value));
        }
        Ok(Self { values: properties })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LoggingConfig {
    pub output: LoggingOutput,
    pub level: LogLevel,
    pub include_source: bool,
    pub disable_timestamp: bool,
    pub file: FileLoggingConfig,
    pub syslog: SyslogLoggingConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            output: LoggingOutput::Stdout,
            level: LogLevel::Info,
            include_source: true,
            disable_timestamp: false,
            file: FileLoggingConfig::default(),
            syslog: SyslogLoggingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoggingOutput {
    Stdout,
    File,
    Syslog,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FileLoggingConfig {
    pub dir: String,
    pub file_name: String,
    pub rotation: LogRotationConfig,
}

impl Default for FileLoggingConfig {
    fn default() -> Self {
        Self {
            dir: "./logs".to_string(),
            file_name: "app.log".to_string(),
            rotation: LogRotationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SyslogLoggingConfig {
    pub enable: bool,
    pub level: Option<LogLevel>,
    pub tag: String,
    pub network: String,
    pub address: String,
}

impl Default for SyslogLoggingConfig {
    fn default() -> Self {
        Self {
            enable: false,
            level: None,
            tag: "veloflux".to_string(),
            network: String::new(),
            address: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LogRotationConfig {
    pub keep_days: u64,
    pub max_num: u64,
    pub max_size_mb: u64,
}

impl Default for LogRotationConfig {
    fn default() -> Self {
        Self {
            keep_days: 7,
            max_num: 30,
            max_size_mb: 256,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ProfilingConfig {
    pub enabled: Option<bool>,
    pub addr: Option<String>,
    pub cpu_profile_freq_hz: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub poll_interval_secs: Option<u64>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: Some(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub manager_addr: Option<String>,
    /// Pipeline patrol scheduler interval in seconds. 0 disables the scheduler.
    /// Default: 15.
    #[serde(default = "default_patrol_interval_secs")]
    pub pipeline_patrol_interval_secs: u64,
    #[serde(default = "default_flow_instances")]
    pub flow_instances: Vec<FlowInstanceSpec>,
}

fn default_patrol_interval_secs() -> u64 {
    15
}

fn default_flow_instances() -> Vec<FlowInstanceSpec> {
    vec![FlowInstanceSpec {
        id: manager::DEFAULT_FLOW_INSTANCE_ID.to_string(),
        ..FlowInstanceSpec::default()
    }]
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            manager_addr: Some(crate::server::DEFAULT_MANAGER_ADDR.to_string()),
            pipeline_patrol_interval_secs: default_patrol_interval_secs(),
            flow_instances: default_flow_instances(),
        }
    }
}

impl AppConfig {
    pub fn load_required(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .map_err(|err| format!("failed to read config file {}: {}", path.display(), err))?;
        let mut cfg: AppConfig = serde_yaml::from_str(&raw)
            .map_err(|err| format!("failed to parse yaml config {}: {}", path.display(), err))?;
        env::apply_env_overrides(&mut cfg)?;
        Ok(cfg)
    }

    pub fn load_optional(path: impl AsRef<Path>) -> ConfigResult<Option<Self>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(Self::load_required(path)?))
    }

    pub fn load_default_with_env() -> ConfigResult<Self> {
        let mut cfg = Self::default();
        env::apply_env_overrides(&mut cfg)?;
        Ok(cfg)
    }

    pub fn apply_to_server_options(&self, opts: &mut ServerOptions) {
        if let Some(enabled) = self.profiling.enabled {
            opts.profiling_enabled = Some(enabled);
        }
        if let Some(addr) = self.profiling.addr.as_ref() {
            opts.profile_addr = Some(addr.clone());
        }
        if let Some(freq_hz) = self.profiling.cpu_profile_freq_hz {
            opts.cpu_profile_freq_hz = Some(freq_hz);
        }
        if let Some(secs) = self.metrics.poll_interval_secs {
            opts.metrics_poll_interval_secs = Some(secs);
        }
        opts.pipeline_patrol_interval_secs = Some(self.server.pipeline_patrol_interval_secs);
        if let Some(addr) = self.server.manager_addr.as_ref() {
            opts.manager_addr = Some(addr.clone());
        }
        opts.flow_instances = self.server.flow_instances.clone();
        opts.property_context = self.properties.context();
    }

    pub fn to_server_options(&self) -> ServerOptions {
        let mut opts = ServerOptions::default();
        self.apply_to_server_options(&mut opts);
        opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_MUTEX: Mutex<()> = Mutex::new(());
    const ENV_LOGGING_OUTPUT: &str = "VELOFLUX_LOGGING__OUTPUT";
    const ENV_LOGGING_LEVEL: &str = "VELOFLUX_LOGGING__LEVEL";
    const ENV_LOGGING_INCLUDE_SOURCE: &str = "VELOFLUX_LOGGING__INCLUDE_SOURCE";
    const ENV_LOGGING_DISABLE_TIMESTAMP: &str = "VELOFLUX_LOGGING__DISABLE_TIMESTAMP";
    const ENV_LOGGING_SYSLOG_ENABLE: &str = "VELOFLUX_LOGGING__SYSLOG__ENABLE";
    const ENV_LOGGING_SYSLOG_LEVEL: &str = "VELOFLUX_LOGGING__SYSLOG__LEVEL";
    const ENV_LOGGING_SYSLOG_TAG: &str = "VELOFLUX_LOGGING__SYSLOG__TAG";
    const ENV_LOGGING_SYSLOG_NETWORK: &str = "VELOFLUX_LOGGING__SYSLOG__NETWORK";
    const ENV_LOGGING_SYSLOG_ADDRESS: &str = "VELOFLUX_LOGGING__SYSLOG__ADDRESS";
    const ENV_PROFILING_ADDR: &str = "VELOFLUX_PROFILING__ADDR";
    const ENV_METRICS_POLL_INTERVAL_SECS: &str = "VELOFLUX_METRICS__POLL_INTERVAL_SECS";
    const ENV_SERVER_MANAGER_ADDR: &str = "VELOFLUX_SERVER__MANAGER_ADDR";

    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("veloflux_test.{}.{}.yaml", name, nanos))
    }

    struct EnvTestGuard {
        _lock: MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvTestGuard {
        fn new() -> Self {
            let lock = ENV_MUTEX.lock().expect("lock env mutex");
            let supported = super::env::supported_env_var_names();
            let mut saved = Vec::with_capacity(supported.len());
            for key in supported {
                saved.push((key, std::env::var_os(key)));
                std::env::remove_var(key);
            }
            Self { _lock: lock, saved }
        }

        fn set(&mut self, key: &'static str, value: &str) {
            std::env::set_var(key, value);
        }

        fn set_any(&mut self, key: &'static str, value: &str) {
            if !self.saved.iter().any(|(saved_key, _)| *saved_key == key) {
                self.saved.push((key, std::env::var_os(key)));
            }
            std::env::set_var(key, value);
        }
    }

    impl Drop for EnvTestGuard {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn loads_optional_missing_file() {
        let path = unique_temp_path("missing");
        let loaded = AppConfig::load_optional(&path).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn applies_only_present_fields() {
        let _env = EnvTestGuard::new();
        let yaml = r#"
profiling:
  enabled: true
server:
  manager_addr: "127.0.0.1:9999"
"#;
        let path = unique_temp_path("apply");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        let mut opts = ServerOptions::default();
        cfg.apply_to_server_options(&mut opts);

        assert_eq!(opts.profiling_enabled, Some(true));
        assert_eq!(opts.manager_addr.as_deref(), Some("127.0.0.1:9999"));
        assert!(opts.profile_addr.is_none());
        assert!(opts.metrics_poll_interval_secs.is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn loads_default_with_env_overrides() {
        let mut env = EnvTestGuard::new();
        env.set(ENV_LOGGING_LEVEL, "debug");
        env.set(ENV_LOGGING_INCLUDE_SOURCE, "false");
        env.set(ENV_LOGGING_DISABLE_TIMESTAMP, "true");
        env.set(ENV_LOGGING_SYSLOG_ENABLE, "true");
        env.set(ENV_LOGGING_SYSLOG_LEVEL, "warn");
        env.set(ENV_LOGGING_SYSLOG_TAG, "vf-env");
        env.set(ENV_LOGGING_SYSLOG_NETWORK, "");
        env.set(ENV_LOGGING_SYSLOG_ADDRESS, "");
        env.set(ENV_PROFILING_ADDR, "127.0.0.1:16060");
        env.set(ENV_METRICS_POLL_INTERVAL_SECS, "30");
        env.set(ENV_SERVER_MANAGER_ADDR, "127.0.0.1:18080");

        let cfg = AppConfig::load_default_with_env().unwrap();

        assert!(matches!(cfg.logging.level, LogLevel::Debug));
        assert!(!cfg.logging.include_source);
        assert!(cfg.logging.disable_timestamp);
        assert!(cfg.logging.syslog.enable);
        assert!(matches!(cfg.logging.syslog.level, Some(LogLevel::Warn)));
        assert_eq!(cfg.logging.syslog.tag, "vf-env");
        assert!(cfg.logging.syslog.network.is_empty());
        assert!(cfg.logging.syslog.address.is_empty());
        assert_eq!(cfg.profiling.addr.as_deref(), Some("127.0.0.1:16060"));
        assert_eq!(cfg.metrics.poll_interval_secs, Some(30));
        assert_eq!(cfg.server.manager_addr.as_deref(), Some("127.0.0.1:18080"));
    }

    #[test]
    fn loads_addresses_and_intervals() {
        let _env = EnvTestGuard::new();
        let yaml = r#"
profiling:
  addr: "127.0.0.1:6060"
metrics:
  poll_interval_secs: 2
"#;
        let path = unique_temp_path("addr");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        let opts = cfg.to_server_options();
        assert_eq!(opts.profile_addr.as_deref(), Some("127.0.0.1:6060"));
        assert_eq!(opts.metrics_poll_interval_secs, Some(2));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn env_overrides_file_values() {
        let yaml = r#"
logging:
  output: stdout
metrics:
  poll_interval_secs: 5
server:
  manager_addr: "127.0.0.1:8080"
"#;
        let path = unique_temp_path("env_override_file");
        std::fs::write(&path, yaml).unwrap();

        let mut env = EnvTestGuard::new();
        env.set(ENV_LOGGING_OUTPUT, "file");
        env.set(ENV_METRICS_POLL_INTERVAL_SECS, "30");
        env.set(ENV_SERVER_MANAGER_ADDR, "127.0.0.1:18080");

        let cfg = AppConfig::load_required(&path).unwrap();

        assert!(matches!(cfg.logging.output, LoggingOutput::File));
        assert_eq!(cfg.metrics.poll_interval_secs, Some(30));
        assert_eq!(cfg.server.manager_addr.as_deref(), Some("127.0.0.1:18080"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn default_manager_addr_is_set() {
        let cfg = AppConfig::default();
        assert_eq!(
            cfg.server.manager_addr.as_deref(),
            Some(crate::server::DEFAULT_MANAGER_ADDR)
        );
    }

    #[test]
    fn default_flow_instance_is_set() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.server.flow_instances.len(), 1);
        let spec = &cfg.server.flow_instances[0];
        assert_eq!(spec.id, manager::DEFAULT_FLOW_INSTANCE_ID);
    }

    #[test]
    fn partial_server_config_keeps_default_flow_instance() {
        let _env = EnvTestGuard::new();
        let yaml = r#"
server:
  manager_addr: "127.0.0.1:9999"
"#;
        let path = unique_temp_path("partial_server");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();

        assert_eq!(cfg.server.manager_addr.as_deref(), Some("127.0.0.1:9999"));
        assert_eq!(cfg.server.flow_instances.len(), 1);
        assert_eq!(
            cfg.server.flow_instances[0].id,
            manager::DEFAULT_FLOW_INSTANCE_ID
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn explicit_empty_flow_instances_are_preserved() {
        let _env = EnvTestGuard::new();
        let yaml = r#"
server:
  flow_instances: []
"#;
        let path = unique_temp_path("empty_flow_instances");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        let opts = cfg.to_server_options();

        assert!(opts.flow_instances.is_empty());
        let err = match crate::server::prepare_registry(&opts.flow_instances) {
            Ok(_) => panic!("expected empty flow_instances to fail registry preparation"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("default in_process flow instance"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn default_logging_is_stdout_with_source() {
        let cfg = AppConfig::default();
        match cfg.logging.output {
            LoggingOutput::Stdout => {}
            _ => panic!("expected default logging.output=stdout"),
        }
        assert!(cfg.logging.include_source);
        assert!(!cfg.logging.disable_timestamp);
        match cfg.logging.level {
            LogLevel::Info => {}
            _ => panic!("expected default logging.level=info"),
        }
        assert!(!cfg.logging.syslog.enable);
        assert!(cfg.logging.syslog.level.is_none());
        assert_eq!(cfg.logging.syslog.tag, "veloflux");
        assert!(cfg.logging.syslog.network.is_empty());
        assert!(cfg.logging.syslog.address.is_empty());
    }

    #[test]
    fn loads_logging_config() {
        let _env = EnvTestGuard::new();
        let yaml = r#"
logging:
  output: syslog
  level: warn
  include_source: false
  disable_timestamp: true
  file:
    dir: "./tmp/logs"
    file_name: "app.log"
    rotation:
      keep_days: 3
      max_num: 10
      max_size_mb: 16
  syslog:
    enable: true
    level: error
    tag: "vf-test"
    network: ""
    address: ""
"#;
        let path = unique_temp_path("logging");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        match cfg.logging.output {
            LoggingOutput::Syslog => {}
            _ => panic!("expected output=syslog"),
        }
        match cfg.logging.level {
            LogLevel::Warn => {}
            _ => panic!("expected level=warn"),
        }
        assert!(!cfg.logging.include_source);
        assert!(cfg.logging.disable_timestamp);
        assert_eq!(cfg.logging.file.dir, "./tmp/logs");
        assert_eq!(cfg.logging.file.file_name, "app.log");
        assert_eq!(cfg.logging.file.rotation.keep_days, 3);
        assert_eq!(cfg.logging.file.rotation.max_num, 10);
        assert_eq!(cfg.logging.file.rotation.max_size_mb, 16);
        assert!(cfg.logging.syslog.enable);
        assert!(matches!(cfg.logging.syslog.level, Some(LogLevel::Error)));
        assert_eq!(cfg.logging.syslog.tag, "vf-test");
        assert!(cfg.logging.syslog.network.is_empty());
        assert!(cfg.logging.syslog.address.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unsupported_env_vars_are_ignored() {
        let yaml = r#"
server:
  flow_instances:
    - id: "default"
      backend: "in_process"
"#;
        let path = unique_temp_path("unsupported_env");
        std::fs::write(&path, yaml).unwrap();

        let mut env = EnvTestGuard::new();
        let unsupported_key = "VELOFLUX_SERVER__FLOW_INSTANCES";
        env.set_any(unsupported_key, "[]");

        let cfg = AppConfig::load_required(&path).unwrap();

        assert_eq!(cfg.server.flow_instances.len(), 1);
        assert_eq!(cfg.server.flow_instances[0].id, "default");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_addr_env_vars_override_loaded_values() {
        let yaml = r#"
profiling:
  addr: "127.0.0.1:6060"
"#;
        let path = unique_temp_path("unsupported_bind_addr_env");
        std::fs::write(&path, yaml).unwrap();

        let mut env = EnvTestGuard::new();
        env.set(ENV_PROFILING_ADDR, "127.0.0.1:16060");

        let cfg = AppConfig::load_required(&path).unwrap();

        assert_eq!(cfg.profiling.addr.as_deref(), Some("127.0.0.1:16060"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_supported_env_value_fails_loading() {
        let mut env = EnvTestGuard::new();
        env.set(ENV_METRICS_POLL_INTERVAL_SECS, "abc");

        let err = AppConfig::load_default_with_env().unwrap_err();
        assert!(err
            .to_string()
            .contains("failed to parse environment variable"));
    }
}
