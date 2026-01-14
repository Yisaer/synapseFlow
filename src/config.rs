use crate::server::ServerOptions;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub logging: LoggingConfig,
    pub profiling: ProfilingConfig,
    pub metrics: MetricsConfig,
    pub server: ServerConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            logging: LoggingConfig::default(),
            profiling: ProfilingConfig {
                enabled: None,
                addr: None,
                cpu_profile_freq_hz: None,
            },
            metrics: MetricsConfig {
                addr: None,
                poll_interval_secs: None,
            },
            server: ServerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub output: LoggingOutput,
    pub level: LogLevel,
    pub include_source: bool,
    pub file: FileLoggingConfig,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            output: LoggingOutput::Stdout,
            level: LogLevel::Info,
            include_source: true,
            file: FileLoggingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggingOutput {
    Stdout,
    File,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
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
    pub addr: Option<String>,
    pub poll_interval_secs: Option<u64>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            addr: Some("0.0.0.0:9898".to_string()),
            poll_interval_secs: Some(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub manager_addr: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            manager_addr: Some(crate::server::DEFAULT_MANAGER_ADDR.to_string()),
        }
    }
}

impl AppConfig {
    pub fn load_required(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .map_err(|err| format!("failed to read config file {}: {}", path.display(), err))?;
        let cfg: AppConfig = serde_yaml::from_str(&raw)
            .map_err(|err| format!("failed to parse yaml config {}: {}", path.display(), err))?;
        Ok(cfg)
    }

    pub fn load_optional(
        path: impl AsRef<Path>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(Self::load_required(path)?))
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
        if let Some(addr) = self.metrics.addr.as_ref() {
            opts.metrics_addr = Some(addr.clone());
        }
        if let Some(secs) = self.metrics.poll_interval_secs {
            opts.metrics_poll_interval_secs = Some(secs);
        }
        if let Some(addr) = self.server.manager_addr.as_ref() {
            opts.manager_addr = Some(addr.clone());
        }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("veloflux_test.{}.{}.yaml", name, nanos))
    }

    #[test]
    fn loads_optional_missing_file() {
        let path = unique_temp_path("missing");
        let loaded = AppConfig::load_optional(&path).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn applies_only_present_fields() {
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
        assert!(opts.metrics_addr.is_none());
        assert!(opts.metrics_poll_interval_secs.is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn loads_addresses_and_intervals() {
        let yaml = r#"
profiling:
  addr: "127.0.0.1:6060"
metrics:
  addr: "127.0.0.1:9898"
  poll_interval_secs: 2
"#;
        let path = unique_temp_path("addr");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        let opts = cfg.to_server_options();
        assert_eq!(opts.profile_addr.as_deref(), Some("127.0.0.1:6060"));
        assert_eq!(opts.metrics_addr.as_deref(), Some("127.0.0.1:9898"));
        assert_eq!(opts.metrics_poll_interval_secs, Some(2));

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
    fn default_logging_is_stdout_with_source() {
        let cfg = AppConfig::default();
        match cfg.logging.output {
            LoggingOutput::Stdout => {}
            LoggingOutput::File => panic!("expected default logging.output=stdout"),
        }
        assert_eq!(cfg.logging.include_source, true);
        match cfg.logging.level {
            LogLevel::Info => {}
            _ => panic!("expected default logging.level=info"),
        }
    }

    #[test]
    fn loads_logging_config() {
        let yaml = r#"
logging:
  output: file
  level: warn
  include_source: false
  file:
    dir: "./tmp/logs"
    file_name: "app.log"
    rotation:
      keep_days: 3
      max_num: 10
      max_size_mb: 16
"#;
        let path = unique_temp_path("logging");
        std::fs::write(&path, yaml).unwrap();

        let cfg = AppConfig::load_required(&path).unwrap();
        match cfg.logging.output {
            LoggingOutput::File => {}
            _ => panic!("expected output=file"),
        }
        match cfg.logging.level {
            LogLevel::Warn => {}
            _ => panic!("expected level=warn"),
        }
        assert_eq!(cfg.logging.include_source, false);
        assert_eq!(cfg.logging.file.dir, "./tmp/logs");
        assert_eq!(cfg.logging.file.file_name, "app.log");
        assert_eq!(cfg.logging.file.rotation.keep_days, 3);
        assert_eq!(cfg.logging.file.rotation.max_num, 10);
        assert_eq!(cfg.logging.file.rotation.max_size_mb, 16);

        let _ = std::fs::remove_file(&path);
    }
}
