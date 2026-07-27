//! Bootstrap utilities for initializing the veloFlux server.
//!
//! This module provides initialization helpers that handle:
//! - CLI argument parsing (--config, --data-dir)
//! - Config file loading
//! - Logging setup

use crate::config::AppConfig;
use crate::logging::{LoggingContext, LoggingGuard};
use crate::server::{self, ServerOptions};
use crate::startup::StartupPhase;
use flow::FlowInstance;

const DEFAULT_CONFIG_PATH: &str = "config.yaml";

/// Result of bootstrap initialization that does not include a FlowInstance.
pub struct BootstrapOptionsResult {
    /// Server options derived from config and CLI.
    pub options: ServerOptions,
    /// Logging guard that must be kept alive for the lifetime of the application.
    pub logging_guard: LoggingGuard,
}

/// Result of the default initialization process.
pub struct BootstrapResult {
    /// The prepared FlowInstance with default registrations.
    pub instance: FlowInstance,
    /// Server options derived from config and CLI.
    pub options: ServerOptions,
    /// Logging guard that must be kept alive for the lifetime of the application.
    pub logging_guard: LoggingGuard,
}

#[derive(Debug, Clone)]
struct CliFlags {
    data_dir: Option<String>,
    config_path: Option<String>,
    init_dir: Option<String>,
}

impl CliFlags {
    fn parse() -> Self {
        let mut data_dir = None;
        let mut config_path = None;
        let mut init_dir = None;
        let mut args = std::env::args().skip(1).peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--data-dir" => {
                    if let Some(val) = args.next() {
                        data_dir = Some(val);
                    }
                }
                "--config" => {
                    if let Some(val) = args.next() {
                        config_path = Some(val);
                    }
                }
                "--init-dir" => {
                    if let Some(val) = args.next() {
                        init_dir = Some(val);
                    }
                }
                _ => {}
            }
        }
        Self {
            data_dir,
            config_path,
            init_dir,
        }
    }
}

fn load_config_with_path(
    config_path: Option<&str>,
) -> Result<(AppConfig, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(path) = config_path {
        let cfg = AppConfig::load_required(path)?;
        Ok((cfg, Some(path.to_string())))
    } else {
        match AppConfig::load_optional(DEFAULT_CONFIG_PATH)? {
            Some(cfg) => Ok((cfg, Some(DEFAULT_CONFIG_PATH.to_string()))),
            None => Ok((AppConfig::load_default_with_env()?, None)),
        }
    }
}

fn init_options_from_loaded_config(
    config: AppConfig,
    loaded_config_path: Option<String>,
    data_dir: Option<&str>,
    init_dir: Option<&str>,
    logging_context: &LoggingContext,
) -> Result<BootstrapOptionsResult, Box<dyn std::error::Error + Send + Sync>> {
    let logging_guard =
        crate::logging::init_logging_with_context(&config.logging, logging_context)?;
    let bootstrap_phase = StartupPhase::new(
        "manager",
        "default",
        "bootstrap",
        loaded_config_path.as_deref(),
    );
    if let Some(path) = loaded_config_path.as_deref() {
        tracing::info!(config_path = path, "loaded config");
    }
    tracing::info!(
        git_sha = build_info::git_sha(),
        git_tag = build_info::git_tag(),
        "build info"
    );

    let mut options = config.to_server_options();
    if let Some(dir) = data_dir {
        options.data_dir = Some(dir.to_string());
    }
    if let Some(dir) = init_dir {
        options.init_dir = Some(dir.to_string());
    }
    tracing::info!(
        mode = bootstrap_phase.mode(),
        flow_instance_id = %bootstrap_phase.flow_instance_id(),
        phase = bootstrap_phase.phase(),
        config_path = bootstrap_phase.config_path(),
        result = "configured",
        data_dir = options.data_dir.as_deref().unwrap_or("<default>"),
        manager_addr = options.manager_addr.as_deref().unwrap_or("<default>"),
        profiling_enabled = options.profiling_enabled.unwrap_or(false),
        declared_flow_instance_count = options.flow_instances.len(),
        "startup configuration"
    );
    bootstrap_phase.log_success();

    Ok(BootstrapOptionsResult {
        options,
        logging_guard,
    })
}

pub fn init_options_from_config_path(
    config_path: &str,
) -> Result<BootstrapOptionsResult, Box<dyn std::error::Error + Send + Sync>> {
    init_options_from_config_path_with_logging_context(config_path, &LoggingContext::manager())
}

pub fn init_options_from_config_path_with_logging_context(
    config_path: &str,
    logging_context: &LoggingContext,
) -> Result<BootstrapOptionsResult, Box<dyn std::error::Error + Send + Sync>> {
    let (config, loaded_config_path) = load_config_with_path(Some(config_path))?;
    init_options_from_loaded_config(config, loaded_config_path, None, None, logging_context)
}

/// Parse CLI/config and initialize logging/options without preparing FlowInstance.
pub fn default_init_options(
) -> Result<BootstrapOptionsResult, Box<dyn std::error::Error + Send + Sync>> {
    let cli_flags = CliFlags::parse();
    let (config, loaded_config_path) = load_config_with_path(cli_flags.config_path.as_deref())?;
    init_options_from_loaded_config(
        config,
        loaded_config_path,
        cli_flags.data_dir.as_deref(),
        cli_flags.init_dir.as_deref(),
        &LoggingContext::manager(),
    )
}

/// Perform default initialization: parse CLI, load config, init logging, prepare instance.
///
/// Returns a `BootstrapResult` containing the FlowInstance (with default registrations),
/// ServerOptions, and the logging guard. The caller can then register custom codecs
/// on `result.instance` before calling `server::init()` and `server::start()`.
pub fn default_init() -> Result<BootstrapResult, Box<dyn std::error::Error + Send + Sync>> {
    let bootstrap = default_init_options()?;
    let prepare_phase = StartupPhase::new("manager", "default", "prepare_registry", None);
    let instance = match server::prepare_registry(&bootstrap.options.flow_instances) {
        Ok(instance) => instance,
        Err(err) => {
            prepare_phase.log_failure(err.as_ref());
            return Err(err);
        }
    };
    prepare_phase.log_success();

    Ok(BootstrapResult {
        instance,
        options: bootstrap.options,
        logging_guard: bootstrap.logging_guard,
    })
}
