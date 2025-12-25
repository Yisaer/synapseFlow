//! Bootstrap utilities for initializing the synapseFlow server.
//!
//! This module provides a default initialization function that handles:
//! - CLI argument parsing (--config, --data-dir)
//! - Config file loading
//! - Logging setup
//! - FlowInstance preparation
//!
//! Custom codecs can call `default_init()` and then register their own
//! encoders/decoders before starting the server.

use crate::config::AppConfig;
use crate::logging::LoggingGuard;
use crate::server::{self, ServerOptions};
use flow::FlowInstance;

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
}

impl CliFlags {
    fn parse() -> Self {
        let mut data_dir = None;
        let mut config_path = None;
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
                _ => {}
            }
        }
        Self {
            data_dir,
            config_path,
        }
    }
}

/// Perform default initialization: parse CLI, load config, init logging, prepare instance.
///
/// Returns a `BootstrapResult` containing the FlowInstance (with default registrations),
/// ServerOptions, and the logging guard. The caller can then register custom codecs
/// on `result.instance` before calling `server::init()` and `server::start()`.
///
/// # Example
/// ```ignore
/// let result = synapse_flow::bootstrap::default_init()?;
/// // Register custom encoders/decoders on result.instance
/// let ctx = server::init(result.options, result.instance).await?;
/// server::start(ctx).await
/// ```
pub fn default_init() -> Result<BootstrapResult, Box<dyn std::error::Error + Send + Sync>> {
    let cli_flags = CliFlags::parse();
    let (config, loaded_config_path) = if let Some(path) = cli_flags.config_path.as_deref() {
        let cfg = AppConfig::load_required(path)?;
        (cfg, Some(path.to_string()))
    } else {
        (AppConfig::default(), None)
    };

    let logging_guard = crate::logging::init_logging(&config.logging)?;
    if let Some(path) = loaded_config_path.as_deref() {
        tracing::info!(config_path = path, "loaded config");
    }
    tracing::info!(
        git_sha = build_info::git_sha(),
        git_tag = build_info::git_tag(),
        "build info"
    );

    let mut options = config.to_server_options();
    if let Some(dir) = cli_flags.data_dir.as_deref() {
        options.data_dir = Some(dir.to_string());
    }

    let instance = server::prepare_registry();

    Ok(BootstrapResult {
        instance,
        options,
        logging_guard,
    })
}
