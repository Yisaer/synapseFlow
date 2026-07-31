use super::{AppConfig, ConfigResult, LogLevel, LoggingOutput};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

const SUPPORTED_ENV_PREFIX: &str = "VELOFLUX_";
const PROPERTIES_ENV_PREFIX: &str = "VELOFLUX_PROPERTIES__";

type ApplyFn = fn(&mut AppConfig, &EnvBinding, &str) -> ConfigResult<()>;

#[derive(Clone, Copy)]
struct EnvBinding {
    env: &'static str,
    config_path: &'static str,
    apply: ApplyFn,
}

impl EnvBinding {
    const fn new(env: &'static str, config_path: &'static str, apply: ApplyFn) -> Self {
        Self {
            env,
            config_path,
            apply,
        }
    }
}

const ENV_BINDINGS: [EnvBinding; 16] = [
    EnvBinding::new(
        "VELOFLUX_LOGGING__OUTPUT",
        "logging.output",
        set_logging_output,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__LEVEL",
        "logging.level",
        set_logging_level,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__INCLUDE_SOURCE",
        "logging.include_source",
        set_logging_include_source,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__DISABLE_TIMESTAMP",
        "logging.disable_timestamp",
        set_logging_disable_timestamp,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__FILE__DIR",
        "logging.file.dir",
        set_logging_file_dir,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__SYSLOG__ENABLE",
        "logging.syslog.enable",
        set_logging_syslog_enable,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__SYSLOG__LEVEL",
        "logging.syslog.level",
        set_logging_syslog_level,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__SYSLOG__TAG",
        "logging.syslog.tag",
        set_logging_syslog_tag,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__SYSLOG__NETWORK",
        "logging.syslog.network",
        set_logging_syslog_network,
    ),
    EnvBinding::new(
        "VELOFLUX_LOGGING__SYSLOG__ADDRESS",
        "logging.syslog.address",
        set_logging_syslog_address,
    ),
    EnvBinding::new(
        "VELOFLUX_PROFILING__ENABLED",
        "profiling.enabled",
        set_profiling_enabled,
    ),
    EnvBinding::new(
        "VELOFLUX_PROFILING__ADDR",
        "profiling.addr",
        set_profiling_addr,
    ),
    EnvBinding::new(
        "VELOFLUX_PROFILING__CPU_PROFILE_FREQ_HZ",
        "profiling.cpu_profile_freq_hz",
        set_profiling_cpu_profile_freq_hz,
    ),
    EnvBinding::new(
        "VELOFLUX_METRICS__POLL_INTERVAL_SECS",
        "metrics.poll_interval_secs",
        set_metrics_poll_interval_secs,
    ),
    EnvBinding::new(
        "VELOFLUX_SERVER__MANAGER_ADDR",
        "server.manager_addr",
        set_server_manager_addr,
    ),
    EnvBinding::new(
        "VELOFLUX_SERVER__PIPELINE_PATROL_INTERVAL_SECS",
        "server.pipeline_patrol_interval_secs",
        set_pipeline_patrol_interval_secs,
    ),
];

pub(super) fn apply_env_overrides(config: &mut AppConfig) -> ConfigResult<()> {
    warn_unsupported_env_vars();
    for binding in ENV_BINDINGS {
        let Some(raw) = read_env_string(binding.env)? else {
            continue;
        };
        (binding.apply)(config, &binding, &raw)?;
    }
    apply_property_env_overrides(config)?;
    Ok(())
}

#[cfg(test)]
pub(super) fn supported_env_var_names() -> Vec<&'static str> {
    ENV_BINDINGS.iter().map(|binding| binding.env).collect()
}

fn warn_unsupported_env_vars() {
    let supported: BTreeSet<&'static str> =
        ENV_BINDINGS.iter().map(|binding| binding.env).collect();
    let mut unknown = BTreeSet::new();
    for (name, _) in std::env::vars_os() {
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(SUPPORTED_ENV_PREFIX)
            || supported.contains(name)
            || name.starts_with(PROPERTIES_ENV_PREFIX)
        {
            continue;
        }
        unknown.insert(name.to_string());
    }

    for name in unknown {
        eprintln!("warning: unsupported veloFlux environment variable ignored: {name}");
    }
}

fn apply_property_env_overrides(config: &mut AppConfig) -> ConfigResult<()> {
    let mut overrides = BTreeMap::<String, (String, String)>::new();
    for (name, value) in std::env::vars_os() {
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(raw_key) = name.strip_prefix(PROPERTIES_ENV_PREFIX) else {
            continue;
        };
        let key = raw_key.to_ascii_lowercase();
        flow::validate_property_key(&key)
            .map_err(|err| format!("invalid property environment variable {name}: {err}"))?;
        let value = value
            .into_string()
            .map_err(|_| format!("environment variable {name} must be valid UTF-8"))?;
        if let Some((previous, _)) = overrides.insert(key.clone(), (name.to_string(), value)) {
            return Err(format!(
                "property environment variables {previous} and {name} both normalize to `{key}`"
            )
            .into());
        }
    }
    for (key, (_, value)) in overrides {
        config.properties.insert_env(key, value);
    }
    Ok(())
}

fn read_env_string(name: &str) -> ConfigResult<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("environment variable {name} must be valid UTF-8").into())
        }
    }
}

fn parse_env<T>(binding: &EnvBinding, raw: &str) -> ConfigResult<T>
where
    T: DeserializeOwned,
{
    serde_yaml::from_str::<T>(raw).map_err(|err| {
        format!(
            "failed to parse environment variable {} for config {}: {}",
            binding.env, binding.config_path, err
        )
        .into()
    })
}

fn set_logging_output(config: &mut AppConfig, binding: &EnvBinding, raw: &str) -> ConfigResult<()> {
    config.logging.output = parse_env::<LoggingOutput>(binding, raw)?;
    Ok(())
}

fn set_logging_level(config: &mut AppConfig, binding: &EnvBinding, raw: &str) -> ConfigResult<()> {
    config.logging.level = parse_env::<LogLevel>(binding, raw)?;
    Ok(())
}

fn set_logging_include_source(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.include_source = parse_env::<bool>(binding, raw)?;
    Ok(())
}

fn set_logging_disable_timestamp(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.disable_timestamp = parse_env::<bool>(binding, raw)?;
    Ok(())
}

fn set_logging_file_dir(
    config: &mut AppConfig,
    _binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.file.dir = raw.to_string();
    Ok(())
}

fn set_logging_syslog_enable(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.syslog.enable = parse_env::<bool>(binding, raw)?;
    Ok(())
}

fn set_logging_syslog_level(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.syslog.level = Some(parse_env::<LogLevel>(binding, raw)?);
    Ok(())
}

fn set_logging_syslog_tag(
    config: &mut AppConfig,
    _binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.syslog.tag = raw.to_string();
    Ok(())
}

fn set_logging_syslog_network(
    config: &mut AppConfig,
    _binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.syslog.network = raw.to_string();
    Ok(())
}

fn set_logging_syslog_address(
    config: &mut AppConfig,
    _binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.logging.syslog.address = raw.to_string();
    Ok(())
}

fn set_profiling_enabled(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.profiling.enabled = Some(parse_env::<bool>(binding, raw)?);
    Ok(())
}

fn set_profiling_addr(
    config: &mut AppConfig,
    _binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.profiling.addr = Some(raw.to_string());
    Ok(())
}

fn set_profiling_cpu_profile_freq_hz(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.profiling.cpu_profile_freq_hz = Some(parse_env::<i32>(binding, raw)?);
    Ok(())
}

fn set_metrics_poll_interval_secs(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.metrics.poll_interval_secs = Some(parse_env::<u64>(binding, raw)?);
    Ok(())
}

fn set_server_manager_addr(
    config: &mut AppConfig,
    _binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.server.manager_addr = Some(raw.to_string());
    Ok(())
}

fn set_pipeline_patrol_interval_secs(
    config: &mut AppConfig,
    binding: &EnvBinding,
    raw: &str,
) -> ConfigResult<()> {
    config.server.pipeline_patrol_interval_secs = parse_env::<u64>(binding, raw)?;
    Ok(())
}
