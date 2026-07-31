# Environment Configuration Overrides

## Background

veloFlux currently loads process startup configuration from `config.yaml`.

In real deployments, a small subset of configuration values changes across environments more
frequently than the rest of the config file. Typical examples are bind addresses, log verbosity,
and profiling or metrics toggles.

These values are good candidates for environment-variable overrides because they are deployment
specific, scalar, and often adjusted by container or orchestration systems.

At the same time, not every field in `config.yaml` should be exposed through OS environment
variables. Some sections describe runtime topology rather than simple deployment parameters, and
those sections are easier to review and maintain in versioned config files.

## Goals

- Allow a small, explicit whitelist of deployment-oriented config fields to be overridden by OS
  environment variables.
- Keep environment-variable naming stable and predictable.
- Apply the same override semantics consistently for process-global
  fields.
- Preserve `config.yaml` as the source of truth for structured topology configuration.

## Non-Goals

- Provide a generic environment-variable mirror for the entire config schema.
- Support overriding `server.flow_instances` from environment variables.
- Support partial overrides for arrays or nested structured objects.
- Introduce runtime hot reload for environment-variable changes.

## Supported Fields

The following fixed environment variables are recognized:

| Config path | Environment variable |
| --- | --- |
| `logging.output` | `VELOFLUX_LOGGING__OUTPUT` |
| `logging.level` | `VELOFLUX_LOGGING__LEVEL` |
| `logging.include_source` | `VELOFLUX_LOGGING__INCLUDE_SOURCE` |
| `logging.disable_timestamp` | `VELOFLUX_LOGGING__DISABLE_TIMESTAMP` |
| `logging.file.dir` | `VELOFLUX_LOGGING__FILE__DIR` |
| `logging.syslog.enable` | `VELOFLUX_LOGGING__SYSLOG__ENABLE` |
| `logging.syslog.level` | `VELOFLUX_LOGGING__SYSLOG__LEVEL` |
| `logging.syslog.tag` | `VELOFLUX_LOGGING__SYSLOG__TAG` |
| `logging.syslog.network` | `VELOFLUX_LOGGING__SYSLOG__NETWORK` |
| `logging.syslog.address` | `VELOFLUX_LOGGING__SYSLOG__ADDRESS` |
| `profiling.enabled` | `VELOFLUX_PROFILING__ENABLED` |
| `profiling.addr` | `VELOFLUX_PROFILING__ADDR` |
| `profiling.cpu_profile_freq_hz` | `VELOFLUX_PROFILING__CPU_PROFILE_FREQ_HZ` |
| `metrics.poll_interval_secs` | `VELOFLUX_METRICS__POLL_INTERVAL_SECS` |
| `server.manager_addr` | `VELOFLUX_SERVER__MANAGER_ADDR` |

Static properties additionally use the dynamic
`VELOFLUX_PROPERTIES__<KEY>` family. For example,
`VELOFLUX_PROPERTIES__SITE_CODE` overrides `properties.site_code`.

All other `config.yaml` fields remain file-only configuration.

In particular, `server.flow_instances` is intentionally excluded because it describes instance
topology and backend-specific runtime structure. That section is better maintained in `config.yaml`
where it can be reviewed as one coherent unit.

## Naming Rules

Environment-variable names follow these rules:

- use the `VELOFLUX_` prefix
- use `__` as the config-path separator
- keep the config field name and convert it to uppercase

Examples:

- `logging.level` -> `VELOFLUX_LOGGING__LEVEL`
- `logging.disable_timestamp` -> `VELOFLUX_LOGGING__DISABLE_TIMESTAMP`
- `logging.syslog.network` -> `VELOFLUX_LOGGING__SYSLOG__NETWORK`
- `logging.syslog.address` -> `VELOFLUX_LOGGING__SYSLOG__ADDRESS`
- `metrics.poll_interval_secs` -> `VELOFLUX_METRICS__POLL_INTERVAL_SECS`
- `properties.site_code` -> `VELOFLUX_PROPERTIES__SITE_CODE`

## Merge Priority

The effective startup configuration uses the following precedence:

`defaults < config.yaml < environment variables < CLI`

This means:

- built-in defaults fill missing fields
- `config.yaml` overrides defaults
- supported environment variables override the loaded file values
- CLI flags remain the final override layer for fields that already have CLI support

## Value Semantics

Supported value types are intentionally simple:

- string fields use the raw environment-variable string
- boolean and integer fields are parsed according to their target config type
- enum fields must use the same lowercase values accepted by `config.yaml`

Examples:

```bash
export VELOFLUX_LOGGING__LEVEL=debug
export VELOFLUX_LOGGING__OUTPUT=stdout
export VELOFLUX_LOGGING__DISABLE_TIMESTAMP=true
export VELOFLUX_LOGGING__SYSLOG__ENABLE=true
export VELOFLUX_LOGGING__SYSLOG__LEVEL=info
export VELOFLUX_LOGGING__SYSLOG__TAG=veloflux
export VELOFLUX_LOGGING__SYSLOG__NETWORK=
export VELOFLUX_LOGGING__SYSLOG__ADDRESS=
export VELOFLUX_PROFILING__ENABLED=false
export VELOFLUX_PROFILING__ADDR=0.0.0.0:16060
export VELOFLUX_METRICS__POLL_INTERVAL_SECS=30
export VELOFLUX_SERVER__MANAGER_ADDR=0.0.0.0:18080
export VELOFLUX_PROPERTIES__VIN=L123456789
```

## Implementation Outline

The implementation keeps the override surface explicit:

1. Load defaults and optionally parse `config.yaml`.
2. Apply the documented fixed whitelist and per-key static property overrides.
3. Convert the effective config into `ServerOptions`.

The whitelist is implemented as a centralized binding table in the config loader. Adding support for
a new environment variable requires an explicit binding entry that declares:

- the environment-variable name
- the target config path
- the apply logic for the target field

Unsupported `VELOFLUX_*` variables are ignored by the config loader and warned as unsupported.
`VELOFLUX_PROPERTIES__*` variables are recognized dynamically and are not
reported as unsupported.
