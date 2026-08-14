# Logging Operations

This directory defines the process-global logging model used by veloFlux.

The logging subsystem is deployment-facing infrastructure. It is configured once
during process startup and then shared by manager and embedded runtime
paths.

## Background

veloFlux already uses `tracing` as the application logging API in runtime code.
Most runtime components emit structured events through `tracing::info!`,
`tracing::warn!`, `tracing::error!`, and `tracing::debug!`.

Before syslog support, the process-global logging backend supported two output
modes:

- `stdout`
- rotating local log files

This design extends that model with a syslog backend while preserving the
existing `tracing` call sites and shared startup initialization flow.

## Goals

- Keep `tracing` as the single in-process logging API.
- Keep logging initialization centralized in shared bootstrap code.
- Allow deployment-time selection of `stdout`, rotating file, or syslog output.
- Preserve consistent behavior across manager and embedded runtime
  paths.
- Keep runtime logging non-blocking on hot paths.
- Document clear startup and runtime failure semantics for syslog.

## Non-Goals

- This design does not introduce per-module log routing rules.
- This design does not add multi-destination fan-out in the first version.
- This design does not replace `tracing` with a different application logging
  API.
- This design does not provide TLS-encrypted syslog transport.
- This design does not define runtime hot reload for logging config.

## Current Logging Architecture

### Logging API

Runtime code should emit logs through `tracing` macros. Structured fields such
as `processor_id`, `connector_id`, `topic`, `error`, and startup phase metadata
should continue to be emitted as structured event fields.

This keeps logging backend selection orthogonal to business logic.

### Initialization Model

Logging is initialized once per process through shared startup code. The same
process-global configuration is used by:

- the manager startup path;
- embedded runtime startup path.

The global subscriber must not be reconfigured at runtime with a different
configuration in the same process.

### Existing Outputs

Before syslog support, the supported logging outputs are:

- `stdout`
- `file`

The file backend uses a rotating local file writer with size-based rotation and
retention controls.

## Target Logging Configuration Model

The top-level config remains under the `logging` section:

```yaml
logging:
  output: stdout
  level: info
  include_source: true
  disable_timestamp: false
  file:
    dir: "./logs"
    file_name: "app.log"
    rotation:
      keep_days: 7
      max_num: 30
      max_size_mb: 256
  syslog:
    enable: false
    level: info
    tag: "veloflux"
    network: unixgram
    address: /var/run/syslog
```

When syslog output is enabled, both `network` and `address` must be explicitly
configured. Empty values cause logging initialization to fail.

### Common Fields

- `logging.output`: selected backend; one of `stdout`, `file`, or `syslog`
- `logging.level`: process-global minimum level
- `logging.include_source`: whether source file and line metadata should be
  included when the backend supports it
- `logging.disable_timestamp`: whether the rendered log payload should omit the
  message timestamp

### File-Specific Fields

- `logging.file.dir`
- `logging.file.file_name`
- `logging.file.rotation.keep_days`
- `logging.file.rotation.max_num`
- `logging.file.rotation.max_size_mb`

### Syslog-Specific Fields

- `logging.syslog.enable`: whether the syslog sink configuration is enabled
- `logging.syslog.level`: syslog sink level; if omitted, it inherits
  `logging.level`
- `logging.syslog.tag`: base syslog tag used to derive the effective runtime
  ident
- `logging.syslog.network`: required transport name; one of `unixgram`, `udp`,
  or `tcp`
- `logging.syslog.address`: Unix socket path for `unixgram`, or `host:port` for
  `udp` and `tcp`

## Output Selection Semantics

`logging.output` remains a single-choice configuration in the first version.

That means the process writes to exactly one backend at a time:

- `stdout`
- `file`
- `syslog`

Simultaneous fan-out such as `stdout + syslog` is intentionally excluded from
the first version to keep initialization, buffering, and failure semantics
simple.

When `logging.output=syslog` is selected, `logging.syslog.enable` must also be
`true`. This keeps the syslog sub-configuration explicit while preserving the
single-sink backend model.

## Syslog Integration Design

### Scope

The configuration surface requires both `logging.syslog.network` and
`logging.syslog.address`. A destination uses one of these combinations:

- `unixgram` and a Unix socket path;
- `udp` and a `host:port` endpoint;
- `tcp` and a `host:port` endpoint.

Unix datagram and UDP transports send one record per datagram. TCP uses RFC
6587 octet-counting framing. The backend opens the selected destination during
startup and fails startup when initialization fails. Each TCP connection
attempt has a three-second timeout per resolved socket address. Runtime write
failures trigger a background reconnect and one retry of the current record.

### Message Identity

The configured `logging.syslog.tag` is treated as a base application identity.
Startup paths may derive a more specific runtime ident from it to
make multi-process deployments easier to inspect.

Recommended effective idents are:

- manager: `veloflux-manager`
- embedded: `veloflux-embedded`

The exact derivation should remain deterministic and documented so that
operators can filter logs reliably.

### Formatting

The syslog backend should preserve the useful parts of existing structured
events while adapting them to syslog conventions:

- no ANSI color output;
- no duplicate terminal formatting assumptions;
- honor `logging.disable_timestamp` so that timestamps can be omitted from the
  rendered payload when syslog already timestamps the record;
- keep the event target, message, and structured fields in the payload;
- include source file and line only when `logging.include_source` is enabled.

## Performance and Buffering

### Non-Blocking Requirement

Logging backends must not block runtime hot paths on slow output operations.

This requirement is especially important for processor code, connector loops,
and high-frequency control paths.

### Backend Model

The recommended backend model is:

- application threads emit `tracing` events;
- events are formatted and handed off to a buffered writer path;
- a background writer is responsible for backend I/O.

The existing file backend already follows this shape through a non-blocking
writer guard. Syslog should follow the same operational model.

### Queue Saturation

If the syslog output queue becomes full, the backend may drop log records
instead of blocking runtime work.

When that happens, the implementation should prefer rate-limited diagnostics
over unbounded recursive logging.

## Failure Semantics

### Startup

If `logging.output=syslog` is configured and the syslog backend cannot be
initialized during startup, startup should fail.

This matches the existing expectation that the selected process-global logging
backend is either configured successfully or the process does not continue with
an unexpected fallback.

### Runtime

If syslog becomes temporarily unavailable after successful startup, the backend
should prefer background recovery and bounded loss over blocking runtime work.

The intended behavior is:

- detect backend write failures in the logging writer path;
- retry or reconnect in the background when practical;
- drop records when required to keep the runtime responsive;
- emit diagnostics about persistent logging failures with rate limiting.

## Manager, Worker, and Embedded Behavior

### Manager

The manager process initializes logging through shared bootstrap code and owns
its own process-global logging backend.

### Worker

base tag and the flow instance id.

service.

### Embedded Runtime

The embedded runtime should continue to use the same logging configuration
model as the standalone runtime unless a future design explicitly introduces a
host-integrated logging bridge.

## Environment Variable Overrides

Environment-variable overrides remain explicit and whitelist-based.

The syslog design adds these deployment-facing overrides:

- `VELOFLUX_LOGGING__DISABLE_TIMESTAMP`
- `VELOFLUX_LOGGING__SYSLOG__ENABLE`
- `VELOFLUX_LOGGING__SYSLOG__LEVEL`
- `VELOFLUX_LOGGING__SYSLOG__TAG`
- `VELOFLUX_LOGGING__SYSLOG__NETWORK`
- `VELOFLUX_LOGGING__SYSLOG__ADDRESS`

The general override rules are documented in `../config/env_overrides.md`.

## Runtime Code Hygiene

To keep backend selection effective, runtime diagnostics should prefer
`tracing` over direct `println!` or `eprintln!` calls once logging has been
initialized.

Startup-time diagnostics that occur before logging initialization may still use
direct process stderr output when required.

## Related Documents

- `audit.md`
- `syslog.md`
- `../config/env_overrides.md`
- `../../runtime/instances/flow_instances.md`
- `../../runtime/extensibility/unified_distro_runtime.md`
