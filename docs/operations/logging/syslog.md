# Syslog Logging Design

This document defines the syslog backend behavior for veloFlux logging.

## Background

Some deployments rely on the host syslog service for log collection, retention,
forwarding, and operational filtering. In those environments, writing only to
`stdout` or local rotating files adds extra log shipping work outside the
application.

veloFlux already emits most runtime logs through `tracing`, which makes syslog
support a backend integration problem rather than an application call-site
migration.

## Goals

- Add a syslog output backend without changing runtime logging call sites.
- Keep process-global logging initialization centralized.
- Preserve structured logging context in the rendered syslog message body.
- Keep syslog I/O off the main runtime execution path.
- Support manager and embedded startup paths consistently.
- Support default and custom Unix datagram sockets and remote UDP or TCP
  destinations.

## Non-Goals

- This design does not support multi-backend fan-out in the first version.
- This design does not support TLS-encrypted syslog transport.
- This design does not define host-process logging callbacks for the embedded
  runtime.

## Configuration

The syslog backend is enabled by selecting `logging.output=syslog`.

Example:

```yaml
logging:
  output: syslog
  level: info
  disable_timestamp: true
  include_source: false
  syslog:
    enable: true
    level: info
    tag: "veloflux"
    network: unixgram
    address: /var/run/syslog
```

This example connects to the explicitly configured Unix datagram socket.

Example local-syslog-focused snippet:

```yaml
logging:
  output: syslog
  level: info
  disable_timestamp: true
  syslog:
    enable: true
    tag: "veloflux"
    network: unixgram
    address: /var/run/syslog
```

Example with a custom Unix datagram socket:

```yaml
logging:
  output: syslog
  syslog:
    enable: true
    network: unixgram
    address: /run/custom/syslog.sock
```

Example with a remote UDP or TCP server:

```yaml
logging:
  output: syslog
  syslog:
    enable: true
    network: udp
    address: syslog.example.com:514
```

Use `network: tcp` with the same `host:port` address format to select TCP.

### Fields

#### `logging.syslog.network`

Syslog transport name. Values are case-insensitive.

Current behavior:

- `unixgram`: connect to the Unix datagram socket path in `address`
- `udp`: send datagrams to the `host:port` endpoint in `address`
- `tcp`: connect to the `host:port` endpoint in `address`

`unixgram` requires a Unix platform. Empty values are rejected.

#### `logging.syslog.address`

Destination selected by `logging.syslog.network`.

Current behavior:

- with `unixgram`: an absolute or relative Unix socket path
- with `udp` or `tcp`: a resolvable `host:port` endpoint

`network` and `address` must both be non-empty.

#### `logging.syslog.enable`

Whether the syslog sink is explicitly enabled.

When `logging.output=syslog` is selected, this field must be `true`.

#### `logging.syslog.level`

Sink-local level for syslog output.

If omitted, it inherits the global `logging.level`.

#### `logging.syslog.tag`

Base application tag used to derive the effective syslog ident.

Default:

- `veloflux`

## Effective Ident Rules

To keep logs attributable in multi-process deployments, the effective syslog
ident should be derived by startup path:

- manager: `<tag>-manager`
- embedded: `<tag>-embedded`

This keeps operator-side filtering stable even when multiple veloFlux runtime
processes use the same host syslog service.

## Severity Mapping

The backend should map `tracing` levels to syslog severities in the obvious
way:

- `ERROR` -> error
- `WARN` -> warning
- `INFO` -> informational
- `DEBUG` -> debug
- `TRACE` -> debug

The first version does not need a separate trace-specific syslog priority.

## Transport Framing

Unix datagram and UDP destinations send one formatted syslog record per
datagram. TCP destinations use RFC 6587 octet-counting framing: each record is
prefixed with its decimal byte length and one space. This preserves record
boundaries even when the rendered event contains a newline.

Each TCP connection attempt has a three-second timeout per resolved socket
address. Hostname resolution itself uses the platform resolver and is not
covered by this connection timeout.

## Message Body Shape

The syslog message body should remain human-readable and preserve useful event
context.

Recommended content:

- event target
- event message
- structured event fields
- source file and line when `logging.include_source=true`

Recommended exclusions:

- ANSI styling
- terminal-only formatting assumptions
- duplicated wall-clock timestamp text in the message body when
  `logging.disable_timestamp=true`

## Startup Behavior

When `logging.output=syslog` is selected:

1. load config;
2. validate that `logging.syslog.enable=true`;
3. resolve the effective runtime ident from `logging.syslog.tag`;
4. resolve the configured destination and open its socket or connection;
5. install the process-global subscriber;
6. continue normal runtime startup.

If syslog initialization fails, startup should fail rather than silently
falling back to another backend.

## Runtime Failure Behavior

After startup succeeds, the runtime should prioritize forward progress over
guaranteed log delivery.

If syslog writes fail after startup:

- do not block processor or connector hot paths;
- use bounded buffering;
- reconnect in the background and retry the current record once;
- tolerate bounded record loss;
- retry connection setup when a later record arrives after a failed reconnect.

## Deployment Notes

- Keep `logging.output=syslog` and `logging.syslog.enable=true` aligned.
- Explicitly configure both `logging.syslog.network` and
  `logging.syslog.address`.
- Use `network=unixgram` with a socket path for a local endpoint.
- Use `network=udp` or `network=tcp` with a `host:port` address for a remote
  endpoint.
- Prefer `include_source=false` in production unless file/line metadata is
  required for active debugging.
- Prefer `disable_timestamp=true` when syslog already supplies the timestamp
  envelope you need.
- Keep runtime code on `tracing` macros so the selected backend remains
  effective.
- Reserve direct `eprintln!` output for startup-time diagnostics that happen
  before logging is initialized.

## Environment Variables

The syslog backend adds these explicit override bindings:

- `VELOFLUX_LOGGING__DISABLE_TIMESTAMP`
- `VELOFLUX_LOGGING__SYSLOG__ENABLE`
- `VELOFLUX_LOGGING__SYSLOG__LEVEL`
- `VELOFLUX_LOGGING__SYSLOG__TAG`
- `VELOFLUX_LOGGING__SYSLOG__NETWORK`
- `VELOFLUX_LOGGING__SYSLOG__ADDRESS`

These overrides follow the same whitelist-based policy used by the rest of the
configuration loader.
