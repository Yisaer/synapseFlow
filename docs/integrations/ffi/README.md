# FFI Embedding

## Background

Some deployments need to start and stop veloFlux from a host process written in
C while continuing to use veloFlux through its existing HTTP/REST manager API.
In that model, the host process is responsible for application lifecycle and
links to veloFlux as a library instead of launching the standalone `veloflux`
service binary as a separate process.

The current runtime startup path is centered around the root service entrypoint
and CLI-driven bootstrap. That shape is correct for the standalone binary, but
it is not the right direct boundary for FFI embedding:

- the binary entrypoint owns CLI parsing;
- the standalone startup path owns signal handling;
- embedding must not assume that the host process command line belongs to
  veloFlux.

This document defines the initial embedded runtime design for veloFlux FFI.

For NNG pub/sub integration, the host process owns the NNG peer sockets and
uses the existing HTTP/REST Manager API to create and start pipelines. The NNG
transport is selected by the connector URL (`tcp://`, `ipc://`, or
`inproc://`), not by an additional FFI or pipeline flag. See
[NNG In-Process Integration](nng_inproc.md) for the detailed contract.

## Goals

- Allow a host process written in C to start veloFlux in-process.
- Allow the same host process to stop veloFlux cleanly.
- Preserve the existing HTTP/REST manager API as the external control plane.
- Reuse the existing runtime startup path as much as possible.
- Keep the FFI surface minimal and stable.
- Keep unsafe code isolated to a thin shim layer.

## Non-Goals

- This design does not expose manager APIs directly through FFI.
- This design does not replace HTTP/REST with a native C API.
  version.
- This design does not support CLI-driven startup when used through FFI.
- This design does not define dynamic plugin loading.
- This design does not expose a second FFI data plane for NNG send/receive.

## Design Summary

The FFI integration is split into two layers:

- an embedded Rust runtime API inside the `veloflux` crate;
- a separate thin FFI shim crate that exposes a C ABI.

The embedded Rust API owns lifecycle orchestration for the in-process manager
runtime. The FFI shim only validates C inputs, converts them into Rust types,
invokes the embedded API, and maps failures to a small ABI-safe error contract.

The embedded runtime still serves the existing HTTP/REST manager endpoints. The
host process uses FFI only for lifecycle control.

For an embedded NNG deployment, the host creates its `pub0`/`sub0` peer sockets
and the veloFlux connectors create their own `sub0`/`pub0` sockets. The two
sides communicate through matching `inproc://` URLs.

## Why Not Wrap `main()`

The standalone binary entrypoint should remain the only service entrypoint for
the normal CLI-driven product flow. Reusing its startup logic is desirable, but
wrapping `main()` directly is the wrong abstraction boundary for embedding.

The embedded path must avoid several standalone-only behaviors:

- it must not read the host process CLI arguments;
- it must not install its own shutdown signal policy by default;
- it must not assume that `std::env::current_exe()` points to a veloFlux
  service binary;
- it must return a lifecycle handle that the host process can stop explicitly.

For that reason, the implementation should extract reusable library startup
components from the current manager path and expose them through an embedded API
instead of attempting to call `main()` from FFI.

## Runtime Contract

### Startup mode

The initial FFI design supports only manager startup in-process. The manager
continues to bind its configured HTTP address and exposes the existing
HTTP/REST endpoints unchanged.

### Flow instance restriction

The initial version supports only `in_process` flow instances.

startup must fail before the manager begins serving traffic.

re-executing the current executable, which is not a valid assumption when
veloFlux is embedded into an arbitrary host process.

### External control plane

After startup succeeds, all runtime operations continue to use the existing
HTTP/REST APIs. Examples include:

- stream management;
- pipeline creation and deletion;
- pipeline start and stop;
- explain and stats endpoints;
- import and export endpoints.

NNG endpoint configuration remains part of the stream and pipeline API. The
host should start publishing only after the relevant pipeline reports that its
NNG source and sink connectors are ready. `running` alone does not guarantee
that an NNG peer is connected.

The FFI layer is not a second manager API. It is only a lifecycle bridge for
embedding.

## Embedded Rust API

The `veloflux` crate should expose a library-first embedded startup API that is
separate from CLI bootstrap.

The embedded API should:

- accept explicit startup parameters instead of reading `std::env::args()`;
- construct and initialize the manager runtime using existing shared startup
  building blocks;
- return an opaque runtime handle for later shutdown;
- avoid owning process-wide signal handling policy;
- validate that all configured flow instances are `in_process`.

At a high level, the embedded startup path should still reuse the normal
runtime sequence:

1. load configuration from an explicit config path;
2. initialize logging;
3. prepare the flow registry;
4. apply the selected distro registration;
5. initialize the server runtime;
6. start the manager HTTP server in the background;
7. return a handle that can stop the runtime.

The key difference is ownership: the standalone binary waits for process-level
termination, while the embedded API returns control to the host process after
successful startup.

## Thin FFI Shim

The FFI shim should live in a dedicated crate, for example `veloflux_ffi`, and
be built as a `cdylib`.

That crate should contain the small amount of `unsafe extern "C"` code needed
to cross the ABI boundary. The main runtime logic should stay in safe Rust
within the embedded API layer.

The C-facing API surface is intentionally small:

- `veloflux_start(...)`
- `veloflux_stop(...)`

No manager operations are exposed directly through this shim.

Pipeline creation, start, stop, and readiness inspection remain Manager HTTP
operations. A future convenience operation may wait for pipeline readiness,
but it must not duplicate the NNG configuration or provide a second transport
selector.

## Proposed C ABI Shape

The exact signatures may evolve during implementation, but the initial design
should follow these rules:

- `start` receives an explicit config path.
- `start` returns an opaque handle on success.
- `stop` receives a pointer to that opaque handle.
- `stop` is responsible for shutting the runtime down completely before it
  returns.
- the ABI uses stable integer error codes and optional human-readable error
  retrieval helpers.

An illustrative shape is:

```c
typedef struct veloflux_handle veloflux_handle_t;

int veloflux_start(const char* config_path, veloflux_handle_t** out_handle);
int veloflux_stop(veloflux_handle_t** handle);
```

This design keeps the first version easy to integrate from C, C++, and other
languages that can consume a C ABI.

## Lifecycle Semantics

### Start

`veloflux_start` should:

1. validate input pointers and the config path;
2. load the specified configuration file;
4. initialize logging and manager runtime state;
5. start the manager HTTP server in the background;
6. return a handle only after the runtime is ready to serve.

If startup fails, no partially initialized handle should be returned.

### Stop

`veloflux_stop` should provide synchronous shutdown semantics.

That means `stop` should:

1. trigger graceful runtime shutdown;
2. wait until the manager listener stops accepting requests;
3. wait until owned background tasks finish their cleanup;
4. release runtime resources;
5. return only after shutdown is complete.

Blocking `stop` is preferred in the first version because the FFI API is
intentionally minimal. Without a separate `join` or `wait` call, the host
process needs `stop` to define a complete shutdown boundary.

### Idempotency

`stop` should be implemented as an idempotent operation from the perspective of
the host process. Calling `stop` on an already stopped handle should not crash
the process. The implementation may either treat it as success or return a
dedicated "already stopped" error code, but the behavior must be documented and
stable.

## Error Handling

The ABI must not let Rust panics or rich Rust error types cross into C
directly.

The shim should map failures into:

- a small stable set of integer error codes;
- optional thread-safe retrieval of the last error message for diagnostics.

Typical startup failures include:

- null or invalid config path input;
- configuration file load failure;
- invalid listen address;
- port bind failure;
- runtime initialization failure.
- an `inproc://` endpoint without a peer in the host process;
- incompatible or separately linked NNG library instances.

The initial implementation should prioritize deterministic lifecycle behavior
over a large error taxonomy.

## Threading and Ownership

The embedded runtime will manage its own Rust async runtime and background
tasks. The host process should treat the returned handle as an opaque owner of
that runtime state.

The FFI contract should make the ownership rules explicit:

- a handle returned by `start` is owned by the caller;
- the caller must eventually release it through `stop`;
- the caller must not copy or free the opaque handle directly;
- concurrent `stop` calls on the same handle are not supported unless the

## Configuration Model

The initial FFI design requires an explicit config file path. It does not infer
startup configuration from the host process CLI arguments.

This keeps the embedded path deterministic and avoids accidental coupling
between host application flags and veloFlux runtime bootstrap.

If the host process needs custom configuration generation, it can materialize a
config file and pass its path to `veloflux_start`.

## Logging

The embedded path should reuse the same logging initialization behavior as the
standalone runtime where possible. However, the integration contract should
document that logging remains configured by veloFlux based on the supplied
veloFlux config file.

Future revisions may define tighter host-process logging integration, but that
is outside the scope of the first version.

## Compatibility Notes

- The standalone `veloflux` binary remains the primary service entrypoint for
  non-embedded deployments.
- The embedded API and FFI shim are additive integration paths.
- The same distro registration path must be reused by both standalone and
  embedded startup so that runtime capabilities remain consistent.

## Open Implementation Constraints

The implementation must account for the following existing runtime assumptions:

- standalone bootstrap currently reads CLI flags directly;
- standalone startup currently owns signal-based shutdown handling;

NNG-specific implementation constraints are documented in
[NNG In-Process Integration](nng_inproc.md). In particular, the shared-library
build must use the same compatible `libnng` runtime as the host process when an
`inproc://` endpoint is used.

The FFI crate exposes NNG support through an explicit Cargo feature:

```bash
NNG_NO_VENDOR=1 NNG_STATIC=0 \
  cargo build --release -p veloflux-ffi --features nng_pubsub
```

The build environment must provide a compatible shared `libnng` and its
headers. The resulting shared library must resolve to the same `libnng` used by
the host process.

The embedded API should factor around those assumptions instead of duplicating
startup logic.

## Initial Rollout Plan

The initial embedded implementation consists of:

1. extract an embedded manager startup path inside `veloflux`;
2. add a runtime handle with explicit shutdown;
3. reject non-`in_process` flow instance configurations;
4. add a dedicated `cdylib` FFI shim crate exposing `start` and `stop`;
5. enable NNG support with the `nng_pubsub` FFI feature;
6. build against the host-compatible shared `libnng` for `inproc://` deployments;
7. validate the host-process round trip with an embedded integration test.

This keeps the first version narrow, testable, and aligned with the existing
HTTP/REST-centric runtime model.
