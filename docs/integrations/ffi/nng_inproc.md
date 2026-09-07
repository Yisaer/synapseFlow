# NNG In-Process Integration

This document defines the design for using the NNG pub/sub connectors when
veloFlux is embedded as a shared library in a host process.

The design follows the same boundary used by ekuiper: the Manager API exposes
only the NNG URL, while the NNG connection layer derives the transport from the
URL scheme. The pipeline API must not duplicate this information with a
separate `transport` or `inproc` field.

## Runtime topology

The host owns the peer sockets and veloFlux owns the connector sockets:

```text
host pub0  -- inproc://veloflux/input -->  veloFlux nng source (sub0)
host sub0  <-- inproc://veloflux/output -- veloFlux nng sink (pub0)
```

The host must create and listen on its sockets before starting the embedded
veloFlux runtime. The host should publish application data only after the
pipeline has become ready.

## Manager API

The public configuration is the same for all NNG transports:

```json
{
  "type": "nng_pubsub",
  "props": {
    "url": "inproc://veloflux/input",
    "topic": "topic/can",
    "topic_delimiter": ":"
  }
}
```

The URL scheme is the single source of truth:

| URL scheme | Transport |
|---|---|
| `tcp://` | TCP |
| `ipc://` | Unix IPC |
| `inproc://` | NNG in-process transport |

The API must not accept a second transport selector such as
`"transport": "inproc"`. Conflicting or duplicated configuration would allow
the URL and the explicit field to disagree.

The source always uses the `sub` protocol. The sink always uses the `pub`
protocol. `protocol` is an internal connector property and is not required in
the Manager request.

## Internal connection model

The flow layer parses the URL once into an internal endpoint model:

```text
NngEndpoint {
    url: String,
    transport: Tcp | Ipc | Inproc,
}
```

Source and sink construction passes this endpoint and its protocol to one NNG
connection factory:

```text
source -> connection factory(endpoint, sub)
sink   -> connection factory(endpoint, pub)
```

The Manager validates the request and builds the pipeline definition. It does
not create NNG sockets or select a language binding. Transport-specific socket
behavior belongs in the connection factory so that URL classification is not
duplicated across Manager, source, sink, and FFI code.

## In-process dialing

`inproc://` only connects NNG sockets in the same process and in the same NNG
runtime. Matching URLs in two different processes do not communicate.

For `inproc` connections, the connection layer should retry dialing until the
host peer is available or the pipeline is stopped. This is different from a
normal external endpoint where asynchronous NNG reconnect behavior is
sufficient. The retry must be cancellation-aware and bounded by the connector
or pipeline startup timeout.

The host startup order is:

1. Create the input `pub0` socket and listen on the input URL.
2. Create the output `sub0` socket, subscribe to the output topic, and listen on the output URL.
3. Start the embedded veloFlux runtime.
4. Create and start the pipeline through the Manager API.
5. Wait until the source and sink connectors are ready.
6. Start publishing application data.

NNG pub/sub is best-effort. Messages published before the subscriber is ready
may be lost, so a fixed sleep is not a readiness protocol.

## Pipeline readiness

`running` means that the pipeline tasks have started. It does not necessarily
mean that every NNG connector has connected to its peer. The runtime should
track connector readiness separately:

```text
starting -> ready
        \-> failed
```

An NNG pipeline is ready only after:

- every source socket is connected and subscribed;
- every sink socket is connected;
- the processor runtime is healthy.

The Manager pipeline response should expose this readiness information without
changing the NNG configuration shape. A future response may contain data such
as:

```json
{
  "status": "running",
  "readiness": {
    "status": "ready",
    "connectors": [
      {
        "id": "input",
        "role": "source",
        "kind": "nng_pubsub",
        "transport": "inproc",
        "status": "connected"
      }
    ]
  }
}
```

The `transport` field in this response is derived diagnostic information. It
is not a request field.

## Embedded versus standalone runtime

The URL still determines the NNG transport. The hosting mode only determines
whether the current deployment is allowed to use an in-process endpoint.

The embedded runtime may accept `inproc://` endpoints. A standalone deployment
should reject them during pipeline validation unless it explicitly provides an
in-process NNG peer in the same executable.

This policy belongs to the runtime/Manager capability configuration, not to a
second NNG transport field in the pipeline request.

## NNG library constraint

The host and veloFlux must use the same NNG library runtime for `inproc`:

```text
host process  ---- same libnng ---- veloFlux shared library
```

The host and veloFlux must not load separate static NNG instances. The
`nng-sys` build used by the shared library should therefore use the host's
compatible shared `libnng` rather than the default vendored static library.

This is a build and packaging requirement; it is not something the Manager can
repair at runtime.

## Scope of the first implementation

The first implementation should:

- keep the existing `nng_pubsub` source and sink API;
- derive transport from the URL scheme;
- centralize endpoint parsing and connection creation;
- support `tcp`, `ipc`, and `inproc` through the same connector kind;
- add connector/pipeline readiness reporting;
- keep `nng_pubsub` streams out of shared-stream promotion;
- add a host-process integration test using the shared-library build.

The first implementation should not add a second FFI data plane for sending or
receiving NNG messages. The host owns its NNG peer sockets, while the Manager
continues to manage streams and pipelines over HTTP/REST.
