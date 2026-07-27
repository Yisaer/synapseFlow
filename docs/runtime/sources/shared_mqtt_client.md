# Shared MQTT Client And `connector_key` Model

## Background

veloFlux allows MQTT sources and MQTT sinks to reuse a named shared runtime resource instead of
opening one standalone MQTT connection per connector.

That shared resource is addressed by `connector_key` and stored as a manager-owned metadata object.
It is a cross-cutting runtime feature because:

- streams may reference it from source definitions
- sinks may reference it from pipeline definitions

## Problem Statement

Without a shared client model, every MQTT source or sink would own its own network client and
connection lifecycle. That makes it difficult to:

- reuse one managed connection across multiple source/sink bindings
- serialize config mutation against pipelines that depend on the same key

The shared client model therefore needs clear answers for ownership, mutation, deletion, and
cross-instance consistency.

## Goals

- Define the shared MQTT client as a first-class metadata and runtime resource.
- Clarify how sources and sinks bind to the resource through `connector_key`.
- Document manager/storage/runtime consistency expectations.
- Explain mutation serialization and deletion behavior.

## Non-Goals

- MQTT broker administration or topic provisioning.
- Cross-host distributed connection sharing.
- A generic shared-connector framework for all connector types.

## Core Resource Model

A shared MQTT client is identified by `key` and currently stores:

- `broker_url`
- `topic`
- `client_id`
- `qos`
- `max_packet_size`
- `username` (optional, plaintext identifier)
- `password` (optional, a secret-store reference — see *Credentials* below)

At runtime, each flow instance owns its own per-key client registry. A shared client entry contains:

- one `rumqttc::AsyncClient`
- one background event loop task
- one backpressured fan-out hub of payload/error/close events
- reference counting for acquired handles

The resource is therefore shared within one flow instance, not across instances. `key` names the
logical resource; each instance still creates its own network client.

## Credentials (VF-51)

MQTT authentication is configured with two fields, kept separate from `broker_url`:

- `username`: a plaintext identifier (not treated as a secret).
- `password`: a **secret reference**. Two forms are accepted:
  - `"store:NAME"` (recommended) — resolves `NAME` from the encrypted secret store; only the pointer
    is persisted, never the value.
  - an inline literal (any value not starting with `store:`) — allowed under the default `warn`
    policy (logs a warning), **rejected** under `strict` (`VELOFLUX_SECRETS_POLICY=strict`). An inline
    password lands in scannable config, so prefer `store:NAME`.

Embedding credentials in the URL (`mqtt://user:pass@host`) is **rejected** as invalid config: the
userinfo would otherwise land in the startup resource manifest / redb, which static scanners read.

### Configuring the store and referring to it

1. Create the secret in the local store (the value is read from a prompt, stdin, or `--from-file` —
   never from the command line):

   ```bash
   veloflux secrets set my-broker-pass --from-file ./broker-pass.txt
   # or:  printf '%s' "$PASS" | veloflux secrets set my-broker-pass
   ```

2. Reference it by that exact name in the shared MQTT client config:

   ```json
   {
     "key": "fleet-broker",
     "broker_url": "tcp://broker.example.com:1883",
     "topic": "fleet/+/telemetry",
     "client_id": "veloflux-fleet",
     "qos": 0,
     "username": "device",
     "password": "store:my-broker-pass"
   }
   ```

The store key (`my-broker-pass`) is whatever you pass to `secrets set`; `"store:my-broker-pass"` is
how config points at it. At registration the manager resolves the reference against the store and
injects the real password into the runtime connection only; `GET`/`LIST`/export and the persisted
record keep the `store:NAME` pointer, never the value.

The store (`<data-dir>/secrets.enc`) is encrypted with a root key from the `VELOFLUX_SECRETS_KEY`
environment variable (base64 32-byte key). Without it, a built-in key is used, which keeps secrets
out of static scanners but is not confidential against someone holding the binary.

## Delivery And Lifecycle Semantics

Shared MQTT client delivery must follow the same high-level runtime principles as processors:

- fan-out is **no-drop**
- slow downstream subscribers apply backpressure to the shared client instead of silently losing
  old messages
- runtime errors are **non-terminal**
- the shared client runtime exists only while it has active references
- explicit close/delete still defines resource removal, but the running connection instance is
  reclaimed once the last active user releases it

The shared client therefore does **not** treat connection errors, reconnect attempts, or subscriber
delivery errors as implicit end-of-stream conditions.

Instead:

- payloads are fanned out through a backpressured delivery path
- runtime errors are surfaced to attached sources/sinks as ordinary runtime errors
- an explicit close operation emits the terminal close/end event
- reaching zero active references tears down the running connection instance without deleting the
  shared client metadata resource

## Source-Side Binding

An MQTT stream may set `props.connector_key`.

When `connector_key` is absent:

- the source connector creates its own standalone MQTT connection
- stream-local `broker_url`, `topic`, `qos`, and optional `client_id` drive runtime behavior

When `connector_key` is present:

- the source connector acquires the shared client by key
- payloads and runtime errors come from the shared client's backpressured event stream
- the shared client config becomes the live owner of subscription topic, client id, QoS, and
  packet-size settings

This means a source definition still keeps its MQTT props in metadata, but runtime subscription
ownership moves to the shared client resource once `connector_key` is used.

Shared streams reuse the same binding model. Their shared ingest connector factory builds an
`MqttSourceConnector`, so `connector_key` works for both shared and non-shared MQTT streams.

## Sink-Side Binding

An MQTT sink may also set `props.connector_key`.

When `connector_key` is absent:

- the sink lazily creates a standalone client on `ready` / first send
- sink-local broker URL, client id, and packet-size settings drive the connection

When `connector_key` is present:

- the sink lazily acquires the shared client
- the shared client supplies the live connection and connection-level settings
- the sink still uses its own publish-time `topic`, `qos`, and `retain` values

This asymmetry is intentional:

- source-side shared binding reuses the shared client's subscription settings
- sink-side shared binding reuses the connection, but publish destination and delivery flags remain
  sink-local

One shared key may therefore be used by both source and sink bindings in the same instance.

## Manager / Storage / Runtime Consistency

The manager stores shared MQTT client configs as global metadata.

Current consistency model:

- `GET`/`LIST` read from storage
- create persists storage first, then installs the client into every local runtime instance
- startup hydration restores shared clients from storage before restoring streams
- import/export and the startup resource manifest include shared client configs in the metadata bundle

Create is idempotent when the existing config matches exactly. A mismatched config is a conflict,
both in storage and in any already-installed runtime instance.

Worker runtimes do not hydrate shared clients independently from manager metadata. Instead, manager
includes the required shared MQTT configs in the apply context.
ensures the runtime has matching entries before stream/pipeline installation proceeds.

## Management Surface

The shared MQTT client is a documented runtime resource, not only a connector implementation
detail.

Manager-side control-plane operations currently provide:

- create with exact-match idempotency
- get and list from storage-backed metadata
- delete with storage-authoritative removal and best-effort runtime cleanup

This management surface exists to keep `connector_key` bindings stable across:

- startup hydration
- import/export bundles
- concurrent pipeline create, update, and start operations

## Busy Guard And Mutation Serialization

Manager serializes shared MQTT mutations with per-key semaphores.

These guards are acquired by:

- shared-client create/delete handlers
- pipeline create/upsert/start paths that reference MQTT streams or MQTT sinks with
  `connector_key`

Important consequences:

- one key cannot be mutated concurrently by two control-plane operations
- pipeline operations depending on that key fail fast with `409 Conflict` instead of mutating
  desired state against an unstable connector resource
- the key set is deduplicated before locking, so one pipeline referencing the same key multiple
  times still acquires one serialized resource lock

## Delete Semantics

Delete is storage-authoritative.

Current manager behavior:

1. acquire the per-key shared MQTT mutation lock
2. delete the config from storage
3. best-effort drop the runtime copy from local instances
4. return success even if some runtime notifications fail

Within a flow instance, dropping the client removes the key from the acquisition registry
immediately. If existing source/sink handles are still holding the client, network shutdown is
deferred until the last handle is released.

This produces a useful invariant:

- new acquisitions fail immediately after delete
- already-running users may finish with their held handle during shutdown
- the shared client only emits its terminal close/end event as part of that explicit shutdown path
- when the key is still configured but no source/sink currently holds it, the running connection
  instance may be absent and will be recreated on the next acquisition

## Multi-Instance / Worker Behavior

Shared MQTT clients are instance-scoped runtime objects.

In-process instances:

- are hydrated from storage during manager startup
- create shared clients directly during manager-side create

Worker-process instances:

- reconcile those configs before installing streams or pipelines

There is no cross-instance shared network client. The same key may exist in multiple instances, but
each instance owns its own runtime connection.

## Failure Semantics

Important failure behaviors:

- create rejects empty required fields before storage mutation
- create conflicts if an existing config differs
- create rolls back newly created runtime copies and newly created storage on local installation
  failure
- delete treats runtime cleanup as best effort after storage deletion
- shared client event loops reconnect with exponential backoff on runtime connection errors
- source/sink publish or acquisition errors surface at connector runtime, not as metadata mutation
  errors
- connection/runtime errors do not implicitly terminate the shared client runtime
- only explicit close/delete drives terminal end-of-stream delivery to attached users
- zero active references reclaim the running runtime instance without removing metadata

## Testing Guidance

- Verify create is idempotent when the same config is submitted twice.
- Verify create conflicts when the same key is reused with a different config.
- Verify local create rollback when one runtime instance fails after earlier instances succeeded.
- Verify a dropped-but-still-held client disappears from new acquisitions immediately and only
  shuts down after the last handle is released.
- Verify busy-key conflicts reject pipeline start before desired state is mutated.
- Verify one key can be referenced by both a source and a sink in the same pipeline/runtime.
- Verify a slow subscriber backpressures the shared client fan-out instead of losing messages.
- Verify runtime connection errors are delivered as non-terminal runtime errors and do not emit
  end-of-stream.
- Verify explicit close/delete emits the terminal end event exactly once.
- Verify the running connection instance is lazily created on first acquisition and reclaimed when
  the last active reference is released.

## Future Work

- If shared-client updates are introduced later, they should reuse the same per-key mutation lock
  model instead of bypassing it with in-place runtime mutation.
