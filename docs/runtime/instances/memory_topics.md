# Memory Topics

## Background

Memory topics are manager-owned metadata resources used by the in-process memory source and memory
sink connectors.

They provide process-local pub/sub inside a `FlowInstance` and are intended for integration,
testing, and runtime chaining scenarios where one pipeline output is consumed by another pipeline
without network I/O.

## Goals

- Define memory topics as first-class runtime resources.
- Clarify topic kind, capacity, and flow-instance scoping.
- Explain how memory sources and memory sinks bind to declared topics.
- Document storage, startup, and import/export behavior.

## Non-Goals

- Cross-process or cross-host memory transport.
- Durable replay or persistence of topic payloads.
- A general replacement for external brokers.

## Resource Model

A memory topic is identified by `topic` and stores:

- `kind`: `bytes` or `collection`
- `capacity`: broadcast channel capacity

The resource is global manager metadata, but the runtime pub/sub channel is instance-scoped:

- each `FlowInstance` owns its own topic registry
- declaring the same topic name in multiple instances creates separate in-process channels
- payloads are never shared across instances by the memory transport itself

The manager installs declared topics into all in-process flow instances.

## Topic Kind

Memory topics are strongly typed.

`bytes`

- used when a source or sink exchanges encoded payload bytes
- compatible with memory streams that decode bytes through a real decoder such as `json`
- compatible with memory sinks whose `encoder.type` is not `none`

`collection`

- used when a source or sink exchanges decoded `Collection` values directly
- compatible with memory streams whose `decoder.type` is `none`
- compatible with memory sinks whose `encoder.type` is `none`

The topic kind is part of the resource contract. A stream or sink binding is rejected when the
declared topic kind does not match the binding mode.

## Capacity

`capacity` controls the underlying broadcast channel capacity for each runtime instance.

If omitted, the manager uses the built-in default capacity. A zero capacity is normalized to the
minimum valid runtime capacity.

Capacity is not a retention guarantee. It only bounds the in-memory broadcast buffer for each
instance-local topic.

## Declaration Semantics

Declaring a memory topic performs two steps:

1. Persist the topic metadata in manager storage.
2. Install the topic into each local runtime instance.

The operation rejects:

- empty topic names
- reuse of an existing topic name with a different `kind`
- reuse of an existing topic name with a different `capacity`
- storage conflicts for already persisted topics

If runtime installation fails after storage was created, the manager rolls back the stored topic.

Listing topics reads from storage and returns the persisted topic set in stable name order.

## Source Binding

A memory stream references a predeclared topic through `props.topic`.

The stream is valid only when:

- the topic exists in storage and in the target runtime instance
- the stream decoder mode matches the topic kind
- `shared=true` is not used with memory streams

The source runtime subscribes to the instance-local topic. If subscribers lag behind the producer,
the underlying broadcast channel may drop old messages for that subscriber. The memory source logs
lag events and continues processing.

See also:

- [Memory Source Connector](../sources/memory.md)

## Sink Binding

A memory sink references a predeclared topic through `props.topic`.

The sink publishes:

- bytes when `encoder.type != "none"`
- collections when `encoder.type == "none"`

For collection topics, the planner inserts `PhysicalMemoryCollectionMaterialize` before the sink so
downstream memory consumers receive a stable row layout.

Publishing to a topic with no subscribers is treated as success with zero deliveries.

See also:

- [Memory Sink](../sinks/connectors/memory.md)

## Storage And Startup Behavior

Memory topics are part of the manager metadata snapshot.

They are included in:

- storage export
- storage import
- startup `--init-dir` resource manifest
- startup hydration from storage

Hydration declares memory topics before streams and pipelines that may reference them. This order
keeps memory source and memory sink validation deterministic during startup.

## Import / Export Semantics

Export includes all persisted memory topics.

Import treats memory topics as part of the full metadata snapshot:

- duplicate topic names inside a bundle are rejected
- zero capacity is rejected in imported bundles
- the imported snapshot fully replaces the stored topic set
- import does not reconcile live runtime resources immediately

the startup resource manifest uses the same bundle shape but add-only startup semantics. A memory topic in
the startup resource manifest conflicts with an already stored topic of the same name and fails startup.

## Testing Guidance

- Verify declaring a bytes topic allows bytes-mode memory source and sink bindings.
- Verify declaring a collection topic allows collection-mode memory source and sink bindings.
- Verify kind mismatches are rejected before dependent streams or pipelines are installed.
- Verify capacity mismatches on existing runtime topics are rejected.
- Verify memory topics are restored before dependent streams and pipelines during startup.
- Verify import/export preserves memory topic kind and capacity.
- Verify publishing without subscribers succeeds with zero deliveries.

## Future Work

- If runtime topic updates are added, they should define whether capacity can change in place or
  requires topic replacement.
- If cross-instance memory delivery is introduced, it should be documented as a new transport
  boundary rather than extending the current in-process topic contract silently.
