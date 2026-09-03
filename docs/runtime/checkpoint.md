# Pipeline Checkpoint Design

## Status

The runtime barrier injection, in-memory snapshot collection, durable manifest commit, and
independent redb-backed `CheckpointStorage` are available. File datasource snapshot and restore are
available; concrete state support for other processors, retention, public checkpoint APIs, and
periodic scheduling remain deferred.

The current manifest uses sparse operator snapshots. Only processors that actually report state
are persisted. During restore, every persisted snapshot must map uniquely to a compatible
processor in the rebuilt physical plan, but the runtime does not yet require every current
stateful processor to have a snapshot. File is the only source connector that currently implements
the checkpoint-fence protocol. Enabling checkpointing with another source connector causes
checkpoint shutdown to fail instead of silently committing an unfenced source position.

## Goals

Pipeline checkpointing should provide:

- a consistent snapshot boundary across the processor graph
- processor-local state persistence and restoration
- a final state snapshot during graceful pipeline shutdown
- atomic selection of the latest committed checkpoint
- recovery with at-least-once semantics

Checkpointing does not provide exactly-once delivery to arbitrary external sinks. Exactly-once
delivery requires a separate transactional or idempotent sink protocol.

## Runtime ownership

`CheckpointCoordinator` is a pipeline runtime component, not a physical-plan processor. It owns
the control-plane lifecycle of a checkpoint:

```text
ProcessorPipeline
  ├── processor graph
  ├── CheckpointCoordinator
  ├── CheckpointStore
  └── CheckpointCoordinatorHandle
```

Checkpointing is enabled per manager pipeline with the nested option
`options.checkpoint.enabled`; it defaults to `false` for backward compatibility:

```json
{
  "options": {
    "checkpoint": {
      "enabled": true
    }
  }
}
```

The manager does not expose a manual or periodic checkpoint trigger in the current phase. The only
runtime trigger is graceful pipeline shutdown. The current coordinator is an in-process handle.
Checkpoint status reporting and periodic request scheduling are deferred.

The coordinator must not hold the complete `ProcessorPipeline` or call a specific
`BarrierProcessor` directly. Instead, pipeline construction creates a cloneable trigger containing
the pipeline ingress sender, barrier-id allocator, and tail-ack registration capability.

## Barrier injection

The coordinator injects one logical checkpoint barrier through the pipeline head:

```text
CheckpointCoordinator
    │
    │ Ingress::data(StreamData::Control(...))
    ▼
ControlSourceProcessor
    │
    │ data channel
    ▼
processor graph
```

Checkpoint barriers use the data channel. They must not be injected through the high-priority
control channel, because a control-channel signal can overtake data that belongs before the
checkpoint boundary.

The coordinator does not send a separate barrier to every downstream processor. The barrier is
propagated by the normal data graph, and fan-in barriers are aligned at explicit
`BarrierProcessor` nodes.

The current implementation provides the following internal runtime path:

```text
ProcessorPipeline::request_checkpoint(timeout)
    ↓
CheckpointCoordinator
    ↓
Ingress::data(StreamData::Control(Checkpoint { mode: Continue }))
    ↓
tail acknowledgement
```

`request_final_checkpoint(timeout)` uses the same path with `mode: Final`. The coordinator waits for
the tail acknowledgement, after which `ProcessorPipeline` commits the manifest through its
configured `CheckpointStore`. Checkpoint-aware participants record owned in-memory snapshots while
the barrier is processed; the pipeline collects them into the manifest only after the tail
acknowledgement.

## Signal model

The signal model is one checkpoint barrier with an explicit mode:

```text
Checkpoint {
    checkpoint_id: u64,
    mode: Continue | Final,
}
```

The signal modes have the following protocol semantics:

| Mode | Processor behavior | Pipeline behavior |
| --- | --- | --- |
| `Continue` | Establish a boundary, report available snapshots, and forward the barrier | Pipeline keeps running |
| `Final` | Forward the terminal barrier and exit | Pipeline terminates after checkpoint commit |

When checkpointing is disabled, graceful shutdown uses one `StreamGracefulEnd` signal. When
checkpointing is enabled, graceful shutdown uses one `Final` checkpoint barrier; it must not send
a normal checkpoint followed by a separate `StreamGracefulEnd`.

The current implementation treats checkpoint barriers as data-channel control items. Stateless
processors forward the barrier unchanged. `Final` follows the terminal-signal behavior, causing
each processor on the path to stop after forwarding it. A file-backed `DataSourceProcessor` is the
first stateful participant: it establishes the connector fence and records its source snapshot
before forwarding either checkpoint mode. Concrete snapshots for other stateful processors remain
deferred.

## Processor handling

Most processors have one upstream. For a single-upstream processor, data-channel ordering provides
the checkpoint boundary:

```text
data-1 → data-2 → Checkpoint(10) → data-3
```

The processor handles the barrier only after `data-1` and `data-2` have been processed.

The general checkpoint-aware contract for `Continue` mode is:

1. finishes the data before the barrier
2. snapshots its local state
3. reports the snapshot to the coordinator
4. forwards the barrier downstream
5. continues processing later data

The general checkpoint-aware contract for `Final` mode is:

1. finishes the data before the barrier
2. performs the same graceful flush used by normal shutdown
3. snapshots the state after the flush
4. reports the snapshot to the coordinator
5. forwards the final barrier downstream
6. exits its processor task

The file datasource implements both contracts. `Continue` remains an internal mode without a
manager trigger.

Stateless processors only need to forward the barrier. Stateful processors must snapshot the
runtime state that affects future results, including buffers, active windows, aggregate groups,
previous-row state, watermarks, and SQL-visible processor state. Metrics such as records-in and
records-out counters are not checkpoint state by default.

The snapshot operation must run in the processor task that owns the mutable state. An external
coordinator must not read processor fields concurrently. If a processor delegates work to an
internal task, it must quiesce that task before producing its snapshot.

## Fan-in alignment

The planner inserts a `BarrierProcessor` at fan-in boundaries. Its responsibilities are:

- wait for the same checkpoint barrier from every upstream
- prevent post-barrier data from passing the checkpoint boundary
- forward one aligned barrier downstream
- stop after forwarding a final barrier

`BarrierProcessor` does not own business state and normally does not produce a business snapshot.

Checkpoint support requires post-barrier data isolation. The current checkpoint-aware alignment
state tracks each data upstream separately. After an upstream delivers a checkpoint barrier, the
`BarrierProcessor` stops polling that upstream, so its post-barrier data remains in the input
channel. The existing cooperative send backpressure then prevents an active sender from overrunning
the paused receiver. Once all upstreams deliver the same checkpoint barrier, the processor forwards
one barrier and resumes polling all upstreams. `BarrierProcessor` does not own a post-barrier data
buffer or a separate buffer limit. Arrival-count-only alignment remains the behavior for the
existing non-checkpoint synchronization signals.

Only `BarrierProcessor` is checkpoint-aware at this boundary. Ordinary upstream processors keep
processing and forwarding data; when they send to a paused link, the link's cooperative
backpressure makes the send await channel capacity.

If the planner guarantees that every fan-in is protected by a `BarrierProcessor`, other
intermediate processors do not need multi-input alignment logic. The datasource boundary remains a
special source-participant boundary because connector data is external to the processor graph.

Source connectors establish their boundary through `SourceConnector::request_checkpoint`. The
request is asynchronous: the connector establishes a production boundary, emits an internal
`ConnectorEvent::CheckpointFence` after all payloads produced before the boundary, and returns an
owned in-memory `CheckpointState`. The fence is an internal connector event and is not forwarded as
a pipeline data-channel checkpoint signal. The current `FileSourceConnector` implements this
protocol and records the source path, file mode, and each file's canonical absolute path, last
emitted byte offset, and physical file fingerprint. Runtime read offsets and pending partial-line
bytes are not persisted. After emitting the
fence, the file worker may resume producing post-fence payloads. Those payloads remain ordered after
the fence in the connector event stream.

When a `DataSourceProcessor` receives a checkpoint on its data input, it stops polling its ordinary
inputs, requests the connector boundary, and continues forwarding connector payloads. The processor
consumes the connector event stream directly so the fence is ordered after every pre-fence payload.
After receiving the matching fence, it records an `OperatorSnapshot` with kind `datasource` and
state version `2`, then forwards the checkpoint. For `Continue`, it returns to its ordinary event
loop and consumes post-fence payloads after the barrier. For `Final`, it closes the connector and
exits. If the request, snapshot, or fence fails, the checkpoint is not forwarded.

Connectors without checkpoint-fence support reject `request_checkpoint`. A connector may return no
snapshot only after still emitting a matching fence; this represents a fenced stateless source, not
an unfenced source.

The current file datasource snapshot schema is:

```text
OperatorSnapshot {
    checkpoint_key: "datasource:<source name>:<same-source occurrence>"
    operator_kind: "datasource"
    state_version: 2
    state: {
        connector_kind: "file"
        source_path: <canonical source path>
        mode: "file" | "directory"
        cursors: [{
            path
            offset: <byte position after the last emitted payload>
            file_identity
        }]
    }
}
```

## Checkpoint lifecycle

For graceful shutdown with checkpointing enabled:

```text
enter stopping state
    ↓
inject Checkpoint(mode=Final)
    ↓
processors forward the final barrier and exit
    ↓
wait for the pipeline-tail acknowledgement
    ↓
atomically commit the final checkpoint
    ↓
finish pipeline shutdown
```

The pipeline collects snapshots produced by checkpoint-aware participants before committing the
manifest. Ordinary processors still use the default passthrough behavior. A datasource backed by
the file source records its in-memory connector state under a stable semantic checkpoint key.

An in-flight checkpoint that has not been committed is discarded during recovery. The previous
committed checkpoint remains the recovery point.

## Processor snapshot contract

Each checkpoint participant has a stable checkpoint key, an implementation kind, and a state
version. The checkpoint key is separate from the runtime processor ID: runtime IDs may contain
physical plan indexes and are used for diagnostics, while checkpoint keys describe the semantic
participant identity used across plan rebuilds. A participant produces an owned in-memory state
tree at the checkpoint boundary. The pipeline collects these trees and the manager serializes the
complete manifest only at the durable storage boundary.

The conceptual contract is:

```text
snapshot(state) -> versioned in-memory state tree
restore(versioned in-memory state tree) -> state
```

Pipeline startup first rebuilds the physical plan, then loads the latest committed manifest before
processor tasks and external connectors are started. Restore has two phases. First, every saved
snapshot must map to exactly one current processor by checkpoint key, and that processor validates
the operator kind, state version, and state tree without changing runtime state. Only after all
snapshots pass validation are they applied. `DataSourceProcessor::restore_checkpoint` then passes
the in-memory state to its single source connector. `FileSourceConnector` retains the restored
cursor state before `subscribe()` creates the worker.

Datasource checkpoint keys currently use
`datasource:<source name>:<same-source occurrence>`. The occurrence is derived from deterministic
physical-plan traversal and distinguishes multiple references to the same source without depending
on global plan indexes. Adding or removing a reference to the same source may intentionally change
the affected keys and invalidate its previous snapshots.

At file-source startup, existing files resume at the stored byte offset with an empty runtime
pending buffer. Any incomplete trailing bytes are read again. Files without a cursor start at offset
zero. A file that is shorter than the stored offset or has a changed physical file identity resets
its cursor to zero; ordinary append growth retains the stored offset. File cursors are keyed by
canonical absolute path. A restored cursor for a currently missing directory file is retained so a
later file at the same path can be checked against its stored physical identity.

Snapshots that are present are compatibility checked as one unit:

- an unknown or ambiguous checkpoint key is incompatible
- a processor-kind mismatch is incompatible
- an unsupported state version is incompatible
- a file connector-kind or source-path mismatch is incompatible
- an invalid file snapshot structure, duplicate cursor, out-of-scope cursor path, relative cursor
  path, offset, or file identity is incompatible

Any such incompatibility clears all checkpoints for that pipeline and starts with empty state. The
runtime never restores only a compatible subset of a manifest. A cleanup failure prevents startup.
An unreadable storage record that is neither the current format nor a recognized legacy format is
treated as storage corruption and also prevents startup. Recognized older manifest formats are
cleared and cold-started.

Processors that do not own recoverable state may use the default passthrough behavior and do not
produce an operator snapshot. Missing snapshots are currently accepted because manifests are
sparse. A checkpoint-aware processor that has started producing a snapshot must fail the barrier
rather than silently omit its state. A complete required-participant registry remains deferred.

## Checkpoint manifest

The checkpoint is a versioned manifest containing operator snapshots:

```text
CheckpointManifest {
    checkpoint_format_version
    flow_instance_id
    pipeline_id
    checkpoint_id
    created_at_unix_ms
    operator_snapshots[]
}

OperatorSnapshot {
    checkpoint_key
    operator_kind
    state_version
    state
}
```

`state` is an in-memory tree containing null, scalar, byte, array, and map values. The first
implementation uses full snapshots. Incremental snapshots, chunking, and compression can be
added after correctness and recovery behavior are established.

`checkpoint_format_version` remains an independent restore guard. It must be incremented when the
manifest format or the runtime snapshot contract becomes incompatible. The current manifest format
version is `2`; version `1` contained the removed pipeline-spec MD5 field. A checkpoint can be
loaded only when its format matches and every persisted snapshot is accepted by the rebuilt
physical plan:

```text
current.checkpoint_format_version == checkpoint.checkpoint_format_version
every snapshot checkpoint_key maps to exactly one current processor
every mapped processor accepts operator_kind, state_version, and state
```

There is deliberately no whole-pipeline specification hash. Unrelated specification or plan
changes do not invalidate state that still maps to compatible operators. When the format or any
saved operator state is incompatible, startup clears all checkpoints for the pipeline and
continues from empty state. After a compatible checkpoint is restored, the shared signal-id
allocator advances to at least `checkpoint_id + 1`, ensuring that a later durable checkpoint cannot
reuse or fall behind a committed checkpoint ID.

## Independent checkpoint storage on the shared redb database

Checkpoint data uses the same redb database file as existing metadata, but it has an independent
`CheckpointStorage` type and dedicated table. The storage layer only persists an opaque payload;
the manager layer's `DurableCheckpointStore` adapts it to the flow-level `CheckpointStore` trait.
It is not embedded in `StoredPipeline.raw_json`, and `MetadataStorage` does not implement
checkpoint APIs.
The two storage namespaces share the redb backend and access lock so concurrent transactions are
serialized without opening the database file twice.

The current logical table is:

```text
checkpoints
    key: flow_instance_id + NUL + pipeline_id + NUL + zero-padded checkpoint_id
    value: bincode-serialized CheckpointManifest
```

The manager adapter validates the manifest and serializes it once at the storage boundary. The
storage layer then uses one redb write transaction. Repeating the same checkpoint with identical
contents is idempotent;
committing different contents under an existing checkpoint identity is rejected. Loading scans the
committed manifests for the requested pipeline and selects the greatest checkpoint ID. Clearing a
pipeline removes only that pipeline's records. There is currently no retention policy or separate
latest-pointer table.

The runtime-facing `flow::CheckpointStore` remains storage-independent. The storage crate exposes
`MetadataStorage::checkpoint_storage()` and `StorageManager::checkpoint_storage()` as factories
for the independent namespace. The manager creates `DurableCheckpointStore` and injects it into
each `FlowInstance`, so enabled pipelines commit manifests after their barriers complete.

Operator state is not serialized by processors. The file datasource constructs an in-memory state
tree and records it in the shared collector; the manager serializes the completed manifest only at
the storage boundary. Processors without recoverable state continue to leave
`operator_snapshots` unchanged.

Checkpoint state and pipeline metadata have different lifecycles even though they share a database:

- stopping a pipeline keeps its checkpoints
- deleting a pipeline clears its checkpoints before deleting its persisted metadata
- changing the pipeline definition clears records during the next startup only when saved operator
  state no longer maps to compatible processors
- normal metadata export should not include runtime checkpoints by default
- a full database backup may include both metadata and checkpoint state

The single-database approach provides operational simplicity and allows metadata and checkpoint
updates to use the same durability boundary. It also means checkpoint writes must be bounded and
observable because large or frequent snapshots can contend with ordinary metadata operations.

## Failure and delivery semantics

The checkpoint protocol provides consistent state snapshots with at-least-once recovery:

- a crash before commit restores the previous committed checkpoint
- records after the previous checkpoint may be replayed
- non-transactional sinks may observe duplicate records
- checkpoint commit failure must never advance the latest pointer
- final-checkpoint failure leaves the pipeline stopped, while recovery uses the previous committed
  checkpoint
- before returning a final-checkpoint failure, the runtime closes ingress and aborts and joins all
  remaining processor and supervisor tasks
- a datasource snapshot, connector fence, or collector failure prevents the final barrier from
  reaching the tail, so the failed checkpoint is never committed

Concurrent checkpoint requests are unsupported in the current phase. The manager only triggers one
`Final` checkpoint during graceful shutdown.

File cursors are keyed by canonical absolute path and carry a physical file fingerprint. The source
resets a cursor when the fingerprint changes or current length is shorter than the stored offset. It
cannot reliably detect an in-place `copytruncate` followed by growth beyond the stored offset before
the next observation.

Exactly-once delivery is deferred to a future sink transaction or idempotency design. It is not an
implicit property of the checkpoint coordinator.

## Implementation phases

1. Add the checkpoint manifest and store abstraction. Done.
2. Add the cloneable pipeline-head barrier trigger and coordinator lifecycle. Done.
3. Add ordinary processor checkpoint passthrough. Done.
4. Extend fan-in barrier alignment with post-barrier data isolation. Done.
5. Add final checkpoint handling to graceful shutdown. Done.
6. Add independent redb checkpoint records and the manager-to-storage adapter. Done.
7. Inject the durable store into the pipeline runtime and commit empty-snapshot manifests after
   ordinary and final checkpoints. Done.
8. Add the in-memory snapshot model, optional participant collector, and final-checkpoint manifest
   collection. Done.
9. Load the latest checkpoint at pipeline startup, validate its identity and compatibility,
   advance the shared signal ID, and dispatch snapshots before processor tasks start. Done.
10. Add the source-connector checkpoint fence protocol and file-source in-memory snapshot
    generation. Done.
11. Connect datasource checkpoint collection to both continue and final checkpoint lifecycles.
    Done.
12. Add file datasource restore and fail-closed state validation before connector subscription.
    Done.
13. Replace whole-spec MD5 compatibility with physical-plan checkpoint-key matching and two-phase
    snapshot validation and application. Clear all pipeline checkpoints and cold-start when the
    format or any saved snapshot is incompatible. Done.
14. Add concrete snapshot and restore implementations for other stateful processors. Deferred.
