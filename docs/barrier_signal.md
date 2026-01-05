# Barrier Control Signals

## Background

In the `flow` module, processors communicate through two strictly isolated channels:

- **Data channel**: carries `StreamData` (including `StreamData::Control(ControlSignal)`).
- **Control channel**: carries `ControlSignal` directly.

Both channels may carry control signals. In a fan-in topology (a node with multiple upstream
children), some control signals must be **synchronized**: the downstream node should process the
signal only after **all upstreams** have delivered the same synchronization point.

A typical example is a pipeline with multiple sinks: a downstream collection stage must not react
to a “sync/end” signal until every sink has reached that point.

Historically, barrier alignment logic was embedded inside individual processors. This is hard to
maintain because every processor would need to implement the same “wait for all upstreams”
behaviour.

## Design Overview

We model control signals as two categories:

- `ControlSignal::Instant(InstantControlSignal)`:
  processed immediately (no synchronization).
- `ControlSignal::Barrier(BarrierControlSignal)`:
  requires synchronization at fan-in boundaries.

Instead of making every processor barrier-aware, we insert a dedicated physical plan node and a
dedicated processor:

- **Physical plan**: `PhysicalPlan::Barrier(PhysicalBarrier)` is inserted between a fan-in node and
  its children during physical plan optimization.
- **Processor**: `BarrierProcessor` implements barrier synchronization and forwarding.

With this structure, normal processors (e.g. `FilterProcessor`, `ResultCollectProcessor`) can treat
control signals as ordinary items to forward/handle, while the barrier semantics are enforced at
the explicit barrier boundary.

## Signal Model

### Barrier Kinds and IDs

Barrier signals have a **kind** and a **monotonically increasing id**:

- `BarrierControlSignalKind`: the kind without an attached id.
- `BarrierControlSignal`: the kind plus `barrier_id`.

The pipeline head is responsible for assigning ids:

- `ControlSourceProcessor` maintains a monotonically increasing counter shared by both channels.
- `BarrierControlSignalKind::with_id(barrier_id)` constructs a `BarrierControlSignal`.

This ensures barriers can appear multiple times over the lifetime of a pipeline and allows
deterministic detection of invalid overlaps.

### Channel Carrier Rule

A barrier signal is delivered via **exactly one channel** (control or data), determined by the
upstream API that emits it. There is no “mirror to both channels” behaviour.

### Terminal Semantics

Processors stop only when they observe a terminal control signal (`ControlSignal::is_terminal()`).
Terminal barrier signals are still aligned: the processor should observe the terminal only after
the barrier reaches “complete” at the barrier boundary.

## Physical Plan Insertion (`PhysicalBarrier`)

Physical plan optimization includes a final topology rewrite rule:

- Rule name: `insert_barrier_for_fan_in` (applied as the last physical optimization).
- For any physical node `P` where `P.children.len() > 1`, rewrite:
  - before: `P(children=[C1, C2, ..., Cn])`
  - after:  `P(children=[Barrier(children=[C1, C2, ..., Cn])])`

This marks every fan-in boundary explicitly in the physical plan.

`EXPLAIN` surfaces this node as `PhysicalBarrier` with `upstream_count=n`.

## BarrierProcessor Behaviour

`BarrierProcessor` is a pure synchronization + forwarding operator:

- It consumes `K` upstreams on both the data channel and the control channel
  (`expected_upstreams = K`).
- It forwards all non-control data downstream unchanged.
- It synchronizes barrier control signals using a shared aligner implementation.

### Per-Channel Isolation

Barrier synchronization state is maintained independently per channel:

- One aligner for barrier signals carried by the **data channel**
  (`StreamData::Control(ControlSignal)`).
- One aligner for barrier signals carried by the **control channel**
  (`ControlSignal`).

This preserves the invariant that data/control channels never cross-route.

### Alignment Rules

Alignment is implemented by `BarrierAligner` (`src/flow/src/processor/barrier.rs`).

Key:

- `BarrierKey = (barrier_id, kind)`

Rules:

- **No overlap**: while a barrier is pending, receiving a different `(barrier_id, kind)` is an
  error.
- **No duplicates**: receiving more than `expected_upstreams` for the same key is an error.
- **No upstream identity requirement**: synchronization uses only the expected upstream count and
  arrival count.

Handling:

- `Instant` signals are forwarded immediately.
- `Barrier` signals are swallowed until the barrier reaches “complete”, then forwarded exactly
  once.
- If the forwarded signal is terminal, the processor forwards it and then stops.

## Testing Notes

`BarrierControlSignal::SyncTest { barrier_id }` is a no-op barrier kind used by unit tests to
validate alignment behaviour without attaching additional runtime semantics.

There is a planner-level physical `EXPLAIN` test case that validates a two-sink pipeline inserts a
`PhysicalBarrier` below `PhysicalResultCollect`.
