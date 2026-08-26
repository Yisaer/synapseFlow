# Pipeline Lifecycle Semantics

## Background

Pipelines in veloFlux are both stored metadata and runtime objects.

The manager persists the pipeline spec and desired state in storage, and execution happens inside
a specific flow instance.

## Goals

- Define the stable pipeline resource model across storage and runtime.
- Clarify create, upsert, start, stop, and delete semantics.
- Explain how desired-state persistence interacts with runtime success or failure.

## Non-Goals

- SQL planning internals.
- Sink-specific connector behavior.
- Full REST endpoint reference.

## Pipeline Resource Model

A pipeline resource consists of:

- stored pipeline spec
- required persisted `revision`
- target `flow_instance_id`
- stored desired state (`running` or `stopped`)
- runtime installation inside the selected flow instance

The stored spec is the source of truth.

## Create Semantics

Create behavior:

1. canonicalize and validate the target `flow_instance_id`
2. validate pipeline request shape
3. acquire per-pipeline and shared-MQTT operation guards
4. persist the pipeline spec into storage
5. apply the pipeline to the runtime
6. runtime create failure triggers storage rollback

## Upsert Semantics

Upsert keeps the existing pipeline identity and flow-instance binding.

Current rules:

- `revision` is required and must be in `1..=9007199254740991`
- a greater revision performs replacement
- a lower revision returns `409 Conflict` with `older_revision`
- an equal revision and equal normalized definition is idempotent and does not
  mutate runtime
- an equal revision and different definition returns `409 Conflict` with
  `same_revision_different_spec`
- if a stored pipeline exists, its `flow_instance_id` is reused
- the new request must still pass normal create validation
- previous desired state is read from storage before replacement

Upsert behavior:

- explain the new definition before deleting the old runtime
- delete the old runtime and storage record
- persist the new spec
- create the new runtime
- if previous desired state was `running`, persist `running` again and best-effort restart

Upsert is therefore replace-in-place for metadata, but restart after replacement is best effort
rather than transactional.

## Start Semantics

Starting a pipeline first persists desired state as `running`, then asks the runtime to converge.

Start does not change pipeline revision.

Behavior:

- `start_pipeline` is idempotent when the runtime is already running
- if no runtime graph is currently materialized, flow lazily builds it from the stored definition
- start failure preserves the stored `running` desired state and records a runtime failure marker
  when the manager observes the failure
- if a processor task exits unexpectedly after startup begins, the runtime status becomes `failed`
  and the desired state is not rewritten automatically

## Stop Semantics

Stopping a pipeline first persists desired state as `stopped`, then asks the runtime to stop.

Stop does not change pipeline revision.

Supported stop modes at the manager surface are:

- `quick` (default)
- `graceful`

Behavior:

- stop is idempotent for already stopped pipelines
- flow marks status `stopped` and drops the runtime pipeline object
- a running pipeline is closed using the requested stop mode and timeout

## Delete Semantics

Delete is storage-authoritative with best-effort runtime cleanup.

Current behavior:

- load the stored pipeline first
- derive the flow-instance binding from stored metadata when possible
- try to delete the runtime copy
- ignore runtime cleanup failures after logging
- delete the stored pipeline record

For running pipelines, delete performs a quick close with a 5-second timeout.

The important invariant is that delete removes manager-owned metadata even if runtime cleanup is
imperfect. This avoids undeletable control-plane objects caused by stale runtime state.

## Desired State Persistence

Desired state is a separate stored record, not just a mirror of runtime status.

Why this matters:

- startup hydration needs a durable desired-state input
- create and upsert can succeed for storage while runtime application is incomplete
- list/status views may need to compare stored intent with current runtime state

Current persistence behavior:

- create does not automatically create a `running` desired state
- start writes `running` before runtime converge
- stop writes `stopped` before runtime converge
- processor-fatal runtime failures persist a runtime failure marker without rewriting desired state
- upsert preserves the previous desired state when possible
- scheduled create, upsert, and startup hydration enter `scheduled_stopped`; patrol evaluates the
  current schedule before writing `scheduled_running` and starting the runtime
- revision orders artifact definitions; operational start/stop state changes do
  not increment it

## Failed Runtime Semantics

`failed` is a runtime status, not a desired state. It means a processor task returned an error,
panicked, or exited unexpectedly while the pipeline was expected to run.

Failure behavior:

- the pipeline supervisor aborts remaining processor tasks
- flow marks the runtime pipeline as `failed`
- manager persists a runtime failure marker with the pipeline revision, failed processor, timestamp,
  and reason
- startup hydration restores the pipeline definition but does not auto-start a revision with a
  matching runtime failure marker
- scheduled patrol also skips auto-start while the matching runtime failure marker is present

Runtime failure handling does not replay data, rebuild state, or restart the pipeline automatically.

Manual lifecycle behavior:

- start is allowed for failed pipelines and acts as an explicit retry; a successful start clears the
  runtime failure marker
- a failed manual start preserves the running desired state and writes a new runtime failure marker
- stop is allowed for failed pipelines; it writes desired state `stopped`, clears the runtime failure
  marker, and reports `stopped`
- stop is also allowed for scheduled failed pipelines; it writes desired state `scheduled_stopped`,
  clears the runtime failure marker, and returns the pipeline to scheduler control for later patrol
  retries
- delete is allowed for failed pipelines and removes the stored definition, desired state, and
  runtime failure marker

## Failure And Rollback Semantics

Important failure behaviors:

- create failure rolls back storage
- upsert validates before removing the old runtime, reducing destructive failures
- restart after upsert is best effort; a failed restart preserves the stored desired state and
  records a runtime failure marker
- a processor-fatal runtime failure leaves desired state unchanged and requires explicit later
  lifecycle handling
- delete does not roll back metadata when runtime cleanup fails

These are deliberate tradeoffs between control-plane durability and synchronous runtime parity.

## Testing Guidance

- Verify create rollback for pipelines when runtime creation fails after storage persistence.
- Verify upsert preserves `flow_instance_id` and previous desired state.
- Verify running pipelines are best-effort restarted after upsert and keep their desired state when
  restart fails.
- Verify stop persists desired state before runtime stop.
- Verify delete removes storage even when runtime cleanup reports an error.
