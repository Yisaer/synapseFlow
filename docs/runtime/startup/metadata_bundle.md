# Metadata Bundle Design

## Background

veloFlux already has three ways to move metadata as a bundle:

- storage export
- storage import
- startup-time `init.json`

All three flows operate on the same logical resource set. The important design point is that the
bundle is a storage snapshot model, not an endpoint spec and not a runtime-only replication
protocol.

This document defines that shared model so future tests and features can reason about consistency
without duplicating import/export handler details.

## Goals

- Define the bundle as a complete metadata snapshot of the manager-owned resource set.
- Clarify replace semantics for import versus add-only semantics for `init.json`.
- Explain how desired state, flow-instance declarations, and startup hydration interact.
- Document failure and observability expectations.

## Non-Goals

- HTTP request/response manuals for import or export endpoints.
- Runtime data migration between live instances.
- Partial merge semantics between a bundle and existing storage.

## Bundle Scope

The current bundle contains:

- memory topics
- shared MQTT client configs
- streams
- named schemas
- pipelines
- pipeline definitions with inline desired run state
- WASM UDFs (metadata + `.wasm` binaries)

This scope is intentionally broader than pipelines alone. A pipeline snapshot is incomplete without
the metadata needed to rebuild its runtime context.

## Bundle Data Model

The exported payload is versioned as `ExportBundleV1` and includes:

- `exported_at`
- `resources.memory_topics`
- `resources.shared_mqtt_clients`
- `resources.streams`
- `resources.pipelines`, with an optional inline `run_state` that defaults to `Stopped`
- `resources.udfs`

Export is delivered as a **ZIP archive** containing `metadata.json`, a `wasm_files/`
directory with one `.wasm` binary per UDF keyed by SHA-256, and the installed source tree for
file-backed schemas under `schemas/<type>/<name>/`. The previous JSON-only format is no longer
supported.

Export sorts each resource collection by stable identity before serialization. This is a design
choice for deterministic diffs and predictable operator review, not a semantic ordering guarantee.

## Validation Rules

Import validates the bundle before touching storage. Current checks include:

- duplicate memory topic names inside the bundle
- memory topic capacity must be greater than zero
- duplicate shared MQTT client keys
- duplicate stream names
- duplicate pipeline ids
- each pipeline must reference a declared `flow_instance_id`
- normalized pipeline requests must still pass normal pipeline request validation
- duplicate UDF names (lowercase)
- each declared UDF must have a corresponding `<sha256>.wasm` file in the archive
- SHA-256 of the extracted `.wasm` file must match the declared value
- when the `wasm_udf` feature is enabled, the `.wasm` module must pass
  `WasmEngine::validate` and its metadata name must match the declared UDF name
- each file-backed schema entry filename must be a single path segment
- the extracted schema tree is copied to an internal staging directory that accepts only regular
  files and directories; symlinks and special files are rejected before schema parsing
- each file-backed schema must parse from
  `schemas/<type>/<name>/<entry>` in that sanitized staging tree; validation never falls back to
  the raw archive extraction directory or process working directory

Validation is whole-bundle and strict. There is no best-effort acceptance of a partially valid
bundle.

Import accepts a ZIP body (`application/zip`). Archive extraction rejects unsafe paths, duplicate
paths, symlinks, special files, excessive entry counts, oversized individual files, and excessive
total uncompressed size. The archive is unpacked to a temporary
directory, schema sources are copied to a sanitized staging tree, `metadata.json` and staged
schema sources are parsed and validated, and `.wasm` files are copied to the shared
`wasm_files/` directory after validation.

Legacy bundles containing a top-level `resources.pipeline_run_states` collection are rejected.
Pipeline run state is serialized using the existing `StoredPipelineDesiredState` representation;
the only format change is moving that value onto its owning pipeline as `run_state`.

## Replace Semantics

Import uses replace semantics.

That means:

- manager validates the incoming bundle
- manager prepares and activates the imported installed-schema tree
- storage replaces the entire metadata snapshot in one write transaction
- a metadata replacement failure restores the previous installed-schema tree
- absent resources are removed from storage

Import does not merge old and new snapshots resource by resource. The incoming bundle is treated as
the desired metadata world.

Import also does not immediately apply the imported metadata to running flow instances. The current
response explicitly reports `applied_to_runtime = false`. Runtime reconciliation remains a separate
step handled by startup hydration or later control-plane actions.

## Relationship To `init.json`

`init.json` reuses the same bundle shape but applies different semantics:

- the file is read from the data directory during startup only
- startup skips it if the file is missing
- startup also skips it when the file modified time is not newer than stored init-apply metadata
- when selected, the file is parsed and validated using the same snapshot-building path as import

The key semantic difference is write mode:

- import replaces the full snapshot
- `init.json` is add-only against existing storage

`init.json` first verifies that all resource identities are absent, then inserts resources and the
new apply metadata in one transaction. Duplicate conflicts fail startup and do not advance the
stored init-apply marker.

## Flow Instance Constraints

Pipelines inside a bundle are not free-floating. Each one must target a flow instance declared by
current manager configuration.

This has two consequences:

- a bundle is not universally portable across deployments with different instance declarations
- pipeline desired state is meaningful only relative to an instance that may later hydrate or apply
  the pipeline

Undeclared flow instances are rejected during import and during `init.json` apply.

## Failure Semantics

Import failure semantics:

- validation failures return before storage changes
- storage replacement failures leave the previous snapshot intact
- runtime state is not mutated as part of import

`init.json` failure semantics:

- file read or parse failure aborts startup
- validation failure aborts startup
- duplicate conflicts abort startup
- storage write failure aborts startup
- init-apply metadata is only advanced after a successful transactional apply

These semantics intentionally favor clear failure over partial convergence.

## Observability

Current observability surfaces include:

- `exported_at` in exported bundles
- import response counts per resource type
- import response echo of the previous exported bundle
- startup logs for `init_json_apply`
- persisted init-apply metadata:
  - `last_applied_at`
  - `last_init_json_modified_time`

The init metadata is part of the storage model because startup skip/apply behavior depends on it,
not just because operators may want timestamps.

## Testing Guidance

- Verify export includes every resource collection needed to rebuild pipeline context.
- Verify export ordering is stable across repeated reads of the same storage snapshot.
- Verify import rejects duplicate resource identities within the bundle.
- Verify omitted inline pipeline run state defaults to `Stopped`.
- Verify legacy top-level pipeline run-state collections are rejected.
- Verify import rejects pipelines bound to undeclared flow instances.
- Verify import rejects UDFs with duplicate names.
- Verify import rejects UDFs whose `.wasm` file is missing from the archive or has a SHA-256
  mismatch.
- Verify import replaces storage state instead of merging with pre-existing resources.
- Verify `init.json` skips when the file is missing.
- Verify `init.json` skips when stored apply metadata is newer or equal to file modified time.
- Verify stale init metadata triggers a retry and that duplicate conflicts fail the retry without
  advancing metadata.
- Verify `init.json` apply is atomic: resources and init metadata appear together or not at all.

## Future Work

- If runtime reconciliation is moved into import in the future, that should be documented as a new
  phase layered on top of the same storage snapshot contract rather than folded into bundle
  semantics silently.
