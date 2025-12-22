# Pipeline Plan Cache (Plan Snapshot)

## Background

Today, every pipeline start rebuilds the full execution stack:

1. Parse SQL
2. Build logical plan
3. Optimize and build physical plan
4. Create processors

This is unnecessary when the pipeline definition and its dependent streams have not changed.
The plan cache persists a previously built plan snapshot so subsequent starts can reuse it.

## Goals

- Skip repeated SQL parsing and logical planning when inputs are unchanged.
- Persist plan snapshots in the existing storage module (metadata namespace, `redb`).
- Keep the design safe: never persist runtime resources; always rebind at start.
- Make behavior observable via simple hit/miss logs.

## Non-Goals

- Cross-version compatibility of snapshots across different `flow` builds.
- Advanced cache invalidation beyond the agreed fingerprint inputs.
- Storing runnable processor instances directly.
- Persisting physical plans (too many runtime-only fields).

## Configuration

Pipelines include an `options` object for optimization switches.
Plan cache is controlled by:

- `options.plan_cache.enabled: bool` (default: `false`)

`options` is carried inside `flow::pipeline::PipelineDefinition` and `flow` owns the cache
hit/miss decision.

## Fingerprint

A plan snapshot is valid only if it matches the inputs computed at startup. The snapshot stores:

- `pipeline_json_hash`: hash of the stored pipeline `raw_json`
- `stream_json_hashes`: hash of each referenced stream `raw_json`
- `flow_build_id`: build identifier from `build_info::build_id()` (`commit_id + git_tag`)

`fingerprint` is currently stored as a convenience string derived from the above (not used as the
storage key). Hashing uses an internal FNV-1a 64-bit hex implementation and hashes the JSON exactly
as stored (no canonicalization).

## Snapshot Content

The snapshot stores serialized plan IR, not live runtime objects:

- Optimized logical plan (logical plan IR)
- Metadata required for validation (`pipeline_json_hash`, `stream_json_hashes`, `flow_build_id`)

At startup, the system rebuilds runtime state by:

- LogicalPlanIR -> LogicalPlan (rebind stream schemas/decoders from Catalog)
- Run `optimize_logical_plan` again to rebuild pruned bindings
- Build and optimize the physical plan, then create processors and attach source connectors

Note: Logical plan IR is encoded as JSON bytes (`serde_json::to_vec/from_slice`) to support embedded
`serde_json::Value` fields (e.g. sink connector settings).

## Storage Layout (Metadata Namespace)

This feature stores snapshots in the existing metadata storage (`redb`) by adding a table:

- `plan_snapshots`: key = `pipeline_id`, value = `StoredPlanSnapshot`

`StoredPlanSnapshot` includes at least:

- `pipeline_id`
- `fingerprint`
- `pipeline_json_hash`
- `stream_json_hashes`
- `flow_build_id`
- `logical_plan_ir` (bytes)

## Startup Flow

When loading pipelines from storage on process startup:

1. Manager reads pipeline `raw_json`, referenced stream `raw_json`, and optional snapshot record.
2. Manager calls `flow` to build the pipeline via `create_pipeline_with_plan_cache(...)`.
3. In `flow`:
   - If `options.plan_cache.enabled == false`: always build from SQL.
   - If enabled and snapshot matches hashes + `flow_build_id`: build from logical plan IR (no SQL parse).
     - Emit a simple log line: plan cache hit.
   - Otherwise: build from SQL, and return `logical_plan_ir` for writeback.
     - Emit a simple log line: plan cache miss.
4. On miss, manager writes back `StoredPlanSnapshot` into `plan_snapshots[pipeline_id]`.
5. Both hit and miss paths print the logical/physical explain after physical plan optimization
   (same output as the standard pipeline build path).

## GC (Deletion Cleanup)

When a pipeline is deleted, its snapshot must be deleted as well:

- `storage.delete_pipeline(pipeline_id)` cascades to delete `plan_snapshots[pipeline_id]` in the same
  `redb` write transaction.

This keeps storage tidy without requiring a separate background GC for this MVP.

## Observability

Log requirements are intentionally minimal:

- On start with cache enabled, emit one line indicating either:
  - plan cache hit (pipeline id)
  - plan cache miss (pipeline id)

No miss reason is required for this iteration.
