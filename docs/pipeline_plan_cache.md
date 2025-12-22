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

- Skip repeated SQL parsing and planning when inputs are unchanged.
- Persist plan snapshots in the existing storage module (metadata namespace, `redb`).
- Keep the design safe: never persist runtime resources; always rebind at start.
- Make behavior observable via simple hit/miss logs.

## Non-Goals

- Cross-version compatibility of snapshots across different `flow` builds.
- Advanced cache invalidation beyond the agreed fingerprint inputs.
- Storing runnable processor instances directly.

## Configuration

Pipelines include an `options` object for optimization switches.
Plan cache is controlled by:

- `options.plan_cache.enabled: bool` (default: `false`)

When disabled, the system neither reads nor writes plan snapshots.

## Fingerprint

A plan snapshot is valid only if its stored fingerprint matches the fingerprint computed at
startup. The fingerprint includes exactly these factors:

1. Pipeline fingerprint: hash of the pipeline JSON used at creation time (`raw_json`).
2. Stream fingerprint(s): hash of each involved stream JSON as stored (`raw_json`).
3. Flow build fingerprint: `commit_id + git_tag` combined as a build identifier.

The final fingerprint is a hash over the concatenation of the above components:

```text
fingerprint = H(pipeline_raw_json || stream_raw_json_1 || ... || stream_raw_json_n || flow_build_id)
```

Notes:

- The pipeline/stream JSON is hashed exactly as stored (no canonicalization).
- The flow build identifier should be embedded at build time so it is available at runtime.

## Snapshot Content

The snapshot stores serialized plan IR, not live runtime objects:

- Optimized logical plan (logical plan IR)
- Metadata required for validation (fingerprint, flow build id)

At startup, the system **rehydrates** the IR by rebinding it into runtime processors and
injecting runtime dependencies (connectors, codecs, schema registry, state backends).

## Storage Layout (Metadata Namespace)

This feature stores snapshots in the existing metadata storage (`redb`) by adding a table:

- `plan_snapshots`: key = `pipeline_id`, value = `StoredPlanSnapshot`

`StoredPlanSnapshot` includes at least:

- `pipeline_id`
- `fingerprint`
- `flow_build_id`
- `logical_plan_ir_bytes`
- optional metadata: `created_at`, size, etc.

## Startup Flow

When starting a pipeline:

1. Load pipeline `raw_json` and the dependent streams' `raw_json`.
2. Compute `fingerprint`.
3. If `options.plan_cache.enabled == true`:
   - Read `plan_snapshots[pipeline_id]`.
   - If present and `stored.fingerprint == fingerprint`, load snapshot and rebind processors.
     - Emit a simple log line: plan cache hit.
   - Otherwise, build from scratch and overwrite `plan_snapshots[pipeline_id]` on success.
     - Emit a simple log line: plan cache miss.
4. If `options.plan_cache.enabled == false`:
   - Always build from scratch and do not write any snapshot.

## GC (Deletion Cleanup)

When a pipeline is deleted, its snapshot must be deleted as well:

- On `delete_pipeline(pipeline_id)`, also delete `plan_snapshots[pipeline_id]`.

This keeps storage tidy without requiring a separate background GC for this MVP.

## Observability

Log requirements are intentionally minimal:

- On start with cache enabled, emit one line indicating either:
  - plan cache hit (pipeline id)
  - plan cache miss (pipeline id)

No miss reason is required for this iteration.
