# Test Plan 06 Interaction Pairings

## Scope

- This document continues the planner/runtime pairing extraction after `test_plan_01.md` through
  `test_plan_04.md`.
- The goal is to separate three outcomes for interaction-oriented coverage work:
  - interactions that already exist in tests and only need registry entries
  - interactions whose testcase already exists but whose `covers` annotation is incomplete
  - interactions that still need a new testcase

## Registry Additions Backed by Existing Tests

- `runtime.eventtime.on_change_tumbling`
  - Backed by `eventtime_tumbling_window_with_on_change_gate_suppresses_unchanged_rows`.
  - This testcase already validates source on-change gating together with eventtime execution,
    watermark advancement, and tumbling-window output.

- `runtime.shared_source.sampler_dynamic_decode_lifecycle`
  - Backed by `shared_stream_sampler_dynamic_decode_lifecycle_keeps_outputs_in_sync`.
  - This testcase already validates sampler behavior together with shared-source dynamic decode
    updates and lifecycle changes.

- `runtime.memory.bytes_input_batching`
  - Backed by `memory_source_bytes_topic_batches_json_output`.
  - This testcase already validates bytes-topic memory input together with sink batching.

- `runtime.memory.collection_layout_normalize_materialize_alias`
  - Backed by `memory_collection_source_layout_normalize_then_collection_sink_materialize_aliases`.
  - This testcase already validates collection-source layout normalization together with collection
    sink materialization and alias-visible output.

## Existing Tests That Needed `covers` Fixes

- `runtime.stateful.memory_output_batching`
  - Backed by `stateful_projection_with_batched_sink_encoder`.
  - The testcase already exercised batching via `batch_count=2` and asserted two staged output
    payloads, but the previous `covers` list did not include `sink.output.batching`.

- `runtime.memory.collection_row_diff_omit_if_empty`
  - Backed by `memory_collection_sink_delta_omit_if_empty_suppresses_unchanged_collection`.
  - The testcase already exercised collection sink materialization, row-diff delta output, and
    omit-if-empty suppression, but the previous `covers` list omitted
    `sink.memory_collection.materialize` and `sink.output.row_diff`.

## Cross-Module Interaction Pairings With Split Evidence

### 1. Eventtime hidden on-change tumbling pairing

- Interaction ID: `runtime.eventtime.hidden_on_change_tumbling_pairing`
- Required features:
  - `planner.eventtime.hidden_column_preservation`
  - `source.on_change.gating`
  - `pipeline.runtime.eventtime`
  - `stream.watermark.propagation`
  - `stream.window.tumbling`
- Why existing coverage is insufficient:
  - Planner-side hidden-column preservation is already covered by
    `explain_pipeline_with_eventtime_enabled_keeps_hidden_eventtime_column_alive`.
  - Runtime-side source-on-change plus eventtime late-drop behavior is now covered by
    `eventtime_tumbling_window_with_on_change_gate_drops_late_rows_after_watermark`.
  - The interaction is still worth keeping as a cross-module planner/runtime pairing,
    even though no single testcase currently carries all required features in
    one coverage record.
- Current evidence split:
  - Keep planner-side evidence in the explain suite and runtime-side evidence in
    the pipeline suite.
- Why the split is intentional:
  - The planner assertion needs explain-level visibility into hidden
    `event_ts` retention and decoder metadata.
  - The runtime assertion needs event delivery, watermark movement, and late-row
    drop behavior that belongs in the pipeline suite.
- Promotion criteria:
  - Promote this interaction from `cross_module` to `active` only if a dedicated
    same-record testcase becomes necessary and can justify all required features
    in one `covers` list.
- Expected evidence:
  - Planner-side assertions should keep hidden `event_ts` alive in schema and decoder metadata.
  - Runtime-side assertions should continue to prove source gating, watermark advancement, and
    late-row dropping.

### 2. Shared-tail barrier alignment

- Interaction ID: `runtime.shared_tail.barrier_alignment`
- Required features:
  - `planner.physical.shared_tail_barrier_insertion`
  - `processor.barrier.alignment`
- Why existing coverage is insufficient:
  - Planner-side barrier insertion is already covered by the explain suite.
  - Runtime-side graceful-close alignment is now covered by
    `shared_tail_barrier_graceful_close_flushes_batched_sibling_before_shutdown`.
  - The interaction is still worth keeping as a cross-module planner/runtime pairing,
    even though the current evidence remains split across specialized suites.
- Current evidence split:
  - Keep planner insertion evidence in the explain suite and runtime alignment
    evidence in the pipeline suite.
- Why the split is intentional:
  - The planner assertion is about `PhysicalBarrier` insertion shape.
  - The runtime assertion is about shutdown-time alignment and sibling flush
    ordering.
- Promotion criteria:
  - Promote this interaction from `cross_module` to `active` only if one testcase can
    justify both barrier insertion and runtime alignment in the same coverage
    record without distorting the current test harness split.
- Expected evidence:
  - Planner-side assertions should continue to prove `PhysicalBarrier` insertion.
  - Runtime-side assertions should continue to prove that graceful shutdown does not terminate the
    shared tail before the batched sibling flushes.
