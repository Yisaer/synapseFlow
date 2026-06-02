# Test Plan 01 Covered Cases

## Scope

- This document tracks only the cases that are already covered within the scope defined by `tmp/test_plan.md`.
- The counted coverage comes from:
  - `src/flow/tests/planner/**`
  - `src/flow/tests/pipeline/**`
  - selected processor unit tests that were explicitly added to validate runtime processor contracts discussed in the test plan
- This is a covered-cases inventory, not a gap analysis. Uncovered items and known defects are intentionally excluded here.

## Planner Logical

### Top-level schema pruning

- Regular source top-level pruning is covered by `logical_optimizer_prunes_datasource_schema`.
- Wildcard degradation is covered by `logical_optimizer_select_star_disables_top_level_pruning`.
- Shared-source schema preservation is covered by:
  - `logical_optimizer_shared_source_keeps_full_schema_under_alias_projection`
  - `logical_optimizer_shared_source_keeps_full_nested_schema_and_records_required_schema`

### Struct and list pruning

- Struct field pruning explain shape is covered by `logical_optimizer_prunes_struct_fields_and_explain_renders_projection`.
- Dynamic list-index pruning is covered by:
  - `logical_optimizer_keeps_dynamic_list_index_and_prunes_element_struct_fields`
  - `logical_optimizer_keeps_dynamic_list_index_and_keeps_full_element_struct`
- Whole-element degradation is covered by `logical_optimizer_selecting_whole_list_element_keeps_full_element_struct`.

### Common subexpression elimination

- Positive `Compute` insertion and temp-column rewrite are covered by `logical_optimizer_inserts_compute_for_repeated_subexpr_in_project_and_filter`.
- Negative cases are covered by:
  - `logical_optimizer_does_not_cse_function_calls`
  - `logical_optimizer_does_not_cse_plain_identifier_or_literal`
- Stateful-boundary placement is covered by `logical_optimizer_cse_keeps_stateful_dependency_boundary`.

## Planner Physical

### Streaming aggregation rewrite

- Explain-level rewrite coverage exists for:
  - count-window streaming aggregation
  - tumbling-window streaming aggregation
  - sliding-window streaming aggregation
  - state-window streaming aggregation
- Representative cases include:
  - `optimize_rewrites_streaming_agg`
  - `optimize_rewrites_streaming_agg_for_sliding_window`
  - `optimize_rewrites_streaming_agg_for_state_window`
  - `case_with_stateful_rewrite`

### Streaming encoder rewrite

- Encoded sink batching lowering is covered by `encoded_sink_with_batch_lowers_to_sink_encoder`.
- Template-transform plus batching rewrite is covered by `transform_template_with_batch_uses_sink_encoder`.

### By-index projection rewrite and row-diff planning

- By-index projection rewrite coverage includes:
  - pure by-index rewrite
  - mixed projection partial rewrite
  - alias rewrite
  - wildcard negative guard
  - transform-enabled negative guard
  - shared-DAG ineligible guard
- Representative cases include:
  - `rewrites_pure_by_index_project_into_encoder`
  - `rewrites_mixed_projection_delaying_only_by_index_fields`
  - `rewrites_by_index_with_alias`
  - `does_not_rewrite_when_contains_wildcard`
  - `does_not_rewrite_by_index_projection_when_transform_enabled`
  - `does_not_rewrite_shared_project_when_any_sink_ineligible`
- Row-diff physical planning coverage includes:
  - standalone row-diff insertion
  - row-diff plus batching
  - partial late materialization
  - alias projection
  - memory-collection sink placement
  - wildcard degradation
  - template orthogonality
  - omit-if-empty insertion after row diff
- Representative cases include:
  - `row_diff_single_json_sink_inserts_physical_row_diff`
  - `row_diff_with_batch_keeps_row_diff_and_uses_sink_encoder`
  - `row_diff_partial_late_materialization_splits_row_diff_and_encoder_owned_columns`
  - `row_diff_alias_project_rewrites_into_row_diff`
  - `row_diff_memory_collection_sink_places_row_diff_before_materialize`
  - `row_diff_wildcard_projection_keeps_materializing_project`
  - `row_diff_and_template_are_orthogonal_in_plan_shape`
  - `row_diff_omit_if_empty_inserts_empty_suppress_after_row_diff`

### Eventtime validation and explain

- Eventtime-enabled explain shape is covered by `explain_pipeline_with_eventtime_enabled_prints_plans`.
- Eventtime validation error paths are covered by:
  - `explain_pipeline_with_eventtime_enabled_rejects_missing_eventtime_column`
  - `explain_pipeline_with_eventtime_enabled_rejects_unknown_eventtime_type`

### Physical sampler and barrier insertion

- Planner explain for sampler insertion is covered by the sampler explain case that emits `PhysicalSampler`.
- Planner explain for shared-tail barrier insertion is covered by the multi-sink by-index rewrite cases that emit `PhysicalBarrier`.

## Runtime Processors

### Barrier processor contracts

- Barrier alignment on the data channel is covered by `data_channel_barrier_waits_until_all_upstreams_arrive`.
- Barrier alignment on the control channel is covered by `control_channel_barrier_waits_until_all_upstreams_arrive`.
- Terminal barrier alignment is covered by `terminal_barrier_waits_until_all_data_upstreams_arrive_before_forwarding`.
- Same-channel overlap rejection is covered by `overlapping_barriers_on_same_channel_return_error`.
- Per-channel isolation is covered by `barrier_alignment_state_is_isolated_per_channel`.

### Streaming aggregation processors

- Tumbling aggregation watermark flush and grouped output are covered by `tumbling_aggregation_flushes_grouped_rows_on_watermark`.
- Sliding aggregation oldest-window emission with zero delay is covered by `sliding_aggregation_emits_oldest_window_per_tuple_when_delay_is_zero`.

## Runtime Pipelines

### Baseline pipeline data path

- Table-driven runtime coverage already exists for baseline query execution, including projection, filtering, ordering, windowed aggregation, and stateful-function pipelines.
- Representative suites include:
  - `pipeline_table_driven_queries`
  - `stateful_function_table_driven`

### Row diff, omit-if-empty, and source on-change

- Row-diff runtime coverage is provided by `pipeline_row_diff_json_table_driven`.
- Omit-if-empty runtime coverage is provided by `pipeline_omit_if_empty_json_table_driven`.
- Source-on-change runtime coverage is provided by `pipeline_source_on_change_json_table_driven`.
- Additional targeted source-on-change plus row-diff plus omit-if-empty coverage is provided by `hidden_tracked_column_with_delta_omit_if_empty_suppresses_redundant_rows`.

### Collection sinks and mixed consumers

- Mixed-consumer runtime coverage is provided by `pipeline_mixed_consumers_json_table_driven`.
- Memory-collection source layout normalization is covered by `pipeline_table_driven_memory_collection_sources_layout_normalize`.
- Collection-sink runtime coverage is provided by `pipeline_table_driven_collection_sinks`.
- Memory-collection sink delta materialization and output-mask preservation are covered by `memory_collection_sink_delta_output_preserves_output_mask`.

### Batching and streaming encoder runtime

- Baseline batching runtime coverage is provided by:
  - `pipeline_batching_table_driven`
  - `pipeline_encoder_transform_table_driven`
- Graceful-stop flushing of a partial batch is covered by `sink_encoder_flushes_partial_batch_on_graceful_stop`.
- Row-diff plus batching plus omit-if-empty runtime behavior is covered by `delta_batched_output_suppresses_empty_batches_but_keeps_non_empty_ones`.

### Sampler runtime

- Latest strategy sampling is covered by `sampler_latest_emits_only_last_value_per_interval`.
- Shutdown/close flush for buffered latest values is covered by `sampler_latest_flushes_buffer_on_close`.
- Sampler-before-filter/projection ordering is covered by `sampler_latest_before_filter_and_projection`.
- Sampler plus omit-if-empty is covered by `sampler_latest_with_omit_if_empty_suppresses_empty_windows`.
- Sampler plus row diff is covered by `sampler_latest_before_row_diff_delta_output`.
- Sampler plus streaming aggregation plus delta output is covered by `sampler_latest_before_streaming_aggregation_then_delta_output`.
- Sampler plus streaming aggregation plus batched output is covered by `sampler_latest_before_streaming_aggregation_then_batched_output`.
- Packer strategy merge and close-flush behavior are covered by:
  - `sampler_packer_emits_merged_payload_once_per_interval`
  - `sampler_packer_flushes_buffer_on_close`

### Stateful runtime pairings

- Stateful projection plus row diff is covered by `stateful_projection_followed_by_row_diff_delta_output`.
- Stateful projection plus batched streaming encoder is covered by `stateful_projection_with_batched_sink_encoder`.
- Stateful window aggregation runtime coverage is included in `stateful_function_table_driven`.

### Pipeline stats

- Base stats fields for a running pipeline are covered by `collect_pipeline_stats_returns_base_fields_for_running_pipeline`.
- Error behavior for a stopped pipeline is covered by `collect_pipeline_stats_returns_error_for_stopped_pipeline`.
- Runtime rebuild reset behavior is covered by `collect_pipeline_stats_reset_after_runtime_rebuild`.

## Mixed Planner and Runtime Pairings

- Shared-project by-index rewrite guard has runtime pairing coverage through `shared_project_with_json_and_collection_sink_preserves_alias_output`.
- Alias projection plus template transform has runtime pairing coverage through `transform_template_with_alias_projection_keeps_output_correct`.
- Row-diff plus memory-collection late materialization has runtime pairing coverage through `memory_collection_sink_delta_output_preserves_output_mask`.
- Sampler plus downstream row-diff, streaming aggregation, batching, and omit-if-empty combinations are covered by the dedicated sampler runtime cases listed above.
- Stateful plus downstream CSE boundary semantics are covered by the planner case `logical_optimizer_cse_keeps_stateful_dependency_boundary` together with the runtime stateful-function coverage already present in `stateful_function_table_driven`.
