# Test Plan 05 Covered Cases

## Scope

- This document tracks newly added coverage for repository-owned semantic validation, SQL planning/lowering boundaries, import/export invariants, and lifecycle behavior.
- Coverage comes from:
  - fixed deterministic API/contract tests
  - `proptest`-based property tests over typed request structs, planner inputs, and bundle models
  - selected planner/runtime boundary tests
- Thin HTTP/body-shape rejection checks are included only as REST smoke coverage and are not counted as core semantic fuzz coverage.
- This is a covered-cases inventory, not a gap analysis. Byte-level fuzzing and parser-hardening work are excluded.
- Startup the startup resource manifest semantics are out of scope; this plan focuses on import/export replacement behavior.

---

## Fixed API and Contract Coverage

### Stream semantic validation

- Extended by:
  - `create_stream_rejects_missing_name`
  - `create_stream_rejects_missing_type`
  - `create_stream_rejects_missing_schema`
  - `create_stream_rejects_duplicate_name`
  - `create_stream_rejects_empty_columns`
  - `create_stream_rejects_column_without_name`
  - `create_stream_rejects_column_without_data_type`
  - `create_stream_rejects_invalid_data_type`
  - `create_stream_rejects_missing_mqtt_broker_url`
  - `create_stream_rejects_missing_mqtt_topic`
  - `create_stream_rejects_empty_mqtt_topic`

- Validates:
  - required metadata and uniqueness constraints
  - schema correctness before planner/runtime use
  - type-specific props enforcement
  - no partial persistence on invalid input

---

### Pipeline semantic validation

- Extended by:
  - `create_pipeline_rejects_invalid_sql_syntax`
  - `create_pipeline_rejects_empty_sql`
  - `create_pipeline_rejects_unknown_stream_reference`
  - `create_pipeline_rejects_unknown_column_reference`
  - `create_pipeline_rejects_missing_sinks`
  - `create_pipeline_rejects_empty_sinks`
  - `create_pipeline_rejects_invalid_sink_type`
  - `create_pipeline_rejects_missing_required_sink_props`
  - `create_pipeline_rejects_valid_sql_with_invalid_sink`
  - `create_pipeline_rejects_valid_sink_with_invalid_sql`

- Validates:
  - planner-visible SQL failures
  - sink semantic constraints
  - atomic rejection of partially valid specs
  - no leakage of partial state

---

### Dependency and mutation handling

- Extended by:
  - `delete_stream_returns_conflict_when_pipeline_still_references_it`
  - `create_pipeline_rejects_nonexistent_stream_reference`
  - `upsert_pipeline_rejects_nonexistent_stream_reference`
  - `start_pipeline_after_deleting_referenced_stream_is_rejected`

- Validates:
  - dependency protection
  - consistency across multi-step mutations
  - rejection of broken runtime states

---

### Upsert lifecycle

- Extended by:
  - `upsert_pipeline_rejects_invalid_sql`
  - `upsert_pipeline_rejects_missing_sinks`
  - `upsert_pipeline_rejects_invalid_stream_reference`
  - `upsert_pipeline_preserves_running_state`
  - `upsert_pipeline_preserves_stopped_state`

- Validates:
  - same constraints as create
  - desired-state preservation
  - no unintended state transitions

---

### Import / export contract

- Import validation:
  - `import_rejects_missing_resources`
  - `import_rejects_invalid_json_shape`
  - `import_rejects_duplicate_identifiers`
  - `import_rejects_invalid_resources`
  - `import_rejects_empty_name_and_zero_capacity`
  - `import_rejects_dangling_pipeline_run_state_reference`

- Replace / rollback:
  - `import_empty_bundle_replaces_existing_snapshot`
  - `import_full_replace_removes_missing_resources`
  - `import_invalid_bundle_after_valid_import_preserves_previous_state`
  - `export_then_reimport_restores_prior_snapshot`

- Export:
  - `export_empty_storage_returns_empty_bundle`
  - `export_populated_storage_returns_expected_snapshot`
  - `export_repeated_without_changes_is_identical`
  - `export_arrays_are_sorted_by_stable_identifiers`

- Validates:
  - semantic bundle correctness
  - full-replace behavior
  - rollback safety
  - deterministic export

---

## SQL Surface and Planner Coverage

### SQL surface across parser, logical validation, and physical build

- Extended by:
  - `proptest_supported_select_fragments_parse_and_explain_without_panic`
  - `proptest_near_boundary_select_fragments_fail_with_controlled_validation_errors`
  - `proptest_window_queries_parse_validate_and_lower_or_reject_cleanly`
  - `proptest_stateful_aggregate_placeholder_interactions_are_handled_without_panic`
  - `proptest_alias_and_wildcard_combinations_preserve_planner_contract`
  - `proptest_struct_and_list_access_fragments_parse_and_lower_or_reject_cleanly`
  - `proptest_source_input_mode_specific_queries_are_handled_consistently`
  - `proptest_eventtime_enabled_queries_build_or_reject_with_controlled_errors`

- Validates:
  - SQL surface behavior across three distinct stages:
    - parser acceptance (`parse_sql_with_registries`)
    - logical semantic validation and rewriting
    - physical plan construction and runtime constraints
  - that parser acceptance alone does not imply support
  - that SQL may:
    - fail at parse time (dialect / syntax constraints)
    - fail at logical validation (schema, alias, aggregation, typing rules)
    - fail at physical build (runtime/operator/registry constraints)
  - all failure modes are controlled, deterministic, and non-panicking

- Explicitly covers:
  - cases where:
    - parse succeeds but logical planning fails
    - logical planning succeeds but physical build fails
  - boundary interactions between parser output (`SelectStmt`) and planner lowering

---

### Alias resolution, wildcard expansion, and projection constraints

- Validates:
  - left-to-right alias resolution semantics in SELECT
  - no forward reference to SELECT aliases
  - alias visibility limited to supported clauses (SELECT / WHERE / ORDER BY)
  - rejection of alias usage in:
    - HAVING
    - GROUP BY
    - window definitions
  - alias name constraints:
    - no duplicates
    - no collision with input column names
    - no reserved/internal names
  - wildcard and qualified wildcard expansion behavior
  - stability of projection naming after alias rewrite
  - enforcement of unique output field names at logical plan level

---

### Schema-aware typed expression validation

- Validates:
  - struct field access correctness (field existence and resolution)
  - list indexing semantics:
    - index must be non-negative
    - index operator only valid on list types
  - JSON / map / nested access propagation
  - qualified column resolution across multiple sources
  - type-aware validation across:
    - SELECT
    - WHERE
    - HAVING
    - GROUP BY
    - ORDER BY
  - that invalid expressions fail during logical validation, not later or silently

---

### Window semantics and lowering behavior

- Validates:
  - parser acceptance of window syntax independent of full validation
  - separation between:
    - syntax extraction (parser stage)
    - semantic validation (logical stage)
    - physical lowering (physical stage)
  - distinct handling of window types:
    - tumbling
    - sliding
    - count
    - state-based windows
  - that different window families produce different logical and physical plans
  - that unsupported or incomplete window specifications fail at controlled stages

---

### Stateful and aggregate transformation interactions

- Validates:
  - transformation order:
    - stateful function rewrite before aggregate rewrite
  - nested interactions such as:
    - stateful functions inside aggregates
  - placeholder allocation and deduplication stability
  - correctness of aggregate and stateful registry resolution
  - that invalid or unsupported combinations fail deterministically without panic

---

### Source binding and input-mode-dependent planning

- Validates:
  - source resolution and schema binding correctness
  - handling of different source binding kinds:
    - regular sources
    - shared streams
    - memory-backed collections
  - decoder constraints and registration requirements
  - on-change input gating behavior and column tracking
  - required column propagation for shared streams
  - that planner behavior changes appropriately with source input configuration
  - that invalid source configurations fail at logical or physical stages as expected

---

### Eventtime-aware planning and build behavior

- Validates:
  - that enabling eventtime changes both logical optimization and physical plan construction
  - correct propagation of eventtime configuration into:
    - logical planning options
    - physical watermark strategy selection
  - distinction between:
    - processing-time execution
    - eventtime-based execution
  - validation of eventtime column existence in effective schema
  - correct handling of late tolerance and watermark configuration
  - that some queries:
    - succeed without eventtime
    - fail or change behavior when eventtime is enabled
  - that eventtime-related failures may occur at physical build stage after successful logical planning

---

### Explain / build consistency

- Extended by:
  - `supported_sql_fragments_match_expected_logical_explain_shape`
  - `supported_sql_fragments_match_expected_physical_explain_shape`
  - `unsupported_sql_fragments_fail_with_stable_errors`

- Validates:
  - stability of logical plan structure across equivalent SQL
  - stability of physical plan topology under supported configurations
  - consistent and deterministic error classification across:
    - logical validation failures
    - physical build failures

---

## Property-Based Coverage

### Stream semantics

- Extended by:
  - `proptest_stream_schema_nested_struct_and_list_combinations`
  - `proptest_stream_schema_rejects_invalid_struct_and_list_shapes`
  - `proptest_stream_schema_rejects_invalid_data_type_combinations`
  - `proptest_stream_type_props_consistency_for_mqtt_and_history`
  - `proptest_stream_rejects_cross_type_props_mismatch`
  - `proptest_decoder_eventtime_and_sampler_validation`

- Validates:
  - recursive schema correctness
  - type/prop consistency
  - stable rejection of invalid combinations

---

### Pipeline semantics

- Extended by:
  - `proptest_pipeline_sink_type_specific_props_validation`
  - `proptest_pipeline_encoder_and_option_shapes_are_validated`
  - `proptest_pipeline_rejects_duplicate_or_missing_sink_identifiers`
  - `proptest_pipeline_mixed_validity_requests_do_not_partially_register`

- Validates:
  - sink constraints
  - option correctness
  - atomic behavior

---

### Import / export invariants

- Extended by:
  - `proptest_invalid_import_never_mutates_existing_storage`
  - `proptest_full_replace_removes_resources_missing_from_new_bundle`
  - `proptest_export_round_trip_restores_prior_state`
  - `proptest_import_rejects_partially_valid_resource_bundles_atomically`
  - `proptest_export_is_deterministic_under_repeated_reads`
  - `proptest_export_arrays_remain_sorted_under_randomized_insert_order`

- Validates:
  - storage safety
  - full-replace semantics
  - deterministic output

---

## Lifecycle Coverage

### Backend-specific behavior

- Extended by:
  - `proptest_in_process_pipeline_state_transition_sequences_preserve_expected_desired_state`
  - `proptest_worker_backed_pipeline_state_transition_sequences_preserve_expected_desired_state`
  - `proptest_import_export_sequence_composition_preserves_contract_invariants`

- Validates:
  - lifecycle behavior
  - correct state transitions
  - stability across operation sequences

---

## REST Smoke Coverage

### HTTP extraction

- Extended by:
  - `rest_rejects_wrong_shape_stream_payload_without_panic`
  - `rest_rejects_wrong_shape_pipeline_payload_without_panic`
  - `rest_rejects_wrong_shape_import_payload_without_panic`

- Validates:
  - clean rejection of malformed requests
  - no panic in extractor layer

---

## Out of Scope

- Startup resource revision arbitration and best-effort Apply semantics
- Parser-level fuzzing beyond planner contract boundaries
