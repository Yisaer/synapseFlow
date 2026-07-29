# manager / metadata / startup test plan 03

## Scope

This plan focuses on manager-side and metadata-side semantics that are still lightly covered by the
current test tree:

- manager REST handlers
- storage snapshot import/export
- startup `--init-dir` resource manifest
- metadata bundle semantics
- shared MQTT client control-plane behavior
- memory topic control-plane behavior
- pipeline create/start/stop/delete/upsert manager semantics
- stream create/delete/describe/list manager semantics
- plan cache startup semantics

This plan intentionally avoids repeating the main focus of `tests/docs/test_plan_02.md`:

- eventtime runtime
- shared stream dynamic decode runtime lifecycle
- shared-stream stats runtime/e2e focus

## Current gap summary

The current repository already has a few manager tests, but the spread is narrow:

- `import.rs`: one replace-snapshot happy path
- `export.rs`: one export response happy path
- `init_process.rs`: a few local unit tests
- `mqtt_client.rs`: two delete-path tests
- `pipeline/handlers.rs`: one busy-key start-path test
- `worker/server.rs`: a couple of shared MQTT reconcile/delete tests

What is still missing is a broader control-plane matrix:

- request validation edge cases
- rollback behavior on partial runtime installation failure
- snapshot import versus revision-based startup Apply semantics
- flow-instance declaration validation
- list/get/describe endpoint behavior
- pipeline desired-state persistence edge cases
- plan-cache startup hit/miss behavior

## Recommended split

I recommend implementing this plan mostly in:

- manager unit/integration tests under existing `src/manager/src/**`
- a small number of `tests/e2e/**` REST tests where HTTP mapping itself matters

## Proposed testcase inventory

### A. Memory topic manager semantics

#### 1. `create_memory_topic_defaults_capacity_when_omitted`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - omitted `capacity` uses `DEFAULT_MEMORY_PUBSUB_CAPACITY`
  - storage record and response payload both reflect the defaulted value

#### 2. `create_memory_topic_rejects_blank_topic_name`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - `400 Bad Request`
  - message contains `topic must not be empty`

#### 3. `create_memory_topic_normalizes_zero_capacity_to_one`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - `capacity = 0` is normalized to `1`
  - no raw zero-capacity record is persisted

#### 4. `create_memory_topic_conflicts_on_kind_mismatch_with_existing_runtime_topic`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - existing runtime topic kind mismatch yields `409 Conflict`

#### 5. `create_memory_topic_conflicts_on_capacity_mismatch_with_existing_runtime_topic`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - same topic, same kind, different capacity yields `409 Conflict`

#### 6. `create_memory_topic_rolls_back_storage_when_runtime_declare_fails`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - runtime declare failure removes the newly written storage record
  - response code matches the mapped runtime failure class

#### 7. `list_memory_topics_returns_stably_sorted_topics`

- Target:
  - `src/manager/src/memory_topic.rs`
- Assert:
  - returned list is sorted by topic name regardless of insertion order

### B. Shared MQTT client manager semantics

#### 8. `create_shared_mqtt_client_rejects_blank_required_fields`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - blank `key`, `broker_url`, `topic`, or `client_id` each fail with `400`

#### 9. `create_shared_mqtt_client_is_idempotent_for_identical_config`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - second identical create returns `200 OK`
  - storage/runtime are not duplicated

#### 10. `create_shared_mqtt_client_conflicts_for_different_config`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - same key with changed config returns `409 Conflict`
  - error includes old config summary

#### 11. `create_shared_mqtt_client_rolls_back_storage_on_runtime_install_failure`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - if one runtime instance rejects creation, storage write is rolled back

#### 12. `create_shared_mqtt_client_rolls_back_earlier_runtime_instances_on_later_failure`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - already-created instance copies are removed if a later instance fails

#### 13. `get_shared_mqtt_client_returns_not_found_for_unknown_key`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - `404 Not Found`

#### 14. `list_shared_mqtt_clients_returns_sorted_keys`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - list ordering is deterministic by key

#### 15. `delete_shared_mqtt_client_returns_not_found_when_missing`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - deleting unknown key returns `404`

#### 16. `create_shared_mqtt_client_returns_conflict_when_key_is_busy`

- Target:
  - `src/manager/src/mqtt_client.rs`
- Assert:
  - mutation lock contention yields `409 Conflict`

### C. Stream create/delete/describe/list manager semantics

#### 17. `create_stream_rejects_unknown_schema_type`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - unknown `schema.type` returns `400`

#### 18. `create_stream_rejects_unknown_non_none_decoder_type`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - unknown decoder returns `400`

#### 19. `create_stream_rejects_shared_memory_stream`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - `shared=true` with `type=memory` returns `400`

#### 20. `create_stream_rejects_shared_stream_with_decoder_none`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - `shared=true` plus `decoder.type=none` returns `400`

#### 21. `create_stream_rejects_decoder_none_with_non_memory_type`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - `decoder.type=none` on non-memory stream returns `400`

#### 22. `create_stream_rejects_eventtime_when_decoder_none`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - memory stream with `decoder.type=none` plus eventtime returns `400`

#### 23. `create_stream_rolls_back_storage_when_late_instance_install_fails`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - partial multi-instance install failure rolls back storage and earlier runtime copies

#### 24. `describe_stream_returns_eventtime_and_shared_flags_from_stored_spec`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - describe response round-trips eventtime config and shared metadata correctly

#### 25. `list_streams_round_trips_shared_flag_without_runtime_shared_stream_item`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - `shared` flag round-trips from storage for both shared and non-shared streams
  - `shared_stream` is currently not populated by `GET /streams`
  - shared-stream runtime details should be validated via `GET /streams/:name/shared/stats`

#### 26. `delete_stream_returns_conflict_while_shared_stream_is_still_referenced`

- Target:
  - `src/manager/src/stream.rs`
- Assert:
  - referenced shared stream cannot be deleted
  - response maps to conflict instead of silent drop

### D. Metadata bundle import/export semantics

#### 27. `export_bundle_contains_all_resource_collections`

- Target:
  - `src/manager/src/export.rs`
- Assert:
  - memory topics, shared MQTT clients, streams, pipelines, and run states all appear

#### 28. `export_bundle_ordering_is_stable_across_repeated_reads`

- Target:
  - `src/manager/src/export.rs`
- Assert:
  - repeated export over unchanged storage produces identical order

#### 29. `import_rejects_duplicate_memory_topic_names`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - whole-bundle validation fails before storage mutation

#### 30. `import_rejects_zero_capacity_memory_topic`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - bundle validation rejects capacity `0`

#### 31. `import_rejects_duplicate_shared_mqtt_keys`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - duplicate key in bundle fails validation

#### 32. `import_rejects_duplicate_stream_names`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - duplicate stream names fail validation

#### 33. `import_rejects_duplicate_pipeline_ids`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - duplicate pipeline ids fail validation

#### 34. `import_rejects_duplicate_pipeline_run_state_entries`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - duplicate run-state entries fail validation

#### 35. `import_rejects_pipeline_run_state_for_missing_pipeline`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - missing referenced pipeline causes `400`

#### 36. `import_rejects_pipeline_bound_to_undeclared_flow_instance`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - undeclared `flow_instance_id` fails validation before replace

#### 37. `import_normalizes_missing_flow_instance_id_to_default`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - omitted instance id is normalized to `default`
  - normalized request still passes standard pipeline validation

#### 38. `import_replace_semantics_remove_absent_old_resources`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - old resources missing from incoming bundle disappear from storage
  - import does not merge snapshots

#### 39. `import_does_not_apply_runtime_and_reports_applied_to_runtime_false`

- Target:
  - `src/manager/src/import.rs`
- Assert:
  - response field `applied_to_runtime` remains `false`
  - runtime objects are not silently created during import

### E. Startup / startup resource directory / plan cache semantics

#### 40. `init_json_apply_is_atomic_for_resources_and_metadata`

- Target:
  - `src/manager/src/init_process.rs`
- Assert:
  - resources and init metadata appear together or not at all

#### 41. `init_json_duplicate_conflict_does_not_advance_apply_metadata`

- Target:
  - `src/manager/src/init_process.rs`
- Assert:
  - failed retry leaves old apply marker untouched

#### 42. `init_json_rejects_undeclared_flow_instance_in_pipeline_bundle`

- Target:
  - `src/manager/src/init_process.rs`
- Assert:
  - startup apply fails fast on undeclared instance binding

#### 43. `bootstrap_from_storage_skips_invalid_pipeline_but_restores_valid_neighbors`

- Target:
  - `src/manager/src/storage_bridge.rs`
- Assert:
  - invalid stored pipeline does not block restoration of unrelated valid resources

#### 44. `plan_cache_disabled_always_builds_from_sql_even_with_snapshot_present`

- Target:
  - startup/hydration integration test
  - candidate file: new test near `src/manager/src/storage_bridge.rs`
- Assert:
  - cached snapshot is ignored when `options.plan_cache.enabled=false`

#### 45. `plan_cache_hit_uses_snapshot_when_pipeline_and_stream_hashes_match`

- Target:
  - startup/hydration integration test
- Assert:
  - matching snapshot path is used
  - logical/physical plan still rebuild correctly

#### 46. `plan_cache_miss_rebuilds_when_stream_definition_hash_changes`

- Target:
  - startup/hydration integration test
- Assert:
  - changed referenced stream invalidates snapshot

#### 47. `plan_cache_delete_cascades_snapshot_cleanup`

- Target:
  - storage-level test
- Assert:
  - deleting a pipeline also removes `plan_snapshots[pipeline_id]`

## Recommended implementation order

### Batch 1

- Cases `8` to `16`
- Cases `17` to `23`

Reason:

- shared MQTT and stream create validation are high-value control-plane guards
- failures are mostly unit/integration friendly and do not require large runtime harnesses

### Batch 2

- Cases `29` to `39`
- Cases `40` to `43`

Reason:

- metadata bundle and the startup resource manifest semantics are easy to regress and already documented clearly

### Batch 3

- Cases `44` to `47`
- Cases `24` to `28`

Reason:

- plan-cache and richer endpoint semantics are useful but require heavier setup and careful fixtures

## Count

This plan proposes **47 testcase ideas**.

Even if you trim duplicated or low-value ones during implementation, there is still enough room to
land a focused subset of **20+ concrete tests** without exhausting the theme.
