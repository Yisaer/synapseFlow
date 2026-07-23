# Test Plan 04 Covered Cases

## Scope

- This document tracks only the cases that were newly covered for the runtime-facing stream,
  source, sink, and connector scope recorded in this tracked plan.
- The counted coverage comes from:
  - `src/flow/tests/planner/**`
  - `src/flow/tests/pipeline/**`
  - selected connector, processor, and manager unit tests that were explicitly added to validate
    runtime-facing contracts from this plan
- This is a covered-cases inventory for the newly added tests, not a gap analysis. Known
  documentation mismatches and intentionally deferred items are excluded from this summary.

## Planner and Explain Pairings

### Source-level sampler and source-input planning

- Regular-stream sampler placement is covered by:
  - `select_with_sampler_inserts_sampler_before_decoder`
  - `test_shared_stream_ingest_decoder_wraps_sampler`
- Together, these cases validate that:
  - a non-shared source inserts `PhysicalSampler` between `PhysicalDataSource` and
    `PhysicalDecoder`
  - the shared-ingest helper keeps the decoder outside the sampler in the ingest explain tree
  - sampler explain output preserves interval and strategy metadata

- Source-on-change planner coverage is extended by:
  - `source_on_change_with_explicit_columns`
  - `source_on_change_without_columns_tracks_all_top_level_columns`
  - `source_on_change_keeps_hidden_tracked_columns_alive`
- These cases validate that:
  - explicit tracked columns become `PhysicalSourceChangeGate` inputs
  - `on_change` without explicit columns expands to all top-level columns
  - hidden tracked columns remain available even when the top-level projection narrows output

### Eventtime validation and hidden-column preservation

- Eventtime build validation is covered by
  `explain_pipeline_with_eventtime_enabled_rejects_when_no_stream_declares_eventtime`.
- This case validates that `eventtime.enabled=true` is rejected when no referenced stream declares
  eventtime metadata.

- Eventtime hidden-column preservation is covered by:
  - `explain_eventtime_simple_projection_keeps_decoder_eventtime_column`
  - `explain_pipeline_with_eventtime_enabled_keeps_hidden_eventtime_column_alive`
- These cases validate that:
  - top-level projection of a visible column does not prune away the eventtime column required by
    the decoder
  - the pruned datasource schema still retains the hidden eventtime column
  - decoder explain output still exposes eventtime metadata after pruning

### Memory source physical planning

- Memory-source physical path selection is covered by:
  - `memory_collection_source_inserts_layout_normalize`
  - `memory_bytes_source_keeps_decoder_path`
- These cases validate that:
  - a memory collection source with `decoder.type=none` inserts
    `PhysicalCollectionLayoutNormalize`
  - a memory bytes source with a decoder keeps the normal decoder path
  - the planner does not blur bytes-topic and collection-topic runtime responsibilities

## Runtime Pipelines

### Memory source runtime semantics

- Bytes-topic decode behavior is covered by
  `memory_source_bytes_topic_with_json_decoder_emits_expected_rows`.
- This case validates that a memory bytes topic with a JSON decoder emits the expected decoded rows
  through a normal pipeline path.

- Collection-topic build rejection is covered by
  `memory_source_collection_topic_with_non_none_decoder_is_rejected`.
- This case validates that a memory collection topic cannot be consumed through a non-`none`
  decoder path, even though the rejection currently happens at pipeline build time rather than
  stream creation time.

- Collection-topic layout normalization at pipeline level is covered by
  `pipeline_table_driven_memory_collection_sources_layout_normalize`.
- The `layout_normalize_reorders_and_fills_missing_with_null` case validates that:
  - message source names are rewritten to the stream binding
  - column order is normalized to the full stream schema order
  - missing columns are filled with `NULL`

### Memory collection sink runtime semantics

- Collection-sink output materialization is covered by
  `pipeline_table_driven_collection_sinks`.
- The covered cases validate that:
  - `select_star_with_alias_materializes_single_message` preserves full output rows while adding
    aliased columns
  - `alias_order_and_derived_columns_materialize_from_output_layout` preserves final alias order
    and materializes derived expressions into the collection payload

- Duplicate output-name rejection is covered by
  `memory_collection_sink_rejects_duplicate_output_column_names`.
- This case validates that duplicate final output names for a memory collection sink are rejected
  before runtime execution.

- Delta collection output-mask behavior is covered by
  `memory_collection_sink_delta_output_preserves_output_mask`.
- This case validates that memory collection sink materialization keeps row values, clears message
  source metadata, and preserves the delta output mask row by row.

- Delta plus omit-if-empty interaction is covered by
  `memory_collection_sink_delta_omit_if_empty_suppresses_unchanged_collection`.
- This case validates that:
  - the first changed collection is published
  - a fully unchanged diff collection is suppressed
  - later changed collections are published again

## Connector and Processor Contracts

### Memory connector boundaries

- Memory source connector contract coverage is provided by:
  - `memory_source_bytes_connector_subscribes_only_to_bytes_topics`
  - `memory_source_bytes_connector_continues_after_broadcast_lag`
- These cases validate that:
  - a bytes-topic source subscribes only to bytes topics and rejects collection-topic mismatch
  - broadcast lag does not terminate the source permanently
  - later bytes payloads still flow after a lag event

- Memory sink connector contract coverage is provided by:
  - `memory_bytes_sink_connector_publishes_bytes_and_rejects_collection_payloads`
  - `memory_collection_sink_connector_publishes_collections_and_rejects_bytes_payloads`
- These cases validate that:
  - the bytes sink accepts bytes payloads and rejects collection payloads
  - the collection sink accepts collection payloads and rejects bytes payloads
  - publish without subscribers is still treated as success for memory sinks

### Collection normalize and materialize processor contracts

- Direct collection-layout normalization is covered by
  `normalize_collection_rewrites_source_and_stabilizes_schema_order`.
- This case validates that the normalize processor:
  - rewrites message source to the stream binding
  - reorders fields to the expected schema order
  - fills missing fields with `NULL`
  - preserves tuple timestamp metadata

- Direct collection materialization fallback behavior is covered by
  `materialize_collection_fills_missing_columns_with_null_and_preserves_mask`.
- This case validates that the materialize processor:
  - fills unresolved message or affiliate columns with `NULL`
  - clears message source and affiliate payloads as expected for collection output
  - preserves the tuple output mask

### MQTT connector configuration ownership

- Standalone MQTT source option ownership is covered by:
  - `mqtt_source_build_mqtt_options_uses_stream_local_broker_and_client_id`
  - `mqtt_source_build_mqtt_options_enables_tls_and_secure_default_port`
- These cases validate that:
  - standalone MQTT source options use stream-local `broker_url` and `client_id`
  - plain MQTT defaults to port `1883`
  - TLS schemes enable secure transport and default to port `8883`

- MQTT sink connector contract and option ownership are covered by:
  - `mqtt_sink_connector_rejects_collection_payloads`
  - `mqtt_sink_build_mqtt_options_uses_sink_local_client_and_packet_limit`
  - `mqtt_sink_build_mqtt_options_enables_tls_and_secure_default_port`
- These cases validate that:
  - MQTT sink remains bytes-only and explicitly rejects collection payloads
  - standalone sink options use sink-local `client_id`
  - sink-local packet limits are respected
  - TLS sink connections use the secure default port

- Shared MQTT client option ownership is covered by:
  - `shared_mqtt_build_mqtt_options_uses_shared_client_packet_limit`
  - `shared_mqtt_build_mqtt_options_enables_tls_and_secure_default_port`
- These cases validate that shared-client option building uses shared-client configuration and keeps
  the documented default packet limit and TLS defaults.

## Manager and Runtime Boundary Semantics

### Shared MQTT manager negative cases

- Shared MQTT busy-operation rejection is covered by:
  - `start_pipeline_returns_conflict_when_shared_mqtt_key_operation_is_busy`
  - `start_pipeline_returns_conflict_when_shared_mqtt_sink_key_operation_is_busy`
- These cases validate that both source-side and sink-side shared MQTT connector keys participate in
  the pipeline operation lock, and busy keys fail with `409 Conflict` before run-state mutation.

### Flow-instance isolation

- Process-local resource isolation is covered by:
  - `memory_pubsub_isolated_across_instances`
  - `memory_source_and_sink_topics_are_isolated_across_instances`
- These cases validate that identical memory topic names in different dedicated flow instances do
  not leak source ingest or sink output across instance boundaries.
