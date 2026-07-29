# Table Scan Source

This document describes the developer-facing design for bounded table scans.

## Purpose

`stream` and `table` are separate catalog concepts:

- A stream subscribes to an external or in-process source and continuously receives new data.
- A table is a finite read source. In the current implementation, a table scan reads historical
  storage once and then terminates the data path.

The first supported table provider is `History`. It reads NanoMQ history Parquet files and decodes
the payload stored in the Parquet `data` column.

## Current Scope

Implemented:

- catalog-level `TableDefinition`
- history-backed table props
- logical `TableScan`
- physical `PhysicalTableScan`
- `TableScanProcessor`
- `SELECT` over one history table
- scalar projection and `WHERE` filtering
- aggregate queries that explicitly use `GROUP BY eoswindow()`

Not implemented yet:

- stream-table joins and lookup table planning
- table aliases in SQL
- table scan decode projection
- manager REST endpoints and persistent storage for table definitions

## Table Definition

A table definition mirrors the stream definition shape where that makes sense:

- `id`: SQL-visible relation name
- `type`: table provider type, currently `History`
- `schema`: decoded row schema
- `props`: provider-specific configuration
- `decoder`: payload decoder, currently using the same decoder registry as streams

Unlike streams, a table does not own a live subscription connector. It also does not currently expose
stream-only options such as `shared`, `sampler`, or `eventtime`.

For `History` tables, props are:

- `datasource`: directory containing history Parquet files
- `topic`: topic used to discover files by filename
- `time_column`: Parquet time column, default `ts`
- `batch_size`: optional Parquet reader batch size, default `100`

History table files use the existing NanoMQ naming convention:

```text
nanomq_{topic}-{start_ts}~{end_ts}_{seq}_{hash}.parquet
```

The scan reads matching files sorted by `seq`. Each Parquet row must contain:

- the configured time column as `Int64` or `UInt64`
- `data` as `Binary`

The `data` bytes are decoded with the table decoder into the table schema.

## Planning

During SQL planning, catalog resolution can return either a stream or a table. A table relation lowers
to logical `TableScan` instead of `DataSource`.

Table scans currently support scalar scan queries and a planner-only aggregate shape:

- exactly one table source is supported in a table-scan query
- stream-table mixed queries are rejected because lookup join planning is deferred
- aggregate mappings require an explicit `GROUP BY eoswindow()`
- incremental aggregate calls are rewritten to `PhysicalStreamingAggregation(window=eos)`
- non-incremental aggregate calls keep the unfused `PhysicalEosWindow -> PhysicalAggregation`
  plan shape

Table bindings use `SourceBindingKind::TableScan`. Logical column pruning keeps the full schema for
this binding kind. This is required because `TableScanProcessor` currently decodes the full payload
schema, and expression execution uses schema-derived `ColumnRef::ByIndex` references. Shrinking the
binding schema before runtime decode projection exists would make indexes point at the wrong
runtime columns.

## Runtime Behavior

`TableScanProcessor` is a source processor. It starts an internal scan task, reads history files,
decodes payload bytes, and emits decoded collections on the data channel.

When all matching input has been read, it sends `StreamData::stream_graceful_end(0)` on the data
channel. Downstream processors use the existing terminal data-channel behavior instead of a separate
end-of-scan signal. This keeps table scan completion aligned with graceful data-path shutdown:
downstream processors should finish already received data and flush/close according to their normal
terminal handling.

The control channel remains reserved for control signals. The table scan completion signal is not
mirrored onto the control channel.

`GROUP BY eoswindow()` has two runtime paths:

- Incremental aggregate calls use `StreamingEosAggregationProcessor`, which updates aggregate
  state for each incoming collection and finalizes once on data-channel graceful end.
- Non-incremental aggregate calls use `EosWindowProcessor`, which buffers decoded rows and emits
  one collection only after receiving data-channel graceful end. If no rows were buffered, it emits
  no collection.

## Test Coverage

Planner explain coverage lives in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Pipeline input/output coverage lives in:

- `src/flow/tests/pipeline/table_scan_tests.rs`

The pipeline tests build temporary history Parquet files, register a history table directly in a
`FlowInstance`, create a SQL pipeline, and assert the memory sink output.
