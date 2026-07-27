# SELECT syntax

This document describes the supported `SELECT` statement syntax in veloFlux.

veloFlux currently accepts exactly one SQL statement, and it must be a `SELECT`.

## Supported grammar (overview)

```text
SELECT <projection>
FROM <source_name>
[WHERE <expr>]
[GROUP BY <expr> [, <expr> ...]]
```

Notes:

- Window declarations are written inside `GROUP BY` (see `user_docs/sql/window.md`).
- `HAVING` is intentionally not documented here (not currently supported).
- `<source_name>` can be a stream or a registered table.

## Projection

`<projection>` is one or more projection items separated by commas:

- `SELECT <expr> [, <expr> ...]`
- `SELECT <expr> AS <alias>`
- `SELECT *`
- `SELECT <source>.*`

Examples:

```sql
SELECT a, b + 1 FROM s
SELECT a + b AS total FROM s
SELECT * FROM s
SELECT t.* FROM t
```

## FROM

`FROM <source_name>`

The source name must match a stream or table exposed by the runtime catalog.

Example:

```sql
SELECT * FROM source_stream
SELECT * FROM history_table
```

### Table Sources

Table sources are finite scan sources. A table scan reads the available table data once and then ends
the pipeline's data path.

Currently supported table query forms:

```sql
SELECT * FROM history_table
SELECT speed + 1 AS next_speed FROM history_table WHERE ts > 1
```

Current limitations for table sources:

- table aliases are not supported
- stream-table joins are not supported
- aggregate functions over table scans are not supported

## WHERE

`WHERE <expr>`

Filters rows before any grouping/windowing.

Example:

```sql
SELECT * FROM s WHERE a > 10 AND b != 0
```

## GROUP BY

`GROUP BY <expr> [, <expr> ...]`

Defines grouping keys for aggregation/windowing. A window declaration, when used, is written as a
special item inside `GROUP BY` (see `user_docs/sql/window.md`).

Examples:

```sql
SELECT a FROM s GROUP BY a
SELECT * FROM s GROUP BY tumblingwindow('ss', 10)
SELECT * FROM s GROUP BY tumblingwindow('ss', 10), device_id
```

## Validation workflow

- Fetch schema: `GET /streams/describe/:name`
- For table sources, ensure the table has been registered in the runtime catalog before pipeline
  creation.
- Create pipeline with SQL: `POST /pipelines`
- Validate lowering/execution plan: `GET /pipelines/:id/explain`
