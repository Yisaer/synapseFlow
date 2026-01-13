# SELECT syntax

This document describes the supported `SELECT` statement syntax in veloFlux.

veloFlux currently accepts exactly one SQL statement, and it must be a `SELECT`.

## Supported grammar (overview)

```text
SELECT <projection>
FROM <stream_name>
[WHERE <expr>]
[GROUP BY <expr> [, <expr> ...]]
```

Notes:

- Window declarations are written inside `GROUP BY` (see `user_docs/sql/window.md`).
- `HAVING` is intentionally not documented here (not currently supported).

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

`FROM <stream_name>`

The stream name must match a stream exposed by the Manager stream catalog.

Example:

```sql
SELECT * FROM source_stream
```

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
- Create pipeline with SQL: `POST /pipelines`
- Validate lowering/execution plan: `GET /pipelines/:id/explain`

