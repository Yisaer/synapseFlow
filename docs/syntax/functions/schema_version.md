# Schema Version Function

## Syntax

```sql
schema_version()
```

`schema_version()` returns the revision of the named schema referenced by the query's input
stream. It takes no arguments and requires exactly one stream source.

```sql
SELECT schema_version() AS version
FROM vehicle_stream
```

## Planning Semantics

The planner resolves the stream's named schema revision and replaces each `schema_version()` call
with an `INT64` literal before logical planning and optimization. The value is therefore fixed for
the lifetime of the compiled pipeline and does not require per-row runtime lookup or evaluation.

The function may be nested in any expression processed by the planner, including `SELECT`,
`WHERE`, `HAVING`, `GROUP BY`, `ORDER BY`, aggregate or stateful arguments, and window
expressions. Other rules for those expression contexts still apply.

## Constraints

- The input stream must reference a named schema through `schema.ref`.
- Inline schemas are rejected because they have no independent schema revision.
- Queries with zero or multiple stream sources are rejected.
- Table sources are not supported.
- `FILTER`, `OVER`, function-local `ORDER BY`, and other function modifiers are not supported.

The bound value changes only when the pipeline is planned again against a stream that resolves to
a different named schema revision.
