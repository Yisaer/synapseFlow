# Table API

This document describes the user-facing table configuration model.

Tables are SQL relations used for finite scans. They are different from streams: a stream continuously
subscribes to new data, while a table scan reads the available table data once and then ends the
pipeline's data path.

## Availability

Current implementation status:

- The flow runtime supports registering table definitions in the runtime catalog.
- The first supported table type is `history`.
- Manager REST endpoints such as `POST /tables`, `GET /tables`, and `DELETE /tables/:name` are not
  exposed yet.
- Table definitions are not included in manager storage export/import yet.

Until manager endpoints are added, applications embedding the flow runtime must register tables
through the flow instance API before creating pipelines that reference those tables.

## Table Definition Shape

Table definitions intentionally resemble stream definitions:

```json
{
  "name": "history_table",
  "type": "history",
  "schema": {
    "type": "json",
    "props": {
      "columns": [
        { "name": "ts", "data_type": "int64" },
        { "name": "vehicle_id", "data_type": "string" },
        { "name": "speed", "data_type": "int64" }
      ]
    }
  },
  "props": {
    "datasource": "/var/lib/nanomq/history",
    "topic": "vehicle",
    "time_column": "ts",
    "batch_size": 100
  },
  "decoder": {
    "type": "json",
    "props": {}
  }
}
```

Fields:

- `name: string` - SQL-visible table name.
- `type: string` - table provider type. Supported: `history`.
- `schema: object` - decoded row schema. The shape matches stream schema declarations.
- `props: object` - table-provider configuration.
- `decoder: object` - payload decoder. The shape matches stream decoder declarations.

Unlike streams, tables do not currently support `shared`, `sampler`, or `eventtime`.

## History Table Props

`type == "history"`:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `datasource` | string | yes | - | Directory containing NanoMQ history Parquet files. |
| `topic` | string | yes | - | Topic used to discover matching history files. |
| `time_column` | string | no | `ts` | Parquet time column used when extracting rows. |
| `batch_size` | number | no | `100` | Number of Parquet rows read per batch. Must be greater than `0` when set. |

History files must follow this naming convention:

```text
nanomq_{topic}-{start_ts}~{end_ts}_{seq}_{hash}.parquet
```

The table scan reads the Parquet `data` column as bytes and decodes those bytes with the table
decoder. The table schema must describe the decoded payload, not the raw Parquet wrapper.

## SQL Usage

Once a table is registered in the runtime catalog, it can be used in a pipeline SQL `FROM` clause:

```sql
SELECT * FROM history_table
```

Scalar projection and filtering are supported:

```sql
SELECT speed + 1 AS next_speed
FROM history_table
WHERE ts > 1
```

Current table-scan limitations:

- table aliases are not supported
- stream-table joins are not supported
- aggregate functions over table scans are not supported
- a table scan reads all matching history input currently visible to the datasource and then ends
