# Table REST API (Manager)

This document describes the **Manager** REST API for managing tables.

Tables are SQL relations used for finite scans. They are different from streams: a stream continuously
subscribes to new data, while a table scan reads the available table data once and then ends the
pipeline's data path.

> **Resource IDs.** The table `name` and a referenced `schema.ref` must match
> `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``: start with an ASCII letter, then ASCII
> letters, digits, or underscores, up to 128 bytes. The table name doubles as a
> SQL source identifier, so hyphens and dots are not allowed. IDs are
> case-sensitive and never trimmed; invalid IDs return `400 Bad Request`.

## Endpoints

### Create Table

`POST /tables`

Creates a table definition, persists it, and registers it in the runtime catalog.

Request body: `CreateTableRequest`

```json
{
  "name": "history_table",
  "revision": 1721797200000,
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

Notes:

- `schema.type` selects the schema declaration format. Built-in: `json`, `proto`.
- `schema.props` is schema-format specific (see stream schema documentation for the full type mapping).
- Alternatively, use `schema.ref` to reference a pre-defined named schema (see `user_docs/api/schema.md`). When `schema.ref` is set, `schema.type` and `schema.props` are ignored.
- `decoder.type` must be registered in the runtime decoder registry.

Response:

- `201 Created` with `{ "name": "...", "revision": 1721797200000 }`.
- `409 Conflict` if the table already exists or another storage operation is active.

Example:

```bash
curl -s -XPOST http://127.0.0.1:8080/tables \
  -H "Content-Type: application/json" \
  -d @table.json | jq .
```

### List Tables

`GET /tables`

Returns a list of persisted tables with schema summaries.

Response:

- `200 OK` with `TableInfo[]`.

Example:

```bash
curl -s http://127.0.0.1:8080/tables | jq .
```

### Delete Table

`DELETE /tables/:name`

Deletes a table from runtime and storage.

Response:

- `200 OK` with a plain text message.
- `404 Not Found` if the table does not exist.
- `409 Conflict` if another storage operation is active.

Example:

```bash
curl -s -XDELETE http://127.0.0.1:8080/tables/history_table
```

## Request Shapes

### `CreateTableRequest`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | yes | - | SQL-visible table name. |
| `revision` | number | yes | - | Positive JSON-safe resource revision. |
| `type` | string | yes | - | Table provider type. Supported: `history`. |
| `schema` | object | yes | - | Decoded row schema. Shape matches stream schema declarations. |
| `props` | object | no | `{}` | Table-provider configuration. |
| `decoder` | object | no | `{"type":"json","props":{}}` | Payload decoder. Shape matches stream decoder declarations. |

Unlike streams, tables do not support `shared`, `sampler`, or `eventtime`.

### Schema Configuration (`schema`)

The `schema` field uses the same shape as stream schemas:

- `schema.type: string` — schema declaration format (e.g. `json`, `proto`).
- `schema.props: object` — format-specific schema definition.
- `schema.ref: string` (optional) — when set, references a named schema installed via `POST /schemas`. All other schema fields are ignored.

See `user_docs/api/schema.md` for named schema management and `user_docs/api/stream.md` for the full column type mapping.

### Table `props` by `type`

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

## Response Shapes

### `TableInfo`

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Table name. |
| `revision` | number | Persisted resource revision. |
| `type` | string | Table provider type (e.g. `history`). |
| `schema` | object | `{ columns: Column[] }` — decoded row schema summary. |

## SQL Usage

Once a table is created, it can be used in a pipeline SQL `FROM` clause:

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

## Export / Import

Table definitions are included in manager storage export (`GET /storage/export`) and import
(`POST /import`). The export manifest contains a `tables` array in its `resources` block, and
the import handler validates table name uniqueness, schema references, and provider-specific
props before committing the snapshot.

Tables also participate in startup init-directory apply (`--init-dir`) and are restored from
persistent storage on restart.

Import performs full-snapshot replacement and does not compare revisions.
Startup init applies a table only when its revision is greater than the stored
table revision; equal and lower revisions are ignored.
