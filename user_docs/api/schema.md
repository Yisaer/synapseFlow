# Schema REST API (Manager)

This document describes the **Manager** REST API for managing named schemas.

For the design of the schema registry and proto parser, see `docs/api/schema_registry.md`.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

## Endpoints

### Create Schema

`POST /schemas`

Creates a named schema definition, parses and validates it, and stores
it in the in-memory schema store and persistent storage.

Request body:

```json
{
  "name": "sensor_schema",
  "type": "proto",
  "props": {
    "proto_path": "schemas/sensor.proto",
    "message_type": "com.example.Sensor"
  }
}
```

Fields:

- `name: string` (required, non-empty) — Unique identifier for this schema.
- `type: string` (required) — Schema parser type. Built-in: `json`, `proto`.
- `props: object` (optional, defaults to `{}`) — Parser-specific properties.
  See the corresponding parser documentation for details.

Notes:

- The schema is validated at creation time. If the parser returns an error
  (e.g., proto file not found, syntax error, message type not found), the
  request fails with `400 Bad Request`.
- Schema names must be unique. Creating a schema whose name already exists
  returns `409 Conflict`.

Response:

- `201 Created` with `{ "name": "..." }`.
- `409 Conflict` if the schema name already exists.

Example:

```bash
curl -s -XPOST http://127.0.0.1:8080/schemas \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sensor_schema",
    "type": "proto",
    "props": {
      "proto_path": "schemas/sensor.proto",
      "message_type": "com.example.Sensor"
    }
  }' | jq .
```

### List Schemas

`GET /schemas`

Returns a list of all named schemas with their parsed column definitions.

Response:

- `200 OK` with `SchemaInfo[]`.

Example:

```bash
curl -s http://127.0.0.1:8080/schemas | jq .
```

### Get Schema

`GET /schemas/:name`

Returns a single named schema's definition and parsed columns.

Response:

- `200 OK` with `SchemaInfo`.
- `404 Not Found` if the schema does not exist.

Example:

```bash
curl -s http://127.0.0.1:8080/schemas/sensor_schema | jq .
```

### Delete Schema

`DELETE /schemas/:name`

Deletes a named schema from the in-memory store and persistent storage.

A schema cannot be deleted if any existing stream references it (via
`schema.ref`). The list of referencing streams is returned in the error
message.

Response:

- `200 OK` with a plain text message.
- `404 Not Found` if the schema does not exist.
- `409 Conflict` if any stream still references the schema.

Example:

```bash
curl -s -XDELETE http://127.0.0.1:8080/schemas/sensor_schema
```

## Request Shapes

### `CreateSchemaRequest`

| Field   | Type     | Required | Description                                 |
|---------|----------|----------|---------------------------------------------|
| `name`  | `string` | yes      | Unique schema identifier.                   |
| `type`  | `string` | yes      | Parser type: `"json"`, `"proto"`, or custom. |
| `props` | `object` | no       | Parser-specific properties. Default: `{}`.  |

### Schema `props` by `type`

#### `type == "json"`

Inline column definitions. `props.columns` is required.

```json
{
  "type": "json",
  "props": {
    "columns": [
      { "name": "user_id", "data_type": "int64" },
      { "name": "score", "data_type": "float64" }
    ]
  }
}
```

See `user_docs/api/stream.md` for the full `Column` type reference.

#### `type == "proto"`

Derive schema from a `.proto` file.

| Prop            | Required | Description                                           |
|-----------------|----------|-------------------------------------------------------|
| `proto_path`    | yes      | Path to the `.proto` file.                            |
| `message_type`  | yes      | Fully qualified message name, e.g. `"Sensor"` or `"com.example.Sensor"`. |
| `include_paths` | no       | Array of additional proto include directories.        |

```json
{
  "type": "proto",
  "props": {
    "proto_path": "schemas/sensor.proto",
    "message_type": "com.example.Sensor"
  }
}
```

See `docs/api/schema_registry.md` for the full proto type mapping
reference.

## Response Shapes

### `SchemaInfo`

| Field     | Type     | Description                                              |
|-----------|----------|----------------------------------------------------------|
| `name`    | `string` | Schema identifier.                                       |
| `type`    | `string` | Parser type.                                             |
| `props`   | `object` | Parser-specific properties as submitted at creation.     |
| `columns` | `Column[]` | Parsed column definitions (see below).                |

### `Column`

| Field      | Type     | Description                                      |
|------------|----------|--------------------------------------------------|
| `name`     | `string` | Column name.                                     |
| `data_type`| `string` | Type string (see supported types below).         |
| `fields`   | `Column[]` | Present when `data_type == "struct"`.          |
| `element`  | `Column` | Present when `data_type == "list"`.              |

Supported type strings:

- `null`, `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`, `bytes`, `timestamp`
- `struct` (with `fields`), `list` (with `element`)

## Using Schemas in Streams

After creating a schema, reference it when creating a stream via the
`schema.ref` field:

```bash
curl -s -XPOST http://127.0.0.1:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sensor_stream",
    "type": "mqtt",
    "schema": { "ref": "sensor_schema" },
    "props": {
      "broker_url": "tcp://127.0.0.1:1883",
      "topic": "/sensors/data",
      "qos": 0
    },
    "decoder": { "type": "json", "props": {} }
  }'
```

When `schema.ref` is present:
- `schema.type` and `schema.props` are ignored.
- The referenced schema must exist.
- The schema is resolved at stream creation time (snapshot semantics).

Inline schema definition (`schema.type` + `schema.props`) continues to
work as before for backwards compatibility.

## Import / Export

Schemas are included in the export bundle produced by `/storage/export`
and validated during `/import`. For details, see `user_docs/api/export.md`
and `user_docs/api/import.md`.

### Export Resource Shape

In the `ExportResources` object:

```json
{
  "schemas": [
    {
      "name": "sensor_schema",
      "type": "proto",
      "props": {
        "proto_path": "schemas/sensor.proto",
        "message_type": "com.example.Sensor"
      }
    }
  ]
}
```

### Import Validation

During import, schemas are validated for:
- Non-empty name (duplicates are rejected).
- Well-formed props (valid JSON).
- Names must be unique within the bundle.
