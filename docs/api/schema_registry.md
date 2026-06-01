# Schema Registry

This document describes the schema registry system — the mechanism for
defining, storing, and reusing named schemas in veloflux.

For the REST API contract, see `user_docs/api/schema.md`.

## Overview

The schema registry decouples schema definition from stream creation:

- **Schema Registry** — a pluggable parser registry that produces `Schema`
  (a `Vec<ColumnSchema>`) from a `type` + `props` pair.
- **Named Schema Store** — an in-memory cache of resolved named schemas,
  keyed by name. Schemas are parsed once at creation time (or on startup
  restore) and cached for O(1) lookup.
- **Stream reference** — when creating a stream, the `schema` field can
  reference a named schema by name instead of inline-defining columns.

```
┌─────────────────────────────────────────────────────────────┐
│                    POST /schemas                             │
│  { name: "sensor", type: "proto", props: {...} }            │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  SchemaRegistry                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │  "json"  │  │ "proto"  │  │  custom  │  ...             │
│  │  parser  │  │  parser  │  │  parser  │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
│       │              │             │                         │
│       ▼              ▼             ▼                         │
│  Schema { columns: [...] }                                   │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  NamedSchemaStore (in-memory)                                │
│  { "sensor" → Schema, "user_profile" → Schema, ... }        │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ O(1) Arc::clone
                   ▼
┌─────────────────────────────────────────────────────────────┐
│  POST /streams                                               │
│  { schema: { ref: "sensor" }, ... }                         │
└─────────────────────────────────────────────────────────────┘
```

## Schema Types

Each schema registered in the system has a `type` field that selects
which parser to use. The following types are built-in:

| Type     | Description                                        |
|----------|----------------------------------------------------|
| `json`   | Inline column definitions (existing, unchanged).   |
| `proto`  | Derive schema from a `.proto` file and a message type name. |

Custom types can be added via the `SchemaRegistry::register_schema()` API
(or through a distribution-specific extension point).

## Proto Schema Parser

The `proto` type parses a protobuf message definition from a `.proto` file
and maps its fields to veloflux column types.

### Props

| Prop             | Required | Description                                                      |
|------------------|----------|------------------------------------------------------------------|
| `proto_path`     | yes      | Path to the `.proto` file (relative or absolute).                |
| `message_type`   | yes      | Fully qualified message name, e.g. `"Sensor"` or `"com.example.Sensor"`. |
| `include_paths`  | no       | Additional proto include directories (analogous to `protoc -I`). The target file's parent directory is always included. |

### Type Mapping

Proto field types are mapped to veloflux `ConcreteDatatype` as follows:

| Proto type                          | veloflux type  |
|-------------------------------------|----------------|
| `double`                            | `float64`      |
| `float`                             | `float32`      |
| `int32`, `sint32`, `sfixed32`       | `int32`        |
| `int64`, `sint64`, `sfixed64`       | `int64`        |
| `uint32`, `fixed32`                 | `uint32`       |
| `uint64`, `fixed64`                 | `uint64`       |
| `bool`                              | `bool`         |
| `string`                            | `string`       |
| `bytes`                             | `bytes`        |
| Enum                                | `int32`        |
| Message (nested)                    | `struct`       |
| `repeated T`                        | `list<T>`      |
| `map<K, V>`                         | `list<struct<key: K, value: V>>` |
| `google.protobuf.Timestamp`         | `timestamp`    |

**Notes:**

- Proto has no 8-bit or 16-bit integer types, so `int8`, `int16`, `uint8`,
  `uint16`, and `null` cannot currently be expressed via proto schemas.
- `map<K, V>` is expanded to `list<struct>` because proto3 maps are
  syntactic sugar for a repeated entry message.
- `oneof` fields are flattened as individual columns; mutual-exclusion
  semantics are not preserved in the veloflux schema.
- The parser uses `protox`, a pure-Rust protobuf compiler, so no system
  `protoc` binary is required at runtime.

### Nested Messages and Recursion

Nested `message` types are recursively expanded into `struct` columns.
A maximum nesting depth of 10 levels is enforced to prevent infinite
recursion from circular references.

### Well-Known Types

| Proto type                          | veloflux type  |
|-------------------------------------|----------------|
| `google.protobuf.Timestamp`         | `timestamp`    |

Other well-known types (`Duration`, `*Value` wrappers, etc.) are treated
as regular nested messages and expanded to `struct`. This is deliberate —
the veloflux type system has no direct analogues for them.

### Example

Given `schemas/sensor.proto`:

```protobuf
syntax = "proto3";
package com.example;

message Sensor {
  string sensor_id = 1;
  double reading = 2;
  int32 quality = 3;
  bool online = 4;
}
```

Create a named schema:

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
  }'
```

The result is equivalent to the following inline JSON schema:

```json
{
  "columns": [
    { "name": "sensor_id", "data_type": "string" },
    { "name": "reading",   "data_type": "float64" },
    { "name": "quality",   "data_type": "int32" },
    { "name": "online",    "data_type": "bool" }
  ]
}
```

### Error Handling

The proto parser returns clear error messages for common failure modes:

| Scenario                          | Error message                                           |
|-----------------------------------|---------------------------------------------------------|
| Missing `proto_path`              | `"missing or empty 'proto_path' in proto schema props"` |
| Missing `message_type`            | `"missing or empty 'message_type' in proto schema props"` |
| File not found                    | Propagated from `protox` (e.g. `"No such file"`)       |
| Proto syntax error                | Propagated from `protox` (e.g. `"unexpected token"`)   |
| Message type not found            | `"message type 'X' not found in 'path.proto'"`         |
| Referenced type not resolvable    | `"referenced message type 'X' not found"`              |
| Nesting depth exceeded            | `"maximum nesting depth (10) exceeded at message 'X'"`  |
| Field without type                | `"proto field without type"`                            |
| Message field without type_name   | `"message field without type_name"`                     |
| Unsupported field type            | `"unsupported proto field type ... for field 'X'"`     |

## Stream Reference

When creating a stream, the `schema.ref` field allows referencing a
pre-defined named schema instead of inline-defining columns:

```json
{
  "name": "my_stream",
  "type": "mqtt",
  "schema": {
    "ref": "sensor_schema"
  },
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "/sensors/data",
    "qos": 0
  },
  "decoder": {
    "type": "json",
    "props": {}
  }
}
```

**Important:** Stream creation resolves the schema at creation time via a
snapshot (`Arc::clone`). Subsequent modifications to the named schema do
not affect existing streams. To update a stream's schema, delete and
recreate the stream.

## Lifecycle and Persistence

```
Create            Delete           Startup
──────            ──────           ───────
POST /schemas     DELETE /schemas  Process restart
  │                  │               │
  ▼                  ▼               ▼
parse → Schema     check refs      storage.list_schemas()
  │                  │               │
  ▼                  │               ▼
NamedSchemaStore   ┌─no refs──┐    for each stored:
.insert()          │          │      parse → Schema
  │                ▼          │      NamedSchemaStore.insert()
  ▼             remove +     │
storage          delete       │
.create_schema()              │
                   ┌──────────┘
                   ▼
              409 Conflict
              (referenced by
              streams: [...])
```

- **Persistence:** Schemas are persisted in the metadata store (redb),
  alongside streams, pipelines, and other resources.
- **Startup restore:** On process restart, all stored schemas are
  re-parsed and re-inserted into the in-memory `NamedSchemaStore`.
  This happens before streams are restored, so stream references resolve
  correctly.
- **Export/Import:** Schemas are included in the export bundle
  (`/storage/export`) and validated during import (`/import`).

## Extensibility

### Adding a Custom Schema Type

New schema types can be registered programmatically:

```rust
use manager::stream::register_schema;
use std::sync::Arc;

fn parse_my_schema(
    stream_name: &str,
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<flow::Schema, String> {
    // Custom parsing logic here
    todo!()
}

register_schema("my_type", Arc::new(parse_my_schema));
```

Registration must happen before any schema of that type is created
(typically during distribution-specific initialization).
