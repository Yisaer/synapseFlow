# Streams and Schema

This document describes stream metadata and schema introspection from a **SQL and schema** perspective.

For the schema registry system (named schemas, proto parser), see `docs/api/schema_registry.md`.
For the schema CRUD REST API, see `user_docs/api/schema.md`.

For agent implementation guidance (workflow, validation loop, do/don't), see `docs/integrations/agents/runtime_playbook.md`.

For stream management (create/delete) REST APIs, see `user_docs/api/stream.md`.

> **Resource IDs.** Stream names and named-schema names must match
> `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``. Because a stream name is also used as a
> SQL source identifier, it is restricted to ASCII letters, digits, and
> underscores (no hyphens or dots) and is case-sensitive. Column and nested field
> names inside a schema are not subject to this rule.

## Manager API

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

### List Streams

`GET /streams`

Returns a list of known streams with their schemas (a lightweight summary).

Example:

```bash
curl -s http://127.0.0.1:8080/streams | jq .
```

### Describe Stream

`GET /streams/describe/:name`

Returns a single stream's schema and definition spec.

Example:

```bash
curl -s http://127.0.0.1:8080/streams/describe/user | jq .
```

If the stream does not exist, the API should return `404` with a descriptive message.

## Response Shapes

Clients should treat field names as stable API contract. Optional fields may be absent.

This manager API field-shape contract is covered by manager API tests rather
than the feature coverage registry. Feature coverage focuses on documented
parser, planner, flow, and runtime behavior; it does not track this endpoint as
a standalone stream feature.

### `GET /streams` → `StreamInfo[]`

- `name: string` (identifier used in SQL)
- `revision: number` (persisted resource revision)
- `shared: boolean`
- `schema: { columns: Column[] }`
- Optional `shared_stream: SharedStreamItem`

Note: current implementation does not populate `shared_stream` in this endpoint.

### `GET /streams/describe/:name` → `DescribeStreamResponse`

- `stream: string` (identifier used in SQL)
- `revision: number` (persisted resource revision)
- `spec: StreamDefinitionSpec`

## Revision-Based Update

`revision` is required when creating a stream and in
`PUT /streams/:name`. It must be an integer from `1` through
`9007199254740991`.

For an existing stream:

- a greater revision executes the normal validation, replacement, and rollback
  flow;
- a lower revision returns `409 Conflict` with `older_revision`;
- an equal revision with the same normalized definition returns idempotent
  success without runtime mutation;
- an equal revision with a different definition returns `409 Conflict` with
  `same_revision_different_spec`.

JSON formatting and object field order do not affect equality.

### `StreamDefinitionSpec`

- `type: string` (stream type label, e.g. `mqtt`, `memory`, `nng_pubsub`)
- `shared: boolean`
- `schema: { columns: Column[] }`
- `decoder: { type: string, props: object }`
- `props: object` (connector-specific stream properties)
- Optional: `eventtime: { column: string, type: string }`

### `SharedStreamItem`

- `id: string`
- `status: string` (`starting`, `running`, `stopped`, `failed`)
- Optional `status_message: string` (present when `status == "failed"`)
- `connector_id: string`
- `subscribers: number`
- `created_at_secs: number` (Unix seconds)

### `Column`

- `name: string`
- `data_type: string`
- Optional:
  - `fields: Column[]` (only when `data_type == "struct"`)
  - `element: Column` (only when `data_type == "list"`)

## Type Strings

The schema uses a compact set of type strings. Clients must not assume other names.

Common scalars:

- `null`, `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`

Nested types:

- `struct` (with `fields`)
- `list` (with `element`)

## Nested Types

Streams may contain nested types (struct/list). The introspection schema represents nested types structurally:

- Struct columns provide `fields[]`.
- List columns provide an `element` column (its `name` may be `"element"` in responses).

Important: nested types in schema do not automatically imply that SQL supports nested field access syntax. Use SQL validation and explain outputs to confirm supported expressions.

## Column Order and Stability

Clients should preserve the column order as returned by the schema when:

- Displaying schemas to users
- Reasoning about index-based semantics in execution/explain output (if applicable)

Do not reorder columns arbitrarily.
