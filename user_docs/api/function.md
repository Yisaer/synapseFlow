# Function Introspection REST API (Manager)

This document describes the **Manager** REST API for introspecting registered
SQL-visible functions (scalar, aggregate, and stateful).

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

## Endpoints

### List Functions

`GET /functions`

Returns metadata for all registered SQL-visible functions (built-in scalars,
aggregates, and stateful functions plus any loaded WASM UDFs).

Response:

- `200 OK` with `FunctionDef[]`.

Example:

```bash
curl -s http://127.0.0.1:8080/functions | jq .
```

### Describe Function

`GET /functions/describe/:name`

Returns metadata for a single function by its canonical name or alias
(case-insensitive).

Response:

- `200 OK` with `FunctionDef`.
- `404 Not Found` if no function matches the given name or alias.

Example:

```bash
curl -s http://127.0.0.1:8080/functions/describe/concat | jq .
curl -s http://127.0.0.1:8080/functions/describe/avg | jq .
```

## Response Shapes

### `FunctionDef`

| Field              | Type                  | Description                                           |
|--------------------|-----------------------|-------------------------------------------------------|
| `kind`             | `string`              | `"scalar"`, `"aggregate"`, or `"stateful"`           |
| `name`             | `string`              | Canonical SQL name (lowercase).                       |
| `aliases`          | `string[]`            | Alternative SQL names (e.g. `"current_timestamp"` for `"now"`). |
| `signature`        | `FunctionSignatureSpec` | Type signature (see below).                         |
| `description`      | `string`              | Human-readable description.                           |
| `allowed_contexts` | `string[]`            | `"select"`, `"where"`, `"group_by"`.                 |
| `requirements`     | `FunctionRequirement[]` | Semantic constraints (see below).                  |
| `constraints`      | `string[]`            | Usage notes and restrictions.                         |
| `examples`         | `string[]`            | Example SQL snippets.                                 |
| `aggregate`        | `AggregateFunctionSpec` | Present when `kind == "aggregate"`.                 |
| `stateful`         | `StatefulFunctionSpec`  | Present when `kind == "stateful"`.                  |

### `FunctionSignatureSpec`

| Field        | Type              | Description                   |
|------------- |-------------------|-------------------------------|
| `args`       | `FunctionArgSpec[]` | Positional argument specs. |
| `return_type`| `TypeSpec`        | Return type specification.    |

### `FunctionArgSpec`

| Field     | Type      | Description                            |
|-----------|-----------|----------------------------------------|
| `name`    | `string`  | Argument name.                         |
| `type`    | `TypeSpec`| Argument type specification.           |
| `optional`| `boolean` | Whether the argument is optional.      |
| `variadic`| `boolean` | Whether the argument accepts varargs.  |

### `TypeSpec`

A discriminated union keyed on `kind`:

| `kind`        | Extra fields                 | Description                   |
|---------------|------------------------------|-------------------------------|
| `any`         | —                            | Any type.                     |
| `named`       | `name: string`               | Concrete type (e.g. `"int64"`). |
| `category`    | `name: string`               | Type category (e.g. `"numeric"`). |
| `list`        | `element: TypeSpec`          | List type.                    |
| `struct`      | `fields: StructFieldSpec[]`  | Struct type.                  |

### `FunctionRequirement`

A discriminated union keyed on `kind`:

| `kind`                 | Meaning                                                    |
|------------------------|------------------------------------------------------------|
| `aggregate_context`    | Must be used inside an aggregation (e.g. `GROUP BY`).     |
| `deterministic_order`  | Requires deterministic row order.                          |
| `requires_partition_by`| Requires `PARTITION BY` clause.                            |
| `requires_eventtime`   | Requires event-time semantics.                             |

### `AggregateFunctionSpec`

| Field                  | Type      | Description                              |
|------------------------|-----------|------------------------------------------|
| `supports_incremental` | `boolean` | Whether the aggregate supports streaming incremental updates. |

### `StatefulFunctionSpec`

| Field              | Type     | Description                                          |
|--------------------|----------|------------------------------------------------------|
| `state_semantics`  | `string` | Description of the internal state and how it evolves. |
