# Kura Sink

This document describes the background and implementation design of the **Kura sink** in veloFlux.

## Coverage Scope

The Kura sink depends on an external [kura](https://github.com/yoriito/kura) server instance for
end-to-end output validation. Its connector-level output behavior is intentionally excluded from the
feature coverage registry; planner and configuration behavior may still be covered by focused tests.

## Background

Some deployments need to write streaming VSS signal values to a kura server. veloFlux provides a
Kura sink that consumes decoded rows (`Collection` / `Tuple`) and publishes updates using the
[yoriito VISS v1 producer](https://github.com/yoriito/yoriito-viss) gRPC `SetCurrent` RPC.

Key requirements:

- Sink consumes decoded values directly (no byte encoding step).
- Column names are mapped to VSS paths via a flat JSON mapping file (`mapping_path`).
- One `Collection` batch results in a single gRPC `SetCurrent` request.
- `null` values are skipped.
- Unsupported value types (`Struct`, `List`, `Bytes`, `Timestamp`) are rejected with an explicit
  error.

## Data Semantics

veloFlux executes sinks over `Collection`s. For the Kura sink:

- A `Collection` is treated as a batch of rows.
- Each row uses the planner-materialized final output names, including SQL aliases and computed
  column aliases.
- Each row is scanned for non-null columns that have an entry in the mapping file.
- Each mapped column is converted to a `DataPointCurrent` value with the corresponding VSS path.
- All data points from the collection are batched into a single `SetCurrentRequest`.
- Value type conversion follows a direct mapping from `datatypes::Value` to the protobuf
  `ValueType` oneof, unless a mapping entry supplies an explicit `data_type` override.

## Configuration

Kura sink properties:

- `addr` (string, required): Kura gRPC endpoint, e.g. `http://127.0.0.1:50053` or `127.0.0.1:50053`.
- `mapping_path` (string, required): file path to the JSON mapping file.

Manager defaults:

- When a sink is of type `kura`, the encoder is implicitly set to `none` (users do not need to
  specify an encoder in the pipeline definition).

## Mapping File

`mapping_path` points to a JSON object where:

- Keys are final pipeline output column names, including SQL aliases and computed column aliases.
- A string value is a VSS path and preserves the native veloFlux value type.
- An object value contains a VSS `path` and a `data_type` override. The override selects the
  protobuf `ValueType` oneof that Kura expects for that VSS node.

Supported `data_type` values use VSS catalog names. Aliases are accepted for the names introduced
by the original float mapping:

| `data_type` | Alias | Protobuf variant |
|-------------|-------|------------------|
| `string` | | `string` |
| `boolean` | `bool` | `bool` |
| `int8`, `int16`, `int32`, `int64` | | `int8` … `int64` |
| `uint8`, `uint16`, `uint32`, `uint64` | | `uint8` … `uint64` |
| `float` | `float32` | `float` |
| `double` | `float64` | `double` |

Numeric overrides accept integer and floating source values. Integer targets require a whole number
in range. `boolean` also accepts integer or whole-float `0` and `1`, which is the usual DBC encoding
for switch signals. `string` only accepts string values. Out-of-range or incompatible values return
an error.

Example:

```json
{
  "speed": {
    "path": "Vehicle.Speed",
    "data_type": "float"
  },
  "door_count": {
    "path": "Vehicle.Cabin.DoorCount",
    "data_type": "uint8"
  },
  "is_open": {
    "path": "Vehicle.Cabin.Door.Row1.DriverSide.IsOpen",
    "data_type": "boolean"
  },
  "name": "Vehicle.VehicleIdentification.VIN"
}
```

At runtime, final pipeline output names are matched **exactly** against the keys. A mapping that
does not match any output column returns an error instead of reporting a successful no-op. A mapped
column whose current value is `null` is still skipped normally.

DBC integer scale/offset signals are `Int64` or `Uint64`. Fractional scale/offset signals are
`Float64`. VSS nodes are typically narrower (`uint8`, `int32`, `float`, `boolean`), so mapping
entries for those nodes should set `data_type` to the VSS type. Native conversion without
`data_type` still sends the veloFlux column type unchanged.

## Execution Model (gRPC SetCurrent)

The sink uses the yoriito VISS v1 producer `SetCurrent` RPC:

1. Connect to the kura producer endpoint via tonic gRPC.
2. Convert each non-null, mapped column to a `DataPointCurrent`.
3. Batch all data points into one `SetCurrentRequest`.
4. Send the request and check the response for errors.

No persistent stream or signal registration is needed — every `send_collection` call is a single
unary RPC.

## Planner / Physical Plan Behavior

For Kura sinks, the physical plan contains no encoder node:

- Logical plan shows `encoder=none` for the sink.
- A `PhysicalMemoryCollectionMaterialize` node resolves the final `OutputLayout` before the sink so
  SQL aliases, computed columns, and fixed output slots become one dense collection.
- The materialized collection is connected directly to the sink processor.

When batching is enabled (`batch_count` or `batch_duration`), a `PhysicalBatch`
node is inserted before the `PhysicalDataSink`:

```
PhysicalBatch(batch_count=10) → PhysicalMemoryCollectionMaterialize → PhysicalDataSink(connector=kura)
```

The `PhysicalBatch` node creates a `BatchProcessor` that accumulates rows into
`RecordBatch` payloads before delivering them to the sink.

This avoids any passthrough/encoding step and ensures the sink receives
`Collection` payloads.

## Limitations

- Authentication tokens are not part of the current sink configuration.
- Struct, list, bytes, and timestamp value types are not supported and will return an error.
- The sink does not query kura for target metadata. Value types come from the veloFlux column type
  or an explicit mapping `data_type` override. Array and struct VSS types are not supported.
- Individually unmapped columns and `null` values are skipped. A mapping with no final output-column
  match is rejected when rows arrive.
- No retry or reconnection logic beyond what tonic provides.

## Example (REST)

Create a pipeline that publishes an aliased speed into kura:

```json
POST /pipelines
{
  "id": "pipeline_kura",
  "revision": 1,
  "sql": "SELECT can_speed AS speed FROM vehicle_stream",
  "sinks": [
    {
      "type": "kura",
      "props": {
        "addr": "http://127.0.0.1:50053",
        "mapping_path": "/etc/veloflux/kura_mapping.json"
      }
    }
  ]
}
```
