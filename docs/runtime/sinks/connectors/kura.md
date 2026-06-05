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
- Each row is scanned for non-null columns that have an entry in the mapping file.
- Each mapped column is converted to a `DataPointCurrent` value with the corresponding VSS path.
- All data points from the collection are batched into a single `SetCurrentRequest`.
- Value type conversion follows a direct mapping from `datatypes::Value` to the protobuf
  `ValueType` oneof.

## Configuration

Kura sink properties:

- `addr` (string, required): Kura gRPC endpoint, e.g. `http://127.0.0.1:50053` or `127.0.0.1:50053`.
- `mapping_path` (string, required): file path to the JSON mapping file.

Manager defaults:

- When a sink is of type `kura`, the encoder is implicitly set to `none` (users do not need to
  specify an encoder in the pipeline definition).

## Mapping File

`mapping_path` points to a flat JSON object where:

- Keys are input column names from the pipeline.
- Values are VSS paths on the kura server.

Example:

```json
{
  "speed": "Vehicle.Speed",
  "name": "Vehicle.VehicleIdentification.VIN"
}
```

At runtime, incoming column names are matched **exactly** against the keys.

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
- Physical plan directly connects the decoder/project chain to the sink processor.

When batching is enabled (`batch_count` or `batch_duration`), a `PhysicalBatch`
node is inserted before the `PhysicalDataSink`:

```
PhysicalBatch(batch_count=10) → PhysicalDataSink(connector=kura)
```

The `PhysicalBatch` node creates a `BatchProcessor` that accumulates rows into
`RecordBatch` payloads before delivering them to the sink.

This avoids any passthrough/encoding step and ensures the sink receives
`Collection` payloads.

## Limitations

- Authentication tokens are not part of the current sink configuration.
- Struct, list, bytes, and timestamp value types are not supported and will return an error.
- The sink does not query kura for target metadata — value types are inferred from the veloFlux
  column type alone.
- Unmapped columns and `null` values are skipped.
- No retry or reconnection logic beyond what tonic provides.

## Example (REST)

Create a pipeline that publishes `SELECT speed, name` into kura:

```json
POST /pipelines
{
  "id": "pipeline_kura",
  "sql": "SELECT speed, name FROM vehicle_stream",
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
