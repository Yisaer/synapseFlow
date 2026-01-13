# Kuksa Sink

This document describes the background and implementation design of the **Kuksa sink** in veloFlux.

## Background

Some deployments need to forward streaming signal values into an Eclipse Kuksa DataBroker instance and update Vehicle Signal Specification (VSS) paths. veloFlux provides a Kuksa sink that consumes decoded rows (`Collection` / `Tuple`) and publishes updates to the broker using the `kuksa.val.v2` API via the `kuksa-rust-sdk`.

Key requirements:

- Sink consumes decoded values directly (no byte encoding step).
- Column names are mapped to VSS paths via a JSON mapping file (`vssPath`).
- One input row corresponds to one update request containing multiple key/value updates.
- `null` values are skipped.

## Data Semantics

veloFlux executes sinks over `Collection`s. For the Kuksa sink:

- A `Collection` is treated as a batch of rows.
- Each **row** is converted into one update operation.
- Each column in the row is a candidate update entry:
  - If the value is `null`, it is skipped.
  - If the column name has no mapping entry in `vssPath`, it is skipped.
  - If multiple columns map to the same VSS path within a row, the later column overwrites the earlier one.
- If a value cannot be converted to the VSS path’s declared data type, the sink returns an error.

## Configuration

Kuksa sink properties:

- `addr` (string, required): Kuksa DataBroker address (for example `localhost:55575`).
- `vssPath` (string, required): file path to the VSS mapping JSON.

Manager defaults:

- When a sink is of type `kuksa`, the encoder is implicitly set to `none` (users do not need to specify an encoder in the pipeline definition).

## VSS Mapping File

`vssPath` points to a JSON file that defines a mapping from input column keys to VSS paths.

veloFlux parses the JSON tree and records entries whenever it encounters:

- `sig2vss.qualifiedName` (string): treated as the input column key.
- The current JSON traversal path (dot-joined) is treated as the VSS path.

At runtime, incoming column names are matched **exactly** against `qualifiedName`.

## Execution Model (kuksa.val.v2)

The sink uses `kuksa.val.v2` provider stream mode:

1. Resolve metadata for VSS paths (`ListMetadata`) to obtain `id` and `data_type`.
2. Ensure signal IDs are provided (`ProvideSignalRequest`), cached per connector instance to avoid repeated provides.
3. Publish values by ID (`PublishValuesRequest`) with one request per input row.

## Planner / Physical Plan Behavior

For Kuksa sinks, the physical plan contains no encoder node:

- Logical plan shows `encoder=none` for the sink.
- Physical plan directly connects the decoder/project chain to the sink processor.

This avoids any passthrough/encoding step and ensures the sink receives `Collection` payloads.

## Limitations

- Authentication tokens are not part of the current sink configuration.
- Complex/nested values that cannot be converted to Kuksa v2 typed values will return an error.
- Unmapped columns and `null` values are skipped.

## Example (REST)

Create a pipeline that publishes `SELECT *` into Kuksa:

```json
POST /pipelines
{
  "id": "pipeline_kuksa",
  "sql": "SELECT * FROM stream",
  "sinks": [
    {
      "id": "kuksa_sink",
      "type": "kuksa",
      "props": {
        "addr": "localhost:55575",
        "vssPath": "/path/to/vss.json"
      }
    }
  ]
}
```
