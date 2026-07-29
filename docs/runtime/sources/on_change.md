# Source On-Change Input Gating

## Background

Some pipelines should not process every decoded source row. A common pattern is "emit only when the
source value changed" so downstream processors, windows, and sinks do not repeatedly handle
identical source state.

`on_change` implements that behavior at the source boundary. It is configured per pipeline source
binding, not in the global stream definition.

This matters because the same stream may feed different pipelines with different semantics:

- one pipeline may want full source delivery
- another pipeline may want source-side change suppression before any downstream operator runs

## Goals

- Document the pipeline request shape for source-side `on_change`.
- Describe the planner/runtime placement of the source change gate.
- Clarify what counts as a "change".
- Record the current validation rules and v1 limitations.

## Non-Goals

- Sink-side delta or row-diff output semantics.
- Stream-definition-level source configuration.
- General processor internals outside the source change gate behavior.

## Configuration Model

`on_change` is configured in the pipeline `sources[]` section:

```json
{
  "id": "vehicle_pipe",
  "revision": 1,
  "sql": "SELECT speed FROM vehicle_stream",
  "sources": [
    {
      "stream": "vehicle_stream",
      "input": {
        "mode": "on_change",
        "on_change": {
          "columns": ["rpm"]
        }
      }
    }
  ],
  "sinks": [
    {
      "type": "nop",
      "props": {}
    }
  ]
}
```

Supported input modes:

- `full`: forward all decoded source rows
- `on_change`: forward only rows whose tracked source columns changed

`input.on_change.columns` is optional:

- when present, only those top-level source columns are tracked
- when omitted, all top-level columns in the effective source schema are tracked

## Validation Rules

Manager-side pipeline validation currently enforces:

- the configured source must be referenced by the SQL
- the source cannot be configured more than once in the same pipeline request
- `input.on_change` is only valid when `input.mode=on_change`
- `input.mode=on_change` requires the source to have a decoder
- `input.mode=on_change` is rejected when the SQL references the same stream more than once
- `input.on_change.columns` must not be empty
- tracked columns must be top-level columns
- tracked columns must not contain duplicates
- tracked columns must exist in the stream schema

The multiple-reference restriction is a current v1 limitation. The runtime keeps one previous
tracked row per source branch and does not yet maintain partitioned state for multiple references
to the same stream inside one SQL statement.

## Planner Behavior

Logical and physical explain output expose the configured source input mode and tracked columns.

Planner inserts a `PhysicalSourceChangeGate` only when `input.mode=on_change`:

- regular stream source: after `PhysicalDecoder`
- shared stream source: after `PhysicalSharedStream`

This placement means `on_change` compares decoded source columns, not raw connector bytes.

Planner also preserves tracked columns even when the final query projection does not output them.
For example, `SELECT speed FROM vehicle_stream` may still keep `rpm` alive internally when
`input.on_change.columns = ["rpm"]`.

When `on_change` is used on a shared stream, planner extends the shared-stream required-column set
so the change gate can still access the tracked columns after shared-stream pruning.

## Runtime Semantics

The runtime gate compares tracked source columns by their stable physical column indexes resolved
during planning.

Behavior:

- the first decoded row is always forwarded
- later rows are forwarded only when the tracked source columns differ from the previously
  forwarded tracked row
- unchanged rows are suppressed before downstream operators run
- gate state is preserved across input messages and collections

Comparison scope:

- comparison is source-local to the configured source branch
- only tracked columns participate in the equality check
- non-tracked columns may change without causing emission

If no rows in an input collection changed, the gate emits nothing for that collection.

## Interaction With Other Runtime Stages

`on_change` is source-side gating, so it runs before downstream compute, aggregation, windowing,
and sink output logic.

Important interactions:

- it is independent from sink-side `delta` / row-diff output
- it works with hidden tracked columns that are preserved only for gate evaluation
- when `input.on_change.columns` is omitted, all top-level columns in the effective decoded schema
  become tracked columns

The gate only filters decoded collection rows. Control signals, watermarks, and other non-collection
payloads pass through unchanged.

## Explain Expectations

Typical explain output includes:

- logical plan info such as `input.mode=on_change`
- logical plan info such as `input.columns=[rpm]` or `input.columns=[ALL]`
- a physical `PhysicalSourceChangeGate` node with the resolved tracked columns

This makes it possible to verify both the configured request shape and the effective tracked-column
set after planner pruning.

## Testing Guidance

- Verify explicit tracked columns suppress repeated rows when only tracked values remain unchanged.
- Verify omitted `input.on_change.columns` tracks all top-level columns.
- Verify hidden tracked columns remain available even when they are not part of the final query
  projection.
- Verify gate state is preserved across multiple input messages.
- Verify shared-stream explain output still includes the source change gate and the required tracked
  columns.
- Verify `input.mode=on_change` is rejected for decoder-less sources.
- Verify `input.mode=on_change` is rejected when the SQL references the same stream more than once.

## Future Work

- Support key-partitioned or reference-partitioned source change state instead of one previous row
  per source branch.
- Consider documenting source input configuration alongside broader pipeline request reference
  material once such API documentation is added.
