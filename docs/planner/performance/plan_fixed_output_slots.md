# Plan-Fixed Output Layout

## Summary

The planner provides every sink consumer with one `OutputLayout`. Each output
column contains its SQL-visible name, datatype, and a fixed address in the
runtime `Tuple`:

```rust
pub enum OutputValueRef {
    Message {
        message_index: usize,
        value_index: usize,
    },
    Affiliate {
        affiliate_index: usize,
    },
    Null,
}
```

Encoders, row-diff, and encoder transforms resolve final output values through
this layout. They do not scan messages by source name or look up computed values
by name.

## Layout Derivation

The physical planner derives the tuple layout from every operator boundary.
Decoder projection slots define message value indexes. Pass-through operators
preserve their child layout, collection normalization and materialization remap
it, and fan-in operators require compatible layouts.

Per-sink column filtering narrows only the visible output columns. It preserves
each selected column's existing fixed value reference because the upstream tuple
is not rebuilt. After row-diff, encoders, and direct collection materialization
capture the filtered layout, the planner-only filter node is removed.

Project applies these rules:

- A direct column and its aliases inherit the child's fixed value reference.
- A wildcard preserves expanded planner order and inherits each reference.
- A computed expression is stored in the ordered affiliate row and receives the
  next `Affiliate` index.
- A missing output that is valid by plan semantics uses `Null`.

Duplicate output names remain representable because output columns and values
are addressed by index. Individual output formats may reject duplicates when
their data model requires unique keys.

## Runtime Contract

`OutputValueRef::resolve` performs only indexed access. A structurally valid
missing value is represented by `Null`. An out-of-range message, message value,
or affiliate index is a planner/runtime contract violation: debug and test
builds assert, while release builds return a processing error.

There is no runtime name-scan fallback.

`AffiliateRow` maintains insertion order in parallel key and value arrays.
Overwriting a key updates its existing value slot, so every planned affiliate
index remains stable.

## Consumers

- JSON, protobuf, and SDV columnar encoders iterate `OutputLayout.columns` and
  resolve each column's fixed reference.
- Row-diff tracks output indexes from its input layout and materializes a dense
  diff row with `NULL` for unchanged tracked columns plus an output mask for
  changed columns.
- Template transforms receive the dense diff row. The output mask is not
  exposed to the initial template input contract.
- Project preserves input messages and materializes only computed expressions
  into the affiliate row.
- Direct collection sinks materialize a filtered layout at the final sink
  boundary because their connectors do not consume `OutputLayout` directly.

The former `ByIndexProjection`, `late_projection`, and
`passthrough_messages` optimization paths were removed. Fixed value addressing
is a planner contract shared by all consumers rather than an encoder-specific
rewrite.

## Validation

Planner tests cover decoder slot ordering, direct and computed projections,
aliases, row-diff, batching, shared streams, and mixed sink consumers. Pipeline
tests verify full output, delta masks, null transitions, template transforms,
and memory collection materialization.
