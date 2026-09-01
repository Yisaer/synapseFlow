# JSON Encoder Options

This document explains the current option model for the built-in JSON sink encoder.

It covers two layers:

- user-facing encoder configuration
- runtime-only encoder options used internally while encoding rows

The user-facing configuration determines which runtime options are active for a given sink branch.

## Scope

This document is about the built-in JSON encoder only:

- `encoder.type = json`

It does **not** define option models for:

- `encoder.type = none`
- custom encoders
- generic sink output options such as `output.mode` or `output.omit_if_empty`

## Why Two Layers Exist

The JSON encoder has to combine several behaviors:

- array or NDJSON delivery framing
- plain full-row JSON object encoding
- item-level template transform
- row-diff mask-aware output
- by-index delayed materialization
- object-field `null` omission

The pipeline API should expose only the options users need to control directly.
The encoder implementation still needs a small internal option set so that each encoding path stays
explicit and testable.

## User-Facing Options

### `encoder.props.format`

Meaning:

- select the framing for each encoded JSON delivery

Supported values:

- `array` (default)
- `ndjson`

Example:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "format": "ndjson"
    }
  }
}
```

Array format emits one compact JSON array per delivery:

```text
[{"a":1},{"a":2}]
```

NDJSON format emits one compact JSON text per row. Every row, including the last row, ends with LF:

```text
{"a":1}\n
{"a":2}\n
```

NDJSON uses UTF-8, does not emit a BOM, and does not emit outer brackets or row-separator commas.
LF and CR inside string values are JSON-escaped and therefore do not create record boundaries.

For a delivery whose lifecycle is started and finished with zero rows, array format emits `[]` and
NDJSON emits zero bytes. If no delivery is started, neither format creates an output by itself.

The format is a delivery-level encoder option. It does not change sink batching or make the file
sink append to a long-lived file.

### `encoder.transform`

Meaning:

- enable item-level row transform before final JSON delivery framing is emitted

Supported value today:

- `template`

Example:

```json
{
  "encoder": {
    "type": "json",
    "transform": {
      "template": "{\"value\":{{ json(.row.amount) }},\"label\":{{ json(.row.status) }} }"
    }
  }
}
```

Function enabled:

- JSON encoder transform

Effect:

- each input SQL row is exposed to the template as `.row`
- the template renders one JSON item
- the JSON encoder still owns the configured array or NDJSON framing
- NDJSON compacts the rendered JSON item before appending the record LF

Example input rows:

```json
[{"amount":10,"status":"ok"},{"amount":20,"status":"fail"}]
```

Example output payload:

```json
[{"value":10,"label":"ok"},{"value":20,"label":"fail"}]
```

Related document:

- [Encoder Transform](encoder_transform.md)

### `encoder.props.omit_null_columns`

Meaning:

- control whether the native JSON object encoding path omits object fields whose runtime value is
  `null`

Default:

- `true`

Example:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "omit_null_columns": true
    }
  }
}
```

Function enabled:

- object-field `null` omission for native JSON object encoding

Effect:

- on normal full-output rows, `null` object fields are omitted by default
- nested object fields inside `struct` values are also affected
- array `null` items are preserved
- this does not change template semantics
- this does not suppress row-diff fields selected by `output_mask`

Example input row:

```json
{"a":1,"b":null}
```

Output with `omit_null_columns=true`:

```json
{"a":1}
```

Output with `omit_null_columns=false`:

```json
{"a":1,"b":null}
```

Nested object example with `omit_null_columns=true`:

```json
{"a":{"b":1}}
```

Related document:

- [JSON Null Field Omission](json_null_column_omit.md)

## Runtime-Only Options

The current implementation uses an internal row-encoding option bundle equivalent to:

```rust
struct JsonRowEncodeOptions<'a> {
    output_layout: &'a OutputLayout,
    output_mask_mode: OutputMaskMode,
    null_policy: NullColumnPolicy,
}
```

These options are not exposed directly in the pipeline API.
They are derived from the planner output layout, sink output mode, and encoder
configuration.

### `output_layout`

Meaning:

- provides the final output column order, datatype, and fixed tuple value
  reference used for every rendering mode

Function enabled:

- row-diff aware encoding
- dense row exposure for template rendering on row-diff branches

Typical source:

- final sink-side output layout built by the planner

Effect:

- the encoder can align runtime tuples with the final logical output columns
- the encoder can tell which field name should be emitted for each final output column

Conceptual example:

```text
SELECT a AS x, b AS y FROM stream
```

Final output layout:

```text
[x, y]
```

The encoder uses this layout for both final output names and indexed runtime
value access.

### `output_mask_mode`

Meaning:

- tells the encoder how to interpret a tuple that carries `output_mask`

Current modes:

- `HonorMask`
- `DenseForTemplate`

#### `HonorMask`

Meaning:

- use `output_mask` to decide which fields are emitted

Function enabled:

- row-diff sparse JSON object encoding

Effect:

- `mask = false` -> omit field
- `mask = true` -> emit field
- `mask = true` and value is `null` -> still emit `"field": null`

Example:

```json
row values: {"a":null,"b":3}
mask:       [false, true]
```

Output:

```json
{"b":3}
```

#### `DenseForTemplate`

Meaning:

- ignore sparse omission for template input and expose the dense row-diff output row

Function enabled:

- `encoder.transform=template` on row-diff sink branches

Effect:

- unchanged tracked columns remain visible to the template as `null`
- the template still sees a stable row shape under `.row`

Example:

```json
row values: {"a":null,"b":3}
mask:       [false, true]
```

Template input `.row`:

```json
{"a":null,"b":3}
```

This preserves the current template contract until a future mask-aware template design is added.

### `null_policy`

Meaning:

- control whether the encoder's native object writer keeps or omits `null` object fields

Current modes:

- `KeepNulls`
- `OmitNullObjectFields`

#### `KeepNulls`

Meaning:

- emit `"field": null` for object fields whose value is `null`

Used for:

- template-adjacent internal paths where we intentionally keep current behavior
- native paths when `encoder.props.omit_null_columns = false`

Example:

```json
{"a":1,"b":null}
```

Output:

```json
{"a":1,"b":null}
```

#### `OmitNullObjectFields`

Meaning:

- omit object fields whose value is `null`

Used for:

- native full-output JSON encoding when `encoder.props.omit_null_columns = true`

Example:

```json
{"a":1,"b":null}
```

Output:

```json
{"a":1}
```

Nested object example:

```json
{"a":{"b":1,"c":null}}
```

Output:

```json
{"a":{"b":1}}
```

Array example:

```json
{"a":[1,null,2]}
```

Output:

```json
{"a":[1,null,2]}
```

Important limit:

- this policy does not override row-diff sparse semantics
- if `output_mask_mode = HonorMask` and the mask selects a changed-to-`null` field, that field must
  still be emitted as `"field": null`

## Feature Mapping Summary

| User or runtime option | Meaning | Enables / affects |
|---|---|---|
| `encoder.props.format` | select `array` or `ndjson` delivery framing | JSON delivery boundaries and row separators |
| `encoder.transform=template` | render one JSON item per row through a template | encoder transform |
| `encoder.props.omit_null_columns` | omit `null` object fields on native JSON object encoding | null-field omission |
| `output_layout` | align final columns with fixed runtime value references | native, row-diff-aware, and template rendering |
| `output_mask_mode=HonorMask` | emit sparse fields according to `output_mask` | row-diff sparse JSON encoding |
| `output_mask_mode=DenseForTemplate` | expose dense `.row` even on row-diff branches | row-diff + template compatibility |
| `null_policy=KeepNulls` | keep object-field `null` values | legacy / explicit keep-null behavior |
| `null_policy=OmitNullObjectFields` | omit `null` object fields | default full-output null omission |

## Practical End-to-End Examples

### Example 0: NDJSON Delivery

Configuration:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "format": "ndjson"
    }
  }
}
```

Two output rows produce:

```text
{"a":1}\n
{"a":2}\n
```

### Example 1: Plain Full Output

Configuration:

```json
{
  "encoder": {
    "type": "json"
  }
}
```

Derived runtime behavior:

- `output_mask_mode = HonorMask`
- `null_policy = OmitNullObjectFields`

Input row:

```json
{"a":1,"b":null}
```

Output row item:

```json
{"a":1}
```

### Example 2: Plain Full Output With Nulls Kept

Configuration:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "omit_null_columns": false
    }
  }
}
```

Derived runtime behavior:

- `output_mask_mode = HonorMask`
- `null_policy = KeepNulls`

Input row:

```json
{"a":1,"b":null}
```

Output row item:

```json
{"a":1,"b":null}
```

### Example 3: Row-Diff JSON Output

Configuration:

```json
{
  "encoder": {
    "type": "json"
  },
  "output": {
    "mode": "delta"
  }
}
```

Derived runtime behavior:

- `output_layout` is required
- `output_mask_mode = HonorMask`
- changed-to-`null` fields are still emitted

Runtime row state:

```json
row values: {"a":null,"b":10}
mask:       [true, false]
```

Output row item:

```json
{"a":null}
```

### Example 4: Row-Diff With Template Transform

Configuration:

```json
{
  "encoder": {
    "type": "json",
    "transform": {
      "template": "{{ json(.row) }}"
    }
  },
  "output": {
    "mode": "delta"
  }
}
```

Derived runtime behavior:

- `output_layout` is required
- `output_mask_mode = DenseForTemplate`
- `null_policy = KeepNulls`

Runtime row state:

```json
row values: {"a":null,"b":10}
mask:       [false, true]
```

Template input `.row`:

```json
{"a":null,"b":10}
```
