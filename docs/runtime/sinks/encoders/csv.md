# CSV Sink Encoder

The built-in CSV sink encoder converts the planner's final ordered output layout into UTF-8 CSV
bytes. It is intended for byte-oriented sink connectors such as file, HTTP, MQTT, and NOP.

## Configuration

```json
{
  "encoder": {
    "type": "csv",
    "props": {
      "delimiter": ",",
      "header": true
    }
  }
}
```

| Property | Type | Default | Description |
|---|---|---|---|
| `delimiter` | string | `","` | Exactly one ASCII byte. CR, LF, and `"` are rejected. |
| `header` | boolean | `true` | Emit the final SQL-visible column names once at the beginning of each delivery unit. |

Unknown encoder properties are ignored consistently with the existing encoder configuration
model. `encoder.transform` is not supported for CSV and is rejected during planning.

## Record and Delivery Semantics

- Output is UTF-8 with LF record terminators and a trailing LF.
- RFC 4180-style quoting is applied when a field contains the delimiter, a quote, CR, or LF.
- Quotes inside quoted fields are doubled.
- A header, when enabled, is emitted once per delivery unit. Consequently, file-sink rolling or
  common sink batching produces a self-describing CSV payload for every delivery.
- Rows and fields preserve the order of the planner-provided `OutputLayout`. Aliases become header
  names, and direct and computed columns are read through their fixed output value references.
- The encoder emits streaming chunks from `begin_delivery`, `append`, and `finish_delivery`; it
  does not first build a complete CSV document or an intermediate row map.

## Value Encoding

| veloFlux value | CSV field representation |
|---|---|
| `null` | empty field |
| boolean | `true` or `false` |
| signed/unsigned integer | base-10 text |
| finite float | shortest round-trippable decimal text |
| non-finite float | `NaN`, `Infinity`, or `-Infinity` |
| string | UTF-8 text with CSV escaping as needed |
| bytes | standard Base64 text |
| timestamp | `%Y-%m-%d %H:%M:%S%.f%:z` |
| array/object/map | compact JSON text inside one CSV field |

The `null` representation is intentionally indistinguishable from an empty string in CSV. This is
the conventional flat CSV behavior and avoids introducing a format-specific null sentinel.

## Output Mode and Batching

CSV requires a stable, dense row schema. `output.mode=delta` can attach sparse change intent through
an output mask, so CSV rejects that combination during planning. Use `output.mode=full` with CSV.

Common `batch_count` and `batch_duration` settings are supported. The streaming encoder rewrite
keeps one CSV encoder instance per processor, reuses its buffers, and emits one header at the start
of every encoded delivery unit.

## Connector Notes

- HTTP infers `Content-Type: text/csv; charset=utf-8` when `content_type` is omitted.
- File writes each encoded delivery unit as one file. Use a suffix such as `.csv`; the connector
  never infers filename suffixes from the encoder.
- Compression and encryption remain downstream delivery transforms and operate on the CSV bytes.

## Error Handling

Invalid CSV properties and unsupported combinations fail while the sink plan is built. A runtime
row whose fixed value reference cannot be resolved fails encoding instead of silently changing the
column layout.
