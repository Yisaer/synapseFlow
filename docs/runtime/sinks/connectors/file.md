# File Sink

The **File sink** writes already encoded sink delivery units to local files. It is a byte sink:
the connector does not inspect rows, schema, output mode, or encoder format.

## Configuration

```json
{
  "id": "file_sink",
  "type": "file",
  "props": {
    "path": "/var/lib/veloflux/output",
    "filename_pattern": "speed_{write_start_ms}_{write_end_ms}_{seq}.json",
    "retention": {
      "max_file_count": 100,
      "max_file_age_days": 7
    }
  },
  "common_sink_props": {
    "batch_count": 1000,
    "batch_duration": 1000
  },
  "encoder": {
    "type": "json",
    "props": {}
  }
}
```

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `type` | string | Yes | - | Must be `file`. |
| `props.path` | string | Yes | - | Local output directory. |
| `props.filename_pattern` | string | No | `"{write_start_ms}_{seq}"` | Complete final filename pattern. It may use static property templates and the runtime placeholders described below. |
| `props.retention.max_file_count` | integer | No | `0` | Maximum generated files to keep for this filename pattern scope. `0` disables count pruning. |
| `props.retention.max_file_age_days` | integer | No | `0` | Maximum generated file age in days. `0` disables age pruning. |
| `encoder.type` | string | No | `json` | Encoder kind. `none` is rejected for file sinks. |

## Delivery Semantics

Each encoded delivery unit becomes one final file:

```text
PhysicalSinkEncoder -> EncodedDelivery -> FileSinkConnector delivery -> one file
```

The file sink does not add delimiters, newlines, headers, or framing. If the output should be
newline-delimited JSON or another framed format, that behavior belongs to the encoder.

For NDJSON output, configure the JSON encoder and an explicit `.ndjson` filename suffix:

```json
{
  "props": {
    "path": "/var/lib/veloflux/output",
    "filename_pattern": "events_{write_start_ms}_{seq}.ndjson"
  },
  "encoder": {
    "type": "json",
    "props": {
      "format": "ndjson"
    }
  }
}
```

Each delivery still creates one atomically published file. NDJSON changes the bytes inside that
file; it does not keep one file open for append or provide a long-lived `tail -f` target.

For CSV output, select the built-in CSV encoder and include a `.csv` extension explicitly:

```json
{
  "props": {
    "path": "/var/lib/veloflux/output",
    "filename_pattern": "{write_start_ms}_{seq}.csv"
  },
  "encoder": {
    "type": "csv",
    "props": {
      "delimiter": ",",
      "header": true
    }
  }
}
```

Each rolled or batched file is one CSV delivery unit and therefore gets its own header when
`header=true`.

Existing common batching controls the delivery unit boundary:

- Without batching, each encoded output payload is written as one file.
- With `common_sink_props.batch_count` or `common_sink_props.batch_duration`, each encoded batch is
  written as one file.

The connector treats both cases identically.

## Rolling via Batching

File rolling is implemented by the existing sink batching layer, not by a separate file-sink row
buffer.

The runtime path is:

```text
Rows -> common sink batching -> encoder -> encoded delivery unit -> file sink -> one final file
```

For row-count rolling, configure `common_sink_props.batch_count`. For example, `batch_count: 2`
emits one encoded delivery unit after every two rows, so the file sink writes one file per two-row
batch. `batch_duration` rolls files on the common sink fixed millisecond processing-time grid.
Duration windows are left-closed/right-open, the first window can be partial, and a row on a
duration boundary belongs to the next file.

The file sink itself does not inspect row counts, schema, or encoded content. It only receives bytes
from the upstream sink delivery path and finalizes one file for each `send(payload)` call. This keeps
rolling behavior consistent across sink types that share common batching.

## Filename Format

The default filename pattern is:

```text
{write_start_ms}_{seq}
```

For example, this pattern includes both ends of the successful write attempt:

```json
{
  "filename_pattern": "speed_{write_start_ms}_{write_end_ms}_{seq}.json"
}
```

It produces names such as:

```text
speed_1779945123456_1779945123472_000001.json
speed_1779945123456_1779945123472_000002.json
```

Supported runtime placeholders are:

- `{write_start_ms}`: wall-clock Unix epoch milliseconds captured when the delivery write starts.
- `{write_end_ms}`: wall-clock Unix epoch milliseconds captured after the temporary file has been
  fully written and flushed, before the final name is published.
- `{seq}`: a six-digit collision retry sequence. This placeholder is required so existing files are
  never overwritten.

The pattern must contain `{seq}` and at least one of `{write_start_ms}` or `{write_end_ms}`. Each
placeholder may appear at most once. Placeholders must be separated by literal text so generated
names can be matched unambiguously for retention. Unknown placeholders, path separators, empty
patterns, and the reserved names `.` and `..` are rejected.

The pattern also supports process-wide static property templates:

```json
{
  "filename_pattern": "vehicle_{{ prop(\"vin\") }}_{write_start_ms}_{seq}.{{ prop(\"format\") }}"
}
```

Static property expressions are rendered once when the pipeline is applied. The remaining runtime
filename pattern is validated and compiled after static rendering. `props.path` remains literal and
is not template-enabled.

Rendered property values remain redacted in internal configuration,
diagnostics, and planner IR. The resulting filename is necessarily plaintext
on the filesystem.

The file type is determined by the encoder and pipeline context, not by the file sink. The connector
does not infer suffixes from encoder, compression, or encryption settings.

## Temporary Files

Writes use a temporary directory under the output directory:

```text
<path>/               final files
<path>/.veloflux_tmp/<scope>/ temporary files for one pipeline/sink
```

Final filenames are not made visible until the payload has been fully written and flushed.
`<scope>` is derived from the pipeline id and sink id, so startup cleanup is limited to the current
pipeline/sink writer. On startup, the scoped tmp directory is created if missing; if it already
exists, orphaned entries inside that scope are deleted.

Payload writes only ensure the output and scoped tmp directories exist. They do not clean
`.veloflux_tmp/`, because another pipeline or process may be using a different tmp scope in the same
output directory.

Temporary and final files must be on the same filesystem. If finalization fails with a cross-device
rename/link error, the write fails with a clear error. The connector does not fall back to
copy-and-delete because that can expose partial final files.

## Retention

Retention runs after a file is successfully finalized.

Pruning is scoped to generated final files in the same directory that match the compiled
`filename_pattern`. It ignores `.veloflux_tmp/` and unrelated names.

Pipelines that share a directory and filename pattern share retention pruning. Use separate output
directories or distinct literal text in each pattern when independent retention is required. Count
retention sorts matching files by `{write_start_ms}` when present, otherwise `{write_end_ms}`, then
by `{seq}`. Age retention continues to use filesystem modification time.

Retention is best-effort under concurrent writers and tolerates files disappearing between directory
scan and delete.

## Output Mode

`output.mode=delta` is allowed when the selected encoder supports it. The file sink writes encoded
bytes as-is and does not interpret insert, update, or delete events. The CSV encoder rejects delta
mode because it requires stable dense rows.

## Future Compatibility

The file sink does not own delivery compression, encryption, Parquet encoding, size-based batching,
or object storage semantics. Those belong to future common batching, delivery wrapper, encoder, or
object backend features.
