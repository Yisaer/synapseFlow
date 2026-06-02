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
    "filename_prefix": "speed_",
    "filename_suffix": ".json",
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
| `props.filename_prefix` | string | No | `""` | Literal prefix before the timestamp. It may be empty and must not contain path separators. |
| `props.filename_suffix` | string | No | `""` | Literal suffix after the sequence. It may be empty and must not contain path separators. |
| `props.retention.max_file_count` | integer | No | `0` | Maximum generated files to keep for this prefix/suffix scope. `0` disables count pruning. |
| `props.retention.max_file_age_days` | integer | No | `0` | Maximum generated file age in days. `0` disables age pruning. |
| `encoder.type` | string | No | `json` | Encoder kind. `none` is rejected for file sinks. |

## Delivery Semantics

Each encoded delivery unit becomes one final file:

```text
PhysicalSinkEncoder -> EncodedDelivery -> FileSinkConnector delivery -> one file
```

The file sink does not add delimiters, newlines, headers, or framing. If the output should be
newline-delimited JSON or another framed format, that behavior belongs to the encoder.

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
batch. `batch_duration` works the same way for time-based delivery boundaries.

The file sink itself does not inspect row counts, schema, or encoded content. It only receives bytes
from the upstream sink delivery path and finalizes one file for each `send(payload)` call. This keeps
rolling behavior consistent across sink types that share common batching.

## Filename Format

Generated filenames use:

```text
{filename_prefix}{ts_ms}_{seq}{filename_suffix}
```

Example:

```text
speed_1779945123456_000001.json
speed_1779945123456_000002.json
```

`ts_ms` is the file sink wall-clock UTC epoch milliseconds when the payload write starts. `seq` is a
six-digit collision retry sequence in the same timestamp bucket.

`filename_prefix` and `filename_suffix` are literal affixes:

- `filename_prefix: "speed_"` includes the separator before the timestamp.
- `filename_prefix: ""` produces names such as `1779945123456_000001.json`.
- `filename_suffix: ".jsonl.gz"` is allowed.
- `filename_suffix: ""` intentionally produces extension-less files such as
  `speed_1779945123456_000001`.

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

Pruning is scoped to generated final files in the same directory with the same literal
`filename_prefix` and `filename_suffix`. It ignores `.veloflux_tmp/` and unrelated names.

Pipelines that share a directory, prefix, and suffix share retention pruning. Empty
`filename_prefix` is broad: it matches all generated names shaped like `{ts_ms}_{seq}{suffix}` in the
directory. Use separate output directories or non-empty prefixes when independent retention is
required.

Retention is best-effort under concurrent writers and tolerates files disappearing between directory
scan and delete.

## Output Mode

`output.mode=delta` is allowed. The file sink writes encoded bytes as-is and does not interpret
insert, update, or delete events. Delete representation is encoder-owned.

## Future Compatibility

The file sink does not own delivery compression, encryption, Parquet encoding, size-based batching,
or object storage semantics. Those belong to future common batching, delivery wrapper, encoder, or
object backend features.
