# File Source Stream

The `file` source stream watches a local file or one directory level of local files and emits one
stream row for each complete appended line. It is intended for log tailing workloads.

## Configuration

Runtime stream properties:

| Field | Required | Default | Description |
|---|---:|---|---|
| `path` | yes | none | Existing file or directory path to watch. |

The path is validated when the stream is created and again when the connector starts. It must exist
and must be either a regular file or a directory. Other filesystem object types are rejected.
Symbolic links are rejected in the current implementation.

File streams require an explicit decoder:

```json
{ "type": "file_line", "props": {} }
```

`shared=true`, `eventtime`, and `sampler` are not supported for file streams in the current
implementation.

## Schema

The schema is built in and is not inferred from file contents or supplied by the user:

| Column | Type | Description |
|---|---|---|
| `line` | `string` | One complete line, without the trailing line terminator. |
| `filename` | `string` | Basename of the file that produced the line. |

For `\r\n` input, both `\n` and the preceding `\r` are removed. Partial trailing lines are buffered
in memory and are emitted only after a later write completes the line with `\n`.

## Runtime Behavior

Without a compatible checkpoint, the connector reads the configured file or all direct regular
files in the configured directory from byte offset `0`. With checkpointing enabled, a restarted
pipeline resumes each known file from the byte offset immediately after its last emitted complete
line. Filesystem notifications wake the same read path for subsequent writes.

Directory mode is non-recursive. Only direct child regular files are considered. A new direct child
file is read from byte offset `0` when it appears or is first observed through a filesystem event.

The connector keeps in-memory per-file cursors:

- `offset`: the byte position immediately after the last complete line sent to the pipeline.
- `read_offset`: the next byte position to read during the current connector run.
- `pending`: bytes after the last complete newline.

Only `offset` and the file identity are persisted for checkpoint recovery. `read_offset` and
`pending` are runtime-only state. A restarted connector begins reading at `offset`, so an incomplete
trailing line is read again instead of being stored in the checkpoint.

If a watched file is replaced or shrinks below `offset`, the connector resets both offsets to `0`,
clears `pending`, and resumes reading from the beginning. If only the runtime-only partial tail is
truncated, the connector discards `pending` and reads that tail again from `offset`.

Ordering is guaranteed within a single file because each file cursor is read sequentially. Directory
mode does not define a total order across different files.

## Example

```json
POST /streams
{
  "name": "app_logs",
  "revision": 1,
  "type": "file",
  "props": { "path": "/var/log/my-app" },
  "decoder": { "type": "file_line", "props": {} }
}
```

The request schema field, if present, is ignored for `type=file`; the installed schema is always
`line string, filename string`.
