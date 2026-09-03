# File Source Stream

The `file` source stream watches a local file or one directory level of local files and emits stream
rows from appended file content. It is intended for log tailing and append-only ingestion workloads.

## Configuration

Runtime stream properties:

| Field | Required | Default | Description |
|---|---:|---|---|
| `path` | yes | none | Existing file or directory path to watch. |
| `framing` | no | `append_batch` | Selects how newly observed file content is grouped into messages. |

The default framing is:

```json
"framing": { "mode": "append_batch" }
```

`append_batch` treats all bytes observed between the cursor and the current file length during one
file-change handling pass as one message. The batch boundary is therefore the source's observed
write-event handling boundary; it is not a guarantee about the writer's individual system calls.

Delimiter framing is available for workloads that require stable record boundaries:

```json
"framing": {
  "mode": "delimiter",
  "delimiter": "\n",
  "include_delimiter": false
}
```

The delimiter is matched as a byte sequence and may span read buffers. When `include_delimiter` is
false, the delimiter is removed from the emitted message. An incomplete trailing message remains
buffered until a later append supplies the delimiter.

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
| `line` | `string` | One source-framed text payload, currently one complete line without the trailing line terminator. |
| `filename` | `string` | Basename of the file that produced the line. |

For `\r\n` input in delimiter mode with `delimiter="\n"`, both `\n` and the preceding `\r` are
removed. In delimiter mode, partial trailing lines are buffered in memory and emitted only after a
later write completes the delimiter. Under `append_batch` framing, `line` may contain multiple lines
and their original line terminators; the
field name is retained for compatibility with the existing `file_line` decoder.

## Runtime Behavior

Without a compatible checkpoint, the connector reads the configured file or all direct regular
files in the configured directory from byte offset `0`. With checkpointing enabled, a restarted
pipeline resumes each known file from the byte offset immediately after its last emitted framed
payload. Filesystem notifications wake the same read path for subsequent writes.

`append_batch` advances the cursor after the observed batch is emitted and persists the byte offset
immediately after that batch. Duplicate notifications that do not add new bytes do not emit another
message. Delimiter mode persists only the offset after the last complete delimited message, so an
incomplete trailing message can be read again after recovery.

Directory mode is non-recursive. Only direct child regular files are considered. A new direct child
file is read from byte offset `0` when it appears or is first observed through a filesystem event.

The connector keeps in-memory per-file cursors:

- `offset`: the byte position immediately after the last framed payload sent to the pipeline.
- `read_offset`: the next byte position to read during the current connector run.
- `pending`: bytes after the last complete delimiter-framed payload; unused by `append_batch`.

The canonical absolute file path, `offset`, and a physical file fingerprint are persisted for
checkpoint recovery. `read_offset` and `pending` are runtime-only state. A restarted connector begins
reading at `offset`, so an incomplete
trailing line is read again instead of being stored in the checkpoint.

If a watched file is replaced or shrinks below `offset`, the connector resets both offsets to `0`,
clears `pending`, and resumes reading from the beginning. The physical fingerprint detects same-path
replacement when the platform provides a stable file identity.

Ordering is guaranteed within a single file because each file cursor is read sequentially. Directory
mode does not define a total order across different files.

## Example

```json
POST /streams
{
  "name": "app_logs",
  "revision": 1,
  "type": "file",
  "props": {
    "path": "/var/log/my-app",
    "framing": { "mode": "append_batch" }
  },
  "decoder": { "type": "file_line", "props": {} }
}
```

The request schema field, if present, is ignored for `type=file`; the installed schema is always
`line string, filename string`.
