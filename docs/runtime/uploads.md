# File Upload Design

## Background

Several connectors need to reference external files at runtime—TLS certificates, CA bundles,
private keys, protobuf descriptor sets, etc. Today these must be placed on the filesystem
manually before veloFlux starts. There is no API for managing them.

This document describes an upload API that persists files into the data directory and integrates
with the existing export/import and bootstrap (`init.json` / `init.tar.gz`) mechanisms.

## Goals

- Provide a REST API for uploading, listing, downloading, and deleting files.
- Store uploaded files under `data_dir/uploads/` as plain files on the filesystem.
- Integrate with the export tar.gz archive and the `init.tar.gz` bootstrap format.
- Keep the implementation minimal: no redb metadata table, filesystem is the single source of truth.

## Non-Goals

- Version history or rollback of uploaded files.
- SHA-256 content-based deduplication across different file names.
- File-level encryption (sensitive files should go through `secrets.enc`).
- Automatic garbage collection of orphaned files when pipelines are deleted.

## Storage Model

```
data_dir/uploads/
  ca-cert.pem
  client-key.pem
  descriptor.proto
```

Files are stored under the uploads directory directly, keyed by the user-supplied `name`.
There is **no redb table** for upload metadata—the filesystem is the authority. This avoids
dual-write consistency issues and keeps the upload surface simple.

### Overwrite Semantics

Uploading with an existing `name` overwrites the file. This is intentional: uploads are
content that users update over time (e.g., rotated certificates), not immutable records.

## Name Validation

File names map directly to filesystem paths under `uploads/`. To prevent path traversal and
filesystem safety issues, names are validated against a restricted character set:

- Must match `[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}`.
- Must not start with `.` (hidden file prevention).
- Slashes (`/`, `\`) and `..` are rejected by the regex.

This is stricter than the general resource-id grammar because the name becomes a literal
filesystem path component.

## Pipeline / Stream Reference

Pipeline and stream configuration fields that reference an uploaded file accept a path
relative to the data directory:

```json
{
  "props": {
    "ca_file": "uploads/ca-cert.pem",
    "key_file": "uploads/client-key.pem"
  }
}
```

Connectors resolve these paths by joining `{data_dir}/{relative_path}` at runtime.

## Export / Import Integration

The export tar.gz archive (`GET /storage/export`) includes an `uploads/` directory alongside
`metadata.json` and `wasm_files/`. The archive layout:

```
veloflux-export-<timestamp>.tar.gz
  metadata.json
  wasm_files/
    <sha256>.wasm
  uploads/
    ca-cert.pem
    client-key.pem
```

Import (`POST /import`) extracts the `uploads/` directory and copies its contents to
`data_dir/uploads/`. Existing files with the same name are overwritten. If the archive
does not contain an `uploads/` directory, no upload files are affected—the import succeeds
normally for metadata and WASM files only.

Note: upload files are **not** described in `metadata.json` / `ExportBundleV1`. The import
handler treats them as opaque directory contents, validated only by name safety (the same
regex applied to archive entry names).

## Bootstrap Integration (`init.tar.gz`)

The existing `init.json` file is a pure JSON manifest that cannot carry binary files.
To support including uploads in bootstrap, a new `init.tar.gz` format is introduced alongside
`init.json`:

```
data_dir/
  init.tar.gz        ← preferred (can carry metadata + wasm + uploads)
  init.json          ← fallback (legacy, metadata-only)
```

### Priority

1. If `init.tar.gz` exists, unpack and apply it before any `init.json` processing.
2. If only `init.json` exists (and no `init.tar.gz`), fall back to the legacy init path.
3. The init apply metadata (`last_init_json_modified_at_ms`) tracks whichever file was applied.

### Init tar.gz Layout

```
init.tar.gz
  metadata.json
  wasm_files/
    <sha256>.wasm
  uploads/
    ca-cert.pem
    descriptor.proto
```

### Apply Semantics

On startup (`init_process.rs`):

1. Unpack `init.tar.gz` to a temporary directory.
2. Load `metadata.json` → validate → apply init snapshot to redb (existing `init.json` logic).
3. If `uploads/` directory exists in the archive → validate each entry name → copy files
   to `data_dir/uploads/` (overwrite).
4. The apply is **not atomic** across metadata and uploads: metadata goes through redb
   transactions, uploads are filesystem copies. If uploads copy fails after metadata commit,
   the init apply metadata is still advanced (uploads are non-critical; a missing file
   surfaces as a runtime error when a connector tries to open it).

The non-atomicity is a documented trade-off. Since uploads have no transactional semantics on
disk, we accept eventual consistency rather than blocking.

## API Design

See `user_docs/api/upload.md` for the endpoint reference.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/files/upload` | Upload a file (multipart) |
| `GET` | `/files` | List uploaded files |
| `GET` | `/files/:name` | Download a file |
| `DELETE` | `/files/:name` | Delete a file |

### Concurrency

File upload/delete operations are serialized with the same import/export operation guard to
prevent concurrent mutation of the `uploads/` directory.

### Size Limit

Upload requests are subject to a configurable body size limit (default 16 MB), enforced by
axum's `DefaultBodyLimit` layer.

### File Permissions

Uploaded files are written with `0o600` permissions (owner read/write only) to reduce the
risk of accidental exposure of configuration files that may contain sensitive material.

## Failure Semantics

- **Missing file at runtime.** If a pipeline references `uploads/ca-cert.pem` and the file
  does not exist (deleted after the pipeline was created, or init.tar.gz uploads copy failed),
  the connector returns a clear error at initialization time. The pipeline fails to start.
- **Name rejected.** `400 Bad Request` with an error message naming the invalid character.
- **Disk full / I/O error.** `500 Internal Server Error`. No partial state is left behind
  (the file write uses a temp-file-and-rename pattern).

## Future Work

- Automatic upload file inclusion in the `init.tar.gz` during export (a flag like
  `--with-uploads` on the export command).
- Per-pipeline reference tracking so that unused upload files can be identified for manual
  cleanup.
- Content-addressable storage mode with SHA-256 naming for deployments that need deduplication.
