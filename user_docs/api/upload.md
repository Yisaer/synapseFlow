# File Upload REST API (Manager)

This document describes the **Manager** REST API for uploading, listing, downloading,
and deleting user files.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **File names.** File names are validated against `` `[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}` ``.
> Slashes, backslashes, dots as the first character, and path traversal sequences
> (`..`) are rejected.

## Endpoints

### Upload a File

`POST /files/upload`

Upload a file to the veloFlux data directory. The file is stored at
`{data_dir}/uploads/{name}` and can be referenced by pipelines and streams
using the relative path `uploads/{name}`.

**Request:** `multipart/form-data`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | text | yes | Target file name (validated against the name grammar) |
| `file` | binary | yes | File contents |

**Responses:**

- `200 OK` — File uploaded successfully (including overwrite of an existing file)
- `400 Bad Request` — Missing field, empty content, or invalid file name
- `413 Content Too Large` — File exceeds the upload size limit
- `409 Conflict` — Another import/export/upload operation is in progress
- `500 Internal Server Error` — I/O error during file write

**Response body (200):**

```json
{
  "name": "ca-cert.pem",
  "size_bytes": 2048
}
```

**Example:**

```bash
curl -X POST http://127.0.0.1:8080/files/upload \
  -F "name=ca-cert.pem" \
  -F "file=@/local/path/ca-cert.pem"
```

### List Uploaded Files

`GET /files`

Lists all files currently in the uploads directory.

**Responses:**

- `200 OK`

**Response body:**

```json
[
  {
    "name": "ca-cert.pem",
    "size_bytes": 2048,
    "modified_at": "2026-07-20T10:30:00Z"
  },
  {
    "name": "client-key.pem",
    "size_bytes": 1679,
    "modified_at": "2026-07-20T09:15:00Z"
  }
]
```

**Example:**

```bash
curl http://127.0.0.1:8080/files
```

### Download a File

`GET /files/:name`

Download the contents of an uploaded file.

**Responses:**

- `200 OK` with `Content-Type: application/octet-stream`
- `400 Bad Request` — Invalid file name in path
- `404 Not Found` — File does not exist in uploads directory

**Example:**

```bash
curl -O http://127.0.0.1:8080/files/ca-cert.pem
```

### Delete a File

`DELETE /files/:name`

Delete a file from the uploads directory.

**Responses:**

- `200 OK`
- `400 Bad Request` — Invalid file name in path
- `404 Not Found` — File does not exist
- `409 Conflict` — Another import/export/upload operation is in progress

**Response body (200):**

```json
{
  "deleted": "ca-cert.pem"
}
```

**Example:**

```bash
curl -X DELETE http://127.0.0.1:8080/files/ca-cert.pem
```

## Using Uploaded Files in Pipelines and Streams

Uploaded files are referenced by their relative path from the data directory:

```json
{
  "name": "my-mqtt-stream",
  "type": "mqtt",
  "props": {
    "broker_url": "mqtts://broker.example.com:8883",
    "topic": "sensors/#",
    "ca_file": "uploads/ca-cert.pem",
    "key_file": "uploads/client-key.pem"
  }
}
```

The connector resolves the path to `{data_dir}/uploads/{name}` at runtime.

## File Name Constraints

File names must match `[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}`. This means:

- Must start with a letter or digit (not a dot or special character)
- May contain letters, digits, dots, underscores, and hyphens
- Maximum 255 characters
- No slashes, backslashes, or path traversal sequences

Examples:

| Name | Valid? | Reason |
|------|--------|--------|
| `ca-cert.pem` | Yes | |
| `.hidden` | No | Starts with `.` |
| `dir/file` | No | Contains `/` |
| `../escape` | No | Path traversal |
| `my_cert-2026.pem` | Yes | |

## Notes

- Uploading with the same `name` as an existing file **overwrites** it.
- There is no version history; the previous content is lost on overwrite.
- Uploaded files are stored as plain files on disk (no encryption).
  For sensitive material (private keys, passwords), use the secret store (`secrets.enc`)
  instead.
- Files remain on disk after deletion of the pipelines that reference them.
  Delete unused files manually via `DELETE /files/:name`.
- The upload endpoint is serialized with import/export operations.
  Concurrent `POST /files/upload` and `POST /import` requests will receive
  `409 Conflict`.
