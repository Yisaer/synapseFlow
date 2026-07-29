# Import REST API (Manager)

This document describes the **Manager** REST API for importing persisted metadata.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** Every resource id in the bundle — stream names, pipeline ids,
> sink ids, schema names, table names, memory topics, shared MQTT client keys, UDF names, and
> `flow_instance_id` references — must match `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``,
> the same grammar enforced by the REST API. Import cannot bypass these rules: a
> bundle containing any invalid id is rejected as a whole with `400 Bad Request`,
> naming the first offending field. The same validation is applied to
> startup resource directory.

## Endpoint

### Import Metadata Bundle

`POST /import`

Imports a full metadata bundle and atomically replaces the current persisted metadata snapshot in
storage.

The multipart `file` field contains the ZIP returned by the export API. Its `manifest.json`
uses the same `ResourceManifestV1` shape accepted from a startup resource
directory. The ZIP is only the HTTP envelope; `--init-dir` reads its extracted
contents directly.

Request:

```text
Content-Type: multipart/form-data
file: required ZIP file, exactly once
```

The file must be non-empty, its filename must end in `.zip`, and its compressed
size must not exceed 512 MiB. The former raw `application/zip` request body is
not supported.

Important behavior:

- import is **storage-only**
- runtime resources are **not** reconciled by this API call
- the imported bundle fully replaces the existing persisted metadata set
- every resource must include a positive JSON-safe `revision`
- import stores revisions but does not compare them; a lower imported revision
  replaces a higher stored revision
- resources missing from the imported bundle are removed from persisted storage
- storage replacement is atomic: either the entire snapshot is replaced or nothing is changed

This means the API is suitable for backup restore and rollback of persisted metadata, but it is
not a runtime checkpoint restore mechanism.

## Request Shape

### `ResourceManifestV1`

- `format_version: number` (currently `1`)
- `bundle_version: string` (validated but not written to startup apply state)
- `resources: ExportResources`

### `ExportResources`

- `memory_topics: ExportMemoryTopic[]`
- `shared_mqtt_clients: SharedMqttClientConfig[]`
- `schemas: ExportSchema[]`
- `streams: CreateStreamRequest[]`
- `pipelines: ExportPipeline[]`, where each pipeline has an optional inline `run_state`
  that defaults to `Stopped`
- `udfs: ExportUdf[]`
- `tables: ExportTable[]`

## Validation

The import request is rejected with `400 Bad Request` if:

- a resource array contains duplicate identifiers
- a memory topic has an empty name or zero capacity
- a stream has an empty name
- a pipeline fails basic request validation
- a table has a duplicate name, an unsupported table type, or an invalid schema ref
- the archive contains an unsafe path, duplicate path, symlink, special file, too many entries,
  an oversized file, or excessive total uncompressed data
- the legacy top-level `pipeline_run_states` resource is present

## Response

- `200 OK` with `Content-Type: application/json`
- `400 Bad Request` if request validation fails
- `413 Content Too Large` if the uploaded ZIP exceeds 512 MiB
- `415 Unsupported Media Type` if the request is not multipart
- `409 Conflict` if another import/export command is in progress
- `500 Internal Server Error` if reading or replacing the storage snapshot fails

### `ImportStorageResponse`

- `applied_to_runtime: boolean`
  - currently always `false`
- `imported_resource_counts: ImportResourceCounts`
- `previous_resources: ExportResources`
  - the metadata snapshot captured before the import transaction
  - this is not a complete import ZIP and has no `bundle_version`

### `ImportResourceCounts`

- `memory_topics: number`
- `shared_mqtt_clients: number`
- `schemas: number`
- `streams: number`
- `pipelines: number`
- `udfs: number`
- `tables: number`

## Example

```bash
curl -X POST \
  -F 'file=@veloflux-export.zip' \
  http://127.0.0.1:8080/import
```

## Notes

- The import API is defined as **full replace**, not partial upsert.
- Use `GET /storage/export?bundle_version=<version>` before import when a complete
  restorable backup is required.
- Because runtime reconciliation is out of scope for this endpoint, a restart or a separate runtime
  apply workflow may still be required after import.
