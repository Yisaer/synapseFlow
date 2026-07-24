# Import REST API (Manager)

This document describes the **Manager** REST API for importing persisted metadata.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** Every resource id in the bundle — stream names, pipeline ids,
> sink ids, schema names, memory topics, shared MQTT client keys, UDF names, and
> `flow_instance_id` references — must match `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``,
> the same grammar enforced by the REST API. Import cannot bypass these rules: a
> bundle containing any invalid id is rejected as a whole with `400 Bad Request`,
> naming the first offending field. The same validation is applied to
> `init.json` at startup.

## Endpoint

### Import Metadata Bundle

`POST /import`

Imports a full metadata bundle and atomically replaces the current persisted metadata snapshot in
storage.

The request body is the ZIP returned by the export API. Its `metadata.json` uses the
`ExportBundleV1` shape.

Important behavior:

- import is **storage-only**
- runtime resources are **not** reconciled by this API call
- the imported bundle fully replaces the existing persisted metadata set
- resources missing from the imported bundle are removed from persisted storage
- storage replacement is atomic: either the entire snapshot is replaced or nothing is changed

This means the API is suitable for backup restore and rollback of persisted metadata, but it is
not a runtime checkpoint restore mechanism.

## Request Shape

### `ExportBundleV1`

- `exported_at: number` (Unix seconds from the exported bundle; accepted but not used for storage)
- `resources: ExportResources`

### `ExportResources`

- `memory_topics: ExportMemoryTopic[]`
- `shared_mqtt_clients: SharedMqttClientConfig[]`
- `streams: CreateStreamRequest[]`
- `pipelines: ExportPipeline[]`, where each pipeline has an optional inline `run_state`
  that defaults to `Stopped`

## Validation

The import request is rejected with `400 Bad Request` if:

- a resource array contains duplicate identifiers
- a memory topic has an empty name or zero capacity
- a stream has an empty name
- a pipeline fails basic request validation
- the archive contains an unsafe path, duplicate path, symlink, special file, too many entries,
  an oversized file, or excessive total uncompressed data
- the legacy top-level `pipeline_run_states` resource is present

## Response

- `200 OK` with `Content-Type: application/json`
- `400 Bad Request` if request validation fails
- `409 Conflict` if another import/export command is in progress
- `500 Internal Server Error` if reading or replacing the storage snapshot fails

### `ImportStorageResponse`

- `applied_to_runtime: boolean`
  - currently always `false`
- `imported_resource_counts: ImportResourceCounts`
- `previous_bundle: ExportBundleV1`
  - the full persisted metadata snapshot captured before the import transaction
  - this is metadata only; it is not a complete import ZIP

### `ImportResourceCounts`

- `memory_topics: number`
- `shared_mqtt_clients: number`
- `streams: number`
- `pipelines: number`

## Example

```bash
curl -X POST \
  -H 'Content-Type: application/zip' \
  --data-binary @veloflux-export.zip \
  http://127.0.0.1:8080/import
```

## Notes

- The import API is defined as **full replace**, not partial upsert.
- Use `GET /storage/export` before import when a complete restorable backup is required.
- Because runtime reconciliation is out of scope for this endpoint, a restart or a separate runtime
  apply workflow may still be required after import.
