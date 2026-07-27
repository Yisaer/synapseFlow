# Export REST API (Manager)

This document describes the **Manager** REST API for exporting persisted metadata.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** Resource ids in the exported bundle (stream names, pipeline
> ids, schema names, memory topics, shared MQTT client keys, UDF names, and
> `flow_instance_id` references) follow `` `[A-Za-z][A-Za-z0-9_]{0,127}` `` — the
> same grammar required by the create APIs and re-validated on import.

## Endpoints

### Export Metadata Bundle

`GET /storage/export?bundle_version=<version>`

Exports the current persisted metadata as a downloadable ZIP bundle. The ZIP contains
`manifest.json`, referenced WASM files, and installed schema sources. The caller
must provide the opaque, non-empty `bundle_version`. Select the version for the
artifact being prepared; manual edits made after export remain part of that
version.

The export is a storage-level metadata snapshot. It does **not** include runtime-only state such as:

- processor internal state
- runtime metrics or stats
- shared stream live subscriber state
- other in-memory runtime resources

Response:

- `200 OK` with `Content-Type: application/zip`
- `200 OK` includes `Content-Disposition: attachment; filename="veloflux-export.zip"`
- `409 Conflict` if another import/export command is in progress
- `500 Internal Server Error` if export snapshot building fails

Example:

```bash
curl -sOJ 'http://127.0.0.1:8080/storage/export?bundle_version=2026.07.24-1'
```

## Response Shape

### `ResourceManifestV1`

- `format_version: number` (currently `1`)
- `bundle_version: string`
- `resources: ExportResources`

### `ExportResources`

- `memory_topics: ExportMemoryTopic[]`
- `shared_mqtt_clients: SharedMqttClientConfig[]`
- `schemas: ExportSchema[]`
- `streams: CreateStreamRequest[]`
- `pipelines: ExportPipeline[]`
- `udfs: ExportUdf[]`

### `ExportMemoryTopic`

- `topic: string`
- `kind: string` (`bytes` or `collection`)
- `capacity: number`

### `SharedMqttClientConfig`

- `key: string`
- `broker_url: string`
- `topic: string`
- `client_id: string`
- `qos: number`

### `ExportPipeline`

- all fields from `CreatePipelineRequest`
- `run_state: StoredPipelineDesiredState`
  - `Stopped`
  - `Running`
  - `{ "RunningScheduled": <unix_timestamp_ms> }`

## Export, Edit, and Use as an Init Directory

Export, import, and startup initialization use the same canonical resource
directory. Export returns that directory in a ZIP envelope, while
`--init-dir` reads the extracted directory directly.

```bash
curl -sS -o veloflux-export.zip \
  'http://127.0.0.1:8080/storage/export?bundle_version=2026.07.24-1'
unzip veloflux-export.zip -d ./init

# Edit ./init/manifest.json, ./init/schemas/, or ./init/wasm_files/.

veloflux \
  --config ./config.yaml \
  --data-dir ./data \
  --init-dir ./init
```

The extracted layout must have `manifest.json` directly under `./init`:

```text
init/
├── manifest.json
├── schemas/<type>/<name>/
└── wasm_files/<sha256>.wasm
```

When preparing the artifact:

- choose its `bundle_version` in the export request
- keep resource IDs and cross-resource references valid
- keep referenced schema files under `schemas/<type>/<name>/`
- after changing WASM content, recompute its SHA-256, rename the file, and
  update the UDF `wasm_sha256`
- do not change `format_version`

A node skips a startup artifact whose `bundle_version` it has already applied.
Choose a new version when preparing a later revision for that node.

Startup uses add-only Apply semantics: missing resources are created, but live
resources with the same identity are retained and resources omitted from the
manifest are not deleted. To replace the complete persisted resource set, send
the ZIP to the import API instead.

## Notes

- `streams` are exported using the same shape as `CreateStreamRequest`.
- `pipelines` use the `CreatePipelineRequest` shape with inline `run_state`.
- The exported arrays are sorted by stable identifiers to make the output easier to diff.
- The bundle is intended for future import / migration workflows, not for runtime checkpoint recovery.
