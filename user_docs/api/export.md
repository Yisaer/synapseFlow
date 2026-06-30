# Export REST API (Manager)

This document describes the **Manager** REST API for exporting persisted metadata.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** Resource ids in the exported bundle (stream names, pipeline
> ids, schema names, memory topics, shared MQTT client keys, UDF names, and
> `flow_instance_id` references) follow `` `[A-Za-z][A-Za-z0-9_]{0,127}` `` — the
> same grammar required by the create APIs and re-validated on import.

## Endpoints

### Export Metadata Bundle

`GET /storage/export`

Exports the current persisted metadata as a downloadable JSON bundle.

The export is a storage-level metadata snapshot. It does **not** include runtime-only state such as:

- processor internal state
- runtime metrics or stats
- shared stream live subscriber state
- other in-memory runtime resources

Response:

- `200 OK` with `Content-Type: application/json`
- `200 OK` includes `Content-Disposition: attachment; filename="veloflux-metadata-export-<unix_secs>.json"`
- `409 Conflict` if another import/export command is in progress
- `500 Internal Server Error` if export snapshot building fails

Example:

```bash
curl -sOJ http://127.0.0.1:8080/storage/export
```

## Response Shape

### `ExportBundleV1`

- `exported_at: number` (Unix seconds)
- `resources: ExportResources`

### `ExportResources`

- `memory_topics: ExportMemoryTopic[]`
- `shared_mqtt_clients: SharedMqttClientConfig[]`
- `streams: CreateStreamRequest[]`
- `pipelines: CreatePipelineRequest[]`
- `pipeline_run_states: ExportPipelineRunState[]`

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

### `ExportPipelineRunState`

- `pipeline_id: string`
- `desired_state: string` (`Running` or `Stopped`)

## Notes

- `streams` are exported using the same shape as `CreateStreamRequest`.
- `pipelines` are exported using the same shape as `CreatePipelineRequest`.
- The exported arrays are sorted by stable identifiers to make the output easier to diff.
- The bundle is intended for future import / migration workflows, not for runtime checkpoint recovery.
