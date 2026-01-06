# `sf_rest_tool.py`: Create a Stream + Pipeline via REST

`scripts/sf_rest_tool.py` is a small Python helper that provisions:

- A **stream** (`POST /streams`) backed by an MQTT source (JSON decoder + JSON schema).
- A **pipeline** (`POST /pipelines`) that runs a simple `SELECT ... FROM <stream>` query with a `nop` sink.
- Optionally starts the pipeline (`POST /pipelines/:id/start`).

It is mainly intended for quick local bring-up and basic end-to-end validation.

## Prerequisites

- `python3`
- A running SynapseFlow manager API (default `http://127.0.0.1:8080`)
- An MQTT broker reachable from SynapseFlow (e.g. `tcp://127.0.0.1:1883`)

## Quickstart (teaching-friendly: 5 columns)

Create a stream named `demo_stream` and a pipeline `demo_pipeline`, and start it:

```bash
python3 scripts/sf_rest_tool.py provision \
  --base-url http://127.0.0.1:8080 \
  --stream-name demo_stream \
  --pipeline-id demo_pipeline \
  --columns 5 \
  --broker-url tcp://127.0.0.1:1883 \
  --topic /demo/data \
  --qos 0
```

Clean up (delete pipeline + stream):

```bash
python3 scripts/sf_rest_tool.py cleanup \
  --base-url http://127.0.0.1:8080 \
  --stream-name demo_stream \
  --pipeline-id demo_pipeline
```

Recreate only the pipeline (useful when iterating SQL), and start it:

```bash
python3 scripts/sf_rest_tool.py recreate-pipeline \
  --base-url http://127.0.0.1:8080 \
  --stream-name demo_stream \
  --pipeline-id demo_pipeline \
  --columns 5
```

## Useful flags

- `--dry-run`: prints a truncated preview of the JSON bodies (and some sizes) without calling the API.
- `--no-start`: creates the pipeline but does not start it.
- `--force`: on `provision`, runs `cleanup` first (best-effort deletes).

Example:

```bash
python3 scripts/sf_rest_tool.py provision --stream-name demo_stream --pipeline-id demo_pipeline --columns 5 --dry-run
```

## What gets created

### Stream (`POST /streams`)

The tool creates a stream definition shaped like:

```json
{
  "name": "demo_stream",
  "type": "mqtt",
  "props": { "broker_url": "tcp://127.0.0.1:1883", "topic": "/demo/data", "qos": 0 },
  "schema": {
    "type": "json",
    "props": {
      "columns": [
        { "name": "a1", "data_type": "string" },
        { "name": "a2", "data_type": "string" },
        { "name": "a3", "data_type": "string" },
        { "name": "a4", "data_type": "string" },
        { "name": "a5", "data_type": "string" }
      ]
    }
  },
  "decoder": { "type": "json", "props": {} },
  "shared": false
}
```

Notes:

- Column names are generated as `a1..aN`.
- All columns are typed as `string` (intended as a simple baseline).

### Pipeline (`POST /pipelines`)

The tool creates a pipeline definition shaped like:

```json
{
  "id": "demo_pipeline",
  "sql": "select a1,a2,a3,a4,a5 from demo_stream",
  "sinks": [
    {
      "type": "nop",
      "commonSinkProps": { "batchDuration": 100, "batchCount": 50 }
    }
  ],
  "options": {
    "plan_cache": { "enabled": false },
    "eventtime": { "enabled": false }
  }
}
```

### Start pipeline (`POST /pipelines/:id/start`)

By default (unless `--no-start` is set), `provision` and `recreate-pipeline` will also call:

```text
POST /pipelines/demo_pipeline/start
```

## API-only (curl) examples

If you prefer calling the REST endpoints directly:

```bash
curl -sS -X POST http://127.0.0.1:8080/streams \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo_stream","type":"mqtt","props":{"broker_url":"tcp://127.0.0.1:1883","topic":"/demo/data","qos":0},"schema":{"type":"json","props":{"columns":[{"name":"a1","data_type":"string"},{"name":"a2","data_type":"string"},{"name":"a3","data_type":"string"},{"name":"a4","data_type":"string"},{"name":"a5","data_type":"string"}]}},"decoder":{"type":"json","props":{}},"shared":false}'
```

```bash
curl -sS -X POST http://127.0.0.1:8080/pipelines \
  -H 'Content-Type: application/json' \
  -d '{"id":"demo_pipeline","sql":"select a1,a2,a3,a4,a5 from demo_stream","sinks":[{"type":"nop","commonSinkProps":{"batchDuration":100,"batchCount":50}}],"options":{"plan_cache":{"enabled":false},"eventtime":{"enabled":false}}}'
```

```bash
curl -sS -X POST http://127.0.0.1:8080/pipelines/demo_pipeline/start
```

## Troubleshooting

- `HTTP 404` on delete: the tool treats “not found” as OK during cleanup/recreate.
- Connection errors/timeouts: check `--base-url` and increase `--timeout-secs` if needed.
- MQTT data not flowing: verify broker reachability from SynapseFlow, and that `topic`/`qos` match your publisher.
