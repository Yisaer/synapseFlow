# REST API Audit Logs

Manager REST API mutations for versioned resources emit structured audit logs
after the manager has a resource revision to report. Audit logs do not include
resource specs, connector properties, schema payloads, WASM bytes, or other
request body content.

Each audit event uses the same semantic fields:

| Field | Description |
| --- | --- |
| `kind` | Versioned resource kind. |
| `name` | Resource identity, such as a pipeline id or stream name. |
| `action` | REST mutation action. |
| `revision` | Resource revision associated with the action. |

The first version covers these REST mutation actions:

| `kind` | `action` values |
| --- | --- |
| `pipeline` | `create`, `update`, `delete`, `start`, `stop` |
| `stream` | `create`, `update`, `delete` |
| `schema` | `create`, `delete` |
| `memory_topic` | `create` |
| `shared_mqtt_client` | `create`, `delete` |
| `table` | `create`, `delete` |
| `udf` | `upload`, `delete` |

Read-only APIs such as list, get, describe, stats, explain, status, metrics,
and export are not audit events in this version.
