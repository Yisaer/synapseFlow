# HTTP Sink Connector Design

## Background

The HTTP sink delivers final sink-branch encoded payload bytes to a remote HTTP endpoint. Each
delivery unit (one `start_delivery` → `write_chunk*` → `finish_delivery` cycle) is sent as a single
HTTP request with the complete body. The connector accumulates chunks into an in-memory buffer
and flushes them on `finish_delivery`.

Like all byte-delivery connectors, encoder/output behavior upstream of the connector materially
affects what is sent. The HTTP sink requires an encoder (e.g. `json`, `csv`, `protobuf`) and rejects
`encoder.type=none`.

## Goals

- Define the runtime contract of the HTTP sink connector.
- Document HTTP-specific configuration parameters and the common sink retry behavior.
- Explain how encoder and sink-output features interact with HTTP delivery.
- Specify raw and multipart request body behavior.

## Non-Goals

- Generic encoder design beyond the HTTP sink boundary.
- TLS certificate management (default system trust store is used).
- API reference documentation for pipeline create/update.

## Configuration Model

HTTP sink definitions accept:

- `url` (required) — target URL
- Optional `method` (default: `"POST"`) — `GET`, `POST`, `PUT`, `PATCH`, or `DELETE`
- Optional `timeout_secs` (default: `30`) — per-request timeout
- Optional `headers` (default: `{}`) — extra HTTP headers
- Optional `content_type` — explicit `Content-Type`. When omitted, inferred from encoder kind
  (`application/json` for JSON, `text/csv; charset=utf-8` for CSV,
  `application/octet-stream` for protobuf)
- Optional `max_body_size` (default: 64 MiB) — single-delivery body limit
- Optional `body` (default: `{ "type": "raw" }`) — raw or multipart request body mode
- Optional common sink-level `retry` config:
  - `max_attempts` (default: none) — maximum delivery attempts including the first
  - `initial_backoff_ms` (default: `1000`) — initial backoff, doubles each retry
  - `max_backoff_ms` (default: `30000`) — backoff upper bound
  - `jitter` (accepted, not yet applied; current backoff is deterministic)
- encoder config (required)
- sink output config (`full` / `delta`, `omit_if_empty`, encoder transform, batching, common sink
  props)

Manager validates `url` as required and rejects `encoder.type=none`. Legacy HTTP props
`retry_max_attempts`, `retry_backoff_ms`, and `retry_max_backoff_ms` are still accepted as
compatibility input and are converted to the common sink-level retry config.

### Multipart body

Multipart mode uploads the final delivery as one file part and adds zero or more static UTF-8 text
parts:

```json
{
  "type": "http",
  "props": {
    "url": "https://example.com/api/offline/upload",
    "body": {
      "type": "multipart",
      "file_field_name": "d",
      "file_name": "payload.bin",
      "fields": {
        "tp": "1",
        "rid": "cold"
      }
    }
  },
  "encoder": {
    "type": "json"
  }
}
```

| Field | Required | Default | Meaning |
|---|---:|---|---|
| `type` | yes | — | Must be `multipart` |
| `file_field_name` | yes | — | Form field name for the file part |
| `file_name` | no | `payload.bin` | Filename in the part's `Content-Disposition` |
| `fields` | no | `{}` | Static text field name/value pairs |

Field names and the filename are trimmed. Empty names and names containing CR, LF, or NUL are
rejected. A text field cannot use the file field name. Text values are preserved exactly and do
not perform template, environment variable, or property expansion.

The file part media type is always `application/octet-stream`. Multipart mode rejects both
`props.content_type` and a `Content-Type` entry in `props.headers`; reqwest generates the request
boundary and the matching `multipart/form-data; boundary=...` header.

## Delivery Lifecycle

```
start_delivery()
  └─ buffer.clear()                 // reset delivery buffer

write_chunk(bytes)                  // called 0..N times
  └─ buffer.extend(bytes)           // accumulate payload bytes
  └─ Err if buffer > max_body_size  // size guard

finish_delivery()
  └─ raw: send buffer as the complete HTTP body
  └─ multipart: send buffer as one file part with static text parts
     └─ on 2xx: return DeliveryResult
     └─ on 5xx/429: return SinkConnectorError::Transient
     └─ on 4xx (not 429): return SinkConnectorError::Permanent
     └─ on network error: return SinkConnectorError::Transient

abort_delivery()
  └─ buffer.clear()                 // discard partial delivery
```

## Retry Strategy

Retry is implemented by `SinkProcessor`, not by the HTTP connector. The HTTP connector performs one
request per `finish_delivery()` call and classifies failures so the processor can make the retry
decision.

Retryable HTTP failures include:

| Category | Examples | Retried |
|---|---|---|
| Network failures | connection refused, DNS resolution, timeout | ✅ |
| Server errors | 500, 502, 503, 504 | ✅ |
| Rate limiting | 429 Too Many Requests | ✅ |
| Client errors | 400, 401, 403, 404 | ❌ |

Retry uses deterministic exponential backoff:

1. First attempt fails → wait `initial_backoff_ms`
2. Backoff doubles each attempt, capped at `max_backoff_ms`
3. Loop until success or `max_attempts` exhausted

When retry is not configured (`retry.max_attempts` unset), each delivery is attempted exactly once.

## Hot Path Considerations

- `start_delivery` and `write_chunk` are constant-time operations that append to a `Vec<u8>`. No
  I/O is performed.
- `finish_delivery` is the only connector method that performs network I/O. It is called from a
  tokio async context, so other pipeline branches are not blocked.
- The `SinkProcessor` retains the completed encoded payload while retries are pending and replays
  the full delivery on each attempt.
- `max_body_size` is exposed to `SinkProcessor`, so oversized encoded deliveries are rejected while
  being accumulated and before a connector attempt is made.
- In multipart mode, reqwest streams the multipart envelope around the retained delivery bytes. The
  connector does not build a second complete multipart buffer.

## Encoder and Output Feature Interaction

- **Encoder**: The HTTP sink requires an encoder (`json`, `csv`, `protobuf`). `encoder.type=none` is
  rejected because the sink needs serialized bytes to send as an HTTP body.
- **Batching** (`batch_count`, `batch_duration`): Batched rows are encoded into a single delivery
  unit and sent as one HTTP request body.
- **Delta output**: Row-diff output works with compatible encoders. CSV rejects
  `output.mode=delta` because CSV deliveries require stable dense rows.
- **Encoder transform**: Template transforms are supported when the encoder is `json`. The
  transform output forms the HTTP body.
- **Compression / Encryption**: Delivery compression (`gzip`, `zstd`) and encryption (`aes-gcm`) are
  supported as pre-processing steps before the HTTP request is sent. In multipart mode, only the
  file part contains the transformed bytes; the multipart envelope and static fields remain
  untransformed.

## `Content-Type` Inference

When `content_type` is not explicitly configured:

| Encoder type | Inferred `Content-Type` |
|---|---|
| `json` | `application/json` |
| `csv` | `text/csv; charset=utf-8` |
| `protobuf` | `application/octet-stream` |
| Other / custom | (no Content-Type header set) |

The inference happens during physical plan building, before the connector is instantiated.
Explicit `content_type` always takes precedence in raw mode. Multipart mode disables inference and
does not allow an explicit request `Content-Type`.

Compression of the file payload does not automatically add a request-level `Content-Encoding`,
because that header would describe the entire multipart representation. Endpoint-specific headers
can still be configured explicitly.

## Size and Metrics

`max_body_size` limits the encoded delivery payload. In multipart mode this is the file part size;
multipart boundaries, part headers, static fields, and trailing framing are not included.

`DeliveryResult.bytes_written` uses the same payload-byte definition and does not include multipart
envelope overhead.

## Error Handling

- URL validation occurs in `ready()`. Invalid URLs are rejected before the pipeline is started.
- Permanent errors (4xx client errors) are propagated as `SinkConnectorError::Permanent`.
- Transient errors (network errors, 5xx, 429) are propagated as `SinkConnectorError::Transient`.
- Connector errors are classified for logs and metrics. The processor retries every connector
  delivery error until `max_attempts` is exhausted.
- Exhausted retries are logged, then the delivery is dropped and processing continues.

## Testing

- **Unit tests** (`connector/sink/http.rs`): config defaults, builder methods, content-type
  inference, status classification, and structurally parsed multipart requests.
- **E2E tests** (`tests/e2e/http_sink_e2e.rs`): full pipeline with mock stream source → JSON
  decoder → raw and multipart HTTP sinks → embedded axum test server. Verifies raw compatibility
  and that the complete JSON encoder output is delivered in the configured file part.
