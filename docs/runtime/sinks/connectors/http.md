# HTTP Sink Connector Design

## Background

The HTTP sink delivers final sink-branch encoded payload bytes to a remote HTTP endpoint. Each
delivery unit (one `start_delivery` → `write_chunk*` → `finish_delivery` cycle) is sent as a single
HTTP request with the complete body. The connector accumulates chunks into an in-memory buffer
and flushes them on `finish_delivery`.

Like all byte-delivery connectors, encoder/output behavior upstream of the connector materially
affects what is sent. The HTTP sink requires an encoder (e.g. `json`, `protobuf`) and rejects
`encoder.type=none`.

## Goals

- Define the runtime contract of the HTTP sink connector.
- Document configuration parameters including retry behavior.
- Explain how encoder and sink-output features interact with HTTP delivery.
- Specify `Content-Type` inference rules.

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
  (`application/json` for JSON, `application/octet-stream` for protobuf)
- Optional `max_body_size` (default: 64 MiB) — single-delivery body limit
- Optional `retry_max_attempts` (default: none) — maximum delivery attempts including the first
- Optional `retry_backoff_ms` (default: `1000`) — initial backoff, doubles each retry
- Optional `retry_max_backoff_ms` (default: `30000`) — backoff upper bound
- encoder config (required)
- sink output config (`full` / `delta`, `omit_if_empty`, encoder transform, batching, common sink
  props)

Manager validates `url` as required and rejects `encoder.type=none`.

## Delivery Lifecycle

```
start_delivery()
  └─ buffer.clear()                 // reset delivery buffer

write_chunk(bytes)                  // called 0..N times
  └─ buffer.extend(bytes)           // accumulate payload bytes
  └─ Err if buffer > max_body_size  // size guard

finish_delivery()
  └─ send HTTP request with buffer as body
     └─ on 2xx: return DeliveryResult
     └─ on 5xx/429: retry (if configured) with exponential backoff + jitter
     └─ on 4xx (not 429): return error immediately (no retry)
     └─ on network error: retry (if configured)

abort_delivery()
  └─ buffer.clear()                 // discard partial delivery
```

## Retry Strategy

The HTTP sink supports configurable retry for transient failures. Retryable errors include:

| Category | Examples | Retried |
|---|---|---|
| Network failures | connection refused, DNS resolution, timeout | ✅ |
| Server errors | 500, 502, 503, 504 | ✅ |
| Rate limiting | 429 Too Many Requests | ✅ |
| Client errors | 400, 401, 403, 404 | ❌ |

Retry uses **exponential backoff with random jitter** (±25% of current backoff):

1. First attempt fails → wait `backoff_ms + jitter` ms
2. Backoff doubles each attempt, capped at `max_backoff_ms`
3. Loop until success or `max_attempts` exhausted

When retry is not configured (`retry_max_attempts` unset), each delivery is attempted exactly once.

## Hot Path Considerations

- `start_delivery` and `write_chunk` are constant-time operations that append to a `Vec<u8>`. No
  I/O is performed.
- `finish_delivery` is the only method that performs network I/O. It is called from a tokio async
  context, so other pipeline branches are not blocked.
- The body buffer is converted to `bytes::Bytes` (ref-counted) before the retry loop, making retry
  clones nearly free.
- A `max_body_size` guard prevents unbounded memory growth from large or long-lived deliveries.

## Encoder and Output Feature Interaction

- **Encoder**: The HTTP sink requires an encoder (`json`, `protobuf`). `encoder.type=none` is
  rejected because the sink needs serialized bytes to send as an HTTP body.
- **Batching** (`batch_count`, `batch_duration`): Batched rows are encoded into a single delivery
  unit and sent as one HTTP request body.
- **Delta output**: Row-diff output works with the HTTP sink in the same way as other byte-delivery
  sinks. The encoder receives the row-diff output and serializes it according to its output schema.
- **Encoder transform**: Template transforms are supported when the encoder is `json`. The
  transform output forms the HTTP body.
- **Compression / Encryption**: Delivery compression (`gzip`, `zstd`) and encryption (`aes-gcm`) are
  supported as pre-processing steps before the HTTP request is sent.

## `Content-Type` Inference

When `content_type` is not explicitly configured:

| Encoder type | Inferred `Content-Type` |
|---|---|
| `json` | `application/json` |
| `protobuf` | `application/octet-stream` |
| Other / custom | (no Content-Type header set) |

The inference happens during physical plan building, before the connector is instantiated.
Explicit `content_type` always takes precedence.

## Error Handling

- URL validation occurs in `ready()`. Invalid URLs are rejected before the pipeline is started.
- Non-retryable errors (4xx client errors) are propagated as `SinkConnectorError` and logged.
- Retryable errors generate `WARN`-level log entries for each retry attempt, including attempt
  number and backoff duration.
- Exhausted retries produce an error containing the total attempt count.

## Testing

- **Unit tests** (`connector/sink/http.rs`): config defaults, builder methods, content-type
  inference, retry status classification, retryable error detection.
- **E2E tests** (`tests/e2e/http_sink_e2e.rs`): full pipeline with mock stream source → JSON
  decoder → HTTP sink → embedded axum test server. Verifies correct method, Content-Type, and
  JSON body delivery.
