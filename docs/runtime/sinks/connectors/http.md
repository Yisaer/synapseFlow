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
  (`application/json` for JSON, `text/csv; charset=utf-8` for CSV,
  `application/octet-stream` for protobuf)
- Optional `max_body_size` (default: 64 MiB) — single-delivery body limit
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
  supported as pre-processing steps before the HTTP request is sent.

## `Content-Type` Inference

When `content_type` is not explicitly configured:

| Encoder type | Inferred `Content-Type` |
|---|---|
| `json` | `application/json` |
| `csv` | `text/csv; charset=utf-8` |
| `protobuf` | `application/octet-stream` |
| Other / custom | (no Content-Type header set) |

The inference happens during physical plan building, before the connector is instantiated.
Explicit `content_type` always takes precedence.

## Error Handling

- URL validation occurs in `ready()`. Invalid URLs are rejected before the pipeline is started.
- Permanent errors (4xx client errors) are propagated as `SinkConnectorError::Permanent`.
- Transient errors (network errors, 5xx, 429) are propagated as `SinkConnectorError::Transient`.
- Connector errors are classified for logs and metrics. The processor retries every connector
  delivery error until `max_attempts` is exhausted.
- Exhausted retries are logged, then the delivery is dropped and processing continues.

## Testing

- **Unit tests** (`connector/sink/http.rs`): config defaults, builder methods, content-type
  inference, and status classification.
- **E2E tests** (`tests/e2e/http_sink_e2e.rs`): full pipeline with mock stream source → JSON
  decoder → HTTP sink → embedded axum test server. Verifies correct method, Content-Type, and
  JSON body delivery.
