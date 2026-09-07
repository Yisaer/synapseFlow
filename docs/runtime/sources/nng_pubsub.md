# NNG Pub/Sub Source

The `nng_pubsub` source consumes bytes from an NNG `pub0` peer through a local `sub0`
socket. It is intended for SDV deployments that already use the Mande NNG pub/sub
framing contract.

## Configuration

Runtime stream properties:

| Field | Required | Default | Description |
|---|---:|---|---|
| `url` | yes | none | NNG endpoint to dial. Supported schemes: `tcp://`, `ipc://`, `inproc://`. |
| `topic` | no | `""` | NNG byte-prefix subscription. Empty means subscribe to all messages. |
| `topic_delimiter` | no | `":"` | Application-level separator between topic bytes and payload bytes. |

NNG pub/sub does not carry a separate topic field. Topic matching is byte-prefix matching
against the message body, and the matched topic bytes remain in the received message. The
source strips the application-level frame before forwarding bytes to the decoder.

Frame format:

```text
[topic bytes][topic_delimiter bytes][payload bytes]
```

If `topic_delimiter` is empty, the source strips exactly `len(topic)` bytes. This only works
when `topic` is the full exact prefix. With the default delimiter `":"`, the source strips
through the first delimiter occurrence and preserves any later delimiter bytes inside the payload.

## Lifecycle

The source follows normal VeloFlux connector behavior. Starting a pipeline does not wait
indefinitely for the external peer. The connector worker attempts to connect in the background,
emits connector errors on connection failures, and retries with bounded backoff until the
pipeline stops.

NNG pub/sub is best-effort and lossy. Messages published before the subscriber connects or while
subscriber queues overflow can be dropped by the protocol.

## Embedded `inproc` deployments

When veloFlux is embedded as a shared library, the same connector can use an
`inproc://` URL to communicate with an NNG peer owned by the host process. The
URL scheme is the only transport selector; do not add a separate `inproc` or
`transport` property to the stream request.

The host must create and listen on its `pub0` socket before starting the
embedded veloFlux runtime. The source uses an NNG `sub0` socket and dials the
configured URL. The source may retry while the host peer is starting, but the
host should wait for pipeline readiness before publishing data because NNG
pub/sub does not acknowledge delivery and early messages may be lost.

The complete embedded design, including the Manager boundary, transport
parsing, connection retry behavior, and shared `libnng` requirement, is documented in
[NNG In-Process Integration](../../integrations/ffi/nng_inproc.md).
