# Sink Delivery Encryption

The `PhysicalSinkEncrypt` node encrypts encoded delivery output between the encoder or compression
stage and the connector stage in the sink delivery pipeline.

## Pipeline Position

Without compression:

```text
PhysicalSinkConnector
└─PhysicalSinkEncrypt(algorithm=aes-gcm, key_bits=256, key_id=sink-aes-v1)
  └─PhysicalSinkEncoder(encoder=json, ...)
    └─...
```

With compression:

```text
PhysicalSinkConnector
└─PhysicalSinkEncrypt(algorithm=aes-gcm, key_bits=256, key_id=sink-aes-v1)
  └─PhysicalSinkCompress(codec=gzip)
    └─PhysicalSinkEncoder(encoder=json, ...)
      └─...
```

The transform order is fixed: encode, then compress when configured, then encrypt. Connectors do not
parse or choose the encryption algorithm; they only receive encrypted bytes.

## Configuration

Encryption is a delivery feature, not a connector-specific property. The JSON API accepts:

```json
{
  "delivery": {
    "encryption": {
      "algorithm": "aes-gcm",
      "key_id": "sink-aes-v1",
      "key": {
        "value": "BASE64_ENCODED_16_24_OR_32_BYTE_KEY",
        "encoding": "base64"
      }
    }
  }
}
```

`key.encoding` supports `base64` and `hex`. The decoded key length selects the AES-GCM suite:

| Key length | Suite |
|------------|-------|
| 16 bytes | AES-128-GCM |
| 24 bytes | AES-192-GCM |
| 32 bytes | AES-256-GCM |

Inline key material is an MVP mechanism for local tests and controlled environments. It is still
secret even when base64 or hex encoded. Long-term production deployments should use a future secret
reference mechanism rather than storing key material directly in pipeline config.

Inline key values are decoded during configuration conversion and are not retained by the runtime
encryption config. Decoded inline key bytes are held in zeroizing memory and dropped with the writer.
The RustCrypto AES-GCM dependency is built with its `zeroize` feature so cipher key material owned by
the AEAD implementation is also cleared when those values are dropped.

## Encrypted Delivery Format

Each encrypted delivery is a self-describing byte stream:

```text
stream_header:
  magic             4 bytes   "VFE1"
  version           u8        1
  algorithm         u8        1 = aes-gcm-stream-be32
  key_bits          u16be     128, 192, or 256
  key_id_len        u16be
  key_id            bytes     utf-8, non-empty
  salt_len          u8        16
  delivery_salt     bytes     16 random bytes

encrypted_frame:
  ciphertext_len    u32be
  ciphertext        bytes
```

The stream header is cleartext metadata and is authenticated as associated data for every encrypted
frame. The connector writes or publishes the header and frames unchanged.

## Runtime Behavior

- `START` creates a new per-delivery encryption stream and forwards the stream header.
- Middle chunks produce encrypted frames for non-empty input bytes.
- `END` always writes a final encrypted frame, even for empty plaintext.
- `ABORT` discards in-progress encryption state and is forwarded only if downstream has already
  received a `START`.
- A terminal signal during an incomplete delivery aborts the encryption state; the transform does
  not synthesize a complete encrypted delivery.

AES-GCM uses RustCrypto AEAD STREAM with a per-delivery subkey:

```text
master key:    decoded inline key
salt:          16 random bytes per delivery
stream key:    HKDF-SHA256(master key, salt, info="veloflux:sink-encrypt:aes-gcm-stream:v1")
stream nonce:  fixed 7-byte zero nonce prefix
```

The per-delivery subkey prevents nonce reuse under the same AES-GCM key.

## Redaction

The inline key value and decoded key bytes must not appear in explain output, logs, errors, processor
stats, or debug output. Explain output contains only:

- `algorithm`
- resolved `key_bits`
- non-secret `key_id`

## Stats Accounting

- `record_in`: each `EncodedDelivery` event counts as 1.
- `record_out`: only completed deliveries (`END`/`START|END`) count as 1. This matches the
  connector and compression accounting.
