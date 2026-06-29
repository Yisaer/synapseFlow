# Sink Delivery Encryption

The `PhysicalSinkEncrypt` node encrypts encoded delivery output between the encoder or compression
stage and the connector stage in the sink delivery pipeline.

## Pipeline Position

Without compression:

```text
PhysicalSinkConnector
└─PhysicalSinkEncrypt(algorithm=aes-gcm, key_bits=256, key_id=sink-aes-key)
  └─PhysicalSinkEncoder(encoder=json, ...)
    └─...
```

With compression:

```text
PhysicalSinkConnector
└─PhysicalSinkEncrypt(algorithm=aes-gcm, key_bits=256, key_id=sink-aes-key)
  └─PhysicalSinkCompress(codec=gzip)
    └─PhysicalSinkEncoder(encoder=json, ...)
      └─...
```

The transform order is fixed: encode, then compress when configured, then encrypt. Connectors do not
parse or choose the encryption algorithm; they only receive encrypted bytes.

## Configuration

Encryption is a delivery feature, not a connector-specific property. The config is a discriminated
union keyed on the required `algorithm` field; each algorithm carries only its own parameters. For
`aes-gcm` the only parameter is `key` — a `SecretRef` (store reference or inline literal):

**Recommended — store reference (VF-51):**

```json
{
  "delivery": {
    "encryption": {
      "algorithm": "aes-gcm",
      "key": "store:sink-aes-key"
    }
  }
}
```

`"store:NAME"` resolves `NAME` from the encrypted secret store at config apply time. The pipeline
config keeps only the pointer — the key material is never written into it.

**Inline literal (discouraged):**

```json
{
  "delivery": {
    "encryption": {
      "algorithm": "aes-gcm",
      "key": "BASE64_ENCODED_16_24_OR_32_BYTE_KEY"
    }
  }
}
```

Any `key` value that does **not** start with `store:` is treated as an inline base64 literal. Whether
it is allowed depends on the secret policy (`VELOFLUX_SECRETS_POLICY`, default `warn`): `warn` accepts
it and logs a warning; `strict` rejects config apply. Inline material is still a secret and lands in
scannable pipeline config — prefer `store:NAME`.

### Field reference

- `algorithm` (required) — selects the suite. Only `aes-gcm` is supported today. The config is a
  discriminated union: one config selects exactly one suite, each suite carries only its own
  parameters, and fields not belonging to the selected algorithm are rejected. Future suites (e.g.
  `chacha20-poly1305`, or an `aes-cbc-hmac` that adds its own `mac_key`) add a variant without
  affecting `aes-gcm`. The IV/nonce is never a config field — it is generated per delivery and
  embedded in the ciphertext header.

- `key` (required for `aes-gcm`) — `store:NAME` reference or inline base64 literal. The stored/inline
  value is **base64-encoded key bytes**; the decoded length selects the AES-GCM suite:

  | Key length | Suite |
  |------------|-------|
  | 16 bytes | AES-128-GCM |
  | 24 bytes | AES-192-GCM |
  | 32 bytes | AES-256-GCM |

- `key_id` is **not** a config field. It is derived from the store name (`sink-aes-key` above) — or
  `inline` for an inline key — and recorded in the ciphertext stream header / EXPLAIN as a non-secret
  identifier.

### Creating the stored key

The store lives at `<data-dir>/secrets.enc` (the same `--data-dir` the server uses; default `./tmp`).
Create the entry with the local CLI — the value is read from stdin/prompt/`--from-file`, never argv —
then reference it by name:

```bash
# generate a 32-byte AES-256 key, base64-encode it, and store it under `sink-aes-key`:
head -c 32 /dev/urandom | base64 | veloflux secrets set sink-aes-key --data-dir ./tmp
```

```json
{ "delivery": { "encryption": { "algorithm": "aes-gcm", "key": "store:sink-aes-key" } } }
```

The store is encrypted with a root key from `VELOFLUX_SECRETS_KEY` (base64 32-byte key); without it a
built-in key keeps secrets out of static scanners but is not confidential against someone holding the
binary. See `docs/runtime/sources/shared_mqtt_client.md` for the full store/root-key model.

Resolved key values are decoded during configuration conversion and are not retained by the runtime
encryption config. Decoded key bytes are held in zeroizing memory and dropped with the writer.
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
