# Sink Encoders

This directory contains sink-encoder behavior and encoder-local capabilities.

## Encoder Capabilities

Each encoder registered in `EncoderRegistry` declares two boolean capabilities:

- `supports_by_index_projection` — whether the encoder can perform delayed
  column materialization via `ByIndexProjection`. Used by the
  `ByIndexProjectionIntoEncoderRewrite` optimizer rule.
- `supports_streaming` — whether the encoder can accept records one at a time
  (streaming delivery). Used by the `StreamingEncoderRewrite` optimizer rule
  to decide whether to fuse `PhysicalBatch → PhysicalSinkEncoder` into
  `PhysicalIncSinkEncoder`.

| Encoder   | supports_by_index_projection | supports_streaming |
|-----------|------------------------------|--------------------|
| json      | true(!transform)             | true               |
| protobuf  | false                        | true (expected)    |
| Parquet   | false                        | false              |

Current encoder-local documents:

- [Encoder Transform](encoder_transform.md)
- [JSON Encoder Options](json_encoder_options.md)
- [JSON Null Field Omission](json_null_column_omit.md)
