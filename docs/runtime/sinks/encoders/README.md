# Sink Encoders

This directory contains sink-encoder behavior and encoder-local capabilities.

## Encoder Capabilities

Each encoder registered in `EncoderRegistry` implements the standard
`SinkEncoder` delivery API:

- `begin_delivery`
- `append`
- `finish_delivery`

This API is streaming by contract. An encoder that cannot emit incrementally may
buffer records in `append` and emit the final payload from `finish_delivery`;
that does not require a separate encoder category.

Registered encoders may declare one additional optimizer capability:

- `supports_by_index_projection` — whether the encoder can perform delayed
  column materialization via `ByIndexProjection`. Used by the
  `ByIndexProjectionIntoEncoderRewrite` optimizer rule.

| Encoder   | supports_by_index_projection |
|-----------|------------------------------|
| json      | true(!transform)             |
| protobuf  | false                        |

Current encoder-local documents:

- [Encoder Transform](encoder_transform.md)
- [JSON Encoder Options](json_encoder_options.md)
- [JSON Null Field Omission](json_null_column_omit.md)
