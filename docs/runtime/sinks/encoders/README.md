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

The planner binds an `OutputLayout` to each encoder factory. The layout defines
the ordered output columns and their fixed tuple value references. This contract
is independent of encoder kind and template configuration.

Current encoder-local documents:

- [CSV Sink Encoder](csv.md)
- [Encoder Transform](encoder_transform.md)
- [JSON Encoder Options](json_encoder_options.md)
- [JSON Null Field Omission](json_null_column_omit.md)
