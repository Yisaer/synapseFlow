# StreamingEncoderRewrite

## Overview

`StreamingEncoderRewrite` removes a `Batch -> Encoder` sink chain when the configured encoder
supports incremental streaming, by rewriting it into a single `StreamingEncoder` stage.

The goal is to avoid building intermediate `RecordBatch`es that are only used as an input to the
encoder, and to let the encoder stream rows incrementally while still honoring sink batching
settings (count / duration).

Implementation: `src/flow/src/planner/optimizer.rs` (`StreamingEncoderRewrite`).

## Inputs (What It Recognizes)

This rule matches `PhysicalPlan::DataSink(sink)` where the sink’s (first) child is:

```
Encoder
  -> Batch
    -> upstream
```

I.e.:

- `PhysicalPlan::DataSink`
  - child `PhysicalPlan::Encoder`
    - child `PhysicalPlan::Batch`

## Preconditions (When It Is Safe)

The rewrite is applied only when the encoder kind reports streaming support:

- `encoder_registry.supports_streaming(encoder.encoder.kind())`

If streaming is not supported, the plan is left unchanged.

## Outputs (What It Produces)

When the match and preconditions hold, the rule rewrites:

```
Batch -> Encoder -> DataSink
```

into:

```
StreamingEncoder -> DataSink
```

Details:

- `PhysicalStreamingEncoder` keeps the encoder node’s plan index, and inherits:
  - `sink_id`, `encoder` config
  - sink batching config (`batch.common`)
- The `PhysicalBatch` node is dropped; the streaming encoder’s child becomes the batch’s upstream.
- `PhysicalSinkConnector.encoder_plan_index` is updated to the streaming encoder’s plan index to
  preserve connector-to-encoder binding.

## Runtime Mapping

`PhysicalStreamingEncoder` is executed by `StreamingEncoderProcessor`
(`src/flow/src/processor/streaming_encoder_processor.rs`). It uses `CollectionEncoderStream` to:

- append tuples incrementally (`stream.append(tuple)`)
- flush by batch count / duration
- flush on terminal control signals (e.g. StreamEnd)

## Explain / Tests

The rewrite is covered by table-driven planner explain tests in:

- `src/flow/tests/planner/physical/plan_explain_table_driven.rs`

Example case: `optimize_rewrites_batch_encoder_chain_to_streaming_encoder`.

## Limitations

- Only rewrites when the `Encoder -> Batch` chain is a direct child of `PhysicalDataSink`.
- Only checks the sink’s first child (sinks are expected to be unary in the current physical
  plan).

