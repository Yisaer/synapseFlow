# Video Sink

## Overview

The video sink records video frame tuples to an external video output. It is a direct
`Collection` / `Tuple` sink and does not use a veloFlux encoder stage.

Implementation:
- `src/flow/src/connector/sink/video.rs`
- `src/flow/src/codec/video.rs`

The GStreamer-backed runtime is compiled only when the `video_gstreamer` feature is enabled. When
that feature is not enabled, the video sink connector reports that the backend is unavailable.

## Input Contract

The video sink consumes materialized tuple data from its upstream flow branch. It does not read
hidden source metadata or bypass the final SQL output shape.

The sink input must contain the complete video tuple:

| Column | Required Type |
| --- | --- |
| `payload` | `bytes` |
| `width` | integer |
| `height` | integer |
| `format` | `string` |
| `timestamp` | `timestamp` |

`payload` is raw frame bytes. `width`, `height`, and `format` are required so the sink can
construct video caps before writing encoded output. `timestamp` is part of the required video tuple
contract, but the current GStreamer-backed sink writes output timing from the live push path rather
than applying tuple timestamps as buffer PTS/DTS/duration. Tuple timestamp driven output timing is
deferred.

Valid examples:

```sql
SELECT * FROM camera
```

```sql
SELECT payload, width, height, format, timestamp FROM camera
```

Invalid example:

```sql
SELECT payload FROM camera
```

This query does not provide the frame metadata required by the video sink.

## Encoder

Video sinks require `encoder=none`.

The sink receives `Collection` payloads directly. A configured JSON or custom encoder would turn
the tuple into bytes before the sink sees it, which is not the video sink contract.

## Planner Validation

The physical planner validates:

- the sink connector is `video`
- the encoder is `none`
- the final sink input contains the required video tuple columns
- each required column has a compatible type
- `output.mode=delta` is not used with video sinks

The validation runs after logical optimization and before physical execution, so it applies to the
actual sink-facing row shape.

## Runtime Output

The current video sink supports file output through the GStreamer backend. The sink configuration
selects:

- target directory
- optional filename prefix
- codec, currently H.264 or H.265
- container, currently MP4
- rolling policy, currently duration-based rolling

The manager validates file-oriented settings before the flow runtime is built:

- `path` must be non-empty
- `filename_prefix` must not contain path separators
- rolling duration must be positive

## Explain Shape

For a video sink branch, the physical plan contains no `PhysicalSinkEncoder`:

```text
PhysicalDataSink(connector=video)
  <- upstream collection plan
```

When the upstream SQL projection is an identity video tuple projection, the logical optimizer can
also eliminate the `Project` node. See
`docs/planner/optimize/logical/eliminate_video_sink_identity_project.md`.
