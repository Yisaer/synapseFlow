# Video Source

## Overview

Video sources ingest live video frames and expose them to the flow runtime as normal
`Collection` / `Tuple` data. The source connector owns the external video input, while the flow
module continues to evaluate SQL over tuple columns.

Implementation:
- `src/flow/src/connector/source/video.rs`
- `src/flow/src/codec/video.rs`

The GStreamer-backed runtime is compiled only when the `video_gstreamer` feature is enabled. When
that feature is not enabled, the video source connector reports that the backend is unavailable.

## Schema

Users do not provide a schema for video streams. The manager and flow layers use a fixed schema:

| Column | Type | Meaning |
| --- | --- | --- |
| `payload` | `bytes` | Raw video frame bytes. |
| `width` | `int64` | Frame width in pixels. |
| `height` | `int64` | Frame height in pixels. |
| `format` | `string` | Raw frame pixel format. |
| `timestamp` | `timestamp` | Frame timestamp used by downstream tuple processing. |

`payload` is raw frame payload, not an encoded video packet, container segment, or compressed file
fragment. Downstream video sinks need the metadata columns to reconstruct caps and timing for
encoding.

## Decoder

Video streams use `decoder=none`.

The video source connector already emits decoded `Collection` payloads with the fixed video tuple
schema. There is no separate bytes-to-row decoder stage for video streams.

## Runtime Shape

A video source event is delivered as:

```text
ConnectorEvent::Collection
  -> RecordBatch
    -> Tuple
      -> Message(source=camera, columns=[payload, width, height, format, timestamp])
```

Because video data follows the same tuple model as other sources, normal SQL operators such as
`Filter` and `Project` can read the video columns. For example:

```sql
SELECT payload, width FROM camera WHERE width > 0
```

This query is a normal tuple computation and may be sent to a regular encoded sink such as JSON.

## Configuration Constraints

Current video streams are standalone connector-backed sources:

- shared stream mode is not supported for video streams
- eventtime configuration is not supported for video streams
- sampler configuration is not supported for video streams
- decoder configuration must be `none`

RTSP and HLS URLs are accepted by the video source configuration. RTSP transport and reconnect
settings are normalized by the manager before reaching the flow runtime.

## Manager API Example

Create a video stream. The request does not include a user schema because video streams use the
fixed schema described above.

```http
POST /streams
Content-Type: application/json
```

```json
{
  "name": "camera",
  "revision": 1,
  "type": "video",
  "props": {
    "url": "rtsp://127.0.0.1:8554/camera",
    "rtsp_transport": "tcp",
    "reconnect": {
      "enabled": true,
      "initial_delay_ms": 1000,
      "max_delay_ms": 10000
    }
  },
  "decoder": {
    "type": "none",
    "props": {}
  }
}
```

Create a pipeline that records the video stream to rolling MP4 files through a video sink.

```http
POST /pipelines
Content-Type: application/json
```

```json
{
  "id": "camera_recording",
  "revision": 1,
  "sql": "SELECT * FROM camera",
  "sources": [
    {
      "stream": "camera"
    }
  ],
  "sinks": [
    {
      "id": "video_sink",
      "type": "video",
      "props": {
        "path": "/tmp/veloflux-video",
        "filename_prefix": "camera",
        "codec": "h264",
        "container": "mp4",
        "rolling": {
          "type": "duration",
          "seconds": 10
        }
      },
      "encoder": {
        "type": "none",
        "props": {}
      },
      "output": {
        "mode": "full"
      }
    }
  ]
}
```

Start the pipeline after it is created.

```http
POST /pipelines/camera_recording/start
```

The SQL uses `SELECT *` so the sink receives the complete video tuple:
`payload`, `width`, `height`, `format`, and `timestamp`.
