# History Source Connector

The History source is designed to read EMQ SDV Flow's historical storage data in Parquet format. It enables replay of historical CAN data by reading time-ranged Parquet files stored by the system.

## Overview

This source reads Parquet files that follow a specific naming convention and filters them based on a time range. Files are processed in chronological order, and data is ingested row by row.

## Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `datasource` | string | Yes | - | Directory path containing the Parquet files. |
| `topic` | string | Yes | - | Topic name to filter files (e.g., "can0"). |
| `start` | int64 | No | - | Start timestamp (Unix milliseconds) for filtering. |
| `end` | int64 | No | - | End timestamp (Unix milliseconds) for filtering. |
| `batch_size` | int | No | 100 | Number of rows to read per batch. |
| `send_interval_ms` | int | No | - | Interval between batches in milliseconds to control replay speed. |

## File Naming Convention

The source expects Parquet files to follow this naming pattern:

```text
nanomq_{topic}-{start_ts}~{end_ts}_{seq}_{hash}.parquet
```

Example: `nanomq_can0-1700000000~1700001000_42_abc123.parquet`

## Example Usage

Create a history stream via the HTTP API:

```json
POST /streams
{
  "name": "history_stream",
  "type": "history",
  "props": {
    "datasource": "/var/lib/nanomq/history",
    "topic": "can0",
    "start": 1700000000000,
    "end": 1700010000000,
    "send_interval_ms": 10
  }
}
```
