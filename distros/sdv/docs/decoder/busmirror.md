# BusMirror Decoder

The `busmirror` decoder reads AUTOSAR Bus Mirroring v1 destination frames and
decodes CAN, CAN FD, and LIN payloads through the DBC files compiled with the
stream's BusMirror schema.

## Install the schema

Create a ZIP containing the JSON entry and its same-stem companion directory as
described in [BusMirror Schema](../schema/busmirror.md), then install it:

```http
POST /schemas
Content-Type: application/json
```

```json
{
  "name": "vehicle_busmirror",
  "type": "busmirror",
  "props": {
    "schema_path": "/opt/schema-source/vehicle-busmirror.zip"
  }
}
```

## Create a stream

The stream references the installed schema. Decoder props do not repeat DBC
paths, network topology, signal naming, or format options:

```json
{
  "name": "vehicle_input",
  "type": "mqtt",
  "schema": { "ref": "vehicle_busmirror" },
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "vehicle/busmirror",
    "qos": 0
  },
  "decoder": { "type": "busmirror", "props": {} }
}
```

Each valid destination frame produces one row. A source payload may concatenate
multiple destination frames and therefore produce multiple rows. The `ts` column
is the destination-frame header timestamp converted to milliseconds. A valid
empty or state-only destination frame still produces a timestamp row with null
signal columns.

Malformed packets with a known outer boundary are discarded without discarding
valid rows before or after them. A truncated outer header or length stops scanning
the remaining input. The decoder returns an error only if the input produces no
valid destination-frame row.

## Fused packer merger

Use the matching `busmirror` merger to accumulate latest DBC frames within a
sampling window:

```json
{
  "sampler": {
    "interval": "500ms",
    "strategy": {
      "type": "packer",
      "props": {
        "merger": { "type": "busmirror", "props": {} }
      }
    }
  }
}
```

The merger must be paired with `decoder.type = "busmirror"`. Repeated messages
use last-wins semantics; multiplexed messages use `(frame identity, mux value)`
as the key. One trigger emits at most one decoded row using the last valid packet
timestamp, then clears the window. The GBF and BusMirror mergers share the same
DBC window accumulator, projection, multiplexing, and row materialization code.
