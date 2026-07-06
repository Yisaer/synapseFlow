# GBF Decoder

The GBF (General Binary Format) decoder converts binary packets into structured data.
GBF operates at two independent layers:

| Layer | Responsibility | Input | Output |
|-------|---------------|-------|--------|
| **Transport** | Parse raw bytes into packets and frames, extracting `{ timestamp, format_id, payload }` | MQTT binary message | `GbfPayloadFrame { timestamp, format_id, payload }` |
| **Signal** | Decode payload bytes into named signal values | `(format_id, payload_bytes)` | `{ "Mess0$Sig1": 1, "BswAppVersion": ... }` |

The two layers are **independent** and **composable**. The bridge between them is
`format.id_ref` in the GBF JSON transport schema, which marks which frame field
carries the message ID.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         GBF Decoder Architecture                         │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  MQTT binary data                                                        │
│       │                                                                  │
│       ▼                                                                  │
│  GbfParser (reads GBF JSON transport schema)                             │
│       │                                                                  │
│       ▼                                                                  │
│  Vec<GbfPayloadFrame { timestamp, format_id, payload }>                  │
│       │                                                                  │
│       ▼                                                                  │
│  PayloadDecoder (selected by format_type)                                │
│    ├── format_type = "can"     → CanPayloadDecoder (DBC JSON)            │
│    └── format_type = "someip"  → SomeIpPayloadDecoder (ARXML)            │
│       │                                                                  │
│       ▼                                                                  │
│  RecordBatch                                                             │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### Configuration

```json
{
  "type": "gbf",
  "props": {
    "schema_path": "/path/to/transport.json",
    "format_type": "can",
    "format_schema_path": "/path/to/signals.dbc"
  }
}
```

| Property | Required | Description |
|----------|----------|-------------|
| `schema_path` | Yes | GBF JSON transport schema. Describes packet/frame layout and how to extract the message ID and payload bytes. |
| `format_type` | Yes | Payload decoder type. `"can"` for DBC-based CAN signals. `"someip"` for ARXML/SOME-IP signals. |
| `format_schema_path` | Yes | Path to the format schema for payload decoding. For `"can"`, a DBC JSON file. For `"someip"`, an ARXML file. Ignored when `format_schema_ref` is set. |
| `format_schema_ref` | No | Name of a pre-registered ARXML schema resource. When set, `format_schema_path` is ignored and the decoder reuses the cached ARXML metadata. |

The same GBF transport schema model is used for both CAN and SOME/IP.
The only difference is the payload decoder selected by `format_type`.

---

## CAN Configuration (existing)

```json
{
  "type": "gbf",
  "props": {
    "schema_path": "/etc/veloflux/schemas/gbf/can_packet.json",
    "format_type": "can",
    "format_schema_path": "/etc/veloflux/schemas/can/signals.dbc",
    "can_id_mapping": "raw"
  }
}
```

Runtime flow:

```
GBF bytes
  → GbfParser extracts (ts, can_id, payload)
  → format_type = "can"
  → CanDecoder looks up DBC message by can_id
  → output row columns
```

---

## SOME/IP Configuration

For SOME/IP signal decoding via `arxml_converter_rs`, the ARXML signal definitions
must be registered as a named schema resource before a stream can reference them.

### Step 1: Register the ARXML schema

```http
POST /schemas
Content-Type: application/json
```

```json
{
  "name": "vehicle_someip_arxml",
  "type": "arxml",
  "props": {
    "schema_path": "/etc/veloflux/schemas/arxml/system.arxml"
  }
}
```

This parses the ARXML with `arxml_converter_rs`, extracts all service interfaces,
methods/events, and payload fields, producing a VeloFlux schema. The parsed ARXML
metadata is cached under the name `"vehicle_someip_arxml"` for reuse.

By default, raw ARXML field names are used as column names (matching eKuiper's
behavior). To prefix with service and entry names, add `signal_name_pattern`:

```json
{
  "name": "vehicle_someip_arxml",
  "type": "arxml",
  "props": {
    "schema_path": "/etc/veloflux/schemas/arxml/system.arxml",
    "signal_name_pattern": "{service}.{method}.{field}"
  }
}
```

Placeholders: `{service}` (service interface name), `{method}` or `{event}`
(synonyms for the method/event entry name), and `{field}` (payload field name).

Example output columns (with pattern `{service}.{method}.{field}`):

```
ts
VehicleSpeedService.GetSpeed.speed
VehicleSpeedService.GetSpeed.quality
PowerModeService.GetPowerMode.mode
```

### Step 2: Define the GBF transport schema

The GBF JSON transport schema describes the packet layout. It is a separate file
from the ARXML signal schema and follows the same format as CAN transport schemas.

`/etc/veloflux/schemas/gbf/someip_packet.json`:

```json
{
  "structure": {
    "type": "struct",
    "fields": [
      { "name": "ts", "type": "u64be" },
      { "name": "total_len", "type": "u16be" },
      {
        "name": "frames",
        "type": "sequence",
        "length_ref": "total_len",
        "length_unit": "bytes",
        "structure": {
          "type": "struct",
          "fields": [
            { "name": "message_id", "type": "u32be" },
            { "name": "data_len", "type": "u16be" },
            {
              "name": "payload",
              "type": "bytes",
              "length_ref": "data_len",
              "format": { "id_ref": "message_id" }
            }
          ]
        }
      }
    ]
  }
}
```

The `message_id` field carries the SOME/IP message ID as a single `u32`:

```
message_id = (service_id << 16) | method_id
```

The GBF parser does not need to know this is SOME/IP. It only extracts:

```
(ts, format_id = 0x1234_0007, payload)
```

### Step 3: Create the stream

#### Using inline ARXML schema props

```http
POST /streams
Content-Type: application/json
```

```json
{
  "name": "someip_stream",
  "type": "mqtt",
  "schema": {
    "type": "arxml",
    "props": {
      "schema_path": "/etc/veloflux/schemas/arxml/system.arxml"
    }
  },
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "vehicle/someip",
    "qos": 0
  },
  "decoder": {
    "type": "gbf",
    "props": {
      "schema_path": "/etc/veloflux/schemas/gbf/someip_packet.json",
      "format_type": "someip",
      "format_schema_path": "/etc/veloflux/schemas/arxml/system.arxml"
    }
  }
}
```

By default, raw ARXML field names are used as column names (e.g., `firstSlot`, `tplen`).
To prefix with service and entry names, add `signal_name_pattern` to the schema props.

#### Using a named ARXML schema reference

If the ARXML schema was pre-registered via `POST /schemas` (Step 1):

```json
{
  "name": "someip_stream",
  "type": "mqtt",
  "schema": {
    "ref": "vehicle_someip_arxml"
  },
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "vehicle/someip",
    "qos": 0
  },
  "decoder": {
    "type": "gbf",
    "props": {
      "schema_path": "/etc/veloflux/schemas/gbf/someip_packet.json",
      "format_type": "someip",
      "format_schema_ref": "vehicle_someip_arxml"
    }
  }
}
```

`format_schema_ref` tells the decoder to reuse the already-parsed ARXML metadata
from the named schema instead of reparsing the ARXML file.
Column naming (including any `signal_name_pattern`) is inherited from the named
schema resource.

### Runtime decode flow

```
GBF packet bytes
  → GbfParser parses transport schema
  → extracts (timestamp = 1731316891295, message_id = 0x1234_0007, payload)
  → service_id = message_id >> 16    = 0x1234
  → method_id  = message_id & 0xffff = 0x0007
  → lookup SomeIpMessageDecodePlan(0x1234, 0x0007)
  → decode required fields into output slots
  → RecordBatch
```

---

## GBF Transport Schema

### Supported Types

| Type | Size | Description |
|------|------|-------------|
| `u8` | 1 byte | Unsigned 8-bit integer |
| `u16be` | 2 bytes | Unsigned 16-bit big-endian |
| `u16le` | 2 bytes | Unsigned 16-bit little-endian |
| `u32be` | 4 bytes | Unsigned 32-bit big-endian |
| `u32le` | 4 bytes | Unsigned 32-bit little-endian |
| `u64be` | 8 bytes | Unsigned 64-bit big-endian |
| `u64le` | 8 bytes | Unsigned 64-bit little-endian |
| `bytes` | Variable | Raw byte payload |
| `sequence` | Variable | Array of typed items |

### Field Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Field name for reference |
| `type` | String | One of the supported types |
| `const` | u64 | Constant value constraint (e.g., magic bytes) |
| `length_ref` | String | Reference to field containing length |
| `length_unit` | String | `"bytes"` for sequences (only supported value) |
| `structure` | Object | Inline struct definition for sequence items |
| `format` | Object | Marks field as embedded payload (see below) |
| `read_mask` | u64 | Bit mask applied after reading |
| `read_shift` | u32 | Bit shift applied after masking |

### Embedded Payload

Fields with a `format` object are treated as **embedded payloads**. The format
type is determined by the decoder configuration, not the schema:

```json
{
  "name": "payload",
  "type": "bytes",
  "length_ref": "data_len",
  "format": {
    "id_ref": "can_id"
  }
}
```

- `id_ref`: **Required.** The name of the sibling field that carries the message ID.
  The parser uses this — and only this — to identify the ID field; no field-naming
  conventions are applied.

> **Constraint:** The frame item struct must contain **exactly one** `bytes` field
> with a `format` object, and that field must include `format.id_ref`. The parser
> rejects schemas that have zero or more than one such field at load time.

---

## Example Transport Schema (CAN)

```json
{
  "structure": {
    "type": "struct",
    "fields": [
      { "name": "ts", "type": "u64be" },
      { "name": "total_len", "type": "u16be" },
      {
        "name": "frames",
        "type": "sequence",
        "structure": {
          "type": "struct",
          "fields": [
            { "name": "magic", "type": "u8", "const": 85 },
            { "name": "can_id", "type": "u16be" },
            { "name": "data_len", "type": "u8" },
            {
              "name": "payload",
              "type": "bytes",
              "length_ref": "data_len",
              "format": { "id_ref": "can_id" }
            }
          ]
        },
        "length_ref": "total_len",
        "length_unit": "bytes"
      }
    ]
  }
}
```

---

## Edge Cases

### Frame Alignment Recovery

If a frame's magic byte doesn't match, the decoder scans forward byte-by-byte
until it finds the next valid magic byte (`0x55`), enabling recovery from
corrupted or padded data.

### Bit-Field Extraction

Use `read_mask` and `read_shift` for packed fields:

```json
{
  "name": "data_len",
  "type": "u8",
  "read_mask": 240,
  "read_shift": 4
}
```

This extracts the high nibble: `(value & 0xF0) >> 4`

### Padding

Zero bytes (`0x00`) between frames are automatically skipped.

---

## Signal Value Types

| Condition | Output Type |
|-----------|-------------|
| `factor = 1.0` AND `offset = 0.0` | `Int64` |
| Factor and offset are integers | `Int64` |
| Fractional factor or offset | `Float64` |

## Bit Ordering

- **Little-endian (Intel)**: `is_big_endian = false` – LSB first
- **Big-endian (Motorola)**: `is_big_endian = true` – MSB first

---

## Error Handling

### Errors (decode returns error)

| Condition | Behavior |
|-----------|----------|
| Empty payload | Returns error: "no packets found" |
| Payload smaller than header | Returns error: "no packets found" |
| No valid frames in any packet | Returns error: "no valid frames decoded" |

### Graceful Recovery

| Condition | Behavior |
|-----------|----------|
| Invalid magic byte | Skips to next byte, continues scanning |
| Truncated frame (payload shorter than `data_len`) | Stops processing current packet, uses available frames |
| Frame without matching signal definition | Signal values set to `Null`, other signals unaffected |
| Packet with zero frames (heartbeat) | Creates dummy frame with timestamp, returns row with all nulls |
| SOME/IP frame with unknown `(service_id, method_id)` | Skip that frame, leave schema columns as `NULL` |
