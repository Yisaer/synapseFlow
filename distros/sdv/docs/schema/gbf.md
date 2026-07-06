# GBF Schema

GBF uses two layers of schema:

| Layer | Schema type | Responsible for |
|-------|------------|-----------------|
| **Transport** | GBF JSON schema (`.json`) | Parsing raw bytes into packets and frames |
| **Signal** | DBC JSON or ARXML | Decoding payload bytes into named signal values |

---

## CAN: DBC Schema

For CAN, the signal schema is loaded from a DBC JSON file via `schema.type = "dbc"`.

```json
{
  "schema": {
    "type": "dbc",
    "props": {
      "schema_path": "/etc/veloflux/schemas/can/signals.dbc"
    }
  }
}
```

See [DBC Schema](dbc.md) for details on the DBC JSON format and signal column naming.

---

## SOME/IP: ARXML Schema

For SOME/IP, the signal schema is parsed from an AUTOSAR ARXML file via
`arxml_converter_rs`. ARXML schemas can be used **inline** (via `schema.type = "arxml"`)
or **by reference** (via `schema.ref` pointing to a pre-registered named schema).

### Schema registration flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     ARXML Schema Resolution                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  POST /schemas  或  stream schema.props                         │
│       │                                                         │
│       ▼                                                         │
│  arxml_converter_rs::ArxmlCodec::load("system.arxml")           │
│       │                                                         │
│       ▼                                                         │
│  Extract service interfaces, methods/events, payload fields     │
│       │                                                         │
│       ▼                                                         │
│  Schema {                                                       │
│    ts: Int64,                                                   │
│    "VehicleSpeedService.GetSpeed.speed": Float64,              │
│    "VehicleSpeedService.GetSpeed.quality": Uint8,              │
│    "PowerModeService.GetPowerMode.mode": Uint8,                │
│    ...                                                          │
│  }                                                              │
│                                                                 │
│  Cache ArxmlCodec under name (for format_schema_ref reuse)     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Registering an ARXML schema

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

When `signal_name_pattern` is not set, column names default to raw ARXML
field names. To prefix with service and entry names:

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

`{method}` and `{event}` are synonyms — use whichever reads better.

This parses the ARXML, produces a VeloFlux schema with named columns, and caches
the `ArxmlCodec` under the name `"vehicle_someip_arxml"`.

### Inline ARXML schema (no pre-registration)

When the ARXML is not pre-registered, the stream can provide the path directly:

```json
{
  "schema": {
    "type": "arxml",
    "props": {
      "schema_path": "/etc/veloflux/schemas/arxml/system.arxml"
    }
  }
}
```

The ARXML is parsed at stream creation time. No named cache entry is created.
`signal_name_pattern` is optional and defaults to raw field names.

### Named ARXML schema reference

```json
{
  "schema": {
    "ref": "vehicle_someip_arxml"
  }
}
```

This reuses the schema and cached ARXML metadata from the named resource created
via `POST /schemas`.

### Signal name pattern

The `signal_name_pattern` controls how ARXML service/entry/field names are
combined into column names. When not specified, raw ARXML field names are used
as column names directly (matching eKuiper's default behavior).

| Placeholder | Meaning | Example |
|-------------|---------|---------|
| `{service}` | Service interface SHORT-NAME | `VehicleSpeedService` |
| `{method}` | Method / event SHORT-NAME (synonym: `{event}`) | `GetSpeed` / `SpeedChanged` |
| `{field}` | Field name within the payload type | `speed` |

`{method}` and `{event}` are synonyms pointing to the same value — the ARXML
entry name regardless of whether it's a method or an event. Use whichever reads
better for the deployment.

| Pattern | Example output |
|---------|---------------|
| (default / not set) | `speed`, `quality` — raw field names |
| `{service}.{method}.{field}` | `VehicleSpeedService.GetSpeed.speed` |
| `{service}.{event}.{field}` | `VehicleSpeedService.SpeedChanged.speed` |
| `{service}_{method}_{field}` | `VehicleSpeedService_GetSpeed_speed` |

---

## GBF Transport Schema

The GBF JSON transport schema describes the binary packet layout: where the
timestamp is, where frame boundaries are, and which field carries the message ID.
This is a separate file from the signal schema (DBC/ARXML).

### Schema Structure

```json
{
  "structure": {
    "type": "struct",
    "fields": [ ... ]
  }
}
```

| Property | Description |
|----------|-------------|
| `structure` | Root packet definition |

### Supported Types

| Type | Size | Description |
|------|------|-------------|
| `u8` | 1 byte | Unsigned 8-bit |
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
| `name` | String | Field name |
| `type` | String | Field type |
| `const` | u64 | Magic byte constraint |
| `length_ref` | String | Reference to length field |
| `length_unit` | String | `"bytes"` (only supported value) |
| `structure` | Object | Definition for sequence items |
| `format` | Object | Marks embedded payload |
| `format.id_ref` | String | **Required** when `format` is present — name of the sibling integer field that carries the message ID |
| `read_mask` | u64 | Bit mask after reading |
| `read_shift` | u32 | Bit shift after masking |

### Example: CAN Packet

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
        }
      }
    ]
  }
}
```

### Example: SOME/IP Packet

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

The `message_id` field carries `(service_id << 16) | method_id`. The GBF parser
extracts it as a generic `format_id`; the SOME/IP payload decoder interprets the
upper/lower 16 bits.

### Bit-Field Extraction

Use `read_mask` and `read_shift` for packed fields:

```json
{
  "name": "data_len",
  "type": "u8",
  "read_mask": 127,
  "read_shift": 0
}
```

Extracts: `value & 0x7F`

---

> **Breaking change (VF-83.1):** The root packet key has been renamed from
> `"packet"` to `"structure"`. The named-type library (`"types"` key) has been
> removed. All struct definitions must be inlined inside the `"structure"` field
> of the sequence that uses them (replaces `"item"`).

## See Also

- [GBF Decoder](../decoder/gbf.md) — Full decoder documentation
- [DBC Schema](dbc.md) — CAN signal definitions
