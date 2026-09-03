# DBC Schema

CAN database schema for defining signal structures. Supports both legacy JSON format and standard Vector DBC files.

## Overview

The DBC schema defines:
- **Buses**: CAN network channels (chassis, powertrain, etc.)
- **Messages**: CAN frames identified by ID
- **Signals**: Data fields within messages with encoding parameters

## Supported Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| JSON | `.json` | Custom JSON format with buses, messages, and signals |
| DBC | `.dbc` | Standard Vector DBC format |
| Directory | folder | Multiple DBC files with naming convention |

### Parser Compatibility

DBC input accepts both `NS_:` and `NS_ :` section headers. Quoted comments may
span multiple lines, including object comments such as `CM_ SG_`. Parse errors
include the source path and the parser's line/column context.

---

## Loading Schema

The schema loader auto-detects the format based on the path:

```rust
use veloflux_ex::schema::dbc::load_can_schema;

// Single DBC file (defaults to Bus ID 0, Name "Bus0")
let schema = load_can_schema("/path/to/signals.dbc")?;

// Single JSON file
let schema = load_can_schema("/path/to/schema.json")?;

// Directory of DBC files
let schema = load_can_schema("/path/to/dbc_dir/")?;
```

---

## Directory Naming Convention

When loading a directory, all `.dbc` files **must** follow the pattern:

```
{id}_{name}.dbc
```

| Filename | Bus ID | Bus Name |
|----------|--------|----------|
| `1_chassis.dbc` | 1 | chassis |
| `2_powertrain.dbc` | 2 | powertrain |
| `3_body_control.dbc` | 3 | body_control |

**Validation Rules:**
- All files must match the naming pattern → Error on invalid format
- Bus IDs must be unique → Error on duplicate ID

---

## JSON Format

```json
{
  "buses": [
    {
      "name": "Chassis",
      "id": 1,
      "messages": [
        {
          "name": "WheelSpeed",
          "id": 256,
          "frameId": "0x100",
          "length": 8,
          "signals": [
            {
              "name": "FrontLeft",
              "start": 0,
              "length": 16,
              "scale": 0.01,
              "offset": 0,
              "isBigEndian": false,
              "isSigned": false
            }
          ]
        }
      ]
    }
  ]
}
```

### Signal Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | String | Signal name used by the configured output name pattern |
| `start` | u32 | Start bit position |
| `length` | u32 | Bit length |
| `scale` | f64 | Factor: `physical = raw * scale + offset` |
| `offset` | f64 | Offset: `physical = raw * scale + offset` |
| `isBigEndian` | bool | `true` = Motorola, `false` = Intel byte order |
| `isSigned` | bool | `true` for signed values |
| `isMultiplexer` | bool | `true` if this is the MUX selector |
| `isMultiplexed` | bool | `true` if this signal is multiplexed |
| `multiplexerValue` | i64 | MUX value that activates this signal |

### Output Column Naming

Set `signal_name_pattern` in DBC schema props. The default is `{sig_name}`. Supported tokens are:

| Token | Value |
|-------|-------|
| `{bus_name}` | Bus name |
| `{bus_id}` | Bus ID in decimal |
| `{bus_id_hex_lower}` / `{bus_id_hex_upper}` | Bus ID in hexadecimal |
| `{msg_id}` | Message `u32` lookup key in decimal |
| `{msg_id_hex_lower}` / `{msg_id_hex_upper}` | Message `u32` lookup key in hexadecimal |
| `{msg_name}` | Message name |
| `{sig_name}` | Signal name |

Hex tokens do not add a prefix. Unknown tokens and duplicate generated column
names are rejected while the schema is compiled. Legacy aliases such as `{id}`
and `{sig}` are not accepted.

### Data Types

| Condition | Output Type |
|-----------|-------------|
| Integer scale and offset | `Int64` |
| Fractional scale or offset | `Float64` |

### Multiplexed Signals

Multiplexed signals share the same bit positions but are activated by different MUX selector values:

```json
{
  "signals": [
    {
      "name": "MuxSelector",
      "start": 0,
      "length": 8,
      "isMultiplexer": true
    },
    {
      "name": "Speed",
      "start": 8,
      "length": 16,
      "isMultiplexed": true,
      "multiplexerValue": 0
    },
    {
      "name": "Temperature",
      "start": 8,
      "length": 16,
      "isMultiplexed": true,
      "multiplexerValue": 1
    }
  ]
}
```

When `MuxSelector = 0`, only `Speed` is decoded. When `MuxSelector = 1`, only `Temperature` is decoded.

---

## GBF Private Format Configuration

When DBC is used inside GBF, configure it in the complete GBF entry:

```json
{
  "format": {
    "type": "can",
    "props": {
      "dbc_path": "format/vehicle.dbc",
      "can_id_mapping": "raw"
    }
  }
}
```

`dbc_path` is relative to the GBF entry's companion directory. Schema resolution compiles the packet layout and DBC into one artifact, so decoder and merger props do not repeat these values.

### CAN ID Mapping

`can_id_mapping` controls how a DBC `(bus.id, msg.id)` pair is turned into the
lookup key that the **wire** CAN ID (the frame ID read via the GBF
`format.id_ref` field) is matched against.

| Value | Lookup key | Use |
|-------|------------|-----|
| `"raw"` *(default)* | `msg.id` | Generic CAN/DBC. The wire CAN ID equals the DBC message ID. |
| `{ "mode": "bus_shift", "bits": N }` | `(bus.id << N) \| msg.id` | Historical synthetic packing. `bits: 12` reproduces the legacy `(bus_id << 12) | msg_id` rule; larger widths suit extended / CAN FD message IDs. `N` must be in `1..=31`. |

Notes:

- **Default is `raw`.** Buses with `id = 0` produce identical keys under both
  modes (`(0 << N) | msg == msg`), so single-`.dbc`-file loads (which default to
  bus 0) and bus-0 JSON definitions are unaffected by the choice.
- **`bus_shift` is width-limited.** Under `bits: N`, a `msg.id >= (1 << N)`
  overflows into the bus field and mis-keys (e.g. an extended/CAN-FD ID under
  `bits: 12`). The decoder logs a warning when this overlap is detected; use a
  larger `bits` or `raw` for such IDs.
- **Duplicate IDs in `raw` mode.** If multiple buses carry the same `msg.id` and
  the input frame has no bus discriminator, the keys collide: the **last**
  message definition wins and a warning is logged. Use `bus_shift` to keep buses
  in separate key ranges.
- An unrecognized value (unknown string, unknown `mode`, or out-of-range `bits`)
  is a hard configuration error at decoder construction.
- A GBF payload may instead declare `format.bus_id_ref` alongside `id_ref`.
  That mode matches the structured `(bus_id, can_id)` pair, preserves all CAN
  ID bits, and requires `can_id_mapping` to be omitted.

> **Migration (issue #217).** Earlier versions applied the synthetic
> `(bus_id << 12) | msg_id` rule implicitly. That mapping is now **opt-in**.
> Deployments that relied on it — multi-bus configs whose frames embed the bus in
> the upper bits — must set
> `"can_id_mapping": { "mode": "bus_shift", "bits": 12 }` to keep matching frames.

### Packed `u32` CAN IDs

DBC lookup uses the message `id` as a `u32` key (optionally paired with a bus
ID). That key follows the Vector/SocketCAN convention: bit 31 is the extended
frame flag (IDE), and bits 0–28 are the CAN ID. Standard `0x123` and extended
`0x123` are therefore `0x00000123` and `0x80000123`.

`.dbc` files keep Vector's `BO_` encoding: an extended frame stores
`0x80000000 | can_id`. JSON DBC files use the `id` field as written. Signal
name tokens such as `{msg_id_hex_lower}` render this same `u32` key.

1. **Declare a wide id field in the GBF schema.** The `id_ref` field may be
   `u8`, `u16be`, `u16le`, `u32be`, or `u32le`. An 11-bit id fits a `u16`; a
   packed extended id needs a `u32` field:

   ```json
   { "name": "can_id", "type": "u32be" },
   { "name": "payload", "type": "bytes", "length_ref": "data_len",
     "format": { "type": "dbc", "id_ref": "can_id" } }
   ```

   When `extend_ref` is omitted, the wire `id_ref` must already be this packed
   `u32` (bits 29–30 clear). GBF does not strip FrameIDCAN FD/reserved bits.
   When the wire carries a separate IDE flag, add `extend_ref` so GBF composes
   the packed key at parse time.

2. **Use the default `raw` mapping.** The wire `u32` is matched directly against
   the DBC `msg.id`. Do **not** use `bus_shift` for extended ids: `bits: 12` only
   leaves room for an 11-bit id and would overflow (the decoder warns when this
   happens).

In-range ids that have no DBC entry are traced and skipped, exactly like
standard ids — never silently dropped for being "too wide".

