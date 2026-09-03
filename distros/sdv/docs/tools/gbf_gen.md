# gbf_gen - GBF Data Generator

A CLI tool for generating GBF (General Binary Format) encoded binary data from schema definitions.

## Overview

`gbf_gen` is a schema-driven tool that:
- Encodes binary data according to GBF schema definitions
- Outputs to files (hex lines) or publishes via MQTT
- Supports random data generation or custom field values
- Works with any valid GBF schema (fully configurable packet structure)

## Installation

Build from the VeloFlux-Ex repository:

```bash
cargo build --release --bin gbf_gen
```

The binary will be at `target/release/gbf_gen`.

## Usage

```
gbf_gen -s <SCHEMA> [OPTIONS] <OUTPUT>

Arguments:
  <OUTPUT>  Output destination: "file:<path>" or "mqtt:<topic>"

Required:
  -s, --schema <PATH>     GBF schema JSON file

Options:
  -c, --count <N>         Number of packets to generate [default: 1]
  -i, --interval <MS>     Interval between MQTT publishes [default: 1000]
      --broker <URL>      MQTT broker (host:port) [default: 127.0.0.1:1883]
      --dbc <PATH>        DBC schema file/directory for CAN signal encoding
  -r, --random            Generate random payload data
  -d, --data <JSON>       JSON object with field values
      --items <N>         Items per sequence [default: 2]
      --timestamp <TS>    Timestamp value or "now"
      --can-ids <IDS>     CAN IDs (comma-separated hex)
      --seed <N>          Random seed for reproducibility
  -h, --help              Print help
```

## DBC Support

When a DBC schema is specified with `--dbc`, the tool generates valid CAN signal payloads instead of random bytes. The signals are packed according to the DBC definitions (bit position, endianness, etc.).

```bash
# Generate with DBC schema - payloads contain valid signal values
gbf_gen -s spi_packet.json --dbc signals.dbc -c 5 -r file:output.hex

# Use DBC directory with multiple buses
gbf_gen -s schema.json --dbc /path/to/dbc/ -c 10 file:data.hex
```

The tool will:
1. Load signal definitions from the DBC file
2. Generate random values within each signal's valid range
3. Pack signals into 8-byte CAN payloads using correct bit positions

```bash
# Generate 10 packets with random data
gbf_gen -s schema.json -c 10 -r file:output.hex

# Use specific CAN IDs (packed extended IDs keep bit 31)
gbf_gen -s schema.json -c 5 --can-ids 0x586,0x24A,0x80000103 -r file:data.hex

# Reproducible output with seed
gbf_gen -s schema.json -c 3 -r --seed 42 file:test.hex
```

### Publish to MQTT

```bash
# Publish 100 packets at 100ms intervals
gbf_gen -s schema.json -c 100 -i 100 -r mqtt:/sensor/can

# Custom broker
gbf_gen -s schema.json --broker 192.168.1.10:1883 -c 10 mqtt:/test/data
```

### Custom Field Values

```bash
# Set specific timestamp
gbf_gen -s schema.json --timestamp 1720765705290 file:output.hex

# Use current time
gbf_gen -s schema.json --timestamp now file:output.hex

# Provide field values via JSON
gbf_gen -s schema.json -d '{"ts": 1234567890}' file:output.hex
```

## Output Formats

### File Output (Hex Lines)

Each packet is output as a single line of uppercase hexadecimal characters:

```
00000190A5A0EC4A001855058608D171B278EADFA855024A08FBE8379B5E471E1F
EEA11039D26BAE370018550586082E5243DA17FC809055024A08CA7CF321E47A1FC9
```

This format is:
- Easy to read and diff
- Compatible with standard hex decode tools
- One packet per line for simple processing

### MQTT Output

Binary packets are published directly to the specified MQTT topic. This is useful for:
- Testing GBF decoders
- Simulating sensor data
- Integration testing

## Schema Requirements

The tool requires a valid GBF schema JSON file. See [GBF Schema Documentation](../schema/gbf.md) for the schema format.

Key schema features used:
- **`const` values**: Auto-filled during encoding (e.g., magic bytes)
- **`length_ref`**: Automatically calculated from referenced data
- **Inline struct fields**: Nested structures are defined inline under `structure.fields`

## Example Schema

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
            { "name": "can_id", "type": "u32be" },
            { "name": "data_len", "type": "u8" },
            {
              "name": "payload",
              "type": "bytes",
              "length_ref": "data_len"
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

## See Also

- [GBF Schema](../schema/gbf.md) - Schema format documentation
- [GBF Decoder](../decoder/gbf.md) - Decoder for parsing GBF data
