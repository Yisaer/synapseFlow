# Schema

Schema definitions for CAN signal decoding and binary packet parsing.

## Module Structure

```
schema/
├── busmirror.md - Complete BusMirror schema
├── dbc.md       - CAN DBC schema (JSON + .dbc file support)
└── gbf.md       - Complete GBF schema
```

## Available Schema Types

| Schema | Type Name | Description |
|--------|-----------|-------------|
| [DBC Schema](dbc.md) | `dbc` | CAN signal definitions from JSON or DBC files |
| [GBF Schema](gbf.md) | `gbf` | Binary packet structure definition |
| [BusMirror Schema](busmirror.md) | `busmirror` | AUTOSAR Bus Mirroring topology and private DBC files |
