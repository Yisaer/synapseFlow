# BusMirror Schema

A `busmirror` schema is one complete AUTOSAR Bus Mirroring v1 contract. Its JSON
entry defines the network topology, private DBC files, and output signal names.
The DBC files are private members of this schema; they are not separately
registered schemas.

## Source package

Install the schema from a server-local ZIP through `POST /schemas`:

```text
vehicle-busmirror.zip
├── vehicle.json
└── vehicle/
    ├── powertrain.dbc
    └── body.dbc
```

The ZIP root contains exactly one entry file. All DBC members must be regular
`.dbc` files below the entry's same-stem companion directory. Absolute paths,
parent traversal, backslashes, and files outside that directory are rejected.

After installation, VeloFlux restores the schema from:

```text
<data_dir>/schemas/busmirror/<schema-name>/
```

The original ZIP path is not used at runtime or after restart.

## Entry format

```json
{
  "version": "v1",
  "signal_name_pattern": "{network_type}{network_id}__{msg_id_hex_lower}__{sig_name}",
  "format": {
    "type": "can",
    "props": {}
  },
  "buses": [
    {
      "network_type": "can",
      "network_id": 1,
      "name": "Powertrain",
      "dbc": "powertrain.dbc"
    },
    {
      "network_type": "lin",
      "network_id": 2,
      "name": "Body",
      "dbc": "body.dbc"
    }
  ]
}
```

`version` must be `v1`, `format.type` must be `can`, and `network_type` may be
`can` or `lin`. Classic CAN and CAN FD use the same CAN network declaration.
Each `(network_type, network_id)` pair and each frame identity within a bus must
be unique.

## Signal names

`signal_name_pattern` is compiled when the schema is installed. The default is:

```text
{bus_name}_{msg_name}_{sig_name}
```

BusMirror DBC tokens are:

| Token | Value |
|---|---|
| `{bus_name}` | Bus name from the entry |
| `{msg_id}` | Message `u32` lookup key in decimal |
| `{msg_id_hex_lower}` / `{msg_id_hex_upper}` | Message `u32` lookup key in hexadecimal |
| `{msg_name}` | DBC message name |
| `{sig_name}` | DBC signal name |

BusMirror network tokens are:

| Token | Value |
|---|---|
| `{network_type}` | `can` or `lin` |
| `{network_type_id}` | Numeric AUTOSAR network type in decimal |
| `{network_type_id_hex_lower}` / `{network_type_id_hex_upper}` | Numeric network type in hexadecimal |
| `{network_id}` | Network ID in decimal |
| `{network_id_hex_lower}` / `{network_id_hex_upper}` | Network ID in hexadecimal |

BusMirror has no public bus ID. The DBC compiler's packed internal lookup key is
not part of the schema contract, so `{bus_id}`, `{bus_id_hex_lower}`, and
`{bus_id_hex_upper}` are rejected for BusMirror patterns.

Hexadecimal tokens do not add a prefix. Add a literal prefix when needed, for
example `0x{msg_id_hex_upper}`. Unknown, empty, or unclosed tokens, empty output
names, duplicate output names, and a generated `ts` name are rejected during
installation. Legacy aliases such as `{id}` and `{sig}` are not accepted.

CAN FrameID is a 4-byte AUTOSAR `FrameIDCAN`. The decoder keeps IDE (bit 31) and
clears FD (bit 30) and reserved (bit 29), then uses that packed `u32` as the DBC
lookup key. Standard `0x123` and extended `0x123` therefore stay distinct.
Network type and network ID remain part of the BusMirror frame identity, so
equal CAN IDs on different buses do not collide.
