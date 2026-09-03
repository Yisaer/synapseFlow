# GBF Decoder

The GBF decoder parses an outer binary packet and decodes each embedded payload with the format compiled by the stream's GBF schema.

```text
bytes
  -> compiled GBF packet layout
  -> { timestamp, optional_bus_id, format_id, payload } frames
  -> compiled CAN or SOME/IP format
  -> record batch
```

## Configuration

Install a named complete GBF schema through the schema management API first:

```http
POST /schemas
Content-Type: application/json
```

```json
{
  "name": "vehicle_gbf",
  "revision": 1,
  "type": "gbf",
  "props": {
    "schema_path": "/opt/schema-source/vehicle-gbf.zip"
  }
}
```

`schema_path` is used only by `POST /schemas` and must identify a server-local ZIP
package. VeloFlux validates and extracts the package into the managed schema store;
streams and runtime decoders do not use the source archive.

The ZIP root must contain exactly one regular entry file. It may also contain the
entry's same-stem companion directory:

```text
vehicle-gbf.zip
├── vehicle.gbf.json
└── vehicle.gbf/
    └── format/
        └── vehicle.dbc
```

No archive entry may exist outside these two locations, and VeloFlux never scans
files beside the ZIP. See [GBF Schema](../schema/gbf.md) for the complete entry
grammar, package rules, and CAN/SOME/IP format examples.

Then create the stream through `POST /streams` and reference the installed schema by
its ID. The decoder has no schema or format properties:

```http
POST /streams
Content-Type: application/json
```

```json
{
  "name": "vehicle_input",
  "revision": 1,
  "type": "mqtt",
  "schema": { "ref": "vehicle_gbf" },
  "props": {
    "broker_url": "tcp://127.0.0.1:1883",
    "topic": "vehicle/input",
    "qos": 0
  },
  "decoder": { "type": "gbf", "props": {} }
}
```

Packet layout, `format.type`, private DBC/ARXML paths, signal naming, CAN ID mapping, and clamping all belong to the GBF entry. The decoder never reloads these values from stream configuration.

For multi-bus CAN packets, the payload format may reference separate bus and
CAN ID fields:

```json
"format": {
  "bus_id_ref": "bus_id",
  "id_ref": "can_id"
}
```

This mode uses `(bus_id, can_id)` for DBC lookup. Without `extend_ref`, `can_id`
must already be the DBC `u32` key (bit 31 = IDE, bits 0–28 = CAN ID, bits 29–30
= 0). GBF does not canonicalize FrameIDCAN/SocketCAN flag bits. When the
envelope splits ID and extended-frame flag, add `extend_ref` so the parser
composes that key. Do not configure `format.props.can_id_mapping` when
`bus_id_ref` is present.

## Fused packer merger

The SDV GBF merger uses the same `CompiledGbfSchema` as the normal decoder:

```json
{
  "sampler": {
    "interval": "500ms",
    "strategy": {
      "type": "packer",
      "props": {
        "merger": { "type": "gbf", "props": {} }
      }
    }
  }
}
```

The fused merger currently supports the CAN format. Within one sampling interval,
non-multiplexed frames use the configured CAN identity as their key. With
`bus_id_ref`, this is `(bus_id, can_id)`; multiplexed frames use
`(bus_id, can_id, mux_value)`. Repeated keys keep the newest payload. Unknown
identities are discarded.

See [GBF Schema](../schema/gbf.md) for the entry grammar and multi-file source layout.
