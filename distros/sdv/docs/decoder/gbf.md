# GBF Decoder

The GBF decoder parses an outer binary packet and decodes each embedded payload with the format compiled by the stream's GBF schema.

```text
bytes
  -> compiled GBF packet layout
  -> { timestamp, format_id, payload } frames
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

The fused merger currently supports the CAN format. Within one sampling interval, non-multiplexed frames use the CAN ID as their key, multiplexed frames use `(can_id, mux_value)`, and repeated keys keep the newest payload. Unknown CAN IDs are discarded.

See [GBF Schema](../schema/gbf.md) for the entry grammar and multi-file source layout.
