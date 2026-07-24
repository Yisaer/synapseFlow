# GBF Schema

A GBF schema is one complete input contract. Its entry file describes the outer packet layout and selects one private payload format. Streams do not combine a GBF packet schema with a separate DBC or ARXML schema.

## Source layout

Every file-backed schema is installed from a ZIP package. The ZIP root must contain exactly one regular entry file. Additional files, when needed, must be placed below the entry's companion directory, whose name is the entry filename without its final extension.

```text
vehicle-gbf.zip
├── vehicle.gbf.json
└── vehicle.gbf/
    └── format/
        └── vehicle.dbc
```

When a named schema is created, VeloFlux copies this complete source to:

```text
<data_dir>/schemas/gbf/<schema-name>/
```

No archive entry may exist outside the root entry and companion directory. The path supplied to `POST /schemas` identifies the ZIP and is an installation input only. Runtime decoding and restart recovery use the extracted copy. VeloFlux does not discover dependencies beside the archive.

## CAN entry

```json
{
  "signal_name_pattern": "{sig_name}",
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
            { "name": "can_id", "type": "u32be" },
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
  },
  "format": {
    "type": "can",
    "props": {
      "dbc_path": "format/vehicle.dbc",
      "clamp_to_range": true,
      "can_id_mapping": "raw"
    }
  }
}
```

`dbc_path` is relative to the companion directory, not to the entry file and not to the process working directory. `can_id_mapping` also accepts `{ "mode": "bus_shift", "bits": 12 }`.

## SOME/IP entry

The packet `structure` is defined in the same way. The private format section is:

```json
{
  "signal_name_pattern": "{service}.{method}.{field}",
  "format": {
    "type": "someip",
    "props": {
      "arxml_path": "format/system.arxml"
    }
  }
}
```

The ARXML file belongs only to this GBF resource. It is parsed once while the GBF schema is resolved and is not registered as a nested schema.

`signal_name_pattern` belongs to the complete GBF schema and is therefore an
entry-level property for every private format. It is not read from
`format.props`, schema-install props, decoder props, or merger props.

## Runtime artifact

Resolving the entry produces one logical stream schema and one immutable `CompiledGbfSchema`. The artifact contains the validated packet layout, compiled DBC or ARXML state, naming policy, identity mapping, and decode options. Both the normal decoder and fused merger share it and do not read paths or format options from their own props.

Standalone `dbc` and `arxml` schema types remain available for protocols that use those formats directly. They reuse the same compiler functions but have independent resource lifecycles.
