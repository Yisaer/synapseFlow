# VSS and Kuksa Integration

This guide shows how to map decoded vehicle signals to Vehicle Signal Specification (VSS) paths and
publish them to Eclipse Kuksa DataBroker with veloFlux:

1. Receive packed CAN frames from MQTT.
2. Decode the GBF packet container.
3. Decode CAN payloads with a DBC schema.
4. Transform decoded signals with SQL.
5. Publish derived telemetry to MQTT or mapped VSS values to Eclipse Kuksa DataBroker.

## Prerequisites

- A running veloFlux manager, usually at `http://127.0.0.1:8080`.
- A local MQTT broker, usually at `tcp://127.0.0.1:1883`.
- A local Kuksa DataBroker for the VSS example, for example `http://127.0.0.1:55555`.
- The SDV distribution registration enabled. The default `veloflux` binary registers the SDV
  schema and decoder extensions at startup.

## Schema Files

The stream needs two schema files:

- `vehicle_signals.dbc`: the CAN signal database.
- `spi_packet.json`: the GBF packet structure that wraps CAN frames.

This repository includes example files that can be copied directly:

- `distros/sdv/src/tests/dbc/1_PropulsionCAN.dbc`
- `distros/sdv/src/tests/spi_packet.json`

The GBF schema must use the root `structure` form. Older `types` / `packet` / `item` schemas are
not accepted by the current decoder.

Example `spi_packet.json`:

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
            { "name": "data_len", "type": "u8", "read_mask": 127 },
            {
              "name": "payload",
              "type": "bytes",
              "length_ref": "data_len",
              "format": { "type": "dbc", "id_ref": "can_id" }
            }
          ]
        }
      }
    ]
  }
}
```

Copy the schemas to paths that the veloFlux process can read:

```bash
cp distros/sdv/src/tests/dbc/1_PropulsionCAN.dbc /tmp/vehicle_signals.dbc
cp distros/sdv/src/tests/spi_packet.json /tmp/spi_packet.json
```

## Create the Sample Stream

The sample stream consumes binary packets from MQTT topic `/vehicle/powertrain`, uses the DBC schema
for stream columns, and uses the GBF decoder for runtime payload decoding.

```bash
curl -s -XPOST http://127.0.0.1:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "powertrain_stream",
    "type": "mqtt",
    "schema": { "ref": "vehicle_gbf" },
    "props": {
      "broker_url": "tcp://127.0.0.1:1883",
      "topic": "/vehicle/powertrain",
      "qos": 0
    },
    "shared": false,
    "decoder": { "type": "gbf", "props": {} }
  }' | jq .
```

Create the `vehicle_gbf` complete GBF schema before creating the stream; see the
[GBF schema documentation](../../../distros/sdv/docs/schema/gbf.md). By default, DBC-backed stream columns use the simple signal name (`{sig}`), so a DBC signal named
`Mess0_Sig1` becomes a SQL column named `Mess0_Sig1`. If a deployment needs globally qualified
names, set `signal_name_pattern` in the GBF entry's `format.props`, for example
`"{bus}__{id}__{sig}"`, and use those generated column names in SQL and VSS mappings.

Describe the stream to confirm the generated columns:

```bash
curl -s http://127.0.0.1:8080/streams/describe/powertrain_stream | jq .schema.columns
```

## Publish Processed Telemetry to MQTT

Create a pipeline that projects selected decoded signals and publishes JSON to `/telemetry/processed`.

```bash
curl -s -XPOST http://127.0.0.1:8080/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "id": "powertrain_pipeline",
    "sql": "SELECT ts, Mess0_Sig1 AS speed, Mess0_Sig2 AS rpm FROM powertrain_stream",
    "sinks": [
      {
        "type": "mqtt",
        "props": {
          "broker_url": "tcp://127.0.0.1:1883",
          "topic": "/telemetry/processed",
          "qos": 0
        },
        "encoder": {
          "type": "json",
          "props": {}
        }
      }
    ]
  }' | jq .
```

Start the pipeline:

```bash
curl -s -XPOST http://127.0.0.1:8080/pipelines/powertrain_pipeline/start
```

Publish a sample GBF packet with MQTTX or another MQTT client that can send binary payloads:

| Field | Value |
|-------|-------|
| Broker | `127.0.0.1:1883` |
| Topic | `/vehicle/powertrain` |
| Payload format | Hex |
| Payload | `00000190A5A0EC4A000C55024A08FFFFFFFFFFFFFFFF` |

Subscribe to `/telemetry/processed` in MQTTX to inspect the output.

The exact timestamp comes from the packet payload. With the sample payload above, the selected
signals decode to a single-row JSON batch like:

```json
[
  {
    "rpm": 1023,
    "speed": 1023,
    "ts": 1720765705290
  }
]
```

## Hot Update the Pipeline

Use `PUT /pipelines/:id` to replace a pipeline definition. The request body does not include `id`;
the path identifies the pipeline.

```bash
curl -s -XPUT http://127.0.0.1:8080/pipelines/powertrain_pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT ts, Mess0_Sig1 AS speed, Mess0_Sig2 AS rpm FROM powertrain_stream WHERE Mess0_Sig2 > 900",
    "sinks": [
      {
        "type": "mqtt",
        "props": {
          "broker_url": "tcp://127.0.0.1:1883",
          "topic": "/telemetry/critical",
          "qos": 0
        },
        "encoder": {
          "type": "json",
          "props": {}
        }
      }
    ]
  }' | jq .
```

If the previous desired state was running, manager attempts to restart the updated pipeline.
The sample DBC defines `Mess0_Sig2` as a 10-bit signal, so its valid range is `0..1023`.

## Publish VSS Values to Kuksa

Kuksa output uses a VSS mapping file. The current sink reads `sig2vss.qualifiedName` as the input
column name and maps it to the VSS path implied by the JSON tree location.

Minimal `vss_map.json`:

```json
{
  "Vehicle": {
    "children": {
      "Speed": {
        "datatype": "float",
        "type": "sensor",
        "sig2vss": {
          "qualifiedName": "Mess0_Sig1"
        }
      }
    }
  }
}
```

Create the file where veloFlux can read it:

```bash
cat >/tmp/vss_map.json <<'JSON'
{
  "Vehicle": {
    "children": {
      "Speed": {
        "datatype": "float",
        "type": "sensor",
        "sig2vss": {
          "qualifiedName": "Mess0_Sig1"
        }
      }
    }
  }
}
JSON
```

Create a Kuksa pipeline:

```bash
curl -s -XPOST http://127.0.0.1:8080/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "id": "vss_pipeline",
    "sql": "SELECT * FROM powertrain_stream",
    "sinks": [
      {
        "type": "kuksa",
        "props": {
          "addr": "http://127.0.0.1:55555",
          "vss_path": "/tmp/vss_map.json"
        }
      }
    ]
  }' | jq .
```

Start the pipeline and publish the same sample packet with MQTTX:

```bash
curl -s -XPOST http://127.0.0.1:8080/pipelines/vss_pipeline/start
```

Use topic `/vehicle/powertrain`, hex payload format, and payload
`00000190A5A0EC4A000C55024A08FFFFFFFFFFFFFFFF`.

Validate the VSS value with a Kuksa client:

```text
databroker-cli> get Vehicle.Speed
```

## Diagnostics

Check the stored desired state:

```bash
curl -s http://127.0.0.1:8080/pipelines/powertrain_pipeline | jq .status
```

Check the execution plan:

```bash
curl -s http://127.0.0.1:8080/pipelines/powertrain_pipeline/explain
```

Collect processor stats from a running pipeline:

```bash
curl -s http://127.0.0.1:8080/pipelines/powertrain_pipeline/stats | jq .
```

For Kuksa-specific semantics and limitations, see
[`docs/runtime/sinks/connectors/kuksa.md`](../../runtime/sinks/connectors/kuksa.md).
