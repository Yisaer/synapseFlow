# Static Properties

## Configuration

Static properties are deployment-provided strings loaded when the process
starts:

```yaml
properties:
  vin: "L123456789"
  site_code: "cn"
```

Keys must match `[a-z][a-z0-9_]*`. Values must be YAML strings; numbers,
booleans, lists, and maps are rejected rather than converted implicitly. Empty
strings are valid.

Each property can be added or overridden with an environment variable:

```bash
export VELOFLUX_PROPERTIES__VIN=L987654321
export VELOFLUX_PROPERTIES__SITE_CODE=de
```

The suffix is normalized to lowercase. File values are merged per key first,
then environment values override matching keys. An empty environment value is
an explicit empty string. Changes require a process restart.

All in-process flow instances share the same immutable property snapshot.
Properties are not manager resources and are not included in metadata
export/import.

## Security Boundary

Configuration files and environment variables contain plaintext strings.
After loading, property values and connector strings rendered from them use
redacted, zeroizing runtime string types. Their `Debug` and `Display`
representations do not reveal the value.

Pipeline GET, storage, and export preserve the original connector template.
They do not contain the rendered value or the process property map. Logs and
errors may include a property key and field path, but must not include the
property value or complete rendered connector string.

The value is necessarily exposed as plaintext at the MQTT or HTTP client call
boundary and in generated file names when used by a file sink affix. When
`prop()` is used by a JSON encoder transform, it can also enter the encoded
payload bytes. Transport confidentiality depends on the connector's TLS and
authentication configuration; file-name confidentiality depends on filesystem
access controls.

See [connector property templates](../../syntax/connectors/property_templates.md)
for the supported fields and grammar.
