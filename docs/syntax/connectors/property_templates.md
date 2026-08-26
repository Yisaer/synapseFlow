# Connector Property Templates

Connector property templates use the same Upon engine as JSON encoder transform
templates. They are compiled and rendered once when a pipeline is applied:

```text
vehicle/{{ prop("vin") }}/telemetry
```

The connector profile registers `prop("key")`, where the key must match
`[a-z][a-z0-9_]*`. It otherwise follows Upon's native template syntax. It does
not provide `.row` or the encoder-only `json()` function. References to missing
context values and calls to unregistered functions fail during pipeline apply.

Literal text, Upon expressions, filters, and control blocks use Upon's normal
semantics. VeloFlux does not add a separate delimiter escape or connector
expression parser.

Property values are inserted exactly as configured. They are not trimmed,
converted, encoded, parsed recursively, or expanded a second time.

The initial template-enabled connector fields are:

- MQTT sink `props.topic`;
- MQTT sink `props.user_properties[].value`;
- HTTP sink `props.headers.*` values;
- HTTP multipart `props.body.fields.*` values.
- File sink `props.filename_pattern`.

Other connector fields are not interpreted as templates and retain their
literal configuration value. In particular, HTTP `auth` and `secret_headers`
remain `SecretRef` paths: template-looking text there is handled as literal
secret input by the existing secret policy.

MQTT User Property keys remain literal. Their values use the process property
profile described above and are rendered once during pipeline apply. They do
not provide `.row` or access to incoming MQTT 5 User Properties. Declaration
order and duplicate keys are preserved after rendering.

Missing properties, invalid Upon syntax, unavailable context values, and
invalid rendered protocol fields reject only the affected pipeline during
apply or hydration. They do not stop other pipelines or the service.
