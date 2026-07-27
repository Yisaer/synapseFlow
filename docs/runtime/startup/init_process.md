# Startup Resource Directory

VeloFlux can apply an optional resource directory before runtime hydration:

```shell
veloflux --config ./config.yaml --data-dir ./data --init-dir ./init
```

The directory contains `manifest.json`, optional installed schema sources under
`schemas/`, and referenced WASM modules under `wasm_files/`. It must not be
inside `data_dir`. There is no startup archive and no fallback file under
`data_dir`.

This is the same canonical resource directory used by HTTP export and import.
Export and import wrap the directory in a ZIP file; startup reads the extracted
directory directly. See `metadata_bundle.md` for the export, edit, and
initialize workflow.

The manifest format is:

```json
{
  "format_version": 1,
  "bundle_version": "2026.07.24-1",
  "resources": {
    "schemas": [],
    "streams": [],
    "pipelines": [],
    "memory_topics": [],
    "shared_mqtt_clients": [],
    "udfs": []
  }
}
```

`bundle_version` is an opaque user-provided identity. VeloFlux only tests it for
equality with the last successfully applied version; it does not order versions.
The version is normally selected when exporting the artifact. A later revision
intended for a target that already applied that version requires a new value.

Startup uses add-only Apply semantics. Missing resources are created, resources
with the same kind and ID retain the live `data_dir` value, and live resources
absent from the manifest are retained. The selected live and incoming resources
form one candidate and are validated together before any metadata is committed.

Referenced files are validated and prepared under
`<data_dir>/.init-staging/`. Files are installed before metadata, while all new
metadata and the apply state are committed in one redb transaction. Failures
before commit do not advance `bundle_version`; stale staging work directories
from an interrupted apply are cleaned during startup.

A missing, unreadable, non-directory init path or missing/unparseable/unsupported
manifest is logged as a warning and skipped. A parsed manifest with an invalid
`bundle_version`, an unsafe source, invalid resources, broken dependencies, or
failed compilation aborts startup.
