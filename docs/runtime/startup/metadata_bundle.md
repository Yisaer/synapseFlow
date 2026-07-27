# Resource Directory Format

Export, import, and startup initialization share one canonical directory:

```text
manifest.json
schemas/<type>/<name>/
wasm_files/<sha256>.wasm
```

`manifest.json` uses the `ResourceManifestV1` shape:

```json
{
  "format_version": 1,
  "bundle_version": "2026.07.24-1",
  "resources": {
    "memory_topics": [],
    "shared_mqtt_clients": [],
    "schemas": [],
    "streams": [],
    "pipelines": [],
    "udfs": []
  }
}
```

Pipeline run state is inline in each pipeline. Resource collections are sorted
by identity when exported. Schema files and WASM modules are present only when
referenced by the manifest.

Ordinary uploads, config, secrets, checkpoints, offsets, and runtime state are
not part of this format.

The physical format does not select write semantics:

- HTTP export writes the directory inside a ZIP envelope and requires
  `bundle_version`.
- Startup `--init-dir` reads the directory directly and uses add-only Apply.
- HTTP import extracts a ZIP and uses full-snapshot Sync.

Apply retains live conflicts and resources absent from the manifest. Sync
replaces all managed resource kinds and removes resources absent from the
manifest. Both validate a complete candidate before committing metadata.

`bundle_version` identifies producer content. It is not a database revision,
resource version, timestamp, or sortable release number.

## Export, Edit, and Initialize

A common workflow is to export a working node, edit the exported resources, and
use the extracted directory to initialize another node:

```shell
curl -sS -o veloflux-export.zip \
  'http://127.0.0.1:8080/storage/export?bundle_version=2026.07.24-1'
unzip veloflux-export.zip -d ./init

# Edit ./init/manifest.json and referenced files as needed.

veloflux --config ./config.yaml --data-dir ./data --init-dir ./init
```

Choose the final `bundle_version` when exporting. Manual edits made while
preparing that artifact remain part of the selected version. A target that has
already applied the same `bundle_version` skips it, so choose a new version for
a later revision.

The extracted directory, not the ZIP file, is passed to `--init-dir`.
`manifest.json` must be directly under that directory. HTTP import instead
accepts the ZIP envelope.

When editing an exported directory:

- keep all resource references valid
- keep file-backed schema sources under `schemas/<type>/<name>/`
- after changing a WASM module, recompute its SHA-256, rename it to
  `wasm_files/<sha256>.wasm`, and update the UDF `wasm_sha256`
- leave `format_version` unchanged

Use startup Apply to add missing resources without changing live conflicts. Use
HTTP import Sync when the edited artifact must replace the complete persisted
resource set.
