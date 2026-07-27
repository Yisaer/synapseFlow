# Uploaded Files

The file API stores ordinary user uploads under `<data_dir>/uploads/`. Paths are
validated, nested directories are supported, and writes use temporary files
before rename.

Uploads are runtime data and are intentionally outside the canonical resource
directory used by export, import, and startup `--init-dir`. Managed schema
sources live under `<data_dir>/schemas/` and are exported through the schema
resource declarations. WASM modules referenced by UDF resources live under
`<data_dir>/wasm_files/`.

Operators must back up ordinary uploads separately when they are required by
deployment-specific runtime configuration.
