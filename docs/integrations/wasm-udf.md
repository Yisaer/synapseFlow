# WASM UDF Design

## Overview

This document describes the design for user-defined functions (UDFs) executed inside a
WebAssembly (WASM) sandbox. Users compile their logic into `.wasm` files, upload them
through the manager API, and reference them directly in SQL (e.g. `SELECT my_udf(col) FROM stream`).

At startup, all persisted WASM UDFs are loaded into the `CustomFuncRegistry`, so they are
available to the flow planner and all processor pipelines without additional configuration.

### Design assumptions

- **Single-process deployment only.** All `FlowInstance`s run in the same OS process as
the manager. There is no remote worker mode. This means wasm files and metadata are
always locally accessible; no file distribution or remote synchronization is needed.
- **Upload hot-registers for new pipelines.** Uploaded UDFs are persisted to storage and
immediately registered into the running `CustomFuncRegistry` via `clone_and_add`. New
pipelines created after upload can reference the UDF in SQL. Startup reloads all
persisted UDFs before restoring pipelines.
- **DELETE is restart-only.** Deleting a UDF removes it from persistent storage but the
function remains registered in the running instance until the next restart.
Hot-unregister is deferred to future work.

## Architecture

```
                          ┌─────────────────────────┐
                          │   Manager REST API       │
                          │                          │
                          │ POST   /udfs/upload      │
                          │ GET    /udfs             │
                          │ GET    /udfs/:name       │
                          │ DELETE /udfs/:name       │
                          └──────────┬───────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                 │
                    ▼                ▼                 ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
           │  wasm_files/ │ │  metadata    │ │  export/import   │
           │  (on-disk)   │ │  (redb)      │ │  (bundle json)   │
           └──────┬───────┘ └──────┬───────┘ └──────────────────┘
                  │                │
                  │    ┌───────────┘
                  │    │
                  ▼    ▼
        ┌─────────────────────────┐
        │   Startup / Bootstrap    │
        │                         │
        │ 1. List StoredUdf from  │
        │    metadata redb         │
        │ 2. Load .wasm from disk │
        │ 3. Instantiate via      │
        │    wasmtime             │
        │ 4. Register into        │
        │    CustomFuncRegistry   │
        └───────────┬─────────────┘
                    │
                    ▼
        ┌─────────────────────────┐
        │   Flow Module           │
        │                         │
        │  ScalarExpr::CallFunc { │
        │    func: Arc<           │
        │      dyn CustomFunc     │◀── WasmUdf implements
        │    >,                   │   CustomFunc trait
        │    args: [...]          │
        │  }                      │
        └─────────────────────────┘
```

### Key design decisions

1. **WasmuUdf implements the existing `CustomFunc` trait.** Zero changes to the planner,
   optimizer, or processor pipeline. The flow module only sees `Arc<dyn CustomFunc>`.

2. **WASM runtime: wasmtime.** wasmtime is a pure-Rust, production-grade WASM runtime
   with JIT compilation. It provides sandbox isolation, fuel metering (for timeout
   protection), and a safe Rust API (no `unsafe` in call sites).

3. **WASM files stored on-disk, metadata in redb.** The WASM module binary is written to
   a `wasm_files/` directory under `base_dir`. A `udfs` table in `MetadataStorage` holds
   the serialized `StoredUdf` record (name, file hash, metadata). This follows the same
   pattern as existing persisted resources (streams, pipelines, mqtt configs, memory topics).

4. **Batch mode via optional `eval_batch` on `CustomFunc`.** WASM functions can optionally
   export a batch entry point that processes multiple rows in one FFI call, amortizing the
   WASM boundary crossing cost.

## Storage Design

### New storage table

Add a `udfs` table to `MetadataStorage` alongside the existing tables:

```rust
// In src/storage/src/lib.rs

const UDFS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("udfs");
```

### StoredUdf record

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUdf {
    /// Canonical function name (case-insensitive in SQL, stored lowercase).
    pub name: String,
    /// SHA-256 hex digest of the WASM file contents.
    pub wasm_sha256: String,
    /// Original upload metadata (serialized as JSON for export portability).
    pub raw_json: String,
}
```

The `raw_json` field holds a `UdfUploadRequest` as JSON — the same struct accepted by
the upload API. This keeps import/export semantics consistent with streams and pipelines.

### On-disk WASM files

WASM binaries are stored under `<base_dir>/wasm_files/<sha256>.wasm`.

**Why SHA-256 content-addressable storage?**

1. **Deduplication.** If the same `.wasm` binary is uploaded twice (e.g. two different
   UDF names wrapping the same logic, or a re-upload by mistake), only one copy exists
   on disk. The `StoredUdf` records both point to the same file via the hash.
2. **Integrity verification on load.** At startup, the system re-hashes the file on disk
   and compares it to the stored `wasm_sha256`. This detects silent disk corruption or
   tampering before the module is instantiated.
3. **Safe GC on delete.** When a UDF is deleted, the system checks if any other
   `StoredUdf` references the same SHA-256. The on-disk file is only removed when the
   reference count drops to zero.

## Manager API

### Routes

| Method   | Path              | Description                        |
|----------|-------------------|------------------------------------|
| `POST`   | `/udfs/upload`    | Upload a new WASM UDF              |
| `GET`    | `/udfs`           | List all registered UDFs           |
| `GET`    | `/udfs/:name`     | Get UDF metadata                   |
| `DELETE` | `/udfs/:name`     | Delete a UDF                       |

### POST `/udfs/upload`

Request: `multipart/form-data`
- Field `name` (text): canonical function name (e.g. `my_udf`). The name is
  canonicalized to lowercase, then must match the resource-id grammar
  `` `[A-Za-z][A-Za-z0-9_]{0,127}` `` (a letter followed by letters, digits, or
  underscores). `GET`/`DELETE /udfs/:name` apply the same lowercase
  canonicalization before lookup. Invalid names return `400 Bad Request`.
- Field `wasm_file` (binary): the `.wasm` file

Response `200 OK`:
```json
{
  "name": "my_udf",
  "wasm_sha256": "a1b2c3...",
  "status": "registered",
  "metadata": {
    "description": "optional description from wasm metadata",
    "args": [{"name": "input", "type": "string"}],
    "return_type": "int64",
    "batch_mode": false
  }
}
```

Server-side behaviour:
1. Compute SHA-256 of the WASM binary.
2. Check for name collision with built-in functions and existing UDFs; return
   `409 Conflict` if already registered.
3. Validate the WASM module: instantiate it with wasmtime, verify required exports
   (`udf_row`, `udf_metadata_ptr`, `udf_metadata_len`), verify memory cap, and
   validate the metadata JSON.
4. Write `<sha256>.wasm` to `wasm_files/`.
5. Insert `StoredUdf` into the `udfs` redb table.
6. Instantiate and hot-register the UDF into the running `CustomFuncRegistry` via
   `clone_and_add`. New pipelines created after this point can reference the UDF.
   Startup reload remains the recovery path after restart.
   If hot-registration fails, the response includes `status: "persisted (hot-register
   failed: ...)"` — the UDF is stored but not yet callable.

### GET `/udfs`

Returns a list of all stored UDFs with their metadata (excluding WASM binary).

### GET `/udfs/:name`

Returns the full metadata for a single UDF.

### DELETE `/udfs/:name`

1. Remove the `StoredUdf` record from redb.
2. Garbage-collect the WASM file on disk if no other UDF references the same SHA-256.

The UDF remains registered in the running `CustomFuncRegistry` until the next
restart. Hot-unregister and pipeline-reference checking are deferred to future
work; for now, only persist a new pipeline if you intend to keep the UDF.

### Integration with import/export

Export produces a **ZIP archive** containing:

```
veloflux-export-<timestamp>.zip
├── manifest.json          # ResourceManifestV1 (streams, pipelines, mqtt, memory_topics, udfs)
└── wasm_files/            # WASM UDF binaries (one .wasm per function)
    ├── <sha256_1>.wasm
    └── <sha256_2>.wasm
```

**Why an archive instead of base64-inline JSON?**

- Large WASM binaries don't bloat the JSON payload.
- The archive is a single file, easy to transfer and archive.
- Each `.wasm` file can be independently SHA-256 verified against the stored hash.
- Users can unpack, inspect, and selectively modify before re-importing.

**Export flow**

1. Serialize `ResourceManifestV1` to `manifest.json`. The `udfs` field contains
   `ExportUdf` records with `name` and `wasm_sha256` (not the binary).
2. Copy referenced `.wasm` files from `<base_dir>/wasm_files/` into the archive's
   `wasm_files/` directory.
3. Return as `application/zip` with `Content-Disposition: attachment`.

**Import flow**

1. Safely extract the ZIP to a temporary directory.
2. Parse and validate `manifest.json` (streams, pipelines, etc.).
3. For each UDF in the `udfs` list:
   - Require `<sha256>.wasm` to exist in the archive.
   - Recomputed SHA-256 and compare to declared value.
   - Validate the WASM module via `WasmEngine::validate`.
   - Confirm metadata name matches the declared name.
4. Copy `.wasm` files to `<base_dir>/wasm_files/` (skip if already present).
5. Apply the `MetadataExportSnapshot` to redb.

**Current behavior**

| Method | Path              | Content-Type       |
|--------|-------------------|--------------------|
| `GET`  | `/storage/export?bundle_version=<version>` | `application/zip` |
| `POST` | `/import`         | `application/zip` |

The previous JSON-only format is no longer supported.

## WASM Runtime Integration (wasmtime)

### Crate structure

A new crate `src/udf/` (sibling to `src/flow/`, `src/storage/`, `src/manager/`):

```
src/udf/
  Cargo.toml
  src/
    lib.rs          # public API
    wasm_func.rs    # WasmUdf implements CustomFunc
    engine.rs       # WasmEngine (wasmtime Engine + Linker pool)
    abi.rs          # WASM <-> Value type mapping
```

### WASM ABI specification

Each UDF `.wasm` file must export at minimum the following function:

```wat
(module
  ;; Row-level evaluation (required)
  ;; Parameters: i32 pointer to input JSON bytes, i32 byte length
  ;; Returns: i32 pointer to output JSON bytes. The host reads length via a callback.
  (func (export "udf_row") (param i32 i32) (result i32))

  ;; Optional metadata export
  (memory (export "memory") 1)

  ;; Allocate memory from the WASM linear memory (host-provided)
  (import "host" "alloc" (func (param i32) (result i32)))

  ;; Metadata export (required): returns a pointer+length to JSON metadata blob
  (func (export "udf_metadata_ptr") (result i32))
  (func (export "udf_metadata_len") (result i32))
)
```

**Why JSON as the ABI wire format?**

The parameters and return value are serialized as JSON strings. This is a deliberate
trade-off between performance and interoperability:

1. **Universal compatibility.** JSON serialization is available in every language that
   compiles to WASM (Rust via `serde_json`, C via `cJSON`, Go via `encoding/json`,
   AssemblyScript via built-in `JSON`). A struct-based C ABI would require per-language
   binding generators and struggle with dynamic types like `Value::List` or `Value::Struct`.
2. **Matches veloFlux's type system.** The `Value` enum already has round-trip JSON
   conversion (`value_to_json_value` / `json_value_to_value` in `helpers.rs`). The host
   side of the ABI is essentially `serde_json::to_vec(&args) -> call wasm ->
   serde_json::from_slice(&result)`.
3. **Simplifies versioning.** Adding new types (e.g. a `Interval` type in the future)
   does not break the ABI. The JSON schema simply gains a new variant.
4. **Acceptable overhead for the UDF use case.** A UDF is, by definition, user-defined
   logic whose runtime dominates the FFI cost. JSON serialize/deserialize of a handful
   of scalar arguments is in the hundreds-of-nanoseconds range — negligible next to
   typical UDF body execution. For bulk operations, the optional batch mode (`eval_batch`)
   amortizes this across many rows in a single JSON array.

If a specific UDF is found to be JSON-bound (i.e. the serialization cost dominates),
a future optimization could add a raw pointer-based ABI variant (`udf_row_raw`) that
passes typed arrays. But this adds complexity and should be driven by profiling data,
not premature optimization.

### WasmUdf type

```rust
// In src/udf/src/wasm_func.rs

#[derive(Debug)]
pub struct WasmUdf {
    name: String,
    metadata: UdfMetadata,
    instance: wasmtime::Instance,
    // Cached callables for hot path
    udf_row_fn: wasmtime::TypedFunc<(i32, i32), i32>,
    alloc_fn: wasmtime::TypedFunc<i32, i32>,
    memory: wasmtime::Memory,
}
```

`WasmUdf` implements `CustomFunc`:

```rust
impl CustomFunc for WasmUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        // Type-check args against metadata.signature
        // (or skip for simplicity — type errors caught by wasm)
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        // 1. Serialize args into JSON string
        // 2. Write JSON to WASM linear memory
        // 3. Call udf_row(input_ptr, input_len) -> result_ptr
        // 4. Read result JSON from WASM memory
        // 5. Deserialize JSON into Value
    }

    fn eval_batch(&self, batch_args: &[Vec<Value>]) -> Result<Vec<Value>, EvalError> {
        // If batch mode supported, serialize all rows into one JSON array
        // and call udf_batch(count, ptr, len) -> ptr
        // Otherwise fall back to per-row eval_row
    }
}
```

### Engine pooling

A single `wasmtime::Engine` and `wasmtime::Linker` are shared across all UDF instances.
Per-UDF state (like linear memory) is freshly initialized via `wasmtime::Instance`.

### Safety limits

- **Fuel metering**: each `eval_row` / `eval_batch` call gets a fuel budget.
  Exhausting it produces a `Timeout` error.
- **Memory limit**: WASM linear memory is capped (e.g. 16 MiB per instance) via
  `wasmtime::Config::memory_pages`.
- **No `unsafe`** in the call site. wasmtime's safe API handles all FFI.

## Flow Module Integration

### CustomFuncRegistry extension

`CustomFuncRegistry` is built at startup with all built-in functions plus all
persisted WASM UDFs. After startup, hot-loaded UDFs are added via `clone_and_add`
which atomically replaces the `Arc`; the hot path (`compute_processor`,
`filter_processor`) reads from the current `Arc` lock-free — no lock is taken per
expression evaluation.

```rust
impl CustomFuncRegistry {
    /// Build a registry containing built-in functions and the given WASM UDFs.
    /// Returns an error on name collision.
    pub fn with_builtins_and_wasm(wasm_udfs: Vec<Arc<dyn CustomFunc>>) -> Result<Arc<Self>, String> {
        let mut registry = Self::builtins();
        for udf in wasm_udfs {
            let name = udf.name().to_lowercase();
            if registry.functions.contains_key(&name) {
                return Err(format!("UDF '{name}' conflicts with an existing function"));
            }
            registry.register(udf);
        }
        Ok(Arc::new(registry))
    }

    fn register(&mut self, func: Arc<dyn CustomFunc>) {
        self.functions.insert(func.name().to_lowercase(), Arc::clone(&func));
        for alias in func.aliases() {
            self.functions.insert(alias.to_lowercase(), Arc::clone(&func));
        }
    }
}
```

### Startup loading sequence

UDF loading happens during FlowInstance construction, before the manager boots
its HTTP server. The sequence is:

```
FlowInstance::new()
  ├── Build CustomFuncRegistry (built-ins only)
  └── Rebuild registry with builtins + WASM UDFs (read lock on startup only)

Manager bootstrap
  ├── hydrate WASM UDFs from storage              ← NEW
  │    1. List all StoredUdf records
  │    2. For each: load <sha256>.wasm from disk
  │    3. Re-hash and verify against stored wasm_sha256
  │    4. Instantiate WasmUdf via wasmtime
  │    5. Collect validated UDFs
  │    6. Rebuild CustomFuncRegistry with built-ins + WASM UDFs
  │    7. Replace the FlowInstance's registry with the new one
  ├── hydrate memory topics
  ├── hydrate shared MQTT configs
  ├── hydrate streams
  └── hydrate pipelines
```

UDFs must be loaded **before** pipelines, because pipeline SQL may reference them.
After startup, hot-load replaces the `Arc` via `clone_and_add` (write lock on upload only).
FlowInstance holds an updated `Arc<CustomFuncRegistry>` via `FlowInstanceSharedRegistries`.

### Batching extension (optional)

Add a default `eval_batch` to `CustomFunc`:

```rust
pub trait CustomFunc: Send + Sync + std::fmt::Debug {
    // ... existing methods ...

    /// Evaluate the function for multiple rows at once.
    ///
    /// Default implementation falls back to per-row `eval_row`.
    /// Implementers may override for batched FFI (e.g. WASM).
    fn eval_batch(&self, batch_args: &[Vec<Value>]) -> Result<Vec<Value>, EvalError> {
        batch_args.iter().map(|args| self.eval_row(args)).collect()
    }
}
```

The `ComputeProcessor` can then use `eval_batch` when it encounters the same `CallFunc`
expression across multiple rows, avoiding repeated WASM boundary crossings. This is a
performance optimization and not required for initial implementation.

## WASM UDF Authorship

### Required exports

Every WASM UDF module must export:

1. `memory`: WebAssembly linear memory
2. `udf_metadata_ptr() -> i32` and `udf_metadata_len() -> i32`: pointers to a
   JSON metadata blob describing the function
3. `udf_row(ptr: i32, len: i32) -> i32`: row-level evaluation

### Metadata format

The `udf_metadata` JSON blob:

```json
{
  "name": "my_udf",
  "description": "Computes a custom score from sensor data",
  "aliases": ["custom_score"],
  "args": [
    {"name": "sensor_value", "type": "float"},
    {"name": "threshold", "type": "float"}
  ],
  "return_type": "float",
  "batch_mode": false
}
```

Supported types: `"int"`, `"float"`, `"string"`, `"bool"`, `"timestamp"`,
`"object"`, `"array"`, `"any"`.

### Example: Rust UDF (with `veloflux-udf-sdk`)

We provide `veloflux-udf-sdk`, a helper crate that hides the entire WASM ABI
behind a `define_udf!` macro. Users write plain Rust functions with zero
boilerplate:

```rust
use veloflux_udf_sdk::define_udf;

define_udf! {
    name = "my_udf",
    description = "Multiplies sensor value by threshold",
    return_type = "float",
    fn my_udf(sensor_value: f64, threshold: f64) -> f64 {
        sensor_value * threshold
    }
}
```

The macro automatically generates:
- `udf_row(input_ptr, input_len)` — JSON args deserialization → call user fn → serialize result
- `udf_metadata_ptr()` / `udf_metadata_len()` — metadata JSON blob
- `memory` — WASM linear memory for the bump allocator

Compile with:
```
cargo build --target wasm32-unknown-unknown --release
```

The JSON ABI (`Value` ↔ JSON ↔ WASM bytes) is a transparent implementation detail;
users of `veloflux-udf-sdk` never interact with it directly.

## Performance

**Current limitation (MVP):** Each `eval_row` call creates a new `wasmtime::Store`,
instantiates the module, and resolves typed exports. This is orders of magnitude
more expensive than built-in scalar functions. WASM UDFs are suitable for
prototyping and low-throughput use cases, but not yet recommended for
production streaming workloads.

**Planned optimization:** Cache reusable `Store`/`Instance` pools or use
wasmtime's `InstancePre` for amortized instantiation. This will bring WASM UDF
performance within the same order of magnitude as built-in custom functions.

## Security

1. **Sandbox**: Each UDF runs in a separate `wasmtime::Instance` with isolated
   linear memory. No access to filesystem, network, or environment variables.

2. **Resource limits**: Fuel metering prevents infinite loops. Memory cap prevents
   OOM. These are configured per-engine, not per-UDF (to avoid overhead).

3. **Input validation**: Argument count and types are validated against metadata
   before calling into WASM.

4. **No `unsafe`**: The wasmtime safe API is used throughout. All pointer reads/writes
   go through wasmtime's checked `Memory::read`/`Memory::write` methods.

## Future Extensions

### gRPC remote UDFs

A `RemoteUdf` could implement `CustomFunc` to call an external UDF service via gRPC.
This would follow the same registration pattern and be transparent to the flow module.
This is appropriate for UDFs that:
- Require languages that don't compile to WASM (Python, JavaScript)
- Are computationally expensive (seconds per call)
- Need access to external services or databases

### Stateful WASM UDFs

WASM UDFs could maintain state across rows by exporting `udf_init`/`udf_eval`/`udf_close`,
mapping to the existing `StatefulFunction` trait. This would enable windowed aggregations
and custom stateful transformations in WASM.

### WASI support

If use cases demand it, wasmtime's WASI support could selectively enable file system or
network access for UDFs that need it, with explicit opt-in and capability controls.

## Migration & Compatibility

- `CustomFunc` trait: adding `eval_batch` with a default implementation is
  backwards-compatible. All existing `CustomFunc` implementations continue to work.
- `CustomFuncRegistry`: adding `register_wasm` does not affect existing built-in functions.
- Storage: new `udfs` table is additive. Existing storage instances don't need migration.
- Export/import: the `udfs` field in `ExportResources` is required. Old JSON-only export
  bundles (without `resources.udfs`) are not compatible with the canonical resource directory format
  and must be migrated by adding `"udfs": []` to the resources object and wrapping the
  metadata in the resource directory layout. The previous JSON body format is no longer supported.
