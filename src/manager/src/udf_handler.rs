use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;
use std::sync::Arc;
use storage::StoredUdf;
use udf::WasmEngine;

use crate::pipeline::AppState;
use crate::resource_id::{ResourceIdKind, validate_resource_id};

#[derive(Serialize)]
pub struct UdfInfo {
    pub name: String,
    pub revision: u64,
    pub wasm_sha256: String,
    pub size_bytes: u64,
}

#[derive(Serialize)]
pub struct UploadUdfResponse {
    pub name: String,
    pub revision: u64,
    pub wasm_sha256: String,
    pub status: String,
}

/// POST /udfs/upload
///
/// Accepts multipart form data with fields:
/// - `name` (text): canonical function name
/// - `revision` (text): positive JSON-safe integer
/// - `wasm_file` (binary): the compiled .wasm file
pub async fn upload_udf_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut name: Option<String> = None;
    let mut wasm_bytes: Option<Vec<u8>> = None;
    let mut revision: Option<u64> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "name" => {
                name = field.text().await.ok();
            }
            "wasm_file" => {
                wasm_bytes = field.bytes().await.ok().map(|b| b.to_vec());
            }
            "revision" => {
                revision = field
                    .text()
                    .await
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok());
            }
            _ => {}
        }
    }

    // UDF names are canonicalized to lowercase (the custom function registry is
    // case-insensitive), then validated against the unified resource-id grammar
    // (VF-51 §4.3).
    let name = match name {
        Some(n) if !n.trim().is_empty() => n.trim().to_lowercase(),
        _ => return (StatusCode::BAD_REQUEST, "field 'name' is required").into_response(),
    };
    if let Err(err) = validate_resource_id(ResourceIdKind::UdfName, &name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let wasm_bytes = match wasm_bytes {
        Some(b) if !b.is_empty() => b,
        _ => return (StatusCode::BAD_REQUEST, "field 'wasm_file' is required").into_response(),
    };
    let revision = match revision.and_then(|value| {
        crate::revision::validate_revision(value)
            .ok()
            .map(|()| value)
    }) {
        Some(revision) => revision,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "field 'revision' is required and must be a positive JSON safe integer",
            )
                .into_response();
        }
    };

    // Validate the WASM module
    let engine = match WasmEngine::new() {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to initialize WASM engine: {e}"),
            )
                .into_response();
        }
    };

    let metadata = match engine.validate(&wasm_bytes) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid WASM module: {e}")).into_response();
        }
    };

    if metadata.name != name {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "name mismatch: UDF metadata name is '{}', but upload name is '{name}'",
                metadata.name
            ),
        )
            .into_response();
    }

    // Check for name collision with built-in functions before persisting
    let instance = state.instances.default_instance();
    let current_registry = instance.custom_func_registry();
    if current_registry.is_registered(&name) {
        return (
            StatusCode::CONFLICT,
            format!("function name '{name}' conflicts with an existing built-in or UDF"),
        )
            .into_response();
    }

    let sha256 = sha256_hex(&wasm_bytes);
    let wasm_dir = state.storage.wasm_files_dir();
    let wasm_path = wasm_dir.join(format!("{sha256}.wasm"));

    // Write .wasm file to disk (if it doesn't already exist)
    if !wasm_path.exists()
        && let Err(e) = std::fs::write(&wasm_path, &wasm_bytes)
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to write WASM file: {e}"),
        )
            .into_response();
    }

    let stored = StoredUdf {
        name: name.clone(),
        revision,
        wasm_sha256: sha256.clone(),
        raw_json: serde_json::to_string(&metadata).unwrap_or_default(),
    };

    match state.storage.create_udf(stored) {
        Ok(()) => {}
        Err(storage::StorageError::AlreadyExists(_)) => {
            return (StatusCode::CONFLICT, format!("UDF '{name}' already exists")).into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to persist UDF: {e}"),
            )
                .into_response();
        }
    }

    // Hot-load: instantiate and register into the running instance
    let status = match engine.instantiate(&name, &wasm_bytes) {
        Ok(wasm_udf) => {
            let current_registry = instance.custom_func_registry();
            match current_registry.clone_and_add(Arc::new(wasm_udf)) {
                Ok(new_registry) => {
                    instance.set_custom_func_registry(new_registry);
                    "registered".to_string()
                }
                Err(e) => {
                    format!("persisted (hot-register failed: {e})")
                }
            }
        }
        Err(e) => format!("persisted (instantiation failed: {e})"),
    };

    (
        StatusCode::OK,
        Json(UploadUdfResponse {
            name,
            revision,
            wasm_sha256: sha256,
            status,
        }),
    )
        .into_response()
}

/// GET /udfs
pub async fn list_udfs_handler(State(state): State<AppState>) -> impl IntoResponse {
    let udfs = match state.storage.list_udfs() {
        Ok(udfs) => udfs,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let infos: Vec<UdfInfo> = udfs
        .into_iter()
        .map(|udf| {
            let size_bytes = state
                .storage
                .wasm_files_dir()
                .join(format!("{}.wasm", udf.wasm_sha256))
                .metadata()
                .ok()
                .map(|m| m.len())
                .unwrap_or(0u64);
            UdfInfo {
                name: udf.name,
                revision: udf.revision,
                wasm_sha256: udf.wasm_sha256,
                size_bytes,
            }
        })
        .collect();

    (StatusCode::OK, Json(infos)).into_response()
}

/// Canonicalize a `:name` path segment for UDF lookup: lowercase to match the
/// case-insensitive registry, then validate against the resource-id grammar.
fn canonical_udf_path_name(name: &str) -> Result<String, Box<axum::response::Response>> {
    let canonical = name.trim().to_lowercase();
    validate_resource_id(ResourceIdKind::UdfName, &canonical)
        .map_err(|err| Box::new((StatusCode::BAD_REQUEST, err).into_response()))?;
    Ok(canonical)
}

/// GET /udfs/:name
pub async fn get_udf_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let name = match canonical_udf_path_name(&name) {
        Ok(name) => name,
        Err(resp) => return *resp,
    };
    match state.storage.get_udf(&name) {
        Ok(Some(udf)) => {
            let size_bytes = state
                .storage
                .wasm_files_dir()
                .join(format!("{}.wasm", udf.wasm_sha256))
                .metadata()
                .ok()
                .map(|m| m.len())
                .unwrap_or(0u64);
            (
                StatusCode::OK,
                Json(UdfInfo {
                    name: udf.name,
                    revision: udf.revision,
                    wasm_sha256: udf.wasm_sha256,
                    size_bytes,
                }),
            )
                .into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, format!("UDF '{name}' not found")).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// DELETE /udfs/:name
///
/// Removal takes effect on the next restart. The UDF is removed from persistent
/// storage immediately but stays registered in the running instance. We do not
/// attempt to scan pipeline SQL (fragile) or unregister from the running
/// registry (requires robust reference detection).
pub async fn delete_udf_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let name = match canonical_udf_path_name(&name) {
        Ok(name) => name,
        Err(resp) => return *resp,
    };
    let stored = match state.storage.get_udf(&name) {
        Ok(Some(udf)) => udf,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, format!("UDF '{name}' not found")).into_response();
        }
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    // Drop the stored record. The UDF stays registered in the running instance
    // until the next restart.
    if let Err(e) = state.storage.delete_udf(&name) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    // Garbage-collect the WASM file if no other UDF references the same hash
    let remaining = state
        .storage
        .list_udfs()
        .map(|udfs| udfs.iter().any(|u| u.wasm_sha256 == stored.wasm_sha256))
        .unwrap_or(false);

    if !remaining {
        let wasm_path = state
            .storage
            .wasm_files_dir()
            .join(format!("{}.wasm", stored.wasm_sha256));
        let _ = std::fs::remove_file(&wasm_path);
    }

    (StatusCode::OK, Json(serde_json::json!({"deleted": name}))).into_response()
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::canonical_udf_path_name;

    #[test]
    fn canonical_udf_path_name_lowercases_then_validates() {
        // Uppercase input is canonicalized to lowercase, matching the
        // case-insensitive registry, then accepted by the resource-id grammar.
        assert_eq!(
            canonical_udf_path_name("MyUdf").expect("valid after canonicalization"),
            "myudf"
        );
        assert_eq!(
            canonical_udf_path_name("  Add_One  ").expect("trim + lowercase"),
            "add_one"
        );
    }

    #[test]
    fn canonical_udf_path_name_rejects_invalid_canonical_name() {
        // A hyphen survives lowercasing and is rejected by the grammar.
        let resp = canonical_udf_path_name("bad-udf").expect_err("rejected");
        assert_eq!(resp.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
