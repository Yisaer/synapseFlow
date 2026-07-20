use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Serialize;

use crate::pipeline::AppState;
use crate::resource_id::validate_upload_file_name;

#[derive(Serialize)]
pub struct UploadFileResponse {
    pub name: String,
    pub size_bytes: u64,
}

/// POST /files/upload
///
/// Accepts multipart form data with fields:
/// - `name` (text): target file name
/// - `file` (binary): file contents
///
/// Overwrites if a file with the same name already exists.
pub async fn upload_file_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let _permit = match state.try_acquire_import_export_op() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another import/export/upload operation is in progress",
            )
                .into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "operation guard closed").into_response();
        }
    };

    let mut name: Option<String> = None;
    let mut file_bytes: Option<Vec<u8>> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "name" => {
                name = field.text().await.ok();
            }
            "file" => {
                file_bytes = field.bytes().await.ok().map(|b| b.to_vec());
            }
            _ => {}
        }
    }

    let name = match name {
        Some(n) if !n.trim().is_empty() => n.trim().to_string(),
        _ => return (StatusCode::BAD_REQUEST, "field 'name' is required").into_response(),
    };

    if let Err(err) = validate_upload_file_name(&name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let file_bytes = match file_bytes {
        Some(b) if !b.is_empty() => b,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "field 'file' is required and must not be empty",
            )
                .into_response();
        }
    };

    let size_bytes = file_bytes.len() as u64;

    match state.storage.save_upload(&name, &file_bytes) {
        Ok(()) => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to save uploaded file: {e}"),
            )
                .into_response();
        }
    }

    (
        StatusCode::OK,
        Json(UploadFileResponse { name, size_bytes }),
    )
        .into_response()
}

/// GET /files
///
/// Lists all files in the uploads directory.
pub async fn list_files_handler(State(state): State<AppState>) -> impl IntoResponse {
    let files = match state.storage.list_uploads() {
        Ok(files) => files,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to list uploads: {e}"),
            )
                .into_response();
        }
    };

    (StatusCode::OK, Json(files)).into_response()
}

/// GET /files/:name
///
/// Downloads the contents of an uploaded file.
pub async fn get_file_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_upload_file_name(&name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    match state.storage.read_upload(&name) {
        Ok(data) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            data,
        )
            .into_response(),
        Err(storage::StorageError::Io(e))
            if e.contains("not found") || e.contains("No such file") =>
        {
            (StatusCode::NOT_FOUND, format!("file '{name}' not found")).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to read file: {e}"),
        )
            .into_response(),
    }
}

/// DELETE /files/:name
///
/// Deletes a file from the uploads directory.
pub async fn delete_file_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(err) = validate_upload_file_name(&name) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    let _permit = match state.try_acquire_import_export_op() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            return (
                StatusCode::CONFLICT,
                "another import/export/upload operation is in progress",
            )
                .into_response();
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "operation guard closed").into_response();
        }
    };

    match state.storage.delete_upload(&name) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({"deleted": name}))).into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, format!("file '{name}' not found")).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to delete file: {e}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instances::{DEFAULT_FLOW_INSTANCE_ID, FlowInstanceSpec};
    use crate::new_default_flow_instance;
    use axum::body::to_bytes;
    use axum::extract::FromRequest;
    use axum::http::StatusCode;
    use std::io::Write;
    use storage::UploadFileInfo;

    fn test_state() -> AppState {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage::StorageManager::new(dir.path()).unwrap();
        AppState::new(
            new_default_flow_instance(),
            storage,
            vec![FlowInstanceSpec {
                id: DEFAULT_FLOW_INSTANCE_ID.to_string(),
                ..FlowInstanceSpec::default()
            }],
            0,
        )
        .unwrap()
    }

    fn multipart_body(fields: &[(&str, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        let boundary = "test_boundary_12345";
        for (name, data) in fields {
            write!(
                &mut body,
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n"
            )
            .unwrap();
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        write!(&mut body, "--{boundary}--\r\n").unwrap();
        body
    }

    async fn multipart_from_body(body: &[u8]) -> Multipart {
        let boundary = "test_boundary_12345";
        let content_type =
            axum::http::HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}"))
                .unwrap();
        let request = axum::http::Request::builder()
            .header(axum::http::header::CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(body.to_vec()))
            .unwrap();
        Multipart::from_request(request, &())
            .await
            .expect("valid multipart")
    }

    #[tokio::test]
    async fn upload_then_list_and_get() {
        let state = test_state();
        let body = multipart_body(&[("name", b"test.txt"), ("file", b"hello world")]);
        let mp = multipart_from_body(&body).await;

        let resp = upload_file_handler(State(state.clone()), mp)
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let list_body = to_bytes(
            list_files_handler(State(state.clone()))
                .await
                .into_response()
                .into_body(),
            1024,
        )
        .await
        .unwrap();
        let list: Vec<UploadFileInfo> = serde_json::from_slice(&list_body).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "test.txt");
        assert_eq!(list[0].size_bytes, 11);

        let get_resp = get_file_handler(State(state.clone()), Path("test.txt".to_string()))
            .await
            .into_response();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let data = to_bytes(get_resp.into_body(), 1024).await.unwrap();
        assert_eq!(&data[..], b"hello world");
    }

    #[tokio::test]
    async fn upload_overwrites_existing_file() {
        let state = test_state();

        let body1 = multipart_body(&[("name", b"conf.txt"), ("file", b"old")]);
        let resp1 = upload_file_handler(State(state.clone()), multipart_from_body(&body1).await)
            .await
            .into_response();
        assert_eq!(resp1.status(), StatusCode::OK);

        let body2 = multipart_body(&[("name", b"conf.txt"), ("file", b"new")]);
        let resp2 = upload_file_handler(State(state.clone()), multipart_from_body(&body2).await)
            .await
            .into_response();
        assert_eq!(resp2.status(), StatusCode::OK);

        let get_resp = get_file_handler(State(state.clone()), Path("conf.txt".to_string()))
            .await
            .into_response();
        let data = to_bytes(get_resp.into_body(), 1024).await.unwrap();
        assert_eq!(&data[..], b"new");
    }

    #[tokio::test]
    async fn delete_file_returns_not_found_for_missing() {
        let state = test_state();
        let resp = delete_file_handler(State(state), Path("nonexistent.txt".to_string()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_file_works_after_upload() {
        let state = test_state();
        let body = multipart_body(&[("name", b"tmp.txt"), ("file", b"data")]);
        let _ = upload_file_handler(State(state.clone()), multipart_from_body(&body).await)
            .await
            .into_response();

        let resp = delete_file_handler(State(state.clone()), Path("tmp.txt".to_string()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let get_resp = get_file_handler(State(state), Path("tmp.txt".to_string()))
            .await
            .into_response();
        assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn upload_rejects_invalid_name() {
        let state = test_state();
        let body = multipart_body(&[("name", b"bad\\name"), ("file", b"x")]);
        let resp = upload_file_handler(State(state), multipart_from_body(&body).await)
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn upload_rejects_missing_file_field() {
        let state = test_state();
        let mut body = Vec::new();
        write!(
            &mut body,
            "--test_boundary_12345\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ntest.txt\r\n--test_boundary_12345--\r\n"
        )
        .unwrap();
        let resp = upload_file_handler(State(state), multipart_from_body(&body).await)
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn upload_nested_path_and_retrieve() {
        let state = test_state();
        let body = multipart_body(&[
            ("name", b"proto/sensor.proto"),
            ("file", b"message Sensor {}"),
        ]);
        let mp = multipart_from_body(&body).await;
        let resp = upload_file_handler(State(state.clone()), mp)
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let list = state.storage.list_uploads().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "proto/sensor.proto");

        let get_resp =
            get_file_handler(State(state.clone()), Path("proto/sensor.proto".to_string()))
                .await
                .into_response();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let data = to_bytes(get_resp.into_body(), 1024).await.unwrap();
        assert_eq!(&data[..], b"message Sensor {}");
    }

    #[tokio::test]
    async fn delete_file_with_nested_path() {
        let state = test_state();
        let body = multipart_body(&[("name", b"a/b/c.txt"), ("file", b"deep")]);
        upload_file_handler(State(state.clone()), multipart_from_body(&body).await)
            .await
            .into_response();

        let resp = delete_file_handler(State(state.clone()), Path("a/b/c.txt".to_string()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let get_resp = get_file_handler(State(state), Path("a/b/c.txt".to_string()))
            .await
            .into_response();
        assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_file_returns_not_found_for_missing() {
        let state = test_state();
        let resp = get_file_handler(State(state), Path("missing.txt".to_string()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
