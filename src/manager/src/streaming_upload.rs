use std::path::{Path, PathBuf};

use axum::extract::{
    Multipart,
    multipart::{Field, MultipartError, MultipartRejection},
};
use axum::http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use storage::StorageManager;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub(crate) const MAX_UPLOAD_FILE_SIZE: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_MULTIPART_BODY_SIZE: usize = MAX_UPLOAD_FILE_SIZE as usize + 1024 * 1024;
pub(crate) const MAX_TEXT_FIELD_SIZE: u64 = 64 * 1024;

pub(crate) struct UploadFailure {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
}

impl UploadFailure {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn too_large() -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: format!(
                "uploaded file exceeds the maximum size of {} bytes",
                MAX_UPLOAD_FILE_SIZE
            ),
        }
    }

    pub(crate) fn multipart(context: &str, err: MultipartError) -> Self {
        Self {
            status: err.status(),
            message: format!("{context}: {}", err.body_text()),
        }
    }
}

pub(crate) struct TemporaryUpload {
    path: PathBuf,
    size_bytes: u64,
    cleanup_armed: bool,
}

impl TemporaryUpload {
    pub(crate) async fn receive(
        storage: &StorageManager,
        mut field: Field<'_>,
        extension: Option<&str>,
    ) -> Result<Self, UploadFailure> {
        let tmp_dir = storage.uploads_tmp_dir();
        tokio::fs::create_dir_all(&tmp_dir).await.map_err(|err| {
            UploadFailure::internal(format!("create temporary upload directory: {err}"))
        })?;
        let suffix = extension
            .filter(|value| !value.is_empty())
            .map(|value| format!(".{value}"))
            .unwrap_or_default();
        let path = tmp_dir.join(format!(".upload-{}{suffix}", Uuid::new_v4()));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|err| {
                UploadFailure::internal(format!("create temporary upload file: {err}"))
            })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(err) =
                tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).await
            {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(UploadFailure::internal(format!(
                    "set temporary upload permissions: {err}"
                )));
            }
        }

        let mut size_bytes = 0u64;
        loop {
            let chunk = match field.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(err) => {
                    drop(file);
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(UploadFailure::multipart("read multipart file field", err));
                }
            };
            size_bytes = match size_bytes.checked_add(chunk.len() as u64) {
                Some(size) if size <= MAX_UPLOAD_FILE_SIZE => size,
                _ => {
                    drop(file);
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(UploadFailure::too_large());
                }
            };
            if let Err(err) = file.write_all(&chunk).await {
                drop(file);
                let _ = tokio::fs::remove_file(&path).await;
                return Err(UploadFailure::internal(format!(
                    "write temporary upload file: {err}"
                )));
            }
        }

        if let Err(err) = file.flush().await {
            drop(file);
            let _ = tokio::fs::remove_file(&path).await;
            return Err(UploadFailure::internal(format!(
                "flush temporary upload file: {err}"
            )));
        }
        drop(file);

        if size_bytes == 0 {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(UploadFailure::bad_request(
                "field 'file' is required and must not be empty",
            ));
        }

        Ok(Self {
            path,
            size_bytes,
            cleanup_armed: true,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) async fn persist_as(
        mut self,
        storage: &StorageManager,
        name: &str,
    ) -> Result<u64, UploadFailure> {
        let target = storage.uploads_dir().join(name);
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                UploadFailure::internal(format!("create upload target directory: {err}"))
            })?;
        }
        tokio::fs::rename(&self.path, &target)
            .await
            .map_err(|err| UploadFailure::internal(format!("persist uploaded file: {err}")))?;
        self.cleanup_armed = false;
        Ok(self.size_bytes)
    }
}

impl Drop for TemporaryUpload {
    fn drop(&mut self) {
        if self.cleanup_armed
            && let Err(err) = std::fs::remove_file(&self.path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.path.display(),
                error = %err,
                "failed to remove temporary upload"
            );
        }
    }
}

pub(crate) async fn read_text_field(
    mut field: Field<'_>,
    field_name: &str,
) -> Result<String, UploadFailure> {
    let mut bytes = Vec::new();
    loop {
        let chunk = field.chunk().await.map_err(|err| {
            UploadFailure::multipart(&format!("read multipart field '{field_name}'"), err)
        })?;
        let Some(chunk) = chunk else {
            break;
        };
        let size = bytes.len() as u64 + chunk.len() as u64;
        if size > MAX_TEXT_FIELD_SIZE {
            return Err(UploadFailure::bad_request(format!(
                "multipart field '{field_name}' exceeds {MAX_TEXT_FIELD_SIZE} bytes"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|err| {
        UploadFailure::bad_request(format!(
            "multipart field '{field_name}' is not UTF-8: {err}"
        ))
    })
}

pub(crate) fn is_zip_filename(filename: Option<&str>) -> bool {
    filename
        .and_then(|value| Path::new(value).extension())
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("zip"))
}

pub(crate) fn required_multipart(
    headers: &HeaderMap,
    multipart: Result<Multipart, MultipartRejection>,
) -> Result<Multipart, Box<Response>> {
    match multipart {
        Ok(multipart) => Ok(multipart),
        Err(err) => {
            let is_multipart = headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    value
                        .split(';')
                        .next()
                        .is_some_and(|media_type| media_type.trim() == "multipart/form-data")
                });
            if is_multipart {
                Err(Box::new(
                    (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
                ))
            } else {
                Err(Box::new(
                    (
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        "Content-Type must be multipart/form-data",
                    )
                        .into_response(),
                ))
            }
        }
    }
}
