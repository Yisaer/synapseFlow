//! File sink connector for writing encoded delivery units to local files.

use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const TMP_DIR_NAME: &str = ".veloflux_tmp";
const MAX_SEQUENCE: u32 = 999_999;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSinkConfig {
    pub sink_name: String,
    pub pipeline_id: String,
    pub path: String,
    pub filename_prefix: crate::ConnectorString,
    pub filename_suffix: crate::ConnectorString,
    pub retention: FileRetentionConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileRetentionConfig {
    pub max_file_count: u64,
    pub max_file_age_days: u64,
}

pub(crate) struct FileSinkConnector {
    id: String,
    config: FileSinkConfig,
    tmp_scope: String,
    current: Option<FileDeliveryState>,
}

struct FileDeliveryState {
    tmp_path: PathBuf,
    file: File,
    bytes_written: u64,
    ts_ms: u128,
}

impl FileSinkConnector {
    pub fn new(id: impl Into<String>, config: FileSinkConfig) -> Self {
        Self {
            id: id.into(),
            tmp_scope: tmp_scope(&config.pipeline_id, &config.sink_name),
            config,
            current: None,
        }
    }

    fn output_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.path)
    }

    fn tmp_root_dir(&self) -> PathBuf {
        self.output_dir().join(TMP_DIR_NAME)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.tmp_root_dir().join(&self.tmp_scope)
    }

    fn validate_config(&self) -> Result<(), SinkConnectorError> {
        validate_file_sink_path(&self.config.path)?;
        validate_file_name_affixes(
            self.config.filename_prefix.expose(),
            self.config.filename_suffix.expose(),
        )
        .map_err(SinkConnectorError::Other)?;
        Ok(())
    }

    fn ensure_dirs_exist(&self) -> Result<(), SinkConnectorError> {
        let output_dir = self.output_dir();
        fs::create_dir_all(&output_dir).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to create output directory `{}`: {err}",
                self.id,
                output_dir.display()
            ))
        })?;

        let tmp_root_dir = self.tmp_root_dir();
        fs::create_dir_all(&tmp_root_dir).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to create tmp root directory `{}`: {err}",
                self.id,
                tmp_root_dir.display()
            ))
        })?;
        self.ensure_real_directory(&tmp_root_dir, "tmp root directory")?;

        let tmp_dir = self.tmp_dir();
        fs::create_dir_all(&tmp_dir).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to create tmp directory `{}`: {err}",
                self.id,
                tmp_dir.display()
            ))
        })?;
        self.ensure_real_directory(&tmp_dir, "tmp directory")?;
        Ok(())
    }

    fn ensure_real_directory(&self, path: &Path, label: &str) -> Result<(), SinkConnectorError> {
        let metadata = fs::symlink_metadata(path).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to inspect {label} `{}`: {err}",
                self.id,
                path.display()
            ))
        })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() || !file_type.is_dir() {
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` requires {label} `{}` to be a real directory",
                self.id,
                path.display()
            )));
        }
        Ok(())
    }

    fn cleanup_orphaned_tmp_files(&self) -> Result<(), SinkConnectorError> {
        let tmp_dir = self.tmp_dir();
        match fs::symlink_metadata(&tmp_dir) {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                if file_type.is_symlink() || !file_type.is_dir() {
                    return Err(SinkConnectorError::Other(format!(
                        "file sink `{}` requires tmp directory `{}` to be a real directory",
                        self.id,
                        tmp_dir.display()
                    )));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(SinkConnectorError::Other(format!(
                    "file sink `{}` failed to inspect tmp directory `{}`: {err}",
                    self.id,
                    tmp_dir.display()
                )));
            }
        }

        for entry in fs::read_dir(&tmp_dir).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to read tmp directory `{}`: {err}",
                self.id,
                tmp_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                SinkConnectorError::Other(format!(
                    "file sink `{}` failed to inspect tmp directory `{}`: {err}",
                    self.id,
                    tmp_dir.display()
                ))
            })?;
            remove_path_best_effort(&entry.path());
        }
        Ok(())
    }

    fn begin_tmp_file(&self) -> Result<(PathBuf, File), SinkConnectorError> {
        let tmp_path = self
            .tmp_dir()
            .join(format!("{}.tmp", Uuid::new_v4().as_simple()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|err| {
                SinkConnectorError::Other(format!(
                    "file sink `{}` failed to create tmp file `{}`: {err}",
                    self.id,
                    tmp_path.display()
                ))
            })?;
        Ok((tmp_path, file))
    }

    fn finalize_tmp_file(
        &self,
        tmp_path: &Path,
        ts_ms: u128,
    ) -> Result<PathBuf, SinkConnectorError> {
        for seq in 1..=MAX_SEQUENCE {
            let final_path = self.final_path(ts_ms, seq);
            // Link-after-write gives atomic final-name visibility and fails if the target exists.
            match fs::hard_link(tmp_path, &final_path) {
                Ok(()) => {
                    let _ = fs::remove_file(tmp_path);
                    return Ok(final_path);
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) if is_cross_device_error(&err) => {
                    return Err(SinkConnectorError::Other(format!(
                        "file sink `{}` cannot finalize tmp file `{}` to `{}` across devices: {err}",
                        self.id,
                        tmp_path.display(),
                        self.final_path_for_diagnostics(&final_path)
                    )));
                }
                Err(err) => {
                    return Err(SinkConnectorError::Other(format!(
                        "file sink `{}` failed to finalize tmp file `{}` to `{}`: {err}",
                        self.id,
                        tmp_path.display(),
                        self.final_path_for_diagnostics(&final_path)
                    )));
                }
            }
        }

        Err(SinkConnectorError::Other(format!(
            "file sink `{}` exhausted filename sequence for timestamp {ts_ms}",
            self.id
        )))
    }

    fn final_path(&self, ts_ms: u128, seq: u32) -> PathBuf {
        self.output_dir().join(format!(
            "{}{}_{:06}{}",
            self.config.filename_prefix.expose(),
            ts_ms,
            seq,
            self.config.filename_suffix.expose()
        ))
    }

    fn final_path_for_diagnostics<'a>(&self, path: &'a Path) -> Cow<'a, str> {
        if self.config.filename_prefix.is_sensitive() || self.config.filename_suffix.is_sensitive()
        {
            Cow::Borrowed("<redacted>")
        } else {
            Cow::Owned(path.display().to_string())
        }
    }

    fn apply_retention(&self) -> Result<(), SinkConnectorError> {
        let retention = &self.config.retention;
        if retention.max_file_count == 0 && retention.max_file_age_days == 0 {
            return Ok(());
        }

        let output_dir = self.output_dir();
        let mut files = Vec::new();
        for entry in fs::read_dir(&output_dir).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to read output directory `{}` for retention: {err}",
                self.id,
                output_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                SinkConnectorError::Other(format!(
                    "file sink `{}` failed to inspect output directory `{}` for retention: {err}",
                    self.id,
                    output_dir.display()
                ))
            })?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let Some((ts_ms, seq)) = self.generated_filename_parts(file_name) else {
                continue;
            };
            let modified = match entry.metadata().and_then(|metadata| metadata.modified()) {
                Ok(modified) => Some(modified),
                Err(err) => {
                    tracing::warn!(
                        connector_id = %self.id,
                        path = %self.final_path_for_diagnostics(&path),
                        error = %err,
                        "file sink retention skipped file with unreadable modified time"
                    );
                    None
                }
            };
            files.push(RetentionFile {
                path,
                modified,
                ts_ms,
                seq,
            });
        }

        files.sort_by_key(|file| (file.ts_ms, file.seq));

        if retention.max_file_age_days > 0 {
            let cutoff = SystemTime::now()
                .checked_sub(Duration::from_secs(
                    retention.max_file_age_days.saturating_mul(24 * 60 * 60),
                ))
                .unwrap_or(UNIX_EPOCH);
            files.retain(|file| {
                let Some(modified) = file.modified else {
                    return true;
                };
                if modified <= cutoff {
                    let _ = fs::remove_file(&file.path);
                    false
                } else {
                    true
                }
            });
        }

        if retention.max_file_count > 0 {
            let keep = retention.max_file_count as usize;
            if files.len() > keep {
                for file in files.iter().take(files.len() - keep) {
                    let _ = fs::remove_file(&file.path);
                }
            }
        }

        Ok(())
    }

    fn generated_filename_parts(&self, file_name: &str) -> Option<(u128, u32)> {
        let rest = file_name.strip_prefix(self.config.filename_prefix.expose())?;
        let middle = rest.strip_suffix(self.config.filename_suffix.expose())?;
        let (ts, seq) = middle.rsplit_once('_')?;
        if ts.is_empty()
            || !ts.bytes().all(|byte| byte.is_ascii_digit())
            || seq.len() != 6
            || !seq.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        Some((ts.parse().ok()?, seq.parse().ok()?))
    }

    fn abort_tmp_file(&self, tmp_path: &Path) {
        let _ = fs::remove_file(tmp_path);
    }

    fn cleanup_lost_delivery_tmp_file(&self, tmp_path: &Path, reason: &str) {
        match fs::remove_file(tmp_path) {
            Ok(()) => {
                tracing::warn!(
                    connector_id = %self.id,
                    tmp_path = %tmp_path.display(),
                    reason,
                    "file sink cleaned up lost delivery tmp file"
                );
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    connector_id = %self.id,
                    tmp_path = %tmp_path.display(),
                    reason,
                    error = %err,
                    "file sink failed to clean up lost delivery tmp file"
                );
            }
        }
    }

    fn prepare_blocking(&self) -> Result<(), SinkConnectorError> {
        self.validate_config()?;
        self.ensure_dirs_exist()?;
        self.cleanup_orphaned_tmp_files()
    }

    fn start_delivery_blocking(&mut self) -> Result<(), SinkConnectorError> {
        if self.current.is_some() {
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` already has an active delivery",
                self.id
            )));
        }
        self.validate_config()?;
        self.ensure_dirs_exist()?;
        let ts_ms = current_epoch_millis()?;
        let (tmp_path, file) = self.begin_tmp_file()?;
        self.current = Some(FileDeliveryState {
            tmp_path,
            file,
            bytes_written: 0,
            ts_ms,
        });
        Ok(())
    }

    fn write_chunk_blocking(&mut self, bytes: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(state) = self.current.as_mut() else {
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` received chunk without active delivery",
                self.id
            )));
        };
        state.file.write_all(bytes).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to write payload: {err}",
                self.id
            ))
        })?;
        state.bytes_written += bytes.len() as u64;
        Ok(())
    }

    fn finish_delivery_blocking(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let Some(state) = self.current.take() else {
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` finished without active delivery",
                self.id
            )));
        };
        let result = (|| {
            state.file.sync_all().map_err(|err| {
                SinkConnectorError::Other(format!(
                    "file sink `{}` failed to flush payload: {err}",
                    self.id
                ))
            })?;
            drop(state.file);
            let _final_path = self.finalize_tmp_file(&state.tmp_path, state.ts_ms)?;
            self.apply_retention()?;
            Ok(DeliveryResult {
                bytes_written: state.bytes_written,
            })
        })();
        if result.is_err() {
            self.abort_tmp_file(&state.tmp_path);
        }
        result
    }

    fn abort_delivery_blocking(&mut self) {
        if let Some(state) = self.current.take() {
            drop(state.file);
            self.abort_tmp_file(&state.tmp_path);
        }
    }

    fn blocking_view(&self) -> Self {
        Self {
            id: self.id.clone(),
            config: self.config.clone(),
            tmp_scope: self.tmp_scope.clone(),
            current: None,
        }
    }

    async fn run_blocking<T>(
        connector_id: String,
        operation: impl FnOnce() -> Result<T, SinkConnectorError> + Send + 'static,
    ) -> Result<T, SinkConnectorError>
    where
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(|err| {
                SinkConnectorError::Other(format!(
                    "file sink `{connector_id}` blocking task failed: {err}"
                ))
            })?
    }
}

#[async_trait]
impl SinkConnector for FileSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        let connector_id = self.id.clone();
        let connector = self.blocking_view();
        Self::run_blocking(connector_id, move || connector.prepare_blocking()).await
    }

    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        let connector_id = self.id.clone();
        let mut connector = self.blocking_view();
        let state = Self::run_blocking(connector_id.clone(), move || {
            connector.start_delivery_blocking()?;
            connector.current.take().ok_or_else(|| {
                SinkConnectorError::Other(format!(
                    "file sink `{connector_id}` started without active delivery"
                ))
            })
        })
        .await?;
        self.current = Some(state);
        Ok(())
    }

    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), SinkConnectorError> {
        let Some(state) = self.current.take() else {
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` received chunk without active delivery",
                self.id
            )));
        };

        let connector_id = self.id.clone();
        let tmp_path = state.tmp_path.clone();
        let mut connector = self.blocking_view();
        connector.current = Some(state);
        let payload = bytes.to_vec();
        let result = Self::run_blocking(connector_id.clone(), move || {
            if let Err(err) = connector.write_chunk_blocking(&payload) {
                connector.abort_delivery_blocking();
                return Err(err);
            }
            connector.current.take().ok_or_else(|| {
                SinkConnectorError::Other(format!(
                    "file sink `{connector_id}` wrote chunk without active delivery"
                ))
            })
        })
        .await;
        let state = match result {
            Ok(state) => state,
            Err(err) => {
                self.cleanup_lost_delivery_tmp_file(
                    &tmp_path,
                    "write_chunk failed after state handoff",
                );
                return Err(err);
            }
        };
        self.current = Some(state);
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        let Some(state) = self.current.take() else {
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` finished without active delivery",
                self.id
            )));
        };

        let connector_id = self.id.clone();
        let tmp_path = state.tmp_path.clone();
        let mut connector = self.blocking_view();
        connector.current = Some(state);
        let result =
            Self::run_blocking(connector_id, move || connector.finish_delivery_blocking()).await;
        if result.is_err() {
            self.cleanup_lost_delivery_tmp_file(
                &tmp_path,
                "finish_delivery failed after state handoff",
            );
        }
        result
    }

    async fn abort_delivery(&mut self) {
        let Some(state) = self.current.take() else {
            return;
        };

        let connector_id = self.id.clone();
        let mut connector = self.blocking_view();
        connector.current = Some(state);
        if let Err(err) =
            tokio::task::spawn_blocking(move || connector.abort_delivery_blocking()).await
        {
            tracing::warn!(
                connector_id = %connector_id,
                error = %err,
                "file sink failed to abort delivery in blocking task"
            );
        }
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        Ok(())
    }
}

struct RetentionFile {
    path: PathBuf,
    modified: Option<SystemTime>,
    ts_ms: u128,
    seq: u32,
}

fn validate_file_sink_path(path: &str) -> Result<(), SinkConnectorError> {
    if path.trim().is_empty() {
        return Err(SinkConnectorError::Other(
            "file sink requires non-empty path".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_file_name_affixes(
    filename_prefix: &str,
    filename_suffix: &str,
) -> Result<(), String> {
    validate_file_name_part("filename_prefix", filename_prefix)?;
    validate_file_name_part("filename_suffix", filename_suffix)?;
    if matches!(filename_suffix, "." | "..") {
        return Err("filename_suffix must not be `.` or `..`".to_string());
    }
    Ok(())
}

fn validate_file_name_part(field: &str, value: &str) -> Result<(), String> {
    if value.contains('/') || value.contains('\\') {
        return Err(format!("{field} must not contain path separators"));
    }
    Ok(())
}

fn current_epoch_millis() -> Result<u128, SinkConnectorError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|err| {
            SinkConnectorError::Other(format!("system clock is before UNIX epoch: {err}"))
        })
}

fn is_cross_device_error(err: &io::Error) -> bool {
    is_cross_device_error_impl(err)
}

#[cfg(unix)]
fn is_cross_device_error_impl(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::EXDEV)
}

#[cfg(not(unix))]
fn is_cross_device_error_impl(_err: &io::Error) -> bool {
    false
}

fn remove_path_best_effort(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    let file_type = metadata.file_type();
    if file_type.is_dir() && !file_type.is_symlink() {
        let _ = fs::remove_dir_all(path);
        return;
    }
    let _ = fs::remove_file(path);
}

fn tmp_scope(pipeline_id: &str, sink_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pipeline_id.as_bytes());
    hasher.update([0]);
    hasher.update(sink_name.as_bytes());
    let digest = hasher.finalize();
    format!("{:x}", digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::sink::SinkConnector;

    fn config(path: &Path) -> FileSinkConfig {
        FileSinkConfig {
            sink_name: "file_sink".to_string(),
            pipeline_id: "pipe_1".to_string(),
            path: path.to_string_lossy().into_owned(),
            filename_prefix: "speed_".into(),
            filename_suffix: ".json".into(),
            retention: FileRetentionConfig::default(),
        }
    }

    fn final_files(path: &Path) -> Vec<PathBuf> {
        let mut files: Vec<_> = fs::read_dir(path)
            .expect("read output dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();
        files.sort();
        files
    }

    async fn deliver(connector: &mut FileSinkConnector, bytes: &[u8]) {
        connector.start_delivery().await.expect("start delivery");
        connector.write_chunk(bytes).await.expect("write chunk");
        connector.finish_delivery().await.expect("finish delivery");
    }

    #[tokio::test]
    async fn file_sink_writes_one_payload_to_one_final_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut connector = FileSinkConnector::new("sink_1", config(dir.path()));

        connector.ready().await.expect("ready");
        deliver(&mut connector, br#"{"speed":42}"#).await;

        let files = final_files(dir.path());
        assert_eq!(files.len(), 1);
        let name = files[0].file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("speed_"));
        assert!(name.ends_with(".json"));
        assert_eq!(fs::read(&files[0]).expect("read file"), br#"{"speed":42}"#);
        assert!(connector.tmp_dir().exists());
    }

    #[tokio::test]
    async fn file_sink_exclusive_create_retry_handles_existing_final_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let connector = FileSinkConnector::new("sink_1", config(dir.path()));
        connector.ensure_dirs_exist().expect("ready dirs");
        let ts_ms = current_epoch_millis().expect("clock");
        fs::write(connector.final_path(ts_ms, 1), b"existing").expect("write existing");
        let (tmp_path, mut tmp) = connector.begin_tmp_file().expect("tmp");
        tmp.write_all(b"new").expect("write tmp");
        tmp.sync_all().expect("sync tmp");
        drop(tmp);

        let final_path = connector
            .finalize_tmp_file(&tmp_path, ts_ms)
            .expect("finalize");

        assert!(final_path.ends_with(format!("speed_{ts_ms}_000002.json")));
        assert_eq!(fs::read(final_path).expect("read final"), b"new");
    }

    #[tokio::test]
    async fn file_sink_retention_by_count_prunes_oldest_generated_files_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.retention.max_file_count = 2;
        let connector = FileSinkConnector::new("sink_1", cfg);
        connector.ensure_dirs_exist().expect("ready dirs");
        fs::write(dir.path().join("speed_1000_000001.json"), b"old").expect("old");
        fs::write(dir.path().join("speed_1001_000001.json"), b"mid").expect("mid");
        fs::write(dir.path().join("speed_1002_000001.json"), b"new").expect("new");
        fs::write(dir.path().join("other_1000_000001.json"), b"other").expect("other");

        connector.apply_retention().expect("retention");

        assert!(!dir.path().join("speed_1000_000001.json").exists());
        assert!(dir.path().join("speed_1001_000001.json").exists());
        assert!(dir.path().join("speed_1002_000001.json").exists());
        assert!(dir.path().join("other_1000_000001.json").exists());
    }

    #[tokio::test]
    async fn file_sink_empty_prefix_and_suffix_are_supported() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_prefix = "".into();
        cfg.filename_suffix = "".into();
        let mut connector = FileSinkConnector::new("sink_1", cfg);

        connector.ready().await.expect("ready");
        deliver(&mut connector, b"bytes").await;

        let files = final_files(dir.path());
        assert_eq!(files.len(), 1);
        let name = files[0].file_name().unwrap().to_str().unwrap();
        assert!(name.ends_with("_000001"));
        assert_eq!(fs::read(&files[0]).expect("read file"), b"bytes");
    }

    #[tokio::test]
    async fn file_sink_exposes_sensitive_affixes_only_for_file_operations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_prefix = crate::ConnectorString::sensitive("VIN-123_");
        cfg.filename_suffix = crate::ConnectorString::sensitive(".json");
        let mut connector = FileSinkConnector::new("sink_1", cfg);

        connector.ready().await.expect("ready");
        deliver(&mut connector, b"bytes").await;

        let files = final_files(dir.path());
        let name = files[0].file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("VIN-123_"));
        assert!(name.ends_with(".json"));
        assert_eq!(
            connector.final_path_for_diagnostics(&files[0]),
            "<redacted>"
        );
    }

    #[tokio::test]
    async fn file_sink_delivery_does_not_cleanup_tmp_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut connector = FileSinkConnector::new("sink_1", config(dir.path()));
        connector.ready().await.expect("ready");
        let orphan = connector.tmp_dir().join("orphan.tmp");
        fs::write(&orphan, b"orphan").expect("write orphan");

        deliver(&mut connector, b"bytes").await;

        assert!(orphan.exists(), "delivery must not run tmp cleanup");
    }

    #[tokio::test]
    async fn file_sink_ready_cleans_only_own_tmp_scope() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut own = FileSinkConnector::new("sink_1", config(dir.path()));
        let mut other_cfg = config(dir.path());
        other_cfg.sink_name = "other_sink".to_string();
        let other = FileSinkConnector::new("other_sink", other_cfg);
        own.ensure_dirs_exist().expect("own dirs");
        other.ensure_dirs_exist().expect("other dirs");
        let own_tmp = own.tmp_dir().join("own.tmp");
        let other_tmp = other.tmp_dir().join("other.tmp");
        fs::write(&own_tmp, b"own").expect("write own tmp");
        fs::write(&other_tmp, b"other").expect("write other tmp");

        own.ready().await.expect("ready");

        assert!(!own_tmp.exists(), "ready should cleanup own tmp scope");
        assert!(
            other_tmp.exists(),
            "ready must not cleanup another sink tmp scope"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_sink_ready_removes_symlink_without_following_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let target = tempfile::tempdir().expect("target tempdir");
        let protected = target.path().join("protected.txt");
        fs::write(&protected, b"protected").expect("write protected");

        let mut connector = FileSinkConnector::new("sink_1", config(dir.path()));
        connector.ensure_dirs_exist().expect("dirs");
        let link = connector.tmp_dir().join("linked-dir");
        symlink(target.path(), &link).expect("create symlink");

        connector.ready().await.expect("ready");

        assert!(!link.exists(), "cleanup should remove the symlink itself");
        assert!(
            protected.exists(),
            "cleanup must not follow symlink and delete target contents"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_sink_ready_rejects_symlink_tmp_scope() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let target = tempfile::tempdir().expect("target tempdir");
        let protected = target.path().join("protected.txt");
        fs::write(&protected, b"protected").expect("write protected");

        let mut connector = FileSinkConnector::new("sink_1", config(dir.path()));
        fs::create_dir_all(connector.tmp_root_dir()).expect("tmp root");
        symlink(target.path(), connector.tmp_dir()).expect("create tmp scope symlink");

        let err = connector
            .ready()
            .await
            .expect_err("ready must reject symlink");

        assert!(
            err.to_string().contains("real directory"),
            "unexpected error: {err}"
        );
        assert!(
            protected.exists(),
            "ready must not follow symlinked tmp scope"
        );
    }
}
