//! File sink connector for writing encoded delivery units to local files.

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
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
    pub filename_prefix: String,
    pub filename_suffix: String,
    pub retention: FileRetentionConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileRetentionConfig {
    pub max_file_count: u64,
    pub max_file_age_days: u64,
}

#[derive(Clone)]
pub(crate) struct FileSinkConnector {
    id: String,
    config: FileSinkConfig,
    tmp_scope: String,
}

impl FileSinkConnector {
    pub fn new(id: impl Into<String>, config: FileSinkConfig) -> Self {
        Self {
            id: id.into(),
            tmp_scope: tmp_scope(&config.pipeline_id, &config.sink_name),
            config,
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
        validate_file_name_affixes(&self.config.filename_prefix, &self.config.filename_suffix)
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

    fn write_payload(&self, file: &mut File, payload: &[u8]) -> Result<(), SinkConnectorError> {
        file.write_all(payload).map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to write payload: {err}",
                self.id
            ))
        })?;
        file.sync_all().map_err(|err| {
            SinkConnectorError::Other(format!(
                "file sink `{}` failed to flush payload: {err}",
                self.id
            ))
        })
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
                        final_path.display()
                    )));
                }
                Err(err) => {
                    return Err(SinkConnectorError::Other(format!(
                        "file sink `{}` failed to finalize tmp file `{}` to `{}`: {err}",
                        self.id,
                        tmp_path.display(),
                        final_path.display()
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
            self.config.filename_prefix, ts_ms, seq, self.config.filename_suffix
        ))
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
                        path = %path.display(),
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
        let rest = file_name.strip_prefix(&self.config.filename_prefix)?;
        let middle = rest.strip_suffix(&self.config.filename_suffix)?;
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

    fn prepare_blocking(&self) -> Result<(), SinkConnectorError> {
        self.validate_config()?;
        self.ensure_dirs_exist()?;
        self.cleanup_orphaned_tmp_files()
    }

    fn send_blocking(&self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        self.validate_config()?;
        self.ensure_dirs_exist()?;
        let ts_ms = current_epoch_millis()?;
        let (tmp_path, mut file) = self.begin_tmp_file()?;
        let result = (|| {
            self.write_payload(&mut file, payload)?;
            drop(file);
            let _final_path = self.finalize_tmp_file(&tmp_path, ts_ms)?;
            self.apply_retention()
        })();
        if result.is_err() {
            self.abort_tmp_file(&tmp_path);
        }
        result
    }

    async fn run_blocking<T>(
        &self,
        operation: impl FnOnce() -> Result<T, SinkConnectorError> + Send + 'static,
    ) -> Result<T, SinkConnectorError>
    where
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(|err| {
                SinkConnectorError::Other(format!(
                    "file sink `{}` blocking task failed: {err}",
                    self.id
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
        let state = self.clone();
        self.run_blocking(move || state.prepare_blocking()).await
    }

    async fn send(&mut self, payload: &[u8]) -> Result<(), SinkConnectorError> {
        let state = self.clone();
        let payload = payload.to_vec();
        self.run_blocking(move || state.send_blocking(&payload))
            .await
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
            filename_prefix: "speed_".to_string(),
            filename_suffix: ".json".to_string(),
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

    #[tokio::test]
    async fn file_sink_writes_one_payload_to_one_final_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut connector = FileSinkConnector::new("sink_1", config(dir.path()));

        connector.ready().await.expect("ready");
        connector.send(br#"{"speed":42}"#).await.expect("send");

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
        connector
            .write_payload(&mut tmp, b"new")
            .expect("write tmp");
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
        cfg.filename_prefix.clear();
        cfg.filename_suffix.clear();
        let mut connector = FileSinkConnector::new("sink_1", cfg);

        connector.ready().await.expect("ready");
        connector.send(b"bytes").await.expect("send");

        let files = final_files(dir.path());
        assert_eq!(files.len(), 1);
        let name = files[0].file_name().unwrap().to_str().unwrap();
        assert!(name.ends_with("_000001"));
        assert_eq!(fs::read(&files[0]).expect("read file"), b"bytes");
    }

    #[tokio::test]
    async fn file_sink_send_does_not_cleanup_tmp_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut connector = FileSinkConnector::new("sink_1", config(dir.path()));
        connector.ready().await.expect("ready");
        let orphan = connector.tmp_dir().join("orphan.tmp");
        fs::write(&orphan, b"orphan").expect("write orphan");

        connector.send(b"bytes").await.expect("send");

        assert!(orphan.exists(), "send must not run tmp cleanup");
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
