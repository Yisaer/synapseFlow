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
pub const DEFAULT_FILENAME_PATTERN: &str = "{write_start_ms}_{seq}";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSinkConfig {
    pub sink_name: String,
    pub pipeline_id: String,
    pub path: String,
    pub filename_pattern: crate::ConnectorString,
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
    write_start_ms: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilenamePlaceholder {
    WriteStartMs,
    WriteEndMs,
    Sequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilenamePatternSegment {
    Literal(String),
    Placeholder(FilenamePlaceholder),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompiledFilenamePattern {
    segments: Vec<FilenamePatternSegment>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FilenameCaptures {
    write_start_ms: Option<u128>,
    write_end_ms: Option<u128>,
    seq: Option<u32>,
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
        validate_filename_pattern(self.config.filename_pattern.expose())
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
        write_start_ms: u128,
        write_end_ms: u128,
    ) -> Result<PathBuf, SinkConnectorError> {
        let pattern = compile_filename_pattern(self.config.filename_pattern.expose())
            .map_err(SinkConnectorError::Other)?;
        if pattern.has_seq() {
            for seq in 1..=MAX_SEQUENCE {
                let final_path = self.final_path(&pattern, write_start_ms, write_end_ms, seq);
                match self.publish_tmp_file(tmp_path, &final_path)? {
                    PublishResult::Published => return Ok(final_path),
                    PublishResult::AlreadyExists => continue,
                }
            }
            return Err(SinkConnectorError::Other(format!(
                "file sink `{}` exhausted filename sequence for write range {write_start_ms}..{write_end_ms}",
                self.id,
            )));
        }

        let final_path = self.final_path(&pattern, write_start_ms, write_end_ms, 1);
        match self.publish_tmp_file(tmp_path, &final_path)? {
            PublishResult::Published => Ok(final_path),
            PublishResult::AlreadyExists => Err(SinkConnectorError::Other(format!(
                "file sink `{}` cannot publish `{}` because it already exists",
                self.id,
                self.final_path_for_diagnostics(&final_path)
            ))),
        }
    }

    fn publish_tmp_file(
        &self,
        tmp_path: &Path,
        final_path: &Path,
    ) -> Result<PublishResult, SinkConnectorError> {
        // Link-after-write gives atomic final-name visibility and fails if the target exists.
        match fs::hard_link(tmp_path, final_path) {
            Ok(()) => {
                let _ = fs::remove_file(tmp_path);
                Ok(PublishResult::Published)
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                Ok(PublishResult::AlreadyExists)
            }
            Err(err) if is_cross_device_error(&err) => Err(SinkConnectorError::Other(format!(
                "file sink `{}` cannot finalize tmp file `{}` to `{}` across devices: {err}",
                self.id,
                tmp_path.display(),
                self.final_path_for_diagnostics(final_path)
            ))),
            Err(err) => Err(SinkConnectorError::Other(format!(
                "file sink `{}` failed to finalize tmp file `{}` to `{}`: {err}",
                self.id,
                tmp_path.display(),
                self.final_path_for_diagnostics(final_path)
            ))),
        }
    }

    fn final_path(
        &self,
        pattern: &CompiledFilenamePattern,
        write_start_ms: u128,
        write_end_ms: u128,
        seq: u32,
    ) -> PathBuf {
        self.output_dir()
            .join(pattern.render(write_start_ms, write_end_ms, seq))
    }

    fn final_path_for_diagnostics<'a>(&self, path: &'a Path) -> Cow<'a, str> {
        if self.config.filename_pattern.is_sensitive() {
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
        let pattern = compile_filename_pattern(self.config.filename_pattern.expose())
            .map_err(SinkConnectorError::Other)?;
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
            let Some(file_name) = path
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string)
            else {
                continue;
            };
            let Some(captures) = pattern.captures(&file_name) else {
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
                sort_ms: captures.write_start_ms.or(captures.write_end_ms),
                seq: captures.seq,
                name: file_name,
            });
        }

        files.sort_by(|left, right| match (left.sort_ms, right.sort_ms) {
            (Some(left_ms), Some(right_ms)) => left_ms
                .cmp(&right_ms)
                .then(left.seq.cmp(&right.seq))
                .then(left.name.cmp(&right.name)),
            _ => left
                .modified
                .cmp(&right.modified)
                .then(left.name.cmp(&right.name)),
        });

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
        let write_start_ms = current_epoch_millis()?;
        let (tmp_path, file) = self.begin_tmp_file()?;
        self.current = Some(FileDeliveryState {
            tmp_path,
            file,
            bytes_written: 0,
            write_start_ms,
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
            let write_end_ms = current_epoch_millis()?;
            drop(state.file);
            let _final_path =
                self.finalize_tmp_file(&state.tmp_path, state.write_start_ms, write_end_ms)?;
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
    sort_ms: Option<u128>,
    seq: Option<u32>,
    name: String,
}

enum PublishResult {
    Published,
    AlreadyExists,
}

fn validate_file_sink_path(path: &str) -> Result<(), SinkConnectorError> {
    if path.trim().is_empty() {
        return Err(SinkConnectorError::Other(
            "file sink requires non-empty path".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_filename_pattern(pattern: &str) -> Result<(), String> {
    compile_filename_pattern(pattern).map(|_| ())
}

fn compile_filename_pattern(pattern: &str) -> Result<CompiledFilenamePattern, String> {
    if pattern.is_empty() {
        return Err("filename_pattern must not be empty".to_string());
    }
    if pattern.contains('\0') {
        return Err("filename_pattern must not contain NUL".to_string());
    }
    if pattern.contains('/') || pattern.contains('\\') {
        return Err("filename_pattern must not contain path separators".to_string());
    }
    if matches!(pattern, "." | "..") {
        return Err("filename_pattern must not be `.` or `..`".to_string());
    }

    let mut segments = Vec::new();
    let mut literal_start = 0;
    let mut cursor = 0;
    let mut seen_start = false;
    let mut seen_end = false;
    let mut seen_seq = false;
    while cursor < pattern.len() {
        let Some(relative) = pattern[cursor..].find(['{', '}']) else {
            break;
        };
        let brace = cursor + relative;
        if pattern.as_bytes()[brace] == b'}' {
            return Err("filename_pattern contains an unmatched `}`".to_string());
        }
        if brace > literal_start {
            segments.push(FilenamePatternSegment::Literal(
                pattern[literal_start..brace].to_string(),
            ));
        } else if matches!(
            segments.last(),
            Some(FilenamePatternSegment::Placeholder(_))
        ) {
            return Err("filename_pattern placeholders must be separated by a literal".to_string());
        }
        let Some(close_relative) = pattern[brace + 1..].find('}') else {
            return Err("filename_pattern contains an unclosed placeholder".to_string());
        };
        let close = brace + 1 + close_relative;
        let name = &pattern[brace + 1..close];
        let placeholder = match name {
            "write_start_ms" if !seen_start => {
                seen_start = true;
                FilenamePlaceholder::WriteStartMs
            }
            "write_end_ms" if !seen_end => {
                seen_end = true;
                FilenamePlaceholder::WriteEndMs
            }
            "seq" if !seen_seq => {
                seen_seq = true;
                FilenamePlaceholder::Sequence
            }
            "write_start_ms" | "write_end_ms" | "seq" => {
                return Err(format!(
                    "filename_pattern placeholder `{{{name}}}` must not be repeated"
                ));
            }
            _ => {
                return Err(format!(
                    "filename_pattern contains unsupported placeholder `{{{name}}}`"
                ));
            }
        };
        segments.push(FilenamePatternSegment::Placeholder(placeholder));
        cursor = close + 1;
        literal_start = cursor;
    }
    if literal_start < pattern.len() {
        segments.push(FilenamePatternSegment::Literal(
            pattern[literal_start..].to_string(),
        ));
    }
    Ok(CompiledFilenamePattern { segments })
}

impl CompiledFilenamePattern {
    fn has_seq(&self) -> bool {
        self.segments.iter().any(|segment| {
            matches!(
                segment,
                FilenamePatternSegment::Placeholder(FilenamePlaceholder::Sequence)
            )
        })
    }

    fn render(&self, write_start_ms: u128, write_end_ms: u128, seq: u32) -> String {
        let mut output = String::new();
        for segment in &self.segments {
            match segment {
                FilenamePatternSegment::Literal(literal) => output.push_str(literal),
                FilenamePatternSegment::Placeholder(FilenamePlaceholder::WriteStartMs) => {
                    output.push_str(&write_start_ms.to_string());
                }
                FilenamePatternSegment::Placeholder(FilenamePlaceholder::WriteEndMs) => {
                    output.push_str(&write_end_ms.to_string());
                }
                FilenamePatternSegment::Placeholder(FilenamePlaceholder::Sequence) => {
                    output.push_str(&format!("{seq:06}"));
                }
            }
        }
        output
    }

    fn captures(&self, file_name: &str) -> Option<FilenameCaptures> {
        let mut captures = FilenameCaptures::default();
        match_filename_segments(&self.segments, file_name, &mut captures).then_some(captures)
    }
}

fn match_filename_segments(
    segments: &[FilenamePatternSegment],
    input: &str,
    captures: &mut FilenameCaptures,
) -> bool {
    let Some((segment, remaining_segments)) = segments.split_first() else {
        return input.is_empty();
    };
    match segment {
        FilenamePatternSegment::Literal(literal) => {
            input.strip_prefix(literal).is_some_and(|remaining| {
                match_filename_segments(remaining_segments, remaining, captures)
            })
        }
        FilenamePatternSegment::Placeholder(placeholder) => {
            let candidate_ends: Vec<usize> = match remaining_segments.first() {
                Some(FilenamePatternSegment::Literal(literal)) => input
                    .match_indices(literal)
                    .map(|(index, _)| index)
                    .collect(),
                Some(FilenamePatternSegment::Placeholder(_)) => return false,
                None => vec![input.len()],
            };
            for end in candidate_ends {
                let value = &input[..end];
                let mut candidate = *captures;
                if capture_filename_placeholder(*placeholder, value, &mut candidate)
                    && match_filename_segments(remaining_segments, &input[end..], &mut candidate)
                {
                    *captures = candidate;
                    return true;
                }
            }
            false
        }
    }
}

fn capture_filename_placeholder(
    placeholder: FilenamePlaceholder,
    value: &str,
    captures: &mut FilenameCaptures,
) -> bool {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    match placeholder {
        FilenamePlaceholder::WriteStartMs => {
            captures.write_start_ms = value.parse().ok();
            captures.write_start_ms.is_some()
        }
        FilenamePlaceholder::WriteEndMs => {
            captures.write_end_ms = value.parse().ok();
            captures.write_end_ms.is_some()
        }
        FilenamePlaceholder::Sequence => {
            captures.seq = (value.len() == 6)
                .then(|| value.parse().ok())
                .flatten()
                .filter(|seq| (1..=MAX_SEQUENCE).contains(seq));
            captures.seq.is_some()
        }
    }
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
            filename_pattern: "speed_{write_start_ms}_{seq}.json".into(),
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
    async fn file_sink_renders_write_start_and_end_in_final_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern = "speed_{write_start_ms}_{write_end_ms}_{seq}.json".into();
        let mut connector = FileSinkConnector::new("sink_1", cfg);

        connector.ready().await.expect("ready");
        deliver(&mut connector, b"bytes").await;

        let files = final_files(dir.path());
        let file_name = files[0].file_name().unwrap().to_str().unwrap();
        let pattern = compile_filename_pattern(connector.config.filename_pattern.expose())
            .expect("compile pattern");
        let captures = pattern.captures(file_name).expect("match final name");
        assert!(captures.write_start_ms.is_some());
        assert!(captures.write_end_ms.is_some());
        assert!(captures.write_end_ms >= captures.write_start_ms);
        assert_eq!(captures.seq, Some(1));
    }

    #[test]
    fn filename_pattern_renders_and_matches_write_range() {
        let pattern = compile_filename_pattern("speed_{write_start_ms}_{write_end_ms}_{seq}.json")
            .expect("compile pattern");

        let name = pattern.render(1_000, 1_025, 7);

        assert_eq!(name, "speed_1000_1025_000007.json");
        assert_eq!(
            pattern.captures(&name),
            Some(FilenameCaptures {
                write_start_ms: Some(1_000),
                write_end_ms: Some(1_025),
                seq: Some(7),
            })
        );
        assert!(pattern.captures("speed_1000_bad_000007.json").is_none());
    }

    #[test]
    fn filename_pattern_renders_static_and_timestamp_only_names() {
        let static_name = compile_filename_pattern("VIN-123-928_V7.zst").expect("compile");
        assert_eq!(static_name.render(1, 2, 3), "VIN-123-928_V7.zst");
        assert_eq!(
            static_name.captures("VIN-123-928_V7.zst"),
            Some(FilenameCaptures::default())
        );
        assert!(static_name.captures("VIN-123-928_V7.json").is_none());

        let start_only = compile_filename_pattern("{write_start_ms}.zst").expect("compile");
        assert_eq!(start_only.render(1_700, 1_800, 9), "1700.zst");
        assert_eq!(
            start_only.captures("1700.zst"),
            Some(FilenameCaptures {
                write_start_ms: Some(1_700),
                write_end_ms: None,
                seq: None,
            })
        );
    }

    #[test]
    fn filename_pattern_validation_rejects_unsafe_or_ambiguous_patterns() {
        let cases = [
            ("", "must not be empty"),
            ("nested/{write_start_ms}_{seq}", "path separators"),
            ("{write_start_ms}{seq}", "must be separated"),
            ("speed_\0_{seq}.json", "NUL"),
            (
                "{write_start_ms}_{unknown}_{seq}",
                "unsupported placeholder",
            ),
            ("{write_start_ms}_{seq}_{seq}", "must not be repeated"),
        ];

        for (pattern, expected) in cases {
            let error = compile_filename_pattern(pattern).expect_err("pattern must fail");
            assert!(
                error.contains(expected),
                "unexpected error for {pattern:?}: {error}"
            );
        }
    }

    #[test]
    fn filename_pattern_allows_optional_runtime_placeholders() {
        for pattern in [
            DEFAULT_FILENAME_PATTERN,
            "{write_start_ms}_{write_end_ms}_{seq}.zst",
            "{write_start_ms}.zst",
            "{seq}.json",
            "VIN-123-928_V7.zst",
        ] {
            compile_filename_pattern(pattern).expect(pattern);
        }
    }

    #[test]
    fn filename_pattern_accepts_seq_less_names() {
        validate_filename_pattern("{write_start_ms}.zst").expect("timestamp-only pattern");
        validate_filename_pattern("latest.json").expect("static name");
        validate_filename_pattern(DEFAULT_FILENAME_PATTERN).expect("default pattern");
    }

    #[tokio::test]
    async fn file_sink_exclusive_create_retry_handles_existing_final_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let connector = FileSinkConnector::new("sink_1", config(dir.path()));
        connector.ensure_dirs_exist().expect("ready dirs");
        let write_start_ms = current_epoch_millis().expect("clock");
        let write_end_ms = write_start_ms + 1;
        let pattern =
            compile_filename_pattern(connector.config.filename_pattern.expose()).expect("pattern");
        fs::write(
            connector.final_path(&pattern, write_start_ms, write_end_ms, 1),
            b"existing",
        )
        .expect("write existing");
        let (tmp_path, mut tmp) = connector.begin_tmp_file().expect("tmp");
        tmp.write_all(b"new").expect("write tmp");
        tmp.sync_all().expect("sync tmp");
        drop(tmp);

        let final_path = connector
            .finalize_tmp_file(&tmp_path, write_start_ms, write_end_ms)
            .expect("finalize");

        assert!(final_path.ends_with(format!("speed_{write_start_ms}_000002.json")));
        assert_eq!(fs::read(final_path).expect("read final"), b"new");
    }

    #[tokio::test]
    async fn file_sink_static_name_does_not_overwrite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern = "latest.json".into();
        let connector = FileSinkConnector::new("sink_1", cfg);
        connector.ensure_dirs_exist().expect("ready dirs");
        let existing = dir.path().join("latest.json");
        fs::write(&existing, b"existing").expect("write existing");
        let (tmp_path, mut tmp) = connector.begin_tmp_file().expect("tmp");
        tmp.write_all(b"new").expect("write tmp");
        tmp.sync_all().expect("sync tmp");
        drop(tmp);

        let err = connector
            .finalize_tmp_file(&tmp_path, 1, 2)
            .expect_err("existing static name must fail");

        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(fs::read(&existing).expect("read existing"), b"existing");
        assert!(
            tmp_path.exists(),
            "failed publish must leave tmp for abort cleanup"
        );
    }

    #[tokio::test]
    async fn file_sink_static_name_publishes_without_seq() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern = "VIN-123-928_V7.zst".into();
        let mut connector = FileSinkConnector::new("sink_1", cfg);

        connector.ready().await.expect("ready");
        deliver(&mut connector, b"payload").await;

        let files = final_files(dir.path());
        assert_eq!(files, vec![dir.path().join("VIN-123-928_V7.zst")]);
        assert_eq!(fs::read(&files[0]).expect("read file"), b"payload");
    }

    #[tokio::test]
    async fn file_sink_seq_less_pattern_fails_second_delivery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern = "events_{write_start_ms}.json".into();
        let connector = FileSinkConnector::new("sink_1", cfg);
        connector.ensure_dirs_exist().expect("ready dirs");
        let write_start_ms = 1_700_000_000_000;
        let existing = connector.final_path(
            &compile_filename_pattern(connector.config.filename_pattern.expose()).expect("pattern"),
            write_start_ms,
            write_start_ms + 1,
            1,
        );
        fs::write(&existing, b"existing").expect("write existing");
        let (tmp_path, mut tmp) = connector.begin_tmp_file().expect("tmp");
        tmp.write_all(b"new").expect("write tmp");
        tmp.sync_all().expect("sync tmp");
        drop(tmp);

        let err = connector
            .finalize_tmp_file(&tmp_path, write_start_ms, write_start_ms + 1)
            .expect_err("collision without seq must fail");
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(fs::read(&existing).expect("read existing"), b"existing");
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
    async fn file_sink_retention_without_timestamp_uses_mtime_then_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern = "keep_{seq}.json".into();
        cfg.retention.max_file_count = 2;
        let connector = FileSinkConnector::new("sink_1", cfg);
        connector.ensure_dirs_exist().expect("ready dirs");

        let older = dir.path().join("keep_000001.json");
        let newer = dir.path().join("keep_000002.json");
        let newest = dir.path().join("keep_000003.json");
        fs::write(&older, b"old").expect("old");
        fs::write(&newer, b"mid").expect("mid");
        fs::write(&newest, b"new").expect("new");
        fs::write(dir.path().join("other_000001.json"), b"other").expect("other");

        let old_mtime = SystemTime::now()
            .checked_sub(Duration::from_secs(120))
            .expect("mtime");
        let mid_mtime = SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .expect("mtime");
        fs::File::open(&older)
            .expect("open old")
            .set_modified(old_mtime)
            .expect("set old mtime");
        fs::File::open(&newer)
            .expect("open mid")
            .set_modified(mid_mtime)
            .expect("set mid mtime");

        connector.apply_retention().expect("retention");

        assert!(!older.exists());
        assert!(newer.exists());
        assert!(newest.exists());
        assert!(dir.path().join("other_000001.json").exists());
    }

    #[tokio::test]
    async fn file_sink_default_pattern_is_supported() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern = DEFAULT_FILENAME_PATTERN.into();
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
    async fn file_sink_exposes_sensitive_pattern_only_for_file_operations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = config(dir.path());
        cfg.filename_pattern =
            crate::ConnectorString::sensitive("VIN-123_{write_start_ms}_{seq}.json");
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
