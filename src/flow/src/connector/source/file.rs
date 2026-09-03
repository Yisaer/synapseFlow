use crate::catalog::FileSourceFraming;
use crate::checkpoint::CheckpointState;
use crate::connector::{
    ConnectorError, ConnectorEvent, ConnectorStream, SourceCheckpointRequest, SourceConnector,
};
use crate::processor::base::normalize_channel_capacity;
use crate::runtime::TaskSpawner;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Arc};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

const SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(100);
const FILENAME_LEN_BYTES: usize = 4;
#[cfg(not(test))]
const READ_BUFFER_SIZE: usize = 64 * 1024;
#[cfg(test)]
const READ_BUFFER_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    pub path: PathBuf,
    pub framing: FileSourceFraming,
}

impl FileSourceConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            framing: FileSourceFraming::default(),
        }
    }

    pub fn with_framing(mut self, framing: FileSourceFraming) -> Self {
        self.framing = framing;
        self
    }
}

pub(crate) struct FileSourceConnector {
    id: String,
    config: FileSourceConfig,
    channel_capacity: usize,
    shutdown: Option<Arc<AtomicBool>>,
    checkpoint_tx: Option<std_mpsc::Sender<FileSourceCommand>>,
    restored_state: Option<FileSourceRestoreState>,
    spawner: TaskSpawner,
}

struct FileSourceRestoreState {
    cursors: HashMap<PathBuf, FileCursor>,
}

enum FileSourceCommand {
    Checkpoint {
        checkpoint_id: u64,
        response: oneshot::Sender<Result<Option<CheckpointState>, ConnectorError>>,
    },
}

impl FileSourceConnector {
    pub fn new(id: impl Into<String>, config: FileSourceConfig, spawner: TaskSpawner) -> Self {
        Self {
            id: id.into(),
            config,
            channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            shutdown: None,
            checkpoint_tx: None,
            restored_state: None,
            spawner,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = normalize_channel_capacity(capacity);
        self
    }
}

impl SourceConnector for FileSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.shutdown.is_some() {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }

        let source_path = validate_source_path(&self.config.path)?;
        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        self.shutdown = Some(Arc::clone(&shutdown));
        let (checkpoint_tx, checkpoint_rx) = std_mpsc::channel();
        self.checkpoint_tx = Some(checkpoint_tx);
        let connector_id = self.id.clone();
        let framing = self.config.framing.clone();
        let restored_state = self.restored_state.take();

        let _task = self.spawner.spawn_blocking(move || {
            run_file_source(
                connector_id,
                source_path,
                framing,
                sender,
                shutdown,
                checkpoint_rx,
                restored_state,
            );
        });

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    fn request_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<SourceCheckpointRequest, ConnectorError> {
        let checkpoint_tx = self.checkpoint_tx.as_ref().ok_or_else(|| {
            ConnectorError::Other(format!("file source `{}` is not active", self.id))
        })?;
        let (response_tx, response_rx) = oneshot::channel();
        checkpoint_tx
            .send(FileSourceCommand::Checkpoint {
                checkpoint_id,
                response: response_tx,
            })
            .map_err(|_| {
                ConnectorError::Other(format!(
                    "file source `{}` checkpoint worker is not available",
                    self.id
                ))
            })?;
        Ok(Box::pin(async move {
            response_rx.await.map_err(|_| {
                ConnectorError::Other("file source checkpoint response was dropped".to_string())
            })?
        }))
    }

    fn restore_checkpoint(&mut self, state: &CheckpointState) -> Result<(), ConnectorError> {
        if self.shutdown.is_some() {
            return Err(ConnectorError::Other(format!(
                "file source `{}` checkpoint must be restored before subscribe",
                self.id
            )));
        }
        self.restored_state = Some(parse_restore_state(state, &self.config.path)?);
        Ok(())
    }

    fn validate_checkpoint(&self, state: &CheckpointState) -> Result<(), ConnectorError> {
        parse_restore_state(state, &self.config.path).map(|_| ())
    }

    fn clear_checkpoint_restore(&mut self) {
        self.restored_state = None;
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.store(true, Ordering::Release);
        }
        self.checkpoint_tx.take();
        info!(connector_id = %self.id, "file source closed");
        Ok(())
    }
}

fn validate_source_path(path: &Path) -> Result<PathBuf, ConnectorError> {
    let canonical = fs::canonicalize(path).map_err(|err| {
        ConnectorError::Other(format!(
            "file stream path `{}` does not exist or is not accessible: {err}",
            path.display()
        ))
    })?;
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        ConnectorError::Other(format!(
            "failed to inspect file stream path `{}`: {err}",
            path.display()
        ))
    })?;
    if !metadata.is_file() && !metadata.is_dir() {
        return Err(ConnectorError::Other(format!(
            "file stream path `{}` must be a regular file or directory",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn parse_restore_state(
    state: &CheckpointState,
    configured_source_path: &Path,
) -> Result<FileSourceRestoreState, ConnectorError> {
    let state = checkpoint_map(state, "root state")?;
    let connector_kind = checkpoint_string(
        required_checkpoint_field(state, "connector_kind")?,
        "connector_kind",
    )?;
    if connector_kind != "file" {
        return Err(invalid_checkpoint(format!(
            "connector_kind must be `file`, got `{connector_kind}`"
        )));
    }

    let configured_source_path = validate_source_path(configured_source_path)?;
    let source_path = PathBuf::from(checkpoint_string(
        required_checkpoint_field(state, "source_path")?,
        "source_path",
    )?);
    if source_path != configured_source_path {
        return Err(invalid_checkpoint(format!(
            "source path mismatch: expected `{}`, got `{}`",
            configured_source_path.display(),
            source_path.display()
        )));
    }

    let metadata = fs::symlink_metadata(&configured_source_path).map_err(|err| {
        ConnectorError::Other(format!(
            "failed to inspect file stream path `{}` while restoring checkpoint: {err}",
            configured_source_path.display()
        ))
    })?;
    let expected_mode = if metadata.is_file() {
        "file"
    } else if metadata.is_dir() {
        "directory"
    } else {
        return Err(invalid_checkpoint(format!(
            "configured source path `{}` is not a regular file or directory",
            configured_source_path.display()
        )));
    };
    let mode = checkpoint_string(required_checkpoint_field(state, "mode")?, "mode")?;
    if mode != expected_mode {
        return Err(invalid_checkpoint(format!(
            "source mode mismatch: expected `{expected_mode}`, got `{mode}`"
        )));
    }

    let cursors = match required_checkpoint_field(state, "cursors")? {
        CheckpointState::Array(cursors) => cursors,
        _ => return Err(invalid_checkpoint("cursors must be an array")),
    };
    let mut restored_cursors = HashMap::with_capacity(cursors.len());
    let mut seen_paths = HashSet::with_capacity(cursors.len());
    for (index, cursor) in cursors.iter().enumerate() {
        let cursor = checkpoint_map(cursor, &format!("cursor at index {index}"))?;
        let path = PathBuf::from(checkpoint_string(
            required_checkpoint_field(cursor, "path")?,
            "cursor.path",
        )?);
        if path.as_os_str().is_empty() {
            return Err(invalid_checkpoint(format!(
                "cursor at index {index} has an empty path"
            )));
        }
        let path_is_valid = if expected_mode == "file" {
            path == configured_source_path
        } else {
            path.parent() == Some(configured_source_path.as_path()) && path.file_name().is_some()
        };
        if !path_is_valid {
            return Err(invalid_checkpoint(format!(
                "cursor path `{}` is outside source `{}`",
                path.display(),
                configured_source_path.display()
            )));
        }
        if !seen_paths.insert(path.clone()) {
            return Err(invalid_checkpoint(format!(
                "duplicate cursor path `{}`",
                path.display()
            )));
        }

        let offset = checkpoint_unsigned(
            required_checkpoint_field(cursor, "offset")?,
            "cursor.offset",
        )?;
        let file_identity = checkpoint_optional_string(
            required_checkpoint_field(cursor, "file_identity")?,
            "cursor.file_identity",
        )?;
        if file_identity.as_ref().is_some_and(String::is_empty) {
            return Err(invalid_checkpoint(format!(
                "cursor `{}` has an empty file_identity",
                path.display()
            )));
        }
        if !path.is_absolute() {
            return Err(invalid_checkpoint(format!(
                "cursor path `{}` must be absolute",
                path.display()
            )));
        }

        restored_cursors.insert(
            path.clone(),
            FileCursor {
                path,
                offset,
                read_offset: offset,
                pending: Vec::new(),
                file_identity,
            },
        );
    }

    Ok(FileSourceRestoreState {
        cursors: restored_cursors,
    })
}

fn required_checkpoint_field<'a>(
    state: &'a BTreeMap<String, CheckpointState>,
    field: &str,
) -> Result<&'a CheckpointState, ConnectorError> {
    state
        .get(field)
        .ok_or_else(|| invalid_checkpoint(format!("missing `{field}`")))
}

fn checkpoint_map<'a>(
    state: &'a CheckpointState,
    label: &str,
) -> Result<&'a BTreeMap<String, CheckpointState>, ConnectorError> {
    match state {
        CheckpointState::Map(state) => Ok(state),
        _ => Err(invalid_checkpoint(format!("{label} must be a map"))),
    }
}

fn checkpoint_string<'a>(
    state: &'a CheckpointState,
    label: &str,
) -> Result<&'a str, ConnectorError> {
    match state {
        CheckpointState::String(value) => Ok(value),
        _ => Err(invalid_checkpoint(format!("{label} must be a string"))),
    }
}

fn checkpoint_unsigned(state: &CheckpointState, label: &str) -> Result<u64, ConnectorError> {
    match state {
        CheckpointState::Unsigned(value) => Ok(*value),
        _ => Err(invalid_checkpoint(format!("{label} must be unsigned"))),
    }
}

fn checkpoint_optional_string(
    state: &CheckpointState,
    label: &str,
) -> Result<Option<String>, ConnectorError> {
    match state {
        CheckpointState::Null => Ok(None),
        CheckpointState::String(value) => Ok(Some(value.clone())),
        _ => Err(invalid_checkpoint(format!(
            "{label} must be null or a string"
        ))),
    }
}

fn invalid_checkpoint(message: impl Into<String>) -> ConnectorError {
    ConnectorError::Other(format!(
        "invalid file source checkpoint: {}",
        message.into()
    ))
}

fn run_file_source(
    connector_id: String,
    source_path: PathBuf,
    framing: FileSourceFraming,
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    shutdown: Arc<AtomicBool>,
    checkpoint_rx: std_mpsc::Receiver<FileSourceCommand>,
    restored_state: Option<FileSourceRestoreState>,
) {
    info!(
        connector_id = %connector_id,
        path = %source_path.display(),
        "starting file source"
    );

    let mut state = match FileSourceState::new_with_framing(source_path, framing, restored_state) {
        Ok(state) => state,
        Err(err) => {
            let _ = sender.blocking_send(Err(err));
            return;
        }
    };

    let (event_tx, event_rx) = std_mpsc::channel();
    let mut watcher = match RecommendedWatcher::new(
        move |result| {
            let _ = event_tx.send(result);
        },
        Config::default(),
    ) {
        Ok(watcher) => watcher,
        Err(err) => {
            let _ = sender.blocking_send(Err(ConnectorError::Other(format!(
                "failed to create file watcher: {err}"
            ))));
            return;
        }
    };

    if let Err(err) = watcher.watch(state.watch_path(), RecursiveMode::NonRecursive) {
        let _ = sender.blocking_send(Err(ConnectorError::Other(format!(
            "failed to watch file stream path `{}`: {err}",
            state.watch_path().display()
        ))));
        return;
    }

    if state.read_initial_files(&sender).is_err() {
        return;
    }

    while !shutdown.load(Ordering::Acquire) {
        match checkpoint_rx.try_recv() {
            Ok(FileSourceCommand::Checkpoint {
                checkpoint_id,
                response,
            }) => {
                let result = state.snapshot();
                match result {
                    Ok(snapshot) => {
                        let _ = sender
                            .blocking_send(Ok(ConnectorEvent::CheckpointFence { checkpoint_id }));
                        let _ = response.send(Ok(Some(snapshot)));
                    }
                    Err(err) => {
                        let _ = sender.blocking_send(Err(err.clone()));
                        let _ = response.send(Err(err));
                        return;
                    }
                }
            }
            Err(std_mpsc::TryRecvError::Empty) => {}
            Err(std_mpsc::TryRecvError::Disconnected) => return,
        }
        match event_rx.recv_timeout(SHUTDOWN_CHECK_INTERVAL) {
            Ok(Ok(event)) => {
                if state.handle_event(event, &sender).is_err() {
                    return;
                }
            }
            Ok(Err(err)) => {
                warn!(connector_id = %connector_id, error = %err, "file watcher event error");
                let _ = sender.blocking_send(Err(ConnectorError::Other(format!(
                    "file watcher event error: {err}"
                ))));
                return;
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                warn!(connector_id = %connector_id, "file watcher event channel closed");
                return;
            }
        }
    }
}

struct FileSourceState {
    source_path: PathBuf,
    watch_path: PathBuf,
    framing: FileSourceFraming,
    mode: FileSourceMode,
    cursors: HashMap<PathBuf, FileCursor>,
}

enum FileSourceMode {
    File { target: PathBuf },
    Directory,
}

impl FileSourceState {
    #[cfg(test)]
    fn new(
        source_path: PathBuf,
        restored_state: Option<FileSourceRestoreState>,
    ) -> Result<Self, ConnectorError> {
        Self::new_with_framing(
            source_path,
            FileSourceFraming::Delimiter {
                delimiter: b"\n".to_vec(),
                include_delimiter: false,
            },
            restored_state,
        )
    }

    fn new_with_framing(
        source_path: PathBuf,
        framing: FileSourceFraming,
        restored_state: Option<FileSourceRestoreState>,
    ) -> Result<Self, ConnectorError> {
        let metadata = fs::symlink_metadata(&source_path).map_err(|err| {
            ConnectorError::Other(format!(
                "failed to inspect file stream path `{}`: {err}",
                source_path.display()
            ))
        })?;
        if metadata.is_file() {
            let watch_path = source_path.parent().ok_or_else(|| {
                ConnectorError::Other(format!(
                    "file stream path `{}` has no parent directory",
                    source_path.display()
                ))
            })?;
            return Ok(Self {
                source_path: source_path.clone(),
                watch_path: watch_path.to_path_buf(),
                framing,
                mode: FileSourceMode::File {
                    target: source_path.clone(),
                },
                cursors: restored_state
                    .map(|state| state.cursors)
                    .unwrap_or_default(),
            });
        }
        if metadata.is_dir() {
            return Ok(Self {
                source_path: source_path.clone(),
                watch_path: source_path,
                framing,
                mode: FileSourceMode::Directory,
                cursors: restored_state
                    .map(|state| state.cursors)
                    .unwrap_or_default(),
            });
        }
        Err(ConnectorError::Other(format!(
            "file stream path `{}` must be a regular file or directory",
            source_path.display()
        )))
    }

    fn watch_path(&self) -> &Path {
        &self.watch_path
    }

    fn read_initial_files(
        &mut self,
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        let files = match &self.mode {
            FileSourceMode::File { target } => vec![target.clone()],
            FileSourceMode::Directory => {
                let entries = fs::read_dir(&self.source_path).map_err(|err| {
                    error!(
                        path = %self.source_path.display(),
                        error = %err,
                        "failed to read file stream directory"
                    );
                    let _ = sender.blocking_send(Err(ConnectorError::Other(format!(
                        "failed to read file stream directory `{}`: {err}",
                        self.source_path.display()
                    ))));
                })?;
                let mut files = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(|err| {
                        error!(error = %err, "failed to inspect file stream directory entry");
                        let _ = sender.blocking_send(Err(ConnectorError::Other(format!(
                            "failed to inspect file stream directory entry: {err}"
                        ))));
                    })?;
                    let path = entry.path();
                    if is_regular_file_without_symlink(&path).unwrap_or(false) {
                        files.push(path);
                    }
                }
                files.sort();
                files
            }
        };

        for path in files {
            self.read_path(path, sender)?;
        }
        Ok(())
    }

    fn handle_event(
        &mut self,
        event: Event,
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        if matches!(event.kind, EventKind::Remove(_)) {
            for path in event.paths {
                if let Some(path) = self.normalize_event_path(&path) {
                    self.cursors.remove(&path);
                }
            }
            return Ok(());
        }

        for path in event.paths {
            let Some(path) = self.normalize_event_path(&path) else {
                continue;
            };
            self.read_path(path, sender)?;
        }
        Ok(())
    }

    fn normalize_event_path(&self, path: &Path) -> Option<PathBuf> {
        match &self.mode {
            FileSourceMode::File { target } => {
                let event_name = path.file_name()?;
                let target_name = target.file_name()?;
                (event_name == target_name).then(|| target.clone())
            }
            FileSourceMode::Directory => {
                let parent = path.parent()?;
                let filename = path.file_name()?;
                (parent == self.source_path).then(|| self.source_path.join(filename))
            }
        }
    }

    fn read_path(
        &mut self,
        path: PathBuf,
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        let cursor = self
            .cursors
            .entry(path.clone())
            .or_insert_with(|| FileCursor::new(path));
        cursor.read_available(&self.framing, sender)
    }

    fn snapshot(&mut self) -> Result<CheckpointState, ConnectorError> {
        let mut files = Vec::with_capacity(self.cursors.len());
        for cursor in self.cursors.values_mut() {
            cursor.refresh_metadata()?;
            files.push(cursor.snapshot());
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));

        let mut state = BTreeMap::new();
        state.insert(
            "connector_kind".to_string(),
            CheckpointState::String("file".to_string()),
        );
        state.insert(
            "source_path".to_string(),
            CheckpointState::String(self.source_path.to_string_lossy().into_owned()),
        );
        state.insert(
            "mode".to_string(),
            CheckpointState::String(
                match self.mode {
                    FileSourceMode::File { .. } => "file",
                    FileSourceMode::Directory => "directory",
                }
                .to_string(),
            ),
        );
        state.insert(
            "cursors".to_string(),
            CheckpointState::Array(files.into_iter().map(|(_, snapshot)| snapshot).collect()),
        );
        Ok(CheckpointState::Map(state))
    }
}

struct FileCursor {
    path: PathBuf,
    /// Byte position immediately after the last payload sent to the pipeline.
    offset: u64,
    /// Next byte position to read during the current connector run.
    read_offset: u64,
    pending: Vec<u8>,
    file_identity: Option<String>,
}

impl FileCursor {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            read_offset: 0,
            pending: Vec::new(),
            file_identity: None,
        }
    }

    fn refresh_metadata(&mut self) -> Result<(), ConnectorError> {
        let Some(metadata) = regular_file_metadata_without_symlink(&self.path).map_err(|err| {
            ConnectorError::Other(format!(
                "failed to inspect file stream source file `{}`: {err}",
                self.path.display()
            ))
        })?
        else {
            return Ok(());
        };
        self.reconcile_metadata(&metadata);
        Ok(())
    }

    fn reconcile_metadata(&mut self, metadata: &fs::Metadata) {
        let current_identity = file_identity(metadata);
        let identity_changed = self
            .file_identity
            .as_ref()
            .zip(current_identity.as_ref())
            .is_some_and(|(previous, current)| previous != current);
        if identity_changed || metadata.len() < self.offset {
            self.offset = 0;
            self.read_offset = 0;
            self.pending.clear();
        } else if metadata.len() < self.read_offset {
            self.read_offset = self.offset;
            self.pending.clear();
        }
        self.file_identity = current_identity;
    }

    fn snapshot(&self) -> (String, CheckpointState) {
        let mut state = BTreeMap::new();
        state.insert(
            "path".to_string(),
            CheckpointState::String(self.path.to_string_lossy().into_owned()),
        );
        state.insert("offset".to_string(), CheckpointState::Unsigned(self.offset));
        state.insert(
            "file_identity".to_string(),
            self.file_identity
                .clone()
                .map(CheckpointState::String)
                .unwrap_or(CheckpointState::Null),
        );
        (
            self.path.to_string_lossy().into_owned(),
            CheckpointState::Map(state),
        )
    }

    fn read_available(
        &mut self,
        framing: &FileSourceFraming,
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        let metadata = match regular_file_metadata_without_symlink(&self.path).map_err(|err| {
            warn!(
                path = %self.path.display(),
                error = %err,
                "failed to inspect file stream source file"
            );
        })? {
            Some(metadata) => metadata,
            None => return Ok(()),
        };
        let file_len = metadata.len();
        self.reconcile_metadata(&metadata);
        if file_len == self.read_offset {
            return Ok(());
        }

        let mut file = open_regular_file_without_following_symlink(&self.path).map_err(|err| {
            warn!(
                path = %self.path.display(),
                error = %err,
                "failed to open file stream source file"
            );
        })?;
        let metadata = file.metadata().map_err(|err| {
            warn!(
                path = %self.path.display(),
                error = %err,
                "failed to inspect opened file stream source file"
            );
        })?;
        if !metadata.is_file() {
            return Ok(());
        }
        self.reconcile_metadata(&metadata);
        let file_len = metadata.len();
        if file_len == self.read_offset {
            return Ok(());
        }

        file.seek(SeekFrom::Start(self.read_offset))
            .map_err(|err| {
                warn!(
                    path = %self.path.display(),
                    offset = self.read_offset,
                    error = %err,
                    "failed to seek file stream source file"
                );
            })?;

        let mut buffer = [0_u8; READ_BUFFER_SIZE];
        let mut remaining = file_len - self.read_offset;
        while remaining > 0 {
            let read_len = if remaining > buffer.len() as u64 {
                buffer.len()
            } else {
                remaining as usize
            };
            let bytes_read = file.read(&mut buffer[..read_len]).map_err(|err| {
                warn!(
                    path = %self.path.display(),
                    error = %err,
                    "failed to read file stream source file"
                );
            })?;
            if bytes_read == 0 {
                break;
            }
            self.read_offset += bytes_read as u64;
            remaining -= bytes_read as u64;
            self.pending.extend_from_slice(&buffer[..bytes_read]);
        }

        self.emit_framed(framing, sender)?;
        Ok(())
    }

    fn emit_framed(
        &mut self,
        framing: &FileSourceFraming,
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        match framing {
            FileSourceFraming::AppendBatch => {
                if self.pending.is_empty() {
                    return Ok(());
                }
                self.emit_payload(&self.pending, sender)?;
                self.offset += self.pending.len() as u64;
                self.pending.clear();
            }
            FileSourceFraming::Delimiter {
                delimiter,
                include_delimiter,
            } => {
                while let Some(delimiter_start) = find_subslice(&self.pending, delimiter) {
                    let consumed = delimiter_start + delimiter.len();
                    let mut payload_end = if *include_delimiter {
                        consumed
                    } else {
                        delimiter_start
                    };
                    if !include_delimiter
                        && delimiter == b"\n"
                        && payload_end > 0
                        && self.pending[payload_end - 1] == b'\r'
                    {
                        payload_end -= 1;
                    }
                    self.emit_payload(&self.pending[..payload_end], sender)?;
                    self.pending.drain(..consumed);
                    self.offset += consumed as u64;
                }
            }
        }

        Ok(())
    }

    fn emit_payload(
        &self,
        payload: &[u8],
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        let payload = encode_file_line_frame(&self.path, payload).map_err(|err| {
            let _ = sender.blocking_send(Err(err));
        })?;
        if sender
            .blocking_send(Ok(ConnectorEvent::Payload(payload)))
            .is_err()
        {
            debug!(path = %self.path.display(), "file source receiver closed");
            return Err(());
        }
        Ok(())
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn is_regular_file_without_symlink(path: &Path) -> std::io::Result<bool> {
    Ok(regular_file_metadata_without_symlink(path)?.is_some())
}

fn regular_file_metadata_without_symlink(path: &Path) -> std::io::Result<Option<fs::Metadata>> {
    let metadata = fs::symlink_metadata(path)?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() || !file_type.is_file() {
        return Ok(None);
    }
    Ok(Some(metadata))
}

fn file_identity(metadata: &fs::Metadata) -> Option<String> {
    #[cfg(unix)]
    {
        Some(format!("{}:{}", metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

fn open_regular_file_without_following_symlink(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

pub(crate) fn encode_file_line_frame(path: &Path, line: &[u8]) -> Result<Vec<u8>, ConnectorError> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ConnectorError::Other(format!(
                "file stream path `{}` does not have a valid UTF-8 filename",
                path.display()
            ))
        })?;
    let filename_len = u32::try_from(filename.len()).map_err(|_| {
        ConnectorError::Other(format!(
            "file stream filename `{filename}` exceeds maximum frame length"
        ))
    })?;
    let mut payload = Vec::with_capacity(FILENAME_LEN_BYTES + filename.len() + line.len());
    payload.extend_from_slice(&filename_len.to_le_bytes());
    payload.extend_from_slice(filename.as_bytes());
    payload.extend_from_slice(line);
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use tempfile::tempdir;
    use tokio::sync::mpsc::error::TryRecvError;

    fn write_file(path: &Path, contents: &[u8]) {
        let mut file = File::create(path).expect("create file");
        file.write_all(contents).expect("write file");
    }

    fn append_file(path: &Path, contents: &[u8]) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open file for append");
        file.write_all(contents).expect("append file");
    }

    fn decode_frame(payload: &[u8]) -> (String, String) {
        let mut len_bytes = [0_u8; FILENAME_LEN_BYTES];
        len_bytes.copy_from_slice(&payload[..FILENAME_LEN_BYTES]);
        let filename_len = u32::from_le_bytes(len_bytes) as usize;
        let filename_start = FILENAME_LEN_BYTES;
        let filename_end = filename_start + filename_len;
        let filename =
            String::from_utf8(payload[filename_start..filename_end].to_vec()).expect("filename");
        let line = String::from_utf8(payload[filename_end..].to_vec()).expect("line");
        (filename, line)
    }

    fn drain_payloads(
        receiver: &mut mpsc::Receiver<Result<ConnectorEvent, ConnectorError>>,
    ) -> Vec<(String, String)> {
        let mut payloads = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(Ok(ConnectorEvent::Payload(payload))) => {
                    payloads.push(decode_frame(&payload));
                }
                Ok(Ok(other)) => panic!("unexpected connector event: {other:?}"),
                Ok(Err(err)) => panic!("unexpected connector error: {err}"),
                Err(TryRecvError::Empty) => return payloads,
                Err(TryRecvError::Disconnected) => panic!("receiver disconnected"),
            }
        }
    }

    #[test]
    fn file_source_reads_initial_complete_lines_and_strips_crlf() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"first\nsecond\r\npartial");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path, None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");

        assert_eq!(
            drain_payloads(&mut receiver),
            vec![
                ("app.log".to_string(), "first".to_string()),
                ("app.log".to_string(), "second".to_string()),
            ]
        );
    }

    #[test]
    fn file_source_buffers_partial_line_until_newline() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"part");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path.clone(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert!(drain_payloads(&mut receiver).is_empty());

        append_file(&path, b"ial\n");
        state
            .read_path(path, &sender)
            .expect("read appended complete line");

        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "partial".to_string())]
        );
    }

    #[test]
    fn file_source_append_batch_emits_each_observed_growth_as_one_payload() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"a\nb\n");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state =
            FileSourceState::new_with_framing(path.clone(), FileSourceFraming::AppendBatch, None)
                .expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial batch");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "a\nb\n".to_string())]
        );

        append_file(&path, b"c\n d\n");
        state.read_path(path, &sender).expect("read appended batch");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "c\n d\n".to_string())]
        );
    }

    #[test]
    fn file_source_delimiter_framing_emits_multiple_records_and_keeps_partial_tail() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"first|second");
        let framing = FileSourceFraming::Delimiter {
            delimiter: b"|".to_vec(),
            include_delimiter: false,
        };
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new_with_framing(path.clone(), framing.clone(), None)
            .expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial delimiter records");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "first".to_string())]
        );

        append_file(&path, b"|third|");
        state
            .read_path(path, &sender)
            .expect("read appended delimiter records");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![
                ("app.log".to_string(), "second".to_string()),
                ("app.log".to_string(), "third".to_string()),
            ]
        );
    }

    #[test]
    fn file_source_checkpoint_restores_from_last_emitted_offset() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"complete\npartial");
        let path = fs::canonicalize(path).expect("canonical file path");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path.clone(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "complete".to_string())]
        );

        let cursor = state.cursors.get(&path).expect("file cursor");
        assert_eq!(cursor.offset, b"complete\n".len() as u64);
        assert_eq!(cursor.read_offset, b"complete\npartial".len() as u64);
        assert_eq!(cursor.pending, b"partial");

        let snapshot = state.snapshot().expect("snapshot file source");
        let snapshot_map = checkpoint_map(&snapshot, "root state").expect("snapshot map");
        let CheckpointState::Array(cursors) = snapshot_map.get("cursors").expect("cursors") else {
            panic!("cursors must be an array");
        };
        let cursor_snapshot = checkpoint_map(&cursors[0], "cursor").expect("cursor snapshot");
        assert_eq!(
            cursor_snapshot.get("offset"),
            Some(&CheckpointState::Unsigned(b"complete\n".len() as u64))
        );
        assert!(!cursor_snapshot.contains_key("pending"));
        assert!(!cursor_snapshot.contains_key("file_length"));
        assert!(!cursor_snapshot.contains_key("last_update_unix_ms"));

        let restored = parse_restore_state(&snapshot, &path).expect("parse checkpoint");
        append_file(&path, b"-done\n");
        let (restored_sender, mut restored_receiver) = mpsc::channel(8);
        let mut restored_state =
            FileSourceState::new(path, Some(restored)).expect("restored file source state");
        restored_state
            .read_initial_files(&restored_sender)
            .expect("read restored file");

        assert_eq!(
            drain_payloads(&mut restored_receiver),
            vec![("app.log".to_string(), "partial-done".to_string())]
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_source_checkpoint_resets_offset_after_file_replacement() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        let replacement = dir.path().join("replacement.log");
        write_file(&path, b"old\n");
        write_file(&replacement, b"replacement\n");
        let path = fs::canonicalize(path).expect("canonical file path");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path.clone(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "old".to_string())]
        );
        let snapshot = state.snapshot().expect("snapshot file source");
        let restored = parse_restore_state(&snapshot, &path).expect("parse checkpoint");

        fs::remove_file(&path).expect("remove original file");
        fs::rename(&replacement, &path).expect("install replacement file");
        let (restored_sender, mut restored_receiver) = mpsc::channel(8);
        let mut restored_state =
            FileSourceState::new(path, Some(restored)).expect("restored file source state");
        restored_state
            .read_initial_files(&restored_sender)
            .expect("read replacement file");

        assert_eq!(
            drain_payloads(&mut restored_receiver),
            vec![("app.log".to_string(), "replacement".to_string())]
        );
    }

    #[test]
    fn file_source_resets_offset_after_truncate() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"old line one\nold line two\n");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path.clone(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![
                ("app.log".to_string(), "old line one".to_string()),
                ("app.log".to_string(), "old line two".to_string()),
            ]
        );

        write_file(&path, b"new\n");
        state.read_path(path, &sender).expect("read after truncate");

        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "new".to_string())]
        );
    }

    #[test]
    fn file_source_directory_reads_direct_files_and_ignores_nested_events() {
        let dir = tempdir().expect("tempdir");
        let direct = dir.path().join("direct.log");
        let nested_dir = dir.path().join("nested");
        let nested = nested_dir.join("nested.log");
        fs::create_dir(&nested_dir).expect("create nested dir");
        write_file(&direct, b"direct\n");
        write_file(&nested, b"nested\n");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state =
            FileSourceState::new(dir.path().to_path_buf(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("direct.log".to_string(), "direct".to_string())]
        );

        let nested_event = Event::new(EventKind::Any).add_path(nested);
        state
            .handle_event(nested_event, &sender)
            .expect("handle nested event");
        assert!(drain_payloads(&mut receiver).is_empty());

        let new_direct = dir.path().join("new.log");
        write_file(&new_direct, b"new\n");
        let direct_event = Event::new(EventKind::Any).add_path(new_direct);
        state
            .handle_event(direct_event, &sender)
            .expect("handle direct event");

        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("new.log".to_string(), "new".to_string())]
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_source_directory_ignores_symlink_created_after_start() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("tempdir");
        let outside = tempdir().expect("outside tempdir");
        let target = outside.path().join("secret.log");
        let link = dir.path().join("link.log");
        write_file(&target, b"secret\n");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state =
            FileSourceState::new(dir.path().to_path_buf(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert!(drain_payloads(&mut receiver).is_empty());

        symlink(&target, &link).expect("create symlink");
        let event = Event::new(EventKind::Any).add_path(link);
        state
            .handle_event(event, &sender)
            .expect("handle symlink event");

        assert!(drain_payloads(&mut receiver).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn file_source_file_mode_ignores_target_replaced_by_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().expect("tempdir");
        let outside = tempdir().expect("outside tempdir");
        let path = dir.path().join("app.log");
        let target = outside.path().join("secret.log");
        write_file(&path, b"visible\n");
        write_file(&target, b"secret\n");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path.clone(), None).expect("file source state");

        state
            .read_initial_files(&sender)
            .expect("read initial files");
        assert_eq!(
            drain_payloads(&mut receiver),
            vec![("app.log".to_string(), "visible".to_string())]
        );

        fs::remove_file(&path).expect("remove original file");
        symlink(&target, &path).expect("replace with symlink");
        let event = Event::new(EventKind::Any).add_path(path);
        state
            .handle_event(event, &sender)
            .expect("handle symlink event");

        assert!(drain_payloads(&mut receiver).is_empty());
    }
}
