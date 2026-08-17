use crate::connector::{ConnectorError, ConnectorEvent, ConnectorStream, SourceConnector};
use crate::processor::base::normalize_channel_capacity;
use crate::runtime::TaskSpawner;
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Arc};
use std::time::Duration;
use tokio::sync::mpsc;
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
}

impl FileSourceConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

pub(crate) struct FileSourceConnector {
    id: String,
    config: FileSourceConfig,
    channel_capacity: usize,
    shutdown: Option<Arc<AtomicBool>>,
    spawner: TaskSpawner,
}

impl FileSourceConnector {
    pub fn new(id: impl Into<String>, config: FileSourceConfig, spawner: TaskSpawner) -> Self {
        Self {
            id: id.into(),
            config,
            channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            shutdown: None,
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
        let connector_id = self.id.clone();

        let _task = self.spawner.spawn_blocking(move || {
            run_file_source(connector_id, source_path, sender, shutdown);
        });

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.store(true, Ordering::Release);
        }
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

fn run_file_source(
    connector_id: String,
    source_path: PathBuf,
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    shutdown: Arc<AtomicBool>,
) {
    info!(
        connector_id = %connector_id,
        path = %source_path.display(),
        "starting file source"
    );

    let mut state = match FileSourceState::new(source_path) {
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
    mode: FileSourceMode,
    cursors: HashMap<PathBuf, FileCursor>,
}

enum FileSourceMode {
    File { target: PathBuf },
    Directory,
}

impl FileSourceState {
    fn new(source_path: PathBuf) -> Result<Self, ConnectorError> {
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
                mode: FileSourceMode::File {
                    target: source_path.clone(),
                },
                cursors: HashMap::new(),
            });
        }
        if metadata.is_dir() {
            return Ok(Self {
                source_path: source_path.clone(),
                watch_path: source_path,
                mode: FileSourceMode::Directory,
                cursors: HashMap::new(),
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
        cursor.read_available_lines(sender)
    }
}

struct FileCursor {
    path: PathBuf,
    offset: u64,
    pending: Vec<u8>,
}

impl FileCursor {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            pending: Vec::new(),
        }
    }

    fn read_available_lines(
        &mut self,
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
        if file_len < self.offset {
            self.offset = 0;
            self.pending.clear();
        }
        if file_len == self.offset {
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
        let file_len = metadata.len();
        if file_len < self.offset {
            self.offset = 0;
            self.pending.clear();
        }
        if file_len == self.offset {
            return Ok(());
        }

        file.seek(SeekFrom::Start(self.offset)).map_err(|err| {
            warn!(
                path = %self.path.display(),
                offset = self.offset,
                error = %err,
                "failed to seek file stream source file"
            );
        })?;

        let mut buffer = [0_u8; READ_BUFFER_SIZE];
        let mut remaining = file_len - self.offset;
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
            self.offset += bytes_read as u64;
            remaining -= bytes_read as u64;
            self.pending.extend_from_slice(&buffer[..bytes_read]);
            self.emit_complete_lines(sender)?;
        }

        Ok(())
    }

    fn emit_complete_lines(
        &mut self,
        sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    ) -> Result<(), ()> {
        while let Some(newline_pos) = self.pending.iter().position(|byte| *byte == b'\n') {
            let mut line = self.pending.drain(..=newline_pos).collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let payload = encode_file_line_frame(&self.path, &line).map_err(|err| {
                let _ = sender.blocking_send(Err(err));
            })?;
            if sender
                .blocking_send(Ok(ConnectorEvent::Payload(payload)))
                .is_err()
            {
                debug!(path = %self.path.display(), "file source receiver closed");
                return Err(());
            }
        }

        Ok(())
    }
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
        let mut state = FileSourceState::new(path).expect("file source state");

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
        let mut state = FileSourceState::new(path.clone()).expect("file source state");

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
    fn file_source_resets_offset_after_truncate() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("app.log");
        write_file(&path, b"old line one\nold line two\n");
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = FileSourceState::new(path.clone()).expect("file source state");

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
        let mut state = FileSourceState::new(dir.path().to_path_buf()).expect("file source state");

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
        let mut state = FileSourceState::new(dir.path().to_path_buf()).expect("file source state");

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
        let mut state = FileSourceState::new(path.clone()).expect("file source state");

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
