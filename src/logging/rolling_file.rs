use crate::config::FileLoggingConfig;
use crate::logging::cleanup::prune_rotated_logs;
use crate::logging::filename::{format_rotated_filename, parse_rotated_filename};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

pub struct RollingFileWriter {
    cfg: FileLoggingConfig,
    path: PathBuf,
    file: Option<File>,
    bytes_written: u64,
}

impl RollingFileWriter {
    pub fn open(cfg: FileLoggingConfig) -> io::Result<Self> {
        let dir = PathBuf::from(&cfg.dir);
        fs::create_dir_all(&dir)?;
        let path = dir.join(&cfg.file_name);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes_written = file.metadata().map(|m| m.len()).unwrap_or(0);
        let writer = Self {
            cfg,
            path,
            file: Some(file),
            bytes_written,
        };
        prune_rotated_logs(&dir, &writer.cfg.file_name, &writer.cfg.rotation)?;
        Ok(writer)
    }

    fn max_size_bytes(&self) -> u64 {
        (self.cfg.rotation.max_size_mb as u64) * 1024 * 1024
    }

    fn maybe_rotate(&mut self, incoming_bytes: usize) -> io::Result<()> {
        let max_size = self.max_size_bytes();
        if max_size == 0 {
            return Ok(());
        }
        if self.bytes_written.saturating_add(incoming_bytes as u64) < max_size {
            return Ok(());
        }
        self.rotate()
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            let _ = file.flush();
            drop(file);
        }

        let dir = Path::new(&self.cfg.dir);
        let stem = self
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("app");

        let now = OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .unwrap_or_else(|_| OffsetDateTime::UNIX_EPOCH);
        let seq = next_seq_for_timestamp(dir, stem, now);
        let rotated_name = format_rotated_filename(stem, now, seq);
        let rotated_path = dir.join(rotated_name);

        fs::rename(&self.path, &rotated_path)?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.file = Some(file);
        self.bytes_written = 0;

        prune_rotated_logs(dir, &self.cfg.file_name, &self.cfg.rotation)?;
        Ok(())
    }
}

impl Write for RollingFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.maybe_rotate(buf.len())?;
        let written = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "log file not open"))?
            .write(buf)?;
        self.bytes_written = self.bytes_written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "log file not open"))?
            .flush()
    }
}

fn next_seq_for_timestamp(dir: &Path, stem: &str, ts: OffsetDateTime) -> u16 {
    let mut max_seen: u16 = 0;
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return 1,
    };
    for entry in read_dir.flatten() {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let parsed = match parse_rotated_filename(&file_name, stem) {
            Some(p) => p,
            None => continue,
        };
        if parsed.timestamp_utc == ts && parsed.seq > max_seen {
            max_seen = parsed.seq;
        }
    }
    max_seen.saturating_add(1).max(1)
}
