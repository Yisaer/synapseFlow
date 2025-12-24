use crate::config::LogRotationConfig;
use crate::logging::filename::parse_rotated_filename;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use time::{Duration, OffsetDateTime};

#[derive(Debug, Clone)]
struct RotatedFile {
    path: PathBuf,
    ts_utc: OffsetDateTime,
    seq: u16,
}

pub fn prune_rotated_logs(
    dir: &Path,
    current_file_name: &str,
    rotation: &LogRotationConfig,
) -> io::Result<()> {
    let stem = Path::new(current_file_name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("app");

    let mut rotated = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        let file_name = match entry.file_name().to_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let parsed = match parse_rotated_filename(&file_name, stem) {
            Some(p) => p,
            None => continue,
        };
        rotated.push(RotatedFile {
            path: entry.path(),
            ts_utc: parsed.timestamp_utc,
            seq: parsed.seq,
        });
    }

    rotated.sort_by(|a, b| a.ts_utc.cmp(&b.ts_utc).then_with(|| a.seq.cmp(&b.seq)));

    let now = OffsetDateTime::now_utc();
    let keep_days = rotation.keep_days as i64;
    let cutoff = now - Duration::days(keep_days);
    let mut keep = Vec::with_capacity(rotated.len());
    for file in rotated {
        if file.ts_utc < cutoff {
            let _ = fs::remove_file(&file.path);
            continue;
        }
        keep.push(file);
    }

    while keep.len() > rotation.max_num as usize {
        if let Some(file) = keep.first() {
            let _ = fs::remove_file(&file.path);
        }
        keep.remove(0);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::filename::format_rotated_filename;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("synapse_flow_test.logs.{}.{}", name, nanos))
    }

    #[test]
    fn prunes_by_keep_days_and_max_num() {
        let dir = unique_temp_dir("prune");
        fs::create_dir_all(&dir).unwrap();
        let stem = "app";

        let now = OffsetDateTime::now_utc();
        let old = now - Duration::days(10);
        let recent1 = now - Duration::days(1);
        let recent2 = now - Duration::hours(1);

        let old_name = format_rotated_filename(stem, old, 1);
        let recent1_name = format_rotated_filename(stem, recent1, 1);
        let recent2_name = format_rotated_filename(stem, recent2, 1);

        fs::write(dir.join(old_name), b"x").unwrap();
        fs::write(dir.join(recent1_name), b"x").unwrap();
        fs::write(dir.join(recent2_name), b"x").unwrap();

        let rotation = LogRotationConfig {
            keep_days: 3,
            max_num: 1,
            max_size_mb: 1,
        };
        prune_rotated_logs(&dir, "app.log", &rotation).unwrap();

        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().ok().map(|t| t.is_file()).unwrap_or(false))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(files.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }
}
