use time::format_description::FormatItem;
use time::macros::format_description;
use time::{OffsetDateTime, PrimitiveDateTime};

const UTC_FILENAME_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]UTC");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotatedLogName {
    pub stem: String,
    pub timestamp_utc: OffsetDateTime,
    pub seq: u16,
}

pub fn format_rotated_filename(stem: &str, timestamp_utc: OffsetDateTime, seq: u16) -> String {
    let ts = timestamp_utc
        .format(UTC_FILENAME_FORMAT)
        .unwrap_or_else(|_| "1970-01-01T00-00-00UTC".to_string());
    format!("{stem}.{ts}.{seq:03}.log")
}

pub fn parse_rotated_filename(file_name: &str, expected_stem: &str) -> Option<RotatedLogName> {
    if !file_name.starts_with(expected_stem) {
        return None;
    }
    let prefix = format!("{expected_stem}.");
    if !file_name.starts_with(&prefix) {
        return None;
    }
    let suffix = ".log";
    if !file_name.ends_with(suffix) {
        return None;
    }
    let middle = &file_name[prefix.len()..file_name.len() - suffix.len()];
    let (ts_raw, seq_raw) = middle.rsplit_once('.')?;
    if seq_raw.len() != 3 || !seq_raw.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let seq = seq_raw.parse::<u16>().ok()?;
    let timestamp_utc = PrimitiveDateTime::parse(ts_raw, UTC_FILENAME_FORMAT)
        .ok()?
        .assume_utc();
    Some(RotatedLogName {
        stem: expected_stem.to_string(),
        timestamp_utc,
        seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_format_and_parse() {
        let stem = "app";
        let ts = OffsetDateTime::from_unix_timestamp(1_735_048_200).unwrap(); // 2024-12-24...
        let name = format_rotated_filename(stem, ts, 1);
        let parsed = parse_rotated_filename(&name, stem).unwrap();
        assert_eq!(parsed.stem, stem);
        assert_eq!(parsed.seq, 1);
        assert_eq!(parsed.timestamp_utc, ts);
    }

    #[test]
    fn rejects_other_stem() {
        let ts = OffsetDateTime::from_unix_timestamp(1_735_048_200).unwrap();
        let name = format_rotated_filename("app", ts, 1);
        assert!(parse_rotated_filename(&name, "other").is_none());
    }
}
