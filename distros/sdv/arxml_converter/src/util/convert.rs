//! Low-level conversion utilities — string ↔ numeric, path helpers,
//! and ID merging.
//!
//! Corresponds to the Go `util/parser.go`.

use std::num::ParseIntError;

/// Parse a decimal string into a `u16`.
///
/// Corresponds to Go `util.ToUint16`.
pub fn to_u16(raw: &str) -> Result<u16, ParseIntError> {
    raw.parse::<u16>()
}

/// Parse a decimal string into a `u32`.
///
/// Corresponds to Go `util.ToUint32`.
pub fn to_u32(raw: &str) -> Result<u32, ParseIntError> {
    raw.parse::<u32>()
}

/// Parse a decimal string into a `u64`.
pub fn to_u64(raw: &str) -> Result<u64, ParseIntError> {
    raw.parse::<u64>()
}

/// Parse a decimal string into an `i64`.
///
/// Corresponds to Go `util.ToInt64`.  Note: the Go function uses
/// `ParseUint` but casts to `int64`; here we parse directly as signed
/// to avoid silent truncation surprises.
pub fn to_i64(raw: &str) -> Result<i64, ParseIntError> {
    raw.parse::<i64>()
}

/// Extract the last `/`-separated segment of a reference path.
///
/// ```text
/// "/A/B/C"   → "C"
/// "no_slash"  → "no_slash"
/// ```
///
/// Corresponds to Go `util.ExtractLast`.
pub fn extract_last(r: &str) -> &str {
    r.rsplit('/').next().unwrap_or(r)
}

/// Merge two `u16` values into a single `u32` — high 16 bits `high`,
/// low 16 bits `low`.
///
/// Corresponds to Go `util.MergeUint16ToUint32`.
pub fn merge_u16_to_u32(high: u16, low: u16) -> u32 {
    (u32::from(high) << 16) | u32::from(low)
}

/// Parse two hex strings (service-ID and event-ID) and merge them into
/// `(service_id: u16, merged_id: u32)`.
///
/// The hex strings may contain an optional `0x` prefix.
///
/// Corresponds to Go `util.MergeHexUint16ToUint32`.
pub fn merge_hex_u16_to_u32(svc_hex: &str, event_hex: &str) -> Result<(u16, u32), ParseIntError> {
    let svc_id = parse_hex_u16(svc_hex)?;
    let event_id = parse_hex_u16(event_hex)?;
    Ok((svc_id, merge_u16_to_u32(svc_id, event_id)))
}

fn parse_hex_u16(hex: &str) -> Result<u16, ParseIntError> {
    let stripped = hex.strip_prefix("0x").unwrap_or(hex);
    u16::from_str_radix(stripped, 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_u16() {
        assert_eq!(to_u16("42").unwrap(), 42);
        assert_eq!(to_u16("0").unwrap(), 0);
        assert_eq!(to_u16("65535").unwrap(), 65535);
    }

    #[test]
    fn parse_invalid_u16() {
        assert!(to_u16("99999").is_err());
        assert!(to_u16("-1").is_err());
        assert!(to_u16("abc").is_err());
    }

    #[test]
    fn parse_u32() {
        assert_eq!(to_u32("4294967295").unwrap(), 4294967295);
    }

    #[test]
    fn parse_i64() {
        assert_eq!(to_i64("-42").unwrap(), -42);
        assert_eq!(to_i64("0").unwrap(), 0);
    }

    #[test]
    fn extract_last_segment() {
        assert_eq!(extract_last("/A/B/C"), "C");
        assert_eq!(extract_last("no_slash"), "no_slash");
        assert_eq!(extract_last("/"), "");
    }

    #[test]
    fn merge_ids() {
        assert_eq!(merge_u16_to_u32(0x1234, 0x5678), 0x1234_5678);
        assert_eq!(merge_u16_to_u32(0, 0), 0);
        assert_eq!(merge_u16_to_u32(0xFFFF, 0xFFFF), 0xFFFF_FFFF);
    }

    #[test]
    fn merge_hex_ids() {
        let (svc, merged) = merge_hex_u16_to_u32("0x12", "0x34").unwrap();
        assert_eq!(svc, 0x12);
        assert_eq!(merged, 0x0012_0034);
    }

    #[test]
    fn merge_hex_ids_no_prefix() {
        let (svc, merged) = merge_hex_u16_to_u32("FF", "AB").unwrap();
        assert_eq!(svc, 0xFF);
        assert_eq!(merged, 0x00FF_00AB);
    }
}
