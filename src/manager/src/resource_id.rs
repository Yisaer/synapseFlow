//! Central resource-id validation (VF-51 Task 2).
//!
//! Many control-plane APIs accept a user-provided `id` / `name` / `key` /
//! `topic` that becomes a persistent storage key, a runtime registry key, a SQL
//! source reference, or a metrics/log label. Historically each entry point did
//! its own ad-hoc check (usually only `trim().is_empty()`), so import / init /
//! storage-hydrate could bypass the REST rules and unusual characters could
//! pollute logs, exports, and SQL references.
//!
//! This module is the single source of truth for the resource-id grammar so
//! every entry point — REST handlers, import bundles, `init.json`, and config
//! `flow_instances[].id` — agrees on what a valid id is.
//!
//! Grammar (v1):
//!
//! ```text
//! ^[A-Za-z][A-Za-z0-9_]{0,127}$
//! ```
//!
//! - 1 to 128 bytes.
//! - First character: ASCII letter (`A-Z` / `a-z`).
//! - Remaining characters: ASCII letter, digit, or underscore.
//! - Case is preserved; lookups remain case-sensitive (UDF canonicalization is
//!   handled by the caller before validation — see `udf_handler`).
//! - No leading/trailing whitespace; the validator never silently trims an id
//!   into a different value.

use crate::instances::DEFAULT_FLOW_INSTANCE_ID;

/// Maximum resource-id length, in bytes.
pub(crate) const MAX_RESOURCE_ID_LEN: usize = 128;

/// The kinds of user-provided resource ids subject to the unified grammar.
///
/// Used purely to produce a precise field name in error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceIdKind {
    PipelineId,
    StreamName,
    SchemaName,
    FlowInstanceId,
    SinkId,
    MemoryTopic,
    SharedMqttClientKey,
    UdfName,
}

impl ResourceIdKind {
    /// Human-readable field label used in error messages.
    fn label(self) -> &'static str {
        match self {
            ResourceIdKind::PipelineId => "pipeline id",
            ResourceIdKind::StreamName => "stream name",
            ResourceIdKind::SchemaName => "schema name",
            ResourceIdKind::FlowInstanceId => "flow_instance_id",
            ResourceIdKind::SinkId => "sink id",
            ResourceIdKind::MemoryTopic => "memory topic",
            ResourceIdKind::SharedMqttClientKey => "shared mqtt client key",
            ResourceIdKind::UdfName => "UDF name",
        }
    }
}

const GRAMMAR_HINT: &str = "expected [A-Za-z][A-Za-z0-9_]{0,127}";

/// Maximum upload file name length in bytes (255).
pub(crate) const MAX_UPLOAD_NAME_LEN: usize = 255;

const UPLOAD_NAME_GRAMMAR_HINT: &str = "expected [a-zA-Z0-9][a-zA-Z0-9._/-]{0,254} (no leading/trailing /, no //, no .., no \\, no control chars)";

/// Validate `value` against the unified resource-id grammar.
///
/// On failure returns a message that names the field, explains the specific
/// problem (empty / whitespace / too long / invalid char), restates the
/// grammar, and echoes the offending value through [`str::escape_debug`] so
/// control characters never land verbatim in logs or responses.
pub(crate) fn validate_resource_id(kind: ResourceIdKind, value: &str) -> Result<(), String> {
    let label = kind.label();
    if value.is_empty() {
        return Err(format!("{label} must not be empty ({GRAMMAR_HINT})"));
    }
    // Distinguish "looks empty but is whitespace" so API users see why a value
    // that visually looks present is rejected.
    if value.trim().is_empty() {
        return Err(format!("{label} must not be whitespace ({GRAMMAR_HINT})"));
    }
    if value.trim() != value {
        return Err(format!(
            "{label} must not have leading or trailing whitespace ({GRAMMAR_HINT}); got `{}`",
            value.escape_debug()
        ));
    }
    if value.len() > MAX_RESOURCE_ID_LEN {
        return Err(format!(
            "{label} must be at most {MAX_RESOURCE_ID_LEN} bytes ({GRAMMAR_HINT}); got {} bytes",
            value.len()
        ));
    }
    let mut chars = value.chars();
    // Length check above guarantees at least one char.
    if let Some(first) = chars.next()
        && !first.is_ascii_alphabetic()
    {
        return Err(format!(
            "{label} must start with an ASCII letter ({GRAMMAR_HINT}); got `{}`",
            value.escape_debug()
        ));
    }
    for ch in chars {
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            return Err(format!(
                "{label} contains invalid character `{}` ({GRAMMAR_HINT}); got `{}`",
                ch.escape_debug(),
                value.escape_debug()
            ));
        }
    }
    Ok(())
}

/// Validate `value` and return it as an owned `String` on success.
///
/// This is a deliberate *no silent trim*: a value with surrounding whitespace is
/// rejected rather than canonicalized, so `" pipe "` never quietly becomes
/// `"pipe"`.
pub(crate) fn parse_resource_id(kind: ResourceIdKind, value: &str) -> Result<String, String> {
    validate_resource_id(kind, value)?;
    Ok(value.to_string())
}

/// Validate a file name for uploaded files. Supports subdirectories via `/`
/// separators.
///
/// Grammar: segments separated by `/`, each segment matching
/// `[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}`. No leading/trailing `/`, no `//`, no `..`,
/// no `\\`, no control characters.
pub(crate) fn validate_upload_file_name(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!(
            "file name must not be empty ({UPLOAD_NAME_GRAMMAR_HINT})"
        ));
    }
    if value.trim().is_empty() {
        return Err(format!(
            "file name must not be whitespace ({UPLOAD_NAME_GRAMMAR_HINT})"
        ));
    }
    if value.trim() != value {
        return Err(format!(
            "file name must not have leading or trailing whitespace ({UPLOAD_NAME_GRAMMAR_HINT}); got `{}`",
            value.escape_debug()
        ));
    }
    if value.len() > MAX_UPLOAD_NAME_LEN {
        return Err(format!(
            "file name must be at most {MAX_UPLOAD_NAME_LEN} bytes ({UPLOAD_NAME_GRAMMAR_HINT}); got {} bytes",
            value.len()
        ));
    }
    // Must not start with `/`.
    if value.starts_with('/') {
        return Err(format!(
            "file name must not start with / ({UPLOAD_NAME_GRAMMAR_HINT}); got `{}`",
            value.escape_debug()
        ));
    }
    // Must not end with `/`.
    if value.ends_with('/') {
        return Err(format!(
            "file name must not end with / ({UPLOAD_NAME_GRAMMAR_HINT}); got `{}`",
            value.escape_debug()
        ));
    }
    // Each segment must be a valid filename.
    for segment in value.split('/') {
        validate_upload_segment(segment)?;
    }
    Ok(())
}

/// Validate a single path segment (no `/` allowed).
fn validate_upload_segment(segment: &str) -> Result<(), String> {
    if segment.is_empty() {
        return Err(format!(
            "file name contains empty path segment ({UPLOAD_NAME_GRAMMAR_HINT}); got `{}`",
            segment.escape_debug()
        ));
    }
    // File name segment must start with a letter or digit (not a dot).
    let first = segment.chars().next().expect("non-empty segment");
    if !first.is_ascii_alphanumeric() {
        return Err(format!(
            "file name segment must start with a letter or digit ({UPLOAD_NAME_GRAMMAR_HINT}); got `{}`",
            segment.escape_debug()
        ));
    }
    for ch in segment.chars() {
        if ch == '\\' {
            return Err(format!(
                "file name contains invalid character `{}` ({UPLOAD_NAME_GRAMMAR_HINT})",
                ch.escape_debug()
            ));
        }
        if ch.is_control() {
            return Err(format!(
                "file name contains control character ({UPLOAD_NAME_GRAMMAR_HINT})"
            ));
        }
        if !(ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-') {
            return Err(format!(
                "file name contains invalid character `{}` ({UPLOAD_NAME_GRAMMAR_HINT})",
                ch.escape_debug()
            ));
        }
    }
    // Reject path traversal: `..` as a standalone segment or `..something`
    let lower = segment.to_ascii_lowercase();
    if lower == ".." || lower.starts_with("..") {
        return Err(format!(
            "file name segment contains path traversal ({UPLOAD_NAME_GRAMMAR_HINT})"
        ));
    }
    if segment.len() > MAX_UPLOAD_NAME_LEN {
        return Err(format!(
            "file name segment must be at most {MAX_UPLOAD_NAME_LEN} bytes ({UPLOAD_NAME_GRAMMAR_HINT})"
        ));
    }
    Ok(())
}

/// Resolve and validate an optional `flow_instance_id`.
///
/// A missing/`None` value defaults to [`DEFAULT_FLOW_INSTANCE_ID`]; any provided
/// value must satisfy the resource-id grammar (no silent trim).
pub(crate) fn defaulted_flow_instance_id(value: Option<&str>) -> Result<String, String> {
    match value {
        None => Ok(DEFAULT_FLOW_INSTANCE_ID.to_string()),
        Some(value) => parse_resource_id(ResourceIdKind::FlowInstanceId, value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_ids() {
        for value in [
            "default",
            "StreamA",
            "pipe_1",
            "FI_Critical",
            "A",
            &"a".repeat(MAX_RESOURCE_ID_LEN),
        ] {
            assert!(
                validate_resource_id(ResourceIdKind::StreamName, value).is_ok(),
                "expected `{value}` to be accepted"
            );
        }
    }

    #[test]
    fn rejects_invalid_ids() {
        let cases = [
            "",
            " pipe",
            "pipe ",
            "1pipe",
            "pipe-a",
            "pipe.a",
            "pipe/a",
            "pipe:a",
            "pipe%2F",
            "pipe\nx",
            "中文",
            &"a".repeat(MAX_RESOURCE_ID_LEN + 1),
        ];
        for value in cases {
            assert!(
                validate_resource_id(ResourceIdKind::StreamName, value).is_err(),
                "expected `{value}` to be rejected"
            );
        }
    }

    #[test]
    fn error_message_names_field_and_grammar() {
        let err = validate_resource_id(ResourceIdKind::PipelineId, "1bad").unwrap_err();
        assert!(err.contains("pipeline id"), "missing field label: {err}");
        assert!(err.contains(GRAMMAR_HINT), "missing grammar hint: {err}");
    }

    #[test]
    fn empty_and_whitespace_have_distinct_messages() {
        let empty = validate_resource_id(ResourceIdKind::StreamName, "").unwrap_err();
        assert!(empty.contains("must not be empty"), "got: {empty}");
        let ws = validate_resource_id(ResourceIdKind::StreamName, "   ").unwrap_err();
        assert!(ws.contains("must not be whitespace"), "got: {ws}");
    }

    #[test]
    fn control_characters_are_escaped_in_error() {
        let err = validate_resource_id(ResourceIdKind::StreamName, "pipe\nx").unwrap_err();
        assert!(
            !err.contains('\n'),
            "raw newline leaked into error: {err:?}"
        );
        assert!(err.contains("\\n"), "newline not escaped: {err}");
    }

    #[test]
    fn parse_resource_id_does_not_trim() {
        assert!(parse_resource_id(ResourceIdKind::StreamName, " pipe ").is_err());
        assert_eq!(
            parse_resource_id(ResourceIdKind::StreamName, "pipe").unwrap(),
            "pipe"
        );
    }

    #[test]
    fn validate_upload_file_name_accepts_valid_names() {
        for value in [
            "ca-cert.pem",
            "client-key.pem",
            "descriptor.proto",
            "a.txt",
            "my_cert-2026.crt",
            "A",
            "proto/sensor.proto",
            "dbc/can/chassis.dbc",
        ] {
            assert!(
                validate_upload_file_name(value).is_ok(),
                "expected `{value}` to be accepted"
            );
        }
    }

    #[test]
    fn validate_upload_file_name_rejects_invalid_names() {
        let cases = [
            ("", "empty"),
            (".hidden", "starts with dot"),
            ("../escape", "path traversal .."),
            ("a/../b", "path traversal in segment"),
            ("/leading", "leading slash"),
            ("trailing/", "trailing slash"),
            ("a//b", "empty segment"),
            ("key\\path", "contains backslash"),
            ("a\nb", "contains newline"),
            ("spaced name", "contains space"),
            ("name:colon", "contains colon"),
            ("  leading", "leading whitespace"),
            ("trailing ", "trailing whitespace"),
            ("/onlyslash/", "starts and ends with /"),
        ];
        for (value, desc) in cases {
            assert!(
                validate_upload_file_name(value).is_err(),
                "({desc}) expected `{value}` to be rejected"
            );
        }
    }

    #[test]
    fn defaulted_flow_instance_id_defaults_and_validates() {
        assert_eq!(
            defaulted_flow_instance_id(None).unwrap(),
            DEFAULT_FLOW_INSTANCE_ID
        );
        assert_eq!(
            defaulted_flow_instance_id(Some("worker_a")).unwrap(),
            "worker_a"
        );
        assert!(defaulted_flow_instance_id(Some(" worker ")).is_err());
        assert!(defaulted_flow_instance_id(Some("bad-id")).is_err());
    }
}
