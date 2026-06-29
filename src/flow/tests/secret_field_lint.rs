//! VF-51 §6.5 guardrail: connector/DTO fields whose names denote a secret must
//! be typed `SecretRef` (config layer) or `SecretString` (resolved), never a
//! bare `String`. Types can't force their own use, so this test enforces it.
//!
//! Escape hatch: append `// secret-lint: allow <reason>` on the field line when
//! a secret-named field is genuinely a non-secret identifier.

use std::fs;
use std::path::{Path, PathBuf};

/// Field-name substrings that unambiguously denote a secret value.
const SECRET_NAME_PATTERNS: &[&str] = &[
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "credential",
    "apikey",
    "api_key",
];

/// Types that are acceptable for a secret-named field.
const ALLOWED_TYPE_MARKERS: &[&str] = &["SecretRef", "SecretString"];

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = <root>/src/flow
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Parse `pub <name>: <type>` field declarations from a line. Returns
/// `(field_name, type_text)` if it looks like a struct field.
fn parse_field(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("pub ")?;
    let colon = rest.find(':')?;
    let name = rest[..colon].trim();
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    let ty = rest[colon + 1..].trim().trim_end_matches(',').trim();
    Some((name.to_string(), ty.to_string()))
}

fn name_denotes_secret(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    SECRET_NAME_PATTERNS.iter().any(|p| lower.contains(p))
}

#[test]
fn secret_named_fields_use_secret_types() {
    let root = workspace_root();
    let scan_dirs = [
        root.join("src/flow/src/connector"),
        root.join("src/manager/src/pipeline"),
    ];

    let mut files = Vec::new();
    for dir in &scan_dirs {
        rust_files(dir, &mut files);
    }
    assert!(!files.is_empty(), "no source files found to lint");

    let mut violations = Vec::new();
    for file in &files {
        // Skip this lint's own helper files and test modules are fine to scan;
        // the patterns only match field declarations.
        let content = fs::read_to_string(file).expect("read source");
        for (lineno, line) in content.lines().enumerate() {
            if line.contains("secret-lint: allow") {
                continue;
            }
            let Some((name, ty)) = parse_field(line) else {
                continue;
            };
            if !name_denotes_secret(&name) {
                continue;
            }
            if ALLOWED_TYPE_MARKERS.iter().any(|m| ty.contains(m)) {
                continue;
            }
            violations.push(format!(
                "{}:{} field `{name}: {ty}` is secret-named but not a SecretRef/SecretString",
                file.strip_prefix(&root).unwrap_or(file).display(),
                lineno + 1
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "secret-named fields must use SecretRef/SecretString (VF-51 §6.5):\n{}",
        violations.join("\n")
    );
}

#[test]
fn lint_detects_a_bad_field() {
    // Sanity: the parser flags a plain-String secret field and accepts SecretRef.
    assert!(name_denotes_secret("password"));
    assert!(name_denotes_secret("auth_token"));
    let (name, ty) = parse_field("    pub password: String,").unwrap();
    assert_eq!(name, "password");
    assert!(!ALLOWED_TYPE_MARKERS.iter().any(|m| ty.contains(m)));
    let (_, ok_ty) = parse_field("    pub password: SecretRef,").unwrap();
    assert!(ALLOWED_TYPE_MARKERS.iter().any(|m| ok_ty.contains(m)));
}
