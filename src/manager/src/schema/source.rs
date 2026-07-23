use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde_json::{Map as JsonMap, Value as JsonValue};
use storage::{StorageManager, StoredSchema};
use zip::ZipArchive;

const SCHEMA_PATH_KEY: &str = "schema_path";
const PROTO_PATH_KEY: &str = "proto_path";
const MAX_ARCHIVE_ENTRIES: usize = 4096;
const MAX_UNCOMPRESSED_SIZE: u64 = 512 * 1024 * 1024;

pub struct PreparedSchemaSource {
    parse_props: JsonMap<String, JsonValue>,
    stored_props: JsonMap<String, JsonValue>,
    staging_dir: Option<PathBuf>,
    installed_dir: Option<PathBuf>,
    committed: bool,
}

pub struct PreparedSchemaTree {
    target_dir: PathBuf,
    staging_dir: Option<PathBuf>,
    backup_dir: Option<PathBuf>,
    work_dir: Option<tempfile::TempDir>,
    had_target: bool,
    backup_active: bool,
    activated: bool,
    finished: bool,
}

impl PreparedSchemaTree {
    pub fn prepare(storage: &StorageManager, source_dir: &Path) -> Result<Self, String> {
        let target_dir = storage.schemas_dir();
        let source_metadata = match fs::symlink_metadata(source_dir) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    target_dir,
                    staging_dir: None,
                    backup_dir: None,
                    work_dir: None,
                    had_target: false,
                    backup_active: false,
                    activated: false,
                    finished: false,
                });
            }
            Err(err) => {
                return Err(format!(
                    "inspect staged schema root `{}`: {err}",
                    source_dir.display()
                ));
            }
        };
        if !source_metadata.file_type().is_dir() {
            return Err(format!(
                "staged schema root `{}` is not a regular directory",
                source_dir.display()
            ));
        }
        let work_dir = tempfile::Builder::new()
            .prefix(".schemas.import.")
            .tempdir_in(storage.base_dir())
            .map_err(|err| format!("create schema import work directory: {err}"))?;
        let staging_dir = work_dir.path().join("staged");
        let backup_dir = work_dir.path().join("backup");
        fs::create_dir(&staging_dir)
            .map_err(|err| format!("create schema import staging directory: {err}"))?;
        copy_schema_tree(source_dir, &staging_dir)?;
        Ok(Self {
            target_dir,
            staging_dir: Some(staging_dir),
            backup_dir: Some(backup_dir),
            work_dir: Some(work_dir),
            had_target: false,
            backup_active: false,
            activated: false,
            finished: false,
        })
    }

    pub fn staged_root(&self) -> Option<&Path> {
        self.staging_dir.as_deref()
    }

    pub fn activate(&mut self) -> Result<(), String> {
        let Some(staging_dir) = &self.staging_dir else {
            return Ok(());
        };
        self.had_target = self.target_dir.exists();
        if self.had_target {
            let backup_dir = self
                .backup_dir
                .as_ref()
                .ok_or_else(|| "schema import backup path is not prepared".to_string())?;
            fs::rename(&self.target_dir, backup_dir)
                .map_err(|err| format!("backup installed schema sources: {err}"))?;
            self.backup_active = true;
        }
        if let Err(err) = fs::rename(staging_dir, &self.target_dir) {
            if self.backup_active {
                let backup_dir = self
                    .backup_dir
                    .as_ref()
                    .ok_or_else(|| "schema import backup path is not prepared".to_string())?;
                match fs::rename(backup_dir, &self.target_dir) {
                    Ok(()) => self.backup_active = false,
                    Err(restore_err) => {
                        return Err(format!(
                            "activate imported schema sources: {err}; restore previous schema sources: {restore_err}"
                        ));
                    }
                }
            }
            return Err(format!("activate imported schema sources: {err}"));
        }
        self.activated = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        if self.activated {
            match fs::remove_dir_all(&self.target_dir) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(self.preserve_after_rollback_failure(format!(
                        "remove imported schema sources: {err}"
                    )));
                }
            }
            self.activated = false;
        }
        if self.backup_active {
            let Some(backup_dir) = self.backup_dir.as_ref() else {
                return Err(self.preserve_after_rollback_failure(
                    "schema import backup path is not prepared".to_string(),
                ));
            };
            if let Err(err) = fs::rename(backup_dir, &self.target_dir) {
                return Err(self.preserve_after_rollback_failure(format!(
                    "restore previous schema sources: {err}"
                )));
            }
            self.backup_active = false;
        }
        self.finished = true;
        self.close_work_dir()
    }

    pub fn finish(&mut self) {
        self.finished = true;
        if let Err(err) = self.close_work_dir() {
            tracing::warn!(
                error = %err,
                "failed to remove schema import work directory"
            );
        }
    }

    fn close_work_dir(&mut self) -> Result<(), String> {
        let Some(work_dir) = self.work_dir.take() else {
            return Ok(());
        };
        work_dir
            .close()
            .map_err(|err| format!("remove schema import work directory: {err}"))
    }

    fn preserve_after_rollback_failure(&mut self, error: String) -> String {
        let Some(work_dir) = self.work_dir.take() else {
            return error;
        };
        let preserved = work_dir.keep();
        format!(
            "{error}; preserved schema import recovery files at `{}`",
            preserved.display()
        )
    }
}

impl Drop for PreparedSchemaTree {
    fn drop(&mut self) {
        if !self.finished
            && let Err(err) = self.rollback()
        {
            tracing::error!(error = %err, "failed to roll back imported schema sources");
        }
    }
}

impl PreparedSchemaSource {
    pub fn prepare(
        storage: &StorageManager,
        schema_name: &str,
        schema_type: &str,
        props: &JsonMap<String, JsonValue>,
    ) -> Result<Self, String> {
        if props.contains_key(SCHEMA_PATH_KEY) && props.contains_key(PROTO_PATH_KEY) {
            return Err(
                "schema props must not contain both `schema_path` and `proto_path`".to_string(),
            );
        }
        let Some(path_key) = source_path_key(props) else {
            return Ok(Self {
                parse_props: props.clone(),
                stored_props: props.clone(),
                staging_dir: None,
                installed_dir: None,
                committed: false,
            });
        };
        if props.contains_key("include_paths") {
            return Err(
                "`include_paths` is not supported for installed schemas; include dependencies in the schema zip companion directory"
                    .to_string(),
            );
        }
        let schema_type = schema_type.trim().to_ascii_lowercase();
        validate_path_segment(&schema_type, "schema type")?;
        let archive_path = props
            .get(path_key)
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| format!("`{path_key}` must be a non-empty zip path"))?;
        let archive_path = Path::new(archive_path);
        if archive_path.extension().and_then(|ext| ext.to_str()) != Some("zip") {
            return Err(format!("`{path_key}` must point to a .zip schema source"));
        }
        let metadata = fs::symlink_metadata(archive_path)
            .map_err(|err| format!("inspect schema archive `{}`: {err}", archive_path.display()))?;
        if !metadata.file_type().is_file() {
            return Err(format!(
                "schema archive `{}` must be a regular file",
                archive_path.display()
            ));
        }

        let parent = storage.schemas_dir().join(&schema_type);
        let installed_dir = parent.join(schema_name);
        let staging_dir = parent.join(format!(".{schema_name}.tmp"));
        if installed_dir.exists() || staging_dir.exists() {
            return Err(format!(
                "installed schema source `{schema_name}` already exists"
            ));
        }
        fs::create_dir_all(&staging_dir)
            .map_err(|err| format!("create schema staging directory: {err}"))?;
        let entry_name = match extract_schema_archive(archive_path, &staging_dir) {
            Ok(entry) => entry,
            Err(err) => {
                return match fs::remove_dir_all(&staging_dir) {
                    Ok(()) => Err(err),
                    Err(cleanup_err) => Err(format!(
                        "{err}; remove schema staging directory `{}`: {cleanup_err}",
                        staging_dir.display()
                    )),
                };
            }
        };

        let mut parse_props = props.clone();
        parse_props.insert(
            path_key.to_string(),
            JsonValue::String(staging_dir.join(&entry_name).to_string_lossy().into_owned()),
        );
        let mut stored_props = props.clone();
        stored_props.insert(
            path_key.to_string(),
            JsonValue::String(entry_name.to_string_lossy().into_owned()),
        );
        Ok(Self {
            parse_props,
            stored_props,
            staging_dir: Some(staging_dir),
            installed_dir: Some(installed_dir),
            committed: false,
        })
    }

    pub fn parse_props(&self) -> &JsonMap<String, JsonValue> {
        &self.parse_props
    }

    pub fn stored_props(&self) -> &JsonMap<String, JsonValue> {
        &self.stored_props
    }

    pub fn commit(&mut self) -> Result<(), String> {
        let (Some(staging), Some(installed)) = (&self.staging_dir, &self.installed_dir) else {
            self.committed = true;
            return Ok(());
        };
        fs::rename(staging, installed).map_err(|err| format!("install schema source: {err}"))?;
        self.committed = true;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        if self.committed
            && let Some(installed) = &self.installed_dir
        {
            match fs::remove_dir_all(installed) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(format!(
                        "remove installed schema source `{}`: {err}",
                        installed.display()
                    ));
                }
            }
        }
        self.committed = false;
        Ok(())
    }
}

fn copy_schema_tree(source: &Path, destination: &Path) -> Result<(), String> {
    for entry in fs::read_dir(source)
        .map_err(|err| format!("read staged schema directory `{}`: {err}", source.display()))?
    {
        let entry = entry.map_err(|err| format!("read staged schema entry: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("inspect staged schema entry: {err}"))?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            fs::create_dir(&target).map_err(|err| {
                format!(
                    "create staged schema directory `{}`: {err}",
                    target.display()
                )
            })?;
            copy_schema_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &target)
                .map_err(|err| format!("copy staged schema file `{}`: {err}", target.display()))?;
        } else {
            return Err(format!(
                "staged schema entry `{}` is not a regular file or directory",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

impl Drop for PreparedSchemaSource {
    fn drop(&mut self) {
        if !self.committed
            && let Some(staging) = &self.staging_dir
            && let Err(err) = fs::remove_dir_all(staging)
            && err.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %staging.display(),
                error = %err,
                "failed to remove schema staging directory"
            );
        }
    }
}

pub fn reconcile_installed_sources(
    storage: &StorageManager,
    stored_schemas: &[StoredSchema],
) -> Result<(), String> {
    let root = storage.schemas_dir();
    if !root.exists() {
        return Ok(());
    }

    let mut expected = HashSet::new();
    for stored in stored_schemas {
        let schema_type = stored.schema_type.trim().to_ascii_lowercase();
        if validate_path_segment(&schema_type, "schema type").is_err()
            || validate_path_segment(&stored.name, "schema name").is_err()
        {
            continue;
        }
        let Ok(props) = serde_json::from_str::<JsonMap<String, JsonValue>>(&stored.props_json)
        else {
            continue;
        };
        if source_path_key(&props).is_some() {
            expected.insert(root.join(schema_type).join(&stored.name));
        }
    }

    for type_entry in fs::read_dir(&root).map_err(|err| {
        format!(
            "read schema installation directory `{}`: {err}",
            root.display()
        )
    })? {
        let type_entry = type_entry.map_err(|err| format!("read schema type entry: {err}"))?;
        let type_path = type_entry.path();
        if !type_entry
            .file_type()
            .map_err(|err| format!("inspect schema type entry `{}`: {err}", type_path.display()))?
            .is_dir()
        {
            remove_orphaned_path(&type_path)?;
            continue;
        }
        for schema_entry in fs::read_dir(&type_path).map_err(|err| {
            format!(
                "read schema type directory `{}`: {err}",
                type_path.display()
            )
        })? {
            let schema_entry =
                schema_entry.map_err(|err| format!("read installed schema entry: {err}"))?;
            let schema_path = schema_entry.path();
            let is_directory = schema_entry
                .file_type()
                .map_err(|err| {
                    format!(
                        "inspect installed schema entry `{}`: {err}",
                        schema_path.display()
                    )
                })?
                .is_dir();
            if !is_directory || !expected.contains(&schema_path) {
                remove_orphaned_path(&schema_path)?;
            }
        }
    }
    Ok(())
}

fn remove_orphaned_path(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|err| format!("inspect orphaned schema source `{}`: {err}", path.display()))?;
    let result = if metadata.file_type().is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    result.map_err(|err| format!("remove orphaned schema source `{}`: {err}", path.display()))
}

pub fn resolve_installed_props(
    storage: &StorageManager,
    schema_name: &str,
    schema_type: &str,
    props: &mut JsonMap<String, JsonValue>,
) -> Result<(), String> {
    let Some(path_key) = source_path_key(props) else {
        return Ok(());
    };
    let Some(entry) = props.get(path_key).and_then(JsonValue::as_str) else {
        return resolve_props_from_root(&storage.schemas_dir(), schema_name, schema_type, props);
    };
    if Path::new(entry).components().count() != 1 {
        return Ok(());
    }
    resolve_props_from_root(&storage.schemas_dir(), schema_name, schema_type, props)
}

pub fn resolve_props_from_root(
    schema_root: &Path,
    schema_name: &str,
    schema_type: &str,
    props: &mut JsonMap<String, JsonValue>,
) -> Result<(), String> {
    if props.contains_key(SCHEMA_PATH_KEY) && props.contains_key(PROTO_PATH_KEY) {
        return Err(
            "schema props must not contain both `schema_path` and `proto_path`".to_string(),
        );
    }
    let Some(path_key) = source_path_key(props) else {
        return Ok(());
    };
    let entry = props
        .get(path_key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .ok_or_else(|| format!("`{path_key}` must be a non-empty installed entry filename"))?;
    validate_path_segment(entry, "schema source entry")?;
    let schema_type = schema_type.trim().to_ascii_lowercase();
    validate_path_segment(&schema_type, "schema type")?;
    validate_path_segment(schema_name, "schema name")?;
    let resolved = schema_root.join(schema_type).join(schema_name).join(entry);
    props.insert(
        path_key.to_string(),
        JsonValue::String(resolved.to_string_lossy().into_owned()),
    );
    Ok(())
}

pub fn delete_installed_source(storage: &StorageManager, schema_name: &str, schema_type: &str) {
    let path = storage
        .schemas_dir()
        .join(schema_type.trim().to_ascii_lowercase())
        .join(schema_name);
    let _ = fs::remove_dir_all(path);
}

fn extract_schema_archive(archive_path: &Path, destination: &Path) -> Result<PathBuf, String> {
    extract_schema_archive_with_limits(
        archive_path,
        destination,
        MAX_ARCHIVE_ENTRIES,
        MAX_UNCOMPRESSED_SIZE,
    )
}

fn extract_schema_archive_with_limits(
    archive_path: &Path,
    destination: &Path,
    max_entries: usize,
    max_uncompressed_size: u64,
) -> Result<PathBuf, String> {
    let file = fs::File::open(archive_path)
        .map_err(|err| format!("open schema archive `{}`: {err}", archive_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .map_err(|err| format!("open schema zip `{}`: {err}", archive_path.display()))?;
    if archive.is_empty() {
        return Err("schema zip must not be empty".to_string());
    }
    if archive.len() > max_entries {
        return Err(format!(
            "schema zip has too many entries: {} > {max_entries}",
            archive.len()
        ));
    }

    let mut paths = HashSet::with_capacity(archive.len());
    let mut entries = Vec::with_capacity(archive.len());
    let mut root_entry = None;
    let mut total_size = 0u64;
    for index in 0..archive.len() {
        let file = archive
            .by_index(index)
            .map_err(|err| format!("read schema zip entry {index}: {err}"))?;
        let name = file.name();
        if name.contains('\\') {
            return Err(format!("schema zip entry `{name}` contains a backslash"));
        }
        let path = file
            .enclosed_name()
            .ok_or_else(|| format!("schema zip entry `{name}` has an unsafe path"))?;
        validate_archive_path(&path, name)?;
        if !paths.insert(path.clone()) {
            return Err(format!(
                "schema zip contains duplicate entry `{}`",
                path.display()
            ));
        }
        if let Some(mode) = file.unix_mode()
            && mode & 0o170000 != 0
            && mode & 0o170000 != 0o040000
            && mode & 0o170000 != 0o100000
        {
            return Err(format!(
                "schema zip entry `{}` is not a regular file or directory",
                path.display()
            ));
        }
        let is_dir = file.is_dir();
        if !is_dir {
            total_size = total_size
                .checked_add(file.size())
                .ok_or_else(|| "schema zip uncompressed size overflow".to_string())?;
            if total_size > max_uncompressed_size {
                return Err(format!(
                    "schema zip uncompressed size exceeds {max_uncompressed_size} bytes"
                ));
            }
            if path.components().count() == 1 && root_entry.replace(path.clone()).is_some() {
                return Err("schema zip must contain exactly one root entry file".to_string());
            }
        }
        entries.push((path, is_dir));
    }

    let root_entry = root_entry
        .ok_or_else(|| "schema zip must contain exactly one root entry file".to_string())?;
    if root_entry.extension().is_none() {
        return Err("schema entry filename must have an extension".to_string());
    }
    let companion = root_entry
        .file_stem()
        .ok_or_else(|| "schema entry filename must have a stem".to_string())?;
    for (path, is_dir) in &entries {
        if path == &root_entry {
            continue;
        }
        let mut components = path.components();
        let Some(Component::Normal(top)) = components.next() else {
            return Err(format!("invalid schema zip entry `{}`", path.display()));
        };
        if top != companion {
            return Err(format!(
                "schema zip entry `{}` is outside companion directory `{}`",
                path.display(),
                Path::new(companion).display()
            ));
        }
        if components.next().is_none() && !is_dir {
            return Err(format!(
                "schema companion root `{}` must be a directory",
                path.display()
            ));
        }
    }

    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|err| format!("read schema zip entry {index}: {err}"))?;
        let path = file
            .enclosed_name()
            .ok_or_else(|| format!("schema zip entry `{}` has an unsafe path", file.name()))?;
        let target = destination.join(path);
        if file.is_dir() {
            fs::create_dir_all(&target)
                .map_err(|err| format!("create schema directory `{}`: {err}", target.display()))?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|err| {
                    format!("create schema directory `{}`: {err}", parent.display())
                })?;
            }
            let mut output = fs::File::create(&target)
                .map_err(|err| format!("create schema file `{}`: {err}", target.display()))?;
            io::copy(&mut file, &mut output)
                .map_err(|err| format!("extract schema file `{}`: {err}", target.display()))?;
        }
    }
    Ok(root_entry)
}

fn source_path_key(props: &JsonMap<String, JsonValue>) -> Option<&'static str> {
    if props.contains_key(PROTO_PATH_KEY) {
        Some(PROTO_PATH_KEY)
    } else if props.contains_key(SCHEMA_PATH_KEY) {
        Some(SCHEMA_PATH_KEY)
    } else {
        None
    }
}

fn validate_path_segment(value: &str, label: &str) -> Result<(), String> {
    let path = Path::new(value);
    if value.is_empty()
        || path.components().count() != 1
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(format!("{label} must be a single path segment"));
    }
    Ok(())
}

fn validate_archive_path(path: &Path, original: &str) -> Result<(), String> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(format!("schema zip entry `{original}` has an unsafe path"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;

    fn write_archive(path: &Path, entries: &[(&str, Option<&[u8]>)]) {
        let file = fs::File::create(path).expect("create archive");
        let mut archive = zip::ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .unix_permissions(0o644);
        for (name, contents) in entries {
            if let Some(contents) = contents {
                archive
                    .start_file(*name, options)
                    .expect("start archive file");
                archive.write_all(contents).expect("write archive file");
            } else {
                archive
                    .add_directory(*name, options)
                    .expect("add archive directory");
            }
        }
        archive.finish().expect("finish archive");
    }

    fn schema_props(path: &Path) -> JsonMap<String, JsonValue> {
        let mut props = JsonMap::new();
        props.insert(
            SCHEMA_PATH_KEY.to_string(),
            JsonValue::String(path.to_string_lossy().into_owned()),
        );
        props
    }

    #[test]
    fn resolve_props_from_root_builds_installed_layout_path() {
        let root = Path::new("/staged/schemas");
        let mut props = serde_json::from_value(serde_json::json!({
            "proto_path": "simple.proto",
            "message_type": "Simple"
        }))
        .expect("props");

        resolve_props_from_root(root, "simple_schema", "PROTO", &mut props).expect("resolve props");

        assert_eq!(
            props.get(PROTO_PATH_KEY).and_then(JsonValue::as_str),
            Some("/staged/schemas/proto/simple_schema/simple.proto")
        );
    }

    #[test]
    fn resolve_props_from_root_rejects_non_entry_paths() {
        for entry in [
            "../simple.proto",
            "nested/simple.proto",
            "/tmp/simple.proto",
        ] {
            let mut props = serde_json::from_value(serde_json::json!({
                "proto_path": entry,
                "message_type": "Simple"
            }))
            .expect("props");

            let err = resolve_props_from_root(Path::new("/schemas"), "simple", "proto", &mut props)
                .unwrap_err();
            assert!(err.contains("single path segment"), "entry={entry}: {err}");
        }
    }

    #[test]
    fn resolve_installed_props_reports_conflicting_source_keys() {
        let temp = TempDir::new().expect("temp dir");
        let storage = StorageManager::new(temp.path()).expect("storage");
        let mut props = serde_json::from_value(serde_json::json!({
            "schema_path": "vehicle.json",
            "proto_path": "vehicle.proto"
        }))
        .expect("props");

        let err = resolve_installed_props(&storage, "vehicle", "gbf", &mut props).unwrap_err();

        assert!(err.contains("must not contain both"), "{err}");
    }

    #[test]
    fn extract_accepts_root_entry_and_companion_directory() {
        let temp = TempDir::new().expect("temp dir");
        let archive = temp.path().join("schema.zip");
        let destination = temp.path().join("extracted");
        fs::create_dir(&destination).expect("create destination");
        write_archive(
            &archive,
            &[
                ("vehicle.json", Some(b"{}")),
                ("vehicle/", None),
                ("vehicle/can.dbc", Some(b"VERSION \"\"")),
            ],
        );

        let entry = extract_schema_archive(&archive, &destination).expect("extract schema");

        assert_eq!(entry, PathBuf::from("vehicle.json"));
        assert_eq!(
            fs::read(destination.join("vehicle/can.dbc")).expect("read companion"),
            b"VERSION \"\""
        );
    }

    #[test]
    fn extract_rejects_unsafe_or_invalid_layouts() {
        let cases: &[(&str, &[(&str, Option<&[u8]>)], &str)] = &[
            ("empty", &[], "must not be empty"),
            (
                "multiple_roots",
                &[("one.json", Some(b"{}")), ("two.json", Some(b"{}"))],
                "exactly one root entry",
            ),
            (
                "path_traversal",
                &[("main.json", Some(b"{}")), ("../escape", Some(b"x"))],
                "unsafe path",
            ),
            (
                "backslash",
                &[("main.json", Some(b"{}")), ("main\\file", Some(b"x"))],
                "contains a backslash",
            ),
            (
                "outside_companion",
                &[("main.json", Some(b"{}")), ("other/file", Some(b"x"))],
                "outside companion directory",
            ),
            (
                "companion_is_file",
                &[("main.json", Some(b"{}")), ("main", Some(b"x"))],
                "exactly one root entry",
            ),
        ];

        for (name, entries, expected) in cases {
            let temp = TempDir::new().expect("temp dir");
            let archive = temp.path().join(format!("{name}.zip"));
            let destination = temp.path().join("extracted");
            fs::create_dir(&destination).expect("create destination");
            write_archive(&archive, entries);

            let err = extract_schema_archive(&archive, &destination).unwrap_err();
            assert!(
                err.contains(expected),
                "case {name}: expected `{expected}`, got `{err}`"
            );
        }
    }

    #[test]
    fn extract_enforces_entry_and_uncompressed_size_limits() {
        let temp = TempDir::new().expect("temp dir");
        let archive = temp.path().join("schema.zip");
        write_archive(
            &archive,
            &[
                ("main.json", Some(b"1234")),
                ("main/", None),
                ("main/member", Some(b"x")),
            ],
        );

        let entry_destination = temp.path().join("entry-limit");
        fs::create_dir(&entry_destination).expect("create entry destination");
        let err = extract_schema_archive_with_limits(&archive, &entry_destination, 2, u64::MAX)
            .unwrap_err();
        assert!(err.contains("too many entries"), "got: {err}");

        let size_destination = temp.path().join("size-limit");
        fs::create_dir(&size_destination).expect("create size destination");
        let err = extract_schema_archive_with_limits(&archive, &size_destination, usize::MAX, 4)
            .unwrap_err();
        assert!(err.contains("uncompressed size exceeds"), "got: {err}");
    }

    #[test]
    fn prepared_source_commit_and_rollback_manage_installation() {
        let temp = TempDir::new().expect("temp dir");
        let storage = StorageManager::new(temp.path()).expect("storage");
        let archive = temp.path().join("schema.zip");
        write_archive(&archive, &[("vehicle.json", Some(b"{}"))]);

        let mut source =
            PreparedSchemaSource::prepare(&storage, "vehicle", "gbf", &schema_props(&archive))
                .expect("prepare source");
        let staging_entry = PathBuf::from(
            source
                .parse_props()
                .get(SCHEMA_PATH_KEY)
                .and_then(JsonValue::as_str)
                .expect("parse path"),
        );
        assert!(staging_entry.is_file());
        assert_eq!(
            source
                .stored_props()
                .get(SCHEMA_PATH_KEY)
                .and_then(JsonValue::as_str),
            Some("vehicle.json")
        );

        source.commit().expect("commit source");
        let installed = storage.schemas_dir().join("gbf/vehicle/vehicle.json");
        assert!(installed.is_file());
        assert!(!staging_entry.exists());

        source.rollback().expect("rollback source");
        assert!(!installed.exists());
    }

    #[test]
    fn prepared_schema_tree_rollback_restores_previous_sources() {
        let temp = TempDir::new().expect("temp dir");
        let storage = StorageManager::new(temp.path()).expect("storage");
        let installed = storage.schemas_dir().join("proto/simple/simple.proto");
        fs::create_dir_all(installed.parent().expect("installed parent"))
            .expect("create installed schema directory");
        fs::write(&installed, b"old").expect("write installed schema");

        let source_root = temp.path().join("archive-schemas");
        let imported = source_root.join("proto/simple/simple.proto");
        fs::create_dir_all(imported.parent().expect("imported parent"))
            .expect("create imported schema directory");
        fs::write(&imported, b"new").expect("write imported schema");

        let mut tree =
            PreparedSchemaTree::prepare(&storage, &source_root).expect("prepare schema tree");
        tree.activate().expect("activate schema tree");
        assert_eq!(fs::read(&installed).expect("read imported schema"), b"new");

        tree.rollback().expect("rollback schema tree");
        assert_eq!(fs::read(&installed).expect("read restored schema"), b"old");
    }

    #[test]
    fn prepared_schema_tree_preserves_backup_when_restore_fails() {
        let temp = TempDir::new().expect("temp dir");
        let storage = StorageManager::new(temp.path()).expect("storage");
        let installed = storage.schemas_dir().join("proto/simple/simple.proto");
        fs::create_dir_all(installed.parent().expect("installed parent"))
            .expect("create installed schema directory");
        fs::write(&installed, b"old").expect("write installed schema");

        let source_root = temp.path().join("archive-schemas");
        let imported = source_root.join("proto/simple/simple.proto");
        fs::create_dir_all(imported.parent().expect("imported parent"))
            .expect("create imported schema directory");
        fs::write(&imported, b"new").expect("write imported schema");

        let mut tree =
            PreparedSchemaTree::prepare(&storage, &source_root).expect("prepare schema tree");
        let backup_dir = tree.backup_dir.clone().expect("backup path");
        let work_dir = tree
            .work_dir
            .as_ref()
            .expect("work directory")
            .path()
            .to_path_buf();
        fs::rename(storage.schemas_dir(), &backup_dir).expect("backup installed sources");
        tree.had_target = true;
        tree.backup_active = true;
        fs::create_dir_all(storage.schemas_dir()).expect("recreate target directory");
        fs::write(storage.schemas_dir().join("blocker"), b"x").expect("block restore rename");

        let err = tree.rollback().unwrap_err();

        assert!(err.contains("restore previous schema sources"), "{err}");
        assert!(
            err.contains("preserved schema import recovery files"),
            "{err}"
        );
        assert!(work_dir.join("backup/proto/simple/simple.proto").is_file());
        assert!(tree.work_dir.is_none());
        tree.finished = true;
    }

    #[test]
    fn prepare_cleans_staging_after_invalid_archive() {
        let temp = TempDir::new().expect("temp dir");
        let storage = StorageManager::new(temp.path()).expect("storage");
        let archive = temp.path().join("invalid.zip");
        write_archive(
            &archive,
            &[("one.json", Some(b"{}")), ("two.json", Some(b"{}"))],
        );

        let err =
            PreparedSchemaSource::prepare(&storage, "vehicle", "gbf", &schema_props(&archive))
                .err()
                .expect("invalid archive");

        assert!(err.contains("exactly one root entry"), "got: {err}");
        assert!(!storage.schemas_dir().join("gbf/.vehicle.tmp").exists());
    }

    #[test]
    fn reconcile_preserves_metadata_sources_and_removes_orphans() {
        let temp = TempDir::new().expect("temp dir");
        let storage = StorageManager::new(temp.path()).expect("storage");
        let type_dir = storage.schemas_dir().join("gbf");
        let installed = type_dir.join("installed");
        let orphan = type_dir.join("orphan");
        let staging = type_dir.join(".interrupted.tmp");
        fs::create_dir_all(&installed).expect("create installed");
        fs::create_dir_all(&orphan).expect("create orphan");
        fs::create_dir_all(&staging).expect("create staging");
        fs::write(type_dir.join("unexpected-file"), b"x").expect("write unexpected file");

        let stored = StoredSchema {
            name: "installed".to_string(),
            schema_type: "gbf".to_string(),
            props_json: serde_json::json!({"schema_path": "main.json"}).to_string(),
        };
        reconcile_installed_sources(&storage, &[stored]).expect("reconcile sources");

        assert!(installed.is_dir());
        assert!(!orphan.exists());
        assert!(!staging.exists());
        assert!(!type_dir.join("unexpected-file").exists());
    }
}
