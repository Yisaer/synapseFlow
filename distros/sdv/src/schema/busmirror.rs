//! Complete BusMirror schemas backed by a JSON entry and companion DBC files.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use manager::{ParsedSchema, register_schema};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::schema::dbc::{CompiledDbcSchema, DbcJson, load_dbc_bus};

const DEFAULT_SIGNAL_NAME_PATTERN: &str = "{bus_name}_{msg_name}_{sig_name}";

/// Register the complete BusMirror schema parser.
pub fn register_busmirror_schema() {
    register_schema("busmirror", Arc::new(parse_busmirror_schema));
}

#[derive(Debug, Deserialize)]
struct BusMirrorDocument {
    version: String,
    #[serde(default)]
    signal_name_pattern: Option<String>,
    format: BusMirrorFormat,
    buses: Vec<BusMirrorBus>,
}

#[derive(Debug, Deserialize)]
struct BusMirrorFormat {
    #[serde(rename = "type")]
    format_type: String,
    #[serde(default, rename = "props")]
    _props: JsonMap<String, JsonValue>,
}

#[derive(Debug, Deserialize)]
struct BusMirrorBus {
    network_type: String,
    network_id: u8,
    name: String,
    dbc: String,
}

/// Immutable runtime state for one complete BusMirror schema.
#[derive(Clone)]
pub struct CompiledBusMirrorSchema {
    dbc: Arc<CompiledDbcSchema>,
}

impl CompiledBusMirrorSchema {
    pub(crate) fn from_dbc(dbc: Arc<CompiledDbcSchema>) -> Self {
        Self { dbc }
    }

    pub fn dbc(&self) -> &Arc<CompiledDbcSchema> {
        &self.dbc
    }
}

fn parse_busmirror_schema(
    stream_name: &str,
    props: &JsonMap<String, JsonValue>,
) -> Result<ParsedSchema, String> {
    let entry = props
        .get("schema_path")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "busmirror schema requires `schema_path` prop".to_string())?;
    let content = std::fs::read_to_string(entry)
        .map_err(|err| format!("failed to read BusMirror schema entry `{entry}`: {err}"))?;
    let document: BusMirrorDocument = serde_json::from_str(&content)
        .map_err(|err| format!("failed to parse BusMirror schema entry `{entry}`: {err}"))?;

    if document.version != "v1" {
        return Err(format!(
            "unsupported BusMirror schema version `{}`; expected `v1`",
            document.version
        ));
    }
    if document.format.format_type != "can" {
        return Err(format!(
            "unsupported BusMirror format type `{}`; expected `can`",
            document.format.format_type
        ));
    }
    if document.buses.is_empty() {
        return Err("BusMirror schema requires at least one bus".to_string());
    }

    let member_root = Path::new(entry).with_extension("");
    let mut bus_identities = HashSet::new();
    let mut frame_identities = HashSet::new();
    let mut buses = Vec::with_capacity(document.buses.len());

    for bus in document.buses {
        let network_type = network_type_id(&bus.network_type)?;
        if bus.name.is_empty() {
            return Err(format!(
                "BusMirror bus ({network_type}, {}) requires a non-empty name",
                bus.network_id
            ));
        }
        if !bus_identities.insert((network_type, bus.network_id)) {
            return Err(format!(
                "duplicate BusMirror bus identity ({network_type}, {})",
                bus.network_id
            ));
        }

        let dbc_path = member_path(&member_root, &bus.dbc)?;
        let encoded_bus_id = (u32::from(network_type) << 8) | u32::from(bus.network_id);
        let dbc_bus = load_dbc_bus(&dbc_path, encoded_bus_id, bus.name.clone()).map_err(|err| {
            format!(
                "failed to compile BusMirror bus `{}` from `{}`: {err}",
                bus.name, bus.dbc
            )
        })?;

        for message in &dbc_bus.messages {
            let normalized_id = message.id & 0x1fff_ffff;
            let identity = (u64::from(network_type) << 40)
                | (u64::from(bus.network_id) << 32)
                | u64::from(normalized_id);
            if !frame_identities.insert(identity) {
                return Err(format!(
                    "duplicate BusMirror frame identity for bus `{}`, message `{}` (0x{normalized_id:X})",
                    bus.name, message.name
                ));
            }
        }
        buses.push(dbc_bus);
    }

    let pattern = document
        .signal_name_pattern
        .as_deref()
        .unwrap_or(DEFAULT_SIGNAL_NAME_PATTERN);
    let compiled = Arc::new(CompiledDbcSchema::new_busmirror(
        DbcJson { buses },
        pattern,
    )?);
    let schema = compiled.schema(stream_name);
    Ok((
        schema,
        None,
        Some(Arc::new(CompiledBusMirrorSchema::from_dbc(compiled))),
    ))
}

fn network_type_id(value: &str) -> Result<u8, String> {
    match value {
        "can" => Ok(1),
        "lin" => Ok(2),
        other => Err(format!(
            "unsupported BusMirror network type `{other}`; expected `can` or `lin`"
        )),
    }
}

fn member_path(member_root: &Path, relative: &str) -> Result<PathBuf, String> {
    let path = Path::new(relative);
    if path.as_os_str().is_empty()
        || relative.contains('\\')
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "BusMirror DBC member `{relative}` must be a relative path"
        ));
    }
    if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_none_or(|extension| !extension.eq_ignore_ascii_case("dbc"))
    {
        return Err(format!(
            "BusMirror member `{relative}` must have a `.dbc` extension"
        ));
    }
    let resolved = member_root.join(path);
    let metadata = std::fs::metadata(&resolved).map_err(|err| {
        format!(
            "failed to access BusMirror DBC member `{relative}` at `{}`: {err}",
            resolved.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "BusMirror DBC member `{relative}` is not a regular file"
        ));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_schema_source() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "veloflux-busmirror-schema-{}-{unique}",
            std::process::id()
        ))
    }

    #[test]
    fn compiles_multiple_private_dbcs_and_network_name_tokens() {
        let root = temp_schema_source();
        let entry = root.join("vehicle.json");
        let companion = root.join("vehicle");
        std::fs::create_dir_all(&companion).expect("create companion");
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
        std::fs::copy(&fixture, companion.join("powertrain.dbc")).expect("copy first DBC");
        std::fs::copy(&fixture, companion.join("body.dbc")).expect("copy second DBC");
        std::fs::write(
            &entry,
            r#"{
                "version": "v1",
                "signal_name_pattern": "{network_id}__{msg_id_hex_upper}__{sig_name}",
                "format": { "type": "can", "props": {} },
                "buses": [
                    { "network_type": "can", "network_id": 1, "name": "Powertrain", "dbc": "powertrain.dbc" },
                    { "network_type": "lin", "network_id": 2, "name": "Body", "dbc": "body.dbc" }
                ]
            }"#,
        )
        .expect("write entry");
        let props = JsonMap::from_iter([(
            "schema_path".to_string(),
            JsonValue::String(entry.to_string_lossy().into_owned()),
        )]);

        let (schema, _, artifact) =
            parse_busmirror_schema("vehicle", &props).expect("compile BusMirror schema");

        assert!(schema.contains_column("1__100__StandardUnsigned"));
        assert!(schema.contains_column("2__100__StandardUnsigned"));
        assert!(
            artifact
                .and_then(|value| value.downcast::<CompiledBusMirrorSchema>().ok())
                .is_some()
        );
        std::fs::remove_dir_all(root).expect("remove test source");
    }
}
