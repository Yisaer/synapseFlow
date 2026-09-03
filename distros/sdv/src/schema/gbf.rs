//! GBF (General Binary Format) schema definitions for packet decoding.
//!
//! This module defines the JSON schema format for describing binary packet structures.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use manager::{ParsedSchema, register_schema};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::codec::gbf_parser::GbfParser;
use crate::decoder::can::{CanFrameIdentityMapping, CanIdMapping};
use crate::schema::arxml::{CompiledArxmlSchema, compile_arxml_schema};
use crate::schema::dbc::{CompiledDbcSchema, compile_dbc_schema};

/// Register the complete GBF schema parser.
pub fn register_gbf_schema() {
    register_schema("gbf", Arc::new(parse_gbf_schema));
}

/// Root schema definition. Contains a single inline `structure` that describes the packet layout.
/// All sequence item types are defined inline via nested `structure` fields, not a named-type registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GbfSchema {
    /// Root packet structure definition. Required — serde will produce a clear
    /// 'missing field `structure`' error at load time for malformed schemas.
    pub structure: GbfStructure,
}

/// File-backed GBF entry containing both packet layout and private format configuration.
#[derive(Debug, Clone, Deserialize)]
struct GbfSchemaDocument {
    structure: GbfStructure,
    #[serde(default)]
    signal_name_pattern: Option<String>,
    format: GbfFormatDefinition,
}

#[derive(Debug, Clone, Deserialize)]
struct GbfFormatDefinition {
    #[serde(rename = "type")]
    format_type: String,
    #[serde(default)]
    props: JsonMap<String, JsonValue>,
}

/// Immutable runtime state produced by resolving one complete GBF schema.
#[derive(Clone)]
pub struct CompiledGbfSchema {
    parser: crate::codec::gbf_parser::GbfParser,
    format: CompiledGbfFormat,
}

#[derive(Clone)]
pub enum CompiledGbfFormat {
    Can {
        schema: Arc<CompiledDbcSchema>,
        clamp_to_range: bool,
        identity_mapping: CanFrameIdentityMapping,
    },
    SomeIp {
        schema: Arc<CompiledArxmlSchema>,
    },
}

impl CompiledGbfSchema {
    pub fn can(
        layout: GbfSchema,
        schema: Arc<CompiledDbcSchema>,
        clamp_to_range: bool,
        can_id_mapping: CanIdMapping,
    ) -> Result<Self, flow::codec::CodecError> {
        let parser = GbfParser::new(layout)?;
        Self::can_with_parser(parser, schema, clamp_to_range, can_id_mapping)
    }

    fn can_with_parser(
        parser: GbfParser,
        schema: Arc<CompiledDbcSchema>,
        clamp_to_range: bool,
        can_id_mapping: CanIdMapping,
    ) -> Result<Self, flow::codec::CodecError> {
        let identity_mapping = resolve_can_identity_mapping(&parser, can_id_mapping)?;
        Ok(Self {
            parser,
            format: CompiledGbfFormat::Can {
                schema,
                clamp_to_range,
                identity_mapping,
            },
        })
    }

    pub fn someip(
        layout: GbfSchema,
        schema: Arc<CompiledArxmlSchema>,
    ) -> Result<Self, flow::codec::CodecError> {
        let parser = GbfParser::new(layout)?;
        Ok(Self::someip_with_parser(parser, schema))
    }

    fn someip_with_parser(parser: GbfParser, schema: Arc<CompiledArxmlSchema>) -> Self {
        Self {
            parser,
            format: CompiledGbfFormat::SomeIp { schema },
        }
    }

    pub fn format(&self) -> &CompiledGbfFormat {
        &self.format
    }

    pub fn parser(&self) -> crate::codec::gbf_parser::GbfParser {
        self.parser.clone()
    }
}

pub(crate) fn resolve_can_identity_mapping(
    parser: &GbfParser,
    can_id_mapping: CanIdMapping,
) -> Result<CanFrameIdentityMapping, flow::codec::CodecError> {
    match (parser.has_bus_id_ref(), can_id_mapping) {
        (true, CanIdMapping::Raw) => Ok(CanFrameIdentityMapping::BusAndCanId),
        (true, CanIdMapping::BusShift { .. }) => Err(flow::codec::CodecError::Other(
            "GBF `bus_id_ref` must not be combined with `CanIdMapping::BusShift`".to_string(),
        )),
        (false, mapping) => Ok(CanFrameIdentityMapping::Mapped(mapping)),
    }
}

fn parse_gbf_schema(
    stream_name: &str,
    props: &JsonMap<String, JsonValue>,
) -> Result<ParsedSchema, String> {
    let entry = props
        .get("schema_path")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "gbf schema requires `schema_path` prop".to_string())?;
    let content = std::fs::read_to_string(entry)
        .map_err(|err| format!("failed to read GBF schema entry `{entry}`: {err}"))?;
    let document: GbfSchemaDocument = serde_json::from_str(&content)
        .map_err(|err| format!("failed to parse GBF schema entry `{entry}`: {err}"))?;
    let member_root = Path::new(entry).with_extension("");
    let layout = GbfSchema {
        structure: document.structure,
    };

    match document.format.format_type.as_str() {
        "can" | "dbc" => {
            let dbc_path = required_member_path(&member_root, &document.format.props, "dbc_path")?;
            let pattern = document
                .signal_name_pattern
                .as_deref()
                .unwrap_or("{sig_name}");
            let clamp_to_range = document
                .format
                .props
                .get("clamp_to_range")
                .and_then(JsonValue::as_bool)
                .unwrap_or(true);
            let parser = GbfParser::new(layout).map_err(|err| err.to_string())?;
            if parser.has_bus_id_ref() && document.format.props.contains_key("can_id_mapping") {
                return Err(
                    "GBF CAN format must not configure `can_id_mapping` when `bus_id_ref` is present"
                        .to_string(),
                );
            }
            let can_id_mapping =
                CanIdMapping::from_prop(document.format.props.get("can_id_mapping"))
                    .map_err(|err| err.to_string())?;
            let path = path_to_str(&dbc_path)?;
            let (schema, compiled) = compile_dbc_schema(stream_name, path, pattern)?;
            let compiled = CompiledGbfSchema::can_with_parser(
                parser,
                compiled,
                clamp_to_range,
                can_id_mapping,
            )
            .map_err(|err| err.to_string())?;
            Ok((schema, None, Some(Arc::new(compiled))))
        }
        "someip" | "arxml" => {
            let parser = GbfParser::new(layout).map_err(|err| err.to_string())?;
            if parser.has_bus_id_ref() {
                return Err("GBF `bus_id_ref` is supported only for CAN format".to_string());
            }
            if parser.has_extend_ref() {
                return Err("GBF `extend_ref` is supported only for CAN format".to_string());
            }
            let arxml_path =
                required_member_path(&member_root, &document.format.props, "arxml_path")?;
            let pattern = document.signal_name_pattern.as_deref().unwrap_or("{field}");
            let path = path_to_str(&arxml_path)?;
            let (schema, compiled) = compile_arxml_schema(stream_name, path, pattern)?;
            let compiled = CompiledGbfSchema::someip_with_parser(parser, compiled);
            Ok((schema, None, Some(Arc::new(compiled))))
        }
        other => Err(format!("unsupported GBF format type `{other}`")),
    }
}

fn required_member_path(
    member_root: &Path,
    props: &JsonMap<String, JsonValue>,
    key: &str,
) -> Result<PathBuf, String> {
    let relative = props
        .get(key)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| format!("GBF format requires `{key}` prop"))?;
    let path = Path::new(relative);
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(format!("GBF format member `{key}` must be a relative path"));
    }
    Ok(member_root.join(path))
}

fn path_to_str(path: &Path) -> Result<&str, String> {
    path.to_str()
        .ok_or_else(|| format!("schema member path is not valid UTF-8: {}", path.display()))
}

/// A structure definition (struct with fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GbfStructure {
    /// Type discriminator (usually "struct").
    #[serde(rename = "type")]
    pub type_name: String,
    /// Fields of the structure.
    #[serde(default)]
    pub fields: Vec<Field>,
}

/// A single field in a struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Field name (used for output key and references).
    pub name: String,
    /// Field type (u8, u16be, u16le, u32be, u64be, bytes, sequence).
    #[serde(rename = "type")]
    pub field_type: String,
    /// Constant value constraint (for magic bytes).
    #[serde(rename = "const")]
    pub const_value: Option<u64>,
    /// Reference to another field for length.
    pub length_ref: Option<String>,
    /// Unit of length. Only `"bytes"` is supported.
    pub length_unit: Option<String>,
    /// For sequence types: the item structure.
    pub structure: Option<GbfStructure>,
    /// Format specification for payload decoding.
    pub format: Option<FormatSpec>,
    /// Bit mask to apply after reading the value (for integers).
    pub read_mask: Option<u64>,
    /// Bit shift to apply after masking.
    pub read_shift: Option<u32>,
}

/// Format specification for payload decoding.
/// Presence of this object marks a field as an embedded payload.
/// The actual format type is determined by decoder configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatSpec {
    /// Reference to the field containing the message ID.
    pub id_ref: Option<String>,
    /// Optional reference to a separate CAN bus ID field.
    pub bus_id_ref: Option<String>,
    /// Optional reference to a CAN IDE / extended-frame flag.
    /// When set, the lookup key is `(extend ? 0x80000000 : 0) | (id & 0x1FFFFFFF)`.
    pub extend_ref: Option<String>,
}

impl GbfSchema {
    /// Load a GBF schema from a JSON file.
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let schema: GbfSchema = serde_json::from_str(&content)?;
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_parse_schema() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { 
                        "name": "frames", 
                        "type": "sequence", 
                        "length_ref": "total_len",
                        "length_unit": "bytes",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "magic", "type": "u8", "const": 85 },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "data_len", "type": "u8" },
                                { 
                                    "name": "payload", 
                                    "type": "bytes",
                                    "length_ref": "data_len",
                                    "format": { "type": "dbc", "id_ref": "can_id" }
                                }
                            ]
                        }
                    }
                ]
            }
        }
        "#;

        let schema: GbfSchema = serde_json::from_str(json).expect("parse schema");
        let root = &schema.structure;

        assert_eq!(root.fields.len(), 3);

        let ts_field = &root.fields[0];
        assert_eq!(ts_field.name, "ts");
        assert_eq!(ts_field.field_type, "u64be");

        let frames_field = &root.fields[2];
        assert_eq!(frames_field.name, "frames");
        assert_eq!(frames_field.field_type, "sequence");
        let can_frame_item = frames_field.structure.as_ref().expect("sequence structure"); // Field::structure stays Option
        assert_eq!(can_frame_item.type_name, "struct");
        assert_eq!(can_frame_item.fields.len(), 4);

        let magic_field = &can_frame_item.fields[0];
        assert_eq!(magic_field.const_value, Some(85));

        let payload_field = &can_frame_item.fields[3];
        assert_eq!(
            payload_field
                .format
                .as_ref()
                .unwrap()
                .id_ref
                .as_ref()
                .unwrap(),
            "can_id"
        );
    }

    #[test]
    fn test_parse_minimal_schema() {
        let json = r#"{ "structure": { "type": "struct", "fields": [] } }"#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        assert_eq!(schema.structure.fields.len(), 0);
    }

    #[test]
    fn test_schema_with_read_mask_and_shift() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "flags", "type": "u8", "read_mask": 127, "read_shift": 1 }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let root = &schema.structure;
        let field = &root.fields[0];
        assert_eq!(field.read_mask, Some(127));
        assert_eq!(field.read_shift, Some(1));
    }

    #[test]
    fn test_schema_with_length_ref() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "len", "type": "u16be" },
                    { "name": "data", "type": "bytes", "length_ref": "len" }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let root = &schema.structure;
        let data_field = &root.fields[1];
        assert_eq!(data_field.length_ref.as_ref().unwrap(), "len");
    }

    #[test]
    fn complete_gbf_schema_reads_name_pattern_only_from_entry_top_level() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "veloflux-gbf-name-pattern-{}-{unique}",
            std::process::id()
        ));
        let entry = root.join("vehicle.json");
        let companion = root.join("vehicle");
        std::fs::create_dir_all(&companion).expect("create companion");
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc"),
            companion.join("vehicle.dbc"),
        )
        .expect("copy DBC");
        let mut document: JsonValue = serde_json::from_slice(
            &std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/spi_packet.json"))
                .expect("read GBF layout"),
        )
        .expect("parse GBF layout");
        document["signal_name_pattern"] =
            JsonValue::String("{bus_name}__{msg_id_hex_upper}__{sig_name}".to_string());
        document["format"] = serde_json::json!({
            "type": "can",
            "props": {
                "dbc_path": "vehicle.dbc",
                "signal_name_pattern": "{sig_name}"
            }
        });
        std::fs::write(
            &entry,
            serde_json::to_vec_pretty(&document).expect("encode GBF entry"),
        )
        .expect("write entry");
        let props = JsonMap::from_iter([(
            "schema_path".to_string(),
            JsonValue::String(entry.to_string_lossy().into_owned()),
        )]);

        let (schema, _, _) = parse_gbf_schema("vehicle", &props).expect("compile GBF schema");

        assert!(schema.contains_column("Bus0__100__StandardUnsigned"));
        assert!(!schema.contains_column("StandardUnsigned"));
        std::fs::remove_dir_all(root).expect("remove test source");
    }

    #[test]
    fn complete_gbf_schema_rejects_bus_id_ref_with_can_id_mapping() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "veloflux-gbf-bus-id-conflict-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create source root");
        let entry = root.join("vehicle.json");
        let document = serde_json::json!({
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "total_len",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "bus_id", "type": "u8" },
                                { "name": "can_id", "type": "u32be" },
                                { "name": "data_len", "type": "u8" },
                                {
                                    "name": "payload",
                                    "type": "bytes",
                                    "length_ref": "data_len",
                                    "format": {
                                        "bus_id_ref": "bus_id",
                                        "id_ref": "can_id"
                                    }
                                }
                            ]
                        }
                    }
                ]
            },
            "format": {
                "type": "can",
                "props": {
                    "dbc_path": "format/vehicle.dbc",
                    "can_id_mapping": "raw"
                }
            }
        });
        std::fs::write(
            &entry,
            serde_json::to_vec_pretty(&document).expect("encode entry"),
        )
        .expect("write entry");
        let props = JsonMap::from_iter([(
            "schema_path".to_string(),
            JsonValue::String(entry.to_string_lossy().into_owned()),
        )]);

        let err = parse_gbf_schema("vehicle", &props).expect_err("conflict must fail");
        assert!(err.contains("must not configure `can_id_mapping`"));
        std::fs::remove_dir_all(root).expect("remove test source");
    }
}
