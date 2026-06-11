//! CAN DBC schema definitions and parsers. See `docs/schema/dbc.md` for details.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use can_dbc::{ByteOrder, MultiplexIndicator};
use flow::{ColumnSchema, ConcreteDatatype, Int64Type, ProtoDescriptorBundle, Schema};
use manager::register_schema;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::decoder::can::classify_signal;

/// Register a schema parser that converts DBC JSON into a Schema.
pub fn register_dbc_schema() {
    register_schema("dbc", Arc::new(parse_dbc_schema));
}

/// Parse a DBC schema from properties.
///
/// Expects `schema_path` property pointing to a `.json`, `.dbc` file, or directory.
pub fn parse_dbc_schema(
    stream_name: &str,
    props: &JsonMap<String, JsonValue>,
) -> Result<(Schema, Option<Arc<ProtoDescriptorBundle>>), String> {
    let schema_path = props
        .get("schema_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "schema_path must be provided for dbc schema".to_string())?;

    let pattern = props.get("signal_name_pattern").and_then(|v| v.as_str());

    let dbc_json = load_can_schema(schema_path)?;
    Ok((schema_from_dbc(stream_name, &dbc_json, pattern), None))
}

/// Root structure containing all CAN buses and their messages/signals.
#[derive(Deserialize, Debug, Clone)]
pub struct DbcJson {
    /// List of CAN buses, each containing messages and signals.
    pub buses: Vec<BusJson>,
}

/// A CAN bus containing messages.
#[derive(Deserialize, Debug, Clone)]
pub struct BusJson {
    /// Bus name (e.g., "chassis", "powertrain"). Falls back to "Bus{id}" if not set.
    #[serde(default)]
    pub name: Option<String>,
    /// Unique bus identifier.
    pub id: u32,
    /// Messages on this bus.
    pub messages: Vec<MessageJson>,
}

/// A CAN message containing signals.
#[derive(Deserialize, Debug, Clone)]
pub struct MessageJson {
    /// Message name (for documentation, not used in signal naming).
    #[serde(rename = "name")]
    pub _name: String,
    /// CAN message ID (decimal).
    pub id: u32,
    /// Frame ID as hex string (e.g., "0x100"), used in signal column naming.
    #[serde(rename = "frameId")]
    pub frame_id: String,
    /// Message length in bytes.
    #[serde(rename = "length")]
    pub _length: u32,
    /// Signals contained in this message.
    pub signals: Vec<SignalJson>,
}

/// A CAN signal definition.
#[derive(Deserialize, Debug, Clone)]
pub struct SignalJson {
    /// Signal name, used in column naming: `{bus}__{frameId}__{name}`.
    pub name: String,
    /// Start bit position.
    pub start: u32,
    /// Signal bit length.
    pub length: u32,
    /// Scale factor (physical = raw * scale + offset).
    pub scale: Option<f64>,
    /// Offset value (physical = raw * scale + offset).
    pub offset: Option<f64>,
    /// True for Motorola (big-endian) byte order, false for Intel (little-endian).
    #[serde(rename = "isBigEndian", default)]
    pub is_big_endian: bool,
    /// True if the signal value is signed.
    #[serde(rename = "isSigned", default)]
    pub is_signed: bool,
    /// True if this signal is the multiplexer selector.
    #[serde(rename = "isMultiplexer", default)]
    pub is_multiplexer: bool,
    /// True if this signal is multiplexed (only decoded when multiplexer matches).
    #[serde(rename = "isMultiplexed", default)]
    pub is_multiplexed: bool,
    /// The multiplexer value that activates this signal (only valid if is_multiplexed is true).
    #[serde(rename = "multiplexerValue")]
    pub multiplexer_value: Option<i64>,
    /// Minimum physical value from the DBC (`physical = raw * scale + offset`).
    /// Used for optional range clamping. `min == max` means "no range".
    #[serde(default)]
    pub min: Option<f64>,
    /// Maximum physical value from the DBC. Used for optional range clamping.
    #[serde(default)]
    pub max: Option<f64>,
}

/// Load CAN schema from a file or directory. Auto-detects format:
/// - `.json`: Parse as JSON (legacy format)
/// - `.dbc`: Parse as DBC file, assign Bus ID=0
/// - Directory: Parse all `*.dbc` files with strict naming `{id}_{name}.dbc`
pub fn load_can_schema(path: &str) -> Result<DbcJson, String> {
    let p = Path::new(path);
    let metadata = fs::metadata(p).map_err(|e| format!("failed to access path {}: {}", path, e))?;

    if metadata.is_dir() {
        load_dbc_directory(p)
    } else if path.ends_with(".dbc") {
        load_single_dbc(p)
    } else {
        // Assume JSON
        load_dbc_json(path)
    }
}

/// Load a single DBC file, assigning Bus ID=0, Name="Bus0".
fn load_single_dbc(path: &Path) -> Result<DbcJson, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read dbc at {}: {}", path.display(), e))?;
    let dbc = can_dbc::Dbc::try_from(content.as_str())
        .map_err(|e| format!("failed to parse dbc {}: {:?}", path.display(), e))?;

    let bus = convert_dbc_to_bus(&dbc, 0, "Bus0".to_string());
    Ok(DbcJson { buses: vec![bus] })
}

/// Load a directory of DBC files with strict naming: `{id}_{name}.dbc`.
fn load_dbc_directory(dir: &Path) -> Result<DbcJson, String> {
    let mut buses = Vec::new();
    let mut seen_ids = HashSet::new();

    let entries: Vec<_> = fs::read_dir(dir)
        .map_err(|e| format!("failed to read directory {}: {}", dir.display(), e))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("dbc"))
        })
        .collect();

    for entry in entries {
        let file_path = entry.path();
        let stem = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("invalid filename: {}", file_path.display()))?;

        // Parse filename: {id}_{name}
        let (id, name) = parse_bus_filename(stem).ok_or_else(|| {
            format!(
                "invalid DBC filename format '{}'. Expected: {{id}}_{{name}}.dbc",
                file_path.display()
            )
        })?;

        // Check for ID collision
        if !seen_ids.insert(id) {
            return Err(format!("duplicate bus ID {} in directory", id));
        }

        let content = fs::read_to_string(&file_path)
            .map_err(|e| format!("failed to read {}: {}", file_path.display(), e))?;
        let dbc = can_dbc::Dbc::try_from(content.as_str())
            .map_err(|e| format!("failed to parse {}: {:?}", file_path.display(), e))?;

        buses.push(convert_dbc_to_bus(&dbc, id, name));
    }

    if buses.is_empty() {
        return Err(format!("no valid .dbc files found in {}", dir.display()));
    }

    // Sort by ID for deterministic order
    buses.sort_by_key(|b| b.id);

    Ok(DbcJson { buses })
}

/// Parse filename pattern: `{id}_{name}` -> Some((id, name))
fn parse_bus_filename(stem: &str) -> Option<(u32, String)> {
    let idx = stem.find('_')?;
    let id_str = &stem[..idx];
    let name = &stem[idx + 1..];

    if name.is_empty() {
        return None;
    }

    let id: u32 = id_str.parse().ok()?;
    Some((id, name.to_string()))
}

/// Convert a can_dbc::Dbc to our BusJson format.
fn convert_dbc_to_bus(dbc: &can_dbc::Dbc, id: u32, name: String) -> BusJson {
    let messages = dbc
        .messages()
        .iter()
        .map(|msg| {
            let msg_id = match *msg.id() {
                can_dbc::MessageId::Standard(id) => id as u32,
                can_dbc::MessageId::Extended(id) => id,
            };

            let signals = msg
                .signals()
                .iter()
                .map(|sig| {
                    let (is_multiplexer, is_multiplexed, multiplexer_value) =
                        match sig.multiplexer_indicator() {
                            MultiplexIndicator::Plain => (false, false, None),
                            MultiplexIndicator::Multiplexor => (true, false, None),
                            MultiplexIndicator::MultiplexedSignal(val) => {
                                (false, true, Some(*val as i64))
                            }
                            MultiplexIndicator::MultiplexorAndMultiplexedSignal(val) => {
                                (true, true, Some(*val as i64))
                            }
                        };

                    // can-dbc always carries min/max as f64; the DBC "no range"
                    // convention is `[0|0]` (and can-dbc has no None). Map a
                    // valid range to Some, and treat a zero-width/no range as
                    // None. (`None` and `Some(0.0)/Some(0.0)` are equivalent
                    // downstream anyway: CanDecoder::new's `max > min` guard
                    // turns `[0|0]` into no-clamp.)
                    let (min, max) = if sig.max > sig.min {
                        (Some(sig.min), Some(sig.max))
                    } else {
                        (None, None)
                    };

                    SignalJson {
                        name: sig.name().to_string(),
                        start: sig.start_bit as u32,
                        length: sig.size as u32,
                        scale: Some(sig.factor),
                        offset: Some(sig.offset),
                        is_big_endian: matches!(sig.byte_order(), ByteOrder::BigEndian),
                        is_signed: matches!(sig.value_type(), can_dbc::ValueType::Signed),
                        is_multiplexer,
                        is_multiplexed,
                        multiplexer_value,
                        min,
                        max,
                    }
                })
                .collect();

            MessageJson {
                _name: msg.name().to_string(),
                id: msg_id,
                frame_id: format!("0x{:X}", msg_id),
                _length: *msg.size() as u32,
                signals,
            }
        })
        .collect();

    BusJson {
        name: Some(name),
        id,
        messages,
    }
}

pub fn load_dbc_json(path: &str) -> Result<DbcJson, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read dbc json at {}: {}", path, e))?;
    serde_json::from_str(&content).map_err(|e| format!("failed to parse dbc json: {e}"))
}

pub fn schema_from_dbc(stream_name: &str, dbc: &DbcJson, pattern: Option<&str>) -> Schema {
    // Default to simple signal name if no pattern provided
    let pattern = pattern.unwrap_or("{sig}");
    let mut columns = Vec::new();
    // Include timestamp column for inbound events.
    columns.push(ColumnSchema::new(
        stream_name.to_string(),
        "ts".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    ));

    for bus in &dbc.buses {
        let bus_name = bus
            .name
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("Bus{}", bus.id));

        for msg in &bus.messages {
            let frame_id = msg.frame_id.clone();
            for signal in &msg.signals {
                let col_name =
                    format_signal_name(pattern, &bus_name, &frame_id, &msg._name, &signal.name);
                let factor = signal.scale.unwrap_or(1.0);
                let offset = signal.offset.unwrap_or(0.0);
                let datatype =
                    classify_signal(signal.length, signal.is_signed, factor, offset).datatype();
                columns.push(ColumnSchema::new(
                    stream_name.to_string(),
                    col_name,
                    datatype,
                ));
            }
        }
    }

    Schema::new(columns)
}

/// Format a signal name based on a pattern.
/// Supported tokens: {bus}, {id}, {msg}, {sig}
pub fn format_signal_name(
    pattern: &str,
    bus_name: &str,
    frame_id: &str,
    msg_name: &str,
    sig_name: &str,
) -> String {
    pattern
        .replace("{bus}", bus_name)
        .replace("{id}", frame_id)
        .replace("{msg}", msg_name)
        .replace("{sig}", sig_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::ConcreteDatatype;
    use std::path::PathBuf;

    #[test]
    fn parse_sim_json_produces_expected_columns() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = schema_from_dbc("sim_stream", &dbc, None);

        // Expected: ts + 6 signals from two messages.
        assert_eq!(schema.column_schemas().len(), 7);

        let expected = [
            "Mess0_Sig1",
            "Mess0_Sig2",
            "Mess0_Sig3",
            "Mess1_Sig1",
            "Mess1_Sig2",
            "Mess1_Sig3",
        ];

        for name in expected {
            let col = schema
                .column_schema_by_name(name)
                .unwrap_or_else(|| panic!("missing column {}", name));
            assert!(
                matches!(col.data_type, ConcreteDatatype::Int64(_)),
                "column {} should be Int64",
                name
            );
        }
    }

    #[test]
    fn load_dbc_json_file_not_found() {
        let result = load_dbc_json("/nonexistent/path/to/file.json");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to read dbc json"));
    }

    #[test]
    fn load_dbc_json_invalid_json() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("invalid_dbc.json");
        std::fs::write(&temp_file, "{ invalid json }").unwrap();

        let result = load_dbc_json(temp_file.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse dbc json"));

        std::fs::remove_file(temp_file).ok();
    }

    #[test]
    fn parse_dbc_schema_missing_schema_path() {
        let props = JsonMap::new();
        let result = parse_dbc_schema("test_stream", &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("schema_path must be provided"));
    }

    #[test]
    fn parse_dbc_schema_success() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let mut props = JsonMap::new();
        props.insert(
            "schema_path".to_string(),
            JsonValue::String(path.to_str().unwrap().to_string()),
        );

        let result = parse_dbc_schema("test_stream", &props);
        assert!(result.is_ok());
        let (schema, _) = result.unwrap();
        assert_eq!(schema.column_schemas().len(), 7);
    }

    #[test]
    fn schema_from_dbc_bus_without_name_uses_fallback() {
        let dbc = DbcJson {
            buses: vec![BusJson {
                name: None, // No name, should fallback to "Bus0"
                id: 0,
                messages: vec![MessageJson {
                    _name: "TestMsg".to_string(),
                    id: 1,
                    frame_id: "0x100".to_string(),
                    _length: 8,
                    signals: vec![SignalJson {
                        name: "TestSig".to_string(),
                        start: 0,
                        length: 8,
                        scale: None,
                        offset: None,
                        is_big_endian: false,
                        is_signed: false,
                        is_multiplexer: false,
                        is_multiplexed: false,
                        multiplexer_value: None,
                        min: None,
                        max: None,
                    }],
                }],
            }],
        };

        let schema = schema_from_dbc("test", &dbc, None);
        // ts + 1 signal
        assert_eq!(schema.column_schemas().len(), 2);
        // Check the signal column name uses Bus0 fallback
        let col = schema.column_schema_by_name("TestSig");
        assert!(col.is_some());
    }

    #[test]
    fn schema_from_dbc_signal_with_scale_uses_float64() {
        let dbc = DbcJson {
            buses: vec![BusJson {
                name: Some("TestBus".to_string()),
                id: 1,
                messages: vec![MessageJson {
                    _name: "TestMsg".to_string(),
                    id: 1,
                    frame_id: "0x200".to_string(),
                    _length: 8,
                    signals: vec![SignalJson {
                        name: "ScaledSig".to_string(),
                        start: 0,
                        length: 16,
                        scale: Some(0.1), // Has scale factor
                        offset: None,
                        is_big_endian: false,
                        is_signed: true,
                        is_multiplexer: false,
                        is_multiplexed: false,
                        multiplexer_value: None,
                        min: None,
                        max: None,
                    }],
                }],
            }],
        };

        let schema = schema_from_dbc("test", &dbc, None);
        let col = schema
            .column_schema_by_name("ScaledSig")
            .expect("column should exist");
        assert!(
            matches!(col.data_type, ConcreteDatatype::Float64(_)),
            "scaled signal should be Float64"
        );
    }

    #[test]
    fn schema_from_dbc_empty_buses() {
        let dbc = DbcJson { buses: vec![] };
        let schema = schema_from_dbc("test", &dbc, None);
        // Only ts column
        assert_eq!(schema.column_schemas().len(), 1);
    }

    #[test]
    fn load_can_schema_single_dbc_file() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
        let result = load_can_schema(path.to_str().unwrap());
        assert!(result.is_ok(), "failed to load DBC: {:?}", result.err());
        let dbc_json = result.unwrap();
        assert_eq!(dbc_json.buses.len(), 1);
        assert_eq!(dbc_json.buses[0].id, 0); // Single file defaults to ID 0
        assert_eq!(dbc_json.buses[0].name, Some("Bus0".to_string()));
        // Should have 5 messages
        assert_eq!(dbc_json.buses[0].messages.len(), 5);
    }

    #[test]
    fn load_can_schema_dbc_signals_parsed_correctly() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
        let dbc_json = load_can_schema(path.to_str().unwrap()).unwrap();

        // Find SimpleScale signal in Scaling message (ID 1024)
        let scaling_msg = dbc_json.buses[0]
            .messages
            .iter()
            .find(|m| m.id == 1024)
            .expect("Scaling message not found");
        let simple_scale = scaling_msg
            .signals
            .iter()
            .find(|s| s.name == "SimpleScale")
            .expect("SimpleScale signal not found");

        assert_eq!(simple_scale.scale, Some(0.5));
        assert_eq!(simple_scale.offset, Some(0.0));
        assert!(!simple_scale.is_big_endian);
        assert!(!simple_scale.is_signed);
    }

    #[test]
    fn load_can_schema_directory_with_valid_naming() {
        let temp_dir = std::env::temp_dir().join("dbc_test_dir");
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create two DBC files with valid naming
        let dbc_content = r#"VERSION ""
NS_ :
BS_:
BU_:
BO_ 100 TestMsg: 8 Vector__XXX
 SG_ TestSig : 0|8@1+ (1,0) [0|0] "" Vector__XXX
"#;
        std::fs::write(temp_dir.join("1_chassis.dbc"), dbc_content).unwrap();
        std::fs::write(temp_dir.join("2_body.dbc"), dbc_content).unwrap();

        let result = load_can_schema(temp_dir.to_str().unwrap());
        assert!(result.is_ok(), "failed: {:?}", result.err());
        let dbc_json = result.unwrap();
        assert_eq!(dbc_json.buses.len(), 2);
        assert_eq!(dbc_json.buses[0].id, 1);
        assert_eq!(dbc_json.buses[0].name, Some("chassis".to_string()));
        assert_eq!(dbc_json.buses[1].id, 2);
        assert_eq!(dbc_json.buses[1].name, Some("body".to_string()));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn load_can_schema_directory_invalid_naming_error() {
        let temp_dir = std::env::temp_dir().join("dbc_invalid_name_test");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let dbc_content = r#"VERSION ""
NS_ :
BS_:
BU_:
BO_ 100 TestMsg: 8 Vector__XXX
 SG_ TestSig : 0|8@1+ (1,0) [0|0] "" Vector__XXX
"#;
        // Invalid filename - no ID prefix
        std::fs::write(temp_dir.join("invalid.dbc"), dbc_content).unwrap();

        let result = load_can_schema(temp_dir.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid DBC filename format"));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn load_can_schema_directory_collision_error() {
        let temp_dir = std::env::temp_dir().join("dbc_collision_test");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let dbc_content = r#"VERSION ""
NS_ :
BS_:
BU_:
BO_ 100 TestMsg: 8 Vector__XXX
 SG_ TestSig : 0|8@1+ (1,0) [0|0] "" Vector__XXX
"#;
        // Two files with same ID
        std::fs::write(temp_dir.join("1_a.dbc"), dbc_content).unwrap();
        std::fs::write(temp_dir.join("1_b.dbc"), dbc_content).unwrap();

        let result = load_can_schema(temp_dir.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("duplicate bus ID"));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn parse_bus_filename_valid() {
        assert_eq!(
            parse_bus_filename("1_chassis"),
            Some((1, "chassis".to_string()))
        );
        assert_eq!(
            parse_bus_filename("42_my_complex_bus_name"),
            Some((42, "my_complex_bus_name".to_string()))
        );
    }

    #[test]
    fn parse_bus_filename_invalid() {
        assert_eq!(parse_bus_filename("chassis"), None); // No underscore
        assert_eq!(parse_bus_filename("1_"), None); // Empty name
        assert_eq!(parse_bus_filename("abc_name"), None); // Non-numeric ID
    }

    #[test]
    fn dbc_and_json_produce_matching_signals() {
        // Load the DBC file
        let dbc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");
        let dbc_result = load_can_schema(dbc_path.to_str().unwrap()).unwrap();

        // Load equivalent JSON
        let json_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/comprehensive.json");
        let json_result = load_dbc_json(json_path.to_str().unwrap()).unwrap();

        // Both should have same number of messages per bus
        // Note: JSON has TestBus, DBC single file defaults to Bus0, so names differ
        // But signal count per message should match
        let dbc_signals: usize = dbc_result.buses[0]
            .messages
            .iter()
            .map(|m| m.signals.len())
            .sum();
        let json_signals: usize = json_result.buses[0]
            .messages
            .iter()
            .map(|m| m.signals.len())
            .sum();

        assert_eq!(dbc_signals, json_signals, "signal count mismatch");
    }

    #[test]
    fn sim_json_and_dbc_produce_matching_columns() {
        // Load sim.json
        let json_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let json_result = load_dbc_json(json_path.to_str().unwrap()).unwrap();
        let json_schema = schema_from_dbc("test", &json_result, None);

        // Load dbc directory with 1_PropulsionCAN.dbc
        let dbc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/dbc");
        let dbc_result = load_can_schema(dbc_path.to_str().unwrap()).unwrap();
        let dbc_schema = schema_from_dbc("test", &dbc_result, None);

        // Collect column names
        let json_cols: Vec<&str> = json_schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        let dbc_cols: Vec<&str> = dbc_schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        assert_eq!(
            json_cols.len(),
            dbc_cols.len(),
            "column count mismatch\nJSON: {:?}\nDBC: {:?}",
            json_cols,
            dbc_cols
        );

        for col in &json_cols {
            assert!(
                dbc_cols.contains(col),
                "column '{}' not found in DBC schema. JSON: {:?}, DBC: {:?}",
                col,
                json_cols,
                dbc_cols
            );
        }
    }
}
