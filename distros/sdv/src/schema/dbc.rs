//! CAN DBC schema definitions and parsers. See `docs/schema/dbc.md` for details.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use can_dbc::{ByteOrder, MultiplexIndicator, NumericValue};
use flow::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use manager::{ParsedSchema, register_schema};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::decoder::can::classify_signal;
use crate::schema::name_pattern::{
    CompiledNamePattern, DbcNameContext, DbcNamePatternMode, NetworkNameContext,
};

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
) -> Result<ParsedSchema, String> {
    let schema_path = props
        .get("schema_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "schema_path must be provided for dbc schema".to_string())?;

    let pattern = props
        .get("signal_name_pattern")
        .and_then(|v| v.as_str())
        .unwrap_or("{sig_name}");

    let (schema, compiled) = compile_dbc_schema(stream_name, schema_path, pattern)?;
    Ok((schema, None, Some(compiled)))
}

/// Compile a DBC source for either a standalone DBC schema or a private GBF format.
pub fn compile_dbc_schema(
    stream_name: &str,
    schema_path: &str,
    signal_name_pattern: &str,
) -> Result<(Schema, Arc<CompiledDbcSchema>), String> {
    let dbc_json = load_can_schema(schema_path)?;
    let compiled = Arc::new(CompiledDbcSchema::new(dbc_json, signal_name_pattern)?);
    let schema = compiled.schema(stream_name);
    Ok((schema, compiled))
}

#[derive(Clone)]
pub struct CompiledDbcSchema {
    dbc: Arc<DbcJson>,
    signal_name_pattern: Arc<str>,
    compiled_name_pattern: CompiledNamePattern,
    naming_mode: DbcNamePatternMode,
}

impl CompiledDbcSchema {
    pub fn new(dbc: DbcJson, signal_name_pattern: &str) -> Result<Self, String> {
        Self::build(dbc, signal_name_pattern, DbcNamePatternMode::Standard)
    }

    pub fn new_busmirror(dbc: DbcJson, signal_name_pattern: &str) -> Result<Self, String> {
        Self::build(dbc, signal_name_pattern, DbcNamePatternMode::BusMirror)
    }

    fn build(
        dbc: DbcJson,
        signal_name_pattern: &str,
        naming_mode: DbcNamePatternMode,
    ) -> Result<Self, String> {
        let compiled_name_pattern = CompiledNamePattern::compile(signal_name_pattern, naming_mode)?;
        validate_dbc_signal_ranges(&dbc)?;
        if naming_mode == DbcNamePatternMode::BusMirror {
            for bus in &dbc.buses {
                for message in &bus.messages {
                    if message.id > 0x1fff_ffff {
                        return Err(format!(
                            "BusMirror DBC message `{}` on bus {} has ID 0x{:X} outside the 29-bit CAN identity range",
                            message.name, bus.id, message.id
                        ));
                    }
                }
            }
        }
        let schema = Self {
            dbc: Arc::new(dbc),
            signal_name_pattern: Arc::from(signal_name_pattern),
            compiled_name_pattern,
            naming_mode,
        };
        schema.validate_column_names()?;
        Ok(schema)
    }

    pub fn dbc(&self) -> Arc<DbcJson> {
        Arc::clone(&self.dbc)
    }

    pub fn pattern(&self) -> &str {
        &self.signal_name_pattern
    }

    pub fn column_name(
        &self,
        bus_id: u32,
        bus_name: &str,
        message: &MessageJson,
        signal_name: &str,
    ) -> String {
        let network = match self.naming_mode {
            DbcNamePatternMode::Standard => None,
            DbcNamePatternMode::BusMirror => {
                let network_type_id = (bus_id >> 8) as u8;
                Some(NetworkNameContext {
                    network_type: network_type_name(network_type_id),
                    network_type_id,
                    network_id: bus_id as u8,
                })
            }
        };
        self.compiled_name_pattern.render(&DbcNameContext {
            bus_name,
            bus_id,
            message_id: message.id,
            message_name: &message.name,
            signal_name,
            network,
        })
    }

    pub fn schema(&self, stream_name: &str) -> Schema {
        let mut columns = Vec::new();
        columns.push(ColumnSchema::new(
            stream_name.to_string(),
            "ts".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ));
        for bus in &self.dbc.buses {
            let bus_name = bus.name.clone().unwrap_or_else(|| format!("Bus{}", bus.id));
            for message in &bus.messages {
                for signal in &message.signals {
                    columns.push(ColumnSchema::new(
                        stream_name.to_string(),
                        self.column_name(bus.id, &bus_name, message, &signal.name),
                        classify_signal(
                            signal.length,
                            signal.is_signed,
                            signal.scale.unwrap_or(1.0),
                            signal.offset.unwrap_or(0.0),
                        )
                        .datatype(),
                    ));
                }
            }
        }
        Schema::new(columns)
    }

    fn validate_column_names(&self) -> Result<(), String> {
        let mut names = HashSet::new();
        names.insert("ts".to_string());
        for bus in &self.dbc.buses {
            let bus_name = bus.name.clone().unwrap_or_else(|| format!("Bus{}", bus.id));
            for message in &bus.messages {
                for signal in &message.signals {
                    let name = self.column_name(bus.id, &bus_name, message, &signal.name);
                    if name.is_empty() {
                        return Err(format!(
                            "signal name pattern produced an empty column for bus `{bus_name}`, message `{}`, signal `{}`",
                            message.name, signal.name
                        ));
                    }
                    if !names.insert(name.clone()) {
                        return Err(format!(
                            "signal name pattern produced duplicate column `{name}` for bus `{bus_name}`, message `{}`, signal `{}`",
                            message.name, signal.name
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn network_type_name(network_type_id: u8) -> &'static str {
    match network_type_id {
        1 => "can",
        2 => "lin",
        _ => "unknown",
    }
}

fn validate_dbc_signal_ranges(dbc: &DbcJson) -> Result<(), String> {
    for bus in &dbc.buses {
        for message in &bus.messages {
            let frame_bits = usize::try_from(message._length)
                .ok()
                .and_then(|length| length.checked_mul(8))
                .ok_or_else(|| {
                    format!(
                        "DBC message `{}` on bus {} has an invalid frame length {}",
                        message.name, bus.id, message._length
                    )
                })?;
            for signal in &message.signals {
                if signal.length == 0 || signal.length > 64 {
                    return Err(format!(
                        "DBC signal `{}` in message `{}` on bus {} has invalid bit length {}",
                        signal.name, message.name, bus.id, signal.length
                    ));
                }
                let last_bit = dbc_signal_last_bit(signal).ok_or_else(|| {
                    format!(
                        "DBC signal `{}` in message `{}` on bus {} has an overflowing bit range",
                        signal.name, message.name, bus.id
                    )
                })?;
                if last_bit >= frame_bits {
                    return Err(format!(
                        "DBC signal `{}` in message `{}` on bus {} exceeds the {}-byte frame: start={}, length={}",
                        signal.name,
                        message.name,
                        bus.id,
                        message._length,
                        signal.start,
                        signal.length
                    ));
                }
            }
        }
    }
    Ok(())
}

fn dbc_signal_last_bit(signal: &SignalJson) -> Option<usize> {
    let start = usize::try_from(signal.start).ok()?;
    let length = usize::try_from(signal.length).ok()?;
    if signal.is_big_endian {
        let start_byte = start / 8;
        let bits_in_start_byte = start % 8 + 1;
        if length <= bits_in_start_byte {
            Some(start)
        } else {
            let remaining = length - bits_in_start_byte;
            start_byte
                .checked_add(remaining.div_ceil(8))?
                .checked_mul(8)?
                .checked_add(7)
        }
    } else {
        start.checked_add(length - 1)
    }
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
    /// Message name, used in column naming via `{msg_name}` token and in error messages.
    pub name: String,
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
    let dbc = parse_dbc_file(path)?;
    let bus = convert_dbc_to_bus(&dbc, 0, "Bus0".to_string());
    Ok(DbcJson { buses: vec![bus] })
}

/// Load one DBC file and assign the bus identity supplied by its owning schema.
pub(crate) fn load_dbc_bus(path: &Path, id: u32, name: String) -> Result<BusJson, String> {
    let dbc = parse_dbc_file(path)?;
    Ok(convert_dbc_to_bus(&dbc, id, name))
}

fn parse_dbc_file(path: &Path) -> Result<can_dbc::Dbc, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read dbc at {}: {}", path.display(), e))?;
    can_dbc::Dbc::try_from(content.as_str())
        .map_err(|e| format!("failed to parse dbc {}: {e}", path.display()))
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

        let dbc = parse_dbc_file(&file_path)?;
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
        .messages
        .iter()
        .map(|msg| {
            let msg_id = match msg.id {
                can_dbc::MessageId::Standard(id) => {
                    let id = id as u32;
                    // A standard frame is 11-bit by definition. A larger value
                    // means the `BO_` id was not flagged as extended (bit 31), so
                    // `can-dbc` took the standard branch and may have truncated a
                    // wide id to 16 bits — it will then never match a wire frame
                    // carrying the real id. Warn instead of failing silently
                    // (issue #202). See docs/schema/dbc.md "Extended / 29-bit".
                    if id > 0x7FF {
                        tracing::warn!(
                            message = %msg.name,
                            msg_id = format!("0x{id:X}"),
                            "DBC message id exceeds the 11-bit standard range but is \
                             not marked as an extended frame; an extended id may be \
                             missing its bit-31 flag and could be truncated"
                        );
                    }
                    id
                }
                can_dbc::MessageId::Extended(id) => id,
            };

            let signals = msg
                .signals
                .iter()
                .map(|sig| {
                    let (is_multiplexer, is_multiplexed, multiplexer_value) =
                        match sig.multiplexer_indicator {
                            MultiplexIndicator::Plain => (false, false, None),
                            MultiplexIndicator::Multiplexor => (true, false, None),
                            MultiplexIndicator::MultiplexedSignal(val) => {
                                (false, true, Some(val as i64))
                            }
                            MultiplexIndicator::MultiplexorAndMultiplexedSignal(val) => {
                                (true, true, Some(val as i64))
                            }
                        };

                    // can-dbc preserves integer bounds in NumericValue. VeloFlux
                    // applies physical scaling as f64, so convert them at this
                    // boundary. Treat a zero-width range as unspecified.
                    let min = numeric_value_to_f64(sig.min);
                    let max = numeric_value_to_f64(sig.max);
                    let (min, max) = if max > min {
                        (Some(min), Some(max))
                    } else {
                        (None, None)
                    };

                    SignalJson {
                        name: sig.name.to_string(),
                        start: sig.start_bit as u32,
                        length: sig.size as u32,
                        scale: Some(sig.factor),
                        offset: Some(sig.offset),
                        is_big_endian: matches!(sig.byte_order, ByteOrder::BigEndian),
                        is_signed: matches!(sig.value_type, can_dbc::ValueType::Signed),
                        is_multiplexer,
                        is_multiplexed,
                        multiplexer_value,
                        min,
                        max,
                    }
                })
                .collect();

            MessageJson {
                name: msg.name.to_string(),
                id: msg_id,
                frame_id: format!("0x{:X}", msg_id),
                _length: msg.size as u32,
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

fn numeric_value_to_f64(value: NumericValue) -> f64 {
    match value {
        NumericValue::Uint(value) => value as f64,
        NumericValue::Int(value) => value as f64,
        NumericValue::Double(value) => value,
    }
}

pub fn load_dbc_json(path: &str) -> Result<DbcJson, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read dbc json at {}: {}", path, e))?;
    serde_json::from_str(&content).map_err(|e| format!("failed to parse dbc json: {e}"))
}

pub fn schema_from_dbc(
    stream_name: &str,
    dbc: &DbcJson,
    pattern: Option<&str>,
) -> Result<Schema, String> {
    let compiled = CompiledDbcSchema::new(dbc.clone(), pattern.unwrap_or("{sig_name}"))?;
    Ok(compiled.schema(stream_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::ConcreteDatatype;
    use std::path::PathBuf;

    fn load_inline_dbc(file_name: &str, content: &str) -> DbcJson {
        let path = std::env::temp_dir().join(file_name);
        std::fs::write(&path, content).expect("write inline DBC");
        let result = load_can_schema(path.to_str().expect("inline DBC path"));
        std::fs::remove_file(path).ok();
        result.expect("load inline DBC")
    }

    #[test]
    fn parse_sim_json_produces_expected_columns() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = schema_from_dbc("sim_stream", &dbc, None).expect("compile DBC schema");

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
        let (schema, _, _) = result.unwrap();
        assert_eq!(schema.column_schemas().len(), 7);
    }

    #[test]
    fn schema_from_dbc_bus_without_name_uses_fallback() {
        let dbc = DbcJson {
            buses: vec![BusJson {
                name: None, // No name, should fallback to "Bus0"
                id: 0,
                messages: vec![MessageJson {
                    name: "TestMsg".to_string(),
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

        let schema = schema_from_dbc("test", &dbc, None).expect("compile DBC schema");
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
                    name: "TestMsg".to_string(),
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

        let schema = schema_from_dbc("test", &dbc, None).expect("compile DBC schema");
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
        let schema = schema_from_dbc("test", &dbc, None).expect("compile DBC schema");
        // Only ts column
        assert_eq!(schema.column_schemas().len(), 1);
    }

    #[test]
    fn load_can_schema_extended_message_id() {
        // Extended (29-bit) frames set bit 31 in the DBC `BO_` id; the parser
        // must yield the bare 29-bit id, not the raw value with the flag bit
        // (issue #202).
        const EXT_ID: u32 = 0x18FE_F100; // J1939-style 29-bit id
        let raw = 0x8000_0000u32 | EXT_ID; // DBC extended-frame encoding
        let dbc_content = format!(
            "VERSION \"\"\nNS_ :\nBS_:\nBU_:\nBO_ {raw} ExtMsg: 8 Vector__XXX\n \
             SG_ ExtSig : 0|8@1+ (1,0) [0|0] \"\" Vector__XXX\n"
        );
        let temp = std::env::temp_dir().join("vf202_extended.dbc");
        std::fs::write(&temp, dbc_content).unwrap();

        let dbc = load_can_schema(temp.to_str().unwrap()).expect("load extended dbc");
        std::fs::remove_file(&temp).ok();

        let msg = &dbc.buses[0].messages[0];
        assert_eq!(
            msg.id, EXT_ID,
            "extended BO_ id must parse to the bare 29-bit message id"
        );
        assert!(msg.id > 0x7FF, "id must exceed the 11-bit standard range");
    }

    #[test]
    fn load_can_schema_unflagged_wide_id_loads_and_is_preserved() {
        // An id above the 11-bit standard range but WITHOUT the extended flag
        // (bit 31) is loaded as-is when it fits 16 bits (no truncation here) and
        // only warns; it must not fail the load (issue #202).
        let dbc_content = "VERSION \"\"\nNS_ :\nBS_:\nBU_:\nBO_ 2048 StdOver: 8 Vector__XXX\n \
             SG_ S : 0|8@1+ (1,0) [0|0] \"\" Vector__XXX\n";
        let temp = std::env::temp_dir().join("vf202_unflagged_wide.dbc");
        std::fs::write(&temp, dbc_content).unwrap();

        let dbc = load_can_schema(temp.to_str().unwrap()).expect("load unflagged wide dbc");
        std::fs::remove_file(&temp).ok();

        // 2048 (0x800) fits 16 bits, so it is preserved; the warning fires because
        // it exceeds the 11-bit standard range.
        assert_eq!(dbc.buses[0].messages[0].id, 2048);
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
    fn load_can_schema_accepts_ns_with_or_without_space() {
        for (file_name, ns_header) in [
            ("vf179_ns_without_space.dbc", "NS_:"),
            ("vf179_ns_with_space.dbc", "NS_ :"),
        ] {
            let dbc_content = format!(
                "VERSION \"\"\n\n{ns_header}\n\nBS_:\n\nBU_:\n\n\
                 BO_ 512 TestMsg: 8 Vector__XXX\n \
                 SG_ TestSig : 0|8@1+ (1,0) [0|255] \"\" Vector__XXX\n"
            );
            let dbc = load_inline_dbc(file_name, &dbc_content);
            assert_eq!(dbc.buses[0].messages[0].name, "TestMsg");
            assert_eq!(dbc.buses[0].messages[0].signals[0].name, "TestSig");
        }
    }

    #[test]
    fn load_can_schema_accepts_multiline_signal_comment() {
        let dbc_content = r#"VERSION ""

NS_ :

BS_:

BU_:

BO_ 100 BeforeMsg: 8 Vector__XXX
 SG_ BeforeSig : 0|8@1+ (1,0) [0|255] "" Vector__XXX
CM_ SG_ 100 BeforeSig "first line
second line";
BO_ 200 AfterMsg: 8 Vector__XXX
 SG_ AfterSig : 8|8@1+ (1,0) [0|255] "" Vector__XXX
"#;

        let dbc = load_inline_dbc("vf179_multiline_comment.dbc", dbc_content);
        assert_eq!(dbc.buses[0].messages.len(), 2);
        assert_eq!(dbc.buses[0].messages[1].name, "AfterMsg");
        assert_eq!(dbc.buses[0].messages[1].signals[0].name, "AfterSig");
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
        let json_schema =
            schema_from_dbc("test", &json_result, None).expect("compile JSON DBC schema");

        // Load dbc directory with 1_PropulsionCAN.dbc
        let dbc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/dbc");
        let dbc_result = load_can_schema(dbc_path.to_str().unwrap()).unwrap();
        let dbc_schema = schema_from_dbc("test", &dbc_result, None).expect("compile DBC schema");

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
