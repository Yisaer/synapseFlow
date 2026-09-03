//! GBF data generator CLI tool.
//!
//! Generates GBF-encoded binary data from schema definitions.
//! Supports output to file (hex lines) or MQTT publishing.
//! When a DBC file is specified, generates valid CAN signal payloads.

use clap::Parser;
use rand::SeedableRng;
use rumqttc::{Client, MqttOptions, QoS};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;
use veloflux_sdv::encoder::can::CanEncoder;
use veloflux_sdv::encoder::gbf::{FieldValue, FieldValues, GbfEncoder};
use veloflux_sdv::schema::dbc::load_can_schema;
use veloflux_sdv::schema::gbf::GbfSchema;

/// GBF data generator - Generate GBF encoded data from schema
#[derive(Parser, Debug)]
#[command(name = "gbf_gen")]
#[command(about = "Generate GBF encoded binary data based on schema definitions")]
#[command(version)]
struct Args {
    /// GBF schema JSON file path (required)
    #[arg(short, long)]
    schema: String,

    /// Output destination: "file:<path>" for hex file, "mqtt:<topic>" for MQTT
    #[arg(value_name = "OUTPUT")]
    output: String,

    /// DBC schema file or directory for CAN signal encoding
    /// When specified, generates valid CAN signal payloads instead of random bytes
    #[arg(long)]
    dbc: Option<String>,

    /// Number of packets to generate
    #[arg(short, long, default_value = "1")]
    count: usize,

    /// Interval between packets in milliseconds (for MQTT output)
    #[arg(short, long, default_value = "1000")]
    interval: u64,

    /// MQTT broker URL (host:port)
    #[arg(long, default_value = "127.0.0.1:1883")]
    broker: String,

    /// Generate random payload data
    #[arg(short, long)]
    random: bool,

    /// JSON object with field values to use
    #[arg(short, long)]
    data: Option<String>,

    /// Number of items in sequences (frames per packet)
    #[arg(long, default_value = "2")]
    items: usize,

    /// Timestamp value or "now" for current time
    #[arg(long)]
    timestamp: Option<String>,

    /// CAN IDs to use (comma-separated hex, e.g., "0x586,0x24A,0x80000103")
    #[arg(long)]
    can_ids: Option<String>,

    /// Signal values as JSON (e.g., '{"Mess0_Sig1":1,"Mess0_Sig2":2}')
    /// Specified signals use these values; unspecified signals are random
    #[arg(long)]
    signals: Option<String>,

    /// Random seed for reproducible output
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Debug, PartialEq)]
enum OutputMode {
    File(String),
    Mqtt(String),
}

impl OutputMode {
    fn parse(s: &str) -> Result<Self, String> {
        if let Some(path) = s.strip_prefix("file:") {
            Ok(OutputMode::File(path.to_string()))
        } else if let Some(topic) = s.strip_prefix("mqtt:") {
            Ok(OutputMode::Mqtt(topic.to_string()))
        } else {
            Err(format!(
                "Invalid output format: '{}'. Use 'file:<path>' or 'mqtt:<topic>'",
                s
            ))
        }
    }
}

fn parse_can_ids(ids_str: &Option<String>, can_encoder: Option<&CanEncoder>) -> Option<Vec<u32>> {
    if let Some(s) = ids_str {
        Some(
            s.split(',')
                .filter_map(|s| {
                    let s = s.trim();
                    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                        u32::from_str_radix(hex, 16).ok()
                    } else {
                        s.parse().ok()
                    }
                })
                .collect(),
        )
    } else if let Some(can_enc) = can_encoder {
        let ids: Vec<u32> = can_enc.message_ids();
        if ids.is_empty() { None } else { Some(ids) }
    } else {
        None
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load GBF schema
    let schema = GbfSchema::load(&args.schema)?;
    let encoder = GbfEncoder::new(schema);

    // Load DBC schema if specified
    let can_encoder = if let Some(ref dbc_path) = args.dbc {
        let dbc = load_can_schema(dbc_path)?;
        Some(CanEncoder::new(&dbc))
    } else {
        None
    };

    // Parse output mode
    let output_mode = OutputMode::parse(&args.output)?;

    // Create RNG with optional seed for reproducibility
    let mut rng: rand::rngs::StdRng = match args.seed {
        Some(seed) => rand::rngs::StdRng::seed_from_u64(seed),
        None => rand::rngs::StdRng::from_entropy(),
    };

    // Parse CAN IDs if provided, or get from DBC
    let can_ids = parse_can_ids(&args.can_ids, can_encoder.as_ref());

    // Generate and output packets
    match output_mode {
        OutputMode::File(path) => {
            let file = File::create(&path)?;
            let mut writer = BufWriter::new(file);

            for i in 0..args.count {
                let values = generate_values(
                    &encoder,
                    can_encoder.as_ref(),
                    &args,
                    &mut rng,
                    i,
                    can_ids.as_deref(),
                )?;
                let hex = encoder.encode_hex(&values)?;
                writeln!(writer, "{}", hex)?;
            }

            writer.flush()?;
            println!("Generated {} packets to {}", args.count, path);
            if can_encoder.is_some() {
                println!("  (using DBC schema for signal encoding)");
            }
        }
        OutputMode::Mqtt(topic) => {
            // Parse broker address
            let (host, port) = parse_broker(&args.broker)?;

            let mut mqtt_opts = MqttOptions::new("gbf_gen", host, port);
            mqtt_opts.set_keep_alive(Duration::from_secs(30));

            let (client, mut connection) = Client::new(mqtt_opts, 10);

            // Spawn connection handler
            std::thread::spawn(move || for _ in connection.iter() {});

            // Give time to connect
            std::thread::sleep(Duration::from_millis(100));

            for i in 0..args.count {
                let values = generate_values(
                    &encoder,
                    can_encoder.as_ref(),
                    &args,
                    &mut rng,
                    i,
                    can_ids.as_deref(),
                )?;
                let binary = encoder.encode(&values)?;

                client.publish(&topic, QoS::AtLeastOnce, false, binary)?;

                if i < args.count - 1 && args.interval > 0 {
                    std::thread::sleep(Duration::from_millis(args.interval));
                }
            }

            println!("Published {} packets to {}", args.count, topic);
        }
    }

    Ok(())
}

fn parse_broker(broker: &str) -> Result<(String, u16), Box<dyn std::error::Error>> {
    if let Some((host, port_str)) = broker.rsplit_once(':') {
        let port = port_str.parse()?;
        Ok((host.to_string(), port))
    } else {
        Ok((broker.to_string(), 1883))
    }
}

fn generate_values<R: rand::Rng>(
    encoder: &GbfEncoder,
    can_encoder: Option<&CanEncoder>,
    args: &Args,
    rng: &mut R,
    packet_index: usize,
    can_ids: Option<&[u32]>,
) -> Result<FieldValues, Box<dyn std::error::Error>> {
    let mut values = if args.random {
        encoder.generate_random(rng, args.items)
    } else if let Some(ref json_str) = args.data {
        parse_json_values(json_str)?
    } else {
        // Generate default values with random frames
        encoder.generate_random(rng, args.items)
    };

    // Override timestamp if specified
    if let Some(ref ts_str) = args.timestamp {
        let ts = if ts_str == "now" {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64
        } else {
            ts_str.parse()?
        };
        values.insert("ts".to_string(), FieldValue::Int(ts));
    } else if !values.contains_key("ts") {
        // Use current time if not specified
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        values.insert("ts".to_string(), FieldValue::Int(ts));
    }

    // Parse user-specified signal values
    let user_signals: HashMap<String, i64> = if let Some(ref sig_json) = args.signals {
        parse_signal_values(sig_json)?
    } else {
        HashMap::new()
    };

    // Override CAN IDs and generate proper payloads if DBC is provided
    if let Some(FieldValue::Sequence(frames)) = values.get_mut("frames") {
        for (i, frame) in frames.iter_mut().enumerate() {
            // Set CAN ID
            if let Some(ids) = can_ids {
                let can_id = ids[i % ids.len()];
                frame.insert("can_id".to_string(), FieldValue::Int(u64::from(can_id)));

                // Generate proper payload using DBC if available
                if let Some(can_enc) = can_encoder
                    && can_enc.has_message(can_id)
                {
                    // Start with random values, then override with user-specified ones
                    let mut signal_values = can_enc.generate_random(can_id, rng)?;
                    for (sig_name, sig_val) in &user_signals {
                        signal_values.insert(sig_name.clone(), *sig_val);
                    }
                    let payload = can_enc.encode(can_id, &signal_values)?;
                    frame.insert("payload".to_string(), FieldValue::Bytes(payload));
                }
            }
        }
    }

    // Add packet index for debugging/tracking
    let _ = packet_index; // Currently unused, could be added as metadata

    Ok(values)
}

/// Parse signal values from JSON string.
fn parse_signal_values(json_str: &str) -> Result<HashMap<String, i64>, Box<dyn std::error::Error>> {
    let json: serde_json::Value = serde_json::from_str(json_str)?;
    let mut values = HashMap::new();

    if let serde_json::Value::Object(map) = json {
        for (key, val) in map {
            if let Some(n) = val.as_i64() {
                values.insert(key, n);
            } else if let Some(n) = val.as_u64() {
                values.insert(key, n as i64);
            }
        }
    }

    Ok(values)
}

fn parse_json_values(json_str: &str) -> Result<FieldValues, Box<dyn std::error::Error>> {
    let json: serde_json::Value = serde_json::from_str(json_str)?;
    let mut values = FieldValues::new();

    if let serde_json::Value::Object(map) = json {
        for (key, val) in map {
            let field_val = json_to_field_value(val)?;
            values.insert(key, field_val);
        }
    }

    Ok(values)
}

fn json_to_field_value(val: serde_json::Value) -> Result<FieldValue, Box<dyn std::error::Error>> {
    match val {
        serde_json::Value::Number(n) => {
            let v = n.as_u64().unwrap_or(n.as_i64().unwrap_or(0) as u64);
            Ok(FieldValue::Int(v))
        }
        serde_json::Value::Array(arr) => {
            // Check if it's a bytes array (all numbers 0-255) or sequence
            if arr.iter().all(|v| v.as_u64().is_some_and(|n| n <= 255)) {
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect();
                Ok(FieldValue::Bytes(bytes))
            } else {
                // Assume it's a sequence of objects
                let items: Result<Vec<FieldValues>, Box<dyn std::error::Error>> = arr
                    .into_iter()
                    .map(|item| {
                        if let serde_json::Value::Object(map) = item {
                            let mut fv = FieldValues::new();
                            for (k, v) in map {
                                fv.insert(k, json_to_field_value(v)?);
                            }
                            Ok(fv)
                        } else {
                            Err("Sequence items must be objects".to_string().into())
                        }
                    })
                    .collect();
                Ok(FieldValue::Sequence(items?))
            }
        }
        serde_json::Value::String(s) => {
            // Try to parse as hex bytes
            if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                let bytes = hex::decode(hex)?;
                Ok(FieldValue::Bytes(bytes))
            } else {
                Err(format!("Unsupported string value: {}", s).into())
            }
        }
        _ => Err("Unsupported JSON value type".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_signal_values() {
        let json = r#"{"Signal1": 123, "Signal2": -456, "Signal3": 0}"#;
        let values = parse_signal_values(json).expect("parse");

        assert_eq!(values.get("Signal1"), Some(&123));
        assert_eq!(values.get("Signal2"), Some(&-456));
        assert_eq!(values.get("Signal3"), Some(&0));
    }

    #[test]
    fn test_parse_signal_values_invalid_types() {
        let json = r#"{"Signal1": 123, "Invalid": "string", "Null": null}"#;
        let values = parse_signal_values(json).expect("parse");

        assert_eq!(values.get("Signal1"), Some(&123));
        assert!(!values.contains_key("Invalid"));
        assert!(!values.contains_key("Null"));
    }

    #[test]
    fn test_parse_json_values_simple() {
        let json = r#"{"Field1": 42, "Field2": [1, 2, 3], "Field3": "0xDEADBEEF"}"#;
        let values = parse_json_values(json).expect("parse");

        match values.get("Field1") {
            Some(FieldValue::Int(v)) => assert_eq!(*v, 42),
            _ => panic!("Field1 mismatch"),
        }

        match values.get("Field2") {
            Some(FieldValue::Bytes(b)) => assert_eq!(b, &[1, 2, 3]),
            _ => panic!("Field2 mismatch"),
        }

        match values.get("Field3") {
            Some(FieldValue::Bytes(b)) => assert_eq!(b, &[0xDE, 0xAD, 0xBE, 0xEF]),
            _ => panic!("Field3 mismatch"),
        }
    }

    #[test]
    fn test_parse_nested_sequence() {
        let json = r#"{"frames": [{"id": 1}, {"id": 2}]}"#;
        let values = parse_json_values(json).expect("parse");

        match values.get("frames") {
            Some(FieldValue::Sequence(seq)) => {
                assert_eq!(seq.len(), 2);
                match seq[0].get("id") {
                    Some(FieldValue::Int(v)) => assert_eq!(*v, 1),
                    _ => panic!("Item 1 mismatch"),
                }
            }
            _ => panic!("Frames mismatch"),
        }
    }

    #[test]
    fn test_output_mode_parse() {
        assert_eq!(
            OutputMode::parse("file:foo.hex"),
            Ok(OutputMode::File("foo.hex".to_string()))
        );
        assert_eq!(
            OutputMode::parse("mqtt:topic"),
            Ok(OutputMode::Mqtt("topic".to_string()))
        );
        assert!(OutputMode::parse("invalid").is_err());
    }

    #[test]
    fn test_parse_can_ids() {
        // From string
        let ids = parse_can_ids(&Some("0x123, 456, 0X1A".to_string()), None);
        assert_eq!(ids, Some(vec![0x123, 456, 0x1A]));

        // Packed extended DBC u32 key (bit 31 = IDE)
        let ids = parse_can_ids(&Some("0x80000103".to_string()), None);
        assert_eq!(ids, Some(vec![0x8000_0103]));

        // Invalid parts skipped
        let ids = parse_can_ids(&Some("0x123, invalid, 10".to_string()), None);
        assert_eq!(ids, Some(vec![0x123, 10]));

        // None input
        let ids = parse_can_ids(&None, None);
        assert_eq!(ids, None);
    }

    #[test]
    fn parse_can_ids_from_dbc_keeps_packed_extended_id() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/tests/std_ext_same_id.dbc");
        let dbc = load_can_schema(path.to_str().unwrap()).expect("load DBC");
        let encoder = CanEncoder::new(&dbc);
        let ids = parse_can_ids(&None, Some(&encoder)).expect("ids from DBC");
        assert!(ids.contains(&0x123), "standard 0x123: {ids:?}");
        assert!(
            ids.contains(&0x8000_0123),
            "packed extended 0x80000123: {ids:?}"
        );
    }
}
