//! CAN decoder module providing reusable CAN frame signal decoding.
//!
//! This module contains the core CAN signal decoding logic that can be reused
//! by various protocol decoders (SPI, raw CAN, etc.) that work with CAN frames.

use std::collections::HashMap;
use std::sync::Arc;

use datatypes::{Schema, Value};
use flow::{
    codec::CodecError,
    model::{Message, Tuple},
};
use tracing::trace;

use crate::schema::dbc::{DbcJson, format_signal_name};

/// A parsed CAN frame with timestamp, ID, and payload.
#[derive(Clone, Debug)]
pub struct CanFrame<'a> {
    pub timestamp: u64,
    pub can_id: u16,
    pub payload: &'a [u8],
}

/// Specification for decoding a single CAN signal.
pub struct SignalSpec {
    pub col_index: usize,
    pub start: u32,
    pub length: u32,
    pub is_big_endian: bool,
    pub is_signed: bool,
    pub factor: f64,
    pub offset: f64,
    /// Pre-computed precision (decimal places) for rounding.
    /// Calculated once at initialization to avoid expensive string formatting during decode.
    pub precision: u32,
    /// True if factor and offset are both integers, allowing integer math.
    pub use_integer_math: bool,
    /// Pre-computed integer factor (only valid if use_integer_math is true)
    pub factor_int: i64,
    /// Pre-computed integer offset (only valid if use_integer_math is true)
    pub offset_int: i64,
    /// True if this signal is the multiplexer selector
    pub is_multiplexer: bool,
    /// True if this signal is multiplexed (only decoded when multiplexer matches)
    pub is_multiplexed: bool,
    /// The multiplexer value that activates this signal (only valid if is_multiplexed is true)
    pub multiplexer_value: Option<i64>,
    /// Lower bound for clamping the physical value to the DBC range.
    /// `None` disables clamping (range unspecified, clamping turned off, or a
    /// multiplexer selector signal which must never be clamped).
    pub clamp_min: Option<f64>,
    /// Upper bound for clamping the physical value to the DBC range.
    pub clamp_max: Option<f64>,
}

/// Specification for a CAN message containing multiple signals.
pub struct MessageSpec {
    pub signals: Vec<SignalSpec>,
    /// True if any signal in this message is a multiplexer.
    /// Pre-computed at init time to skip multiplexer search for non-multiplex messages.
    pub has_multiplexer: bool,
}

/// CAN decoder that converts CAN frames into Tuple values based on DBC schema.
///
/// This decoder can be composed into protocol-specific decoders (SPI, raw CAN, etc.)
/// to provide reusable CAN signal decoding functionality.
///
/// Notes:
/// - Tuple column order strictly follows schema order.
/// - Column names are zero-copy: reusing `Arc<str>` from schema.
/// - Values are created only once: written to corresponding column position during decoding.
pub struct CanDecoder {
    source_name: Arc<str>,
    keys: Arc<[Arc<str>]>,
    messages: HashMap<u16, MessageSpec>,
    ts_index: Option<usize>,
    /// Cached Null value to avoid allocating new Arc on every decode
    null_value: Arc<Value>,
}

impl CanDecoder {
    /// Create a new CAN decoder from a DBC schema.
    ///
    /// # Arguments
    /// * `source_name` - Name of the source stream
    /// * `schema` - Schema defining the output columns
    /// * `dbc` - DBC JSON definition with bus, message, and signal definitions
    ///
    /// # Returns
    /// A configured CanDecoder or an error if configuration fails.
    pub fn new(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        dbc: DbcJson,
        pattern: Option<String>,
        clamp_to_range: bool,
    ) -> Result<Self, CodecError> {
        let source_name: String = source_name.into();
        // Default to simple signal name if no pattern provided
        let pattern_str = pattern.as_deref().unwrap_or("{sig}");
        let keys: Vec<Arc<str>> = schema
            .column_schemas()
            .iter()
            .map(|col| Arc::<str>::from(col.name.as_str()))
            .collect();
        let mut name_to_index = HashMap::with_capacity(keys.len());
        for (idx, key) in keys.iter().enumerate() {
            name_to_index.insert(key.clone(), idx);
        }
        let ts_index = name_to_index.get("ts").copied();

        let mut messages = HashMap::new();
        for bus in dbc.buses {
            let bus_name = bus.name.unwrap_or_else(|| format!("Bus{}", bus.id));
            for msg in bus.messages {
                // Custom CAN ID mapping: (bus_id << 12) | message_id
                let can_id = ((bus.id as u16) << 12) | (msg.id as u16);
                let frame_id = msg.frame_id.clone();
                let mut signals = Vec::new();
                for sig in msg.signals {
                    let full_name = format_signal_name(
                        pattern_str,
                        &bus_name,
                        &frame_id,
                        &msg._name,
                        &sig.name,
                    );
                    let Some(&col_index) = name_to_index.get(full_name.as_str()) else {
                        continue;
                    };
                    let factor = sig.scale.unwrap_or(1.0);
                    let offset = sig.offset.unwrap_or(0.0);
                    // Pre-compute precision to avoid expensive string formatting during decode
                    let precision = calculate_precision(factor, offset);
                    // Check if we can use integer math (both factor and offset are whole numbers)
                    let use_integer_math = is_integer_value(factor) && is_integer_value(offset);
                    let factor_int = factor as i64;
                    let offset_int = offset as i64;
                    // Clamp bounds: only when enabled, the DBC range is valid
                    // (max > min; min == max means "no range"), and the signal is
                    // not the multiplexer selector (clamping it could corrupt
                    // multiplex matching).
                    let (clamp_min, clamp_max) = match (sig.min, sig.max) {
                        (Some(mn), Some(mx))
                            if clamp_to_range && !sig.is_multiplexer && mx > mn =>
                        {
                            (Some(mn), Some(mx))
                        }
                        _ => (None, None),
                    };
                    signals.push(SignalSpec {
                        col_index,
                        start: sig.start,
                        length: sig.length,
                        is_big_endian: sig.is_big_endian,
                        is_signed: sig.is_signed,
                        factor,
                        offset,
                        precision,
                        use_integer_math,
                        factor_int,
                        offset_int,
                        is_multiplexer: sig.is_multiplexer,
                        is_multiplexed: sig.is_multiplexed,
                        multiplexer_value: sig.multiplexer_value,
                        clamp_min,
                        clamp_max,
                    });
                }
                // Warn if duplicate CAN ID found (same bus + frame ID)
                if messages.contains_key(&can_id) {
                    tracing::warn!(
                        can_id = format!("0x{:04X}", can_id),
                        bus = %bus_name,
                        frame_id = %frame_id,
                        "Duplicate CAN ID detected, previous signals will be overwritten"
                    );
                }
                // Pre-compute if this message has a multiplexer signal
                let has_multiplexer = signals.iter().any(|s| s.is_multiplexer);
                messages.insert(
                    can_id,
                    MessageSpec {
                        signals,
                        has_multiplexer,
                    },
                );
            }
        }

        Ok(Self {
            source_name: Arc::<str>::from(source_name),
            keys: Arc::from(keys),
            messages,
            ts_index,
            null_value: Arc::new(Value::Null),
        })
    }

    /// Decode a list of CAN frames into a Tuple.
    ///
    /// # Arguments
    /// * `frames` - Vector of CAN frames to decode
    /// * `projection` - Optional projection to select specific columns. If Some, only columns
    ///   present in the projection (and necessary multiplexers) will be decoded.
    ///
    /// # Returns
    /// A Tuple containing decoded signal values, or None if frames is empty.
    pub fn decode_frames(
        &self,
        frames: Vec<CanFrame<'_>>,
        projection: Option<&flow::planner::decode_projection::DecodeProjection>,
    ) -> Option<Tuple> {
        if frames.is_empty() {
            return None;
        }

        // Optimization: Pre-calculate a mask of required columns to avoid hash lookups in the hot loop
        let required_mask = projection.map(|proj| {
            self.keys
                .iter()
                .map(|key| proj.column(key).is_some())
                .collect::<Vec<bool>>()
        });

        let mut values: Vec<Option<Value>> = vec![None; self.keys.len()];

        if let Some(ts_idx) = self.ts_index {
            values[ts_idx] = Some(Value::Int64(frames[0].timestamp as i64));
        }

        for frame in frames {
            if let Some(spec) = self.messages.get(&frame.can_id) {
                // Step 1: Find and decode the multiplexer signal to get current mux value
                // (only if this message has any multiplexer signals)
                let mut multiplexer_value: Option<i64> = None;
                if spec.has_multiplexer {
                    for signal in &spec.signals {
                        if signal.is_multiplexer {
                            let val = decode_signal(frame.payload, signal);
                            if let Value::Int64(v) = val {
                                multiplexer_value = Some(v);
                            }
                            // Store the multiplexer value if it's required by projection
                            if required_mask
                                .as_ref()
                                .is_none_or(|mask| mask[signal.col_index])
                            {
                                values[signal.col_index] = Some(val);
                            }
                            break;
                        }
                    }
                }

                // Step 2: Decode all signals, but skip multiplexed signals that don't match
                for signal in &spec.signals {
                    // Skip multiplexer signal (already decoded above)
                    if signal.is_multiplexer {
                        continue;
                    }

                    // Check projection mask: Skip if not required
                    if let Some(mask) = &required_mask
                        && !mask[signal.col_index]
                    {
                        continue;
                    }

                    // Skip multiplexed signals that don't match the current multiplexer value
                    if signal.is_multiplexed
                        && signal
                            .multiplexer_value
                            .is_some_and(|v| multiplexer_value != Some(v))
                    {
                        // Don't decode this signal - leave as Null
                        continue;
                    }

                    let v = decode_signal(frame.payload, signal);
                    values[signal.col_index] = Some(v);
                }
            } else {
                trace!(can_id = frame.can_id, "frame spec not found for can_id");
            }
        }

        // Construct final values in column order, fill unwritten with cached Null
        let null_arc = &self.null_value;
        let finalized: Vec<Arc<Value>> = values
            .into_iter()
            .map(|opt| opt.map(Arc::new).unwrap_or_else(|| Arc::clone(null_arc)))
            .collect();

        let message = Arc::new(Message::new_shared_keys(
            Arc::clone(&self.source_name),
            Arc::clone(&self.keys),
            finalized,
        ));
        Some(Tuple::new(vec![message]))
    }

    /// Get the message specifications map.
    #[allow(dead_code)]
    pub fn messages(&self) -> &HashMap<u16, MessageSpec> {
        &self.messages
    }

    /// Get the column keys.
    #[allow(dead_code)]
    pub fn keys(&self) -> &Arc<[Arc<str>]> {
        &self.keys
    }
}

/// Decode a single signal from CAN frame payload.
pub fn decode_signal(payload: &[u8], spec: &SignalSpec) -> Value {
    let raw = extract_bits(
        payload,
        spec.start as usize,
        spec.length as usize,
        spec.is_big_endian,
    );
    let val = if spec.is_signed {
        sign_extend(raw, spec.length as usize)
    } else {
        raw as i64
    };

    // Apply scaling: physical_value = raw_value * factor + offset
    let value = if spec.factor == 1.0 && spec.offset == 0.0 {
        // No scaling needed
        Value::Int64(val)
    } else if spec.use_integer_math {
        // Integer scaling - use pure integer arithmetic (faster, no precision loss)
        // Use checked arithmetic to handle potential overflow
        match val
            .checked_mul(spec.factor_int)
            .and_then(|v| v.checked_add(spec.offset_int))
        {
            Some(result) => Value::Int64(result),
            None => {
                // Overflow - fall back to float
                let physical_value = val as f64 * spec.factor + spec.offset;
                Value::Float64(round_to_precision(physical_value, spec.precision))
            }
        }
    } else {
        // Fractional scaling - use floating point with precision rounding
        let physical_value = val as f64 * spec.factor + spec.offset;
        let rounded_value = round_to_precision(physical_value, spec.precision);
        Value::Float64(rounded_value)
    };

    clamp_to_dbc_range(value, spec.clamp_min, spec.clamp_max)
}

/// Clamp a decoded physical value to the DBC `[min, max]` range, matching
/// eKuiper's `can_dbc` behaviour. A no-op when both bounds are `None`. The value
/// type (Int64 / Float64) is preserved.
#[inline]
fn clamp_to_dbc_range(value: Value, min: Option<f64>, max: Option<f64>) -> Value {
    if min.is_none() && max.is_none() {
        return value;
    }
    let clamp = |x: f64| {
        let mut x = x;
        if let Some(mn) = min
            && x < mn
        {
            x = mn;
        }
        if let Some(mx) = max
            && x > mx
        {
            x = mx;
        }
        x
    };
    match value {
        // Clamp integers in integer space: routing the value through f64 would
        // lose precision above 2^53 (e.g. 64-bit signals). Bounds may be
        // fractional (an integer signal can still have a non-integer DBC
        // min/max), so use ceil(min) / floor(max) — never plain truncation — so
        // the clamped integer always stays within [min, max]. `f64 as i64`
        // saturates, so a u64-range upper bound becomes i64::MAX (effectively no
        // upper clamp, correct since the true bound exceeds i64 — see #65).
        Value::Int64(v) => {
            let mut out = v;
            if let Some(mn) = min {
                let lo = mn.ceil() as i64;
                if out < lo {
                    out = lo;
                }
            }
            if let Some(mx) = max {
                let hi = mx.floor() as i64;
                if out > hi {
                    out = hi;
                }
            }
            Value::Int64(out)
        }
        Value::Float64(v) => Value::Float64(clamp(v)),
        other => other,
    }
}

/// Calculate the number of decimal places needed based on factor and offset.
/// The precision is derived from the smallest significant decimal place in either value.
/// For example, factor = 2.77778E-7 should give 9 decimal places (7 from exponent + 2 from significand).
pub fn calculate_precision(factor: f64, offset: f64) -> u32 {
    let factor_precision = if factor != 0.0 && factor.abs() < 1.0 {
        // For small numbers, count: exponent digits + significant figures after first digit
        // E.g., 2.77778E-7 = 0.000000277778 needs ~12 decimal places for full precision
        // But practically, we use: -log10(factor) + significant_figures_in_mantissa
        let log_precision = (-factor.abs().log10()).ceil() as u32;

        // Count additional precision from mantissa (significant figures after the leading digit)
        // Format to scientific notation and count digits in mantissa
        let s = format!("{:e}", factor.abs());
        let mantissa_digits = if let Some(e_pos) = s.find('e') {
            let mantissa = &s[..e_pos];
            // Count digits after decimal point in mantissa (e.g., "2.77778" -> 5)
            if let Some(dot_pos) = mantissa.find('.') {
                (mantissa.len() - dot_pos - 1) as u32
            } else {
                0
            }
        } else {
            0
        };

        // Total precision: log10 precision (position of first significant digit) + mantissa digits
        // No overlap adjustment needed - log_precision gives position of first digit,
        // mantissa_digits gives additional digits after that
        log_precision.saturating_add(mantissa_digits)
    } else {
        decimal_places(factor)
    };

    let offset_precision = decimal_places(offset);

    // Use the maximum precision needed, cap at 11 decimal places
    factor_precision.max(offset_precision).min(11)
}

/// Check if a float value represents an integer (no fractional part).
/// Also ensures the value fits within i64 range.
pub fn is_integer_value(value: f64) -> bool {
    value.fract() == 0.0 && value.abs() < i64::MAX as f64
}

/// Count the number of decimal places in a float value.
pub fn decimal_places(value: f64) -> u32 {
    if value == 0.0 || value.fract() == 0.0 {
        return 0;
    }

    // Convert to string and count decimal places
    let s = format!("{}", value);
    if let Some(dot_pos) = s.find('.') {
        let decimal_part = &s[dot_pos + 1..];
        // Trim trailing zeros
        decimal_part.trim_end_matches('0').len() as u32
    } else {
        0
    }
}

/// Round a value to the specified number of decimal places.
pub fn round_to_precision(value: f64, decimal_places: u32) -> f64 {
    let multiplier = 10f64.powi(decimal_places as i32);
    (value * multiplier).round() / multiplier
}

/// Extract bits from a CAN payload.
///
/// # Arguments
/// * `payload` - The raw CAN frame payload bytes
/// * `start_bit` - Starting bit position
/// * `bit_length` - Number of bits to extract
/// * `big_endian` - True for Motorola (big-endian), false for Intel (little-endian)
///
/// # Returns
/// The extracted bits as a u64 value.
pub fn extract_bits(payload: &[u8], start_bit: usize, bit_length: usize, big_endian: bool) -> u64 {
    match big_endian {
        false => {
            // little-endian: extract bits starting from start_bit, incrementing
            let mut value = 0u64;
            for i in 0..bit_length {
                let bit_pos = start_bit + i;
                let byte_idx = bit_pos / 8;
                let bit_idx = bit_pos % 8;
                if byte_idx < payload.len() {
                    let bit = (payload[byte_idx] >> bit_idx) & 1;
                    value |= (bit as u64) << i;
                }
            }
            value
        }
        true => {
            // big-endian: traverse from start_bit toward lower bits
            let mut value = 0u64;
            let mut byte_idx = start_bit / 8;
            let mut bit_idx = (start_bit % 8) as i8;
            for _ in 0..bit_length {
                if byte_idx < payload.len() {
                    let bit = (payload[byte_idx] >> bit_idx) & 1;
                    value = (value << 1) | bit as u64;
                }
                bit_idx -= 1;
                if bit_idx < 0 {
                    bit_idx = 7;
                    byte_idx += 1;
                }
            }
            value
        }
    }
}

/// Sign-extend a raw value based on its bit length.
///
/// # Arguments
/// * `raw` - The raw unsigned value
/// * `bit_length` - The original bit length of the value
///
/// # Returns
/// The sign-extended value as i64.
pub fn sign_extend(raw: u64, bit_length: usize) -> i64 {
    if bit_length == 0 {
        return 0;
    }
    if bit_length < 64 && (raw & (1 << (bit_length - 1))) != 0 {
        let mask = !0u64 << bit_length;
        (raw | mask) as i64
    } else {
        raw as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::dbc::{load_dbc_json, schema_from_dbc};

    #[test]
    fn clamp_preserves_type_and_bounds() {
        // no bounds -> untouched
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(999), None, None),
            Value::Int64(999)
        );
        // integer clamp to max / min, type preserved
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(215), Some(-40.0), Some(210.0)),
            Value::Int64(210)
        );
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(-1), Some(0.0), Some(100.0)),
            Value::Int64(0)
        );
        // float clamp, type preserved
        assert_eq!(
            clamp_to_dbc_range(Value::Float64(5.11), Some(0.0), Some(5.0)),
            Value::Float64(5.0)
        );
    }

    #[test]
    fn clamp_int64_with_fractional_bounds_uses_ceil_floor() {
        // Integer value, fractional bounds: result must stay within [min, max].
        // max = 5.5 -> floor -> 5 ; min = 2.5 -> ceil -> 3
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(6), Some(2.5), Some(5.5)),
            Value::Int64(5)
        );
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(2), Some(2.5), Some(5.5)),
            Value::Int64(3)
        );
        // negative max = -2.5 -> floor -> -3 (truncation would wrongly give -2)
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(-2), None, Some(-2.5)),
            Value::Int64(-3)
        );
        // in-range value untouched
        assert_eq!(
            clamp_to_dbc_range(Value::Int64(4), Some(2.5), Some(5.5)),
            Value::Int64(4)
        );
    }

    #[test]
    fn clamp_int64_above_2pow53_is_exact() {
        // Large in-range integer must pass through unchanged (no f64 round-trip
        // precision loss). 9_007_199_254_740_993 = 2^53 + 1 is not representable
        // exactly as f64.
        let v = 9_007_199_254_740_993_i64;
        let out = clamp_to_dbc_range(Value::Int64(v), Some(0.0), Some(i64::MAX as f64));
        assert_eq!(out, Value::Int64(v));
    }

    #[test]
    fn can_decoder_selects_clamp_bounds() {
        use crate::schema::dbc::{BusJson, DbcJson, MessageJson, SignalJson};
        fn sig(name: &str, mux: bool, min: Option<f64>, max: Option<f64>) -> SignalJson {
            SignalJson {
                name: name.to_string(),
                start: 0,
                length: 8,
                scale: Some(1.0),
                offset: Some(0.0),
                is_big_endian: false,
                is_signed: false,
                is_multiplexer: mux,
                is_multiplexed: false,
                multiplexer_value: None,
                min,
                max,
            }
        }
        let dbc = DbcJson {
            buses: vec![BusJson {
                name: Some("B".into()),
                id: 0,
                messages: vec![MessageJson {
                    _name: "M".into(),
                    id: 1,
                    frame_id: "0x1".into(),
                    _length: 8,
                    signals: vec![
                        sig("Normal", false, Some(-40.0), Some(210.0)),
                        sig("Mux", true, Some(0.0), Some(7.0)),
                        sig("NoRange", false, Some(0.0), Some(0.0)),
                    ],
                }],
            }],
        };
        let schema = Arc::new(schema_from_dbc("can", &dbc, None));
        let find = |dec: &CanDecoder, name: &str| -> (Option<f64>, Option<f64>) {
            let specs = &dec.messages()[&1u16].signals; // can_id = (bus 0 << 12) | id 1
            let s = specs
                .iter()
                .find(|s| dec.keys()[s.col_index].as_ref() == name)
                .expect("signal in specs");
            (s.clamp_min, s.clamp_max)
        };

        // clamp enabled (default)
        let dec = CanDecoder::new("can", schema.clone(), dbc.clone(), None, true).unwrap();
        assert_eq!(find(&dec, "Normal"), (Some(-40.0), Some(210.0))); // valid range -> set
        assert_eq!(find(&dec, "Mux"), (None, None)); // multiplexer selector -> never clamped
        assert_eq!(find(&dec, "NoRange"), (None, None)); // min == max -> no range

        // clamp disabled -> nothing clamps
        let dec = CanDecoder::new("can", schema, dbc, None, false).unwrap();
        assert_eq!(find(&dec, "Normal"), (None, None));
    }

    fn get_test_decoder() -> CanDecoder {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/sim.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load sim.json");
        let schema = Arc::new(schema_from_dbc("can", &dbc, None));
        CanDecoder::new("can", schema.clone(), dbc.clone(), None, true).expect("build decoder")
    }

    #[test]
    fn test_can_decoder_with_dbc() {
        let decoder = get_test_decoder();

        // Verify messages were loaded
        assert!(!decoder.messages().is_empty());

        // Verify keys were loaded
        assert!(!decoder.keys().is_empty());
    }

    #[test]
    fn test_decode_frames_empty() {
        let decoder = get_test_decoder();
        let result = decoder.decode_frames(vec![], None);
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_frames_with_valid_frame() {
        let decoder = get_test_decoder();

        // Frame with CAN ID 0x1586 (PropulsionCAN, frameId 0x586)
        let payload = [0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11, 0x55];
        let frames = vec![CanFrame {
            timestamp: 1720765705290,
            can_id: 0x1586,
            payload: &payload,
        }];

        let result = decoder.decode_frames(frames, None);
        assert!(result.is_some());

        let tuple = result.unwrap();
        let ts = tuple.value_by_name("can", "ts").expect("ts not found");
        assert_eq!(*ts, Value::Int64(1720765705290));
    }

    #[test]
    fn test_extract_bits_little_endian() {
        // Test little-endian bit extraction
        let payload = [0b11110000, 0b00001111];
        // Extract 4 bits starting at bit 4 (little endian)
        let value = extract_bits(&payload, 4, 4, false);
        assert_eq!(value, 0b1111);

        // Extract 8 bits starting at bit 0
        let value = extract_bits(&payload, 0, 8, false);
        assert_eq!(value, 0b11110000);
    }

    #[test]
    fn test_extract_bits_big_endian() {
        // Test big-endian bit extraction
        let payload = [0b10101010, 0b01010101];
        // Extract 4 bits starting at bit 7 (MSB of first byte)
        let value = extract_bits(&payload, 7, 4, true);
        assert_eq!(value, 0b1010);
    }

    #[test]
    fn test_sign_extend_positive() {
        // Positive value (MSB is 0)
        let raw = 0b0111u64; // 7 in 4 bits
        let extended = sign_extend(raw, 4);
        assert_eq!(extended, 7);
    }

    #[test]
    fn test_sign_extend_negative() {
        // Negative value (MSB is 1)
        let raw = 0b1111u64; // -1 in 4 bits (two's complement)
        let extended = sign_extend(raw, 4);
        assert_eq!(extended, -1);
    }

    #[test]
    fn test_sign_extend_zero_length() {
        let extended = sign_extend(0, 0);
        assert_eq!(extended, 0);
    }

    #[test]
    fn test_decode_signal_with_factor() {
        let spec = SignalSpec {
            col_index: 0,
            start: 0,
            length: 8,
            is_big_endian: false,
            is_signed: false,
            factor: 0.5,
            offset: 0.0,
            precision: 1,
            use_integer_math: false,
            factor_int: 0,
            offset_int: 0,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            clamp_min: None,
            clamp_max: None,
        };
        let payload = [100u8];
        let value = decode_signal(&payload, &spec);
        assert_eq!(value, Value::Float64(50.0));
    }

    #[test]
    fn test_decode_signal_signed() {
        let spec = SignalSpec {
            col_index: 0,
            start: 0,
            length: 8,
            is_big_endian: false,
            is_signed: true,
            factor: 1.0,
            offset: 0.0,
            precision: 0,
            use_integer_math: true,
            factor_int: 1,
            offset_int: 0,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            clamp_min: None,
            clamp_max: None,
        };
        // 0xFF = -1 in signed 8-bit
        let payload = [0xFFu8];
        let value = decode_signal(&payload, &spec);
        assert_eq!(value, Value::Int64(-1));
    }

    #[test]
    fn test_decode_signal_with_integer_offset() {
        // Integer scaling: factor=1, offset=-20000 should use integer math
        let spec = SignalSpec {
            col_index: 0,
            start: 0,
            length: 16,
            is_big_endian: false,
            is_signed: false,
            factor: 1.0,
            offset: -20000.0,
            precision: 0,
            use_integer_math: true,
            factor_int: 1,
            offset_int: -20000,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            clamp_min: None,
            clamp_max: None,
        };
        // Raw value 20500 -> 20500 - 20000 = 500
        let payload = [0xF4, 0x50]; // 20724 in little-endian
        let value = decode_signal(&payload, &spec);
        assert_eq!(value, Value::Int64(724)); // 20724 - 20000 = 724
    }

    #[test]
    fn test_decode_signal_with_integer_factor() {
        // Integer scaling: factor=2, offset=0 should use integer math
        let spec = SignalSpec {
            col_index: 0,
            start: 0,
            length: 8,
            is_big_endian: false,
            is_signed: false,
            factor: 2.0,
            offset: 0.0,
            precision: 0,
            use_integer_math: true,
            factor_int: 2,
            offset_int: 0,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            clamp_min: None,
            clamp_max: None,
        };
        let payload = [50u8];
        let value = decode_signal(&payload, &spec);
        assert_eq!(value, Value::Int64(100)); // 50 * 2 = 100
    }

    #[test]
    fn test_decode_signal_no_scaling() {
        // Test the path where factor=1.0 and offset=0.0 (no scaling)
        let spec = SignalSpec {
            col_index: 0,
            start: 0,
            length: 16,
            is_big_endian: false,
            is_signed: false,
            factor: 1.0,
            offset: 0.0,
            precision: 0,
            use_integer_math: true,
            factor_int: 1,
            offset_int: 0,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            clamp_min: None,
            clamp_max: None,
        };
        let payload = [0x34, 0x12]; // 0x1234 in little-endian
        let value = decode_signal(&payload, &spec);
        assert_eq!(value, Value::Int64(0x1234));
    }

    #[test]
    fn test_calculate_precision_small_factor() {
        // Test precision calculation for small factors
        let precision = calculate_precision(2.77778e-7, 0.0);
        assert!(
            precision >= 9,
            "Expected at least 9 decimal places for 2.77778e-7"
        );
    }

    #[test]
    fn test_calculate_precision_normal_factor() {
        let precision = calculate_precision(0.1, 0.0);
        assert_eq!(precision, 1);
    }

    #[test]
    fn test_calculate_precision_integer_factor() {
        let precision = calculate_precision(2.0, 0.0);
        assert_eq!(precision, 0);
    }

    #[test]
    fn test_is_integer_value() {
        assert!(is_integer_value(1.0));
        assert!(is_integer_value(-20000.0));
        assert!(!is_integer_value(0.5));
        assert!(!is_integer_value(1.1));
    }

    #[test]
    fn test_decimal_places() {
        assert_eq!(decimal_places(0.0), 0);
        assert_eq!(decimal_places(1.0), 0);
        assert_eq!(decimal_places(0.1), 1);
        assert_eq!(decimal_places(0.01), 2);
        assert_eq!(decimal_places(1.5), 1);
    }

    #[test]
    fn test_round_to_precision() {
        assert_eq!(round_to_precision(1.2345, 2), 1.23);
        assert_eq!(round_to_precision(1.2355, 2), 1.24);
        assert_eq!(round_to_precision(1.5, 0), 2.0);
    }

    // ========================================================================
    // Multiplex Signal Tests (TDD - tests first, implementation follows)
    // ========================================================================

    fn get_multiplex_decoder() -> CanDecoder {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/mul.json");
        let dbc = load_dbc_json(path.to_str().unwrap()).expect("load mul.json");
        let schema = Arc::new(schema_from_dbc("mul", &dbc, None));
        CanDecoder::new("mul", schema.clone(), dbc.clone(), None, true).expect("build decoder")
    }

    /// Test multiplex group1 (mux=1): Should decode no_filt_* signals, NOT group0 signals
    #[test]
    fn test_multiplex_group1() {
        let decoder = get_multiplex_decoder();

        // Hex: 00000190a5a0ec4a00185500c888015465737400001155124a880854657374000011
        // CAN ID 0x00C8 with bus=0 -> combined ID = 0x00C8
        // Byte 4 of frame data: 0x01 -> mux=1 (bits 0-3)
        let hex = "00000190a5a0ec4a00185500c888015465737400001155124a880854657374000011";
        let packet: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();

        // Parse the SPI packet to get CAN frames
        // Timestamp bytes 0-7, frames_len bytes 8-9, then frames
        let timestamp = u64::from_be_bytes(packet[0..8].try_into().unwrap());

        // First frame starts at byte 10: 55 00 C8 88 01 54 65 73 74 00 00 11
        // Magic=0x55, CAN_ID=0x00C8, metadata=0x88 (len=8), payload: 01 54 65 73 74 00 00 11
        let frame1_payload = &packet[14..22]; // 8 bytes after header

        let frames = vec![CanFrame {
            timestamp,
            can_id: 0x00C8, // bus_id=0 << 12 | msg_id=200
            payload: frame1_payload,
        }];

        let result = decoder.decode_frames(frames, None);
        assert!(result.is_some(), "Expected decoded tuple");
        let tuple = result.unwrap();

        // Verify mux value = 1
        let mux = tuple.value_by_name("mul", "SENSOR_SONARS_mux");
        assert!(mux.is_some(), "mux signal not found");
        assert_eq!(*mux.unwrap(), Value::Int64(1));

        // Verify mux1 signal is present (group 1)
        let mux1 = tuple.value_by_name("mul", "SENSOR_SONARS_mux1");
        assert!(mux1.is_some(), "mux1 signal not found");
        assert_eq!(*mux1.unwrap(), Value::Int64(1));

        // Verify no_filt_left (group 1 signal with scale 0.1)
        let no_filt_left = tuple.value_by_name("mul", "SENSOR_SONARS_no_filt_left");
        assert!(no_filt_left.is_some(), "no_filt_left signal not found");
        assert_eq!(*no_filt_left.unwrap(), Value::Float64(86.9));

        // Verify err_count (common signal, not multiplexed)
        let err_count = tuple.value_by_name("mul", "SENSOR_SONARS_err_count");
        assert!(err_count.is_some(), "err_count signal not found");
        assert_eq!(*err_count.unwrap(), Value::Int64(1344));

        // CRITICAL: Group 0 signals should be Null (NOT decoded when mux=1)
        let mux0 = tuple.value_by_name("mul", "SENSOR_SONARS_mux0");
        assert!(mux0.is_some(), "mux0 signal column not in schema");
        assert_eq!(
            *mux0.unwrap(),
            Value::Null,
            "mux0 should be Null when mux=1"
        );

        let left = tuple.value_by_name("mul", "SENSOR_SONARS_left");
        assert!(left.is_some(), "left signal column not in schema");
        assert_eq!(
            *left.unwrap(),
            Value::Null,
            "left should be Null when mux=1"
        );
    }

    /// Test multiplex group0 (mux=0): Should decode left/middle/right/rear signals, NOT group1 signals
    #[test]
    fn test_multiplex_group0() {
        let decoder = get_multiplex_decoder();

        // Hex: 00000190a5a0ec4a00185500c888002465737400001155124a880854657374000010
        // mux=0 (bits 0-3 = 0)
        let hex = "00000190a5a0ec4a00185500c888002465737400001155124a880854657374000010";
        let packet: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();

        let timestamp = u64::from_be_bytes(packet[0..8].try_into().unwrap());
        let frame1_payload = &packet[14..22];

        let frames = vec![CanFrame {
            timestamp,
            can_id: 0x00C8,
            payload: frame1_payload,
        }];

        let result = decoder.decode_frames(frames, None);
        assert!(result.is_some(), "Expected decoded tuple");
        let tuple = result.unwrap();

        // Verify mux = 0
        let mux = tuple.value_by_name("mul", "SENSOR_SONARS_mux");
        assert!(mux.is_some(), "mux signal not found");
        assert_eq!(*mux.unwrap(), Value::Int64(0));

        // Verify mux0 signal (group 0)
        let mux0 = tuple.value_by_name("mul", "SENSOR_SONARS_mux0");
        assert!(mux0.is_some(), "mux0 signal not found");
        assert_eq!(*mux0.unwrap(), Value::Int64(0));

        // Verify left signal (group 0 with scale 0.1)
        let left = tuple.value_by_name("mul", "SENSOR_SONARS_left");
        assert!(left.is_some(), "left signal not found");
        assert_eq!(*left.unwrap(), Value::Float64(86.9));

        // Verify err_count (common signal)
        let err_count = tuple.value_by_name("mul", "SENSOR_SONARS_err_count");
        assert!(err_count.is_some(), "err_count signal not found");
        assert_eq!(*err_count.unwrap(), Value::Int64(576));

        // CRITICAL: Group 1 signals should be Null (NOT decoded when mux=0)
        let mux1 = tuple.value_by_name("mul", "SENSOR_SONARS_mux1");
        assert!(mux1.is_some(), "mux1 signal column not in schema");
        assert_eq!(
            *mux1.unwrap(),
            Value::Null,
            "mux1 should be Null when mux=0"
        );

        let no_filt_left = tuple.value_by_name("mul", "SENSOR_SONARS_no_filt_left");
        assert!(
            no_filt_left.is_some(),
            "no_filt_left signal column not in schema"
        );
        assert_eq!(
            *no_filt_left.unwrap(),
            Value::Null,
            "no_filt_left should be Null when mux=0"
        );
    }

    #[test]
    fn test_decode_frames_with_projection() {
        use flow::planner::decode_projection::{DecodeProjection, FieldPath};

        let decoder = get_multiplex_decoder();

        // Hex: 00000190a5a0ec4a00185500c888015465737400001155124a880854657374000011
        // This packet contains mux=1 (Group 1)
        let hex = "00000190a5a0ec4a00185500c888015465737400001155124a880854657374000011";
        let packet: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();

        // Construct frames
        let timestamp = u64::from_be_bytes(packet[0..8].try_into().unwrap());
        let frame1_payload = &packet[14..22];
        let frames = vec![CanFrame {
            timestamp,
            can_id: 0x00C8,
            payload: frame1_payload,
        }];

        // Scenario 1: Project only 'SENSOR_SONARS_mux1' (which is in Group 1)
        // We do NOT explicitly project the multiplexer 'SENSOR_SONARS_mux'
        // The decoder must still decode the mux internally to know that mux1 is valid.
        // It should return mux=Null (if strict) or implicitly not return it?
        // CanDecoder returns a Tuple where positions match schema.
        // If 'mux' is not in projection -> result[mux_idx] should be Null.
        // 'mux1' is in -> result[mux1_idx] should be 1.
        // 'no_filt_left' (also Group 1) is NOT in -> result[no_filt_left_idx] should be Null.

        let mut projection = DecodeProjection::default();
        // Mark 'SENSOR_SONARS_mux1' as used
        projection.mark_field_path_used(&FieldPath {
            column: "SENSOR_SONARS_mux1".to_string(),
            segments: vec![],
        });

        let result = decoder.decode_frames(frames.clone(), Some(&projection));
        assert!(result.is_some());
        let tuple = result.unwrap();

        let mux1 = tuple.value_by_name("mul", "SENSOR_SONARS_mux1");
        assert_eq!(*mux1.unwrap(), Value::Int64(1));

        let mux = tuple.value_by_name("mul", "SENSOR_SONARS_mux");
        assert_eq!(
            *mux.unwrap(),
            Value::Null,
            "mux should be Null if not projected"
        );

        let no_filt_left = tuple.value_by_name("mul", "SENSOR_SONARS_no_filt_left");
        assert_eq!(
            *no_filt_left.unwrap(),
            Value::Null,
            "no_filt_left should be Null if not projected"
        );
    }
}
