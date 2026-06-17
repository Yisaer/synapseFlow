//! CAN decoder module providing reusable CAN frame signal decoding.
//!
//! This module contains the core CAN signal decoding logic that can be reused
//! by various protocol decoders (SPI, raw CAN, etc.) that work with CAN frames.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::sync::Mutex;

/// Identity hasher for the `u16` CAN-id message map. CAN ids are already small,
/// well-distributed integers, so the default SipHash is pure overhead: it shows
/// as ~6% (`hash_one`) of the box.home `allcases` decode profile, one lookup per
/// frame. This mirrors eKuiper's `mapaccess2_fast32`. Keyed exclusively by u16,
/// so only `write_u16` is exercised; `write` is a defensive fallback.
#[derive(Default)]
struct CanIdHasher(u64);

impl Hasher for CanIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.0 = i as u64;
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 << 8) | b as u64;
        }
    }
}

type CanIdMap<V> = HashMap<u16, V, BuildHasherDefault<CanIdHasher>>;

use datatypes::{Schema, Value};
use flow::{
    codec::CodecError,
    model::{Message, Tuple},
    planner::decode_projection::DecodeProjection,
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

/// Resolved per-signal numeric decode plan.
///
/// Computed once at decoder/schema construction time so the decode hot path is
/// a single match using native-width integer arithmetic.
#[derive(Debug, Clone, PartialEq)]
pub enum SignalNumeric {
    /// `factor == 1, offset == 0`, value fits `i64`. `signed` selects sign extension.
    IdentityI64 { signed: bool },
    /// `factor == 1, offset == 0`, unsigned value that exceeds `i64`.
    IdentityU64,
    /// Integer scaling whose intermediate product and result fit `i64`.
    ScaledI64 {
        factor: i64,
        offset: i64,
        signed: bool,
    },
    /// Integer scaling whose non-negative result fits `u64` but exceeds `i64`.
    ScaledU64 {
        factor: i64,
        offset: i64,
        signed: bool,
    },
    /// Fractional scaling, or integer scaling that cannot stay in integer range.
    F64 {
        factor: f64,
        offset: f64,
        precision: u32,
        signed: bool,
    },
}

impl SignalNumeric {
    pub fn datatype(&self) -> datatypes::ConcreteDatatype {
        use datatypes::ConcreteDatatype;
        match self {
            SignalNumeric::IdentityI64 { .. } | SignalNumeric::ScaledI64 { .. } => {
                ConcreteDatatype::Int64(datatypes::Int64Type)
            }
            SignalNumeric::IdentityU64 | SignalNumeric::ScaledU64 { .. } => {
                ConcreteDatatype::Uint64(datatypes::Uint64Type)
            }
            SignalNumeric::F64 { .. } => ConcreteDatatype::Float64(datatypes::Float64Type),
        }
    }
}

/// Specification for decoding a single CAN signal.
pub struct SignalSpec {
    pub col_index: usize,
    pub start: u32,
    pub length: u32,
    pub is_big_endian: bool,
    /// Resolved numeric decode plan (sign + scale handling), computed at init time.
    pub numeric: SignalNumeric,
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
    /// Dense per-decoder index used as a key into per-decode scratch tables
    /// (e.g. the seen-message set in [`CanDecoder::decode_frames`]).
    pub msg_index: usize,
}

#[derive(Clone)]
enum RequiredColumns {
    All,
    Mask(Arc<[bool]>),
}

impl RequiredColumns {
    #[inline]
    fn includes(&self, col_index: usize) -> bool {
        match self {
            RequiredColumns::All => true,
            RequiredColumns::Mask(mask) => mask[col_index],
        }
    }
}

/// VF-56: slot-ordered output layout. When present, [`CanDecoder::decode_frames`]
/// emits a narrow row whose width/order is `keys` (slot == position) instead of the
/// full schema, gathering each slot's value from the decoder's full-schema column.
#[derive(Clone)]
struct SlotLayout {
    /// Slot-ordered output column names (the consumer `ByIndex` space).
    keys: Arc<[Arc<str>]>,
    /// position in `self.keys` → output slot (`None` if that column is not in the
    /// output slice). Lets `decode_frames` write decoded values **directly** into
    /// the narrow slot row, avoiding a full-width scratch + gather pass.
    col_to_slot: Arc<[Option<usize>]>,
}

struct ProjectionCache {
    version: u64,
    required: RequiredColumns,
    slot_layout: Option<SlotLayout>,
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
    messages: CanIdMap<MessageSpec>,
    /// Number of distinct `msg_index` values; sizes per-decode scratch tables.
    message_count: usize,
    ts_index: Option<usize>,
    /// Cached Null value to avoid allocating new Arc on every decode
    null_value: Arc<Value>,
    projection_cache: Mutex<Option<ProjectionCache>>,
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

        let mut messages = CanIdMap::default();
        let mut next_msg_index = 0usize;
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
                    let numeric = classify_signal(sig.length, sig.is_signed, factor, offset);
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
                        numeric,
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
                let msg_index = next_msg_index;
                next_msg_index += 1;
                messages.insert(
                    can_id,
                    MessageSpec {
                        signals,
                        has_multiplexer,
                        msg_index,
                    },
                );
            }
        }

        Ok(Self {
            source_name: Arc::<str>::from(source_name),
            keys: Arc::from(keys),
            messages,
            message_count: next_msg_index,
            ts_index,
            null_value: Arc::new(Value::Null),
            projection_cache: Mutex::new(None),
        })
    }

    fn projection_state(
        &self,
        projection: Option<&DecodeProjection>,
    ) -> (RequiredColumns, Option<SlotLayout>) {
        let Some(projection) = projection else {
            return (RequiredColumns::All, None);
        };

        let mut cache = self
            .projection_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cached) = cache.as_ref()
            && cached.version == projection.version()
        {
            return (cached.required.clone(), cached.slot_layout.clone());
        }

        let columns = projection.columns();
        let required = if columns.len() >= self.keys.len()
            && self
                .keys
                .iter()
                .all(|key| columns.contains_key(key.as_ref()))
        {
            RequiredColumns::All
        } else {
            RequiredColumns::Mask(
                self.keys
                    .iter()
                    .map(|key| columns.contains_key(key.as_ref()))
                    .collect::<Vec<bool>>()
                    .into(),
            )
        };

        // VF-56: build the slot-ordered output layout (full-schema column → slot).
        let slot_layout = projection.output_slots().map(|slots| {
            let name_to_col: HashMap<&str, usize> = self
                .keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.as_ref(), i))
                .collect();
            let mut col_to_slot: Vec<Option<usize>> = vec![None; self.keys.len()];
            for (slot, name) in slots.iter().enumerate() {
                if let Some(&col) = name_to_col.get(name.as_ref()) {
                    col_to_slot[col] = Some(slot);
                }
            }
            SlotLayout {
                keys: Arc::clone(slots),
                col_to_slot: col_to_slot.into(),
            }
        });

        *cache = Some(ProjectionCache {
            version: projection.version(),
            required: required.clone(),
            slot_layout: slot_layout.clone(),
        });
        (required, slot_layout)
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
        projection: Option<&DecodeProjection>,
    ) -> Option<Tuple> {
        if frames.is_empty() {
            return None;
        }

        let (required_columns, slot_layout) = self.projection_state(projection);

        // Output position for a full-schema column: its slot (VF-56 narrow row) or
        // the column itself. Decoding writes straight into this position, so the
        // scratch `values` is sized to the output width (union, not full DBC).
        let out_idx = |col: usize| -> Option<usize> {
            match &slot_layout {
                Some(layout) => layout.col_to_slot[col],
                None => Some(col),
            }
        };
        let out_width = slot_layout
            .as_ref()
            .map(|l| l.keys.len())
            .unwrap_or(self.keys.len());
        let mut values: Vec<Option<Value>> = vec![None; out_width];

        if let Some(ts_idx) = self.ts_index
            && let Some(o) = out_idx(ts_idx)
        {
            values[o] = Some(Value::Int64(frames[0].timestamp as i64));
        }

        // A packed window typically repeats the same message many times (a
        // typical replay averages several frames per can_id per window), and
        // only the newest occurrence survives "last frame wins" semantics.
        // Iterate in reverse and decode each message (or, for multiplexed
        // messages, each (message, mux value) pair) only once — the first
        // occurrence seen in reverse is the newest frame. This matches the
        // eKuiper SPI converter, whose Merging step keeps the last frame per
        // can_id (and per mux value for multiplexed messages) before decoding.
        let mut seen_messages = vec![false; self.message_count];
        // (msg_index, mux value) pairs already decoded. Multiplexed messages
        // are rare and carry few distinct mux values per window, so a linear
        // scan beats a hash set here.
        let mut seen_mux: Vec<(usize, Option<i64>)> = Vec::new();

        for frame in frames.iter().rev() {
            let Some(spec) = self.messages.get(&frame.can_id) else {
                trace!(can_id = frame.can_id, "frame spec not found for can_id");
                continue;
            };

            if !spec.has_multiplexer {
                if seen_messages[spec.msg_index] {
                    continue;
                }
                seen_messages[spec.msg_index] = true;

                for signal in &spec.signals {
                    if !required_columns.includes(signal.col_index) {
                        continue;
                    }
                    if let Some(o) = out_idx(signal.col_index) {
                        values[o] = Some(decode_signal(frame.payload, signal));
                    }
                }
                continue;
            }

            // Multiplexed message: decode the multiplexer selector to get the
            // current mux value, then decode this (message, mux value) pair at
            // most once. Columns are written only when still empty so that the
            // newest frame (visited first in reverse) wins.
            let mut multiplexer_value: Option<i64> = None;
            for signal in &spec.signals {
                if signal.is_multiplexer {
                    let val = decode_signal(frame.payload, signal);
                    multiplexer_value = match &val {
                        Value::Int64(v) => Some(*v),
                        Value::Uint64(v) => i64::try_from(*v).ok(),
                        _ => None,
                    };
                    if required_columns.includes(signal.col_index)
                        && let Some(o) = out_idx(signal.col_index)
                        && values[o].is_none()
                    {
                        values[o] = Some(val);
                    }
                    break;
                }
            }

            let mux_key = (spec.msg_index, multiplexer_value);
            if seen_mux.contains(&mux_key) {
                continue;
            }
            seen_mux.push(mux_key);

            for signal in &spec.signals {
                // Skip multiplexer signal (already decoded above)
                if signal.is_multiplexer {
                    continue;
                }

                // Check projection mask: Skip if not required
                if !required_columns.includes(signal.col_index) {
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

                if let Some(o) = out_idx(signal.col_index)
                    && values[o].is_none()
                {
                    values[o] = Some(decode_signal(frame.payload, signal));
                }
            }
        }

        // `values` is already in output order (narrow slot row under VF-56, else
        // full schema). Fill unwritten cells with the cached Null Arc — now at most
        // union-width clones, not full-DBC width.
        let null_arc = &self.null_value;
        let finalized: Vec<Arc<Value>> = values
            .into_iter()
            .map(|opt| opt.map(Arc::new).unwrap_or_else(|| Arc::clone(null_arc)))
            .collect();
        let out_keys = match &slot_layout {
            Some(layout) => Arc::clone(&layout.keys),
            None => Arc::clone(&self.keys),
        };

        let message = Arc::new(Message::new_shared_keys(
            Arc::clone(&self.source_name),
            out_keys,
            finalized,
        ));
        Some(Tuple::new(vec![message]))
    }

    /// Get the message specifications map.
    #[allow(dead_code)]
    fn messages(&self) -> &CanIdMap<MessageSpec> {
        &self.messages
    }

    /// Get the column keys.
    #[allow(dead_code)]
    pub fn keys(&self) -> &Arc<[Arc<str>]> {
        &self.keys
    }
}

/// Decode a single signal from CAN frame payload.
///
/// Hot path: a single dispatch on the pre-resolved [`SignalNumeric`] plan, using
/// native-width integer arithmetic. Clamp remains applied after scaling so this
/// keeps the existing DBC range semantics.
pub fn decode_signal(payload: &[u8], spec: &SignalSpec) -> Value {
    let raw = extract_bits(
        payload,
        spec.start as usize,
        spec.length as usize,
        spec.is_big_endian,
    );
    let length = spec.length as usize;

    let value = match spec.numeric {
        SignalNumeric::IdentityI64 { signed } => {
            let v = if signed {
                sign_extend(raw, length)
            } else {
                raw as i64
            };
            Value::Int64(v)
        }
        SignalNumeric::IdentityU64 => Value::Uint64(raw),
        SignalNumeric::ScaledI64 {
            factor,
            offset,
            signed,
        } => {
            let r = if signed {
                sign_extend(raw, length)
            } else {
                raw as i64
            };
            Value::Int64(r * factor + offset)
        }
        SignalNumeric::ScaledU64 {
            factor,
            offset,
            signed,
        } => {
            let r = if signed {
                i128::from(sign_extend(raw, length))
            } else {
                i128::from(raw)
            };
            Value::Uint64((r * i128::from(factor) + i128::from(offset)) as u64)
        }
        SignalNumeric::F64 {
            factor,
            offset,
            precision,
            signed,
        } => {
            let r = if signed {
                sign_extend(raw, length) as f64
            } else {
                raw as f64
            };
            Value::Float64(round_to_precision(r * factor + offset, precision))
        }
    };

    clamp_to_dbc_range(value, spec.clamp_min, spec.clamp_max)
}

/// Raw value range `[min, max]` for a signal, expressed in `i128`.
fn raw_range_i128(length: u32, is_signed: bool) -> Option<(i128, i128)> {
    if length == 0 || length > 64 {
        return None;
    }
    if is_signed {
        let half = 1i128 << (length - 1);
        Some((-half, half - 1))
    } else {
        Some((0, (1i128 << length) - 1))
    }
}

/// Resolve a signal's numeric decode plan from its DBC attributes.
pub fn classify_signal(length: u32, is_signed: bool, factor: f64, offset: f64) -> SignalNumeric {
    let precision = calculate_precision(factor, offset);
    let float = || SignalNumeric::F64 {
        factor,
        offset,
        precision,
        signed: is_signed,
    };

    if !is_integer_value(factor) || !is_integer_value(offset) {
        return float();
    }

    let factor_i = factor as i64;
    let offset_i = offset as i64;
    let identity = factor_i == 1 && offset_i == 0;

    let Some((raw_min, raw_max)) = raw_range_i128(length, is_signed) else {
        return float();
    };

    let f = i128::from(factor_i);
    let o = i128::from(offset_i);
    let (Some(m1), Some(m2)) = (raw_min.checked_mul(f), raw_max.checked_mul(f)) else {
        return float();
    };
    let (Some(p1), Some(p2)) = (m1.checked_add(o), m2.checked_add(o)) else {
        return float();
    };
    let mmin = m1.min(m2);
    let mmax = m1.max(m2);
    let pmin = p1.min(p2);
    let pmax = p1.max(p2);

    const I64_MIN: i128 = i64::MIN as i128;
    const I64_MAX: i128 = i64::MAX as i128;
    const U64_MAX: i128 = u64::MAX as i128;

    if mmin >= I64_MIN && mmax <= I64_MAX && pmin >= I64_MIN && pmax <= I64_MAX {
        if identity {
            SignalNumeric::IdentityI64 { signed: is_signed }
        } else {
            SignalNumeric::ScaledI64 {
                factor: factor_i,
                offset: offset_i,
                signed: is_signed,
            }
        }
    } else if pmin >= 0 && pmax <= U64_MAX {
        if identity {
            SignalNumeric::IdentityU64
        } else {
            SignalNumeric::ScaledU64 {
                factor: factor_i,
                offset: offset_i,
                signed: is_signed,
            }
        }
    } else {
        float()
    }
}

/// Clamp a decoded physical value to the DBC `[min, max]` range, matching
/// eKuiper's `can_dbc` behaviour. A no-op when both bounds are `None`. The value
/// type (Int64 / Uint64 / Float64) is preserved.
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
        Value::Uint64(v) => {
            let mut out = v;
            if let Some(mn) = min {
                let lo = if mn <= 0.0 { 0 } else { mn.ceil() as u64 };
                if out < lo {
                    out = lo;
                }
            }
            if let Some(mx) = max {
                let hi = mx.floor() as u64;
                if out > hi {
                    out = hi;
                }
            }
            Value::Uint64(out)
        }
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

    /// VF-56: with an output slot layout, decode_frames emits a narrow row whose
    /// width/order is the slot list (slot == index), gathered from full-schema
    /// columns — the consumer `ByIndex` (== slot) reads it directly.
    #[test]
    fn test_decode_frames_slot_layout_emits_narrow_slice() {
        use flow::planner::decode_projection::DecodeProjection;
        let decoder = get_test_decoder();

        // Slot order (first-seen / append-only), deliberately NOT schema order:
        // slot 0 = Mess0_Sig1, slot 1 = ts.
        let slots: Arc<[Arc<str>]> = Arc::from(vec![Arc::from("Mess0_Sig1"), Arc::from("ts")]);
        let projection = DecodeProjection::from_top_level_columns_with_version(
            &["Mess0_Sig1".to_string(), "ts".to_string()],
            1,
        )
        .with_output_slots(Arc::clone(&slots));

        // Mess0 (can_id 0x124A), Sig1 big-endian start 1 len 10 -> byte1=2 => Sig1=2.
        let payload = [0x00u8, 0x02, 0, 0, 0, 0, 0, 0];
        let frames = vec![CanFrame {
            timestamp: 42,
            can_id: 0x124A,
            payload: &payload,
        }];

        let tuple = decoder
            .decode_frames(frames, Some(&projection))
            .expect("decode");

        // Width is exactly the slot count (narrow), in slot order.
        assert_eq!(
            tuple.value_by_index("can", 1),
            Some(&Value::Int64(42)),
            "slot 1 == ts"
        );
        assert!(
            tuple.value_by_index("can", 2).is_none(),
            "no slot beyond the layout width (narrow row)"
        );
        assert_ne!(
            tuple.value_by_index("can", 0),
            Some(&Value::Null),
            "slot 0 == Mess0_Sig1, decoded non-null"
        );
        // Name lookup still resolves against the narrow keys.
        assert_eq!(tuple.value_by_name("can", "ts"), Some(&Value::Int64(42)));
    }

    /// Repeated frames of the same (non-multiplex) message: only the last
    /// frame's values survive, exactly as the previous forward-overwrite
    /// semantics produced.
    #[test]
    fn test_repeated_frames_last_wins() {
        let decoder = get_test_decoder();

        // sim.json bus 1, msg 586 -> can_id 0x124A; Mess0_Sig1 is big-endian,
        // start bit 1, length 10: byte0 bits [1:0] (high) + byte1 (low).
        let payload_old = [0x00u8, 0x01, 0, 0, 0, 0, 0, 0]; // Sig1 = 1
        let payload_new = [0x00u8, 0x02, 0, 0, 0, 0, 0, 0]; // Sig1 = 2
        let frames = vec![
            CanFrame {
                timestamp: 1,
                can_id: 0x124A,
                payload: &payload_old,
            },
            CanFrame {
                timestamp: 2,
                can_id: 0x124A,
                payload: &payload_new,
            },
        ];

        let tuple = decoder.decode_frames(frames, None).expect("decode");
        let sig1 = tuple.value_by_name("can", "Mess0_Sig1").expect("Sig1");
        assert_eq!(*sig1, Value::Int64(2), "last frame must win");
    }

    /// Repeated frames of a multiplexed message with the same mux value:
    /// the newest frame's values win.
    #[test]
    fn test_repeated_mux_frames_last_wins() {
        let decoder = get_multiplex_decoder();

        // mux=1 frames; err_count = bits 4-15.
        let payload_old = [0x01u8, 0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11]; // err_count 1344
        let payload_new = [0x01u8, 0x99, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11]; // err_count 2448
        let frames = vec![
            CanFrame {
                timestamp: 1,
                can_id: 0x00C8,
                payload: &payload_old,
            },
            CanFrame {
                timestamp: 2,
                can_id: 0x00C8,
                payload: &payload_new,
            },
        ];

        let tuple = decoder.decode_frames(frames, None).expect("decode");
        let err = tuple
            .value_by_name("mul", "SENSOR_SONARS_err_count")
            .expect("err_count");
        assert_eq!(*err, Value::Int64(2448), "last frame must win");
        let mux1 = tuple
            .value_by_name("mul", "SENSOR_SONARS_mux1")
            .expect("mux1");
        assert_eq!(*mux1, Value::Int64(1));
    }

    /// Frames with different mux values both contribute their groups; shared
    /// (non-multiplexed) signals and the selector come from the last frame.
    #[test]
    fn test_mux_groups_merge_across_frames() {
        let decoder = get_multiplex_decoder();

        let payload_g1 = [0x01u8, 0x54, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11]; // mux=1, err_count 1344
        let payload_g0 = [0x00u8, 0x24, 0x65, 0x73, 0x74, 0x00, 0x00, 0x11]; // mux=0, err_count 576
        let frames = vec![
            CanFrame {
                timestamp: 1,
                can_id: 0x00C8,
                payload: &payload_g1,
            },
            CanFrame {
                timestamp: 2,
                can_id: 0x00C8,
                payload: &payload_g0,
            },
        ];

        let tuple = decoder.decode_frames(frames, None).expect("decode");

        // Selector and shared signal come from the last frame (mux=0).
        let mux = tuple
            .value_by_name("mul", "SENSOR_SONARS_mux")
            .expect("mux");
        assert_eq!(*mux, Value::Int64(0));
        let err = tuple
            .value_by_name("mul", "SENSOR_SONARS_err_count")
            .expect("err_count");
        assert_eq!(*err, Value::Int64(576));

        // Group 0 from the last frame.
        let left = tuple
            .value_by_name("mul", "SENSOR_SONARS_left")
            .expect("left");
        assert_eq!(*left, Value::Float64(86.9));

        // Group 1 preserved from the earlier frame.
        let no_filt_left = tuple
            .value_by_name("mul", "SENSOR_SONARS_no_filt_left")
            .expect("no_filt_left");
        assert_eq!(*no_filt_left, Value::Float64(86.9));
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
            numeric: classify_signal(8, false, 0.5, 0.0),
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
            numeric: classify_signal(8, true, 1.0, 0.0),
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
            numeric: classify_signal(16, false, 1.0, -20000.0),
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
            numeric: classify_signal(8, false, 2.0, 0.0),
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
            numeric: classify_signal(16, false, 1.0, 0.0),
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

        let full_columns: Vec<String> = decoder
            .keys()
            .iter()
            .map(|key| key.as_ref().to_string())
            .collect();
        let full_projection =
            DecodeProjection::from_top_level_columns_with_version(full_columns.as_slice(), 2);

        let full_tuple = decoder
            .decode_frames(frames.clone(), Some(&full_projection))
            .expect("full projection should decode");
        let unprojected_tuple = decoder
            .decode_frames(frames, None)
            .expect("unprojected decode should decode");

        for key in decoder.keys().iter() {
            assert_eq!(
                full_tuple.value_by_name("mul", key.as_ref()),
                unprojected_tuple.value_by_name("mul", key.as_ref()),
                "full projection should match unprojected decode for {key}"
            );
        }
    }
}
