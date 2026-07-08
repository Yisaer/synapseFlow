//! SOME/IP payload decoder for GBF frames.
//!
//! Uses `arxml_converter` to decode AUTOSAR ARXML-defined payloads.
//! The GBF transport layer provides `format_id = (service_id << 16) | method_id`;
//! this decoder splits it back and resolves the decode plan.
//!
//! Architecture mirrors CanDecoder: all decode plans are pre-built at
//! construction time, and lookups are lock-free HashMap gets on the hot path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arxml_converter::{ArxmlCodec, Value as ArxmlValue};
use bytes::Bytes;

use datatypes::{ConcreteDatatype, Schema, Value};
use flow::{
    codec::CodecError,
    model::{Message, Tuple},
    planner::decode_projection::DecodeProjection,
};

use super::payload::{GbfPayloadFrame, PayloadDecoder};
use crate::schema::arxml::apply_signal_name_pattern;

/// Pre-compiled decode plan for a single SOME/IP message.
#[derive(Debug, Clone)]
struct SomeIpDecodePlan {
    /// Field name → output column index, ordered for fast matching.
    field_to_index: Vec<(String, usize)>,
    /// Unique index for "seen" tracking (last-frame-wins).
    plan_index: usize,
}

/// Payload decoder for SOME/IP (ARXML-based) signals.
///
/// Mirrors `CanDecoder`: plans pre-built in `new()`, hot-path lookups
/// via lock-free `HashMap::get`.
pub struct SomeIpPayloadDecoder {
    /// Parsed ARXML codec (shared via Arc for reuse).
    _codec: Arc<ArxmlCodec>,
    /// (service_id, method_id) → pre-resolved column mapping.
    plans: HashMap<(u16, u16), SomeIpDecodePlan>,
    /// Shared output column keys.
    keys: Arc<[Arc<str>]>,
    /// Stream source name for message construction.
    source_name: Arc<str>,
    /// Pre-computed index of the 'ts' column, if present.
    ts_index: Option<usize>,
    /// Cached Null value for filling output slots.
    null_value: Arc<Value>,
    /// Cached projection state, as in CanDecoder.
    projection_cache: Mutex<Option<CachedProjection>>,
    /// Number of distinct plans (for seen-message tracking).
    plan_count: usize,
}

#[derive(Clone)]
struct CachedProjection {
    version: u64,
    /// Full-schema column index → output slot (None if pruned).
    col_to_slot: Arc<[Option<usize>]>,
    /// Output keys in slot order.
    slot_keys: Arc<[Arc<str>]>,
}

impl SomeIpPayloadDecoder {
    /// Create a decoder from a pre-loaded `ArxmlCodec`.
    /// Builds all decode plans eagerly at construction time.
    pub fn new(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        codec: Arc<ArxmlCodec>,
        signal_name_pattern: Option<&str>,
    ) -> Self {
        let source_name: Arc<str> = Arc::from(source_name.into());
        let keys: Arc<[Arc<str>]> = Arc::from(
            schema
                .column_schemas()
                .iter()
                .map(|col| Arc::<str>::from(col.name.as_str()))
                .collect::<Vec<_>>(),
        );
        let ts_index = keys.iter().position(|k| k.as_ref() == "ts");
        let null_value = Arc::new(Value::Null);

        let signal_name_pattern = signal_name_pattern.unwrap_or("{field}");

        // Pre-build all decode plans from known entries.
        let mut plans = HashMap::new();
        let mut plan_index = 0usize;
        for (service_id, method_id) in codec.known_entries() {
            if let Some(plan) = Self::build_plan(
                &codec,
                &keys,
                signal_name_pattern,
                service_id,
                method_id,
                plan_index,
            ) {
                plans.insert((service_id, method_id), plan);
                plan_index += 1;
            }
        }

        Self {
            _codec: codec,
            plans,
            keys,
            source_name,
            ts_index,
            null_value,
            projection_cache: Mutex::new(None),
            plan_count: plan_index,
        }
    }

    /// Create a decoder by loading the ARXML from a file path.
    pub fn from_path(
        source_name: impl Into<String>,
        schema: Arc<Schema>,
        arxml_path: &str,
    ) -> Result<Self, CodecError> {
        let codec = ArxmlCodec::load(arxml_path)
            .map_err(|e| CodecError::Other(format!("failed to load ARXML: {e}")))?;
        Ok(Self::new(source_name, schema, Arc::new(codec), None))
    }

    /// Build a decode plan for one (service_id, method_id) pair.
    fn build_plan(
        codec: &ArxmlCodec,
        keys: &[Arc<str>],
        signal_name_pattern: &str,
        service_id: u16,
        method_id: u16,
        plan_index: usize,
    ) -> Option<SomeIpDecodePlan> {
        let dt = codec.resolve(service_id, method_id).ok()?;
        let dt = match &dt.kind {
            arxml_converter::ast::types::DataTypeKind::Array(arr) => {
                codec.resolve_ref(&arr.element_ref)?
            }
            _ => dt.clone(),
        };
        let fields = match &dt.kind {
            arxml_converter::ast::types::DataTypeKind::Structure(s) => &s.fields,
            _ => return None,
        };
        let (service_name, entry_name) = codec
            .resolve_entry_names(service_id, method_id)
            .unwrap_or_else(|_| (format!("0x{service_id:04X}"), format!("0x{method_id:04X}")));
        let mut field_to_index = Vec::new();
        for field in fields {
            let column_name = apply_signal_name_pattern(
                signal_name_pattern,
                &service_name,
                &entry_name,
                &field.name,
            );
            if let Some(idx) = keys.iter().position(|k| k.as_ref() == column_name.as_str()) {
                field_to_index.push((field.name.clone(), idx));
            }
        }
        if field_to_index.is_empty() {
            return None;
        }
        Some(SomeIpDecodePlan {
            field_to_index,
            plan_index,
        })
    }

    /// Resolve projection state, caching by version.
    fn projection_state(&self, projection: Option<&DecodeProjection>) -> CachedProjection {
        let Some(proj) = projection else {
            return CachedProjection {
                version: 0,
                col_to_slot: Arc::from((0..self.keys.len()).map(Some).collect::<Vec<_>>()),
                slot_keys: Arc::clone(&self.keys),
            };
        };
        let mut cache = self.projection_cache.lock().expect("projection cache lock");
        if let Some(cached) = cache.as_ref()
            && cached.version == proj.version()
        {
            return cached.clone();
        }
        let columns = proj.columns();
        let col_to_slot: Arc<[Option<usize>]>;
        let slot_keys: Arc<[Arc<str>]>;

        if let Some(slots) = proj.output_slots() {
            let name_to_col: HashMap<&str, usize> = self
                .keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.as_ref(), i))
                .collect();
            let mut mapping: Vec<Option<usize>> = vec![None; self.keys.len()];
            let mut sk: Vec<Arc<str>> = Vec::with_capacity(slots.len());
            for (slot, name) in slots.iter().enumerate() {
                if let Some(&col) = name_to_col.get(name.as_ref()) {
                    mapping[col] = Some(slot);
                }
                sk.push(Arc::clone(name));
            }
            col_to_slot = Arc::from(mapping);
            slot_keys = Arc::from(sk);
        } else {
            let mut mapping: Vec<Option<usize>> = vec![None; self.keys.len()];
            let mut sk: Vec<Arc<str>> = Vec::with_capacity(columns.len());
            for (slot, name) in columns.keys().enumerate() {
                if let Some(idx) = self.keys.iter().position(|k| k.as_ref() == name.as_str()) {
                    mapping[idx] = Some(slot);
                }
                sk.push(Arc::<str>::from(name.as_str()));
            }
            col_to_slot = Arc::from(mapping);
            slot_keys = Arc::from(sk);
        }
        let cached = CachedProjection {
            version: proj.version(),
            col_to_slot,
            slot_keys,
        };
        *cache = Some(cached.clone());
        cached
    }
}

impl PayloadDecoder for SomeIpPayloadDecoder {
    #[inline]
    fn contains_format_id(&self, format_id: u32) -> bool {
        let service_id = (format_id >> 16) as u16;
        let method_id = format_id as u16;
        self.plans.contains_key(&(service_id, method_id))
    }

    fn decode_frames(
        &self,
        frames: Vec<GbfPayloadFrame<'_>>,
        projection: Option<&DecodeProjection>,
    ) -> Option<Tuple> {
        let first = frames.first()?;
        let ts = first.timestamp;
        let state = self.projection_state(projection);

        let out_width = state.slot_keys.len();
        let null_val = &self.null_value;
        let mut values: Vec<Arc<Value>> = (0..out_width).map(|_| Arc::clone(null_val)).collect();

        if let Some(ts_idx) = self.ts_index
            && let Some(slot) = state.col_to_slot[ts_idx]
        {
            values[slot] = Arc::new(Value::Int64(ts as i64));
        }

        // Last-frame-wins: iterate in reverse, decoding each unique
        // (service_id, method_id) only once.
        let mut seen_plans = vec![false; self.plan_count.max(1)];

        for frame in frames.iter().rev() {
            let service_id = (frame.format_id >> 16) as u16;
            let method_id = frame.format_id as u16;
            let plan = self.plans.get(&(service_id, method_id))?;
            if seen_plans[plan.plan_index] {
                continue;
            }
            seen_plans[plan.plan_index] = true;

            let result = self
                ._codec
                .decode(service_id, method_id, frame.payload)
                .ok()?;

            let fields_iter: Box<dyn Iterator<Item = (String, ArxmlValue)>> = match result {
                ArxmlValue::Struct(fields) => Box::new(fields.into_iter()),
                ArxmlValue::Array(items) => {
                    if let Some(ArxmlValue::Struct(fields)) = items.into_iter().next() {
                        Box::new(fields.into_iter())
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };

            for (name, val) in fields_iter {
                for (plan_field, col_idx) in &plan.field_to_index {
                    if plan_field == &name {
                        if let Some(slot) = state.col_to_slot[*col_idx] {
                            values[slot] = Arc::new(to_veloflux_value(val));
                        }
                        break;
                    }
                }
            }
        }

        let msg = Arc::new(Message::new_shared_keys(
            Arc::clone(&self.source_name),
            Arc::clone(&state.slot_keys),
            values,
        ));
        Some(Tuple::new(vec![msg]))
    }
}

/// Convert an arxml_converter Value to a veloFlux datatypes Value.
fn to_veloflux_value(v: ArxmlValue) -> Value {
    match v {
        ArxmlValue::U8(v) => Value::Uint8(v),
        ArxmlValue::U16(v) => Value::Uint16(v),
        ArxmlValue::U32(v) => Value::Uint32(v),
        ArxmlValue::U64(v) => Value::Uint64(v),
        ArxmlValue::I8(v) => Value::Int8(v),
        ArxmlValue::I16(v) => Value::Int16(v),
        ArxmlValue::I32(v) => Value::Int32(v),
        ArxmlValue::I64(v) => Value::Int64(v),
        ArxmlValue::F32(v) => Value::Float32(v),
        ArxmlValue::F64(v) => Value::Float64(v),
        ArxmlValue::Bool(v) => Value::Bool(v),
        ArxmlValue::Str(v) => Value::String(v),
        ArxmlValue::Bytes(v) => Value::Bytes(Bytes::from(v)),
        ArxmlValue::Struct(fields) => {
            let mut items = Vec::with_capacity(fields.len());
            let mut field_defs = Vec::with_capacity(fields.len());
            for (name, val) in fields {
                let converted = to_veloflux_value(val);
                field_defs.push(datatypes::StructField::new(
                    name,
                    converted.datatype(),
                    true,
                ));
                items.push(converted);
            }
            Value::Struct(datatypes::StructValue::new(
                items,
                datatypes::StructType::new(Arc::new(field_defs)),
            ))
        }
        ArxmlValue::Array(items) => {
            let elements: Vec<Value> = items.into_iter().map(to_veloflux_value).collect();
            let elem_type = elements
                .first()
                .map(|v| v.datatype())
                .unwrap_or(ConcreteDatatype::Null);
            Value::List(datatypes::ListValue::new(elements, Arc::new(elem_type)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::gbf::GbfDecoder;
    use crate::schema::gbf::GbfSchema;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema, Uint8Type, Uint16Type};
    use flow::codec::RecordDecoder;
    use std::sync::Arc;

    fn someip_transport_schema() -> GbfSchema {
        /* unchanged */
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u32be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "total_len",
                        "length_unit": "bytes",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "message_id", "type": "u32be" },
                                { "name": "data_len", "type": "u32be" },
                                { "name": "message_type", "type": "u8" },
                                { "name": "payload_len", "type": "u32be" },
                                {
                                    "name": "payload",
                                    "type": "bytes",
                                    "length_ref": "payload_len",
                                    "format": { "id_ref": "message_id" }
                                }
                            ]
                        }
                    }
                ]
            }
        }
        "#;
        serde_json::from_str(json).expect("parse transport schema")
    }

    fn someip_output_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "someip".to_string(),
                "ts".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "someip".to_string(),
                "DTE_SlotID".to_string(),
                ConcreteDatatype::Uint16(Uint16Type),
            ),
            ColumnSchema::new(
                "someip".to_string(),
                "DTE_SlotType".to_string(),
                ConcreteDatatype::Uint8(Uint8Type),
            ),
            ColumnSchema::new(
                "someip".to_string(),
                "DTE_SlotStatus".to_string(),
                ConcreteDatatype::Uint8(Uint8Type),
            ),
        ]))
    }

    #[test]
    fn test_someip_cp_resolve_and_decode() {
        let arxml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let codec = ArxmlCodec::load(arxml_path).expect("load arxml");
        let service_id: u16 = 0xAB04;
        let event_id: u16 = 0x8003;
        let test_payload = &[0x00u8; 8];

        let dt = codec.resolve(service_id, event_id).expect("resolve");
        assert!(!dt.short_name.is_empty(), "type should have a name");

        let val = codec
            .decode(service_id, event_id, test_payload)
            .expect("decode");
        assert!(
            matches!(&val, ArxmlValue::Array(_) | ArxmlValue::Struct(_)),
            "decode produced composite value"
        );
    }

    #[test]
    fn test_someip_decode_matches_ekuiper_e2e() {
        let arxml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");

        let schema = someip_output_schema();
        let payload_decoder = Box::new(
            SomeIpPayloadDecoder::from_path("someip", schema.clone(), arxml_path)
                .expect("create SomeIpPayloadDecoder"),
        );
        let gbf_schema = someip_transport_schema();
        let decoder = GbfDecoder::with_payload_decoder(gbf_schema, payload_decoder)
            .expect("create GbfDecoder");

        let hex = concat!(
            "00001a873e9f005800000355ab0480030000034d0200000348",
            "00460301c4f8ca99c0779029c4d0ddc9c1b251b3c50b6a9bc3f942f4c4eee865c4013c78",
            "0000000000000000000000000000000000000000470201c4cdc0e7c1ebdab4c4b291d5c2b72463c4ea231ac4035640c4cef408c412dbf700000000",
            "000000000000000000000000000000000000480301c4a62e03441f349cc4cdea9d44281f53c4bf2cf5448d9e3bc4e6e9d844921451000000000000",
            "000000000000000000000000000000000000490301c4a7a34ec228b0a9c4823692c2c82bcac4cc10b5c415735fc4a6a3f9c423edcd000000000000",
            "0000000000000000000000000000000000004a0301c47ca3d3441933d8c4a45d1d44201cf9c4960490448b20b4c4bc0fc4448e9544000000000000",
            "0000000000000000000000000000000000004b0303c42571a44415ed1fc47749b1441c6827c453596c4489c30fc49298d4448d008a010000000000",
            "0000000000000000000000000000000000004c0301c47bd983c2762bacc428d9afc2bbea59c49ff2f0c40eebfac46ce60ec417068a000000000000",
            "0000000000000000000000000000000000004d0303c3a15140440fe1f4c41df36b44167c8fc3fe9e5e44869b09c44c99fa4489e856020000000000",
            "0000000000000000000000000000000000004e0203c420b330c2c7b1cec3d1e8a5c31289dbc45e6f93c41287dac426b112c41e32e5090000000000",
            "0000000000000000000000000000000000004f0303c38ff31ac2e47ffdc288c21cc32999fdc401da3fc41803b4c395f2bbc425d8fe080000000000",
            "00000000000000000000000000000000000050030341ea0384440a6cf8c39688e044103bd2c3323fa84482d783c3fe488f4485bef9030000000000",
            "00000000000000000000000000000000000051030342045b5fc30403d9439ea9e1c3389791c38cbbbc430db1c40315d4bc43e000a0700000000000",
            "00000000000000000000000000000000000052030343a4bd6844079dde423a8897440c3d5e430ffedb44828655c30ada884484d60c040000000000",
            "00000000000000000000000000000000000053030343c5931cc316249d441a44fdc340671543222f30c420d11243c00f17c42b62f7060000000000",
            "00000000000000000000000000000000000054030344276687440267d343b654da4406b29243f41bc644801b18433747214482407805000000000000",
            "0000000000000000000000000000000000"
        );
        let data = hex::decode(hex).expect("decode hex");

        let batch = decoder.decode(&data).expect("decode batch");
        let rows = batch.rows();
        assert!(!rows.is_empty(), "expected at least one row");

        let row = &rows[0];
        let ts = row.value_by_name("someip", "ts");
        assert_eq!(ts, Some(&Value::Int64(29168173514840)), "ts mismatch");

        let slot_id = row.value_by_name("someip", "DTE_SlotID");
        assert_eq!(slot_id, Some(&Value::Uint16(70)), "DTE_SlotID mismatch");
        let slot_type = row.value_by_name("someip", "DTE_SlotType");
        assert_eq!(slot_type, Some(&Value::Uint8(3)), "DTE_SlotType mismatch");
        let slot_status = row.value_by_name("someip", "DTE_SlotStatus");
        assert_eq!(
            slot_status,
            Some(&Value::Uint8(1)),
            "DTE_SlotStatus mismatch"
        );
    }
}
