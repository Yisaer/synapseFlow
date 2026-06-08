//! Protobuf encoder implementing [`SinkEncoderFactory`] / [`SinkEncoder`].
//!
//! Encodes [`Collection`] rows into protobuf wire-format bytes using a
//! [`ProtoDescriptorBundle`] obtained from the schema registry.

use super::{EncodeError, SinkEncoder, SinkEncoderFactory};
use crate::codec::decoder::proto_bundle::{ProtoDescriptorBundle, ProtoFieldInfo};
use crate::model::{Collection, Tuple};
use crate::planner::physical::output_schema::{OutputSchema, OutputValueGetter};
use crate::planner::physical::ByIndexProjection;
use datatypes::{ConcreteDatatype, StructValue, TimestampValue, Value};
use std::sync::Arc;

// ── Protobuf wire type constants ──────────────────────────────────────

const WIRE_VARINT: u32 = 0;
const WIRE_FIXED64: u32 = 1;
const WIRE_LENGTH_DELIMITED: u32 = 2;
const WIRE_FIXED32: u32 = 5;

/// Encoder that serialises rows into protobuf binary payloads.
///
/// Each tuple row is encoded as an independent protobuf message. The
/// encoder does not add any framing envelope — begin/finish delivery
/// are no-ops. This matches the wire format produced by standard
/// protobuf serialisation (one top-level message per serialised byte
/// sequence).
pub struct ProtobufEncoder {
    bundle: Arc<ProtoDescriptorBundle>,
    output_schema: Option<Arc<OutputSchema>>,
}

impl ProtobufEncoder {
    /// Create a new protobuf encoder with the given pre-built descriptor bundle.
    pub fn new(bundle: Arc<ProtoDescriptorBundle>) -> Self {
        Self {
            bundle,
            output_schema: None,
        }
    }
}

impl SinkEncoderFactory for ProtobufEncoder {
    fn id(&self) -> &str {
        "protobuf"
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        Ok(Box::new(ProtobufEncoderRuntime::new(
            Arc::clone(&self.bundle),
            self.output_schema.clone(),
        )))
    }

    fn supports_index_lazy_materialization(&self) -> bool {
        false
    }

    fn with_by_index_projection(
        self: Arc<Self>,
        _spec: Arc<ByIndexProjection>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Err(EncodeError::Other(
            "index lazy materialization is not supported for protobuf encoder".to_string(),
        ))
    }

    fn with_output_schema(
        self: Arc<Self>,
        output_schema: Arc<OutputSchema>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Ok(Arc::new(Self {
            bundle: Arc::clone(&self.bundle),
            output_schema: Some(output_schema),
        }))
    }
}

struct ProtobufEncoderRuntime {
    bundle: Arc<ProtoDescriptorBundle>,
    output_schema: Option<Arc<OutputSchema>>,
}

impl ProtobufEncoderRuntime {
    fn new(bundle: Arc<ProtoDescriptorBundle>, output_schema: Option<Arc<OutputSchema>>) -> Self {
        Self {
            bundle,
            output_schema,
        }
    }
}

impl SinkEncoder for ProtobufEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        Ok(None)
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<bytes::Bytes>, EncodeError> {
        if record.num_rows() == 0 {
            return Ok(None);
        }

        let mut buf = Vec::new();
        for tuple in record.rows() {
            encode_tuple(&mut buf, tuple, &self.bundle, self.output_schema.as_deref())?;
        }
        Ok(Some(buf.into()))
    }

    fn finish_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        Ok(None)
    }

    fn abort_delivery(&mut self) {}
}

// ── Tuple encoding ────────────────────────────────────────────────────

/// Encode a single tuple as a protobuf message.
fn encode_tuple(
    buf: &mut Vec<u8>,
    tuple: &Tuple,
    bundle: &ProtoDescriptorBundle,
    output_schema: Option<&OutputSchema>,
) -> Result<(), EncodeError> {
    if let Some(schema) = output_schema {
        if let Some(output_mask) = tuple.output_mask() {
            return encode_tuple_with_output_mask(buf, tuple, bundle, schema, output_mask);
        }
        return encode_tuple_with_output_schema(buf, tuple, bundle, schema);
    }

    // Default path: iterate bundle columns, look up values in tuple by column name.
    for (field_number, info) in &bundle.field_map {
        let column_name = bundle
            .column_names
            .get(info.column_index)
            .map(|n| n.as_ref())
            .ok_or_else(|| {
                EncodeError::Other(format!(
                    "column index {} out of bounds in proto descriptor bundle",
                    info.column_index
                ))
            })?;

        let Some(value) = lookup_value_in_tuple(tuple, column_name) else {
            continue;
        };

        if value.is_null() {
            continue;
        }

        encode_field(buf, *field_number, value, info)?;
    }

    Ok(())
}

/// Encode a tuple using output schema columns (no output mask).
fn encode_tuple_with_output_schema(
    buf: &mut Vec<u8>,
    tuple: &Tuple,
    bundle: &ProtoDescriptorBundle,
    output_schema: &OutputSchema,
) -> Result<(), EncodeError> {
    for column in output_schema.columns.iter() {
        let Some(&field_number) = bundle.column_to_field.get(column.name.as_ref()) else {
            continue;
        };
        let Some(info) = bundle.field_map.get(&field_number) else {
            continue;
        };

        let value = resolve_output_value(tuple, &column.getter).unwrap_or(&Value::Null);
        if value.is_null() {
            continue;
        }

        encode_field(buf, field_number, value, info)?;
    }
    Ok(())
}

/// Encode a tuple with output mask semantics: only encode columns selected
/// by the mask, resolved through the output schema.
fn encode_tuple_with_output_mask(
    buf: &mut Vec<u8>,
    tuple: &Tuple,
    bundle: &ProtoDescriptorBundle,
    output_schema: &OutputSchema,
    output_mask: &[bool],
) -> Result<(), EncodeError> {
    if output_mask.len() != output_schema.columns.len() {
        return Err(EncodeError::Other(format!(
            "output_mask width {} does not match output schema width {}",
            output_mask.len(),
            output_schema.columns.len()
        )));
    }

    for (column, &selected) in output_schema.columns.iter().zip(output_mask.iter()) {
        if !selected {
            continue;
        }
        let Some(&field_number) = bundle.column_to_field.get(column.name.as_ref()) else {
            continue;
        };
        let Some(info) = bundle.field_map.get(&field_number) else {
            continue;
        };

        let value = resolve_output_value(tuple, &column.getter).unwrap_or(&Value::Null);
        if value.is_null() {
            continue;
        }

        encode_field(buf, field_number, value, info)?;
    }
    Ok(())
}

/// Look up a value in a tuple by column name across all messages and
/// affiliate columns.
fn lookup_value_in_tuple<'a>(tuple: &'a Tuple, column_name: &str) -> Option<&'a Value> {
    if let Some(aff) = tuple.affiliate() {
        if let Some(v) = aff.value(column_name) {
            return Some(v);
        }
    }
    for msg in tuple.messages() {
        if let Some(v) = msg.value(column_name) {
            return Some(v);
        }
    }
    None
}

/// Resolve a value from a tuple via an [`OutputValueGetter`].
fn resolve_output_value<'a>(tuple: &'a Tuple, getter: &OutputValueGetter) -> Option<&'a Value> {
    match getter {
        OutputValueGetter::Affiliate { column_name } => {
            tuple.affiliate().and_then(|aff| aff.value(column_name))
        }
        OutputValueGetter::MessageByName {
            source_name,
            column_name,
        } => tuple.messages().iter().find_map(|message| {
            if message.source() != source_name.as_ref() {
                return None;
            }
            message.value(column_name)
        }),
    }
}

// ── Field encoding ────────────────────────────────────────────────────

/// Encode a single protobuf field (scalar or repeated).
fn encode_field(
    buf: &mut Vec<u8>,
    field_number: u32,
    value: &Value,
    info: &ProtoFieldInfo,
) -> Result<(), EncodeError> {
    if info.is_repeated {
        encode_repeated_field(buf, field_number, value, info)
    } else {
        encode_scalar_field(buf, field_number, value, info)
    }
}

/// Encode a repeated field using packed encoding.
fn encode_repeated_field(
    buf: &mut Vec<u8>,
    field_number: u32,
    value: &Value,
    info: &ProtoFieldInfo,
) -> Result<(), EncodeError> {
    let Value::List(list) = value else {
        return Ok(());
    };

    let items = list.items();
    if items.is_empty() {
        return Ok(());
    }

    // Packed encoding: write a length-delimited blob containing all
    // element values.
    let elem_wire_type = element_wire_type(&info.datatype);

    // Pre-encode all elements into a temporary buffer.
    let mut data_buf = Vec::new();
    for item in items {
        encode_scalar_value(&mut data_buf, item, elem_wire_type, info)?;
    }

    if data_buf.is_empty() {
        return Ok(());
    }

    // Write the length-delimited field: tag, length, data.
    write_tag(buf, field_number, WIRE_LENGTH_DELIMITED);
    write_varint(buf, data_buf.len() as u64);
    buf.extend_from_slice(&data_buf);
    Ok(())
}

/// Encode a non-repeated scalar field with tag.
fn encode_scalar_field(
    buf: &mut Vec<u8>,
    field_number: u32,
    value: &Value,
    info: &ProtoFieldInfo,
) -> Result<(), EncodeError> {
    let wire_type = element_wire_type(&info.datatype);
    write_tag(buf, field_number, wire_type);
    encode_scalar_value(buf, value, wire_type, info)
}

/// Encode a scalar value to bytes without a tag.
fn encode_scalar_value(
    buf: &mut Vec<u8>,
    value: &Value,
    wire_type: u32,
    info: &ProtoFieldInfo,
) -> Result<(), EncodeError> {
    match wire_type {
        WIRE_VARINT => encode_varint_value(buf, value, info.datatype.clone(), info.is_zigzag),
        WIRE_FIXED32 => encode_fixed32_value(buf, value),
        WIRE_FIXED64 => encode_fixed64_value(buf, value),
        WIRE_LENGTH_DELIMITED => {
            encode_length_delimited_value(buf, value, &info.datatype, info.nested_bundle.as_deref())
        }
        _ => Err(EncodeError::Other(format!("unknown wire type {wire_type}"))),
    }
}

/// Determine the protobuf wire type for a given concrete datatype.
///
/// Uses VARINT as the default for all integer types, matching the
/// decoder's `default_wire_type` behaviour.  Proto fields declared as
/// `fixed32` / `sfixed32` / `fixed64` / `sfixed64` are indistinguishable
/// from their varint counterparts at the ConcreteDatatype level; a future
/// iteration may track the original proto type in `ProtoFieldInfo` so the
/// encoder can emit the exact wire type.
fn element_wire_type(dt: &ConcreteDatatype) -> u32 {
    match dt {
        ConcreteDatatype::Float32(_) => WIRE_FIXED32,
        ConcreteDatatype::Float64(_) => WIRE_FIXED64,
        ConcreteDatatype::String(_)
        | ConcreteDatatype::Bytes(_)
        | ConcreteDatatype::Struct(_)
        | ConcreteDatatype::Timestamp(_)
        | ConcreteDatatype::List(_) => WIRE_LENGTH_DELIMITED,
        _ => WIRE_VARINT,
    }
}

// ── Wire encoding primitives ──────────────────────────────────────────

fn write_tag(buf: &mut Vec<u8>, field_number: u32, wire_type: u32) {
    let tag = (field_number << 3) | wire_type;
    write_varint(buf, tag as u64);
}

fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn write_fixed32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_fixed64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_length_delimited(buf: &mut Vec<u8>, data: &[u8]) {
    write_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

// ── Value encoders ────────────────────────────────────────────────────

fn encode_varint_value(
    buf: &mut Vec<u8>,
    value: &Value,
    dt: ConcreteDatatype,
    is_zigzag: bool,
) -> Result<(), EncodeError> {
    if is_zigzag {
        let z: i64 = match value {
            Value::Int32(v) => *v as i64,
            Value::Int64(v) => *v,
            Value::Uint32(v) => *v as i64,
            Value::Uint64(v) => *v as i64,
            _ => {
                return Err(EncodeError::Other(format!(
                    "expected integer for zigzag-encoded field, got {value:?}"
                )));
            }
        };
        // Correct zigzag: (n << 1) ^ (n >> 63) for sint64
        let encoded = ((z as u64) << 1) ^ ((z >> 63) as u64);
        write_varint(buf, encoded);
        return Ok(());
    }

    match (&dt, value) {
        (ConcreteDatatype::Bool(_), Value::Bool(v)) => {
            write_varint(buf, if *v { 1 } else { 0 });
        }
        (_, Value::Int8(v)) => write_varint(buf, *v as u64),
        (_, Value::Int16(v)) => write_varint(buf, *v as u64),
        (_, Value::Int32(v)) => write_varint(buf, *v as u64),
        (_, Value::Int64(v)) => write_varint(buf, *v as u64),
        (_, Value::Uint8(v)) => write_varint(buf, *v as u64),
        (_, Value::Uint16(v)) => write_varint(buf, *v as u64),
        (_, Value::Uint32(v)) => write_varint(buf, *v as u64),
        (_, Value::Uint64(v)) => write_varint(buf, *v),
        _ => {
            return Err(EncodeError::Other(format!(
                "expected varint-compatible value, got {value:?} for datatype {dt:?}"
            )));
        }
    }
    Ok(())
}

fn encode_fixed32_value(buf: &mut Vec<u8>, value: &Value) -> Result<(), EncodeError> {
    match value {
        Value::Float32(v) => write_fixed32(buf, v.to_bits()),
        Value::Int32(v) => write_fixed32(buf, *v as u32),
        Value::Uint32(v) => write_fixed32(buf, *v),
        Value::Int8(v) => write_fixed32(buf, *v as i32 as u32),
        Value::Int16(v) => write_fixed32(buf, *v as i32 as u32),
        Value::Uint8(v) => write_fixed32(buf, *v as u32),
        Value::Uint16(v) => write_fixed32(buf, *v as u32),
        _ => {
            return Err(EncodeError::Other(format!(
                "expected fixed32-compatible value, got {value:?}"
            )));
        }
    }
    Ok(())
}

fn encode_fixed64_value(buf: &mut Vec<u8>, value: &Value) -> Result<(), EncodeError> {
    match value {
        Value::Float64(v) => write_fixed64(buf, v.to_bits()),
        Value::Int64(v) => write_fixed64(buf, *v as u64),
        Value::Uint64(v) => write_fixed64(buf, *v),
        // Upcast smaller integers.
        Value::Int8(v) => write_fixed64(buf, *v as i64 as u64),
        Value::Int16(v) => write_fixed64(buf, *v as i64 as u64),
        Value::Int32(v) => write_fixed64(buf, *v as i64 as u64),
        Value::Uint8(v) => write_fixed64(buf, *v as u64),
        Value::Uint16(v) => write_fixed64(buf, *v as u64),
        Value::Uint32(v) => write_fixed64(buf, *v as u64),
        _ => {
            return Err(EncodeError::Other(format!(
                "expected fixed64-compatible value, got {value:?}"
            )));
        }
    }
    Ok(())
}

fn encode_length_delimited_value(
    buf: &mut Vec<u8>,
    value: &Value,
    dt: &ConcreteDatatype,
    nested_bundle: Option<&ProtoDescriptorBundle>,
) -> Result<(), EncodeError> {
    match (dt, value) {
        (ConcreteDatatype::String(_), Value::String(s)) => {
            write_length_delimited(buf, s.as_bytes());
            Ok(())
        }
        (ConcreteDatatype::Bytes(_), Value::Bytes(b)) => {
            write_length_delimited(buf, b.as_ref());
            Ok(())
        }
        (ConcreteDatatype::Timestamp(_), Value::Timestamp(ts)) => {
            encode_timestamp_value(buf, *ts);
            Ok(())
        }
        (ConcreteDatatype::Struct(_), Value::Struct(struct_val)) => {
            encode_struct_value(buf, struct_val, nested_bundle)
        }
        _ => Err(EncodeError::Other(format!(
            "expected length-delimited-compatible value for {dt:?}, got {value:?}"
        ))),
    }
}

/// Encode a google.protobuf.Timestamp as a sub-message.
fn encode_timestamp_value(buf: &mut Vec<u8>, ts: TimestampValue) {
    let epoch_micros = ts.epoch_micros();
    let seconds = epoch_micros.div_euclid(1_000_000);
    let nanos = (epoch_micros.rem_euclid(1_000_000) * 1_000) as i32;

    // Pre-encode the sub-message.
    let mut sub = Vec::new();
    // field 1 (seconds, int64, VARINT)
    write_tag(&mut sub, 1, WIRE_VARINT);
    write_varint(&mut sub, seconds as u64);
    // field 2 (nanos, int32, VARINT)
    if nanos != 0 {
        write_tag(&mut sub, 2, WIRE_VARINT);
        write_varint(&mut sub, nanos as u64);
    }

    write_length_delimited(buf, &sub);
}

/// Encode a nested struct as a sub-message.
fn encode_struct_value(
    buf: &mut Vec<u8>,
    struct_val: &StructValue,
    nested_bundle: Option<&ProtoDescriptorBundle>,
) -> Result<(), EncodeError> {
    let bundle = nested_bundle.ok_or_else(|| {
        EncodeError::Other("struct field has no nested descriptor bundle for encoding".to_string())
    })?;

    let mut sub = Vec::new();
    for (field_number, info) in &bundle.field_map {
        let column_name = bundle
            .column_names
            .get(info.column_index)
            .map(|n| n.as_ref())
            .ok_or_else(|| {
                EncodeError::Other(format!(
                    "nested column index {} out of bounds",
                    info.column_index
                ))
            })?;

        let Some(value) = struct_val.get_field(column_name) else {
            continue;
        };
        if value.is_null() {
            continue;
        }

        encode_field(&mut sub, *field_number, value, info)?;
    }

    write_length_delimited(buf, &sub);
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decoder::proto_bundle::ProtoDescriptorBundle;
    use crate::codec::decoder::RecordDecoder;
    use crate::model::{batch_from_columns_simple, Collection};
    use datatypes::{
        BooleanType, BytesType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type,
        Int32Type, Int64Type, ListType, ListValue, Schema, StringType, StructField, StructType,
        StructValue, TimestampType, TimestampValue,
    };
    use std::collections::BTreeMap;

    fn make_bundle(
        columns: &[ColumnSchema],
        field_infos: Vec<(u32, ProtoFieldInfo)>,
    ) -> Arc<ProtoDescriptorBundle> {
        let field_map: BTreeMap<u32, ProtoFieldInfo> = field_infos.into_iter().collect();
        let column_to_field: BTreeMap<String, u32> = columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                field_map
                    .iter()
                    .find(|(_, info)| info.column_index == i)
                    .map(|(&fnbr, _)| (col.name.clone(), fnbr))
            })
            .collect();
        let column_names: Vec<Arc<str>> =
            columns.iter().map(|c| Arc::from(c.name.as_str())).collect();
        let column_count = columns.len();
        Arc::new(ProtoDescriptorBundle::new(
            field_map,
            column_to_field,
            column_count,
            column_names,
        ))
    }

    fn encode_one(encoder: &ProtobufEncoder, collection: &dyn Collection) -> Vec<u8> {
        let mut runtime = encoder.start_encoder().expect("encoder runtime");
        assert!(runtime.begin_delivery().expect("begin").is_none());
        let chunk = runtime.append(collection).expect("append");
        assert!(runtime.finish_delivery().expect("finish").is_none());
        chunk.expect("expected output").to_vec()
    }

    // ── Scalar type tests ─────────────────────────────────────────────

    #[test]
    fn encode_int32() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int32(Int32Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "a".to_string(),
            vec![Value::Int32(42)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 1, varint = (1 << 3) | 0 = 0x08, value 42 = 0x2a
        assert_eq!(data.as_slice(), &[0x08, 0x2a]);
    }

    #[test]
    fn encode_int64() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                2,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int64(Int64Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "b".to_string(),
            vec![Value::Int64(300)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 2, varint: tag = 0x10, value 300 = 0xac 0x02
        assert_eq!(data.as_slice(), &[0x10, 0xac, 0x02]);
    }

    #[test]
    fn encode_string() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "s".to_string(),
            ConcreteDatatype::String(StringType),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::String(StringType),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "s".to_string(),
            vec![Value::String("hello".to_string())],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 1, LEN: tag = 0x0a, len = 5, "hello"
        assert_eq!(data.as_slice(), &[0x0a, 0x05, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn encode_bool() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "flag".to_string(),
            ConcreteDatatype::Bool(BooleanType),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                3,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Bool(BooleanType),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "flag".to_string(),
            vec![Value::Bool(true)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 3, varint: tag = 0x18, value = 1
        assert_eq!(data.as_slice(), &[0x18, 0x01]);
    }

    #[test]
    fn encode_float32() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "f".to_string(),
            ConcreteDatatype::Float32(Float32Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Float32(Float32Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let v = 1.5f32;
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "f".to_string(),
            vec![Value::Float32(v)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 1, fixed32: tag = 0x0d, value = 1.5 LE
        assert_eq!(data[0], 0x0d);
        assert_eq!(&data[1..5], &v.to_le_bytes());
    }

    #[test]
    fn encode_float64() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "d".to_string(),
            ConcreteDatatype::Float64(Float64Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Float64(Float64Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let v = 2.5f64;
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "d".to_string(),
            vec![Value::Float64(v)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        assert_eq!(data[0], 0x09);
        assert_eq!(&data[1..9], &v.to_le_bytes());
    }

    #[test]
    fn encode_bytes() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "payload".to_string(),
            ConcreteDatatype::Bytes(BytesType),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Bytes(BytesType),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let raw = b"\x00\x01\x02";
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "payload".to_string(),
            vec![Value::Bytes(bytes::Bytes::from_static(raw))],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        assert_eq!(data[0], 0x0a);
        assert_eq!(data[1], 3);
        assert_eq!(&data[2..], raw);
    }

    #[test]
    fn encode_timestamp() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "ts".to_string(),
            ConcreteDatatype::Timestamp(TimestampType),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Timestamp(TimestampType),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle.clone());
        let ts = TimestampValue::parse_rfc3339("2025-01-15T00:00:00Z").expect("valid ts");
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "ts".to_string(),
            vec![Value::Timestamp(ts)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // Should be a valid protobuf message with Timestamp sub-message.
        // Decode it with the existing ProtobufDecoder.
        let decoder_bundle = bundle.clone();
        let decoder = crate::codec::ProtobufDecoder::new(
            "t".to_string(),
            Arc::new(Schema::new(cols)),
            decoder_bundle,
        );
        let batch = decoder.decode(&data).expect("decode round-trip");
        let rows = batch.into_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].value_by_name("t", "ts"),
            Some(&Value::Timestamp(ts))
        );
    }

    #[test]
    fn encode_zigzag_sint32() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "z".to_string(),
            ConcreteDatatype::Int32(Int32Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: false,
                    is_zigzag: true,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);

        for (input, expected_zigzag) in [(-1i32, 1u64), (1i32, 2u64)] {
            let batch = batch_from_columns_simple(vec![(
                "t".to_string(),
                "z".to_string(),
                vec![Value::Int32(input)],
            )])
            .expect("batch");
            let data = encode_one(&encoder, &batch);
            assert_eq!(data[0], 0x08); // field 1, varint
                                       // Extract the varint value
            let mut val: u64 = 0;
            let mut shift = 0;
            for &b in &data[1..] {
                val |= ((b & 0x7f) as u64) << shift;
                shift += 7;
                if b & 0x80 == 0 {
                    break;
                }
            }
            assert_eq!(val, expected_zigzag, "zigzag({input}) mismatch");
        }
    }

    // ── Repeated / packed tests ───────────────────────────────────────

    #[test]
    fn encode_repeated_int32_packed() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "vals".to_string(),
            ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)))),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                2,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: true,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let list = ListValue::new(
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
            Arc::new(ConcreteDatatype::Int32(Int32Type)),
        );
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "vals".to_string(),
            vec![Value::List(list)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 2, LEN: tag = 0x12, len = 3, values = 1, 2, 3
        assert_eq!(data.as_slice(), &[0x12, 0x03, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn encode_empty_repeated_is_omitted() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "vals".to_string(),
            ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Int32(Int32Type)))),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: true,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let list = ListValue::new(vec![], Arc::new(ConcreteDatatype::Int32(Int32Type)));
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "vals".to_string(),
            vec![Value::List(list)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // Empty repeated fields should produce no output.
        assert!(data.is_empty());
    }

    // ── Null / missing column handling ────────────────────────────────

    #[test]
    fn null_column_is_omitted() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int32(Int32Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let batch =
            batch_from_columns_simple(vec![("t".to_string(), "a".to_string(), vec![Value::Null])])
                .expect("batch");

        let data = encode_one(&encoder, &batch);
        assert!(data.is_empty());
    }

    #[test]
    fn multiple_rows_encode_consecutive_messages() {
        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int32(Int32Type),
        )];
        let bundle = make_bundle(
            &cols,
            vec![(
                1,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "a".to_string(),
            vec![Value::Int32(10), Value::Int32(20)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // Two messages: [0x08, 0x0a] [0x08, 0x14]
        assert_eq!(data.as_slice(), &[0x08, 0x0a, 0x08, 0x14]);
    }

    // ── Nested struct test ────────────────────────────────────────────

    #[test]
    fn encode_nested_struct() {
        let inner_type = StructType::new(Arc::new(vec![
            StructField::new("x".to_string(), ConcreteDatatype::Int32(Int32Type), false),
            StructField::new("y".to_string(), ConcreteDatatype::String(StringType), true),
        ]));

        let cols = vec![ColumnSchema::new(
            "t".to_string(),
            "nested".to_string(),
            ConcreteDatatype::Struct(inner_type.clone()),
        )];

        let nested_columns = vec![
            ColumnSchema::new(
                "".to_string(),
                "x".to_string(),
                ConcreteDatatype::Int32(Int32Type),
            ),
            ColumnSchema::new(
                "".to_string(),
                "y".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ];
        let nested_column_names: Vec<Arc<str>> = nested_columns
            .iter()
            .map(|c| Arc::from(c.name.as_str()))
            .collect();
        let nested_field_map = BTreeMap::from([
            (
                1u32,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Int32(Int32Type),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            ),
            (
                2u32,
                ProtoFieldInfo {
                    column_index: 1,
                    datatype: ConcreteDatatype::String(StringType),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: None,
                },
            ),
        ]);
        let nested_column_to_field =
            BTreeMap::from([("x".to_string(), 1u32), ("y".to_string(), 2u32)]);
        let nested_bundle = Arc::new(ProtoDescriptorBundle::new(
            nested_field_map,
            nested_column_to_field,
            2,
            nested_column_names,
        ));

        let bundle = make_bundle(
            &cols,
            vec![(
                3,
                ProtoFieldInfo {
                    column_index: 0,
                    datatype: ConcreteDatatype::Struct(inner_type.clone()),
                    is_repeated: false,
                    is_zigzag: false,
                    nested_bundle: Some(nested_bundle.clone()),
                },
            )],
        );
        let encoder = ProtobufEncoder::new(bundle);

        let struct_val = StructValue::new(
            vec![Value::Int32(7), Value::String("hi".to_string())],
            inner_type,
        );
        let batch = batch_from_columns_simple(vec![(
            "t".to_string(),
            "nested".to_string(),
            vec![Value::Struct(struct_val)],
        )])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        // field 3, LEN, sub-message with x=7 (0x08 0x07) and y="hi" (0x12 0x02 0x68 0x69)
        assert_eq!(
            data.as_slice(),
            &[0x1a, 0x06, 0x08, 0x07, 0x12, 0x02, b'h', b'i']
        );
    }

    // ── Round-trip with ProtobufDecoder ────────────────────────────────

    #[test]
    fn round_trip_through_decoder() {
        use crate::codec::ProtobufDecoder;

        let cols = vec![
            ColumnSchema::new(
                "t".to_string(),
                "id".to_string(),
                ConcreteDatatype::Int32(Int32Type),
            ),
            ColumnSchema::new(
                "t".to_string(),
                "name".to_string(),
                ConcreteDatatype::String(StringType),
            ),
            ColumnSchema::new(
                "t".to_string(),
                "score".to_string(),
                ConcreteDatatype::Float64(Float64Type),
            ),
        ];
        let bundle = make_bundle(
            &cols,
            vec![
                (
                    1,
                    ProtoFieldInfo {
                        column_index: 0,
                        datatype: ConcreteDatatype::Int32(Int32Type),
                        is_repeated: false,
                        is_zigzag: false,
                        nested_bundle: None,
                    },
                ),
                (
                    2,
                    ProtoFieldInfo {
                        column_index: 1,
                        datatype: ConcreteDatatype::String(StringType),
                        is_repeated: false,
                        is_zigzag: false,
                        nested_bundle: None,
                    },
                ),
                (
                    3,
                    ProtoFieldInfo {
                        column_index: 2,
                        datatype: ConcreteDatatype::Float64(Float64Type),
                        is_repeated: false,
                        is_zigzag: false,
                        nested_bundle: None,
                    },
                ),
            ],
        );

        let encoder = ProtobufEncoder::new(bundle.clone());
        let schema = Arc::new(Schema::new(cols.clone()));
        let decoder = ProtobufDecoder::new("t", schema, bundle);

        let batch = batch_from_columns_simple(vec![
            ("t".to_string(), "id".to_string(), vec![Value::Int32(1)]),
            (
                "t".to_string(),
                "name".to_string(),
                vec![Value::String("alice".to_string())],
            ),
            (
                "t".to_string(),
                "score".to_string(),
                vec![Value::Float64(9.5)],
            ),
        ])
        .expect("batch");

        let data = encode_one(&encoder, &batch);
        let decoded = decoder.decode(&data).expect("decode round-trip");
        let tuples = decoded.into_rows();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].value_by_name("t", "id"), Some(&Value::Int64(1)));
        assert_eq!(
            tuples[0].value_by_name("t", "name"),
            Some(&Value::String("alice".to_string()))
        );
        assert_eq!(
            tuples[0].value_by_name("t", "score"),
            Some(&Value::Float64(9.5))
        );
    }
}
