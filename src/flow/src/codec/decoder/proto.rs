//! Protobuf wire-format decoder implementing [`RecordDecoder`].
//!
//! ## Column pruning
//!
//! When a [`DecodeProjection`] is supplied, only the requested top-level columns are decoded.
//! Unwanted fields are skipped at the wire level: the decoder reads the tag (field number
//! + wire type) and then either decodes the value or advances the cursor past it without
//!   materialising any [`Value`].
//!
//! Nested struct field pruning is deferred to a future iteration.

use crate::codec::decoder::proto_bundle::{ProtoDescriptorBundle, ProtoFieldInfo};
use crate::codec::{CodecError, RecordDecoder};
use crate::model::{Message, RecordBatch, Tuple};
use crate::planner::decode_projection::DecodeProjection;
use bytes::Bytes;
use datatypes::{
    ConcreteDatatype, ListValue, Schema, StructType, StructValue, TimestampValue, Value,
};
use std::collections::HashSet;
use std::sync::Arc;

// ── Protobuf wire type constants ──────────────────────────────────────

const WIRE_VARINT: u32 = 0;
const WIRE_FIXED64: u32 = 1;
const WIRE_LENGTH_DELIMITED: u32 = 2;
const WIRE_FIXED32: u32 = 5;

/// Maximum nesting depth for embedded messages (defence against malformed input).
const MAX_NESTING_DEPTH: usize = 32;

// ── Public API ─────────────────────────────────────────────────────────

/// Decoder that deserialises protobuf binary payloads into [`RecordBatch`] rows.
pub struct ProtobufDecoder {
    stream_name: Arc<str>,
    schema: Arc<Schema>,
    schema_keys: Arc<[Arc<str>]>,
    bundle: Arc<ProtoDescriptorBundle>,
}

impl ProtobufDecoder {
    pub fn new(
        stream_name: impl Into<String>,
        _schema: Arc<Schema>,
        bundle: Arc<ProtoDescriptorBundle>,
    ) -> Self {
        let stream_name: Arc<str> = Arc::from(stream_name.into());
        let schema_keys: Arc<[Arc<str>]> = Arc::from(bundle.column_names.clone());
        Self {
            stream_name,
            schema: _schema,
            schema_keys,
            bundle,
        }
    }

    /// Build a `HashSet<u32>` of field numbers to decode from the projection.
    fn keep_set(&self, projection: Option<&DecodeProjection>) -> Option<HashSet<u32>> {
        let proj = projection?;
        let mut keep = HashSet::new();
        for col_name in proj.columns().keys() {
            if let Some(fnbr) = self.bundle.field_number_for_column(col_name) {
                keep.insert(fnbr);
            }
        }
        Some(keep)
    }

    /// Decode a single protobuf top-level message into a [`Tuple`].
    fn decode_one(
        &self,
        payload: &[u8],
        keep_set: Option<&HashSet<u32>>,
    ) -> Result<Tuple, CodecError> {
        let column_count = self.bundle.column_count;
        let mut values: Vec<Arc<Value>> =
            (0..column_count).map(|_| Arc::new(Value::Null)).collect();
        let mut list_bufs: Vec<Vec<Value>> = vec![Vec::new(); column_count];
        let mut cursor = 0usize;

        while cursor < payload.len() {
            let tag = read_varint(payload, &mut cursor)?;
            let field_number = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u32;

            let Some(info) = self.bundle.field_map.get(&field_number) else {
                skip_field(payload, &mut cursor, wire_type)?;
                continue;
            };

            // Column pruning: skip unwanted fields.
            if let Some(keep) = keep_set {
                if !keep.contains(&field_number) {
                    skip_field(payload, &mut cursor, wire_type)?;
                    continue;
                }
            }

            let ci = info.column_index;

            if info.is_repeated {
                // Packed: single length-delimited blob of concatenated values.
                if wire_type == WIRE_LENGTH_DELIMITED {
                    let blob = read_length_delimited(payload, &mut cursor)?;
                    decode_packed_elements(
                        blob,
                        info,
                        &mut list_bufs[ci],
                        info.nested_bundle.as_deref(),
                    )?;
                } else {
                    let val = decode_one_field(
                        payload,
                        &mut cursor,
                        wire_type,
                        info,
                        info.nested_bundle.as_deref(),
                        0,
                    )?;
                    list_bufs[ci].push(val);
                }
            } else {
                let val = decode_one_field(
                    payload,
                    &mut cursor,
                    wire_type,
                    info,
                    info.nested_bundle.as_deref(),
                    0,
                )?;
                values[ci] = Arc::new(val);
            }
        }

        // Materialise accumulated lists.
        for (ci, buf) in list_bufs.iter_mut().enumerate() {
            if buf.is_empty() {
                continue;
            }
            let elem_dt = element_type_of_column(&self.schema, ci);
            values[ci] = Arc::new(Value::List(ListValue::new(
                std::mem::take(buf),
                Arc::new(elem_dt),
            )));
        }

        let message = Arc::new(Message::new_shared_keys(
            Arc::clone(&self.stream_name),
            Arc::clone(&self.schema_keys),
            values,
        ));
        Ok(Tuple::new(vec![message]))
    }
}

impl RecordDecoder for ProtobufDecoder {
    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        let keep = self.keep_set(projection);
        let tuple = self.decode_one(payload, keep.as_ref())?;
        Ok(RecordBatch::new(vec![tuple])?)
    }
}

// ── Wire-format primitives ────────────────────────────────────────────

fn read_varint(data: &[u8], cursor: &mut usize) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *cursor >= data.len() {
            return Err(CodecError::Other(
                "unexpected end of data reading varint".into(),
            ));
        }
        let byte = data[*cursor];
        *cursor += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(CodecError::Other("varint too long (>= 10 bytes)".into()));
        }
    }
}

fn read_varint_zigzag(data: &[u8], cursor: &mut usize) -> Result<i64, CodecError> {
    let raw = read_varint(data, cursor)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

fn read_fixed32(data: &[u8], cursor: &mut usize) -> Result<u32, CodecError> {
    if *cursor + 4 > data.len() {
        return Err(CodecError::Other(
            "unexpected end of data reading fixed32".into(),
        ));
    }
    let val = u32::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(val)
}

fn read_fixed64(data: &[u8], cursor: &mut usize) -> Result<u64, CodecError> {
    if *cursor + 8 > data.len() {
        return Err(CodecError::Other(
            "unexpected end of data reading fixed64".into(),
        ));
    }
    let val = u64::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
        data[*cursor + 4],
        data[*cursor + 5],
        data[*cursor + 6],
        data[*cursor + 7],
    ]);
    *cursor += 8;
    Ok(val)
}

fn read_length_delimited<'a>(data: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], CodecError> {
    let len = read_varint(data, cursor)? as usize;
    if *cursor + len > data.len() {
        return Err(CodecError::Other(format!(
            "length-delimited field declares {len} bytes but only {} remain",
            data.len() - *cursor
        )));
    }
    let start = *cursor;
    *cursor += len;
    Ok(&data[start..*cursor])
}

fn skip_field(data: &[u8], cursor: &mut usize, wire_type: u32) -> Result<(), CodecError> {
    match wire_type {
        WIRE_VARINT => {
            read_varint(data, cursor)?;
        }
        WIRE_FIXED64 => {
            if *cursor + 8 > data.len() {
                return Err(CodecError::Other(
                    "unexpected end of data skipping fixed64".into(),
                ));
            }
            *cursor += 8;
        }
        WIRE_LENGTH_DELIMITED => {
            let len = read_varint(data, cursor)? as usize;
            if *cursor + len > data.len() {
                return Err(CodecError::Other(format!(
                    "cannot skip {len} bytes: only {} remain",
                    data.len() - *cursor
                )));
            }
            *cursor += len;
        }
        WIRE_FIXED32 => {
            if *cursor + 4 > data.len() {
                return Err(CodecError::Other(
                    "unexpected end of data skipping fixed32".into(),
                ));
            }
            *cursor += 4;
        }
        other => {
            return Err(CodecError::Other(format!("unknown wire type: {other}")));
        }
    }
    Ok(())
}

// ── Value decoders ─────────────────────────────────────────────────────

/// Decode a single protobuf field value into a [`Value`].
fn decode_one_field(
    data: &[u8],
    cursor: &mut usize,
    wire_type: u32,
    info: &ProtoFieldInfo,
    nested_bundle: Option<&ProtoDescriptorBundle>,
    depth: usize,
) -> Result<Value, CodecError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(CodecError::Other(
            "maximum protobuf nesting depth exceeded".into(),
        ));
    }

    match wire_type {
        WIRE_VARINT => decode_varint_val(data, cursor, &info.datatype, info.is_zigzag),
        WIRE_FIXED32 => decode_fixed32_val(data, cursor, &info.datatype),
        WIRE_FIXED64 => decode_fixed64_val(data, cursor, &info.datatype),
        WIRE_LENGTH_DELIMITED => {
            decode_len_delimited_val(data, cursor, &info.datatype, nested_bundle, depth)
        }
        other => Err(CodecError::Other(format!(
            "unexpected wire type {other} for datatype {:?}",
            info.datatype
        ))),
    }
}

fn decode_varint_val(
    data: &[u8],
    cursor: &mut usize,
    dt: &ConcreteDatatype,
    is_zigzag: bool,
) -> Result<Value, CodecError> {
    if is_zigzag {
        let z = read_varint_zigzag(data, cursor)?;
        return match dt {
            ConcreteDatatype::Int32(_) => Ok(Value::Int64(z as i32 as i64)),
            ConcreteDatatype::Int64(_) => Ok(Value::Int64(z)),
            _ => Err(CodecError::Other(format!(
                "zigzag encoding unexpected for datatype {dt:?}"
            ))),
        };
    }

    let raw = read_varint(data, cursor)?;
    match dt {
        ConcreteDatatype::Bool(_) => Ok(Value::Bool(raw != 0)),
        ConcreteDatatype::Int32(_) => Ok(Value::Int64((raw as i32) as i64)),
        ConcreteDatatype::Int64(_) => Ok(Value::Int64(raw as i64)),
        ConcreteDatatype::Uint32(_) => Ok(Value::Uint64((raw as u32) as u64)),
        ConcreteDatatype::Uint64(_) => Ok(Value::Uint64(raw)),
        _ => Err(CodecError::Other(format!(
            "varint wire type unexpected for datatype {dt:?}"
        ))),
    }
}

fn decode_fixed32_val(
    data: &[u8],
    cursor: &mut usize,
    dt: &ConcreteDatatype,
) -> Result<Value, CodecError> {
    let raw = read_fixed32(data, cursor)?;
    match dt {
        ConcreteDatatype::Float32(_) => {
            Ok(Value::Float64(f32::from_le_bytes(raw.to_le_bytes()) as f64))
        }
        ConcreteDatatype::Int32(_) => Ok(Value::Int64(raw as i32 as i64)),
        ConcreteDatatype::Uint32(_) => Ok(Value::Uint64(raw as u64)),
        _ => Err(CodecError::Other(format!(
            "fixed32 wire type unexpected for datatype {dt:?}"
        ))),
    }
}

fn decode_fixed64_val(
    data: &[u8],
    cursor: &mut usize,
    dt: &ConcreteDatatype,
) -> Result<Value, CodecError> {
    let raw = read_fixed64(data, cursor)?;
    match dt {
        ConcreteDatatype::Float64(_) => Ok(Value::Float64(f64::from_le_bytes(raw.to_le_bytes()))),
        ConcreteDatatype::Int64(_) => Ok(Value::Int64(raw as i64)),
        ConcreteDatatype::Uint64(_) => Ok(Value::Uint64(raw)),
        _ => Err(CodecError::Other(format!(
            "fixed64 wire type unexpected for datatype {dt:?}"
        ))),
    }
}

fn decode_len_delimited_val(
    data: &[u8],
    cursor: &mut usize,
    dt: &ConcreteDatatype,
    nested_bundle: Option<&ProtoDescriptorBundle>,
    depth: usize,
) -> Result<Value, CodecError> {
    match dt {
        ConcreteDatatype::String(_) => {
            let bytes = read_length_delimited(data, cursor)?;
            String::from_utf8(bytes.to_vec())
                .map(Value::String)
                .map_err(|e| CodecError::Other(format!("invalid UTF-8 in proto string: {e}")))
        }
        ConcreteDatatype::Bytes(_) => {
            let bytes = read_length_delimited(data, cursor)?;
            Ok(Value::Bytes(Bytes::copy_from_slice(bytes)))
        }
        ConcreteDatatype::Timestamp(_) => {
            let blob = read_length_delimited(data, cursor)?;
            decode_timestamp(blob)
        }
        ConcreteDatatype::Struct(struct_type) => {
            let blob = read_length_delimited(data, cursor)?;
            let bundle = nested_bundle.ok_or_else(|| {
                CodecError::Other("struct field has no nested descriptor bundle".into())
            })?;
            decode_struct(blob, bundle, struct_type, depth + 1)
        }
        _ => Err(CodecError::Other(format!(
            "length-delimited wire type unexpected for datatype {dt:?}"
        ))),
    }
}

fn decode_timestamp(blob: &[u8]) -> Result<Value, CodecError> {
    let mut cursor = 0usize;
    let mut seconds: i64 = 0;
    let mut nanos: i32 = 0;
    while cursor < blob.len() {
        let tag = read_varint(blob, &mut cursor)?;
        let fnbr = (tag >> 3) as u32;
        let wt = (tag & 0x07) as u32;
        match fnbr {
            1 if wt == WIRE_VARINT => seconds = read_varint(blob, &mut cursor)? as i64,
            2 if wt == WIRE_VARINT => nanos = read_varint(blob, &mut cursor)? as i32,
            _ => skip_field(blob, &mut cursor, wt)?,
        }
    }
    let epoch_micros = seconds
        .checked_mul(1_000_000)
        .and_then(|s| s.checked_add(nanos as i64 / 1_000))
        .ok_or_else(|| CodecError::Other("Timestamp overflow".into()))?;
    Ok(Value::Timestamp(TimestampValue::from_epoch_micros(
        epoch_micros,
    )))
}

/// Recursively decode a sub-message into a [`StructValue`].
fn decode_struct(
    blob: &[u8],
    bundle: &ProtoDescriptorBundle,
    struct_type: &StructType,
    depth: usize,
) -> Result<Value, CodecError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(CodecError::Other(
            "maximum protobuf nesting depth exceeded".into(),
        ));
    }

    let field_count = struct_type.fields().len();
    let mut values: Vec<Value> = vec![Value::Null; field_count];
    let mut list_bufs: Vec<Vec<Value>> = vec![Vec::new(); field_count];
    let mut cursor = 0usize;

    while cursor < blob.len() {
        let tag = read_varint(blob, &mut cursor)?;
        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u32;

        let Some(info) = bundle.field_map.get(&field_number) else {
            skip_field(blob, &mut cursor, wire_type)?;
            continue;
        };

        let ci = info.column_index;

        if info.is_repeated {
            if wire_type == WIRE_LENGTH_DELIMITED {
                let inner_blob = read_length_delimited(blob, &mut cursor)?;
                decode_packed_elements(
                    inner_blob,
                    info,
                    &mut list_bufs[ci],
                    info.nested_bundle.as_deref(),
                )?;
            } else {
                let val = decode_one_field(
                    blob,
                    &mut cursor,
                    wire_type,
                    info,
                    info.nested_bundle.as_deref(),
                    depth + 1,
                )?;
                list_bufs[ci].push(val);
            }
        } else {
            let val = decode_one_field(
                blob,
                &mut cursor,
                wire_type,
                info,
                info.nested_bundle.as_deref(),
                depth + 1,
            )?;
            values[ci] = val;
        }
    }

    for (ci, buf) in list_bufs.iter_mut().enumerate() {
        if buf.is_empty() {
            continue;
        }
        let elem_dt = struct_type
            .fields()
            .get(ci)
            .map(|f| f.data_type().clone())
            .unwrap_or(ConcreteDatatype::Null);
        values[ci] = Value::List(ListValue::new(std::mem::take(buf), Arc::new(elem_dt)));
    }

    Ok(Value::Struct(StructValue::new(values, struct_type.clone())))
}

/// Decode elements from a packed length-delimited blob into a list accumulator.
fn decode_packed_elements(
    blob: &[u8],
    info: &ProtoFieldInfo,
    buf: &mut Vec<Value>,
    nested_bundle: Option<&ProtoDescriptorBundle>,
) -> Result<(), CodecError> {
    let elem_wt = default_wire_type(&info.datatype);
    let mut cursor = 0usize;
    while cursor < blob.len() {
        let val = decode_one_field(blob, &mut cursor, elem_wt, info, nested_bundle, 0)?;
        buf.push(val);
    }
    Ok(())
}

fn default_wire_type(dt: &ConcreteDatatype) -> u32 {
    match dt {
        ConcreteDatatype::Float32(_) => WIRE_FIXED32,
        ConcreteDatatype::Float64(_) => WIRE_FIXED64,
        ConcreteDatatype::String(_) | ConcreteDatatype::Bytes(_) => WIRE_LENGTH_DELIMITED,
        ConcreteDatatype::Struct(_) | ConcreteDatatype::Timestamp(_) => WIRE_LENGTH_DELIMITED,
        _ => WIRE_VARINT,
    }
}

fn element_type_of_column(schema: &Schema, column_index: usize) -> ConcreteDatatype {
    schema
        .column_schemas()
        .get(column_index)
        .and_then(|col| match &col.data_type {
            ConcreteDatatype::List(lt) => Some(lt.item_type().clone()),
            _ => None,
        })
        .unwrap_or(ConcreteDatatype::Null)
}
