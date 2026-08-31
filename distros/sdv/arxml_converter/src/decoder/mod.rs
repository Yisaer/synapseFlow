//! Binary decoder — interprets raw byte slices according to AUTOSAR
//! data-type definitions.
//!
//! This is the core runtime component consumed by veloFlux pipeline.

pub mod value;

use std::collections::HashMap;

use crate::ast::types::{self, DataType, DataTypeKind};
use crate::util::convert;

pub use value::Value;

/// Decodes `&[u8]` payloads guided by a set of [`DataType`] definitions.
///
/// The type map is keyed by **lowercased** short-name (matching the
/// convention used by [`crate::parser::datatypes::DataTypesParser`]).
#[derive(Debug, Default)]
pub struct Decoder;

impl Decoder {
    pub fn new() -> Self {
        Self
    }

    /// Decode `data` according to `dt`, returning the number of bytes
    /// consumed and the resulting [`Value`].
    ///
    /// For composite types (STRUCTURE, ARRAY, VECTOR) this method
    /// recurses into the referenced sub-types via the type map.
    pub fn decode(
        &self,
        types: &HashMap<String, DataType>,
        data: &[u8],
        dt: &DataType,
    ) -> Result<(usize, Value), String> {
        match &dt.kind {
            DataTypeKind::TypeReference(tr) => self.decode_type_reference(data, tr),

            DataTypeKind::Array(arr) => {
                let element_dt = self
                    .resolve_ref(types, &arr.element_ref)
                    .ok_or_else(|| format!("unknown array element type: {}", arr.element_ref))?;

                if arr.size == 0 {
                    // Variable-length array — consume bytes until exhausted,
                    // gracefully stopping when the next element can't be decoded.
                    let mut offset = 0;
                    let mut elems = Vec::new();
                    while offset < data.len() {
                        match self.decode(types, &data[offset..], &element_dt) {
                            Ok((n, v)) => {
                                if n == 0 {
                                    break;
                                }
                                offset += n;
                                elems.push(v);
                            }
                            Err(_) => break,
                        }
                    }
                    return Ok((offset, Value::Array(elems)));
                }

                let mut offset = 0;
                let mut elems = Vec::with_capacity(arr.size as usize);
                for _ in 0..arr.size {
                    let (n, v) = self.decode(types, &data[offset..], &element_dt)?;
                    offset += n;
                    elems.push(v);
                }
                Ok((offset, Value::Array(elems)))
            }

            DataTypeKind::Vector(vec) => {
                let element_dt = self
                    .resolve_ref(types, &vec.element_ref)
                    .ok_or_else(|| format!("unknown vector element type: {}", vec.element_ref))?;

                // Variable-length: consume all remaining bytes.
                let mut offset = 0;
                let mut elems = Vec::new();
                while offset < data.len() {
                    let (n, v) = self.decode(types, &data[offset..], &element_dt)?;
                    if n == 0 {
                        break; // prevent infinite loop on zero-width types
                    }
                    offset += n;
                    elems.push(v);
                }
                Ok((offset, Value::Array(elems)))
            }

            DataTypeKind::Structure(st) => {
                let mut offset = 0;
                let mut fields = Vec::with_capacity(st.fields.len());
                for sf in &st.fields {
                    let field_dt = self.resolve_ref(types, &sf.type_ref).ok_or_else(|| {
                        format!("unknown type '{}' for field '{}'", sf.type_ref, sf.name)
                    })?;
                    let (n, v) = self.decode(types, &data[offset..], &field_dt)?;
                    offset += n;
                    fields.push((sf.name.clone(), v));
                }
                Ok((offset, Value::Struct(fields)))
            }
        }
    }

    /// Decode a base-type reference.
    fn decode_type_reference(
        &self,
        data: &[u8],
        tr: &types::TypeReference,
    ) -> Result<(usize, Value), String> {
        use crate::ast::resolver::{BasicType, resolve_basic_type};

        let bt = resolve_basic_type(tr)
            .ok_or_else(|| format!("unresolved basic type: {}", tr.type_name))?;

        match bt {
            BasicType::Uint8 => read_u8(data).map(|(n, v)| (n, Value::U8(v))),
            BasicType::Uint16 => read_u16(data).map(|(n, v)| (n, Value::U16(v))),
            BasicType::Uint32 => read_u32(data).map(|(n, v)| (n, Value::U32(v))),
            BasicType::Uint64 => read_u64(data).map(|(n, v)| (n, Value::U64(v))),
            BasicType::Int8 => read_i8(data).map(|(n, v)| (n, Value::I8(v))),
            BasicType::Int16 => read_i16(data).map(|(n, v)| (n, Value::I16(v))),
            BasicType::Int32 => read_i32(data).map(|(n, v)| (n, Value::I32(v))),
            BasicType::Int64 => read_i64(data).map(|(n, v)| (n, Value::I64(v))),
            BasicType::Float => read_f32(data).map(|(n, v)| (n, Value::F32(v))),
            BasicType::Double => read_f64(data).map(|(n, v)| (n, Value::F64(v))),
            BasicType::Boolean => read_u8(data).map(|(n, v)| (n, Value::Bool(v != 0))),
            BasicType::String => {
                // Variable-length string: consume rest of buffer as UTF-8.
                let s =
                    std::str::from_utf8(data).map_err(|e| format!("invalid UTF-8 string: {e}"))?;
                Ok((data.len(), Value::Str(s.to_string())))
            }
            BasicType::FixedLengthString(len) => {
                let len = len as usize;
                if data.len() < len {
                    return Err(format!(
                        "need {len} bytes for fixed-length string, have {}",
                        data.len()
                    ));
                }
                let s = std::str::from_utf8(&data[..len])
                    .map_err(|e| format!("invalid UTF-8 fixed-string: {e}"))?;
                Ok((len, Value::Str(s.to_string())))
            }
        }
    }

    /// Resolve a type-reference path (e.g. `"/DataTypes/SpeedType"` or
    /// `"uint8"`) into a [`DataType`].
    ///
    /// The returned value is either a clone of an entry in the type
    /// map or a synthesised `DataType` for basic / built-in types.
    pub fn resolve_ref(
        &self,
        types: &HashMap<String, DataType>,
        ref_path: &str,
    ) -> Option<DataType> {
        use crate::ast::resolver::resolve_basic_type;

        let key = convert::extract_last(ref_path).to_lowercase();

        // 1) Type-map lookup (application data types).
        if let Some(dt) = types.get(&key) {
            return Some(dt.clone());
        }

        // 2) Fallback: treat as a basic-type reference.
        let tr = types::TypeReference {
            type_name: ref_path.to_string(),
            string_size: None,
        };
        if resolve_basic_type(&tr).is_some() {
            return Some(DataType::new_type_reference(
                key,
                "TYPE_REFERENCE".into(),
                ref_path.to_string(),
            ));
        }

        None
    }
}

// ---------------------------------------------------------------------------
// Primitive readers — each returns (bytes_consumed, value)
// ---------------------------------------------------------------------------

fn read_u8(data: &[u8]) -> Result<(usize, u8), String> {
    ensure_len(data, 1, "u8")?;
    Ok((1, data[0]))
}

fn read_u16(data: &[u8]) -> Result<(usize, u16), String> {
    ensure_len(data, 2, "u16")?;
    Ok((2, u16::from_be_bytes([data[0], data[1]])))
}

fn read_u32(data: &[u8]) -> Result<(usize, u32), String> {
    ensure_len(data, 4, "u32")?;
    Ok((4, u32::from_be_bytes([data[0], data[1], data[2], data[3]])))
}

fn read_u64(data: &[u8]) -> Result<(usize, u64), String> {
    ensure_len(data, 8, "u64")?;
    Ok((
        8,
        u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]),
    ))
}

fn read_i8(data: &[u8]) -> Result<(usize, i8), String> {
    ensure_len(data, 1, "i8")?;
    Ok((1, data[0] as i8))
}

fn read_i16(data: &[u8]) -> Result<(usize, i16), String> {
    let (_, v) = read_u16(data)?;
    Ok((2, v as i16))
}

fn read_i32(data: &[u8]) -> Result<(usize, i32), String> {
    let (_, v) = read_u32(data)?;
    Ok((4, v as i32))
}

fn read_i64(data: &[u8]) -> Result<(usize, i64), String> {
    let (_, v) = read_u64(data)?;
    Ok((8, v as i64))
}

fn read_f32(data: &[u8]) -> Result<(usize, f32), String> {
    let (_, v) = read_u32(data)?;
    Ok((4, f32::from_bits(v)))
}

fn read_f64(data: &[u8]) -> Result<(usize, f64), String> {
    let (_, v) = read_u64(data)?;
    Ok((8, f64::from_bits(v)))
}

fn ensure_len(data: &[u8], need: usize, type_name: &str) -> Result<(), String> {
    if data.len() < need {
        Err(format!(
            "not enough bytes for {type_name}: need {need}, have {}",
            data.len()
        ))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::types::{ArrayType, StructureField};

    fn types_map() -> HashMap<String, DataType> {
        // A simple map with one sub-type: `Point { x: u32, y: u32 }`
        let point_fields = vec![
            StructureField {
                name: "x".into(),
                type_ref: "/Base/uint32".into(),
                in_place: false,
            },
            StructureField {
                name: "y".into(),
                type_ref: "/Base/uint32".into(),
                in_place: false,
            },
        ];
        let point_dt = DataType::new_structure("Point".into(), "STRUCTURE".into(), point_fields);
        [("point".to_string(), point_dt)].into_iter().collect()
    }

    struct TestDecoder {
        decoder: Decoder,
        types: HashMap<String, DataType>,
    }

    impl TestDecoder {
        fn decode(&self, data: &[u8], dt: &DataType) -> Result<(usize, Value), String> {
            self.decoder.decode(&self.types, data, dt)
        }

        fn resolve_ref(&self, ref_path: &str) -> Option<DataType> {
            self.decoder.resolve_ref(&self.types, ref_path)
        }
    }

    fn decoder() -> TestDecoder {
        TestDecoder {
            decoder: Decoder::new(),
            types: types_map(),
        }
    }

    #[test]
    fn decode_uint8() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint8".into());
        let d = decoder();
        let (n, v) = d.decode(&[0xAB], &dt).unwrap();
        assert_eq!(n, 1);
        assert_eq!(v, Value::U8(0xAB));
    }

    #[test]
    fn decode_uint16_big_endian() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint16".into());
        let d = decoder();
        let (n, v) = d.decode(&[0x12, 0x34], &dt).unwrap();
        assert_eq!(n, 2);
        assert_eq!(v, Value::U16(0x1234));
    }

    #[test]
    fn decode_uint32_big_endian() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint32".into());
        let d = decoder();
        let (n, v) = d.decode(&[0xAA, 0xBB, 0xCC, 0xDD], &dt).unwrap();
        assert_eq!(n, 4);
        assert_eq!(v, Value::U32(0xAABBCCDD));
    }

    #[test]
    fn decode_boolean_true() {
        let dt = DataType::new_type_reference("b".into(), "VALUE".into(), "bool".into());
        let d = decoder();
        let (n, v) = d.decode(&[1], &dt).unwrap();
        assert_eq!(n, 1);
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn decode_boolean_false() {
        let dt = DataType::new_type_reference("b".into(), "VALUE".into(), "bool".into());
        let d = decoder();
        let (_n, v) = d.decode(&[0], &dt).unwrap();
        assert_eq!(v, Value::Bool(false));
    }

    #[test]
    fn decode_float() {
        let dt = DataType::new_type_reference("f".into(), "VALUE".into(), "float".into());
        let d = decoder();
        let (n, v) = d.decode(&[0x40, 0x49, 0x0F, 0xDB], &dt).unwrap();
        assert_eq!(n, 4);
        if let Value::F32(x) = v {
            assert!((x - std::f32::consts::PI).abs() < 0.001);
        } else {
            panic!("expected F32");
        }
    }

    #[test]
    fn decode_structure() {
        // Point { x: u32, y: u32 } where x=1, y=2 (big-endian)
        let dt = decoder().resolve_ref("point").unwrap();
        let data = [0, 0, 0, 1, 0, 0, 0, 2];

        let d = decoder();
        let (n, v) = d.decode(&data, &dt).unwrap();
        assert_eq!(n, 8);

        if let Value::Struct(fields) = v {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].0, "x");
            assert_eq!(fields[0].1, Value::U32(1));
            assert_eq!(fields[1].0, "y");
            assert_eq!(fields[1].1, Value::U32(2));
        } else {
            panic!("expected Struct");
        }
    }

    #[test]
    fn decode_fixed_array() {
        let arr_dt = DataType {
            short_name: "Arr".into(),
            category: "ARRAY".into(),
            kind: DataTypeKind::Array(ArrayType {
                size: 3,
                in_place: false,
                element_ref: "uint8".into(),
            }),
        };
        let d = decoder();
        let (n, v) = d.decode(&[10, 20, 30], &arr_dt).unwrap();
        assert_eq!(n, 3);
        assert_eq!(
            v,
            Value::Array(vec![Value::U8(10), Value::U8(20), Value::U8(30),])
        );
    }

    #[test]
    fn not_enough_bytes_error() {
        let dt = DataType::new_type_reference("u".into(), "VALUE".into(), "uint32".into());
        let d = decoder();
        let err = d.decode(&[0xAA], &dt).unwrap_err();
        assert!(err.contains("not enough bytes for u32"));
    }
}
