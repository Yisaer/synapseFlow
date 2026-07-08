//! Decoded value types produced by the [`Decoder`](super::Decoder).
//!
//! The layout here is designed to be trivially convertible to veloFlux's
//! `Collection` / `Tuple` / `Value` data types.

/// A single decoded data element — primitive, composite, or raw bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    /// Variable-length UTF-8 string.
    Str(String),
    /// A structured record with ordered, named fields.
    Struct(Vec<(String, Value)>),
    /// An ordered list of homogeneous or heterogeneous values.
    Array(Vec<Value>),
    /// Opaque byte sequence (for types that cannot be decoded further).
    Bytes(Vec<u8>),
}
