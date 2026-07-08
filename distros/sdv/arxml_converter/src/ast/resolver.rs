//! Basic type resolution — maps AUTOSAR type-reference strings to
//! concrete [`BasicType`] discriminants.
//!
//! Corresponds to the Go `ast.util.go` / `GetBasicTypeFromRef`.

use crate::ast::types::TypeReference;

/// Enum representing every built-in basic type recognised by the converter.
///
/// The variants map 1:1 to the `typeref.TypeRef` constructors in the
/// Go `idl-parser` library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BasicType {
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    Boolean,
    /// Variable-length string (no fixed bound).
    String,
    /// Fixed-length string with the given byte-size.
    FixedLengthString(u64),
}

/// Try to resolve a [`TypeReference`] into a known [`BasicType`].
///
/// Returns `None` when the reference does not name a recognised built-in.
pub fn resolve_basic_type(type_ref: &TypeReference) -> Option<BasicType> {
    let name = extract_type_name_from_ref(&type_ref.type_name);

    if name.contains("string") {
        return type_ref
            .string_size
            .filter(|&s| s > 0)
            .map(BasicType::FixedLengthString)
            .or(Some(BasicType::String));
    }

    // Order matters: check longer substrings before shorter ones so that
    // e.g. "uint16" is matched before "uint8".
    // Order matters: longer substrings first to avoid shadowing
    // (e.g. "uint16" must be tested before "uint8").
    // Use contains() rather than exact match so that prefixed names
    // like "sint32" are recognised (same as Go).
    if name.contains("uint16") {
        Some(BasicType::Uint16)
    } else if name.contains("uint32") {
        Some(BasicType::Uint32)
    } else if name.contains("uint64") {
        Some(BasicType::Uint64)
    } else if name.contains("uint8") || name.contains("int8") {
        Some(BasicType::Uint8) // AUTOSAR maps int8 → uint8 (octet)
    } else if name.contains("int16") {
        Some(BasicType::Int16)
    } else if name.contains("int32") {
        Some(BasicType::Int32)
    } else if name.contains("int64") {
        Some(BasicType::Int64)
    } else if name.contains("float") {
        Some(BasicType::Float)
    } else if name.contains("double") {
        Some(BasicType::Double)
    } else if name.contains("bool") {
        Some(BasicType::Boolean)
    } else {
        None
    }
}

/// Extract the trailing segment of an ARXML type-path (e.g.
/// `"/PlatformTypes/uint32"` → `"uint32"`), lowercased.
pub fn extract_type_name_from_ref(r: &str) -> String {
    r.rsplit('/').next().unwrap_or(r).to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_uint32() {
        let tr = TypeReference {
            type_name: "/PlatformTypes/uint32".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::Uint32));
    }

    #[test]
    fn resolve_float_lowercase() {
        let tr = TypeReference {
            type_name: "/BaseTypes/Float".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::Float));
    }

    #[test]
    fn resolve_string_variable() {
        let tr = TypeReference {
            type_name: "string".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::String));
    }

    #[test]
    fn resolve_fixed_length_string() {
        let tr = TypeReference {
            type_name: "/StringType".into(),
            string_size: Some(64),
        };
        assert_eq!(
            resolve_basic_type(&tr),
            Some(BasicType::FixedLengthString(64))
        );
    }

    #[test]
    fn resolve_boolean() {
        let tr = TypeReference {
            type_name: "boolean".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::Boolean));
    }

    #[test]
    fn resolve_int8_to_uint8() {
        // AUTOSAR convention: int8 is treated as octet (uint8).
        let tr = TypeReference {
            type_name: "/BaseTypes/int8".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), Some(BasicType::Uint8));
    }

    #[test]
    fn unknown_type_returns_none() {
        let tr = TypeReference {
            type_name: "/custom/SomeWeirdType".into(),
            string_size: None,
        };
        assert_eq!(resolve_basic_type(&tr), None);
    }

    #[test]
    fn extract_trailing_segment() {
        assert_eq!(extract_type_name_from_ref("/PlatformTypes/Float"), "float");
        assert_eq!(extract_type_name_from_ref("uint32"), "uint32");
    }
}
