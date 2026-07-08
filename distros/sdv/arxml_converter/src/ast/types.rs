/// Top-level representation of an AUTOSAR data type parsed from ARXML.
///
/// Corresponds to the Go `ast.DataType` struct. The `kind` field carries
/// the category-specific payload; `short_name` and `category` are always present.
#[derive(Debug, Clone, PartialEq)]
pub struct DataType {
    pub short_name: String,
    pub category: String,
    pub kind: DataTypeKind,
}

/// Category-specific payload of a [`DataType`].
#[derive(Debug, Clone, PartialEq)]
pub enum DataTypeKind {
    /// A reference to a primitive / base type (e.g. `uint8`, `float`).
    TypeReference(TypeReference),
    /// A fixed-size array.
    Array(ArrayType),
    /// A variable-length sequence (vector).
    Vector(VectorType),
    /// A structure composed of named fields.
    Structure(StructureType),
}

/// Describes a reference to a basic type, optionally with a fixed string size.
///
/// Corresponds to Go `ast.TypReference`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeReference {
    /// The target type name, e.g. `"uint32"` or `"/DataType/SomeType"`.
    pub type_name: String,
    /// Non-zero when the target is a fixed-length string (size in bytes).
    pub string_size: Option<u64>,
}

/// A fixed-size array with a known element type.
///
/// Corresponds to Go `ast.Array`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayType {
    /// Number of elements in the array.
    pub size: u64,
    /// Whether the array data is inlined in the parent structure.
    pub in_place: bool,
    /// Reference to the element type (path string from ARXML).
    pub element_ref: String,
}

/// A variable-length sequence (vector) with a known element type.
///
/// Corresponds to Go `ast.Vector`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorType {
    /// Reference to the element type (path string from ARXML).
    pub element_ref: String,
}

/// A structured data type composed of named, typed fields.
///
/// Corresponds to Go `ast.Structure`.
#[derive(Debug, Clone, PartialEq)]
pub struct StructureType {
    pub fields: Vec<StructureField>,
}

/// A single field inside a [`StructureType`].
///
/// Corresponds to Go `ast.StructureTypRef`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureField {
    /// Field name (SHORT-NAME in ARXML).
    pub name: String,
    /// Reference to the field's type (TYPE-TREF value).
    pub type_ref: String,
    /// Whether the field's data is inlined.
    pub in_place: bool,
}

// ---------------------------------------------------------------------------
// Convenience constructors — mirror the Go `New*DataType` helpers
// ---------------------------------------------------------------------------

impl DataType {
    /// Basic type reference (e.g. `uint32`, `float`, or a path ref).
    pub fn new_type_reference(short_name: String, category: String, type_name: String) -> Self {
        DataType {
            short_name,
            category,
            kind: DataTypeKind::TypeReference(TypeReference {
                type_name,
                string_size: None,
            }),
        }
    }

    /// Fixed-length string type.
    pub fn new_string(short_name: String, category: String, string_size: u64) -> Self {
        DataType {
            short_name,
            category,
            kind: DataTypeKind::TypeReference(TypeReference {
                type_name: "string".to_string(),
                string_size: Some(string_size),
            }),
        }
    }

    /// Fixed-size array.
    pub fn new_array(short_name: String, category: String, element_ref: String, size: u64) -> Self {
        DataType {
            short_name,
            category,
            kind: DataTypeKind::Array(ArrayType {
                size,
                in_place: false,
                element_ref,
            }),
        }
    }

    /// Variable-length vector.
    pub fn new_vector(short_name: String, category: String, element_ref: String) -> Self {
        DataType {
            short_name,
            category,
            kind: DataTypeKind::Vector(VectorType { element_ref }),
        }
    }

    /// Composite structure.
    pub fn new_structure(
        short_name: String,
        category: String,
        fields: Vec<StructureField>,
    ) -> Self {
        DataType {
            short_name,
            category,
            kind: DataTypeKind::Structure(StructureType { fields }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_type_reference_roundtrip() {
        let dt = DataType::new_type_reference(
            "MyUint32".into(),
            "TYPE_REFERENCE".into(),
            "/PlatformTypes/uint32".into(),
        );
        assert_eq!(dt.short_name, "MyUint32");
        assert_eq!(dt.category, "TYPE_REFERENCE");
        if let DataTypeKind::TypeReference(ref tr) = dt.kind {
            assert_eq!(tr.type_name, "/PlatformTypes/uint32");
            assert!(tr.string_size.is_none());
        } else {
            panic!("expected TypeReference");
        }
    }

    #[test]
    fn string_type_has_size() {
        let dt = DataType::new_string("Name".into(), "STRING".into(), 32);
        if let DataTypeKind::TypeReference(ref tr) = dt.kind {
            assert_eq!(tr.type_name, "string");
            assert_eq!(tr.string_size, Some(32));
        } else {
            panic!("expected TypeReference for string");
        }
    }

    #[test]
    fn array_type_fields() {
        let dt = DataType::new_array("Arr".into(), "ARRAY".into(), "/dt/Inner".into(), 10);
        if let DataTypeKind::Array(ref arr) = dt.kind {
            assert_eq!(arr.size, 10);
            assert_eq!(arr.element_ref, "/dt/Inner");
            assert!(!arr.in_place);
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn structure_with_fields() {
        let dt = DataType::new_structure(
            "MyStruct".into(),
            "STRUCTURE".into(),
            vec![
                StructureField {
                    name: "x".into(),
                    type_ref: "/dt/uint32".into(),
                    in_place: false,
                },
                StructureField {
                    name: "y".into(),
                    type_ref: "/dt/float".into(),
                    in_place: true,
                },
            ],
        );
        assert_eq!(dt.short_name, "MyStruct");
        if let DataTypeKind::Structure(ref s) = dt.kind {
            assert_eq!(s.fields.len(), 2);
            assert_eq!(s.fields[0].name, "x");
            assert_eq!(s.fields[1].name, "y");
            assert!(!s.fields[0].in_place);
            assert!(s.fields[1].in_place);
        } else {
            panic!("expected Structure");
        }
    }
}
