//! ProtoDescriptorBundle — pre-built index mapping protobuf field numbers to decode information.
//!
//! Built once during schema parsing and shared across all streams referencing the same proto message type.

use datatypes::ConcreteDatatype;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Decode metadata for a single protobuf field.
#[derive(Debug, Clone)]
pub struct ProtoFieldInfo {
    /// Index into the schema's column list.
    pub column_index: usize,
    /// Expected datatype of this field.
    ///
    /// For repeated fields this is the *element* type (List wrapping is handled by the decoder).
    pub datatype: ConcreteDatatype,
    /// Whether this field is `repeated` (packed or unpacked).
    pub is_repeated: bool,
    /// Whether the varint wire value uses zigzag encoding (sint32 / sint64).
    pub is_zigzag: bool,
    /// For message fields, a nested bundle to recursively decode sub-messages.
    pub nested_bundle: Option<Arc<ProtoDescriptorBundle>>,
}

/// Pre-built index for one protobuf message type.
///
/// Maps `field_number → ProtoFieldInfo` and `column_name → field_number`,
/// and preserves the total column count of the owning schema so the decoder
/// can pre-allocate the values vector.
#[derive(Debug, Clone)]
pub struct ProtoDescriptorBundle {
    /// field_number → decode metadata.
    pub field_map: BTreeMap<u32, ProtoFieldInfo>,
    /// column_name → field_number (reverse index for projection lookups).
    pub column_to_field: BTreeMap<String, u32>,
    /// Total number of columns in the parent schema.
    pub column_count: usize,
    /// Column names in schema order (index → name).
    pub column_names: Vec<Arc<str>>,
}

impl ProtoDescriptorBundle {
    pub fn new(
        field_map: BTreeMap<u32, ProtoFieldInfo>,
        column_to_field: BTreeMap<String, u32>,
        column_count: usize,
        column_names: Vec<Arc<str>>,
    ) -> Self {
        Self {
            field_map,
            column_to_field,
            column_count,
            column_names,
        }
    }

    /// Look up the field number for a given column name.
    pub fn field_number_for_column(&self, column_name: &str) -> Option<u32> {
        self.column_to_field.get(column_name).copied()
    }
}
