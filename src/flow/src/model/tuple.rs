use datatypes::Value;

/// Tuple represents a single row of data decoded from a source.
///
/// Each tuple keeps the originating source name plus the ordered list of fully
/// qualified column identifiers and their values. Each entry in `columns`
/// stores `(source_name, column_name)` for the corresponding value at the same
/// index in `values`.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple {
    pub source_name: String,
    pub columns: Vec<(String, String)>,
    pub values: Vec<Value>,
}

impl Tuple {
    /// Build a new tuple, assuming the caller provides matching column/value
    /// lengths.
    pub fn new(source_name: String, columns: Vec<(String, String)>, values: Vec<Value>) -> Self {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "Tuple columns and values must have the same length"
        );

        Self {
            source_name,
            columns,
            values,
        }
    }

    /// Return number of fields stored in this tuple.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check whether this tuple contains any fields.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn column_pairs(&self) -> &[(String, String)] {
        &self.columns
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn value_by_name(&self, source_name: &str, column_name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .zip(self.values.iter())
            .find(|((src, name), _)| src == source_name && name == column_name)
            .map(|(_, value)| value)
    }

    pub fn value_by_column(&self, column_name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .zip(self.values.iter())
            .find(|((_, name), _)| name == column_name)
            .map(|(_, value)| value)
    }
}
