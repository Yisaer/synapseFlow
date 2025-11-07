use crate::datatypes::ConcreteDatatype;

/// Schema of a column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: ConcreteDatatype,
    /// Source table name (which table this column belongs to)
    pub source_name: String,
}

impl ColumnSchema {
    /// Create a new column schema with name, source table name, and data type
    pub fn new(name: String, source_name: String, data_type: ConcreteDatatype) -> Self {
        ColumnSchema { 
            name, 
            source_name,
            data_type,
        }
    }

    /// Get the source table name
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Check if this column belongs to a specific source table
    pub fn belongs_to(&self, source: &str) -> bool {
        self.source_name == source
    }
}