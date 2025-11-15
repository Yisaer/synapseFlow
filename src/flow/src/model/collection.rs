use crate::model::Tuple;
use crate::planner::physical::PhysicalProjectField;
use datatypes::Value;
use std::any::Any;

/// Collection trait defines the interface for multi-row data structures
///
/// This trait provides methods to access and manipulate collections of data,
/// supporting both row-based and column-based access patterns.
pub trait Collection: Send + Sync + Any {
    /// Get the number of rows in this collection
    fn num_rows(&self) -> usize;

    /// Get a view of the rows stored in this collection
    fn rows(&self) -> &[Tuple];

    /// Check if the collection is empty
    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Slice the collection (create a view from start to end)
    fn slice(&self, start: usize, end: usize) -> Result<Box<dyn Collection>, CollectionError>;

    /// Take a selection of rows by indices
    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError>;

    /// Apply projection based on PhysicalProjectField definitions
    /// This creates a new collection with projected fields based on the provided field definitions
    fn apply_projection(
        &self,
        fields: &[PhysicalProjectField],
    ) -> Result<Box<dyn Collection>, CollectionError>;

    /// Apply a filter expression to this collection
    /// This creates a new collection containing only the rows that satisfy the filter condition
    ///
    /// # Arguments
    /// * `filter_expr` - A ScalarExpr that evaluates to a boolean value for each row
    ///
    /// # Returns
    /// A new collection with only the rows that satisfy the filter condition, or an error if filtering fails
    ///
    /// # Note
    /// This is an abstract method that must be implemented by specific collection types.
    /// The filter expression should evaluate to boolean values (true for rows to keep, false for rows to discard).
    /// Implementations should consider the collection's storage characteristics for optimal performance.
    fn apply_filter(
        &self,
        filter_expr: &crate::expr::ScalarExpr,
    ) -> Result<Box<dyn Collection>, CollectionError>;

    /// Clone this collection
    fn clone_box(&self) -> Box<dyn Collection>;
}

impl Clone for Box<dyn Collection> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Column represents a column of data in columnar format
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// Source table name (which table this column belongs to)
    pub source_name: String,
    /// Column name
    pub name: String,
    /// Column values
    pub data: Vec<Value>,
}

impl Column {
    /// Create a new column
    pub fn new(source_name: String, name: String, data: Vec<Value>) -> Self {
        Self {
            source_name,
            name,
            data,
        }
    }

    /// Get the number of elements in this column
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a value at the given index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.data.get(index)
    }

    /// Get all values as a slice
    pub fn values(&self) -> &[Value] {
        &self.data
    }

    /// Get the data type (inferred from the first value)
    pub fn data_type(&self) -> Option<datatypes::ConcreteDatatype> {
        self.data.first().map(|v| v.datatype())
    }

    /// Get the source table name
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Error type for collection operations
#[derive(Debug, Clone, PartialEq)]
pub enum CollectionError {
    /// Index out of bounds
    IndexOutOfBounds { index: usize, len: usize },
    /// Invalid slice range
    InvalidSliceRange {
        start: usize,
        end: usize,
        len: usize,
    },
    /// Column not found by name
    ColumnNotFound { name: String },
    /// Type mismatch
    TypeMismatch { expected: String, actual: String },
    /// Filter expression error
    FilterError { message: String },
    /// Other error
    Other(String),
}

impl std::fmt::Display for CollectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectionError::IndexOutOfBounds { index, len } => {
                write!(f, "Index {} out of bounds for length {}", index, len)
            }
            CollectionError::InvalidSliceRange { start, end, len } => {
                write!(
                    f,
                    "Invalid slice range [{}..{}] for length {}",
                    start, end, len
                )
            }
            CollectionError::ColumnNotFound { name } => {
                write!(f, "Column '{}' not found", name)
            }
            CollectionError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            CollectionError::FilterError { message } => {
                write!(f, "Filter error: {}", message)
            }
            CollectionError::Other(msg) => write!(f, "Collection error: {}", msg),
        }
    }
}

impl std::error::Error for CollectionError {}
