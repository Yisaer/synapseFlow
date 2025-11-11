use std::any::Any;
use datatypes::{Schema, Value};

/// Type alias for filter predicate function
pub type FilterPredicate = Box<dyn Fn(&dyn crate::model::Row) -> bool + Send + Sync>;

/// Collection trait defines the interface for multi-row data structures
/// 
/// This trait provides methods to access and manipulate collections of data,
/// supporting both row-based and column-based access patterns.
pub trait Collection: Send + Sync + Any {
    /// Get the schema of this collection
    fn schema(&self) -> &Schema;
    
    /// Get the number of rows in this collection
    fn num_rows(&self) -> usize;
    
    /// Get the number of columns in this collection
    fn num_columns(&self) -> usize {
        self.schema().column_schemas().len()
    }
    
    /// Get a column by index
    fn column(&self, index: usize) -> Option<&Column>;
    
    /// Get a column by name
    fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.schema()
            .column_schemas()
            .iter()
            .position(|col_schema| col_schema.name == name)
            .and_then(|index| self.column(index))
    }
    
    /// Get a row by index (row-based access)
    fn row(&self, index: usize) -> Option<Box<dyn crate::model::Row>>;
    
    /// Check if the collection is empty
    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
    
    /// Slice the collection (create a view from start to end)
    fn slice(&self, start: usize, end: usize) -> Result<Box<dyn Collection>, CollectionError>;
    
    /// Take a selection of rows by indices
    fn take(&self, indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError>;
    
    /// Filter rows based on a predicate function (boxed version for dyn compatibility)
    fn filter_boxed(&self, predicate: FilterPredicate) -> Result<Box<dyn Collection>, CollectionError>; 
    
    /// Get all columns as a slice
    fn columns(&self) -> &[Column];
    
    /// Create a new collection with the specified column indices (projection)
    fn project(&self, column_indices: &[usize]) -> Result<Box<dyn Collection>, CollectionError>;
}

/// Column represents a column of data in columnar format
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: datatypes::ConcreteDatatype,
    /// Column values
    pub data: Vec<Value>,
}

impl Column {
    /// Create a new column
    pub fn new(name: String, data_type: datatypes::ConcreteDatatype, data: Vec<Value>) -> Self {
        Self { name, data_type, data }
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
    
    /// Get the data type
    pub fn data_type(&self) -> &datatypes::ConcreteDatatype {
        &self.data_type
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
    IndexOutOfBounds {
        index: usize,
        len: usize,
    },
    /// Invalid slice range
    InvalidSliceRange {
        start: usize,
        end: usize,
        len: usize,
    },
    /// Invalid column index
    InvalidColumnIndex {
        index: usize,
        num_columns: usize,
    },
    /// Column not found by name
    ColumnNotFound {
        name: String,
    },
    /// Type mismatch
    TypeMismatch {
        expected: String,
        actual: String,
    },
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
                write!(f, "Invalid slice range [{}..{}] for length {}", start, end, len)
            }
            CollectionError::InvalidColumnIndex { index, num_columns } => {
                write!(f, "Invalid column index {} for {} columns", index, num_columns)
            }
            CollectionError::ColumnNotFound { name } => {
                write!(f, "Column '{}' not found", name)
            }
            CollectionError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            CollectionError::Other(msg) => write!(f, "Collection error: {}", msg),
        }
    }
}

impl std::error::Error for CollectionError {}