use std::sync::Arc;

/// Encoder-side projection spec for delayed materialization of `ColumnRef::ByIndex`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByIndexProjection {
    pub columns: Arc<[ByIndexProjectionColumn]>,
}

impl ByIndexProjection {
    pub fn new(columns: Vec<ByIndexProjectionColumn>) -> Self {
        Self {
            columns: Arc::from(columns),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn columns(&self) -> &[ByIndexProjectionColumn] {
        &self.columns
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByIndexProjectionColumn {
    pub source_name: Arc<str>,
    pub column_index: usize,
    pub output_name: Arc<str>,
}

impl ByIndexProjectionColumn {
    pub fn new(
        source_name: impl Into<Arc<str>>,
        column_index: usize,
        output_name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            column_index,
            output_name: output_name.into(),
        }
    }
}
