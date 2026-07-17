use crate::model::Tuple;
use crate::planner::physical::output_layout::{OutputLayout, OutputValueRef};
use crate::processor::ProcessorError;
use datatypes::Value;
use std::sync::Arc;

#[derive(Clone)]
struct OutputRowColumn {
    name: Arc<str>,
    value_ref: OutputValueRef,
}

#[derive(Clone)]
pub(crate) struct OutputRowAccessor {
    columns: Arc<[OutputRowColumn]>,
}

pub(crate) struct ExtractedOutputRow {
    values: Vec<Option<Arc<Value>>>,
}

impl OutputRowAccessor {
    pub(crate) fn from_output_layout(output_layout: &OutputLayout) -> Self {
        Self {
            columns: output_layout
                .columns
                .iter()
                .map(|column| OutputRowColumn {
                    name: Arc::clone(&column.name),
                    value_ref: column.value_ref.clone(),
                })
                .collect::<Vec<_>>()
                .into(),
        }
    }

    pub(crate) fn width(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn column_names(&self) -> Arc<[Arc<str>]> {
        self.columns
            .iter()
            .map(|column| Arc::clone(&column.name))
            .collect::<Vec<_>>()
            .into()
    }

    pub(crate) fn extract_row(&self, tuple: &Tuple) -> Result<ExtractedOutputRow, ProcessorError> {
        let values = self
            .columns
            .iter()
            .map(|column| {
                column
                    .value_ref
                    .resolve(tuple)
                    .map(|value| Some(Arc::new(value.clone())))
                    .map_err(|error| ProcessorError::ProcessingError(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ExtractedOutputRow { values })
    }
}

impl ExtractedOutputRow {
    pub(crate) fn into_values_with_null_fill(self) -> Vec<Arc<Value>> {
        self.values
            .into_iter()
            .map(|value| value.unwrap_or_else(|| Arc::new(Value::Null)))
            .collect()
    }

    pub(crate) fn into_optional_values(self) -> Vec<Option<Arc<Value>>> {
        self.values
    }
}
