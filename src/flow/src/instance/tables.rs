use std::sync::Arc;

use crate::catalog::{TableDefinition, TableProps};

use super::{FlowInstance, FlowInstanceError};

impl FlowInstance {
    pub async fn create_table(
        &self,
        definition: TableDefinition,
    ) -> Result<Arc<TableDefinition>, FlowInstanceError> {
        self.validate_table_definition(&definition)?;
        self.catalog
            .insert_table(definition)
            .map_err(FlowInstanceError::from)
    }

    fn validate_table_definition(
        &self,
        definition: &TableDefinition,
    ) -> Result<(), FlowInstanceError> {
        match definition.props() {
            TableProps::History(props) => {
                if props.datasource.trim().is_empty() {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' history datasource must not be empty",
                        definition.id()
                    )));
                }
                if props.topic.trim().is_empty() {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' history topic must not be empty",
                        definition.id()
                    )));
                }
                if props.batch_size == Some(0) {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' history batch_size must be greater than 0",
                        definition.id()
                    )));
                }
            }
        }
        Ok(())
    }
}
