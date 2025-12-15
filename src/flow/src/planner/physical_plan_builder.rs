//! Physical plan builder - converts logical plans to physical plans using centralized index management
use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings, SchemaBinding, SchemaBindingEntry, SourceBindingKind,
};
use crate::planner::logical::{
    aggregation::Aggregation as LogicalAggregation, DataSinkPlan, DataSource as LogicalDataSource,
    Filter as LogicalFilter, LogicalPlan, LogicalWindow, LogicalWindowSpec,
    Project as LogicalProject,
};
use crate::planner::physical::physical_project::PhysicalProjectField;
use crate::planner::physical::{
    PhysicalAggregation, PhysicalBatch, PhysicalDataSink, PhysicalDataSource, PhysicalEncoder,
    PhysicalFilter, PhysicalPlan, PhysicalProject, PhysicalResultCollect, PhysicalSharedStream,
    PhysicalSinkConnector,
};
use crate::planner::sink::{PipelineSink, PipelineSinkConnector};
use crate::PipelineRegistries;
use std::sync::Arc;

/// Physical plan builder that manages index allocation and node caching
pub struct PhysicalPlanBuilder {
    next_index: i64,
    node_cache: std::collections::HashMap<i64, Arc<PhysicalPlan>>,
}

impl PhysicalPlanBuilder {
    pub fn new() -> Self {
        Self {
            next_index: 0,
            node_cache: std::collections::HashMap::new(),
        }
    }

    pub fn starting_from(start_index: i64) -> Self {
        Self {
            next_index: start_index,
            node_cache: std::collections::HashMap::new(),
        }
    }

    pub fn allocate_index(&mut self) -> i64 {
        let index = self.next_index;
        self.next_index += 1;
        index
    }

    pub fn cache_node(&mut self, logical_index: i64, physical_node: Arc<PhysicalPlan>) {
        self.node_cache.insert(logical_index, physical_node);
    }

    pub fn get_cached_node(&self, logical_index: i64) -> Option<Arc<PhysicalPlan>> {
        self.node_cache.get(&logical_index).cloned()
    }
}

impl Default for PhysicalPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a physical plan from a logical plan using centralized index management
///
/// This function walks through the logical plan tree and creates corresponding physical plan nodes
/// by pattern matching on the logical plan enum, using a centralized index allocator.
/// This is the main entry point that should be used for all physical plan creation.
pub fn create_physical_plan(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut builder = PhysicalPlanBuilder::new();
    create_physical_plan_with_builder_cached(logical_plan, bindings, registries, &mut builder)
}

/// Create a physical plan from a logical plan using centralized index management
///
/// This function walks through the logical plan tree and creates corresponding physical plan nodes
/// by pattern matching on the logical plan enum, using a centralized index allocator.
pub fn create_physical_plan_with_builder(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    create_physical_plan_with_builder_cached(logical_plan, bindings, registries, builder)
}

/// Create a physical plan from a logical plan using centralized index management with node caching
///
/// This function ensures that shared logical nodes are converted to shared physical nodes,
/// maintaining the same instance across multiple references.
fn create_physical_plan_with_builder_cached(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let logical_index = logical_plan.get_plan_index();

    // Check if this logical node has already been converted using builder's cache
    if let Some(cached_physical) = builder.get_cached_node(logical_index) {
        return Ok(cached_physical);
    }

    // Create the physical node
    let physical_plan = match logical_plan.as_ref() {
        LogicalPlan::DataSource(logical_ds) => {
            let index = builder.allocate_index();
            create_physical_data_source_with_builder(
                logical_ds,
                &logical_plan,
                index,
                bindings,
                builder,
            )?
        }
        LogicalPlan::Filter(logical_filter) => create_physical_filter_with_builder_cached(
            logical_filter,
            &logical_plan,
            bindings,
            registries,
            builder,
        )?,
        LogicalPlan::Project(logical_project) => create_physical_project_with_builder_cached(
            logical_project,
            &logical_plan,
            bindings,
            registries,
            builder,
        )?,
        LogicalPlan::Aggregation(logical_agg) => create_physical_aggregation_with_builder(
            logical_agg,
            &logical_plan,
            bindings,
            registries,
            builder,
        )?,
        LogicalPlan::DataSink(logical_sink) => create_physical_data_sink_with_builder_cached(
            logical_sink,
            &logical_plan,
            bindings,
            registries,
            builder,
        )?,
        LogicalPlan::Tail(_logical_tail) => {
            // TailPlan is no longer used in new design, but handle it for backward compatibility
            // Convert to multiple DataSink nodes under a ResultCollect
            create_physical_result_collect_from_tail_with_builder_cached(
                &logical_plan,
                bindings,
                registries,
                builder,
            )?
        }
        LogicalPlan::Window(logical_window) => create_physical_window_with_builder(
            logical_window,
            &logical_plan,
            bindings,
            registries,
            builder,
        )?,
    };

    // Cache the result for future reuse using builder's cache
    builder.cache_node(logical_index, Arc::clone(&physical_plan));
    Ok(physical_plan)
}

/// Create a PhysicalResultCollect from a TailPlan using centralized index management with caching
fn create_physical_result_collect_from_tail_with_builder_cached(
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, registries, builder)?;
        physical_children.push(physical_child);
    }

    if physical_children.is_empty() {
        return Err("TailPlan must have at least one child".to_string());
    }

    // Always create ResultCollect to ensure consistent pipeline structure
    // This ensures that processor pipeline building works correctly
    let result_collect_index = builder.allocate_index();
    let result_collect = PhysicalResultCollect::new(physical_children, result_collect_index);
    Ok(Arc::new(PhysicalPlan::ResultCollect(result_collect)))
}

fn create_physical_window_with_builder(
    logical_window: &LogicalWindow,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, registries, builder)?;
        physical_children.push(physical_child);
    }

    let index = builder.allocate_index();
    let physical = match logical_window.spec {
        LogicalWindowSpec::Tumbling { time_unit, length } => {
            let tumbling = crate::planner::physical::PhysicalTumblingWindow::new(
                time_unit,
                length,
                physical_children,
                index,
            );
            PhysicalPlan::TumblingWindow(tumbling)
        }
        LogicalWindowSpec::Count { count } => {
            let count_window =
                crate::planner::physical::PhysicalCountWindow::new(count, physical_children, index);
            PhysicalPlan::CountWindow(count_window)
        }
    };

    Ok(Arc::new(physical))
}

fn create_physical_aggregation_with_builder(
    logical_agg: &LogicalAggregation,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, registries, builder)?;
        physical_children.push(physical_child);
    }
    let index = builder.allocate_index();
    let physical = PhysicalAggregation::new(
        logical_agg.aggregate_mappings.clone(),
        physical_children,
        index,
        bindings,
        registries.aggregate_registry().as_ref(),
    )?;
    Ok(Arc::new(PhysicalPlan::Aggregation(physical)))
}

/// Create a PhysicalDataSource from a LogicalDataSource using centralized index management
fn create_physical_data_source_with_builder(
    logical_ds: &LogicalDataSource,
    _logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
    _builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let entry = find_binding_entry(logical_ds, bindings)?;
    let schema = entry.schema.clone();
    match entry.kind {
        SourceBindingKind::Regular => {
            let physical_ds = PhysicalDataSource::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                schema,
                logical_ds.decoder().clone(),
                index,
            );
            Ok(Arc::new(PhysicalPlan::DataSource(physical_ds)))
        }
        SourceBindingKind::Shared => {
            let physical_shared = PhysicalSharedStream::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                schema,
                index,
            );
            Ok(Arc::new(PhysicalPlan::SharedStream(physical_shared)))
        }
    }
}

/// Create a PhysicalFilter from a LogicalFilter using centralized index management with caching
fn create_physical_filter_with_builder_cached(
    logical_filter: &LogicalFilter,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, registries, builder)?;
        physical_children.push(physical_child);
    }

    // Convert SQL Expr to ScalarExpr
    let scalar_predicate =
        convert_expr_to_scalar_with_bindings(&logical_filter.predicate, bindings).map_err(|e| {
            format!(
                "Failed to convert filter predicate to scalar expression: {}",
                e
            )
        })?;

    let index = builder.allocate_index();
    let physical_filter = PhysicalFilter::new(
        logical_filter.predicate.clone(),
        scalar_predicate,
        physical_children,
        index,
    );
    Ok(Arc::new(PhysicalPlan::Filter(physical_filter)))
}

/// Create a PhysicalProject from a LogicalProject using centralized index management with caching
fn create_physical_project_with_builder_cached(
    logical_project: &LogicalProject,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, registries, builder)?;
        physical_children.push(physical_child);
    }

    // Convert logical fields to physical fields
    let mut physical_fields = Vec::new();
    for logical_field in &logical_project.fields {
        let physical_field = PhysicalProjectField::from_logical(
            logical_field.field_name.clone(),
            logical_field.expr.clone(),
            bindings,
        )?;
        physical_fields.push(physical_field);
    }

    let index = builder.allocate_index();
    let physical_project = PhysicalProject::new(physical_fields, physical_children, index);
    Ok(Arc::new(PhysicalPlan::Project(physical_project)))
}

/// Create a PhysicalDataSink from a DataSinkPlan using centralized index management with caching
fn create_physical_data_sink_with_builder_cached(
    logical_sink: &DataSinkPlan,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, registries, builder)?;
        physical_children.push(physical_child);
    }
    if physical_children.len() != 1 {
        return Err("DataSink plan must have exactly one child".to_string());
    }

    let input_child = Arc::clone(&physical_children[0]);
    let sink_index = builder.allocate_index();
    let (encoded_child, connector) =
        build_sink_chain_with_builder(&logical_sink.sink, &input_child, builder)?;
    let physical_sink = PhysicalDataSink::new(encoded_child, sink_index, connector);
    Ok(Arc::new(PhysicalPlan::DataSink(physical_sink)))
}

/// Build sink chain using centralized index management
fn build_sink_chain_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
) -> Result<(Arc<PhysicalPlan>, PhysicalSinkConnector), String> {
    let mut encoder_children = Vec::new();
    let mut connectors = Vec::new();
    let batch_processor = create_batch_processor_if_needed_with_builder(sink, input_child, builder);

    let connector = &sink.connector;
    let encoder_input = batch_processor
        .as_ref()
        .map(Arc::clone)
        .unwrap_or_else(|| Arc::clone(input_child));
    add_regular_encoder_with_builder(
        sink,
        connector,
        0,
        encoder_input,
        builder,
        &mut encoder_children,
        &mut connectors,
    );

    if encoder_children.len() != 1 || connectors.len() != 1 {
        return Err(format!(
            "Sink {} must define exactly one connector",
            sink.sink_id
        ));
    }

    Ok((encoder_children.remove(0), connectors.remove(0)))
}

/// Create batch processor if needed using centralized index management
fn create_batch_processor_if_needed_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
) -> Option<Arc<PhysicalPlan>> {
    let needs_batch = sink.common.is_batching_enabled();

    if !needs_batch {
        return None;
    }

    let batch_index = builder.allocate_index();
    let batch_plan = PhysicalBatch::new(
        vec![Arc::clone(input_child)],
        batch_index,
        sink.sink_id.clone(),
        sink.common.clone(),
    );
    Some(Arc::new(PhysicalPlan::Batch(batch_plan)))
}

/// Add regular encoder using centralized index management
fn add_regular_encoder_with_builder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
    _connector_idx: usize,
    encoder_input: Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
    encoder_children: &mut Vec<Arc<PhysicalPlan>>,
    connectors: &mut Vec<PhysicalSinkConnector>,
) {
    let encoder_index = builder.allocate_index();
    let encoder = PhysicalEncoder::new(
        vec![encoder_input],
        encoder_index,
        sink.sink_id.clone(),
        connector.encoder.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::Encoder(encoder)));
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector.clone(),
        encoder_index,
    ));
}

fn find_binding_entry<'a>(
    logical_ds: &LogicalDataSource,
    bindings: &'a SchemaBinding,
) -> Result<&'a SchemaBindingEntry, String> {
    if let Some(alias) = logical_ds.alias.as_deref() {
        if let Some(entry) = bindings
            .entries()
            .iter()
            .find(|entry| entry.alias.as_ref().map(|a| a == alias).unwrap_or(false))
        {
            return Ok(entry);
        }
    }
    bindings
        .entries()
        .iter()
        .find(|entry| entry.source_name == logical_ds.source_name)
        .ok_or_else(|| {
            format!(
                "Schema binding not found for source {}",
                logical_ds.source_name
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_plan_builder_creation() {
        let mut builder = PhysicalPlanBuilder::new();
        let index1 = builder.allocate_index();
        let index2 = builder.allocate_index();

        assert_eq!(index1, 0);
        assert_eq!(index2, 1);
    }
}
