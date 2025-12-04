//! Physical plan builder - converts logical plans to physical plans

use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings, SchemaBinding, SchemaBindingEntry, SourceBindingKind,
};
use crate::planner::logical::{
    DataSinkPlan, DataSource as LogicalDataSource, Filter as LogicalFilter, LogicalPlan,
    Project as LogicalProject,
};
use crate::planner::physical::physical_project::PhysicalProjectField;
use crate::planner::physical::{
    PhysicalBatch, PhysicalDataSink, PhysicalDataSource, PhysicalEncoder, PhysicalFilter,
    PhysicalPlan, PhysicalProject, PhysicalResultCollect, PhysicalSharedStream,
    PhysicalSinkConnector, PhysicalStreamingEncoder,
};
use crate::planner::sink::{PipelineSink, PipelineSinkConnector};
use crate::planner::PhysicalPlanBuilder;
use std::sync::Arc;

/// Create a physical plan from a logical plan using centralized index management
///
/// This function walks through the logical plan tree and creates corresponding physical plan nodes
/// by pattern matching on the logical plan enum, using a centralized index allocator.
pub fn create_physical_plan_with_builder(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    create_physical_plan_with_builder_cached(logical_plan, bindings, builder)
}

/// Create a physical plan from a logical plan using centralized index management with node caching
///
/// This function ensures that shared logical nodes are converted to shared physical nodes,
/// maintaining the same instance across multiple references.
fn create_physical_plan_with_builder_cached(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
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
            builder,
        )?,
        LogicalPlan::Project(logical_project) => create_physical_project_with_builder_cached(
            logical_project,
            &logical_plan,
            bindings,
            builder,
        )?,
        LogicalPlan::DataSink(logical_sink) => create_physical_data_sink_with_builder_cached(
            logical_sink,
            &logical_plan,
            bindings,
            builder,
        )?,
        LogicalPlan::Tail(_logical_tail) => {
            // TailPlan is no longer used in new design, but handle it for backward compatibility
            // Convert to multiple DataSink nodes under a ResultCollect
            create_physical_result_collect_from_tail_with_builder_cached(
                &logical_plan,
                bindings,
                builder,
            )?
        }
    };

    // Cache the result for future reuse using builder's cache
    builder.cache_node(logical_index, Arc::clone(&physical_plan));
    Ok(physical_plan)
}

/// Convenience wrapper that builds a physical plan using a fresh builder.
pub fn create_physical_plan(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut builder = PhysicalPlanBuilder::new();
    create_physical_plan_with_builder_cached(logical_plan, bindings, &mut builder)
}

/// Create a PhysicalResultCollect from a TailPlan using centralized index management with caching
fn create_physical_result_collect_from_tail_with_builder_cached(
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, builder)?;
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
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, builder)?;
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
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, builder)?;
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
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child =
            create_physical_plan_with_builder_cached(child.clone(), bindings, builder)?;
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
    if should_use_streaming_encoder(sink, connector) {
        add_streaming_encoder_with_builder(
            sink,
            connector,
            input_child,
            builder,
            &mut encoder_children,
            &mut connectors,
        );
    } else {
        let encoder_input = batch_processor
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::clone(input_child));
        add_regular_encoder_with_builder(
            sink,
            connector,
            encoder_input,
            builder,
            &mut encoder_children,
            &mut connectors,
        );
    }

    if encoder_children.len() != 1 || connectors.len() != 1 {
        return Err(format!(
            "Sink {} must define exactly one connector",
            sink.sink_id
        ));
    }

    Ok((encoder_children.remove(0), connectors.remove(0)))
}

fn should_use_streaming_encoder(sink: &PipelineSink, connector: &PipelineSinkConnector) -> bool {
    sink.common.is_batching_enabled() && connector.encoder.supports_streaming()
}

/// Create batch processor if needed using centralized index management
fn create_batch_processor_if_needed_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
) -> Option<Arc<PhysicalPlan>> {
    let needs_batch =
        sink.common.is_batching_enabled() && !sink.connector.encoder.supports_streaming();

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

/// Add streaming encoder using centralized index management
fn add_streaming_encoder_with_builder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
    encoder_children: &mut Vec<Arc<PhysicalPlan>>,
    connectors: &mut Vec<PhysicalSinkConnector>,
) {
    let streaming_index = builder.allocate_index();
    let streaming = PhysicalStreamingEncoder::new(
        vec![Arc::clone(input_child)],
        streaming_index,
        sink.sink_id.clone(),
        connector.connector_id.clone(),
        connector.encoder.clone(),
        sink.common.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::StreamingEncoder(streaming)));

    let connector_index = builder.allocate_index();
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector.clone(),
        connector_index,
    ));
}

/// Add regular encoder using centralized index management
fn add_regular_encoder_with_builder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
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
        connector.connector_id.clone(),
        connector.encoder.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::Encoder(encoder)));

    let connector_index = builder.allocate_index();
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector.clone(),
        connector_index,
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
    use crate::planner::logical::create_logical_plan;
    use crate::planner::sink::{
        PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
    };
    use parser::parse_sql;
    use std::sync::Arc;

    /// Helper function to collect all plan names in the physical plan tree
    fn collect_plan_names(plan: &Arc<PhysicalPlan>, names: &mut Vec<String>) {
        names.push(plan.get_plan_name());
        for child in plan.children() {
            collect_plan_names(child, names);
        }
    }

    /// Helper function to print physical plan topology for debugging
    fn print_physical_plan_topology(plan: &Arc<PhysicalPlan>, indent: usize) {
        let spacing = "  ".repeat(indent);
        println!(
            "{}{} (index: {})",
            spacing,
            plan.get_plan_type(),
            plan.get_plan_index()
        );

        for child in plan.children() {
            print_physical_plan_topology(child, indent + 1);
        }
    }

    #[test]
    fn test_two_sinks_physical_plan_topology() {
        // Parse SQL
        let sql = "SELECT * FROM stream";
        let select_stmt = parse_sql(sql).unwrap();

        // Create two different sinks
        let sink1 = PipelineSink::new(
            "sink1",
            PipelineSinkConnector::new(
                "conn1",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json {
                    encoder_id: "json1".to_string(),
                },
            ),
        );

        let sink2 = PipelineSink::new(
            "sink2",
            PipelineSinkConnector::new(
                "conn2",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json {
                    encoder_id: "json2".to_string(),
                },
            ),
        );

        // Create logical plan with two sinks
        let logical_plan = create_logical_plan(select_stmt, vec![sink1, sink2]).unwrap();

        println!("=== Logical Plan Topology ===");
        crate::planner::logical::print_logical_plan(&logical_plan, 0);
        println!("=============================");

        // Create physical plan with proper schema binding
        use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
        use datatypes::Schema;

        let entry = SchemaBindingEntry {
            source_name: "stream".to_string(),
            alias: None,
            schema: Arc::new(Schema::new(vec![])),
            kind: SourceBindingKind::Regular,
        };
        let binding = SchemaBinding::new(vec![entry]);
        let physical_plan = create_physical_plan(logical_plan, &binding).unwrap();

        println!("\n=== Physical Plan Topology ===");
        print_physical_plan_topology(&physical_plan, 0);
        println!("==============================");

        // Collect all plan names
        let mut plan_names = Vec::new();
        collect_plan_names(&physical_plan, &mut plan_names);

        println!("\n=== Plan Names Analysis ===");
        println!("All plan names: {:?}", plan_names);

        // Count PhysicalDataSink occurrences
        let data_sink_count = plan_names
            .iter()
            .filter(|name| name.starts_with("PhysicalDataSink"))
            .count();
        println!("PhysicalDataSink count: {}", data_sink_count);

        // Get PhysicalDataSink names
        let data_sink_names: Vec<String> = plan_names
            .iter()
            .filter(|name| name.starts_with("PhysicalDataSink"))
            .cloned()
            .collect();

        println!("PhysicalDataSink names: {:?}", data_sink_names);

        if data_sink_names.len() >= 2 {
            println!(
                "Are the two PhysicalDataSink names different? {}",
                data_sink_names[0] != data_sink_names[1]
            );

            // This is the key assertion - they should have different indices/names
            assert_ne!(
                data_sink_names[0], data_sink_names[1],
                "Two different sinks should have different PhysicalDataSink plan names"
            );
        }

        // Check for duplicate names
        let mut name_counts = std::collections::HashMap::new();
        for name in &plan_names {
            *name_counts.entry(name.clone()).or_insert(0) += 1;
        }

        println!("\nName frequency:");
        for (name, count) in &name_counts {
            if *count > 1 {
                println!("  {}: {} times (DUPLICATE!)", name, count);
            } else {
                println!("  {}: {} times", name, count);
            }
        }

        // Verify that we have the expected structure
        assert!(
            plan_names
                .iter()
                .any(|name| name.starts_with("PhysicalResultCollect")),
            "Should have PhysicalResultCollect node"
        );

        assert_eq!(
            data_sink_count, 2,
            "Should have exactly 2 PhysicalDataSink nodes"
        );

        // Each sink should contribute unique encoder and sink nodes
        let mut unique_names = std::collections::HashSet::new();
        for name in &plan_names {
            if name.starts_with("PhysicalDataSink") || name.starts_with("PhysicalEncoder") {
                unique_names.insert(name.clone());
            }
        }

        let expected_unique_sink_related = 4; // 2 sinks * (1 DataSink + 1 Encoder)
        assert_eq!(
            unique_names.len(),
            expected_unique_sink_related,
            "Should have unique names for sink-related nodes"
        );
    }
}
