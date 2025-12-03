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
    PhysicalPlan, PhysicalProject, PhysicalResultCollect, PhysicalSharedStream, PhysicalSinkConnector,
    PhysicalStreamingEncoder,
};
use crate::planner::sink::{PipelineSink, PipelineSinkConnector};
use std::sync::Arc;

/// Create a physical plan from a logical plan
///
/// This function walks through the logical plan tree and creates corresponding physical plan nodes
/// by pattern matching on the logical plan enum.
pub fn create_physical_plan(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    match logical_plan.as_ref() {
        LogicalPlan::DataSource(logical_ds) => {
            create_physical_data_source(logical_ds, logical_plan.get_plan_index(), bindings)
        }
        LogicalPlan::Filter(logical_filter) => create_physical_filter(
            logical_filter,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
        LogicalPlan::Project(logical_project) => create_physical_project(
            logical_project,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
        LogicalPlan::DataSink(logical_sink) => create_physical_data_sink(
            logical_sink,
            &logical_plan,
            logical_plan.get_plan_index(),
            bindings,
        ),
        LogicalPlan::Tail(_logical_tail) => {
            // TailPlan is no longer used in new design, but handle it for backward compatibility
            // Convert to multiple DataSink nodes under a ResultCollect
            create_physical_result_collect_from_tail(&logical_plan, logical_plan.get_plan_index(), bindings)
        }
    }
}

/// Create a PhysicalResultCollect from a TailPlan for backward compatibility
fn create_physical_result_collect_from_tail(
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    // TailPlan should only have DataSink children
    let mut sink_children = Vec::new();
    for child in logical_plan.children() {
        match child.as_ref() {
            LogicalPlan::DataSink(data_sink) => {
                sink_children.push(data_sink.sink.clone());
            }
            _ => return Err("TailPlan should only contain DataSink children".to_string()),
        }
    }

    if sink_children.is_empty() {
        return Err("TailPlan must have at least one DataSink child".to_string());
    }

    // Get the base plan (Project) from the first DataSink child
    let base_plan = logical_plan.children()[0].children()[0].clone();
    let base_physical = create_physical_plan(base_plan, bindings)?;

    // Handle single sink vs multiple sinks differently
    if sink_children.len() == 1 {
        // Single sink: directly create sink node to avoid double nesting
        create_physical_sink_node(&sink_children, base_physical, index)
    } else {
        // Multiple sinks: create individual sink nodes under ResultCollect
        let mut physical_sinks = Vec::new();
        let mut next_index = index + 1;

        // Build encoders and connectors for each sink
        for sink in &sink_children {
            let (mut encoder_children, mut connectors) = 
                build_sink_encoders_for_sink(sink, &base_physical, &mut next_index)?;
            
            if encoder_children.len() != 1 || connectors.len() != 1 {
                return Err("Each sink should have exactly one encoder and connector".to_string());
            }

            let encoder_child = encoder_children.remove(0);
            let connector = connectors.remove(0);
            let sink_index = next_index;
            next_index += 1;
            
            let physical_sink = PhysicalDataSink::new(encoder_child, sink_index, connector);
            physical_sinks.push(Arc::new(PhysicalPlan::DataSink(physical_sink)));
        }

        // Create ResultCollect to hold all sink nodes
        let result_collect_index = next_index;
        let result_collect = PhysicalResultCollect::new(physical_sinks, result_collect_index);
        Ok(Arc::new(PhysicalPlan::ResultCollect(result_collect)))
    }
}

/// Create a PhysicalDataSource from a LogicalDataSource
fn create_physical_data_source(
    logical_ds: &LogicalDataSource,
    index: i64,
    bindings: &SchemaBinding,
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

/// Create a PhysicalFilter from a LogicalFilter
fn create_physical_filter(
    logical_filter: &LogicalFilter,
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
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

    let physical_filter = PhysicalFilter::new(
        logical_filter.predicate.clone(),
        scalar_predicate,
        physical_children,
        index,
    );
    Ok(Arc::new(PhysicalPlan::Filter(physical_filter)))
}

/// Create a PhysicalProject from a LogicalProject
fn create_physical_project(
    logical_project: &LogicalProject,
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
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

    let physical_project = PhysicalProject::new(physical_fields, physical_children, index);
    Ok(Arc::new(PhysicalPlan::Project(physical_project)))
}

fn create_physical_data_sink(
    logical_sink: &DataSinkPlan,
    logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan(child.clone(), bindings)?;
        physical_children.push(physical_child);
    }
    if physical_children.len() != 1 {
        return Err("DataSink plan must have exactly one child".to_string());
    }

    let input_child = Arc::clone(&physical_children[0]);
    let sinks = vec![logical_sink.sink.clone()];
    create_physical_sink_node(&sinks, input_child, index)
}

fn create_physical_sink_node(
    sinks: &[PipelineSink],
    input_child: Arc<PhysicalPlan>,
    index: i64,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut connectors = Vec::new();
    let mut encoder_children = Vec::new();
    let mut next_index = index + 1;
    
    // Check if any sink needs forward_to_result
    let needs_result_collect = sinks.iter().any(|sink| sink.forward_to_result);
    
    for sink in sinks {
        let (mut sink_encoders, mut sink_connectors) =
            build_sink_encoders_for_sink(sink, &input_child, &mut next_index)?;
        encoder_children.append(&mut sink_encoders);
        connectors.append(&mut sink_connectors);
    }
    
    if connectors.is_empty() {
        return Err("DataSink plan must define at least one connector".to_string());
    }
    
    if needs_result_collect {
        // Create ResultCollect node to gather outputs from all sinks
        let mut sink_nodes = Vec::new();

        // Create individual sink nodes for each connector
        for (encoder_child, connector) in encoder_children.into_iter().zip(connectors.into_iter()) {
            let sink_index = next_index;
            next_index += 1;
            let physical_sink = PhysicalDataSink::new(encoder_child, sink_index, connector);
            sink_nodes.push(Arc::new(PhysicalPlan::DataSink(physical_sink)));
        }

        // Create ResultCollect node with the sink nodes as children
        let result_collect_index = next_index;
        let result_collect = PhysicalResultCollect::new(sink_nodes, result_collect_index);
        Ok(Arc::new(PhysicalPlan::ResultCollect(result_collect)))
    } else {
        // Single sink case without result forwarding
        let child = encoder_children.remove(0);
        let connector = connectors.remove(0);
        let physical_sink = PhysicalDataSink::new(child, index, connector);
        Ok(Arc::new(PhysicalPlan::DataSink(physical_sink)))
    }
}

fn build_sink_encoders_for_sink(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    next_index: &mut i64,
) -> Result<(Vec<Arc<PhysicalPlan>>, Vec<PhysicalSinkConnector>), String> {
    let mut encoder_children = Vec::new();
    let mut connectors = Vec::new();
    let shared_batch = create_shared_batch_if_needed(sink, input_child, next_index);

    let connector = &sink.connector;
    if should_use_streaming_encoder(sink, connector) {
        add_streaming_encoder(
            sink,
            connector,
            0, // connector_idx is always 0 for single connector
            input_child,
            next_index,
            &mut encoder_children,
            &mut connectors,
        );
    } else {
        let encoder_input = shared_batch
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::clone(input_child));
        add_regular_encoder(
            sink,
            connector,
            0, // connector_idx is always 0 for single connector
            encoder_input,
            next_index,
            &mut encoder_children,
            &mut connectors,
        );
    }

    if connectors.is_empty() {
        return Err(format!(
            "Sink {} must define at least one connector",
            sink.sink_id
        ));
    }

    Ok((encoder_children, connectors))
}

fn should_use_streaming_encoder(sink: &PipelineSink, connector: &PipelineSinkConnector) -> bool {
    sink.common.is_batching_enabled() && connector.encoder.supports_streaming()
}

fn create_shared_batch_if_needed(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    next_index: &mut i64,
) -> Option<Arc<PhysicalPlan>> {
    let needs_batch =
        sink.common.is_batching_enabled() && !sink.connector.encoder.supports_streaming();

    if !needs_batch {
        return None;
    }

    let batch_plan = PhysicalBatch::new(
        vec![Arc::clone(input_child)],
        *next_index,
        sink.sink_id.clone(),
        sink.common.clone(),
    );
    *next_index += 1;
    Some(Arc::new(PhysicalPlan::Batch(batch_plan)))
}

fn add_streaming_encoder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
    _connector_idx: usize,
    input_child: &Arc<PhysicalPlan>,
    next_index: &mut i64,
    encoder_children: &mut Vec<Arc<PhysicalPlan>>,
    connectors: &mut Vec<PhysicalSinkConnector>,
) {
    let streaming = PhysicalStreamingEncoder::new(
        vec![Arc::clone(input_child)],
        *next_index,
        sink.sink_id.clone(),
        connector.connector_id.clone(),
        connector.encoder.clone(),
        sink.common.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::StreamingEncoder(streaming)));
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector_id.clone(),
        connector.connector.clone(),
        *next_index,
    ));
    *next_index += 1;
}

fn add_regular_encoder(
    sink: &PipelineSink,
    connector: &PipelineSinkConnector,
    _connector_idx: usize,
    encoder_input: Arc<PhysicalPlan>,
    next_index: &mut i64,
    encoder_children: &mut Vec<Arc<PhysicalPlan>>,
    connectors: &mut Vec<PhysicalSinkConnector>,
) {
    let encoder = PhysicalEncoder::new(
        vec![encoder_input],
        *next_index,
        sink.sink_id.clone(),
        connector.connector_id.clone(),
        connector.encoder.clone(),
    );
    encoder_children.push(Arc::new(PhysicalPlan::Encoder(encoder)));
    connectors.push(PhysicalSinkConnector::new(
        sink.sink_id.clone(),
        sink.forward_to_result, // Always forward if sink is configured to do so (single connector)
        connector.connector_id.clone(),
        connector.connector.clone(),
        *next_index,
    ));
    *next_index += 1;
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
    use crate::planner::sink::{PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig};
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
        println!("{}{} (index: {})", spacing, plan.get_plan_type(), plan.get_plan_index());
        
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
                SinkEncoderConfig::Json { encoder_id: "json1".to_string() },
            ),
        );

        let sink2 = PipelineSink::new(
            "sink2",
            PipelineSinkConnector::new(
                "conn2",
                SinkConnectorConfig::Nop(Default::default()),
                SinkEncoderConfig::Json { encoder_id: "json2".to_string() },
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
        let data_sink_count = plan_names.iter()
            .filter(|name| name.starts_with("PhysicalDataSink"))
            .count();
        println!("PhysicalDataSink count: {}", data_sink_count);
        
        // Get PhysicalDataSink names
        let data_sink_names: Vec<String> = plan_names.iter()
            .filter(|name| name.starts_with("PhysicalDataSink"))
            .cloned()
            .collect();
        
        println!("PhysicalDataSink names: {:?}", data_sink_names);
        
        if data_sink_names.len() >= 2 {
            println!("Are the two PhysicalDataSink names different? {}", 
                     data_sink_names[0] != data_sink_names[1]);
            
            // This is the key assertion - they should have different indices/names
            assert_ne!(data_sink_names[0], data_sink_names[1], 
                      "Two different sinks should have different PhysicalDataSink plan names");
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
        assert!(plan_names.iter().any(|name| name.starts_with("PhysicalResultCollect")), 
                "Should have PhysicalResultCollect node");
        
        assert_eq!(data_sink_count, 2, "Should have exactly 2 PhysicalDataSink nodes");
        
        // Each sink should contribute unique encoder and sink nodes
        let mut unique_names = std::collections::HashSet::new();
        for name in &plan_names {
            if name.starts_with("PhysicalDataSink") || name.starts_with("PhysicalEncoder") {
                unique_names.insert(name.clone());
            }
        }
        
        let expected_unique_sink_related = 4; // 2 sinks * (1 DataSink + 1 Encoder)
        assert_eq!(unique_names.len(), expected_unique_sink_related, 
                  "Should have unique names for sink-related nodes");
    }
}
