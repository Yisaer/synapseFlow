//! Physical plan builder - converts logical plans to physical plans using centralized index management
use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings_and_custom_registry, SchemaBinding, SchemaBindingEntry,
    SourceBindingKind,
};
use crate::planner::logical::{
    aggregation::Aggregation as LogicalAggregation, DataSinkPlan, DataSource as LogicalDataSource,
    Filter as LogicalFilter, LogicalPlan, LogicalWindow, LogicalWindowSpec,
    Project as LogicalProject, StatefulFunctionPlan as LogicalStatefulFunction,
};
use crate::planner::physical::physical_project::PhysicalProjectField;
use crate::planner::physical::{
    PhysicalAggregation, PhysicalBatch, PhysicalDataSink, PhysicalDataSource, PhysicalDecoder,
    PhysicalDecoderEventtimeSpec, PhysicalEncoder, PhysicalEventtimeWatermark, PhysicalFilter,
    PhysicalPlan, PhysicalProcessTimeWatermark, PhysicalProject, PhysicalResultCollect,
    PhysicalSharedStream, PhysicalSinkConnector, PhysicalStatefulFunction, StatefulCall,
    WatermarkConfig, WatermarkStrategy,
};
use crate::planner::sink::{PipelineSink, PipelineSinkConnector};
use crate::PipelineRegistries;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct PhysicalPlanBuildOptions {
    pub eventtime_enabled: bool,
    pub eventtime_late_tolerance: Duration,
}

impl Default for PhysicalPlanBuildOptions {
    fn default() -> Self {
        Self {
            eventtime_enabled: false,
            eventtime_late_tolerance: Duration::ZERO,
        }
    }
}

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
    create_physical_plan_with_builder_cached_with_options(
        logical_plan,
        bindings,
        registries,
        &PhysicalPlanBuildOptions::default(),
        &mut builder,
    )
}

pub fn create_physical_plan_with_build_options(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    options: &PhysicalPlanBuildOptions,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut builder = PhysicalPlanBuilder::new();
    create_physical_plan_with_builder_cached_with_options(
        logical_plan,
        bindings,
        registries,
        options,
        &mut builder,
    )
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
    create_physical_plan_with_builder_cached_with_options(
        logical_plan,
        bindings,
        registries,
        &PhysicalPlanBuildOptions::default(),
        builder,
    )
}

/// Create a physical plan from a logical plan using centralized index management with node caching
///
/// This function ensures that shared logical nodes are converted to shared physical nodes,
/// maintaining the same instance across multiple references.
fn create_physical_plan_with_builder_cached_with_options(
    logical_plan: Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    options: &PhysicalPlanBuildOptions,
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
                registries,
                options,
                builder,
            )?
        }
        LogicalPlan::StatefulFunction(logical_stateful) => {
            create_physical_stateful_function_with_builder(
                logical_stateful,
                &logical_plan,
                bindings,
                registries,
                options,
                builder,
            )?
        }
        LogicalPlan::Filter(logical_filter) => create_physical_filter_with_builder_cached(
            logical_filter,
            &logical_plan,
            bindings,
            registries,
            options,
            builder,
        )?,
        LogicalPlan::Project(logical_project) => create_physical_project_with_builder_cached(
            logical_project,
            &logical_plan,
            bindings,
            registries,
            options,
            builder,
        )?,
        LogicalPlan::Aggregation(logical_agg) => create_physical_aggregation_with_builder(
            logical_agg,
            &logical_plan,
            bindings,
            registries,
            options,
            builder,
        )?,
        LogicalPlan::DataSink(logical_sink) => create_physical_data_sink_with_builder_cached(
            logical_sink,
            &logical_plan,
            bindings,
            registries,
            options,
            builder,
        )?,
        LogicalPlan::Tail(_logical_tail) => {
            // TailPlan is no longer used in new design, but handle it for backward compatibility
            // Convert to multiple DataSink nodes under a ResultCollect
            create_physical_result_collect_from_tail_with_builder_cached(
                &logical_plan,
                bindings,
                registries,
                options,
                builder,
            )?
        }
        LogicalPlan::Window(logical_window) => create_physical_window_with_builder(
            logical_window,
            &logical_plan,
            bindings,
            registries,
            options,
            builder,
        )?,
    };

    // Cache the result for future reuse using builder's cache
    builder.cache_node(logical_index, Arc::clone(&physical_plan));
    Ok(physical_plan)
}

fn create_physical_stateful_function_with_builder(
    logical_stateful: &LogicalStatefulFunction,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
        physical_children.push(physical_child);
    }

    let mut entries: Vec<_> = logical_stateful.stateful_mappings.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut calls = Vec::with_capacity(entries.len());
    for (output_column, expr) in entries {
        let sqlparser::ast::Expr::Function(func) = expr else {
            return Err(format!(
                "stateful mapping '{}' must be a function expression, got {}",
                output_column, expr
            ));
        };

        let func_name = func
            .name
            .0
            .last()
            .map(|ident| ident.value.to_lowercase())
            .unwrap_or_default();
        registries
            .stateful_registry()
            .get(&func_name)
            .ok_or_else(|| format!("unknown stateful function '{}'", func_name))?;

        let mut arg_scalars = Vec::with_capacity(func.args.len());
        for arg in &func.args {
            match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    arg_expr,
                )) => {
                    arg_scalars.push(
                        convert_expr_to_scalar_with_bindings_and_custom_registry(
                            arg_expr,
                            bindings,
                            registries.custom_func_registry().as_ref(),
                        )
                        .map_err(|err| err.to_string())?,
                    );
                }
                _ => {
                    return Err(format!(
                        "unsupported stateful function argument for {}: {}",
                        func_name, arg
                    ));
                }
            }
        }

        calls.push(StatefulCall {
            output_column: output_column.clone(),
            func_name,
            arg_scalars,
            original_expr: expr.clone(),
        });
    }

    let index = builder.allocate_index();
    let physical = PhysicalStatefulFunction::new(calls, physical_children, index);
    Ok(Arc::new(PhysicalPlan::StatefulFunction(physical)))
}

/// Create a PhysicalResultCollect from a TailPlan using centralized index management with caching
fn create_physical_result_collect_from_tail_with_builder_cached(
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
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
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
        physical_children.push(physical_child);
    }

    let physical = match &logical_window.spec {
        LogicalWindowSpec::Tumbling { time_unit, length } => {
            let watermark_index = builder.allocate_index();
            let strategy = if options.eventtime_enabled {
                WatermarkStrategy::EventTime {
                    late_tolerance: options.eventtime_late_tolerance,
                }
            } else {
                WatermarkStrategy::ProcessingTime {
                    time_unit: *time_unit,
                    interval: *length,
                }
            };
            let watermark_config = WatermarkConfig::Tumbling {
                time_unit: *time_unit,
                length: *length,
                strategy,
            };
            let watermark_plan = if options.eventtime_enabled {
                PhysicalPlan::EventtimeWatermark(PhysicalEventtimeWatermark::new(
                    watermark_config,
                    physical_children,
                    watermark_index,
                ))
            } else {
                PhysicalPlan::ProcessTimeWatermark(PhysicalProcessTimeWatermark::new(
                    watermark_config,
                    physical_children,
                    watermark_index,
                ))
            };
            let index = builder.allocate_index();
            let tumbling = crate::planner::physical::PhysicalTumblingWindow::new(
                *time_unit,
                *length,
                vec![Arc::new(watermark_plan)],
                index,
            );
            PhysicalPlan::TumblingWindow(tumbling)
        }
        LogicalWindowSpec::Count { count } => {
            let index = builder.allocate_index();
            let count_window = crate::planner::physical::PhysicalCountWindow::new(
                *count,
                physical_children,
                index,
            );
            PhysicalPlan::CountWindow(count_window)
        }
        LogicalWindowSpec::Sliding {
            time_unit,
            lookback,
            lookahead,
        } => {
            let watermark_index = builder.allocate_index();
            let strategy = if options.eventtime_enabled {
                WatermarkStrategy::EventTime {
                    late_tolerance: options.eventtime_late_tolerance,
                }
            } else {
                WatermarkStrategy::ProcessingTime {
                    time_unit: *time_unit,
                    interval: 1,
                }
            };
            let watermark_config = WatermarkConfig::Sliding {
                time_unit: *time_unit,
                lookback: *lookback,
                lookahead: *lookahead,
                strategy,
            };
            let watermark_plan = if options.eventtime_enabled {
                PhysicalPlan::EventtimeWatermark(PhysicalEventtimeWatermark::new(
                    watermark_config,
                    physical_children,
                    watermark_index,
                ))
            } else {
                PhysicalPlan::ProcessTimeWatermark(PhysicalProcessTimeWatermark::new(
                    watermark_config,
                    physical_children,
                    watermark_index,
                ))
            };
            let sliding_children = vec![Arc::new(watermark_plan)];
            let index = builder.allocate_index();

            let sliding = crate::planner::physical::PhysicalSlidingWindow::new(
                *time_unit,
                *lookback,
                *lookahead,
                sliding_children,
                index,
            );
            PhysicalPlan::SlidingWindow(sliding)
        }
        LogicalWindowSpec::State {
            open,
            emit,
            partition_by,
        } => {
            let open_scalar = convert_expr_to_scalar_with_bindings_and_custom_registry(
                open.as_ref(),
                bindings,
                registries.custom_func_registry().as_ref(),
            )
            .map_err(|err| err.to_string())?;
            let emit_scalar = convert_expr_to_scalar_with_bindings_and_custom_registry(
                emit.as_ref(),
                bindings,
                registries.custom_func_registry().as_ref(),
            )
            .map_err(|err| err.to_string())?;

            let mut partition_by_scalars = Vec::with_capacity(partition_by.len());
            for expr in partition_by {
                partition_by_scalars.push(
                    convert_expr_to_scalar_with_bindings_and_custom_registry(
                        expr,
                        bindings,
                        registries.custom_func_registry().as_ref(),
                    )
                    .map_err(|err| err.to_string())?,
                );
            }

            let index = builder.allocate_index();
            let state = crate::planner::physical::PhysicalStateWindow::new(
                open.as_ref().clone(),
                emit.as_ref().clone(),
                partition_by.clone(),
                open_scalar,
                emit_scalar,
                partition_by_scalars,
                physical_children,
                index,
            );
            PhysicalPlan::StateWindow(Box::new(state))
        }
    };

    Ok(Arc::new(physical))
}

fn create_physical_aggregation_with_builder(
    logical_agg: &LogicalAggregation,
    logical_plan: &Arc<LogicalPlan>,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
        physical_children.push(physical_child);
    }
    let index = builder.allocate_index();
    let physical = PhysicalAggregation::new(
        logical_agg.aggregate_mappings.clone(),
        logical_agg.group_by_exprs.clone(),
        physical_children,
        index,
        bindings,
        registries.aggregate_registry().as_ref(),
        registries.custom_func_registry().as_ref(),
    )?;
    Ok(Arc::new(PhysicalPlan::Aggregation(physical)))
}

/// Create a PhysicalDataSource from a LogicalDataSource using centralized index management
fn create_physical_data_source_with_builder(
    logical_ds: &LogicalDataSource,
    _logical_plan: &Arc<LogicalPlan>,
    index: i64,
    bindings: &SchemaBinding,
    registries: &PipelineRegistries,
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    let entry = find_binding_entry(logical_ds, bindings)?;
    let decoder_kind = logical_ds.decoder().kind();
    if decoder_kind != "none" && !registries.decoder_registry().is_registered(decoder_kind) {
        return Err(format!(
            "decoder kind `{}` not registered for stream `{}`",
            decoder_kind, logical_ds.source_name
        ));
    }
    let schema = entry.schema.clone();
    let eventtime = if options.eventtime_enabled {
        logical_ds
            .eventtime()
            .map(|cfg| -> Result<PhysicalDecoderEventtimeSpec, String> {
                let column_name = cfg.column().to_string();
                let type_key = cfg.eventtime_type().to_string();
                let column_index = schema.column_index(column_name.as_str()).ok_or_else(|| {
                    format!(
                        "eventtime.column `{}` not found in pruned schema for `{}`",
                        column_name, logical_ds.source_name
                    )
                })?;
                Ok(PhysicalDecoderEventtimeSpec {
                    column_name,
                    type_key,
                    column_index,
                })
            })
            .transpose()?
    } else {
        None
    };
    match entry.kind {
        SourceBindingKind::Regular => {
            let physical_ds = PhysicalDataSource::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                Arc::clone(&schema),
                logical_ds.decode_projection.clone(),
                index,
            );
            let datasource_plan = Arc::new(PhysicalPlan::DataSource(physical_ds));
            if decoder_kind == "none" {
                return Ok(datasource_plan);
            }
            let decoder_index = builder.allocate_index();
            let decoder = PhysicalDecoder::new(
                logical_ds.source_name.clone(),
                logical_ds.decoder().clone(),
                schema,
                logical_ds.decode_projection.clone(),
                eventtime,
                vec![datasource_plan],
                decoder_index,
            );
            Ok(Arc::new(PhysicalPlan::Decoder(decoder)))
        }
        SourceBindingKind::Shared => {
            if decoder_kind == "none" {
                return Err(format!(
                    "shared stream `{}` does not support decoder type `none`",
                    logical_ds.source_name
                ));
            }
            let required_columns = logical_ds
                .shared_required_schema()
                .map(|cols| cols.to_vec())
                .unwrap_or_else(|| {
                    schema
                        .column_schemas()
                        .iter()
                        .map(|col| col.name.clone())
                        .collect()
                });
            let explain_ingest_plan = {
                let ds = PhysicalDataSource::new(
                    logical_ds.source_name.clone(),
                    logical_ds.alias.clone(),
                    Arc::clone(&schema),
                    logical_ds.decode_projection.clone(),
                    0,
                );
                let ds_plan = Arc::new(PhysicalPlan::DataSource(ds));
                let decoder = PhysicalDecoder::new(
                    logical_ds.source_name.clone(),
                    logical_ds.decoder().clone(),
                    Arc::clone(&schema),
                    logical_ds.decode_projection.clone(),
                    eventtime.clone(),
                    vec![ds_plan],
                    1,
                );
                Arc::new(PhysicalPlan::Decoder(decoder))
            };
            let physical_shared = PhysicalSharedStream::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                schema,
                required_columns,
                logical_ds.decoder().clone(),
                Some(explain_ingest_plan),
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
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
        physical_children.push(physical_child);
    }

    // Convert SQL Expr to ScalarExpr
    let scalar_predicate = convert_expr_to_scalar_with_bindings_and_custom_registry(
        &logical_filter.predicate,
        bindings,
        registries.custom_func_registry().as_ref(),
    )
    .map_err(|e| {
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
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
        physical_children.push(physical_child);
    }

    // Convert logical fields to physical fields
    let mut physical_fields = Vec::new();
    for logical_field in &logical_project.fields {
        let physical_field = PhysicalProjectField::from_logical(
            logical_field.field_name.clone(),
            logical_field.expr.clone(),
            bindings,
            registries.custom_func_registry().as_ref(),
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
    options: &PhysicalPlanBuildOptions,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    // Convert children first using the builder with caching
    let mut physical_children = Vec::new();
    for child in logical_plan.children() {
        let physical_child = create_physical_plan_with_builder_cached_with_options(
            child.clone(),
            bindings,
            registries,
            options,
            builder,
        )?;
        physical_children.push(physical_child);
    }
    if physical_children.len() != 1 {
        return Err("DataSink plan must have exactly one child".to_string());
    }

    let input_child = Arc::clone(&physical_children[0]);
    let sink_index = builder.allocate_index();
    let (encoded_child, connector) =
        build_sink_chain_with_builder(&logical_sink.sink, &input_child, registries, builder)?;
    let physical_sink = PhysicalDataSink::new(encoded_child, sink_index, connector);
    Ok(Arc::new(PhysicalPlan::DataSink(physical_sink)))
}

/// Build sink chain using centralized index management
fn build_sink_chain_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<(Arc<PhysicalPlan>, PhysicalSinkConnector), String> {
    let batch_processor = create_batch_processor_if_needed_with_builder(sink, input_child, builder);

    let connector = &sink.connector;
    let connector_kind = connector.connector.kind();
    if !registries
        .connector_registry()
        .is_registered(connector_kind)
    {
        return Err(format!(
            "sink connector kind `{}` not registered for sink `{}`",
            connector_kind, sink.sink_id
        ));
    }

    let encoder_input = batch_processor
        .as_ref()
        .map(Arc::clone)
        .unwrap_or_else(|| Arc::clone(input_child));
    add_regular_encoder_with_builder(sink, connector, encoder_input, registries, builder)
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
    encoder_input: Arc<PhysicalPlan>,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<(Arc<PhysicalPlan>, PhysicalSinkConnector), String> {
    if matches!(
        connector.encoder.kind(),
        crate::planner::sink::SinkEncoderKind::None
    ) {
        Ok((
            encoder_input,
            PhysicalSinkConnector::new(
                sink.sink_id.clone(),
                sink.forward_to_result,
                connector.connector.clone(),
                None,
            ),
        ))
    } else {
        let encoder_kind = connector.encoder.kind_str();
        if !registries.encoder_registry().is_registered(encoder_kind) {
            return Err(format!(
                "encoder kind `{}` not registered for sink `{}`",
                encoder_kind, sink.sink_id
            ));
        }
        let encoder_index = builder.allocate_index();
        let encoder = PhysicalEncoder::new(
            vec![encoder_input],
            encoder_index,
            sink.sink_id.clone(),
            connector.encoder.clone(),
        );
        Ok((
            Arc::new(PhysicalPlan::Encoder(encoder)),
            PhysicalSinkConnector::new(
                sink.sink_id.clone(),
                sink.forward_to_result,
                connector.connector.clone(),
                Some(encoder_index),
            ),
        ))
    }
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
