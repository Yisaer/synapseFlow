//! Physical plan builder - converts logical plans to physical plans using centralized index management
use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings_and_custom_registry, SchemaBinding, SchemaBindingEntry,
    SourceBindingKind,
};
use crate::planner::logical::{
    aggregation::Aggregation as LogicalAggregation, compute::Compute as LogicalCompute,
    order::Order as LogicalOrder, DataSinkPlan, DataSource as LogicalDataSource,
    Filter as LogicalFilter, LogicalPlan, LogicalWindow, LogicalWindowSpec,
    Project as LogicalProject, StatefulFunctionPlan as LogicalStatefulFunction,
};
use crate::planner::physical::physical_compute::PhysicalComputeField;
use crate::planner::physical::physical_project::PhysicalProjectField;
use crate::planner::physical::{
    PartitionGroupKey, PhysicalAggregation, PhysicalBatch, PhysicalCompute, PhysicalDataSink,
    PhysicalDataSource, PhysicalDecoder, PhysicalDecoderEventtimeSpec, PhysicalEmptySuppress,
    PhysicalEventtimeWatermark, PhysicalFilter, PhysicalMemoryCollectionMaterialize, PhysicalOrder,
    PhysicalOrderKey, PhysicalPlan, PhysicalProcessTimeWatermark, PhysicalProject,
    PhysicalResultCollect, PhysicalRowDiff, PhysicalSampler, PhysicalSharedStream,
    PhysicalSharedStreamRequirement, PhysicalSinkCompress, PhysicalSinkConnector,
    PhysicalSinkEncoder, PhysicalSinkEncrypt, PhysicalSourceChangeGate, PhysicalStatefulFunction,
    StatefulCall, WatermarkConfig, WatermarkStrategy,
};
use crate::planner::shared_stream_plan::create_physical_plan_for_shared_stream;
use crate::planner::sink::{CommonSinkProps, PipelineSink, PipelineSinkConnector};
use crate::PipelineRegistries;
use datatypes::ConcreteDatatype;
use std::collections::HashSet;
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
    memory_collection_materialize_cache: std::collections::HashMap<i64, Arc<PhysicalPlan>>,
}

impl PhysicalPlanBuilder {
    pub fn new() -> Self {
        Self {
            next_index: 0,
            node_cache: std::collections::HashMap::new(),
            memory_collection_materialize_cache: std::collections::HashMap::new(),
        }
    }

    pub fn starting_from(start_index: i64) -> Self {
        Self {
            next_index: start_index,
            node_cache: std::collections::HashMap::new(),
            memory_collection_materialize_cache: std::collections::HashMap::new(),
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

    pub fn get_or_create_memory_collection_materialize(
        &mut self,
        child: Arc<PhysicalPlan>,
        output_schema: crate::planner::physical::output_schema::OutputSchema,
    ) -> Arc<PhysicalPlan> {
        let key = child.get_plan_index();
        if let Some(cached) = self.memory_collection_materialize_cache.get(&key) {
            return Arc::clone(cached);
        }

        let index = self.allocate_index();
        let plan = Arc::new(PhysicalPlan::MemoryCollectionMaterialize(
            PhysicalMemoryCollectionMaterialize::new(output_schema, child, index),
        ));
        self.memory_collection_materialize_cache
            .insert(key, Arc::clone(&plan));
        plan
    }
}

impl Default for PhysicalPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn resolve_source_change_gate_columns(
    logical_ds: &LogicalDataSource,
    schema: &datatypes::Schema,
) -> Result<(Vec<Arc<str>>, Vec<usize>), String> {
    let columns: Vec<String> = logical_ds
        .source_input()
        .on_change_columns()
        .map(|cols| cols.to_vec())
        .unwrap_or_else(|| {
            schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect()
        });
    let mut tracked_columns = Vec::with_capacity(columns.len());
    let mut tracked_column_indexes = Vec::with_capacity(columns.len());
    for column in columns {
        let index = schema.column_index(&column).ok_or_else(|| {
            format!(
                "source `{}` input.on_change column `{}` is not present in effective source schema",
                logical_ds.source_name, column
            )
        })?;
        tracked_columns.push(Arc::<str>::from(column));
        tracked_column_indexes.push(index);
    }
    Ok((tracked_columns, tracked_column_indexes))
}

fn wrap_source_change_gate(
    child: Arc<PhysicalPlan>,
    logical_ds: &LogicalDataSource,
    schema: &datatypes::Schema,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    if !logical_ds.source_input().is_on_change() {
        return Ok(child);
    }

    let (tracked_columns, tracked_column_indexes) =
        resolve_source_change_gate_columns(logical_ds, schema)?;
    let gate_index = builder.allocate_index();
    Ok(Arc::new(PhysicalPlan::SourceChangeGate(
        PhysicalSourceChangeGate::new(
            logical_ds.source_name.clone(),
            logical_ds.source_input().clone(),
            tracked_columns,
            tracked_column_indexes,
            vec![child],
            gate_index,
        ),
    )))
}

fn shared_stream_required_columns(
    logical_ds: &LogicalDataSource,
    schema: &datatypes::Schema,
) -> Vec<String> {
    if logical_ds.source_input().is_on_change()
        && logical_ds.source_input().on_change_columns().is_none()
    {
        return schema
            .column_schemas()
            .iter()
            .map(|col| col.name.clone())
            .collect();
    }

    let mut required: HashSet<String> = logical_ds
        .shared_required_schema()
        .map(|cols| cols.iter().cloned().collect())
        .unwrap_or_else(|| {
            schema
                .column_schemas()
                .iter()
                .map(|col| col.name.clone())
                .collect()
        });

    if let Some(columns) = logical_ds.source_input().on_change_columns() {
        required.extend(columns.iter().cloned());
    }

    schema
        .column_schemas()
        .iter()
        .filter(|col| required.contains(&col.name))
        .map(|col| col.name.clone())
        .collect()
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
        LogicalPlan::Compute(logical_compute) => create_physical_compute_with_builder_cached(
            logical_compute,
            &logical_plan,
            bindings,
            registries,
            options,
            builder,
        )?,
        LogicalPlan::Order(logical_order) => create_physical_order_with_builder_cached(
            logical_order,
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

    let mut calls = Vec::with_capacity(logical_stateful.calls.len());
    for entry in &logical_stateful.calls {
        let output_column = &entry.output_column;
        let spec = &entry.spec;
        let func_name = spec.func_name.clone();
        registries
            .stateful_registry()
            .get(&func_name)
            .ok_or_else(|| format!("unknown stateful function '{}'", func_name))?;

        let mut arg_scalars = Vec::with_capacity(spec.args.len());
        for arg_expr in &spec.args {
            arg_scalars.push(
                convert_expr_to_scalar_with_bindings_and_custom_registry(
                    arg_expr,
                    bindings,
                    registries.custom_func_registry().as_ref(),
                )
                .map_err(|err| err.to_string())?,
            );
        }

        let when_scalar = match spec.when.as_ref() {
            Some(expr) => Some(
                convert_expr_to_scalar_with_bindings_and_custom_registry(
                    expr,
                    bindings,
                    registries.custom_func_registry().as_ref(),
                )
                .map_err(|err| err.to_string())?,
            ),
            None => None,
        };

        let mut partition_by_scalars = Vec::with_capacity(spec.partition_by.len());
        for expr in &spec.partition_by {
            partition_by_scalars.push(
                convert_expr_to_scalar_with_bindings_and_custom_registry(
                    expr,
                    bindings,
                    registries.custom_func_registry().as_ref(),
                )
                .map_err(|err| err.to_string())?,
            );
        }

        calls.push(StatefulCall {
            output_column: output_column.clone(),
            func_name,
            arg_scalars,
            when_scalar,
            partition_group_key: PartitionGroupKey::from_partition_by(&spec.partition_by),
            partition_by_scalars,
            spec: spec.clone(),
            original_expr: spec.original_expr.clone(),
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
        SourceBindingKind::Regular | SourceBindingKind::MemoryCollection => {
            let physical_ds = PhysicalDataSource::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                Arc::clone(&schema),
                logical_ds.decode_projection.clone(),
                index,
            );
            let datasource_plan = Arc::new(PhysicalPlan::DataSource(physical_ds));
            let sampler_plan = logical_ds.sampler().map(|sampler_config| {
                let sampler_index = builder.allocate_index();
                Arc::new(PhysicalPlan::Sampler(PhysicalSampler::new(
                    sampler_config.interval,
                    sampler_config.strategy.clone(),
                    vec![Arc::clone(&datasource_plan)],
                    sampler_index,
                )))
            });

            if decoder_kind == "none" {
                let leaf = sampler_plan.unwrap_or(datasource_plan);
                if matches!(entry.kind, SourceBindingKind::MemoryCollection) {
                    let normalize_index = builder.allocate_index();
                    let normalize =
                        crate::planner::physical::PhysicalCollectionLayoutNormalize::new(
                            Arc::clone(&schema),
                            Arc::<str>::from(logical_ds.source_name.as_str()),
                            vec![Arc::clone(&leaf)],
                            normalize_index,
                        );
                    return Ok(Arc::new(PhysicalPlan::CollectionLayoutNormalize(normalize)));
                }
                return Ok(leaf);
            }

            let decoder_children = sampler_plan
                .as_ref()
                .map(|plan| vec![Arc::clone(plan)])
                .unwrap_or_else(|| vec![Arc::clone(&datasource_plan)]);
            let decoder_index = builder.allocate_index();
            let decoder = PhysicalDecoder::new(
                logical_ds.source_name.clone(),
                logical_ds.decoder().clone(),
                schema,
                logical_ds.decode_projection.clone(),
                eventtime,
                decoder_children,
                decoder_index,
            );
            wrap_source_change_gate(
                Arc::new(PhysicalPlan::Decoder(decoder)),
                logical_ds,
                logical_ds.schema().as_ref(),
                builder,
            )
        }
        SourceBindingKind::Shared => {
            if decoder_kind == "none" {
                return Err(format!(
                    "shared stream `{}` does not support decoder type `none`",
                    logical_ds.source_name
                ));
            }
            let required_columns = shared_stream_required_columns(logical_ds, schema.as_ref());
            let explain_ingest_plan = create_physical_plan_for_shared_stream(
                &logical_ds.source_name,
                Arc::clone(&schema),
                logical_ds.decoder().clone(),
                logical_ds.sampler().cloned(),
            );
            let physical_shared = PhysicalSharedStream::new(
                logical_ds.source_name.clone(),
                logical_ds.alias.clone(),
                Arc::clone(&schema),
                PhysicalSharedStreamRequirement::new(
                    required_columns,
                    logical_ds.shared_slot_version.unwrap_or(0),
                ),
                logical_ds.decoder().clone(),
                Some(explain_ingest_plan),
                index,
            );
            wrap_source_change_gate(
                Arc::new(PhysicalPlan::SharedStream(physical_shared)),
                logical_ds,
                schema.as_ref(),
                builder,
            )
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

fn create_physical_order_with_builder_cached(
    logical_order: &LogicalOrder,
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

    let mut keys = Vec::with_capacity(logical_order.items.len());
    for item in &logical_order.items {
        let compiled_expr = convert_expr_to_scalar_with_bindings_and_custom_registry(
            &item.expr,
            bindings,
            registries.custom_func_registry().as_ref(),
        )
        .map_err(|e| format!("Failed to convert ORDER BY expression to scalar: {}", e))?;
        keys.push(PhysicalOrderKey {
            original_expr: item.expr.clone(),
            compiled_expr,
            asc: item.asc,
        });
    }

    let index = builder.allocate_index();
    let physical_order = PhysicalOrder::new(keys, physical_children, index);
    Ok(Arc::new(PhysicalPlan::Order(physical_order)))
}

/// Create a PhysicalCompute from a LogicalCompute using centralized index management with caching
fn create_physical_compute_with_builder_cached(
    logical_compute: &LogicalCompute,
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

    let mut physical_fields = Vec::new();
    for logical_field in &logical_compute.fields {
        let physical_field = PhysicalComputeField::from_logical(
            logical_field.field_name.clone(),
            logical_field.expr.clone(),
            bindings,
            registries.custom_func_registry().as_ref(),
        )?;
        physical_fields.push(physical_field);
    }

    let index = builder.allocate_index();
    let physical_compute = PhysicalCompute::new(physical_fields, physical_children, index);
    Ok(Arc::new(PhysicalPlan::Compute(physical_compute)))
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
            logical_field.field_name.as_str(),
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
    if physical_sink.connector.encoder_plan_index.is_some() {
        Ok(Arc::new(PhysicalPlan::SinkConnector(physical_sink)))
    } else {
        Ok(Arc::new(PhysicalPlan::DataSink(physical_sink)))
    }
}

/// Build sink chain using centralized index management
fn build_sink_chain_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<(Arc<PhysicalPlan>, PhysicalSinkConnector), String> {
    let row_diff_input =
        create_row_diff_processor_if_needed_with_builder(sink, input_child, builder)?;
    let empty_suppress_input = create_empty_suppress_processor_if_needed_with_builder(
        sink,
        row_diff_input.as_ref().unwrap_or(input_child),
        builder,
    );
    let sink_input = empty_suppress_input
        .as_ref()
        .map(Arc::clone)
        .or_else(|| row_diff_input.as_ref().map(Arc::clone))
        .unwrap_or_else(|| Arc::clone(input_child));

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

    // Create PhysicalBatch for ALL sinks with batching enabled.
    // For regular SinkEncoder branches, StreamingEncoderRewrite will later fuse
    // PhysicalBatch → PhysicalSinkEncoder into PhysicalIncSinkEncoder.
    // For encoder=none (kuksa/kura/video), the PhysicalBatch stays as a standalone node.
    let encoder_input = if sink.common.is_batching_enabled() {
        let batch_index = builder.allocate_index();
        Arc::new(PhysicalPlan::Batch(PhysicalBatch::new(
            vec![sink_input],
            batch_index,
            sink.sink_id.clone(),
            sink.common.clone(),
        )))
    } else {
        sink_input
    };

    add_regular_encoder_with_builder(sink, connector, encoder_input, registries, builder)
}

/// Create row diff processor if needed using centralized index management
fn create_row_diff_processor_if_needed_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Option<Arc<PhysicalPlan>>, String> {
    if !sink.output.is_delta() {
        return Ok(None);
    }

    validate_row_diff_sink_path(sink)?;
    let output_schema = input_child.output_schema()?;
    let (tracked_columns, tracked_column_indexes) =
        resolve_row_diff_tracked_columns(sink, &output_schema)?;
    let row_diff_index = builder.allocate_index();
    let row_diff_plan = PhysicalRowDiff::new(
        vec![Arc::clone(input_child)],
        row_diff_index,
        sink.sink_id.clone(),
        sink.output.clone(),
        output_schema,
        tracked_columns,
        tracked_column_indexes,
    );
    Ok(Some(Arc::new(PhysicalPlan::RowDiff(row_diff_plan))))
}

fn create_empty_suppress_processor_if_needed_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
) -> Option<Arc<PhysicalPlan>> {
    if !sink.output.omit_if_empty() {
        return None;
    }

    let empty_suppress_index = builder.allocate_index();
    let empty_suppress = PhysicalEmptySuppress::new(
        vec![Arc::clone(input_child)],
        empty_suppress_index,
        sink.sink_id.clone(),
        sink.output.omit_if_empty(),
    );
    Some(Arc::new(PhysicalPlan::EmptySuppress(empty_suppress)))
}

fn validate_row_diff_sink_path(sink: &PipelineSink) -> Result<(), String> {
    if !sink.output.is_delta() {
        return Ok(());
    }

    if !matches!(
        sink.connector.encoder.kind(),
        crate::planner::sink::SinkEncoderKind::None
    ) {
        return Ok(());
    }

    match &sink.connector.connector {
        crate::planner::sink::SinkConnectorConfig::Memory(cfg)
            if matches!(cfg.kind, crate::connector::MemoryTopicKind::Collection) =>
        {
            Ok(())
        }
        crate::planner::sink::SinkConnectorConfig::Memory(_) => Err(format!(
            "sink `{}` with output.mode=delta and encoder.type=none must publish to a memory collection topic that preserves output_mask",
            sink.sink_id
        )),
        connector => Err(format!(
            "sink `{}` with output.mode=delta is not supported for connector `{}` when encoder.type=none because the final sink path does not preserve output_mask",
            sink.sink_id,
            connector.kind()
        )),
    }
}

fn resolve_row_diff_tracked_columns(
    sink: &PipelineSink,
    output_schema: &crate::planner::physical::output_schema::OutputSchema,
) -> Result<(Vec<Arc<str>>, Vec<usize>), String> {
    if let Some(configured_columns) = sink.output.delta_columns() {
        return resolve_configured_row_diff_tracked_columns(
            sink,
            output_schema,
            configured_columns,
        );
    }

    Ok((
        output_schema
            .columns
            .iter()
            .map(|column| Arc::clone(&column.name))
            .collect(),
        (0..output_schema.columns.len()).collect(),
    ))
}

fn resolve_configured_row_diff_tracked_columns(
    sink: &PipelineSink,
    output_schema: &crate::planner::physical::output_schema::OutputSchema,
    configured_columns: &[String],
) -> Result<(Vec<Arc<str>>, Vec<usize>), String> {
    let mut seen = HashSet::<&str>::new();
    let mut tracked_columns = Vec::with_capacity(configured_columns.len());
    let mut tracked_column_indexes = Vec::with_capacity(configured_columns.len());

    for configured_column in configured_columns {
        if !seen.insert(configured_column.as_str()) {
            return Err(format!(
                "sink `{}` row diff output has duplicate tracked column `{}`",
                sink.sink_id, configured_column
            ));
        }

        let matches = output_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.name.as_ref() == configured_column.as_str())
            .collect::<Vec<_>>();

        if matches.is_empty() {
            return Err(format!(
                "sink `{}` row diff output column `{}` is not present in final output schema [{}]",
                sink.sink_id,
                configured_column,
                format_output_column_names(output_schema)
            ));
        }

        if matches.len() > 1 {
            return Err(format!(
                "sink `{}` row diff output column `{}` is ambiguous in final output schema [{}]",
                sink.sink_id,
                configured_column,
                format_output_column_names(output_schema)
            ));
        }

        let (index, column) = matches[0];
        tracked_columns.push(Arc::clone(&column.name));
        tracked_column_indexes.push(index);
    }

    Ok((tracked_columns, tracked_column_indexes))
}

fn format_output_column_names(
    output_schema: &crate::planner::physical::output_schema::OutputSchema,
) -> String {
    let names = output_schema
        .columns
        .iter()
        .map(|column| column.name.as_ref())
        .collect::<Vec<_>>();
    if names.is_empty() {
        return "<empty>".to_string();
    }
    names.join(", ")
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
        if connector.compression.is_some() {
            return Err(format!(
                "sink `{}` with encoder.type=none does not support delivery compression",
                sink.sink_id
            ));
        }
        if connector.encryption.is_some() {
            return Err(format!(
                "sink `{}` with encoder.type=none does not support delivery encryption",
                sink.sink_id
            ));
        }
        let connector_config = connector.connector.clone();
        let mut sink_input = encoder_input;

        match &connector_config {
            crate::planner::sink::SinkConnectorConfig::Memory(cfg) => match cfg.kind {
                crate::connector::MemoryTopicKind::Bytes => {
                    return Err(format!(
                            "memory sink `{}` with encoder.type=none must publish to a collection topic",
                            sink.sink_id
                        ));
                }
                crate::connector::MemoryTopicKind::Collection => {
                    let output_schema = sink_input.output_schema()?;
                    validate_unique_output_columns(sink.sink_id.as_str(), &output_schema)?;
                    sink_input = builder
                        .get_or_create_memory_collection_materialize(sink_input, output_schema);
                }
            },
            crate::planner::sink::SinkConnectorConfig::Video(_) => {
                let output_schema = sink_input.output_schema()?;
                validate_video_sink_input_schema(sink.sink_id.as_str(), &output_schema)?;
            }
            _ => {}
        }

        Ok((
            sink_input,
            PhysicalSinkConnector::new(
                sink.sink_id.clone(),
                sink.forward_to_result,
                connector_config,
                None,
            ),
        ))
    } else {
        if matches!(
            connector.connector,
            crate::planner::sink::SinkConnectorConfig::Video(_)
        ) {
            return Err(format!(
                "video sink `{}` requires encoder.type=none",
                sink.sink_id
            ));
        }
        connector
            .encoder
            .validate()
            .map_err(|err| format!("invalid encoder config for sink `{}`: {err}", sink.sink_id))?;
        let encoder_kind = connector.encoder.kind_str();
        if !registries.encoder_registry().is_registered(encoder_kind) {
            return Err(format!(
                "encoder kind `{}` not registered for sink `{}`",
                encoder_kind, sink.sink_id
            ));
        }
        let encoder_index = builder.allocate_index();
        // PhysicalSinkEncoder always runs in Immediate mode.
        // Batching is handled by either:
        // - PhysicalBatch (standalone, for encoder=none), or
        // - PhysicalIncSinkEncoder (fused by StreamingEncoderRewrite, for regular encoders).
        let encoder = PhysicalSinkEncoder::new(
            vec![encoder_input],
            encoder_index,
            sink.sink_id.clone(),
            connector.encoder.clone(),
            CommonSinkProps::default(),
        );
        let encoder_node: Arc<PhysicalPlan> = Arc::new(PhysicalPlan::SinkEncoder(encoder));

        // Insert delivery transforms in fixed order: encoder -> compress -> encrypt -> connector.
        let mut delivery_node = if let Some(codec) = connector.compression.clone() {
            let compress_index = builder.allocate_index();
            let compress = PhysicalSinkCompress::new(encoder_node, compress_index, codec);
            Arc::new(PhysicalPlan::SinkCompress(compress))
        } else {
            encoder_node
        };
        if let Some(encryption) = connector.encryption.clone() {
            let key_bits = encryption
                .validate_and_resolve_key_bits()
                .map_err(|err| err.to_string())?;
            let encrypt_index = builder.allocate_index();
            let encrypt = PhysicalSinkEncrypt::new(
                delivery_node,
                encrypt_index,
                encryption.algorithm,
                encryption.key_id,
                key_bits,
                encryption.key,
            );
            delivery_node = Arc::new(PhysicalPlan::SinkEncrypt(encrypt));
        }

        Ok((
            delivery_node,
            PhysicalSinkConnector::new(
                sink.sink_id.clone(),
                sink.forward_to_result,
                connector.connector.clone(),
                Some(encoder_index),
            ),
        ))
    }
}

fn validate_video_sink_input_schema(
    sink_id: &str,
    output_schema: &crate::planner::physical::output_schema::OutputSchema,
) -> Result<(), String> {
    validate_video_sink_column(
        sink_id,
        output_schema,
        crate::codec::VIDEO_PAYLOAD_COLUMN,
        "bytes",
        is_bytes_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_schema,
        crate::codec::VIDEO_WIDTH_COLUMN,
        "integer",
        is_integer_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_schema,
        crate::codec::VIDEO_HEIGHT_COLUMN,
        "integer",
        is_integer_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_schema,
        crate::codec::VIDEO_FORMAT_COLUMN,
        "string",
        is_string_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_schema,
        crate::codec::VIDEO_TIMESTAMP_COLUMN,
        "timestamp",
        is_timestamp_datatype,
    )
}

fn validate_video_sink_column(
    sink_id: &str,
    output_schema: &crate::planner::physical::output_schema::OutputSchema,
    column_name: &str,
    expected: &str,
    matches_expected: fn(&ConcreteDatatype) -> bool,
) -> Result<(), String> {
    let matches = output_schema
        .columns
        .iter()
        .filter(|column| column.name.as_ref() == column_name)
        .collect::<Vec<_>>();

    if matches.is_empty() {
        return Err(format!(
            "video sink `{sink_id}` requires output column `{column_name}`"
        ));
    }
    if matches.len() > 1 {
        return Err(format!(
            "video sink `{sink_id}` output column `{column_name}` is ambiguous"
        ));
    }

    let data_type = &matches[0].data_type;
    if !matches_expected(data_type) {
        return Err(format!(
            "video sink `{sink_id}` output column `{column_name}` must be {expected}, got {}",
            format_datatype(data_type)
        ));
    }
    Ok(())
}

fn is_bytes_datatype(data_type: &ConcreteDatatype) -> bool {
    matches!(data_type, ConcreteDatatype::Bytes(_))
}

fn is_integer_datatype(data_type: &ConcreteDatatype) -> bool {
    matches!(
        data_type,
        ConcreteDatatype::Int8(_)
            | ConcreteDatatype::Int16(_)
            | ConcreteDatatype::Int32(_)
            | ConcreteDatatype::Int64(_)
            | ConcreteDatatype::Uint8(_)
            | ConcreteDatatype::Uint16(_)
            | ConcreteDatatype::Uint32(_)
            | ConcreteDatatype::Uint64(_)
    )
}

fn is_string_datatype(data_type: &ConcreteDatatype) -> bool {
    matches!(data_type, ConcreteDatatype::String(_))
}

fn is_timestamp_datatype(data_type: &ConcreteDatatype) -> bool {
    matches!(data_type, ConcreteDatatype::Timestamp(_))
}

fn format_datatype(data_type: &ConcreteDatatype) -> &'static str {
    match data_type {
        ConcreteDatatype::Null => "null",
        ConcreteDatatype::Float32(_) => "float32",
        ConcreteDatatype::Float64(_) => "float64",
        ConcreteDatatype::Int8(_) => "int8",
        ConcreteDatatype::Int16(_) => "int16",
        ConcreteDatatype::Int32(_) => "int32",
        ConcreteDatatype::Int64(_) => "int64",
        ConcreteDatatype::Uint8(_) => "uint8",
        ConcreteDatatype::Uint16(_) => "uint16",
        ConcreteDatatype::Uint32(_) => "uint32",
        ConcreteDatatype::Uint64(_) => "uint64",
        ConcreteDatatype::String(_) => "string",
        ConcreteDatatype::Bytes(_) => "bytes",
        ConcreteDatatype::Struct(_) => "struct",
        ConcreteDatatype::List(_) => "list",
        ConcreteDatatype::Bool(_) => "bool",
        ConcreteDatatype::Timestamp(_) => "timestamp",
    }
}

fn validate_unique_output_columns(
    sink_id: &str,
    output_schema: &crate::planner::physical::output_schema::OutputSchema,
) -> Result<(), String> {
    let mut seen = HashSet::<String>::new();
    for col in output_schema.columns.iter() {
        let key = col.name.as_ref().to_string();
        if !seen.insert(key.clone()) {
            return Err(format!(
                "memory collection sink `{}` cannot materialize duplicate output column `{}`",
                sink_id, key
            ));
        }
    }
    Ok(())
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
    use crate::processor::SamplerConfig;

    #[test]
    fn test_physical_plan_builder_creation() {
        let mut builder = PhysicalPlanBuilder::new();
        let index1 = builder.allocate_index();
        let index2 = builder.allocate_index();

        assert_eq!(index1, 0);
        assert_eq!(index2, 1);
    }

    #[test]
    fn test_physical_plan_builder_index_allocation_sequential() {
        let mut builder = PhysicalPlanBuilder::new();
        for i in 0..10 {
            assert_eq!(builder.allocate_index(), i);
        }
    }

    #[test]
    fn test_sampler_config_default_strategy() {
        let config = SamplerConfig::new(Duration::from_millis(100));
        assert_eq!(config.interval, Duration::from_millis(100));
        // Default strategy should be Latest
        assert_eq!(config.strategy, crate::processor::SamplingStrategy::Latest);
    }

    #[test]
    fn test_sampler_config_serialization_roundtrip() {
        let config = SamplerConfig::new(Duration::from_secs(1));
        let json = serde_json::to_string(&config).unwrap();
        let parsed: SamplerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.interval, config.interval);
        assert_eq!(parsed.strategy, config.strategy);
    }

    #[test]
    fn test_physical_sampler_has_correct_interval() {
        let interval = Duration::from_millis(500);
        let sampler = PhysicalSampler::new(
            interval,
            crate::processor::SamplingStrategy::Latest,
            vec![],
            0,
        );
        assert_eq!(sampler.interval, interval);
        assert_eq!(sampler.base.index(), 0);
    }

    #[test]
    fn test_physical_sampler_with_children() {
        let interval = Duration::from_millis(100);
        // Create a dummy physical plan child
        let dummy_ds = crate::planner::physical::PhysicalDataSource::new(
            "test_stream".to_string(),
            None,
            Arc::new(datatypes::Schema::new(vec![])),
            None,
            0,
        );
        let child = Arc::new(PhysicalPlan::DataSource(dummy_ds));

        let sampler = PhysicalSampler::new(
            interval,
            crate::processor::SamplingStrategy::Latest,
            vec![child.clone()],
            1,
        );
        assert_eq!(sampler.base.children().len(), 1);
    }
}
