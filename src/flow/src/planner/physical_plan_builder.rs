//! Physical plan builder - converts logical plans to physical plans using centralized index management
use crate::expr::sql_conversion::{
    convert_expr_to_scalar_with_bindings_and_custom_registry, SchemaBinding, SchemaBindingEntry,
    SourceBindingKind,
};
use crate::expr::ScalarExpr;
use crate::planner::logical::{
    aggregation::Aggregation as LogicalAggregation, compute::Compute as LogicalCompute,
    order::Order as LogicalOrder, DataSinkPlan, DataSource as LogicalDataSource,
    Filter as LogicalFilter, LogicalPlan, LogicalWindow, LogicalWindowSpec,
    Project as LogicalProject, StatefulFunctionPlan as LogicalStatefulFunction,
    TableScan as LogicalTableScan,
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
    PhysicalTableScan, PhysicalTableScanSpec, StatefulCall, WatermarkConfig, WatermarkStrategy,
};
use crate::planner::shared_stream_plan::create_physical_plan_for_shared_stream;
use crate::planner::sink::{CommonSinkProps, PipelineSink, PipelineSinkConnector};
use crate::processor::processor_state::ProcessorState;
use crate::shared_stream::SharedStreamRegistry;
use crate::PipelineRegistries;
use datatypes::{ConcreteDatatype, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PhysicalPlanBuildOptions {
    pub eventtime_enabled: bool,
    pub eventtime_late_tolerance: Duration,
    /// Slot versions per shared source, computed by [`apply_shared_stream_slot_schemas`].
    pub shared_slot_versions: HashMap<String, u64>,
}

impl Default for PhysicalPlanBuildOptions {
    fn default() -> Self {
        Self {
            eventtime_enabled: false,
            eventtime_late_tolerance: Duration::ZERO,
            shared_slot_versions: HashMap::new(),
        }
    }
}

/// Feature gate for shared-stream output slice projection (VF-56). Off unless
/// `VF_SHARED_SLICE_PROJECTION` is `1`/`true`, so the planner + decoder change can
/// land incrementally without altering default behavior until validated.
pub(crate) fn shared_slice_projection_enabled() -> bool {
    std::env::var("VF_SHARED_SLICE_PROJECTION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Build a shared source's binding schema from its append-only slot registry
/// (VF-56). Registers this consumer's used columns (in full-schema order, for
/// deterministic slot assignment), then materializes a schema whose column order
/// IS the slot order — so `Schema::column_index(name)` returns the stable slot,
/// which becomes the consumer's `ColumnRef::ByIndex`. Width = current union size.
fn build_shared_slice_schema(
    registry: &SharedStreamRegistry,
    entry: &SchemaBindingEntry,
    used: Option<&HashSet<String>>,
) -> (Arc<Schema>, u64) {
    let cols = entry.schema.column_schemas();
    let reg = registry.slice_registry(&entry.source_name);
    let snapshot = {
        let mut guard = reg.write();
        let names = cols.iter().filter_map(|col| match used {
            Some(used) if !used.contains(col.name.as_str()) => None,
            _ => Some(col.name.as_str()),
        });
        guard.assign_all(names)
    };
    (materialize_slot_schema(cols, &snapshot), snapshot.version)
}

/// Read-only variant: build slot-ordered schema from the current registry snapshot
/// without assigning any new slots. Used by EXPLAIN to reflect the live slot layout.
fn read_shared_slice_schema(
    registry: &SharedStreamRegistry,
    entry: &SchemaBindingEntry,
) -> (Arc<Schema>, u64) {
    let cols = entry.schema.column_schemas();
    let reg = registry.slice_registry(&entry.source_name);
    let snapshot = reg.read().snapshot();
    // If no slots have been assigned yet, keep the source-order schema.
    if snapshot.keys.is_empty() {
        return (Arc::clone(&entry.schema), 0);
    }
    (materialize_slot_schema(cols, &snapshot), snapshot.version)
}

/// Materialize a slot-ordered schema from a set of source columns and a slot snapshot.
fn materialize_slot_schema(
    cols: &[datatypes::ColumnSchema],
    snapshot: &crate::shared_stream::SliceRegistrySnapshot,
) -> Arc<Schema> {
    let by_name: HashMap<&str, &datatypes::ColumnSchema> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();
    let slot_cols: Vec<datatypes::ColumnSchema> = snapshot
        .keys
        .iter()
        .filter_map(|k| by_name.get(k.as_ref()).map(|c| (*c).clone()))
        .collect();
    Arc::new(Schema::new(slot_cols))
}

/// Walk the logical plan tree to find the required columns for a shared source.
///
/// Relies on `shared_required_schema` being set by `TopLevelColumnPruning` during the
/// preceding logical pass. Returns the first DataSource match for `source_name` — in the
/// current planner shape each shared source appears at most once in a logical plan tree.
fn find_shared_required_columns(plan: &LogicalPlan, source_name: &str) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::DataSource(ds) if ds.source_name == source_name => {
            ds.shared_required_schema().map(|s| s.to_vec())
        }
        _ => {
            for child in plan.children() {
                if let Some(cols) = find_shared_required_columns(child, source_name) {
                    return Some(cols);
                }
            }
            None
        }
    }
}

/// Apply VF-56 shared-stream slot-schema projection: assign append-only slots for
/// shared sources and rewrite their `SchemaBinding` entries to slot order.
///
/// Returns the slot versions per shared source and the updated `SchemaBinding`.
/// This runs after logical optimization and before physical plan building, so
/// the physical plan builder receives a `SchemaBinding` whose shared-source
/// column indices already reflect stable slot positions.
///
/// # Relation to logical rules
///
/// The slot rewrite runs **after** all logical optimizer rules (including
/// `StructFieldPruning` and `ListElementPruning`). Those rules observe the
/// source-order schema produced by `TopLevelColumnPruning`, not the slot-order
/// union. This is safe because shared CAN/GBF sources expose only scalar
/// top-level columns — there are no nested struct/list fields whose decode
/// projection could differ between the pruned schema and the slot-ordered union.
pub fn apply_shared_stream_slot_schemas(
    logical_plan: &LogicalPlan,
    bindings: &SchemaBinding,
    registry: &SharedStreamRegistry,
    projection_enabled: bool,
) -> (HashMap<String, u64>, SchemaBinding) {
    let mut slot_versions = HashMap::new();
    let mut new_entries = Vec::new();

    for entry in bindings.entries() {
        if entry.kind != SourceBindingKind::Shared || !projection_enabled {
            new_entries.push(entry.clone());
            continue;
        }

        let required = find_shared_required_columns(logical_plan, &entry.source_name);
        let full_count = entry.schema.column_schemas().len();
        let used: Option<HashSet<String>> = if required
            .as_ref()
            .is_none_or(|r| r.is_empty() || r.len() >= full_count)
        {
            None
        } else {
            required.map(|r| r.into_iter().collect())
        };

        let (slice_schema, version) = build_shared_slice_schema(registry, entry, used.as_ref());

        slot_versions.insert(entry.source_name.clone(), version);

        new_entries.push(SchemaBindingEntry {
            source_name: entry.source_name.clone(),
            alias: None,
            schema: slice_schema,
            kind: entry.kind.clone(),
        });
    }

    (slot_versions, SchemaBinding::new(new_entries))
}

/// Read-only variant for EXPLAIN: rewrites shared-source schemas to slot order based
/// on the current registry snapshot, without assigning any new slots or bumping the
/// version. When no slots have been assigned yet (snapshot is empty), shared-source
/// schemas are kept in source order.
pub fn read_shared_stream_slot_schemas(
    bindings: &SchemaBinding,
    registry: &SharedStreamRegistry,
) -> (HashMap<String, u64>, SchemaBinding) {
    let mut slot_versions = HashMap::new();
    let mut new_entries = Vec::new();
    for entry in bindings.entries() {
        if entry.kind != SourceBindingKind::Shared {
            new_entries.push(entry.clone());
            continue;
        }
        let (slice_schema, version) = read_shared_slice_schema(registry, entry);
        slot_versions.insert(entry.source_name.clone(), version);
        new_entries.push(SchemaBindingEntry {
            source_name: entry.source_name.clone(),
            alias: None,
            schema: slice_schema,
            kind: entry.kind.clone(),
        });
    }
    (slot_versions, SchemaBinding::new(new_entries))
}

/// Physical plan builder that manages index allocation and node caching
pub struct PhysicalPlanBuilder {
    next_index: i64,
    node_cache: std::collections::HashMap<i64, Arc<PhysicalPlan>>,
    memory_collection_materialize_cache: std::collections::HashMap<i64, Arc<PhysicalPlan>>,
    /// Slot versions per shared source, populated by [`apply_shared_stream_slot_schemas`]
    /// before physical plan building.
    shared_slot_versions: HashMap<String, u64>,
}

impl PhysicalPlanBuilder {
    pub fn new() -> Self {
        Self {
            next_index: 0,
            node_cache: std::collections::HashMap::new(),
            memory_collection_materialize_cache: std::collections::HashMap::new(),
            shared_slot_versions: HashMap::new(),
        }
    }

    pub fn starting_from(start_index: i64) -> Self {
        Self {
            next_index: start_index,
            node_cache: std::collections::HashMap::new(),
            memory_collection_materialize_cache: std::collections::HashMap::new(),
            shared_slot_versions: HashMap::new(),
        }
    }

    pub fn with_slot_versions(mut self, slot_versions: HashMap<String, u64>) -> Self {
        self.shared_slot_versions = slot_versions;
        self
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

    pub fn get_or_create_output_layout_materialize(
        &mut self,
        child: Arc<PhysicalPlan>,
        output_layout: crate::planner::physical::output_layout::OutputLayout,
    ) -> Arc<PhysicalPlan> {
        let key = child.get_plan_index();
        if let Some(cached) = self.memory_collection_materialize_cache.get(&key) {
            return Arc::clone(cached);
        }

        let index = self.allocate_index();
        let plan = Arc::new(PhysicalPlan::MemoryCollectionMaterialize(
            PhysicalMemoryCollectionMaterialize::new(output_layout, child, index),
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
    let mut builder =
        PhysicalPlanBuilder::new().with_slot_versions(options.shared_slot_versions.clone());
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
        LogicalPlan::TableScan(logical_table_scan) => {
            let index = builder.allocate_index();
            create_physical_table_scan(logical_table_scan, index)
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

fn create_physical_table_scan(logical_scan: &LogicalTableScan, index: i64) -> Arc<PhysicalPlan> {
    Arc::new(PhysicalPlan::TableScan(PhysicalTableScan::new(
        PhysicalTableScanSpec {
            table_name: logical_scan.table_name.clone(),
            table_type: logical_scan.table_type,
            decoder: logical_scan.decoder.clone(),
            schema: logical_scan.schema(),
            props: logical_scan.props.clone(),
            request: logical_scan.request.clone(),
        },
        index,
    )))
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
                    logical_ds.decoder().schema_artifact.clone(),
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
                Arc::clone(&schema),
                PhysicalSharedStreamRequirement::new(
                    required_columns,
                    builder
                        .shared_slot_versions
                        .get(&logical_ds.source_name)
                        .copied()
                        .unwrap_or(0),
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
        SourceBindingKind::TableScan => Err(format!(
            "table scan binding `{}` cannot build a stream datasource",
            logical_ds.source_name
        )),
    }
}

/// Recursively check whether a [`ScalarExpr`] contains any
/// [`ScalarExpr::PipelineState`] (unresolved pipeline state read).
fn expr_contains_pipeline_state(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::PipelineState { .. } => true,
        ScalarExpr::CallUnary { expr: inner, .. } => expr_contains_pipeline_state(inner),
        ScalarExpr::CallBinary { expr1, expr2, .. } => {
            expr_contains_pipeline_state(expr1) || expr_contains_pipeline_state(expr2)
        }
        ScalarExpr::FieldAccess { expr: inner, .. } => expr_contains_pipeline_state(inner),
        ScalarExpr::ListIndex { expr, index_expr } => {
            expr_contains_pipeline_state(expr) || expr_contains_pipeline_state(index_expr)
        }
        ScalarExpr::CallFunc { args, .. } => args.iter().any(expr_contains_pipeline_state),
        ScalarExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .map(|e| expr_contains_pipeline_state(e))
                .unwrap_or(false)
                || when_then.iter().any(|(w, t)| {
                    expr_contains_pipeline_state(w) || expr_contains_pipeline_state(t)
                })
                || else_expr
                    .as_ref()
                    .map(|e| expr_contains_pipeline_state(e))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// Recursively replace every [`ScalarExpr::PipelineState`] in `expr` with
/// [`ScalarExpr::ProcessorState`] pointing at `state`.
fn inject_processor_state(expr: &mut ScalarExpr, state: &Arc<ProcessorState>) {
    match expr {
        ScalarExpr::PipelineState { field } => {
            *expr = ScalarExpr::ProcessorState {
                state: Arc::clone(state),
                field: field.clone(),
            };
        }
        ScalarExpr::CallUnary { expr: inner, .. } => inject_processor_state(inner, state),
        ScalarExpr::CallBinary { expr1, expr2, .. } => {
            inject_processor_state(expr1, state);
            inject_processor_state(expr2, state);
        }
        ScalarExpr::FieldAccess { expr: inner, .. } => inject_processor_state(inner, state),
        ScalarExpr::ListIndex { expr, index_expr } => {
            inject_processor_state(expr, state);
            inject_processor_state(index_expr, state);
        }
        ScalarExpr::CallFunc { args, .. } => {
            for arg in args {
                inject_processor_state(arg, state);
            }
        }
        ScalarExpr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                inject_processor_state(op, state);
            }
            for (w, t) in when_then {
                inject_processor_state(w, state);
                inject_processor_state(t, state);
            }
            if let Some(e) = else_expr {
                inject_processor_state(e, state);
            }
        }
        _ => {}
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
    let mut scalar_predicate = convert_expr_to_scalar_with_bindings_and_custom_registry(
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

    let processor_state = if expr_contains_pipeline_state(&scalar_predicate) {
        let state = Arc::new(ProcessorState::new());
        inject_processor_state(&mut scalar_predicate, &state);
        Some(state)
    } else {
        None
    };

    let index = builder.allocate_index();
    let mut physical_filter = PhysicalFilter::new(
        logical_filter.predicate.clone(),
        scalar_predicate,
        physical_children,
        index,
    );
    physical_filter.processor_state = processor_state;
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

    let processor_state = if physical_fields
        .iter()
        .any(|f| expr_contains_pipeline_state(&f.compiled_expr))
    {
        let state = Arc::new(ProcessorState::new());
        for field in &mut physical_fields {
            inject_processor_state(&mut field.compiled_expr, &state);
        }
        Some(state)
    } else {
        None
    };

    let index = builder.allocate_index();
    let mut physical_project = PhysicalProject::new(physical_fields, physical_children, index);
    physical_project.processor_state = processor_state;
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

/// Recursively check whether any node in the physical plan tree uses processor state.
fn plan_uses_processor_state(plan: &Arc<PhysicalPlan>) -> bool {
    match plan.as_ref() {
        PhysicalPlan::Filter(f) => {
            if f.processor_state.is_some() {
                return true;
            }
        }
        PhysicalPlan::Project(p) if p.processor_state.is_some() => {
            return true;
        }
        _ => {}
    }
    for child in plan.children() {
        if plan_uses_processor_state(child) {
            return true;
        }
    }
    false
}

/// Insert a `PhysicalColumnFilter` node if the sink's output config specifies
/// include_columns or exclude_columns.
///
/// The node changes only the planner-owned output layout. Runtime tuples remain
/// unchanged until a downstream consumer materializes the selected layout.
fn maybe_insert_column_filter(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    builder: &mut PhysicalPlanBuilder,
) -> Result<Arc<PhysicalPlan>, String> {
    if !sink.output.has_column_filter() {
        return Ok(Arc::clone(input_child));
    }

    sink.output.validate()?;

    let input_layout = input_child.output_layout().map_err(|err| {
        format!(
            "sink `{}` column filter: failed to resolve input output layout: {}",
            sink.sink_id, err
        )
    })?;
    validate_column_filter_columns(
        &sink.sink_id,
        &input_layout,
        sink.output.include_columns.as_deref(),
        sink.output.exclude_columns.as_deref(),
    )?;

    let index = builder.allocate_index();
    let filter = crate::planner::physical::PhysicalColumnFilter::new(
        vec![Arc::clone(input_child)],
        index,
        sink.sink_id.clone(),
        sink.output.include_columns.clone(),
        sink.output.exclude_columns.clone(),
    );
    Ok(Arc::new(PhysicalPlan::ColumnFilter(filter)))
}

fn validate_column_filter_columns(
    sink_id: &str,
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
    include_columns: Option<&[String]>,
    exclude_columns: Option<&[String]>,
) -> Result<(), String> {
    if let Some(include) = include_columns {
        for col_name in include {
            if !output_layout
                .columns
                .iter()
                .any(|c| c.name.as_ref() == col_name.as_str())
            {
                let schema_names: Vec<&str> = output_layout
                    .columns
                    .iter()
                    .map(|c| c.name.as_ref())
                    .collect();
                return Err(format!(
                    "sink `{}` output.include_columns: column `{}` not found in output schema [{}]",
                    sink_id,
                    col_name,
                    schema_names.join(", ")
                ));
            }
        }
    }
    if let Some(exclude) = exclude_columns {
        for col_name in exclude {
            if !output_layout
                .columns
                .iter()
                .any(|c| c.name.as_ref() == col_name.as_str())
            {
                let schema_names: Vec<&str> = output_layout
                    .columns
                    .iter()
                    .map(|c| c.name.as_ref())
                    .collect();
                return Err(format!(
                    "sink `{}` output.exclude_columns: column `{}` not found in output schema [{}]",
                    sink_id,
                    col_name,
                    schema_names.join(", ")
                ));
            }
        }
    }
    Ok(())
}

/// Build sink chain using centralized index management
fn build_sink_chain_with_builder(
    sink: &PipelineSink,
    input_child: &Arc<PhysicalPlan>,
    registries: &PipelineRegistries,
    builder: &mut PhysicalPlanBuilder,
) -> Result<(Arc<PhysicalPlan>, PhysicalSinkConnector), String> {
    sink.retry.validate()?;

    // Reject pipeline state functions with sink configs that can drop rows
    // after Filter (RowDiff for delta mode, EmptySuppress for omit_if_empty).
    if plan_uses_processor_state(input_child) {
        if sink.output.is_delta() {
            return Err(
                "pipeline state functions (e.g. last_hit_count()) are not compatible with output.mode=delta (RowDiff can drop rows after Filter)".to_string(),
            );
        }
        if sink.output.omit_if_empty() {
            return Err(
                "pipeline state functions (e.g. last_hit_count()) are not compatible with output.omit_if_empty=true (EmptySuppress can drop rows after Filter)".to_string(),
            );
        }
    }

    // Insert ColumnFilter BEFORE RowDiff so that RowDiff only sees the
    // columns this sink actually needs.
    let filtered_input = maybe_insert_column_filter(sink, input_child, builder)?;

    let row_diff_input =
        create_row_diff_processor_if_needed_with_builder(sink, &filtered_input, builder)?;
    let empty_suppress_input = create_empty_suppress_processor_if_needed_with_builder(
        sink,
        row_diff_input.as_ref().unwrap_or(&filtered_input),
        builder,
    );
    let sink_input = empty_suppress_input
        .as_ref()
        .map(Arc::clone)
        .or_else(|| row_diff_input.as_ref().map(Arc::clone))
        .unwrap_or_else(|| Arc::clone(&filtered_input));

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
    let output_layout = input_child.output_layout()?;
    let (tracked_columns, tracked_column_indexes) =
        resolve_row_diff_tracked_columns(sink, &output_layout)?;
    let row_diff_index = builder.allocate_index();
    let row_diff_plan = PhysicalRowDiff::new(
        vec![Arc::clone(input_child)],
        row_diff_index,
        sink.sink_id.clone(),
        sink.output.clone(),
        output_layout,
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

    if matches!(
        sink.connector.encoder.kind(),
        crate::planner::sink::SinkEncoderKind::Csv
    ) {
        return Err(format!(
            "sink `{}` does not support output.mode=delta with encoder.type=csv because CSV cannot preserve output_mask semantics",
            sink.sink_id
        ));
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
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
) -> Result<(Vec<Arc<str>>, Vec<usize>), String> {
    if let Some(configured_columns) = sink.output.delta_columns() {
        return resolve_configured_row_diff_tracked_columns(
            sink,
            output_layout,
            configured_columns,
        );
    }

    Ok((
        output_layout
            .columns
            .iter()
            .map(|column| Arc::clone(&column.name))
            .collect(),
        (0..output_layout.columns.len()).collect(),
    ))
}

fn resolve_configured_row_diff_tracked_columns(
    sink: &PipelineSink,
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
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

        let matches = output_layout
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
                format_output_column_names(output_layout)
            ));
        }

        if matches.len() > 1 {
            return Err(format!(
                "sink `{}` row diff output column `{}` is ambiguous in final output schema [{}]",
                sink.sink_id,
                configured_column,
                format_output_column_names(output_layout)
            ));
        }

        let (index, column) = matches[0];
        tracked_columns.push(Arc::clone(&column.name));
        tracked_column_indexes.push(index);
    }

    Ok((tracked_columns, tracked_column_indexes))
}

fn format_output_column_names(
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
) -> String {
    let names = output_layout
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
        let mut layout_materialized = false;

        match &connector_config {
            crate::planner::sink::SinkConnectorConfig::Memory(cfg) => match cfg.kind {
                crate::connector::MemoryTopicKind::Bytes => {
                    return Err(format!(
                            "memory sink `{}` with encoder.type=none must publish to a collection topic",
                            sink.sink_id
                        ));
                }
                crate::connector::MemoryTopicKind::Collection => {
                    let output_layout = sink_input.output_layout()?;
                    validate_unique_output_columns(sink.sink_id.as_str(), &output_layout)?;
                    sink_input =
                        builder.get_or_create_output_layout_materialize(sink_input, output_layout);
                    layout_materialized = true;
                }
            },
            crate::planner::sink::SinkConnectorConfig::Video(_) => {
                let output_layout = sink_input.output_layout()?;
                validate_video_sink_input_schema(sink.sink_id.as_str(), &output_layout)?;
            }
            _ => {}
        }

        if sink.output.has_column_filter() && !layout_materialized {
            let output_layout = sink_input.output_layout()?;
            validate_unique_output_columns(sink.sink_id.as_str(), &output_layout)?;
            sink_input = builder.get_or_create_output_layout_materialize(sink_input, output_layout);
        }

        let connector_config = resolve_connector_content_type(
            connector.connector.clone(),
            connector.encoder.kind_str(),
        );
        Ok((
            sink_input,
            PhysicalSinkConnector::new(
                sink.sink_id.clone(),
                sink.forward_to_result,
                connector_config,
                None,
                sink.retry.clone(),
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

        let connector_config = resolve_connector_content_type(
            connector.connector.clone(),
            connector.encoder.kind_str(),
        );
        Ok((
            delivery_node,
            PhysicalSinkConnector::new(
                sink.sink_id.clone(),
                sink.forward_to_result,
                connector_config,
                Some(encoder_index),
                sink.retry.clone(),
            ),
        ))
    }
}

/// Resolve the Content-Type header for HTTP sink configs by inferring it from
/// the encoder kind when not explicitly configured.
fn resolve_connector_content_type(
    config: crate::planner::sink::SinkConnectorConfig,
    encoder_kind: &str,
) -> crate::planner::sink::SinkConnectorConfig {
    match config {
        crate::planner::sink::SinkConnectorConfig::Http(http_cfg) => {
            crate::planner::sink::SinkConnectorConfig::Http(
                http_cfg.with_inferred_content_type(Some(encoder_kind)),
            )
        }
        other => other,
    }
}

fn validate_video_sink_input_schema(
    sink_id: &str,
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
) -> Result<(), String> {
    validate_video_sink_column(
        sink_id,
        output_layout,
        crate::codec::VIDEO_PAYLOAD_COLUMN,
        "bytes",
        is_bytes_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_layout,
        crate::codec::VIDEO_WIDTH_COLUMN,
        "integer",
        is_integer_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_layout,
        crate::codec::VIDEO_HEIGHT_COLUMN,
        "integer",
        is_integer_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_layout,
        crate::codec::VIDEO_FORMAT_COLUMN,
        "string",
        is_string_datatype,
    )?;
    validate_video_sink_column(
        sink_id,
        output_layout,
        crate::codec::VIDEO_TIMESTAMP_COLUMN,
        "timestamp",
        is_timestamp_datatype,
    )
}

fn validate_video_sink_column(
    sink_id: &str,
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
    column_name: &str,
    expected: &str,
    matches_expected: fn(&ConcreteDatatype) -> bool,
) -> Result<(), String> {
    let matches = output_layout
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
    output_layout: &crate::planner::physical::output_layout::OutputLayout,
) -> Result<(), String> {
    let mut seen = HashSet::<String>::new();
    for col in output_layout.columns.iter() {
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
            None,
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
            Arc::new(datatypes::Schema::new(vec![])),
            None,
            0,
        );
        let child = Arc::new(PhysicalPlan::DataSource(dummy_ds));

        let sampler = PhysicalSampler::new(
            interval,
            crate::processor::SamplingStrategy::Latest,
            None,
            vec![child.clone()],
            1,
        );
        assert_eq!(sampler.base.children().len(), 1);
    }

    #[test]
    fn test_physical_sampler_carries_schema_artifact() {
        let artifact: Arc<dyn std::any::Any + Send + Sync> = Arc::new(42_u32);
        let sampler = PhysicalSampler::new(
            Duration::from_millis(100),
            crate::processor::SamplingStrategy::Latest,
            Some(Arc::clone(&artifact)),
            vec![],
            0,
        );

        let restored = sampler
            .schema_artifact()
            .and_then(|value| value.downcast::<u32>().ok())
            .expect("sampler schema artifact");
        assert_eq!(*restored, 42);
    }
}

#[cfg(test)]
mod slot_schema_tests {
    use super::*;
    use crate::expr::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
    use crate::planner::logical::DataSource;
    use crate::shared_stream::SharedStreamRegistry;
    use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use std::sync::Arc;

    fn test_registry() -> SharedStreamRegistry {
        SharedStreamRegistry::new(crate::runtime::TaskSpawner::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        ))
    }

    fn test_schema(name: &str, columns: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|col| {
                    ColumnSchema::new(
                        name.to_string(),
                        col.to_string(),
                        ConcreteDatatype::Int64(Int64Type),
                    )
                })
                .collect(),
        ))
    }

    fn shared_binding_entry(source_name: &str, columns: &[&str]) -> SchemaBindingEntry {
        SchemaBindingEntry {
            source_name: source_name.to_string(),
            alias: None,
            schema: test_schema(source_name, columns),
            kind: SourceBindingKind::Shared,
        }
    }

    fn make_data_source(
        source_name: &str,
        schema: Arc<Schema>,
        required_columns: Vec<String>,
    ) -> Arc<LogicalPlan> {
        let ds = DataSource::new(
            source_name.to_string(),
            None,
            crate::catalog::StreamDecoderConfig::json(),
            0,
            schema,
            None,
            None,
        );
        let mut ds = ds;
        ds.shared_required_schema = Some(required_columns);
        Arc::new(LogicalPlan::DataSource(ds))
    }

    #[test]
    fn apply_assigns_slots_and_returns_version() {
        let registry = test_registry();
        let entry = shared_binding_entry("s1", &["a", "b", "c"]);
        let bindings = SchemaBinding::new(vec![entry]);
        let plan = make_data_source(
            "s1",
            test_schema("s1", &["a", "b", "c"]),
            vec!["a".into(), "c".into()],
        );

        let (versions, new_bindings) =
            apply_shared_stream_slot_schemas(&plan, &bindings, &registry, true);

        // Slots assigned: a=0, c=1 (source order).
        let entry = &new_bindings.entries()[0];
        let col_names: Vec<&str> = entry
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["a", "c"]);
        assert!(versions.contains_key("s1"));
        assert!(versions["s1"] > 0);
    }

    #[test]
    fn apply_on_disabled_projection_keeps_source_order() {
        let registry = test_registry();
        let entry = shared_binding_entry("s1", &["a", "b"]);
        let bindings = SchemaBinding::new(vec![entry]);
        let plan = make_data_source("s1", test_schema("s1", &["a", "b"]), vec!["b".into()]);

        let (versions, new_bindings) =
            apply_shared_stream_slot_schemas(&plan, &bindings, &registry, false);

        // Projection disabled; schema unchanged.
        let entry = &new_bindings.entries()[0];
        let col_names: Vec<&str> = entry
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["a", "b"]);
        assert!(versions.is_empty());
    }

    #[test]
    fn read_does_not_assign_new_slots() {
        let registry = test_registry();
        let entry = shared_binding_entry("s1", &["x", "y"]);
        let bindings = SchemaBinding::new(vec![entry.clone()]);
        let plan = make_data_source("s1", test_schema("s1", &["x", "y"]), vec!["x".into()]);

        // Pre-populate slots via the write path.
        let (_, _) = apply_shared_stream_slot_schemas(&plan, &bindings, &registry, true);
        let version_before = registry.slice_registry("s1").read().snapshot().version;

        // Read should not bump version or add slots.
        let (read_versions, read_bindings) = read_shared_stream_slot_schemas(&bindings, &registry);
        let version_after = registry.slice_registry("s1").read().snapshot().version;
        assert_eq!(
            version_after, version_before,
            "EXPLAIN must not bump version"
        );
        assert!(!read_versions.is_empty());
        // Schema should reflect the slot order from the write path.
        let entry = &read_bindings.entries()[0];
        let col_names: Vec<&str> = entry
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["x"]);
    }

    #[test]
    fn read_returns_source_order_when_no_slots_assigned() {
        let registry = test_registry();
        let entry = shared_binding_entry("s1", &["p", "q"]);
        let bindings = SchemaBinding::new(vec![entry.clone()]);

        let (versions, new_bindings) = read_shared_stream_slot_schemas(&bindings, &registry);

        let entry = &new_bindings.entries()[0];
        let col_names: Vec<&str> = entry
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["p", "q"]);
        // Version should be 0 when no slots exist.
        assert_eq!(versions.get("s1").copied().unwrap_or(0), 0);
    }
}
