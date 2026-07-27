use datatypes::{
    ColumnSchema, ConcreteDatatype, Int64Type, ListType, Schema, StringType, StructField,
    StructType,
};
use flow::catalog::MemoryStreamProps;
use flow::planner::decode_projection::{ListIndexSelection, ProjectionNode};
use flow::planner::logical::{create_logical_plan, LogicalPlan};
use flow::planner::sink::CustomSinkConnectorConfig;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::{
    ExplainReport, MqttStreamProps, NopSinkConfig, PipelineSink, PipelineSinkConnector,
    SinkConnectorConfig, SinkEncoderConfig, StreamDecoderConfig, StreamDefinition, StreamProps,
};
use parser::{
    builtin_stateful_function_names, parse_sql, parse_sql_with_registries, StaticAggregateRegistry,
    StaticStatefulRegistry,
};
use serde_json::json;
use serde_json::Map as JsonMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

fn setup_streams() -> HashMap<String, Arc<StreamDefinition>> {
    let users_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "users".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "users".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "users".to_string(),
            "c".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "users".to_string(),
            "k1".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "users".to_string(),
            "k2".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let users_def = StreamDefinition::new(
        "users",
        Arc::clone(&users_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let stream_prune_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_prune".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream_prune".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let stream_prune_def = StreamDefinition::new(
        "stream_prune",
        Arc::clone(&stream_prune_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let stream_window_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_window".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream_window".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream_window".to_string(),
            "c".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream_window".to_string(),
            "d".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let stream_window_def = StreamDefinition::new(
        "stream_window",
        Arc::clone(&stream_window_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let user_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("d".to_string(), ConcreteDatatype::String(StringType), false),
    ])));
    let stream_struct_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_struct".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new("stream_struct".to_string(), "b".to_string(), user_struct),
    ]));
    let stream_struct_def = StreamDefinition::new(
        "stream_struct",
        Arc::clone(&stream_struct_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let items_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("d".to_string(), ConcreteDatatype::String(StringType), false),
    ])));
    let stream_3_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_3".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream_3".to_string(),
            "items".to_string(),
            ConcreteDatatype::List(ListType::new(Arc::new(items_struct))),
        ),
    ]));
    let stream_3_def = StreamDefinition::new(
        "stream_3",
        Arc::clone(&stream_3_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let stream_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream".to_string(),
            "flag".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream".to_string(),
            "k1".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream".to_string(),
            "k2".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let stream_def = StreamDefinition::new(
        "stream",
        Arc::clone(&stream_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let demo_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "demo".to_string(),
            "Status".to_string(),
            ConcreteDatatype::String(StringType),
        ),
        ColumnSchema::new(
            "demo".to_string(),
            "ts".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "demo".to_string(),
            "statusCode".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let demo_def = StreamDefinition::new(
        "demo",
        Arc::clone(&demo_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let memory_collect_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "memory_collect".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "memory_collect".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let memory_collect_def = StreamDefinition::new(
        "memory_collect",
        Arc::clone(&memory_collect_schema),
        StreamProps::Memory(MemoryStreamProps::new("demo_memory_collect")),
        StreamDecoderConfig::new("none", JsonMap::new()),
    );

    let mut stream_defs = HashMap::new();
    stream_defs.insert("users".to_string(), Arc::new(users_def));
    stream_defs.insert("stream_prune".to_string(), Arc::new(stream_prune_def));
    stream_defs.insert("stream_window".to_string(), Arc::new(stream_window_def));
    stream_defs.insert("stream_struct".to_string(), Arc::new(stream_struct_def));
    stream_defs.insert("stream_3".to_string(), Arc::new(stream_3_def));
    stream_defs.insert("stream".to_string(), Arc::new(stream_def));
    stream_defs.insert("demo".to_string(), Arc::new(demo_def));
    stream_defs.insert("memory_collect".to_string(), Arc::new(memory_collect_def));
    stream_defs
}

fn bindings_for_select_with_shared_sources(
    select_stmt: &parser::SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
    shared_sources: &[&str],
) -> SchemaBinding {
    SchemaBinding::new(
        select_stmt
            .source_infos
            .iter()
            .map(|source| {
                let def = stream_defs
                    .get(&source.name)
                    .unwrap_or_else(|| panic!("missing stream definition: {}", source.name));
                let kind = if shared_sources.iter().any(|name| *name == source.name) {
                    SourceBindingKind::Shared
                } else if def.stream_type() == flow::StreamType::Memory
                    && def.decoder().kind() == "none"
                {
                    SourceBindingKind::MemoryCollection
                } else {
                    SourceBindingKind::Regular
                };
                SchemaBindingEntry {
                    source_name: source.name.clone(),
                    alias: None,
                    schema: def.schema(),
                    kind,
                }
            })
            .collect(),
    )
}

fn optimized_logical_json(sql: &str) -> String {
    optimized_logical_json_with_shared_sources(sql, &[])
}

fn optimized_logical_json_with_shared_sources(sql: &str, shared_sources: &[&str]) -> String {
    optimized_logical_json_with_shared_sources_and_registries(
        sql,
        shared_sources,
        Arc::new(StaticAggregateRegistry::new([
            "sum", "count", "last_row", "ndv",
        ])),
        Arc::new(StaticStatefulRegistry::new(
            builtin_stateful_function_names().iter().copied(),
        )),
        vec![],
    )
}

fn optimized_logical_json_with_shared_sources_and_registries(
    sql: &str,
    shared_sources: &[&str],
    aggregate_registry: Arc<StaticAggregateRegistry>,
    stateful_registry: Arc<StaticStatefulRegistry>,
    sinks: Vec<PipelineSink>,
) -> String {
    let optimized = optimized_logical_plan_with_shared_sources_and_registries(
        sql,
        shared_sources,
        aggregate_registry,
        stateful_registry,
        sinks,
    );
    let explain = ExplainReport::from_logical(optimized);
    println!("{}", sql);
    println!("{}", explain.table_string());
    explain.to_json().to_string()
}

fn optimized_logical_plan_with_shared_sources(
    sql: &str,
    shared_sources: &[&str],
) -> Arc<LogicalPlan> {
    optimized_logical_plan_with_shared_sources_and_registries(
        sql,
        shared_sources,
        Arc::new(StaticAggregateRegistry::new([
            "sum", "count", "last_row", "ndv",
        ])),
        Arc::new(StaticStatefulRegistry::new(
            builtin_stateful_function_names().iter().copied(),
        )),
        vec![],
    )
}

fn optimized_logical_plan_with_shared_sources_and_registries(
    sql: &str,
    shared_sources: &[&str],
    aggregate_registry: Arc<StaticAggregateRegistry>,
    stateful_registry: Arc<StaticStatefulRegistry>,
    sinks: Vec<PipelineSink>,
) -> Arc<LogicalPlan> {
    let stream_defs = setup_streams();
    let select_stmt =
        parse_sql_with_registries(sql, aggregate_registry, stateful_registry).expect("parse sql");
    let bindings =
        bindings_for_select_with_shared_sources(&select_stmt, &stream_defs, shared_sources);
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs).expect("logical");
    let (optimized, _pruned) = flow::optimize_logical_plan(logical_plan, &bindings);
    optimized
}

fn optimized_logical_json_with_sinks(sql: &str, sinks: Vec<PipelineSink>) -> String {
    optimized_logical_json_with_shared_sources_and_registries(
        sql,
        &[],
        Arc::new(StaticAggregateRegistry::new([
            "sum", "count", "last_row", "ndv",
        ])),
        Arc::new(StaticStatefulRegistry::new(
            builtin_stateful_function_names().iter().copied(),
        )),
        sinks,
    )
}

fn build_nop_json_sink(sink_id: &'static str, connector_id: &'static str) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        connector_id,
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    PipelineSink::new(sink_id, connector)
}

#[test]
fn logical_optimizer_table_driven() {
    struct LogicalOptimizerCase {
        name: &'static str,
        sql: &'static str,
        covers: &'static [&'static str],
        expected: &'static str,
    }

    let cases = vec![
        LogicalOptimizerCase {
            name: "logical_optimizer_prunes_datasource_schema",
            sql: "SELECT a FROM stream_prune",
            covers: &["planner.logical.top_level_column_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_prune","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_keeps_full_schema_for_memory_collection_source",
            sql: "SELECT a FROM memory_collect",
            covers: &["planner.logical.top_level_column_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=memory_collect","decoder=none","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_keeps_window_partition_columns",
            sql: "SELECT a FROM stream_window GROUP BY statewindow(a = 1, a = 4) OVER (PARTITION BY b, c)",
            covers: &["planner.logical.top_level_column_pruning"],
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_window","decoder=json","schema=[a, b, c]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a = 1","emit=a = 4","partition_by=b,c"],"operator":"Window"}],"id":"Project_2","info":["fields=[a]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_prunes_struct_fields_and_explain_renders_projection",
            sql: "SELECT stream_struct.a, stream_struct.b->c FROM stream_struct",
            covers: &["planner.logical.struct_field_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_struct","decoder=json","schema=[a, b{c}]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_struct.a; stream_struct.b -> c]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_keeps_dynamic_list_index_and_prunes_element_struct_fields",
            sql: "SELECT stream_3.items[a]->c FROM stream_3",
            covers: &["planner.logical.list_element_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_3","decoder=json","schema=[items[*][struct{c}]]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_3.items[\"a\"] -> c]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_keeps_dynamic_list_index_and_keeps_full_element_struct",
            sql: "SELECT stream_3.items[a] FROM stream_3",
            covers: &["planner.logical.list_element_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_3","decoder=json","schema=[items[*][struct{c, d}]]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_3.items[\"a\"]]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_inserts_compute_for_repeated_subexpr_in_project_and_filter",
            sql: "SELECT a + 1 AS x, x + 1 AS y FROM stream WHERE x > 1 AND y > 1",
            covers: &["planner.logical.common_subexpression_elimination"],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Compute_3","info":["temps=[__vf_cse_1 = a + 1; __vf_cse_2 = __vf_cse_1 + 1]"],"operator":"Compute"}],"id":"Filter_1","info":["predicate=__vf_cse_1 > 1 AND __vf_cse_2 > 1"],"operator":"Filter"}],"id":"Project_2","info":["fields=[__vf_cse_1 as x; __vf_cse_2 as y]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_does_not_cse_function_calls",
            sql: "SELECT concat(a) AS x, concat(a) AS y FROM stream",
            covers: &["planner.logical.common_subexpression_elimination"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[concat(a) as x; concat(a) as y]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_does_not_cse_plain_identifier_or_literal",
            sql: "SELECT a AS x, a AS y, 1 AS one, 1 AS another_one FROM stream",
            covers: &["planner.logical.common_subexpression_elimination"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; a as y; 1 as one; 1 as another_one]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_select_star_disables_top_level_pruning",
            sql: "SELECT * FROM stream_prune",
            covers: &["planner.logical.top_level_column_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_prune","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[*]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_selecting_whole_list_element_keeps_full_element_struct",
            sql: "SELECT stream_3.items[0] FROM stream_3",
            covers: &["planner.logical.list_element_pruning"],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_3","decoder=json","schema=[items[0][struct{c, d}]]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_3.items[0]]"],"operator":"Project"}"##,
        },
        LogicalOptimizerCase {
            name: "logical_optimizer_cse_keeps_stateful_dependency_boundary",
            sql: "SELECT lag(a) + 1 AS x, x + 1 AS y FROM stream",
            covers: &["planner.logical.common_subexpression_elimination"],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Compute_3","info":["temps=[__vf_cse_1 = col_1 + 1]"],"operator":"Compute"}],"id":"Project_2","info":["fields=[__vf_cse_1 as x; __vf_cse_1 + 1 as y]"],"operator":"Project"}"##,
        },
    ];

    for case in cases {
        assert!(!case.covers.is_empty(), "case={} missing covers", case.name);
        let got = optimized_logical_json(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

fn find_single_datasource(
    plan: &Arc<LogicalPlan>,
) -> &flow::planner::logical::datasource::DataSource {
    fn walk(plan: &Arc<LogicalPlan>) -> Option<&flow::planner::logical::datasource::DataSource> {
        match plan.as_ref() {
            LogicalPlan::DataSource(ds) => Some(ds),
            _ => plan.children().iter().find_map(walk),
        }
    }

    walk(plan).expect("expected a datasource in logical plan")
}

#[test]
fn logical_optimizer_shared_source_keeps_full_nested_schema_and_records_required_schema() {
    let sql = "SELECT stream_3.items[0]->c FROM stream_3";
    let optimized = optimized_logical_plan_with_shared_sources(sql, &["stream_3"]);
    let explain = ExplainReport::from_logical(optimized.clone());
    println!("{}", sql);
    println!("{}", explain.table_string());

    let datasource = find_single_datasource(&optimized);
    assert_eq!(
        datasource.shared_required_schema(),
        Some(&["items".to_string()][..])
    );
    assert!(
        datasource.decode_projection().is_none(),
        "shared source should not enable decode projection"
    );

    let schema = datasource.schema();
    let columns = schema.column_schemas();
    assert_eq!(
        columns.len(),
        2,
        "shared source should keep full top-level schema"
    );
    assert_eq!(columns[0].name, "a");
    assert_eq!(columns[1].name, "items");

    let ConcreteDatatype::List(list_type) = &columns[1].data_type else {
        panic!("expected items to stay list-typed");
    };
    let ConcreteDatatype::Struct(struct_type) = list_type.item_type() else {
        panic!("expected list element to stay struct-typed");
    };
    let struct_fields = struct_type.fields();
    let field_names: Vec<_> = struct_fields
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    assert_eq!(
        field_names,
        vec!["c".to_string(), "d".to_string()],
        "shared source should keep full nested struct fields"
    );
}

#[test]
fn logical_optimizer_shared_source_keeps_full_schema_under_alias_projection() {
    let sql = "SELECT a AS x FROM stream_prune";
    let optimized = optimized_logical_plan_with_shared_sources(sql, &["stream_prune"]);
    let explain = ExplainReport::from_logical(optimized.clone());
    println!("{}", sql);
    println!("{}", explain.table_string());

    let datasource = find_single_datasource(&optimized);
    assert_eq!(
        datasource.shared_required_schema(),
        Some(&["a".to_string()][..])
    );
    assert!(
        datasource.decode_projection().is_none(),
        "shared source alias projection should not enable decode projection"
    );

    let schema = datasource.schema();
    let columns = schema.column_schemas();
    assert_eq!(
        columns.len(),
        2,
        "shared source should keep full top-level schema under alias projection"
    );
    assert_eq!(columns[0].name, "a");
    assert_eq!(columns[1].name, "b");
}

#[test]
fn logical_optimizer_regular_source_decode_projection_keeps_top_level_columns() {
    let sql = "SELECT stream_3.a, stream_3.items[0]->c, stream_3.items[3]->c FROM stream_3";
    let optimized = optimized_logical_plan_with_shared_sources(sql, &[]);
    let explain = ExplainReport::from_logical(optimized.clone());
    println!("{}", sql);
    println!("{}", explain.table_string());

    let datasource = find_single_datasource(&optimized);
    let projection = datasource
        .decode_projection()
        .expect("regular source should produce decode projection");
    println!("decode_projection: {projection:?}");

    assert_eq!(
        datasource.shared_required_schema(),
        None,
        "regular source should not record shared required schema"
    );

    let schema = datasource.schema();
    let columns = schema.column_schemas();
    assert_eq!(
        columns
            .iter()
            .map(|col| col.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "items"]
    );

    assert_eq!(
        projection.column("a"),
        Some(&ProjectionNode::All),
        "top-level primitive column should stay fully decoded",
    );

    let Some(ProjectionNode::List { indexes, element }) = projection.column("items") else {
        panic!("expected list decode projection for items");
    };

    let mut expected_indexes = BTreeSet::new();
    expected_indexes.insert(0usize);
    expected_indexes.insert(3usize);
    assert_eq!(
        indexes,
        &ListIndexSelection::Indexes(expected_indexes),
        "list decode projection should keep only referenced sparse indexes",
    );

    let ProjectionNode::Struct(fields) = element.as_ref() else {
        panic!("expected list element projection to stay struct-shaped");
    };
    assert_eq!(
        fields.get("c"),
        Some(&ProjectionNode::All),
        "projected struct field should stay fully decoded",
    );
    assert_eq!(
        fields.len(),
        1,
        "only referenced struct field should remain in the list element projection",
    );
}

#[test]
fn create_logical_plan_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        covers: &'static [&'static str],
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "test_create_logical_plan_simple",
            sql: "SELECT a, b FROM users",
            covers: &[],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_order_by_single_key",
            sql: "SELECT a FROM users ORDER BY b",
            covers: &[],
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Order_1","info":["keys=[b ASC]"],"operator":"Order"}],"id":"Project_2","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_filter_then_order_by_multi_key",
            sql: "SELECT a FROM users WHERE a > 10 ORDER BY b DESC, c",
            covers: &[],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, c]"],"operator":"DataSource"}],"id":"Filter_1","info":["predicate=a > 10"],"operator":"Filter"}],"id":"Order_2","info":["keys=[b DESC; c ASC]"],"operator":"Order"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_order_by_stateful_key",
            sql: "SELECT a FROM users ORDER BY lag(a)",
            covers: &[],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Order_2","info":["keys=[col_1 ASC]"],"operator":"Order"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_filter",
            sql: "SELECT a, b FROM users WHERE a > 10",
            covers: &[],
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Filter_1","info":["predicate=a > 10"],"operator":"Filter"}],"id":"Project_2","info":["fields=[a; b]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_state_window",
            sql: "SELECT * FROM users GROUP BY statewindow(a > 0, b = 1)",
            covers: &["parser.window.state"],
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, c, k1, k2]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a > 0","emit=b = 1"],"operator":"Window"}],"id":"Project_2","info":["fields=[*]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_state_window_partition_by",
            sql:
                "SELECT * FROM users GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)",
            covers: &["parser.window.state"],
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, c, k1, k2]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a > 0","emit=b = 1","partition_by=k1,k2"],"operator":"Window"}],"id":"Project_2","info":["fields=[*]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_sliding_window",
            sql: "SELECT * FROM users GROUP BY slidingwindow('ss', 10)",
            covers: &["parser.window.sliding"],
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, c, k1, k2]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"Window"}],"id":"Project_2","info":["fields=[*]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_func_field",
            sql: "SELECT a, concat(b), c AS custom_name FROM users",
            covers: &[],
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, c]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; concat(b); c as custom_name]"],"operator":"Project"}"##,
        },
    ];

    for case in cases {
        let _ = case.covers;
        let got = optimized_logical_json(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn create_logical_plan_stateful_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "test_create_logical_plan_with_stateful_projection",
            sql: "SELECT lag(a) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a)]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_acc_projection",
            sql: "SELECT acc_sum(a) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a)]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_acc_filter",
            sql: "SELECT a FROM stream WHERE acc_count(a) > 0",
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_count(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 0"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_acc_filter_over_partition",
            sql:
                "SELECT acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_multiple_acc_filter_over_partition_calls",
            sql:
                "SELECT acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1), acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1; acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_2]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1); col_2 as acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_acc_clause_in_where",
            sql:
                "SELECT a FROM stream WHERE acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) > 1",
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 1"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_acc_clause_and_window_aggregate",
            sql:
                "SELECT sum(a), acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) FROM stream GROUP BY countwindow(4)",
            expected: r##"{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=count","count=4"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2 as sum(a); col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_nested_stateful_filter_dependency",
            sql: "SELECT lag(a), lag(b) FILTER (WHERE lag(a)) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1; lag(b) FILTER (WHERE col_1) -> col_2]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a); col_2 as lag(b) FILTER (WHERE lag(a))]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_stateful_filter_over_partition",
            sql: "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1, k2]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2)]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_stateful_filter_over_partition_alias",
            sql:
                "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) as v1 FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1, k2]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as v1]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_dedup_and_dependent_filter",
            sql: "SELECT lag(a), lag(a) FILTER (WHERE lag(a)) FROM stream",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1; lag(a) FILTER (WHERE col_1) -> col_2]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a); col_2 as lag(a) FILTER (WHERE lag(a))]"],"operator":"Project"}"##,
        },
    ];

    for case in cases {
        let got = optimized_logical_json(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn create_logical_plan_with_sinks_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "test_create_logical_plan_with_single_sink",
            sql: "SELECT a, b FROM users",
            sinks: vec![build_nop_json_sink("test_sink", "test_conn")],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_multiple_sinks",
            sql: "SELECT a, b FROM users",
            sinks: vec![
                build_nop_json_sink("sink1", "conn1"),
                build_nop_json_sink("sink2", "conn2"),
            ],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink1","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink2","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"}"##,
        },
        Case {
            name: "select_star_with_kuksa_sink",
            sql: "SELECT * FROM stream",
            sinks: vec![PipelineSink::new(
                "test_sink",
                PipelineSinkConnector::new(
                    "test_conn",
                    SinkConnectorConfig::Custom(CustomSinkConnectorConfig {
                        kind: "kuksa".to_string(),
                        settings: json!({}),
                    }),
                    SinkEncoderConfig::new("none", serde_json::Map::new()),
                ),
            )],
            expected: r##"{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[*]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=kuksa","encoder=none"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"}"##,
        },
    ];

    for case in cases {
        let got = optimized_logical_json_with_sinks(case.sql, case.sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn create_logical_plan_error_table_driven() {
    struct ErrorCase {
        name: &'static str,
        sql: &'static str,
        expected_contains: &'static [&'static str],
    }

    let cases = vec![
        ErrorCase {
            name: "test_reject_group_by_without_aggregates",
            sql: "SELECT * FROM users GROUP BY b, c",
            expected_contains: &["GROUP BY without aggregate functions is not supported"],
        },
        ErrorCase {
            name: "test_rejects_unknown_list_struct_field_during_logical_planning",
            sql: "SELECT a, items[0]->b FROM stream_3",
            expected_contains: &["field `b` not found"],
        },
        ErrorCase {
            name: "test_reject_relation_alias",
            sql: "SELECT a FROM users AS u",
            expected_contains: &["relation alias `u` for `users` is not supported yet"],
        },
    ];

    for case in cases {
        let stream_defs = setup_streams();
        let select_stmt = parse_sql(case.sql).expect("parse sql");
        let err = create_logical_plan(select_stmt, Vec::new(), &stream_defs).unwrap_err();
        println!("{}", case.sql);
        println!("{err}");
        for needle in case.expected_contains {
            assert!(
                err.contains(needle),
                "case={} expected err to contain {needle:?}, got: {err}",
                case.name
            );
        }
    }
}
