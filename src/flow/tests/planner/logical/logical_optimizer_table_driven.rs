use datatypes::{
    ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, StructField, StructType,
};
use flow::planner::logical::create_logical_plan;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::{ExplainReport, MqttStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use parser::parse_sql;
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

    let mut stream_defs = HashMap::new();
    stream_defs.insert("users".to_string(), Arc::new(users_def));
    stream_defs.insert("stream_prune".to_string(), Arc::new(stream_prune_def));
    stream_defs.insert("stream_window".to_string(), Arc::new(stream_window_def));
    stream_defs.insert("stream_struct".to_string(), Arc::new(stream_struct_def));
    stream_defs
}

fn bindings_for_select(
    select_stmt: &parser::SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
) -> SchemaBinding {
    SchemaBinding::new(
        select_stmt
            .source_infos
            .iter()
            .map(|source| {
                let def = stream_defs
                    .get(&source.name)
                    .unwrap_or_else(|| panic!("missing stream definition: {}", source.name));
                SchemaBindingEntry {
                    source_name: source.name.clone(),
                    alias: source.alias.clone(),
                    schema: def.schema(),
                    kind: SourceBindingKind::Regular,
                }
            })
            .collect(),
    )
}

fn optimized_logical_json(sql: &str) -> String {
    let stream_defs = setup_streams();
    let select_stmt = parse_sql(sql).expect("parse sql");
    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");
    let (optimized, _pruned) = flow::optimize_logical_plan(logical_plan, &bindings);
    let explain = ExplainReport::from_logical(optimized);
    println!("{}", sql);
    println!("{}", explain.table_string());
    explain.to_json().to_string()
}

fn logical_plan_json(sql: &str) -> String {
    let stream_defs = setup_streams();
    let select_stmt = parse_sql(sql).expect("parse sql");
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");
    let explain = ExplainReport::from_logical(logical_plan);
    println!("{}", sql);
    println!("{}", explain.table_string());
    explain.to_json().to_string()
}

#[test]
fn logical_optimizer_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "logical_optimizer_prunes_datasource_schema",
            sql: "SELECT a FROM stream_prune",
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_prune","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "logical_optimizer_keeps_window_partition_columns",
            sql: "SELECT a FROM stream_window GROUP BY statewindow(a = 1, a = 4) OVER (PARTITION BY b, c)",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_window","decoder=json","schema=[a, b, c]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a = 1","emit=a = 4","partition_by=b,c"],"operator":"Window"}],"id":"Project_2","info":["fields=[a]"],"operator":"Project"}"##,
        },
        Case {
            name: "logical_optimizer_prunes_struct_fields_and_explain_renders_projection",
            sql: "SELECT stream_struct.a, stream_struct.b->c FROM stream_struct",
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_struct","decoder=json","schema=[a, b{c}]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_struct.a; stream_struct.b -> c]"],"operator":"Project"}"##,
        },
    ];

    for case in cases {
        let got = optimized_logical_json(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn create_logical_plan_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "test_create_logical_plan_simple",
            sql: "SELECT a, b FROM users",
            expected: r##"{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, k1, k2]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_filter",
            sql: "SELECT a, b FROM users WHERE a > 10",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, k1, k2]"],"operator":"DataSource"}],"id":"Filter_1","info":["predicate=a > 10"],"operator":"Filter"}],"id":"Project_2","info":["fields=[a; b]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_state_window",
            sql: "SELECT * FROM users GROUP BY statewindow(a > 0, b = 1)",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, k1, k2]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a > 0","emit=b = 1"],"operator":"Window"}],"id":"Project_2","info":["fields=[*]"],"operator":"Project"}"##,
        },
        Case {
            name: "test_create_logical_plan_with_state_window_partition_by",
            sql: "SELECT * FROM users GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)",
            expected: r##"{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=users","decoder=json","schema=[a, b, k1, k2]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a > 0","emit=b = 1","partition_by=k1,k2"],"operator":"Window"}],"id":"Project_2","info":["fields=[*]"],"operator":"Project"}"##,
        },
    ];

    for case in cases {
        let got = logical_plan_json(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}
