use datatypes::{
    ColumnSchema, ConcreteDatatype, Int64Type, ListType, Schema, StringType, StructField,
    StructType,
};
use flow::catalog::MockStreamProps;
use flow::planner::logical::create_logical_plan;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::Catalog;
use flow::EventtimeDefinition;
use flow::{
    CommonSinkProps, MqttStreamProps, NopSinkConfig, PipelineExplain, PipelineRegistries,
    PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig,
    StreamDecoderConfig, StreamDefinition, StreamProps,
};
use parser::parse_sql;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flow::pipeline::{EventtimeOptions, PipelineOptions, PlanCacheOptions};

fn setup_streams() -> HashMap<String, Arc<StreamDefinition>> {
    let stream_schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]));
    let stream_def = StreamDefinition::new(
        "stream",
        Arc::clone(&stream_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let user_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("d".to_string(), ConcreteDatatype::String(StringType), false),
    ])));
    let stream_2_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_2".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new("stream_2".to_string(), "b".to_string(), user_struct),
    ]));
    let stream_2_def = StreamDefinition::new(
        "stream_2",
        Arc::clone(&stream_2_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let stream_enc_schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream_enc".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]));
    let stream_enc_def = StreamDefinition::new(
        "stream_enc",
        Arc::clone(&stream_enc_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let stream_ab_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_ab".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "stream_ab".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let stream_ab_def = StreamDefinition::new(
        "stream_ab",
        Arc::clone(&stream_ab_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let element_struct_cd = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
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
            ConcreteDatatype::List(ListType::new(Arc::new(element_struct_cd))),
        ),
    ]));
    let stream_3_def = StreamDefinition::new(
        "stream_3",
        Arc::clone(&stream_3_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let element_xy_struct_cd = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("x".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("y".to_string(), ConcreteDatatype::String(StringType), false),
    ])));
    let b_struct_cd = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new(
            "items".to_string(),
            ConcreteDatatype::List(ListType::new(Arc::new(element_xy_struct_cd))),
            false,
        ),
    ])));
    let stream_4_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_4".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new("stream_4".to_string(), "b".to_string(), b_struct_cd),
    ]));
    let stream_4_def = StreamDefinition::new(
        "stream_4",
        Arc::clone(&stream_4_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let mut stream_defs = HashMap::new();
    stream_defs.insert("stream".to_string(), Arc::new(stream_def));
    stream_defs.insert("stream_2".to_string(), Arc::new(stream_2_def));
    stream_defs.insert("stream_enc".to_string(), Arc::new(stream_enc_def));
    stream_defs.insert("stream_ab".to_string(), Arc::new(stream_ab_def));
    stream_defs.insert("stream_3".to_string(), Arc::new(stream_3_def));
    stream_defs.insert("stream_4".to_string(), Arc::new(stream_4_def));

    stream_defs
}

fn setup_catalog_with_eventtime_stream() -> Catalog {
    let stream_name = "stream_eventtime";
    let schema = Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "event_ts".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]);

    let catalog = Catalog::new();
    catalog.upsert(
        StreamDefinition::new(
            stream_name.to_string(),
            Arc::new(schema),
            StreamProps::Mock(MockStreamProps::default()),
            StreamDecoderConfig::json(),
        )
        .with_eventtime(EventtimeDefinition::new(
            "event_ts".to_string(),
            "unixtimestamp_ms".to_string(),
        )),
    );
    catalog
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

fn build_nop_json_sink(sink_id: &'static str, batch_count: Option<usize>) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new(sink_id, connector);
    match batch_count {
        None => sink,
        Some(batch_count) => {
            let mut common_props = CommonSinkProps::default();
            common_props.batch_count = Some(batch_count);
            sink.with_common_props(common_props)
        }
    }
}

fn explain_json(sql: &str, sinks: Vec<PipelineSink>) -> String {
    let registries = PipelineRegistries::new_with_builtin();
    let stream_defs = setup_streams();

    let select_stmt = parse_sql(sql).expect("parse sql");

    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs).expect("logical");

    let (logical_plan, bindings) = flow::optimize_logical_plan(logical_plan, &bindings);

    let physical_plan =
        flow::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
            .expect("physical");

    let physical_plan = flow::optimize_physical_plan(
        physical_plan,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let explain = PipelineExplain::new(logical_plan, physical_plan);
    println!("{sql}");
    println!("{}", explain.to_pretty_string());
    explain.to_json().to_string()
}

fn explain_json_string(sql: &str) -> String {
    explain_json(sql, vec![])
}

fn explain_eventtime_json_string(sql: &str, options: &PipelineOptions) -> String {
    let catalog = setup_catalog_with_eventtime_stream();
    let registries = PipelineRegistries::new_with_builtin();
    let explain = flow::explain_pipeline_with_options(
        sql,
        vec![],
        &catalog,
        flow::shared_stream_registry(),
        &registries,
        options,
    )
    .expect("explain pipeline");

    println!("{sql}");
    println!("{}", explain.to_pretty_string());
    explain.to_json().to_string()
}

#[test]
fn plan_explain_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        options: PipelineOptions,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "stateful_select_only",
            sql: "SELECT lag(a) FROM stream",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_where_before_project",
            sql: "SELECT a FROM stream WHERE lag(a) > 0",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 0"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalFilter_3","info":["predicate=col_1 > 0"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_4","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_before_window_and_aggregation",
            sql: "SELECT sum(a), lag(a) FROM stream GROUP BY tumblingwindow('ss', 10)",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2; col_1]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProcessTimeWatermark_3","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_5","info":["calls=[sum(a) -> col_2]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_6","info":["fields=[col_2; col_1]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "physical_explain_reflects_pruned_struct_schema",
            sql: "SELECT stream_2.a, stream_2.b->c FROM stream_2",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_2","decoder=json","schema=[a, b{c}]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_2.a; stream_2.b -> c]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_2","schema=[a, b{c}]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b{c}]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[stream_2.a; stream_2.b -> c]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "explain_reflects_pruned_list_struct_schema",
            sql: "SELECT stream_3.a, stream_3.items[0]->c FROM stream_3",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_3","decoder=json","schema=[a, items[0][struct{c}]]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_3.a; stream_3.items[0] -> c]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_3","schema=[a, items[0][struct{c}]]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, items[0][struct{c}]]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[stream_3.a; stream_3.items[0] -> c]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "physical_plan_supports_parenthesized_struct_field_list_index",
            sql: "SELECT a, (b->items)[0] FROM stream_4",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_4","decoder=json","schema=[a, b{items[struct{x, y}]}]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; (b -> items)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_4","schema=[a, b{items[struct{x, y}]}]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b{items[struct{x, y}]}]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; (b -> items)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "explain_renders_list_index_projection_compact",
            sql:
                "SELECT stream_4.a, stream_4.b->items[0]->x, stream_4.b->items[3]->x FROM stream_4",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_4","decoder=json","schema=[a, b{items[0,3][struct{x}]}]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_4.a; stream_4.b -> items[0] -> x; stream_4.b -> items[3] -> x]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_4","schema=[a, b{items[0,3][struct{x}]}]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b{items[0,3][struct{x}]}]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[stream_4.a; stream_4.b -> items[0] -> x; stream_4.b -> items[3] -> x]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "explain_ndv_countwindow_non_incremental",
            sql: "SELECT ndv(a) FROM stream GROUP BY countwindow(4)",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=count","count=4"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[ndv(a) -> col_1]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCountWindow_2","info":["kind=count","count=4"],"operator":"PhysicalCountWindow"}],"id":"PhysicalAggregation_3","info":["calls=[ndv(a) -> col_1]"],"operator":"PhysicalAggregation"}],"id":"PhysicalProject_4","info":["fields=[col_1]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "explain_pipeline_with_eventtime_enabled_prints_plans",
            sql: "SELECT sum(a), lag(a) FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
            options: PipelineOptions {
                plan_cache: PlanCacheOptions { enabled: false },
                eventtime: EventtimeOptions {
                    enabled: true,
                    late_tolerance: Duration::from_secs(5),
                },
            },
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_eventtime","decoder=json","schema=[a, event_ts]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2; col_1]"],"operator":"Project"},"options":{"eventtime_enabled":true,"eventtime_late_tolerance_ms":5000},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_eventtime","schema=[a, event_ts]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, event_ts]","eventtime.column=event_ts","eventtime.type=unixtimestamp_ms","eventtime.index=1"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalEventtimeWatermark_3","info":["window=tumbling","unit=Seconds","length=10","mode=event_time","lateToleranceMs=5000"],"operator":"PhysicalEventtimeWatermark"}],"id":"PhysicalStreamingAggregation_5","info":["calls=[sum(a) -> col_2]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_6","info":["fields=[col_2; col_1]"],"operator":"PhysicalProject"}}"##,
        },
    ];

    for case in cases {
        let got = if case.options.eventtime.enabled {
            explain_eventtime_json_string(case.sql, &case.options)
        } else {
            explain_json_string(case.sql)
        };
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_optimizer_table_driven() {
    #[derive(Clone, Copy)]
    struct SinkSpec {
        sink_id: &'static str,
        batch_count: Option<usize>,
    }

    const SINK_NO_BATCH: &[SinkSpec] = &[SinkSpec {
        sink_id: "test_sink",
        batch_count: None,
    }];
    const SINK_BATCH_10: &[SinkSpec] = &[SinkSpec {
        sink_id: "test_sink",
        batch_count: Some(10),
    }];

    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: &'static [SinkSpec],
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "optimize_rewrites_batch_encoder_chain_to_streaming_encoder",
            sql: "SELECT a FROM stream_enc",
            sinks: SINK_BATCH_10,
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_enc","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","batching=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_enc","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}],"id":"PhysicalStreamingEncoder_5","info":["sink_id=test_sink","encoder=json","batching=true"],"operator":"PhysicalStreamingEncoder"}],"id":"PhysicalDataSink_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_6","info":["sink_count=1"],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "optimize_rewrites_streaming_agg",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY tumblingwindow('ss', 10),b",
            sinks: SINK_NO_BATCH,
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_1]"],"operator":"PhysicalProject"}],"id":"PhysicalEncoder_7","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalEncoder"}],"id":"PhysicalDataSink_6","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_8","info":["sink_count=1"],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "optimize_rewrites_streaming_agg_for_sliding_window",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY slidingwindow('ss', 10),b",
            sinks: SINK_NO_BATCH,
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=sliding","unit=Seconds","lookback=10","lookahead=none","mode=processing_time","interval=1"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_1]"],"operator":"PhysicalProject"}],"id":"PhysicalEncoder_7","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalEncoder"}],"id":"PhysicalDataSink_6","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_8","info":["sink_count=1"],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "optimize_rewrites_streaming_agg_for_state_window",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY statewindow(a > 0, a = 4), b",
            sinks: SINK_NO_BATCH,
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a > 0","emit=a = 4"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStreamingAggregation_3","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=state","open=a > 0","emit=a = 4"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_4","info":["fields=[col_1]"],"operator":"PhysicalProject"}],"id":"PhysicalEncoder_6","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalEncoder"}],"id":"PhysicalDataSink_5","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_7","info":["sink_count=1"],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "physical_plan_sliding_without_lookahead_includes_watermark_for_gc",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY slidingwindow('ss', 10),b",
            sinks: SINK_NO_BATCH,
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=sliding","unit=Seconds","lookback=10","lookahead=none","mode=processing_time","interval=1"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_1]"],"operator":"PhysicalProject"}],"id":"PhysicalEncoder_7","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalEncoder"}],"id":"PhysicalDataSink_6","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_8","info":["sink_count=1"],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        let sinks = case
            .sinks
            .iter()
            .map(|s| build_nop_json_sink(s.sink_id, s.batch_count))
            .collect::<Vec<_>>();

        let got = explain_json(case.sql, sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}
