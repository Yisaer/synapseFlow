use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::planner::sink::{
    PipelineSink, PipelineSinkConnector, SinkConnectorConfig, SinkEncoderConfig, SinkOutputConfig,
};
use flow::{
    CommonSinkProps, NopSinkConfig, PipelineExplain, PipelineExplainConfig, PipelineRegistries,
};
use parser::parse_sql;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

use flow::pipeline::PipelineOptions;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};

fn setup_streams_ab() -> HashMap<String, Arc<StreamDefinition>> {
    let schema = Arc::new(Schema::new(vec![
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
    let def = StreamDefinition::new(
        "stream_ab",
        Arc::clone(&schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    HashMap::from([("stream_ab".to_string(), Arc::new(def))])
}

fn setup_streams_full() -> HashMap<String, Arc<StreamDefinition>> {
    let schema = Arc::new(Schema::new(vec![
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
    ]));
    let def = StreamDefinition::new(
        "stream",
        Arc::clone(&schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    HashMap::from([("stream".to_string(), Arc::new(def))])
}

fn build_nop_json_sink(sink_id: &str, batch_count: Option<usize>) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json(),
    );
    let sink = PipelineSink::new(sink_id.to_string(), connector);
    match batch_count {
        None => sink,
        Some(batch_count) => {
            let mut common_props = CommonSinkProps::default();
            common_props.batch_count = Some(batch_count);
            sink.with_common_props(common_props)
        }
    }
}

fn build_nop_json_include_sink(sink_id: &str, include_columns: &[&str]) -> PipelineSink {
    build_nop_json_sink(sink_id, None).with_output(
        SinkOutputConfig::default().with_include_columns(include_columns.iter().copied()),
    )
}

fn build_nop_json_delta_include_sink(sink_id: &str, include_columns: &[&str]) -> PipelineSink {
    build_nop_json_sink(sink_id, None).with_output(
        SinkOutputConfig::delta().with_include_columns(include_columns.iter().copied()),
    )
}

fn build_nop_json_exclude_sink(sink_id: &str, exclude_columns: &[&str]) -> PipelineSink {
    build_nop_json_sink(sink_id, None).with_output(
        SinkOutputConfig::default().with_exclude_columns(exclude_columns.iter().copied()),
    )
}

fn explain_json(
    sql: &str,
    sinks: Vec<PipelineSink>,
    streams: &HashMap<String, Arc<StreamDefinition>>,
) -> String {
    let registries = PipelineRegistries::new_with_builtin();
    let select_stmt = parse_sql(sql).expect("parse sql");
    let bindings = bindings_for_select(&select_stmt, streams);
    let logical_plan =
        flow::planner::logical::create_logical_plan(select_stmt, sinks, streams).expect("logical");
    let (logical_plan, bindings) = flow::optimize_logical_plan(logical_plan, &bindings);
    let physical_plan =
        flow::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
            .expect("physical");
    let physical_plan = flow::optimize_physical_plan(
        physical_plan,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let explain = PipelineExplain::new(
        logical_plan,
        physical_plan,
        PipelineExplainConfig::default(),
    );
    println!("{sql}");
    println!("{}", explain.to_pretty_string());
    explain.to_json().to_string()
}

fn bindings_for_select(
    select_stmt: &parser::SelectStmt,
    streams: &HashMap<String, Arc<StreamDefinition>>,
) -> SchemaBinding {
    let mut entries = Vec::new();
    for source in &select_stmt.source_infos {
        if let Some(def) = streams.get(&source.name) {
            entries.push(SchemaBindingEntry {
                source_name: source.name.clone(),
                alias: source.alias.clone(),
                schema: Arc::clone(&def.schema()),
                kind: SourceBindingKind::Regular,
            });
        }
    }
    SchemaBinding::new(entries)
}

#[test]
fn plan_explain_column_filter_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        streams: HashMap<String, Arc<StreamDefinition>>,
        expected: &'static str,
    }

    let cases = vec![
        // Case 1: Single sink, include=[a], non-delta
        Case {
            name: "single_sink_include_non_delta",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_nop_json_include_sink("sink_1", &["a"])],
            streams: setup_streams_ab(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_1","connector=nop","encoder=json","output.include_columns=[a]"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=sink_1","encoder=json","by_index_projection=[stream_ab#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_1","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        // Case 2: Single sink, exclude=[b], non-delta
        Case {
            name: "single_sink_exclude_non_delta",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_nop_json_exclude_sink("sink_1", &["b"])],
            streams: setup_streams_ab(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_1","connector=nop","encoder=json","output.exclude_columns=[b]"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=sink_1","encoder=json","by_index_projection=[stream_ab#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_1","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        // Case 3: Two sinks, different include lists
        Case {
            name: "two_sinks_different_include",
            sql: "SELECT a, b, flag FROM stream",
            sinks: vec![
                build_nop_json_include_sink("sink_1", &["a", "flag"]),
                build_nop_json_include_sink("sink_2", &["b"]),
            ],
            streams: setup_streams_full(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b; flag]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_1","connector=nop","encoder=json","output.include_columns=[a, flag]"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b; flag]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_2","connector=nop","encoder=json","output.include_columns=[b]"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=sink_1","encoder=json","by_index_projection=[stream#0->a; stream#2->flag]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_1","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_8","info":["sink_id=sink_2","encoder=json","by_index_projection=[stream#1->b]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_6","info":["sink_id=sink_2","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalBarrier_10","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalResultCollect_9","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        // Case 4: Delta mode + include_columns
        Case {
            name: "single_sink_delta_include",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_nop_json_delta_include_sink("sink_1", &["a"])],
            streams: setup_streams_ab(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_1","connector=nop","encoder=json","output.mode=delta","output.include_columns=[a]"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_5","info":["sink_id=sink_1","mode=delta","columns=[a]","by_index_projection=[stream_ab.a]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_6","info":["sink_id=sink_1","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_1","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_7","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        let got = explain_json(case.sql, case.sinks, &case.streams);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}
