use datatypes::{
    ColumnSchema, ConcreteDatatype, Int64Type, ListType, Schema, StringType, StructField,
    StructType, Value,
};
use flow::catalog::MockStreamProps;
use flow::catalog::{
    MemoryStreamProps, VideoReconnectConfig, VideoRtspTransport, VideoStreamProps,
};
use flow::connector::sink::video::{
    VideoCodecConfig, VideoContainerConfig, VideoFileSinkConfig, VideoRollingConfig,
    VideoSinkConfig, VideoSinkTargetConfig,
};
use flow::connector::{KuksaSinkConfig, MemorySinkConfig, MemoryTopicKind};
use flow::expr::func::EvalError;
use flow::planner::logical::{create_logical_plan, create_logical_plan_with_source_inputs};
use flow::planner::sink::CustomSinkConnectorConfig;
use flow::planner::{
    create_physical_plan_with_build_options, optimize_logical_plan_with_options,
    PhysicalPlanBuildOptions,
};
use flow::processor::SamplerConfig;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::Catalog;
use flow::EventtimeDefinition;
use flow::{
    CommonSinkProps, CustomFunc, MqttStreamProps, NopSinkConfig, PipelineExplain,
    PipelineExplainConfig, PipelineRegistries, PipelineSink, PipelineSinkConnector,
    SinkConnectorConfig, SinkEncoderConfig, SinkOutputConfig, StreamDecoderConfig,
    StreamDefinition, StreamProps,
};
use parser::parse_sql;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flow::pipeline::{EventtimeOptions, PipelineOptions, SourceInputConfig};
use serde_json::Map as JsonMap;

fn setup_streams() -> HashMap<String, Arc<StreamDefinition>> {
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

    let stream_sampler_schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream_sampler".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]));
    let stream_sampler_def = StreamDefinition::new(
        "stream_sampler",
        Arc::clone(&stream_sampler_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    )
    .with_sampler(SamplerConfig::new(Duration::from_millis(100)));

    let shared_stream_sampler_schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "shared_stream_sampler".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]));
    let shared_stream_sampler_def = StreamDefinition::new(
        "shared_stream_sampler",
        Arc::clone(&shared_stream_sampler_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    )
    .with_sampler(SamplerConfig::new(Duration::from_millis(100)));

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

    let memory_bytes_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "memory_bytes".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "memory_bytes".to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let memory_bytes_def = StreamDefinition::new(
        "memory_bytes",
        Arc::clone(&memory_bytes_schema),
        StreamProps::Memory(MemoryStreamProps::new("demo_memory_bytes")),
        StreamDecoderConfig::json(),
    );

    let vehicle_stream_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "vehicle_stream".to_string(),
            "speed".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "vehicle_stream".to_string(),
            "rpm".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            "vehicle_stream".to_string(),
            "gear".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));
    let vehicle_stream_def = StreamDefinition::new(
        "vehicle_stream",
        Arc::clone(&vehicle_stream_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let camera_def = StreamDefinition::new(
        "camera",
        Arc::new(flow::codec::default_video_schema("camera")),
        StreamProps::Video(VideoStreamProps {
            url: "rtsp://127.0.0.1:8554/camera".to_string(),
            rtsp_transport: VideoRtspTransport::Tcp,
            reconnect: VideoReconnectConfig::default(),
        }),
        StreamDecoderConfig::none(),
    );

    let mut stream_defs = HashMap::new();
    stream_defs.insert("stream".to_string(), Arc::new(stream_def));
    stream_defs.insert("stream_sampler".to_string(), Arc::new(stream_sampler_def));
    stream_defs.insert(
        "shared_stream_sampler".to_string(),
        Arc::new(shared_stream_sampler_def),
    );
    stream_defs.insert("stream_2".to_string(), Arc::new(stream_2_def));
    stream_defs.insert("stream_enc".to_string(), Arc::new(stream_enc_def));
    stream_defs.insert("stream_ab".to_string(), Arc::new(stream_ab_def));
    stream_defs.insert("stream_3".to_string(), Arc::new(stream_3_def));
    stream_defs.insert("stream_4".to_string(), Arc::new(stream_4_def));
    stream_defs.insert("memory_collect".to_string(), Arc::new(memory_collect_def));
    stream_defs.insert("memory_bytes".to_string(), Arc::new(memory_bytes_def));
    stream_defs.insert("vehicle_stream".to_string(), Arc::new(vehicle_stream_def));
    stream_defs.insert("camera".to_string(), Arc::new(camera_def));

    stream_defs
}

fn setup_catalog_with_eventtime_stream_config(
    column: &'static str,
    type_key: &'static str,
) -> Catalog {
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
            column.to_string(),
            type_key.to_string(),
        )),
    );
    catalog
}

fn setup_catalog_with_eventtime_stream() -> Catalog {
    setup_catalog_with_eventtime_stream_config("event_ts", "unixtimestamp_ms")
}

fn setup_catalog_without_eventtime_stream() -> Catalog {
    let stream_name = "stream_without_eventtime";
    let schema = Schema::new(vec![ColumnSchema::new(
        stream_name.to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]);

    let catalog = Catalog::new();
    catalog.upsert(StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    ));
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
                let kind = if def.stream_type() == flow::StreamType::Memory
                    && def.decoder().kind() == "none"
                {
                    SourceBindingKind::MemoryCollection
                } else if source.name == "shared_stream_sampler" {
                    SourceBindingKind::Shared
                } else {
                    SourceBindingKind::Regular
                };
                SchemaBindingEntry {
                    source_name: source.name.clone(),
                    alias: source.alias.clone(),
                    schema: def.schema(),
                    kind,
                }
            })
            .collect(),
    )
}

fn build_nop_json_sink(sink_id: &'static str, batch_count: Option<usize>) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
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

fn build_nop_json_delta_sink(sink_id: &'static str, batch_count: Option<usize>) -> PipelineSink {
    build_nop_json_delta_sink_with_columns(sink_id, batch_count, None)
}

fn build_nop_json_delta_sink_with_columns(
    sink_id: &'static str,
    batch_count: Option<usize>,
    columns: Option<&[&'static str]>,
) -> PipelineSink {
    let sink = build_nop_json_sink(sink_id, batch_count);
    match columns {
        Some(columns) => sink.with_output(SinkOutputConfig::delta_with_columns(
            columns.iter().copied(),
        )),
        None => sink.with_output(SinkOutputConfig::delta()),
    }
}

fn build_nop_json_omit_if_empty_sink(
    sink_id: &'static str,
    batch_count: Option<usize>,
) -> PipelineSink {
    build_nop_json_sink(sink_id, batch_count)
        .with_output(SinkOutputConfig::default().with_omit_if_empty(true))
}

fn build_nop_json_delta_omit_if_empty_sink_with_columns(
    sink_id: &'static str,
    batch_count: Option<usize>,
    columns: Option<&[&'static str]>,
) -> PipelineSink {
    build_nop_json_delta_sink_with_columns(sink_id, batch_count, columns).with_output(match columns
    {
        Some(columns) => {
            SinkOutputConfig::delta_with_columns(columns.iter().copied()).with_omit_if_empty(true)
        }
        None => SinkOutputConfig::delta().with_omit_if_empty(true),
    })
}

fn build_nop_json_transform_sink(
    sink_id: &'static str,
    batch_count: Option<usize>,
) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::json_with_transform_template(
            r#"{"x":{{ json(.row.c) }},"y":{{ json(.row.d) }} }"#,
        ),
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

fn build_nop_json_transform_delta_sink_with_columns(
    sink_id: &'static str,
    batch_count: Option<usize>,
    columns: Option<&[&'static str]>,
) -> PipelineSink {
    let sink = build_nop_json_transform_sink(sink_id, batch_count);
    match columns {
        Some(columns) => sink.with_output(SinkOutputConfig::delta_with_columns(
            columns.iter().copied(),
        )),
        None => sink.with_output(SinkOutputConfig::delta()),
    }
}

fn build_nop_none_sink(sink_id: &'static str) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::new("none", JsonMap::new()),
    );
    PipelineSink::new(sink_id, connector)
}

fn build_video_sink(sink_id: &'static str) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_video_connector",
        SinkConnectorConfig::Video(VideoSinkConfig {
            target: VideoSinkTargetConfig::File(VideoFileSinkConfig {
                path: "/tmp/veloflux-video-planner".to_string(),
                filename_prefix: "camera".to_string(),
            }),
            codec: VideoCodecConfig::H264,
            container: VideoContainerConfig::Mp4,
            rolling: VideoRollingConfig::Duration {
                duration: Duration::from_secs(60),
            },
        }),
        SinkEncoderConfig::new("none", JsonMap::new()),
    );
    PipelineSink::new(sink_id, connector)
}

fn build_memory_collection_delta_sink_with_columns(
    sink_id: &'static str,
    topic: &'static str,
    columns: Option<&[&'static str]>,
) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Memory(MemorySinkConfig::new(
            sink_id,
            topic,
            MemoryTopicKind::Collection,
        )),
        SinkEncoderConfig::new("none", JsonMap::new()),
    );
    let sink = PipelineSink::new(sink_id, connector);
    match columns {
        Some(columns) => sink.with_output(SinkOutputConfig::delta_with_columns(
            columns.iter().copied(),
        )),
        None => sink.with_output(SinkOutputConfig::delta()),
    }
}

fn build_kuksa_delta_sink(sink_id: &'static str) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Kuksa(KuksaSinkConfig {
            sink_name: sink_id.to_string(),
            addr: "http://127.0.0.1:55555".to_string(),
            vss_path: "/tmp/test_vss.json".to_string(),
        }),
        SinkEncoderConfig::new("none", JsonMap::new()),
    );
    PipelineSink::new(sink_id, connector).with_output(SinkOutputConfig::delta())
}

fn build_kuksa_sink_with_batch(sink_id: &'static str, batch_count: usize) -> PipelineSink {
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Kuksa(KuksaSinkConfig {
            sink_name: sink_id.to_string(),
            addr: "http://127.0.0.1:55555".to_string(),
            vss_path: "/tmp/test_vss.json".to_string(),
        }),
        SinkEncoderConfig::new("none", JsonMap::new()),
    );
    let mut common = CommonSinkProps::default();
    common.batch_count = Some(batch_count);
    PipelineSink::new(sink_id, connector).with_common_props(common)
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
    let explain = PipelineExplain::new(
        logical_plan,
        physical_plan,
        PipelineExplainConfig::default(),
    );
    println!("{sql}");
    println!("{}", explain.to_pretty_string());
    explain.to_json().to_string()
}

fn explain_json_with_source_inputs(
    sql: &str,
    sinks: Vec<PipelineSink>,
    source_inputs: HashMap<String, SourceInputConfig>,
) -> String {
    let registries = PipelineRegistries::new_with_builtin();
    let stream_defs = setup_streams();

    let select_stmt = parse_sql(sql).expect("parse sql");

    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan =
        create_logical_plan_with_source_inputs(select_stmt, sinks, &stream_defs, &source_inputs)
            .expect("logical");

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

fn explain_json_before_after_physical_opt(sql: &str, sinks: Vec<PipelineSink>) -> (String, String) {
    let registries = PipelineRegistries::new_with_builtin();
    let stream_defs = setup_streams();

    let select_stmt = parse_sql(sql).expect("parse sql");

    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs).expect("logical");

    let (logical_plan, bindings) = flow::optimize_logical_plan(logical_plan, &bindings);

    let physical_plan =
        flow::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
            .expect("physical");

    let explain_before = PipelineExplain::new(
        Arc::clone(&logical_plan),
        Arc::clone(&physical_plan),
        PipelineExplainConfig::default(),
    );
    let before = explain_before.to_json().to_string();

    let optimized = flow::optimize_physical_plan(
        physical_plan,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let explain_after =
        PipelineExplain::new(logical_plan, optimized, PipelineExplainConfig::default());
    let after = explain_after.to_json().to_string();

    println!("{sql}");
    println!("--- before physical optimize ---");
    println!("{}", explain_before.to_pretty_string());
    println!("--- after physical optimize ---");
    println!("{}", explain_after.to_pretty_string());

    (before, after)
}

fn explain_json_string(sql: &str) -> String {
    explain_json(sql, vec![])
}

#[test]
fn plan_explain_video_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        expected: &'static str,
        covers: &'static [&'static str],
    }

    let cases = vec![
        Case {
            name: "video_source_projection_filter_uses_tuple_path",
            sql: "SELECT payload, width FROM camera WHERE width > 0",
            sinks: vec![build_nop_json_sink("json_sink", None)],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=camera","decoder=none","schema=[payload, width]"],"operator":"DataSource"}],"id":"Filter_1","info":["predicate=width > 0"],"operator":"Filter"}],"id":"Project_2","info":["fields=[payload; width]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=json_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=camera","schema=[payload, width]"],"operator":"PhysicalDataSource"}],"id":"PhysicalFilter_1","info":["predicate=width > 0"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=json_sink","encoder=json","by_index_projection=[camera#0->payload; camera#1->width]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=json_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            covers: &["source.video.tuple_input"],
        },
        Case {
            name: "video_sink_consumes_collection_without_veloflux_encoder",
            sql: "SELECT * FROM camera",
            sinks: vec![build_video_sink("video_sink")],
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=camera","decoder=none","schema=[payload, width, height, format, timestamp]"],"operator":"DataSource"}],"id":"DataSink_2","info":["sink_id=video_sink","connector=video","encoder=none"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=camera","schema=[payload, width, height, format, timestamp]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDataSink_1","info":["sink_id=video_sink","connector=video"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_2","info":[],"operator":"PhysicalResultCollect"}}"##,
            covers: &["source.video.tuple_input", "sink.connector.video_output"],
        },
        Case {
            name: "video_sink_eliminates_complete_identity_projection",
            sql: "SELECT payload, width, height, format, timestamp FROM camera",
            sinks: vec![build_video_sink("video_sink")],
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=camera","decoder=none","schema=[payload, width, height, format, timestamp]"],"operator":"DataSource"}],"id":"DataSink_2","info":["sink_id=video_sink","connector=video","encoder=none"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=camera","schema=[payload, width, height, format, timestamp]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDataSink_1","info":["sink_id=video_sink","connector=video"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_2","info":[],"operator":"PhysicalResultCollect"}}"##,
            covers: &[
                "source.video.tuple_input",
                "sink.connector.video_output",
                "planner.logical.video_sink_identity_project_elimination",
            ],
        },
    ];

    for case in cases {
        assert!(
            !case.covers.is_empty(),
            "case={} missing coverage",
            case.name
        );
        let got = explain_json(case.sql, case.sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

fn explain_eventtime_json_string(sql: &str, options: &PipelineOptions) -> String {
    let catalog = setup_catalog_with_eventtime_stream();
    let registries = PipelineRegistries::new_with_builtin();
    let explain =
        flow::explain_pipeline_with_options(sql, vec![], &catalog, None, &registries, options)
            .expect("explain pipeline");

    println!("{sql}");
    println!("{}", explain.to_pretty_string());
    explain.to_json().to_string()
}

#[test]
fn plan_explain_with_sinks_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "select_star_with_kuksa_sink",
            sql: "SELECT * FROM stream",
            sinks: vec![PipelineSink::new(
                "test_sink",
                PipelineSinkConnector::new(
                    "test_connector",
                    SinkConnectorConfig::Custom(CustomSinkConnectorConfig {
                        kind: "kuksa".to_string(),
                        settings: json!({}),
                    }),
                    SinkEncoderConfig::new("none", serde_json::Map::new()),
                ),
            )],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[*]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=kuksa","encoder=none"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[*]"],"operator":"PhysicalProject"}],"id":"PhysicalDataSink_3","info":["sink_id=test_sink","connector=kuksa"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_4","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "memory_collection_sink_includes_layout",
            sql: "SELECT *, a AS x FROM stream_ab",
            sinks: vec![PipelineSink::new(
                "test_sink",
                PipelineSinkConnector::new(
                    "test_connector",
                    SinkConnectorConfig::Memory(MemorySinkConfig::new(
                        "test_sink",
                        "demo_collection",
                        MemoryTopicKind::Collection,
                    )),
                    SinkEncoderConfig::new("none", serde_json::Map::new()),
                ),
            )],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[*; a as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=memory","encoder=none"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[*; a as x]"],"operator":"PhysicalProject"}],"id":"PhysicalMemoryCollectionMaterialize_4","info":[],"operator":"PhysicalMemoryCollectionMaterialize"}],"id":"PhysicalDataSink_3","info":["sink_id=test_sink","connector=memory","topic=demo_collection","kind=collection"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "memory_bytes_sink_does_not_insert_materialize",
            sql: "SELECT a + 1 FROM stream",
            sinks: vec![PipelineSink::new(
                "test_sink",
                PipelineSinkConnector::new(
                    "test_connector",
                    SinkConnectorConfig::Memory(MemorySinkConfig::new(
                        "test_sink",
                        "demo_bytes",
                        MemoryTopicKind::Bytes,
                    )),
                    SinkEncoderConfig::json(),
                ),
            )],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a + 1]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=memory","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=memory","topic=demo_bytes","kind=bytes"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "omit_if_empty_full_output_inserts_empty_suppress_between_project_and_encoder",
            sql: "SELECT a FROM stream",
            sinks: vec![build_nop_json_omit_if_empty_sink("test_sink", None)],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.omit_if_empty=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalEmptySuppress_4","info":["sink_id=test_sink","omit_if_empty=true"],"operator":"PhysicalEmptySuppress"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        let got = explain_json(case.sql, case.sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_two_sinks_inserts_physical_barrier() {
    let sql = "SELECT a FROM stream";
    let sinks = vec![
        PipelineSink::new(
            "sink_a",
            PipelineSinkConnector::new(
                "connector_a",
                SinkConnectorConfig::Nop(NopSinkConfig::default()),
                SinkEncoderConfig::json(),
            ),
        )
        .with_forward_to_result(true),
        PipelineSink::new(
            "sink_b",
            PipelineSinkConnector::new(
                "connector_b",
                SinkConnectorConfig::Nop(NopSinkConfig::default()),
                SinkEncoderConfig::json(),
            ),
        )
        .with_forward_to_result(true),
    ];

    let got = explain_json(sql, sinks);

    // NOTE: keep this as a full JSON string comparison (table-driven style).
    let expected = r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_6","info":["sink_id=sink_b","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalBarrier_8","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalResultCollect_7","info":[],"operator":"PhysicalResultCollect"}}"##;
    assert_eq!(got, expected, "case=two_sinks_inserts_physical_barrier");
}

// coverage-covers: parser.window.count
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
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as lag(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_select_only_uses_stateful_plan_node",
            sql: "SELECT acc_sum(a) FROM stream",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_sum(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as acc_sum(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_filter_uses_stateful_plan_node_before_filter",
            sql: "SELECT a FROM stream WHERE acc_count(a) > 0",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_count(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 0"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_count(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalFilter_3","info":["predicate=col_1 > 0"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_4","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "searched_case_project",
            sql: "SELECT CASE WHEN a < 150 THEN 'S' WHEN a < 170 THEN 'M' WHEN a < 175 THEN 'L' ELSE 'XL' END AS sizeLabel FROM stream",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[CASE WHEN a < 150 THEN 'S' WHEN a < 170 THEN 'M' WHEN a < 175 THEN 'L' ELSE 'XL' END as sizeLabel]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[CASE WHEN a < 150 THEN 'S' WHEN a < 170 THEN 'M' WHEN a < 175 THEN 'L' ELSE 'XL' END as sizeLabel]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "simple_case_project",
            sql: "SELECT CASE a WHEN 1 THEN 'S' WHEN 2 THEN 'M' ELSE 'XL' END AS sizeLabel FROM stream",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[CASE a WHEN 1 THEN 'S' WHEN 2 THEN 'M' ELSE 'XL' END as sizeLabel]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[CASE a WHEN 1 THEN 'S' WHEN 2 THEN 'M' ELSE 'XL' END as sizeLabel]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "case_with_stateful_rewrite",
            sql: "SELECT CASE WHEN lag(a) > 0 THEN 'pos' ELSE 'neg' END AS label FROM stream",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[CASE WHEN col_1 > 0 THEN 'pos' ELSE 'neg' END as label]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[CASE WHEN col_1 > 0 THEN 'pos' ELSE 'neg' END as label]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "order_by_single_key",
            sql: "SELECT a FROM stream ORDER BY a",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Order_1","info":["keys=[a ASC]"],"operator":"Order"}],"id":"Project_2","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalOrder_2","info":["keys=[a ASC]"],"operator":"PhysicalOrder"}],"id":"PhysicalProject_3","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "order_by_projection_alias_non_aggregate",
            sql: "SELECT a + 1 AS x FROM stream ORDER BY x DESC",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Order_1","info":["keys=[(a + 1) DESC]"],"operator":"Order"}],"id":"Project_2","info":["fields=[a + 1 as x]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalOrder_2","info":["keys=[(a + 1) DESC]"],"operator":"PhysicalOrder"}],"id":"PhysicalProject_3","info":["fields=[a + 1 as x]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "order_by_implicit_aggregate_key",
            sql: "SELECT sum(a) FROM stream_ab ORDER BY sum(b)",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Aggregation_1","info":["aggregates=[sum(a) -> col_1; sum(b) -> col_2]"],"operator":"Aggregation"}],"id":"Order_2","info":["keys=[col_2 ASC]"],"operator":"Order"}],"id":"Project_3","info":["fields=[col_1 as sum(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalAggregation_2","info":["calls=[sum(a) -> col_1; sum(b) -> col_2]"],"operator":"PhysicalAggregation"}],"id":"PhysicalOrder_3","info":["keys=[col_2 ASC]"],"operator":"PhysicalOrder"}],"id":"PhysicalProject_4","info":["fields=[col_1 as sum(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "order_by_group_key_alias_with_countwindow",
            sql: "SELECT b AS k, sum(a) AS s FROM stream_ab GROUP BY countwindow(4), b ORDER BY k",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=count","count=4"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Order_3","info":["keys=[(b) ASC]"],"operator":"Order"}],"id":"Project_4","info":["fields=[b as k; col_1 as s]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStreamingAggregation_3","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=count","count=4"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalOrder_4","info":["keys=[(b) ASC]"],"operator":"PhysicalOrder"}],"id":"PhysicalProject_5","info":["fields=[b as k; col_1 as s]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "order_by_aggregate_alias_with_countwindow",
            sql: "SELECT b, sum(a) AS s FROM stream_ab GROUP BY countwindow(4), b ORDER BY s DESC",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=count","count=4"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Order_3","info":["keys=[(col_1) DESC]"],"operator":"Order"}],"id":"Project_4","info":["fields=[b; col_1 as s]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStreamingAggregation_3","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=count","count=4"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalOrder_4","info":["keys=[(col_1) DESC]"],"operator":"PhysicalOrder"}],"id":"PhysicalProject_5","info":["fields=[b; col_1 as s]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "select_with_sampler_inserts_sampler_before_decoder",
            sql: "SELECT a FROM stream_sampler",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_sampler","decoder=json","schema=[a]","sampler.strategy=latest"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_sampler","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalSampler_1","info":["interval=100ms","strategy=latest"],"operator":"PhysicalSampler"}],"id":"PhysicalDecoder_2","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_3","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "planner_explain_sampler_appears_before_decoder_for_shared_ingest_path",
            sql: "SELECT a FROM shared_stream_sampler",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=shared_stream_sampler","decoder=json","schema=[a]","sampler.strategy=latest"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"shared/shared_stream_sampler/PhysicalDataSource_0","info":["scope=shared_stream","source=shared_stream_sampler","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"shared/shared_stream_sampler/PhysicalSampler_1","info":["scope=shared_stream","interval=100ms","strategy=latest"],"operator":"PhysicalSampler"}],"id":"shared/shared_stream_sampler/PhysicalDecoder_2","info":["scope=shared_stream","decoder=json"],"operator":"PhysicalDecoder"}],"id":"shared/shared_stream_sampler/PhysicalResultCollect_3","info":["scope=shared_stream"],"operator":"PhysicalResultCollect"}],"id":"PhysicalSharedStream_0","info":["source=shared_stream_sampler","schema=[a]"],"operator":"PhysicalSharedStream"}],"id":"PhysicalProject_1","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "memory_collection_source_inserts_layout_normalize",
            sql: "SELECT a FROM memory_collect",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=memory_collect","decoder=none","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=memory_collect","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalCollectionLayoutNormalize_1","info":["source=memory_collect","schema=[a, b]"],"operator":"PhysicalCollectionLayoutNormalize"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "memory_bytes_source_keeps_decoder_path",
            sql: "SELECT a FROM memory_bytes",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=memory_bytes","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=memory_bytes","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
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
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2 as sum(a); col_1 as lag(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProcessTimeWatermark_3","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_5","info":["calls=[sum(a) -> col_2]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_6","info":["fields=[col_2 as sum(a); col_1 as lag(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "having_after_streaming_aggregation",
            sql: "SELECT sum(a) FROM stream GROUP BY tumblingwindow('ss', 10) HAVING sum(a) > 0",
            options: PipelineOptions::default(),
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]"],"operator":"Aggregation"}],"id":"Filter_3","info":["predicate=col_1 > 0"],"operator":"Filter"}],"id":"Project_4","info":["fields=[col_1 as sum(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalFilter_5","info":["predicate=col_1 > 0"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_6","info":["fields=[col_1 as sum(a)]"],"operator":"PhysicalProject"}}"##,
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
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=count","count=4"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[ndv(a) -> col_1]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1 as ndv(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalCountWindow_2","info":["kind=count","count=4"],"operator":"PhysicalCountWindow"}],"id":"PhysicalAggregation_3","info":["calls=[ndv(a) -> col_1]"],"operator":"PhysicalAggregation"}],"id":"PhysicalProject_4","info":["fields=[col_1 as ndv(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "explain_pipeline_with_eventtime_enabled_prints_plans",
            sql: "SELECT sum(a), lag(a) FROM stream_eventtime GROUP BY tumblingwindow('ss', 10)",
            options: PipelineOptions {
                eventtime: EventtimeOptions {
                    enabled: true,
                    late_tolerance: Duration::from_secs(5),
                },
                ..PipelineOptions::default()
            },
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_eventtime","decoder=json","schema=[a, event_ts]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2 as sum(a); col_1 as lag(a)]"],"operator":"Project"},"options":{"eventtime_enabled":true,"eventtime_late_tolerance_ms":5000},"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_eventtime","schema=[a, event_ts]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, event_ts]","eventtime.column=event_ts","eventtime.type=unixtimestamp_ms","eventtime.index=1"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalEventtimeWatermark_3","info":["window=tumbling","unit=Seconds","length=10","mode=event_time","late_tolerance_ms=5000"],"operator":"PhysicalEventtimeWatermark"}],"id":"PhysicalStreamingAggregation_5","info":["calls=[sum(a) -> col_2]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_6","info":["fields=[col_2 as sum(a); col_1 as lag(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "explain_eventtime_simple_projection_keeps_decoder_eventtime_column",
            sql: "SELECT a FROM stream_eventtime",
            options: PipelineOptions {
                eventtime: EventtimeOptions {
                    enabled: true,
                    late_tolerance: Duration::ZERO,
                },
                ..PipelineOptions::default()
            },
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_eventtime","decoder=json","schema=[a, event_ts]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"},"options":{"eventtime_enabled":true,"eventtime_late_tolerance_ms":0},"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_eventtime","schema=[a, event_ts]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, event_ts]","eventtime.column=event_ts","eventtime.type=unixtimestamp_ms","eventtime.index=1"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
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

// coverage-covers: planner.physical.sampler_placement, planner.physical.shared_tail_barrier_insertion
#[test]
fn plan_explain_additional_physical_pairings_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "shared_stream_sampler_placement_keeps_sampler_before_decoder",
            sql: "SELECT a FROM shared_stream_sampler",
            sinks: vec![build_nop_json_sink("test_sink", None)],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=shared_stream_sampler","decoder=json","schema=[a]","sampler.strategy=latest"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"shared/shared_stream_sampler/PhysicalDataSource_0","info":["scope=shared_stream","source=shared_stream_sampler","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"shared/shared_stream_sampler/PhysicalSampler_1","info":["scope=shared_stream","interval=100ms","strategy=latest"],"operator":"PhysicalSampler"}],"id":"shared/shared_stream_sampler/PhysicalDecoder_2","info":["scope=shared_stream","decoder=json"],"operator":"PhysicalDecoder"}],"id":"shared/shared_stream_sampler/PhysicalResultCollect_3","info":["scope=shared_stream"],"operator":"PhysicalResultCollect"}],"id":"PhysicalSharedStream_0","info":["source=shared_stream_sampler","schema=[a]"],"operator":"PhysicalSharedStream"}],"id":"PhysicalProject_1","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_3","info":["sink_id=test_sink","encoder=json","by_index_projection=[shared_stream_sampler#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_2","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_4","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "two_sinks_inserts_physical_barrier_for_shared_tail",
            sql: "SELECT a FROM stream",
            sinks: vec![
                PipelineSink::new(
                    "sink_a",
                    PipelineSinkConnector::new(
                        "connector_a",
                        SinkConnectorConfig::Nop(NopSinkConfig::default()),
                        SinkEncoderConfig::json(),
                    ),
                )
                .with_forward_to_result(true),
                PipelineSink::new(
                    "sink_b",
                    PipelineSinkConnector::new(
                        "connector_b",
                        SinkConnectorConfig::Nop(NopSinkConfig::default()),
                        SinkEncoderConfig::json(),
                    ),
                )
                .with_forward_to_result(true),
            ],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_6","info":["sink_id=sink_b","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalBarrier_8","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalResultCollect_7","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        let got = explain_json(case.sql, case.sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_stateful_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "stateful_select_only",
            sql: "SELECT lag(a) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as lag(a)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_nested_filter_dependency",
            sql: "SELECT lag(a), lag(b) FILTER (WHERE lag(a)) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1; lag(b) FILTER (WHERE col_1) -> col_2]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a); col_2 as lag(b) FILTER (WHERE lag(a))]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1; lag(b) FILTER (WHERE col_1) -> col_2]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as lag(a); col_2 as lag(b) FILTER (WHERE lag(a))]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_filter_over_partition",
            sql:
                "SELECT acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_begin_reset_uses_stateful_plan_node",
            sql: "SELECT acc_sum(a, flag = 1, flag = 0) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a, flag = 1, flag = 0) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a, flag = 1, flag = 0)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_sum(a, flag = 1, flag = 0) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as acc_sum(a, flag = 1, flag = 0)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_begin_reset_filter_over_partition",
            sql:
                "SELECT acc_count(a, flag = 1, flag = 0) FILTER (WHERE k2 = 1) OVER (PARTITION BY k1) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1, k2]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_count(a, flag = 1, flag = 0) FILTER (WHERE k2 = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_count(a, flag = 1, flag = 0) FILTER (WHERE k2 = 1) OVER (PARTITION BY k1)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_count(a, flag = 1, flag = 0) FILTER (WHERE k2 = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as acc_count(a, flag = 1, flag = 0) FILTER (WHERE k2 = 1) OVER (PARTITION BY k1)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "multiple_acc_filter_over_partition_calls",
            sql:
                "SELECT acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1), acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1; acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_2]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1); col_2 as acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1; acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_2]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1); col_2 as acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_clause_in_where",
            sql:
                "SELECT a FROM stream WHERE acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) > 1",
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 1"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_count(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalFilter_3","info":["predicate=col_1 > 1"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_4","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "acc_clause_and_window_aggregate",
            sql:
                "SELECT sum(a), acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) FROM stream GROUP BY countwindow(4)",
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=count","count=4"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2 as sum(a); col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_2]","window=count","count=4"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_2 as sum(a); col_1 as acc_sum(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_filter_over_partition",
            sql: "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1, k2]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2)]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2)]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_filter_over_partition_alias",
            sql:
                "SELECT lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) as v1 FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, flag, k1, k2]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as v1]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) FILTER (WHERE flag = 1) OVER (PARTITION BY k1, k2) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as v1]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_dedup_and_dependent_filter",
            sql: "SELECT lag(a), lag(a) FILTER (WHERE lag(a)) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1; lag(a) FILTER (WHERE col_1) -> col_2]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1 as lag(a); col_2 as lag(a) FILTER (WHERE lag(a))]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1; lag(a) FILTER (WHERE col_1) -> col_2]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1 as lag(a); col_2 as lag(a) FILTER (WHERE lag(a))]"],"operator":"PhysicalProject"}}"##,
        },
    ];

    for case in cases {
        let got = explain_json_string(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_optimizer_table_driven() {
    #[derive(Clone, Copy)]
    struct SinkSpec {
        sink_id: &'static str,
        batch_count: Option<usize>,
        transform_template: bool,
    }

    const SINK_NO_BATCH: &[SinkSpec] = &[SinkSpec {
        sink_id: "test_sink",
        batch_count: None,
        transform_template: false,
    }];
    const SINK_BATCH_10: &[SinkSpec] = &[SinkSpec {
        sink_id: "test_sink",
        batch_count: Some(10),
        transform_template: false,
    }];

    struct PhysicalOptimizerCase {
        name: &'static str,
        sql: &'static str,
        sinks: &'static [SinkSpec],
        covers: &'static [&'static str],
        expected: &'static str,
    }

    let cases = vec![
        PhysicalOptimizerCase {
            name: "encoded_sink_with_batch_lowers_to_sink_encoder",
            sql: "SELECT a FROM stream_enc",
            sinks: SINK_BATCH_10,
            covers: &[
                "sink.output.batching",
                "planner.physical.by_index_projection_into_encoder_rewrite",
            ],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_enc","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","batching=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_enc","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalIncSinkEncoder_5","info":["sink_id=test_sink","encoder=json","batch_count=10","by_index_projection=[stream_enc#0->a]"],"operator":"PhysicalIncSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        PhysicalOptimizerCase {
            name: "optimize_rewrites_streaming_agg",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY tumblingwindow('ss', 10),b",
            sinks: SINK_NO_BATCH,
            covers: &["planner.physical.streaming_aggregation_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1 as sum(a)]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_1 as sum(a)]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_6","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        PhysicalOptimizerCase {
            name: "optimize_rewrites_streaming_agg_for_sliding_window",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY slidingwindow('ss', 10),b",
            sinks: SINK_NO_BATCH,
            covers: &["planner.physical.streaming_aggregation_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1 as sum(a)]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=sliding","unit=Seconds","lookback=10","lookahead=none","mode=processing_time","interval=1"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_1 as sum(a)]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_6","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        PhysicalOptimizerCase {
            name: "optimize_rewrites_streaming_agg_for_state_window",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY statewindow(a > 0, a = 4), b",
            sinks: SINK_NO_BATCH,
            covers: &["planner.physical.streaming_aggregation_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=state","open=a > 0","emit=a = 4"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1 as sum(a)]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStreamingAggregation_3","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=state","open=a > 0","emit=a = 4"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_4","info":["fields=[col_1 as sum(a)]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_6","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_7","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        PhysicalOptimizerCase {
            name: "physical_plan_sliding_without_lookahead_includes_watermark_for_gc",
            sql: "SELECT sum(a) FROM stream_ab GROUP BY slidingwindow('ss', 10),b",
            sinks: SINK_NO_BATCH,
            covers: &["planner.physical.streaming_aggregation_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Window_1","info":["kind=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"Window"}],"id":"Aggregation_2","info":["aggregates=[sum(a) -> col_1]","group_by=[b]"],"operator":"Aggregation"}],"id":"Project_3","info":["fields=[col_1 as sum(a)]"],"operator":"Project"}],"id":"DataSink_4","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_5","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProcessTimeWatermark_2","info":["window=sliding","unit=Seconds","lookback=10","lookahead=none","mode=processing_time","interval=1"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_4","info":["calls=[sum(a) -> col_1]","group_by=[b]","window=sliding","unit=Seconds","lookback=10","lookahead=none"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_5","info":["fields=[col_1 as sum(a)]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_6","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        assert!(!case.covers.is_empty(), "case={} missing covers", case.name);
        let sinks = case
            .sinks
            .iter()
            .map(|s| {
                if s.transform_template {
                    build_nop_json_transform_sink(s.sink_id, s.batch_count)
                } else {
                    build_nop_json_sink(s.sink_id, s.batch_count)
                }
            })
            .collect::<Vec<_>>();

        let got = explain_json(case.sql, sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_encoder_transform_table_driven() {
    #[derive(Clone, Copy)]
    struct SinkSpec {
        sink_id: &'static str,
        batch_count: Option<usize>,
    }

    const SINK_NO_BATCH_TRANSFORM: &[SinkSpec] = &[SinkSpec {
        sink_id: "test_sink",
        batch_count: None,
    }];
    const SINK_BATCH_10_TRANSFORM: &[SinkSpec] = &[SinkSpec {
        sink_id: "test_sink",
        batch_count: Some(10),
    }];

    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: &'static [SinkSpec],
        covers: &'static [&'static str],
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "transform_template_without_batch_keeps_regular_encoder",
            sql: "SELECT a + 1 AS c, b + 1 AS d FROM stream_ab",
            sinks: SINK_NO_BATCH_TRANSFORM,
            covers: &["sink.encoder.transform"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a + 1 as c; b + 1 as d]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","transform=template"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as c; b + 1 as d]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json","transform=template"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        Case {
            name: "transform_template_with_batch_uses_sink_encoder",
            sql: "SELECT a + 1 AS c, b + 1 AS d FROM stream_ab",
            sinks: SINK_BATCH_10_TRANSFORM,
            covers: &["sink.encoder.transform"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a + 1 as c; b + 1 as d]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","transform=template","batching=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as c; b + 1 as d]"],"operator":"PhysicalProject"}],"id":"PhysicalIncSinkEncoder_5","info":["sink_id=test_sink","encoder=json","transform=template","batch_count=10"],"operator":"PhysicalIncSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        let _ = case.covers;
        let sinks = case
            .sinks
            .iter()
            .map(|s| build_nop_json_transform_sink(s.sink_id, s.batch_count))
            .collect::<Vec<_>>();

        let got = explain_json(case.sql, sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_by_index_projection_rewrite_table_driven() {
    struct ByIndexRewriteCase {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        covers: &'static [&'static str],
        expected_before: &'static str,
        expected_after: &'static str,
    }

    let cases = vec![
        ByIndexRewriteCase {
            name: "rewrites_pure_by_index_project_into_encoder",
            sql: "SELECT a FROM stream",
            sinks: vec![build_nop_json_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_encoder_rewrite"],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "rewrites_mixed_projection_delaying_only_by_index_fields",
            sql: "SELECT a, a + 1 FROM stream",
            sinks: vec![build_nop_json_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_encoder_rewrite"],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; a + 1]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "does_not_rewrite_when_contains_wildcard",
            sql: "SELECT a, * FROM stream",
            sinks: vec![build_nop_json_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_encoder_rewrite"],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; *]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; *]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; *]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; *]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "rewrites_by_index_with_alias",
            sql: "SELECT a AS x FROM stream",
            sinks: vec![build_nop_json_sink("test_sink", None)],
            covers: &[
                "parser.select.alias_computing",
                "planner.physical.by_index_projection_into_encoder_rewrite",
            ],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a as x]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json","by_index_projection=[stream#0->x]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "does_not_rewrite_by_index_projection_when_transform_enabled",
            sql: "SELECT a AS c, b AS d FROM stream_ab",
            sinks: vec![build_nop_json_transform_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_encoder_rewrite"],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as c; b as d]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","transform=template"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a as c; b as d]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json","transform=template"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as c; b as d]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","transform=template"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a as c; b as d]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json","transform=template"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "does_not_rewrite_shared_project_when_any_sink_ineligible",
            sql: "SELECT a FROM stream",
            sinks: vec![
                build_nop_json_sink("sink_a", None).with_forward_to_result(true),
                build_nop_none_sink("sink_b").with_forward_to_result(true),
            ],
            covers: &["planner.physical.by_index_projection_into_encoder_rewrite"],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=none"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}],"id":"PhysicalDataSink_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=none"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a]"],"operator":"PhysicalProject"}],"id":"PhysicalDataSink_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalDataSink"}],"id":"PhysicalBarrier_7","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "rewrites_shared_project_across_encoder_and_row_diff_consumers",
            sql: "SELECT a, a + 1 AS x FROM stream",
            sinks: vec![
                build_nop_json_sink("sink_a", None).with_forward_to_result(true),
                build_nop_json_delta_sink_with_columns("sink_b", None, Some(&["x"]))
                    .with_forward_to_result(true),
            ],
            covers: &[
                "planner.physical.by_index_projection_across_mixed_consumers_rewrite",
                "planner.physical.by_index_projection_into_encoder_rewrite",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
            ],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=json","output.mode=delta","output.columns=[x]"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; a + 1 as x]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; a + 1 as x]"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_6","info":["sink_id=sink_b","mode=delta","columns=[x]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=sink_b","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=json","output.mode=delta","output.columns=[x]"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as x]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as x]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_6","info":["sink_id=sink_b","mode=delta","columns=[x]","by_index_projection=[stream.a]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=sink_b","encoder=json","by_index_projection=[stream#0->a]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalBarrier_9","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "rewrites_shared_project_across_encoder_and_row_diff_consumers_with_alias",
            sql: "SELECT a AS x, b + 1 AS y FROM stream_ab",
            sinks: vec![
                build_nop_json_sink("sink_a", None).with_forward_to_result(true),
                build_nop_json_delta_sink_with_columns("sink_b", None, Some(&["y"]))
                    .with_forward_to_result(true),
            ],
            covers: &[
                "planner.physical.by_index_projection_across_mixed_consumers_rewrite",
                "planner.physical.by_index_projection_into_encoder_rewrite",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
            ],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; b + 1 as y]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; b + 1 as y]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=json","output.mode=delta","output.columns=[y]"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a as x; b + 1 as y]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a as x; b + 1 as y]"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_6","info":["sink_id=sink_b","mode=delta","columns=[y]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=sink_b","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; b + 1 as y]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=sink_a","connector=nop","encoder=json"],"operator":"DataSink"},{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; b + 1 as y]"],"operator":"Project"}],"id":"DataSink_3","info":["sink_id=sink_b","connector=nop","encoder=json","output.mode=delta","output.columns=[y]"],"operator":"DataSink"}],"id":"Tail_4","info":["sink_count=2"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[b + 1 as y]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=sink_a","encoder=json","by_index_projection=[stream_ab#0->x]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=sink_a","connector=nop"],"operator":"PhysicalSinkConnector"},{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[b + 1 as y]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_6","info":["sink_id=sink_b","mode=delta","columns=[y]","by_index_projection=[stream_ab.a as x]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_7","info":["sink_id=sink_b","encoder=json","by_index_projection=[stream_ab#0->x]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_5","info":["sink_id=sink_b","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalBarrier_9","info":["upstream_count=2"],"operator":"PhysicalBarrier"}],"id":"PhysicalResultCollect_8","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        ByIndexRewriteCase {
            name: "does_not_rewrite_non_by_index_expression",
            sql: "SELECT a + 1 FROM stream",
            sinks: vec![build_nop_json_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_encoder_rewrite"],
            expected_before: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a + 1]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            expected_after: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a + 1]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1]"],"operator":"PhysicalProject"}],"id":"PhysicalSinkEncoder_4","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in cases {
        assert!(!case.covers.is_empty(), "case={} missing covers", case.name);
        let (before, after) = explain_json_before_after_physical_opt(case.sql, case.sinks);
        assert_eq!(before, case.expected_before, "case={} (before)", case.name);
        assert_eq!(after, case.expected_after, "case={} (after)", case.name);
    }
}

#[test]
fn plan_explain_row_diff_table_driven() {
    struct RowDiffExplainCase {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        covers: &'static [&'static str],
        expected: &'static str,
    }

    let explain_cases = vec![
        RowDiffExplainCase {
            name: "row_diff_single_json_sink_inserts_physical_row_diff",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_nop_json_delta_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[a, b]","by_index_projection=[stream_ab.a; stream_ab.b]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_with_batch_keeps_row_diff_and_uses_sink_encoder",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_nop_json_delta_sink_with_columns(
                "test_sink",
                Some(2),
                Some(&["b"]),
            )],
            covers: &[
                "sink.output.batching",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
                "planner.physical.partial_by_index_row_diff_and_encoder_rewrite",
            ],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta","output.columns=[b]","batching=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[b]","by_index_projection=[stream_ab.b]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalIncSinkEncoder_6","info":["sink_id=test_sink","encoder=json","batch_count=2","by_index_projection=[stream_ab#0->a]"],"operator":"PhysicalIncSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_7","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_partial_late_materialization_splits_row_diff_and_encoder_owned_columns",
            sql: "SELECT a, b, flag AS c FROM stream",
            sinks: vec![build_nop_json_delta_sink_with_columns(
                "test_sink",
                None,
                Some(&["b"]),
            )],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b; flag as c]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta","output.columns=[b]"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[b]","by_index_projection=[stream.b]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json","by_index_projection=[stream#0->a; stream#2->c]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_partial_late_materialization_splits_row_diff_and_encoder_owned_columns_with_aliases",
            sql: "SELECT a AS x, b AS y, flag AS z FROM stream",
            sinks: vec![build_nop_json_delta_sink_with_columns(
                "test_sink",
                None,
                Some(&["y"]),
            )],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; b as y; flag as z]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta","output.columns=[y]"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[y]","by_index_projection=[stream.b as y]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json","by_index_projection=[stream#0->x; stream#2->z]"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_alias_project_rewrites_into_row_diff",
            sql: "SELECT a AS x FROM stream",
            sinks: vec![build_nop_json_delta_sink("test_sink", None)],
            covers: &[
                "parser.select.alias_computing",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
            ],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[x]","by_index_projection=[stream.a as x]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_memory_collection_sink_places_row_diff_before_materialize",
            sql: "SELECT a, a + 1 AS x FROM stream",
            sinks: vec![build_memory_collection_delta_sink_with_columns(
                "test_sink",
                "demo_collection",
                Some(&["x"]),
            )],
            covers: &[
                "sink.memory_collection.materialize",
                "sink.output.row_diff",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
            ],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=memory","encoder=none","output.mode=delta","output.columns=[x]"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as x]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[x]","by_index_projection=[stream.a]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalMemoryCollectionMaterialize_5","info":[],"operator":"PhysicalMemoryCollectionMaterialize"}],"id":"PhysicalDataSink_3","info":["sink_id=test_sink","connector=memory","topic=demo_collection","kind=collection"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_mixed_projection_rewrites_partial_by_index_projection",
            sql: "SELECT a, a + 1 AS x FROM stream",
            sinks: vec![build_nop_json_delta_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as x]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[a, x]","by_index_projection=[stream.a]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_mixed_projection_with_alias_rewrites_partial_by_index_projection",
            sql: "SELECT a AS x, b + 1 AS y FROM stream_ab",
            sinks: vec![build_nop_json_delta_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as x; b + 1 as y]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[b + 1 as y]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[x, y]","by_index_projection=[stream_ab.a as x]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_wildcard_projection_keeps_materializing_project",
            sql: "SELECT * FROM stream",
            sinks: vec![build_nop_json_delta_sink("test_sink", None)],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[*]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b, flag, k1, k2]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[*]"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[a, b, flag, k1, k2]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_and_template_are_orthogonal_in_plan_shape",
            sql: "SELECT a AS c, b AS d FROM stream_ab",
            sinks: vec![build_nop_json_transform_delta_sink_with_columns(
                "test_sink",
                None,
                Some(&["c"]),
            )],
            covers: &["planner.physical.by_index_projection_into_row_diff_rewrite"],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a as c; b as d]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta","output.columns=[c]","transform=template"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[c]","by_index_projection=[stream_ab.a as c; stream_ab.b as d]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalSinkEncoder_5","info":["sink_id=test_sink","encoder=json","transform=template"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
        RowDiffExplainCase {
            name: "row_diff_omit_if_empty_inserts_empty_suppress_after_row_diff",
            sql: "SELECT a, a + 1 AS x FROM stream",
            sinks: vec![build_nop_json_delta_omit_if_empty_sink_with_columns(
                "test_sink",
                None,
                Some(&["x"]),
            )],
            covers: &[
                "sink.output.row_diff",
                "sink.output.omit_if_empty",
                "planner.physical.by_index_projection_into_row_diff_rewrite",
            ],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; a + 1 as x]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=test_sink","connector=nop","encoder=json","output.mode=delta","output.columns=[x]","output.omit_if_empty=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a + 1 as x]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalRowDiff_4","info":["sink_id=test_sink","mode=delta","columns=[x]","by_index_projection=[stream.a]"],"operator":"PhysicalRowDiff"}],"id":"PhysicalEmptySuppress_5","info":["sink_id=test_sink","omit_if_empty=true"],"operator":"PhysicalEmptySuppress"}],"id":"PhysicalSinkEncoder_6","info":["sink_id=test_sink","encoder=json"],"operator":"PhysicalSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=test_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_7","info":[],"operator":"PhysicalResultCollect"}}"##,
        },
    ];

    for case in explain_cases {
        assert!(!case.covers.is_empty(), "case={} missing covers", case.name);
        let got = explain_json(case.sql, case.sinks);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn plan_explain_source_on_change_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        source_inputs: HashMap<String, SourceInputConfig>,
        covers: &'static [&'static str],
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "source_on_change_with_explicit_columns",
            sql: "SELECT speed, rpm FROM vehicle_stream",
            source_inputs: HashMap::from([(
                "vehicle_stream".to_string(),
                SourceInputConfig::on_change_with_columns(["speed", "rpm"]),
            )]),
            covers: &["source.on_change.gating"],
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=vehicle_stream","decoder=json","schema=[speed, rpm]","input.mode=on_change","input.columns=[speed, rpm]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[speed; rpm]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=vehicle_stream","schema=[speed, rpm]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[speed, rpm]"],"operator":"PhysicalDecoder"}],"id":"PhysicalSourceChangeGate_2","info":["source=vehicle_stream","mode=on_change","columns=[speed, rpm]"],"operator":"PhysicalSourceChangeGate"}],"id":"PhysicalProject_3","info":["fields=[speed; rpm]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "source_on_change_without_columns_tracks_all_top_level_columns",
            sql: "SELECT speed FROM vehicle_stream",
            source_inputs: HashMap::from([(
                "vehicle_stream".to_string(),
                SourceInputConfig::on_change(),
            )]),
            covers: &["source.on_change.gating"],
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=vehicle_stream","decoder=json","schema=[speed, rpm, gear]","input.mode=on_change","input.columns=[ALL]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[speed]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=vehicle_stream","schema=[speed, rpm, gear]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[speed, rpm, gear]"],"operator":"PhysicalDecoder"}],"id":"PhysicalSourceChangeGate_2","info":["source=vehicle_stream","mode=on_change","columns=[speed, rpm, gear]"],"operator":"PhysicalSourceChangeGate"}],"id":"PhysicalProject_3","info":["fields=[speed]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "source_on_change_keeps_hidden_tracked_columns_alive",
            sql: "SELECT speed FROM vehicle_stream",
            source_inputs: HashMap::from([(
                "vehicle_stream".to_string(),
                SourceInputConfig::on_change_with_columns(["rpm"]),
            )]),
            covers: &[
                "source.on_change.gating",
                "planner.logical.top_level_column_pruning",
            ],
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=vehicle_stream","decoder=json","schema=[speed, rpm]","input.mode=on_change","input.columns=[rpm]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[speed]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=vehicle_stream","schema=[speed, rpm]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[speed, rpm]"],"operator":"PhysicalDecoder"}],"id":"PhysicalSourceChangeGate_2","info":["source=vehicle_stream","mode=on_change","columns=[rpm]"],"operator":"PhysicalSourceChangeGate"}],"id":"PhysicalProject_3","info":["fields=[speed]"],"operator":"PhysicalProject"}}"##,
        },
    ];

    for case in cases {
        let _ = case.covers;
        let got = explain_json_with_source_inputs(case.sql, vec![], case.source_inputs);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}

#[test]
fn row_diff_kuksa_sink_is_rejected() {
    let registries = PipelineRegistries::new_with_builtin();
    let stream_defs = setup_streams();
    let sql = "SELECT a, b FROM stream";
    let sinks = vec![build_kuksa_delta_sink("test_sink")];

    let select_stmt = parse_sql(sql).expect("parse sql");
    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs).expect("logical");
    let (logical_plan, bindings) = flow::optimize_logical_plan(logical_plan, &bindings);

    let err = flow::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
        .expect_err("kuksa delta sink should be rejected");
    assert!(
        err.contains("does not preserve output_mask"),
        "unexpected error: {err}"
    );
}

#[test]
fn explain_pipeline_with_eventtime_enabled_rejects_missing_eventtime_column() {
    let catalog = setup_catalog_with_eventtime_stream_config("missing_ts", "unixtimestamp_ms");
    let registries = PipelineRegistries::new_with_builtin();
    let options = PipelineOptions {
        eventtime: EventtimeOptions {
            enabled: true,
            late_tolerance: Duration::ZERO,
        },
        ..PipelineOptions::default()
    };

    let err = flow::explain_pipeline_with_options(
        "SELECT a FROM stream_eventtime",
        vec![],
        &catalog,
        None,
        &registries,
        &options,
    )
    .expect_err("missing eventtime column should be rejected");

    assert!(
        err.to_string().contains(
            "eventtime.column `missing_ts` not found in stream `stream_eventtime` schema"
        ),
        "unexpected error: {err}"
    );
}

#[test]
fn explain_pipeline_with_eventtime_enabled_rejects_unknown_eventtime_type() {
    let catalog = setup_catalog_with_eventtime_stream_config("event_ts", "unknown_eventtime");
    let registries = PipelineRegistries::new_with_builtin();
    let options = PipelineOptions {
        eventtime: EventtimeOptions {
            enabled: true,
            late_tolerance: Duration::ZERO,
        },
        ..PipelineOptions::default()
    };

    let err = flow::explain_pipeline_with_options(
        "SELECT a FROM stream_eventtime",
        vec![],
        &catalog,
        None,
        &registries,
        &options,
    )
    .expect_err("unknown eventtime type should be rejected");

    let err_text = err.to_string();
    assert!(
        err_text.contains("eventtime.type `unknown_eventtime` not registered"),
        "unexpected error: {err_text}"
    );
    assert!(
        err_text.contains("unixtimestamp_ms") && err_text.contains("unixtimestamp_s"),
        "available builtin eventtime types should be included: {err_text}"
    );
}

#[test]
fn explain_pipeline_with_eventtime_enabled_rejects_when_no_stream_declares_eventtime() {
    let catalog = setup_catalog_without_eventtime_stream();
    let registries = PipelineRegistries::new_with_builtin();
    let options = PipelineOptions {
        eventtime: EventtimeOptions {
            enabled: true,
            late_tolerance: Duration::ZERO,
        },
        ..PipelineOptions::default()
    };

    let err = flow::explain_pipeline_with_options(
        "SELECT a FROM stream_without_eventtime",
        vec![],
        &catalog,
        None,
        &registries,
        &options,
    )
    .expect_err("eventtime without any declared eventtime stream should be rejected");

    assert!(
        err.to_string()
            .contains("eventtime.enabled=true but no stream declares eventtime"),
        "unexpected error: {err}"
    );
}

// coverage-covers: planner.eventtime.hidden_column_preservation, source.on_change.gating
#[test]
fn explain_pipeline_with_eventtime_enabled_keeps_hidden_eventtime_column_alive() {
    let stream_name = "stream_eventtime";
    let schema = Arc::new(Schema::new(vec![
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
    ]));
    let stream_def = Arc::new(
        StreamDefinition::new(
            stream_name,
            Arc::clone(&schema),
            StreamProps::Mock(MockStreamProps::default()),
            StreamDecoderConfig::json(),
        )
        .with_eventtime(EventtimeDefinition::new("event_ts", "unixtimestamp_ms")),
    );
    let stream_defs = HashMap::from([(stream_name.to_string(), Arc::clone(&stream_def))]);

    let registries = PipelineRegistries::new_with_builtin();
    let options = PhysicalPlanBuildOptions {
        eventtime_enabled: true,
        eventtime_late_tolerance: Duration::ZERO,
        shared_slot_versions: HashMap::new(),
    };
    let optimizer_options = flow::planner::LogicalOptimizerOptions {
        eventtime_enabled: true,
        ..Default::default()
    };

    let select_stmt = parse_sql("SELECT a FROM stream_eventtime").expect("parse sql");
    let bindings = SchemaBinding::new(vec![SchemaBindingEntry {
        source_name: stream_name.to_string(),
        alias: select_stmt.source_infos[0].alias.clone(),
        schema,
        kind: SourceBindingKind::Regular,
    }]);
    let logical_plan = create_logical_plan_with_source_inputs(
        select_stmt,
        vec![],
        &stream_defs,
        &HashMap::from([(
            stream_name.to_string(),
            SourceInputConfig::on_change_with_columns(["a"]),
        )]),
    )
    .expect("logical plan with source inputs");
    let (logical_plan, bindings) = optimize_logical_plan_with_options(
        Arc::clone(&logical_plan),
        &bindings,
        &optimizer_options,
    );

    let pruned_columns = bindings.entries()[0]
        .schema
        .column_schemas()
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        pruned_columns,
        vec!["a".to_string(), "event_ts".to_string()],
        "eventtime-enabled pruning must keep the hidden eventtime column alive"
    );

    let physical_plan = create_physical_plan_with_build_options(
        Arc::clone(&logical_plan),
        &bindings,
        &registries,
        &options,
    )
    .expect("hidden eventtime column should keep physical build valid");
    let optimized_plan = flow::optimize_physical_plan(
        physical_plan,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let explain = PipelineExplain::new(
        logical_plan,
        optimized_plan,
        PipelineExplainConfig::default(),
    );
    let explain_json = explain.to_json().to_string();

    assert!(
        explain_json.contains("\"schema=[a, event_ts]\""),
        "eventtime column should remain in the pruned datasource schema: {explain_json}"
    );
    assert!(
        explain_json.contains("\"eventtime.column=event_ts\""),
        "decoder explain should still expose eventtime metadata: {explain_json}"
    );
    assert!(
        explain_json.contains("\"input.columns=[a]\""),
        "source-input tracking should remain limited to the requested on-change columns: {explain_json}"
    );
}

/// A test-only UDF that doubles its integer argument.
#[derive(Debug, Clone)]
struct DoubleUdf;

impl CustomFunc for DoubleUdf {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.len() != 1 {
            return Err(EvalError::TypeMismatch {
                expected: "1 argument".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        match &args[0] {
            Value::Int64(v) => Ok(Value::Int64(v * 2)),
            other => Err(EvalError::TypeMismatch {
                expected: "int64".to_string(),
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "double"
    }
}

#[test]
fn plan_explain_custom_udf_call() {
    let custom_registry =
        flow::CustomFuncRegistry::with_builtins_and_wasm(vec![Arc::new(DoubleUdf)])
            .expect("register test UDF");
    let registries = PipelineRegistries::new(
        flow::connector::ConnectorRegistry::with_builtin_sinks(
            flow::connector::MemoryPubSubRegistry::new(),
        ),
        flow::EncoderRegistry::with_builtin_encoders(),
        flow::DecoderRegistry::with_builtin_decoders(),
        flow::AggregateFunctionRegistry::with_builtins(),
        flow::StatefulFunctionRegistry::with_builtins(),
        custom_registry,
        flow::EventtimeTypeRegistry::with_builtin_types(),
        Arc::new(flow::MergerRegistry::new()),
    );
    let stream_defs = setup_streams();

    let sql = "SELECT double(a) FROM stream";
    let select_stmt = parse_sql(sql).expect("parse sql");

    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");

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
    let explain_json = explain.to_json().to_string();
    assert!(
        explain_json.contains("double"),
        "EXPLAIN should reference the UDF name 'double': {explain_json}"
    );
    assert!(
        explain_json.contains("double(a)"),
        "EXPLAIN should show the UDF call expression: {explain_json}"
    );
}

/// Verify that EXPLAIN output shows `decoder=protobuf` for a stream using protobuf decoder.
#[test]
fn plan_explain_shows_protobuf_decoder() {
    use flow::codec::{ProtoDescriptorBundle, ProtoFieldInfo};
    use std::collections::BTreeMap;

    let registries = PipelineRegistries::new(
        flow::connector::ConnectorRegistry::with_builtin_sinks(
            flow::connector::MemoryPubSubRegistry::new(),
        ),
        flow::EncoderRegistry::with_builtin_encoders(),
        flow::DecoderRegistry::with_builtin_decoders(),
        flow::AggregateFunctionRegistry::with_builtins(),
        flow::StatefulFunctionRegistry::with_builtins(),
        flow::CustomFuncRegistry::with_builtins_and_wasm(vec![]).expect("registry"),
        flow::EventtimeTypeRegistry::with_builtin_types(),
        Arc::new(flow::MergerRegistry::new()),
    );

    let stream_name = "proto_stream";
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            stream_name.to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.to_string(),
            "b".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
    ]));

    let mut field_map = BTreeMap::new();
    field_map.insert(
        1u32,
        ProtoFieldInfo {
            column_index: 0,
            datatype: ConcreteDatatype::Int64(Int64Type),
            is_repeated: false,
            is_zigzag: false,
            nested_bundle: None,
        },
    );
    field_map.insert(
        2u32,
        ProtoFieldInfo {
            column_index: 1,
            datatype: ConcreteDatatype::Int64(Int64Type),
            is_repeated: false,
            is_zigzag: false,
            nested_bundle: None,
        },
    );
    let mut column_to_field = BTreeMap::new();
    column_to_field.insert("a".to_string(), 1);
    column_to_field.insert("b".to_string(), 2);
    let bundle = Arc::new(ProtoDescriptorBundle::new(
        field_map,
        column_to_field,
        2,
        vec![Arc::from("a"), Arc::from("b")],
    ));

    let decoder_config =
        StreamDecoderConfig::new("protobuf", JsonMap::new()).with_proto_bundle(bundle);

    let stream_def = Arc::new(StreamDefinition::new(
        stream_name.to_string(),
        Arc::clone(&schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        decoder_config,
    ));

    let mut stream_defs = setup_streams();
    stream_defs.insert(stream_name.to_string(), stream_def);

    let sql = "SELECT a, b FROM proto_stream";
    let select_stmt = parse_sql(sql).expect("parse sql");
    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");
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

    let explain_json = explain.to_json().to_string();
    assert!(
        explain_json.contains("decoder=protobuf"),
        "EXPLAIN should show decoder=protobuf for proto_stream: {explain_json}"
    );
    assert!(
        explain_json.contains("source=proto_stream"),
        "EXPLAIN should show source name: {explain_json}"
    );
}

/// Helper: build a sink with a custom buffering encoder and batching.
fn build_mock_buffering_encoder_sink_with_batch(
    sink_id: &'static str,
    encoder_kind: &'static str,
    batch_count: usize,
) -> PipelineSink {
    let mut props = JsonMap::new();
    props.insert("mock".to_string(), json!(true));
    let connector = PipelineSinkConnector::new(
        "test_connector",
        SinkConnectorConfig::Nop(NopSinkConfig::default()),
        SinkEncoderConfig::new(encoder_kind, props),
    );
    let mut common = CommonSinkProps::default();
    common.batch_count = Some(batch_count);
    PipelineSink::new(sink_id, connector).with_common_props(common)
}

/// Verify explain output for batching scenarios:
/// 1. json encoder + batch → batch embedded in PhysicalIncSinkEncoder
/// 2. kuksa sink (encoder=none) + batch → PhysicalBatch before PhysicalDataSink
/// 3. buffering encoder + batch → batch embedded in PhysicalIncSinkEncoder
/// 4. by-index buffering encoder + batch → by-index projection on PhysicalIncSinkEncoder
#[test]
fn plan_explain_batch_table_driven() {
    use flow::codec::{EncodeError, SinkEncoder, SinkEncoderFactory};
    use flow::planner::physical::{output_schema::OutputSchema, ByIndexProjection};
    use std::sync::Arc;

    let encoder_registry = flow::EncoderRegistry::with_builtin_encoders();
    struct MockBufferingEncoderFactory {
        id: String,
    }

    impl SinkEncoderFactory for MockBufferingEncoderFactory {
        fn id(&self) -> &str {
            &self.id
        }

        fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
            Err(EncodeError::Other(
                "mock encoder not meant for runtime".into(),
            ))
        }

        fn supports_index_lazy_materialization(&self) -> bool {
            self.id == "mock_by_index_buffering_encoder"
        }

        fn with_by_index_projection(
            self: Arc<Self>,
            _spec: Arc<ByIndexProjection>,
        ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
            Ok(self)
        }

        fn with_output_schema(
            self: Arc<Self>,
            _output_schema: Arc<OutputSchema>,
        ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
            Ok(self)
        }
    }

    encoder_registry.register_encoder(
        "mock_buffering_encoder",
        Arc::new(|_config| {
            Ok(Arc::new(MockBufferingEncoderFactory {
                id: "mock_buffering_encoder".into(),
            }) as Arc<dyn SinkEncoderFactory>)
        }),
    );
    encoder_registry.register_encoder_with_caps(
        "mock_by_index_buffering_encoder",
        Arc::new(|_config| {
            Ok(Arc::new(MockBufferingEncoderFactory {
                id: "mock_by_index_buffering_encoder".into(),
            }) as Arc<dyn SinkEncoderFactory>)
        }),
        true,
    );

    let registries = flow::PipelineRegistries::new(
        flow::connector::ConnectorRegistry::with_builtin_sinks(
            flow::connector::MemoryPubSubRegistry::new(),
        ),
        encoder_registry,
        flow::DecoderRegistry::with_builtin_decoders(),
        flow::AggregateFunctionRegistry::with_builtins(),
        flow::StatefulFunctionRegistry::with_builtins(),
        flow::CustomFuncRegistry::with_builtins_and_wasm(vec![]).expect("custom func registry"),
        flow::EventtimeTypeRegistry::with_builtin_types(),
        Arc::new(flow::MergerRegistry::new()),
    );

    fn explain_for_registries(
        sql: &str,
        sinks: Vec<PipelineSink>,
        registries: &flow::PipelineRegistries,
    ) -> String {
        let stream_defs = setup_streams();
        let select_stmt = parse_sql(sql).expect("parse sql");
        let bindings = bindings_for_select(&select_stmt, &stream_defs);
        let logical_plan = create_logical_plan(select_stmt, sinks, &stream_defs).expect("logical");
        let (logical_plan, bindings) = flow::optimize_logical_plan(logical_plan, &bindings);
        let physical_plan =
            flow::create_physical_plan(Arc::clone(&logical_plan), &bindings, registries)
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

    struct Case {
        name: &'static str,
        sql: &'static str,
        sinks: Vec<PipelineSink>,
        expected: &'static str,
        required_fragments: &'static [&'static str],
        covers: &'static [&'static str],
    }

    let cases = vec![
        Case {
            name: "json_encoder_with_batch_embeds_in_sink_encoder",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_nop_json_sink("json_sink", Some(10))],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=json_sink","connector=nop","encoder=json","batching=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[]","passthrough_messages=true"],"operator":"PhysicalProject"}],"id":"PhysicalIncSinkEncoder_5","info":["sink_id=json_sink","encoder=json","batch_count=10","by_index_projection=[stream_ab#0->a; stream_ab#1->b]"],"operator":"PhysicalIncSinkEncoder"}],"id":"PhysicalSinkConnector_3","info":["sink_id=json_sink","connector=nop"],"operator":"PhysicalSinkConnector"}],"id":"PhysicalResultCollect_6","info":[],"operator":"PhysicalResultCollect"}}"##,
            required_fragments: &[],
            covers: &[
                "sink.output.batching",
                "planner.physical.streaming_encoder_rewrite_fuses_batch",
            ],
        },
        Case {
            name: "kuksa_sink_with_batch_uses_physical_batch",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_kuksa_sink_with_batch("kuksa_sink", 10)],
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_ab","decoder=json","schema=[a, b]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[a; b]"],"operator":"Project"}],"id":"DataSink_2","info":["sink_id=kuksa_sink","connector=kuksa","encoder=none","batching=true"],"operator":"DataSink"}],"id":"Tail_3","info":["sink_count=1"],"operator":"Tail"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_ab","schema=[a, b]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[a; b]"],"operator":"PhysicalProject"}],"id":"PhysicalBatch_4","info":["sink_id=kuksa_sink","batch_count=10"],"operator":"PhysicalBatch"}],"id":"PhysicalDataSink_3","info":["sink_id=kuksa_sink","connector=kuksa"],"operator":"PhysicalDataSink"}],"id":"PhysicalResultCollect_5","info":[],"operator":"PhysicalResultCollect"}}"##,
            required_fragments: &[],
            covers: &[
                "sink.output.batching",
                "planner.physical.external_batch_for_none_encoder",
            ],
        },
        Case {
            name: "buffering_encoder_with_batch_embeds_in_sink_encoder",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_mock_buffering_encoder_sink_with_batch(
                "mock_sink",
                "mock_buffering_encoder",
                1000,
            )],
            expected: "",
            required_fragments: &[
                "\"operator\":\"PhysicalIncSinkEncoder\"",
                "encoder=mock_buffering_encoder",
                "batch_count=1000",
            ],
            covers: &[
                "sink.output.batching",
                "planner.physical.streaming_encoder_rewrite_fuses_buffering_encoder_batch",
            ],
        },
        Case {
            name: "by_index_buffering_encoder_rewrites_through_batch",
            sql: "SELECT a, b FROM stream_ab",
            sinks: vec![build_mock_buffering_encoder_sink_with_batch(
                "mock_by_index_sink",
                "mock_by_index_buffering_encoder",
                1000,
            )],
            expected: "",
            required_fragments: &[
                "\"operator\":\"PhysicalIncSinkEncoder\"",
                "fields=[]",
                "passthrough_messages=true",
                "encoder=mock_by_index_buffering_encoder",
                "batch_count=1000",
                "by_index_projection=[stream_ab#0->a; stream_ab#1->b]",
            ],
            covers: &[
                "sink.output.batching",
                "planner.physical.by_index_projection_into_encoder_rewrite",
                "planner.physical.batch_transparent_for_by_index_projection",
            ],
        },
    ];

    for case in &cases {
        assert!(
            !case.covers.is_empty(),
            "case={} missing coverage",
            case.name
        );
        let got = explain_for_registries(case.sql, case.sinks.clone(), &registries);
        if !case.expected.is_empty() {
            assert_eq!(got, case.expected, "case={}", case.name);
        }
        for fragment in case.required_fragments {
            assert!(
                got.contains(fragment),
                "case={} missing expected fragment `{}` in {got}",
                case.name,
                fragment
            );
        }
    }
}
