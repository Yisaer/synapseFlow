use crate::MQTT_QOS;
use crate::resource_id::{ResourceIdKind, validate_resource_id};
use flow::EncoderRegistry;
use flow::pipeline::{
    FileRetentionConfig, FileSinkProps, HttpSinkProps, KuksaSinkProps, KuraSinkProps,
    MemorySinkProps, MqttSinkProps, NngPubSubSinkProps, NopSinkProps, PipelineDefinition,
    PipelineOptions, PipelineStatus, SinkDefinition, SinkProps, SinkRetryConfig, SinkType,
    SourceDefinition, VideoCodec, VideoContainer, VideoRollingConfig, VideoSinkProps,
};
use flow::planner::sink::{SinkEncoderConfig, SinkEncoderKind};
use parser::SelectStmt;
use serde::Deserialize;
use serde_json::Map as JsonMap;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use super::types::{
    CreatePipelineRequest, CreatePipelineSourceRequest, EncoderTransformRequest,
    FileSinkPropsRequest, HttpSinkPropsRequest, KuraSinkPropsRequest, MemorySinkPropsRequest,
    MqttSinkPropsRequest, NngPubSubSinkPropsRequest, NopSinkPropsRequest, SinkOutputConfigRequest,
    VideoSinkPropsRequest,
};

const MAX_SCHEDULE_DATETIME_RANGES: usize = 128;

#[derive(Deserialize)]
struct KuksaSinkPropsRequest {
    pub addr: Option<String>,
    #[serde(rename = "vss_path")]
    pub vss_path: Option<String>,
}

fn build_video_sink_props(
    sink_id: &str,
    req: VideoSinkPropsRequest,
) -> Result<VideoSinkProps, String> {
    let path = req
        .path
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "video sink requires props.path".to_string())?;
    if let Some(prefix) = &req.filename_prefix {
        flow::pipeline::validate_video_filename_prefix(prefix)
            .map_err(|err| format!("invalid video sink filename_prefix: {err}"))?;
    }
    let codec = match req.codec.trim().to_ascii_lowercase().as_str() {
        "h264" => VideoCodec::H264,
        "h265" => VideoCodec::H265,
        other => {
            return Err(format!(
                "unsupported video codec `{other}` (expected h264|h265)"
            ));
        }
    };
    let container = match req.container.trim().to_ascii_lowercase().as_str() {
        "mp4" => VideoContainer::Mp4,
        other => {
            return Err(format!(
                "unsupported video container `{other}` (expected mp4)"
            ));
        }
    };
    let rolling = match req
        .rolling
        .rolling_type
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "duration" => {
            if req.rolling.seconds == 0 {
                return Err("video sink rolling duration requires positive seconds".to_string());
            }
            VideoRollingConfig::Duration {
                seconds: req.rolling.seconds,
            }
        }
        other => {
            return Err(format!(
                "unsupported video rolling type `{other}` for sink `{sink_id}` (expected duration)"
            ));
        }
    };
    let mut props = VideoSinkProps::new(path, rolling)
        .with_codec(codec)
        .with_container(container);
    if let Some(prefix) = req.filename_prefix {
        props = props.with_filename_prefix(prefix);
    }
    Ok(props)
}

fn build_file_sink_props(
    req: FileSinkPropsRequest,
    properties: &flow::PropertyContext,
    pipeline_id: &str,
    sink_id: &str,
) -> Result<FileSinkProps, String> {
    let path = req
        .path
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "file sink requires props.path".to_string())?;
    let filename_prefix = render_connector_string(
        properties,
        &req.filename_prefix.unwrap_or_default(),
        pipeline_id,
        sink_id,
        "props.filename_prefix",
    )?;
    let filename_suffix = render_connector_string(
        properties,
        &req.filename_suffix.unwrap_or_default(),
        pipeline_id,
        sink_id,
        "props.filename_suffix",
    )?;
    validate_file_name_part("filename_prefix", filename_prefix.expose())?;
    validate_file_name_part("filename_suffix", filename_suffix.expose())?;
    if matches!(filename_suffix.expose(), "." | "..") {
        return Err("file sink filename_suffix must not be `.` or `..`".to_string());
    }
    let retention = FileRetentionConfig {
        max_file_count: req.retention.max_file_count.unwrap_or(0),
        max_file_age_days: req.retention.max_file_age_days.unwrap_or(0),
    };
    Ok(FileSinkProps::new(path, filename_prefix)
        .with_filename_suffix(filename_suffix)
        .with_retention(retention))
}

fn validate_file_name_part(field: &str, value: &str) -> Result<(), String> {
    if value.contains('/') || value.contains('\\') {
        return Err(format!(
            "file sink {field} must not contain path separators"
        ));
    }
    Ok(())
}

fn normalized_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn render_connector_string(
    properties: &flow::PropertyContext,
    raw: &str,
    pipeline_id: &str,
    sink_id: &str,
    field: &str,
) -> Result<flow::ConnectorString, String> {
    properties.render(raw).map_err(|err| {
        format!("pipeline `{pipeline_id}` sink `{sink_id}` failed to resolve {field}: {err}")
    })
}

fn render_http_body(
    body: flow::HttpBodyConfig,
    properties: &flow::PropertyContext,
    pipeline_id: &str,
    sink_id: &str,
) -> Result<flow::HttpBodyConfig, String> {
    match body {
        flow::HttpBodyConfig::Raw => Ok(flow::HttpBodyConfig::Raw),
        flow::HttpBodyConfig::Multipart(mut multipart) => {
            for (name, value) in &mut multipart.fields {
                *value = render_connector_string(
                    properties,
                    value.expose(),
                    pipeline_id,
                    sink_id,
                    &format!("props.body.fields.{name}"),
                )?;
            }
            Ok(flow::HttpBodyConfig::Multipart(multipart))
        }
    }
}

pub(crate) fn validate_create_request(req: &CreatePipelineRequest) -> Result<(), String> {
    validate_resource_id(ResourceIdKind::PipelineId, &req.id)?;
    if req.sql.trim().is_empty() {
        return Err("pipeline sql must not be empty".to_string());
    }
    if req.sinks.is_empty() {
        return Err("pipeline must define at least one sink".to_string());
    }
    // Validate user-provided sink ids. Sinks without an explicit id inherit a
    // generated `{pipeline_id}_sink_{index}` value, which is grammar-safe
    // because the pipeline id is already validated above.
    for sink in &req.sinks {
        if let Some(id) = &sink.id {
            validate_resource_id(ResourceIdKind::SinkId, id)?;
        }
    }
    if req.options.data_channel_capacity == 0 {
        return Err("options.data_channel_capacity must be greater than 0".to_string());
    }
    if let Some(schedule) = &req.options.schedule {
        if schedule.cron.trim().is_empty() {
            return Err("options.schedule.cron must not be empty".to_string());
        }
        if schedule.duration_secs == 0 {
            return Err("options.schedule.duration_secs must be greater than 0".to_string());
        }
        crate::pipeline::scheduler::validate_cron_expression(&schedule.cron)
            .map_err(|err| format!("invalid options.schedule.cron: {err}"))?;
        if schedule.datetime_ranges.len() > MAX_SCHEDULE_DATETIME_RANGES {
            return Err(format!(
                "options.schedule.datetime_ranges must contain at most {MAX_SCHEDULE_DATETIME_RANGES} ranges"
            ));
        }
        for (index, range) in schedule.datetime_ranges.iter().enumerate() {
            if range.begin_timestamp_ms < 0 {
                return Err(format!(
                    "options.schedule.datetime_ranges[{index}].begin_timestamp_ms must be non-negative"
                ));
            }
            if range.end_timestamp_ms < 0 {
                return Err(format!(
                    "options.schedule.datetime_ranges[{index}].end_timestamp_ms must be non-negative"
                ));
            }
            if range.begin_timestamp_ms >= range.end_timestamp_ms {
                return Err(format!(
                    "options.schedule.datetime_ranges[{index}].begin_timestamp_ms must be less than end_timestamp_ms"
                ));
            }
        }
    }
    Ok(())
}

fn parse_pipeline_select_stmt(
    req: &CreatePipelineRequest,
    instance: &flow::FlowInstance,
) -> Result<SelectStmt, String> {
    parser::parse_sql_with_registries(
        &req.sql,
        instance.aggregate_registry(),
        instance.stateful_registry(),
    )
    .map_err(|err| format!("parse pipeline {} sql: {err}", req.id))
}

pub(crate) fn referenced_streams_from_pipeline_sql(
    req: &CreatePipelineRequest,
    instance: &flow::FlowInstance,
) -> Result<BTreeSet<String>, String> {
    let select_stmt = parse_pipeline_select_stmt(req, instance)?;
    Ok(select_stmt
        .source_infos
        .iter()
        .map(|source| source.name.clone())
        .collect())
}

fn build_source_definitions(
    req: &CreatePipelineRequest,
    instance: &flow::FlowInstance,
    select_stmt: &SelectStmt,
) -> Result<Vec<SourceDefinition>, String> {
    let mut source_counts = HashMap::<String, usize>::new();
    for source in &select_stmt.source_infos {
        *source_counts.entry(source.name.clone()).or_insert(0) += 1;
    }

    let mut seen_streams = HashSet::new();
    let mut sources = Vec::with_capacity(req.sources.len());
    for source_req in &req.sources {
        let stream = source_req.normalized_stream();
        validate_source_request(source_req, &source_counts, instance)?;
        if !seen_streams.insert(stream.to_string()) {
            return Err(format!(
                "pipeline source `{stream}` is configured more than once"
            ));
        }
        sources.push(source_req.to_source_definition()?);
    }

    Ok(sources)
}

fn validate_source_request(
    source_req: &CreatePipelineSourceRequest,
    source_counts: &HashMap<String, usize>,
    instance: &flow::FlowInstance,
) -> Result<(), String> {
    let stream = source_req.normalized_stream();
    if stream.is_empty() {
        return Err("pipeline source stream must not be empty".to_string());
    }

    let count = source_counts.get(stream).copied().unwrap_or(0);
    if count == 0 {
        return Err(format!(
            "pipeline source `{stream}` is not referenced by the SQL"
        ));
    }

    let source = source_req.to_source_definition()?;
    if !source.input.is_on_change() {
        return Ok(());
    }

    if count > 1 {
        return Err(format!(
            "pipeline source `{stream}` uses input.mode=on_change but the SQL references that stream multiple times"
        ));
    }

    let definition = instance
        .stream_definition(stream)
        .ok_or_else(|| format!("stream `{stream}` not found in catalog"))?;
    if definition.decoder().kind() == "none" {
        return Err(format!(
            "pipeline source `{stream}` with input.mode=on_change requires a decoder"
        ));
    }

    if let Some(columns) = source.input.on_change_columns() {
        if columns.is_empty() {
            return Err(format!(
                "pipeline source `{stream}` input.on_change.columns must not be empty"
            ));
        }
        let mut seen_columns = HashSet::new();
        for column in columns {
            if column.contains('.') {
                return Err(format!(
                    "pipeline source `{stream}` input.on_change column `{column}` must be a top-level column"
                ));
            }
            if !seen_columns.insert(column.as_str()) {
                return Err(format!(
                    "pipeline source `{stream}` input.on_change has duplicate column `{column}`"
                ));
            }
            if definition.schema().column_index(column).is_none() {
                return Err(format!(
                    "pipeline source `{stream}` input.on_change column `{column}` is not present in stream schema"
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn build_pipeline_definition(
    req: &CreatePipelineRequest,
    encoder_registry: &EncoderRegistry,
    instance: &flow::FlowInstance,
) -> Result<PipelineDefinition, String> {
    let select_stmt = parse_pipeline_select_stmt(req, instance)?;
    let sources = build_source_definitions(req, instance, &select_stmt)?;
    let secret_ctx = instance.secret_context();
    let property_ctx = instance.property_context();
    let mut sinks = Vec::with_capacity(req.sinks.len());
    for (index, sink_req) in req.sinks.iter().enumerate() {
        let sink_id = sink_req
            .id
            .clone()
            .unwrap_or_else(|| format!("{}_sink_{index}", req.id));
        let sink_type = sink_req.sink_type.to_ascii_lowercase();
        let sink_definition = match sink_type.as_str() {
            "mqtt" => {
                let mqtt_props: MqttSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid mqtt sink props: {err}"))?;
                let connector_key = normalized_optional_string(mqtt_props.connector_key);
                if connector_key.is_some() && mqtt_props.protocol_version.is_some() {
                    return Err(
                        "mqtt sink protocol_version is owned by connector_key and must not be set locally"
                            .to_string(),
                    );
                }
                if let Some(key) = &connector_key {
                    validate_resource_id(ResourceIdKind::SharedMqttClientKey, key).map_err(
                        |err| format!("mqtt sink references invalid connector_key: {err}"),
                    )?;
                }
                let broker = normalized_optional_string(mqtt_props.broker_url);
                if connector_key.is_none() && broker.is_none() {
                    return Err("mqtt sink requires broker_url".to_string());
                }
                let raw_topic = mqtt_props
                    .topic
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "mqtt sink requires topic".to_string())?;
                let topic = render_connector_string(
                    &property_ctx,
                    &raw_topic,
                    &req.id,
                    &sink_id,
                    "props.topic",
                )?;
                flow::validate_mqtt_publish_topic(&topic).map_err(|err| {
                    format!(
                        "pipeline `{}` sink `{sink_id}` has invalid props.topic: {err}",
                        req.id
                    )
                })?;
                let qos = mqtt_props.qos.unwrap_or(MQTT_QOS);
                let retain = mqtt_props.retain.unwrap_or(false);
                flow::connector::validate_mqtt_user_properties(&mqtt_props.user_properties)
                    .map_err(|err| format!("invalid mqtt sink user_properties: {err}"))?;
                if !mqtt_props.user_properties.is_empty() {
                    let effective_protocol = if let Some(key) = &connector_key {
                        instance
                            .get_shared_mqtt_client(key)
                            .ok_or_else(|| {
                                format!("mqtt sink references unknown connector_key `{key}`")
                            })?
                            .protocol_version
                    } else {
                        mqtt_props.protocol_version.unwrap_or_default()
                    };
                    if effective_protocol == flow::MqttProtocolVersion::V3 {
                        return Err(
                            "mqtt sink user_properties require effective protocol_version `v5`"
                                .to_string(),
                        );
                    }
                }

                let mut props =
                    MqttSinkProps::new(broker.unwrap_or_default(), topic, qos).with_retain(retain);
                if let Some(protocol_version) = mqtt_props.protocol_version {
                    props = props.with_protocol_version(protocol_version);
                }
                props = props.with_user_properties(mqtt_props.user_properties);
                if let Some(client_id) = mqtt_props.client_id {
                    props = props.with_client_id(client_id);
                }
                if let Some(connector_key) = connector_key {
                    props = props.with_connector_key(connector_key);
                }
                if let Some(max_packet_size) = mqtt_props.max_packet_size {
                    props = props.with_max_packet_size(max_packet_size);
                }
                SinkDefinition::new(sink_id.clone(), SinkType::Mqtt, SinkProps::Mqtt(props))
            }
            "nop" => {
                let nop_props: NopSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid nop sink props: {err}"))?;
                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Nop,
                    SinkProps::Nop(NopSinkProps {
                        log: nop_props.log.unwrap_or(false),
                    }),
                )
            }
            "kuksa" => {
                let kuksa_props: KuksaSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid kuksa sink props: {err}"))?;
                let addr = kuksa_props
                    .addr
                    .ok_or_else(|| "kuksa sink props missing addr".to_string())?;
                let vss_path = kuksa_props
                    .vss_path
                    .ok_or_else(|| "kuksa sink props missing vss_path".to_string())?;
                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Kuksa,
                    SinkProps::Kuksa(KuksaSinkProps { addr, vss_path }),
                )
            }
            "kura" => {
                let kura_props: KuraSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid kura sink props: {err}"))?;
                let addr = kura_props
                    .addr
                    .ok_or_else(|| "kura sink props missing addr".to_string())?;
                let mapping_path = kura_props
                    .mapping_path
                    .ok_or_else(|| "kura sink props missing mapping_path".to_string())?;
                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Kura,
                    SinkProps::Kura(KuraSinkProps { addr, mapping_path }),
                )
            }
            "memory" => {
                let memory_props: MemorySinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid memory sink props: {err}"))?;
                let topic = memory_props.topic;
                validate_resource_id(ResourceIdKind::MemoryTopic, &topic)
                    .map_err(|err| format!("memory sink references invalid topic: {err}"))?;

                let expects_collection = sink_req.encoder.encode_type.eq_ignore_ascii_case("none");
                let expected_kind = if expects_collection {
                    flow::connector::MemoryTopicKind::Collection
                } else {
                    flow::connector::MemoryTopicKind::Bytes
                };
                let actual_kind = instance
                    .memory_topic_kind(&topic)
                    .ok_or_else(|| format!("memory topic `{topic}` not declared"))?;
                if actual_kind != expected_kind {
                    return Err(format!(
                        "memory topic `{topic}` kind mismatch: expected {}, got {}",
                        expected_kind, actual_kind
                    ));
                }

                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::Memory,
                    SinkProps::Memory(MemorySinkProps::new(topic)),
                )
            }
            "file" => {
                let file_props: FileSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid file sink props: {err}"))?;
                let props = build_file_sink_props(file_props, &property_ctx, &req.id, &sink_id)?;
                SinkDefinition::new(sink_id.clone(), SinkType::File, SinkProps::File(props))
            }
            "video" => {
                let video_props: VideoSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid video sink props: {err}"))?;
                let props = build_video_sink_props(&sink_id, video_props)?;
                SinkDefinition::new(sink_id.clone(), SinkType::Video, SinkProps::Video(props))
            }
            "nng_pubsub" => {
                let nng_props: NngPubSubSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid nng_pubsub sink props: {err}"))?;
                let url = nng_props
                    .url
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "nng_pubsub sink requires url".to_string())?;
                let topic = nng_props
                    .topic
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "nng_pubsub sink requires topic".to_string())?;
                let topic_delimiter = nng_props
                    .topic_delimiter
                    .or(nng_props.topic_delimiter_camel)
                    .unwrap_or_else(|| {
                        flow::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER.to_string()
                    });
                let props =
                    NngPubSubSinkProps::new(url, topic).with_topic_delimiter(topic_delimiter);
                SinkDefinition::new(
                    sink_id.clone(),
                    SinkType::NngPubSub,
                    SinkProps::NngPubSub(props),
                )
            }
            "http" => {
                let http_props: HttpSinkPropsRequest =
                    serde_json::from_value(sink_req.props.to_value())
                        .map_err(|err| format!("invalid http sink props: {err}"))?;
                http_props.reject_sensitive_plain_headers()?;
                let resolved_secret_headers = http_props.resolve_secret_headers(&secret_ctx)?;
                let body = render_http_body(
                    http_props.to_body_config()?,
                    &property_ctx,
                    &req.id,
                    &sink_id,
                )?;
                let url = http_props
                    .url
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "http sink requires url".to_string())?;
                let mut props = HttpSinkProps::new(url).with_body(body);
                if let Some(method) = http_props.method.filter(|v| !v.trim().is_empty()) {
                    props = props.with_method(method);
                }
                if let Some(timeout_secs) = http_props.timeout_secs {
                    props = props.with_timeout_secs(timeout_secs);
                }
                if let Some(ref headers) = http_props.headers {
                    for (key, value) in headers {
                        let value = render_connector_string(
                            &property_ctx,
                            value,
                            &req.id,
                            &sink_id,
                            &format!("props.headers.{key}"),
                        )?;
                        reqwest::header::HeaderValue::from_str(value.expose()).map_err(|_| {
                            format!(
                                "pipeline `{}` sink `{sink_id}` has invalid props.headers.{key}",
                                req.id
                            )
                        })?;
                        props = props.with_header(key.as_str(), value);
                    }
                }
                // Structured auth + secret headers resolved from the store (runtime
                // only; the request JSON keeps the `SecretRef` pointers).
                for (key, value) in resolved_secret_headers {
                    props = props.with_header(key, flow::ConnectorString::sensitive(value));
                }
                if let Some(ref ct) = http_props.content_type.filter(|v| !v.trim().is_empty()) {
                    props = props.with_content_type(ct);
                }
                if let Some(max_size) = http_props.max_body_size {
                    props = props.with_max_body_size(max_size);
                }
                // Collect retry config from HTTP request props (applied at sink level below).
                let mut http_retry = SinkRetryConfig::default();
                if let Some(max_attempts) = http_props.retry_max_attempts {
                    http_retry.max_attempts = Some(max_attempts);
                }
                if let Some(backoff_ms) = http_props.retry_backoff_ms {
                    http_retry.initial_backoff_ms = backoff_ms;
                }
                if let Some(max_backoff_ms) = http_props.retry_max_backoff_ms {
                    http_retry.max_backoff_ms = max_backoff_ms;
                }
                SinkDefinition::new(sink_id.clone(), SinkType::Http, SinkProps::Http(props))
                    .with_retry(http_retry)
            }
            other => return Err(format!("unsupported sink type: {other}")),
        };

        let mut encoder_config = match sink_definition.sink_type {
            SinkType::Kuksa | SinkType::Kura => SinkEncoderConfig::new("none", JsonMap::new()),
            SinkType::Video => {
                let default_encoder = sink_req.encoder.encode_type.eq_ignore_ascii_case("json")
                    && sink_req.encoder.props.is_empty()
                    && sink_req.encoder.transform.is_none();
                let none_encoder = sink_req.encoder.encode_type.eq_ignore_ascii_case("none")
                    && sink_req.encoder.props.is_empty()
                    && sink_req.encoder.transform.is_none();
                if !default_encoder && !none_encoder {
                    return Err("video sink does not support encoder config".to_string());
                }
                SinkEncoderConfig::new("none", JsonMap::new())
            }
            SinkType::NngPubSub => {
                let encoder_kind = sink_req.encoder.encode_type.clone();
                if !encoder_registry.is_registered(&encoder_kind) {
                    return Err(format!("encoder kind `{encoder_kind}` not registered"));
                }
                SinkEncoderConfig::new(encoder_kind, sink_req.encoder.props.clone())
            }
            SinkType::Memory if sink_req.encoder.encode_type.eq_ignore_ascii_case("none") => {
                SinkEncoderConfig::new("none", sink_req.encoder.props.clone())
            }
            SinkType::File if sink_req.encoder.encode_type.eq_ignore_ascii_case("none") => {
                return Err(
                    "file sink requires an encoder; encoder.type=none is not supported".to_string(),
                );
            }
            _ => {
                let encoder_kind = sink_req.encoder.encode_type.clone();
                if !encoder_registry.is_registered(&encoder_kind) {
                    return Err(format!("encoder kind `{encoder_kind}` not registered"));
                }
                SinkEncoderConfig::new(encoder_kind, sink_req.encoder.props.clone())
            }
        };

        encoder_config =
            apply_encoder_transform_request(encoder_config, sink_req.encoder.transform.as_ref());
        encoder_config = resolve_proto_bundle_for_encoder(encoder_config, &sink_req.encoder)?;
        encoder_config
            .validate()
            .map_err(|err| format!("invalid encoder config for sink `{sink_id}`: {err}"))?;
        let output_config = sink_req
            .output
            .as_ref()
            .map(SinkOutputConfigRequest::to_output_config)
            .transpose()?
            .unwrap_or_default();
        if matches!(sink_definition.sink_type, SinkType::Video) && output_config.is_delta() {
            return Err("video sink does not support output.mode=delta".to_string());
        }

        let mut sink_definition = sink_definition
            .with_encoder(encoder_config)
            .with_output(output_config)
            .with_common_props(sink_req.common.to_common_props());
        if let Some(delivery) = sink_req.delivery.as_ref() {
            if let Some(compression) = delivery.compression.as_ref() {
                sink_definition =
                    sink_definition.with_compression(compression.to_compression_codec()?);
            }
            if let Some(encryption) = delivery.encryption.as_ref() {
                sink_definition =
                    sink_definition.with_encryption(encryption.to_encryption_config(&secret_ctx)?);
            }
        }
        if let Some(retry) = sink_req.retry.as_ref() {
            sink_definition = sink_definition.with_retry(retry.to_retry_config()?);
        }
        sink_definition.retry.validate()?;
        sinks.push(sink_definition);
    }
    let schedule = req.options.schedule.as_ref().map(|s| {
        flow::pipeline::PipelineScheduleConfig::new(s.cron.trim(), s.duration_secs)
            .with_datetime_ranges(
                s.datetime_ranges
                    .iter()
                    .map(|range| {
                        flow::pipeline::PipelineScheduleDatetimeRange::new(
                            range.begin_timestamp_ms,
                            range.end_timestamp_ms,
                        )
                    })
                    .collect(),
            )
    });
    let options = PipelineOptions {
        data_channel_capacity: req.options.data_channel_capacity,
        eventtime: flow::pipeline::EventtimeOptions {
            enabled: req.options.eventtime.enabled,
            late_tolerance: Duration::from_millis(req.options.eventtime.late_tolerance_ms),
        },
        schedule,
        shared_slice_projection_enabled: None,
    };
    Ok(
        PipelineDefinition::new(req.id.clone(), req.sql.clone(), sinks)
            .with_sources(sources)
            .with_options(options),
    )
}

pub(crate) fn status_label(status: PipelineStatus) -> String {
    match status {
        PipelineStatus::Stopped => "stopped".to_string(),
        PipelineStatus::Running => "running".to_string(),
    }
}

fn apply_encoder_transform_request(
    encoder_config: SinkEncoderConfig,
    transform: Option<&EncoderTransformRequest>,
) -> SinkEncoderConfig {
    let Some(transform) = transform else {
        return encoder_config;
    };

    if matches!(encoder_config.kind(), SinkEncoderKind::None) {
        return encoder_config;
    }

    encoder_config.with_transform_template(transform.template.clone())
}

/// Resolve a [`ProtoDescriptorBundle`] from the schema registry when the encoder
/// type is `"protobuf"` and `props.ref` references a named schema.
fn resolve_proto_bundle_for_encoder(
    encoder_config: SinkEncoderConfig,
    encoder_req: &super::types::EncoderConfigRequest,
) -> Result<SinkEncoderConfig, String> {
    if !matches!(encoder_config.kind(), SinkEncoderKind::Protobuf) {
        return Ok(encoder_config);
    }

    let schema_ref = encoder_req
        .props
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "protobuf encoder requires encoder.props.ref referencing a named proto schema"
                .to_string()
        })?;

    let bundle = crate::stream::named_schema_store()
        .get_proto_bundle(&schema_ref)
        .ok_or_else(|| {
            format!(
                "protobuf encoder references schema `{schema_ref}` which has no proto descriptor bundle"
            )
        })?;

    Ok(encoder_config.with_proto_bundle(bundle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::FlowInstance;
    use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
    use flow::connector::{DEFAULT_MEMORY_PUBSUB_CAPACITY, MemoryTopicKind};
    use flow::secret::SecretString;
    use flow::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn test_instance() -> FlowInstance {
        FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
            "default", None,
        ))
        .expect("create flow instance")
    }

    async fn install_json_stream(instance: &FlowInstance, stream_name: &str) {
        let schema = Schema::new(vec![ColumnSchema::new(
            stream_name.to_string(),
            "speed".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]);
        let definition = StreamDefinition::new(
            stream_name.to_string(),
            Arc::new(schema),
            StreamProps::Mock(MockStreamProps::default()),
            StreamDecoderConfig::json(),
        );
        instance
            .create_stream(definition, false)
            .await
            .expect("create test stream");
    }

    fn sample_request_with_encoder(encoder: serde_json::Value) -> CreatePipelineRequest {
        serde_json::from_value(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": encoder
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request")
    }

    #[tokio::test]
    async fn build_pipeline_definition_wires_encoder_transform_request() {
        let instance = test_instance();
        let request = sample_request_with_encoder(json!({
            "type": "json",
            "transform": {
                "template": "{\"x\":{{ json(.row.a) }} }"
            }
        }));

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];

        assert_eq!(sink.encoder.kind_str(), "json");
        assert_eq!(sink.encoder.transform_kind(), Some("template"));
        assert_eq!(
            sink.encoder.transform_template(),
            Some("{\"x\":{{ json(.row.a) }} }")
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_renders_supported_connector_properties() {
        let instance = test_instance();
        instance.set_property_context(flow::PropertyContext::new(BTreeMap::from([
            ("vin".to_string(), SecretString::new("VIN-123".to_string())),
            ("cs".to_string(), SecretString::new("CS-9".to_string())),
        ])));
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_properties",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "mqtt_sink",
                    "type": "mqtt",
                    "props": {
                        "broker_url": "mqtt://localhost:1883",
                        "topic": "vehicles/{{ prop(\"vin\") }}"
                    },
                    "encoder": { "type": "json" }
                },
                {
                    "id": "http_sink",
                    "type": "http",
                    "props": {
                        "url": "http://localhost/upload",
                        "headers": {
                            "x-device": "{{ prop(\"vin\") }}-{{ prop(\"cs\") }}"
                        },
                        "body": {
                            "type": "multipart",
                            "file_field_name": "payload",
                            "fields": {
                                "vehicle": "{{ prop(\"vin\") }}"
                            }
                        }
                    },
                    "encoder": { "type": "json" }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");

        let SinkProps::Mqtt(mqtt) = &definition.sinks()[0].props else {
            panic!("expected MQTT sink");
        };
        assert_eq!(mqtt.topic.expose(), "vehicles/VIN-123");
        assert!(mqtt.topic.is_sensitive());

        let SinkProps::Http(http) = &definition.sinks()[1].props else {
            panic!("expected HTTP sink");
        };
        let header = http.headers.get("x-device").expect("rendered header");
        assert_eq!(header.expose(), "VIN-123-CS-9");
        assert!(header.is_sensitive());
        let flow::HttpBodyConfig::Multipart(multipart) = &http.body else {
            panic!("expected multipart body");
        };
        let field = multipart.fields.get("vehicle").expect("rendered field");
        assert_eq!(field.expose(), "VIN-123");
        assert!(field.is_sensitive());
    }

    #[tokio::test]
    async fn build_pipeline_definition_ignores_transform_when_encoder_none() {
        let instance = test_instance();
        let topic = "sink_none_transform";
        instance
            .declare_memory_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare collection memory topic");
        let request = serde_json::from_value(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "memory",
                    "props": { "topic": topic },
                    "encoder": {
                        "type": "none",
                        "transform": {
                            "template": "{\"x\":{{ json(.row.a) }} }"
                        }
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];

        assert_eq!(sink.encoder.kind_str(), "none");
        assert_eq!(sink.encoder.transform_kind(), None);
        assert_eq!(sink.encoder.transform_template(), None);
    }

    #[tokio::test]
    async fn build_pipeline_definition_wires_sink_output_request() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json"
                    },
                    "output": {
                        "mode": "delta",
                        "delta": {
                            "columns": ["a"]
                        }
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];

        assert_eq!(sink.output.mode.as_str(), "delta");
        assert_eq!(sink.output.delta_columns(), Some(&["a".to_string()][..]));
    }

    #[test]
    fn create_pipeline_request_rejects_non_object_encoder_transform() {
        let result = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json",
                        "transform": "oops"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }));

        assert!(
            result.is_err(),
            "non-object transform should fail deserialization"
        );
    }

    #[test]
    fn create_pipeline_request_rejects_missing_template_in_encoder_transform() {
        let result = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json",
                        "transform": {
                            "tpl": "{\"x\":{{ json(.row.a) }} }"
                        }
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }));

        assert!(
            result.is_err(),
            "missing template should fail deserialization"
        );
    }

    #[test]
    fn create_pipeline_request_rejects_missing_sink_output_mode() {
        let result = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json"
                    },
                    "output": {
                        "delta": {
                            "columns": ["a"]
                        }
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }));

        assert!(
            result.is_err(),
            "missing output.mode should fail deserialization"
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_invalid_sink_output_mode() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json"
                    },
                    "output": {
                        "mode": "patch"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let err =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("invalid output mode should fail");
        assert!(
            err.contains("invalid sink output.mode"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_normalizes_source_stream_names() {
        let instance = test_instance();
        install_json_stream(&instance, "vehicle_stream").await;

        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT speed FROM vehicle_stream",
            "sources": [
                {
                    "stream": "  vehicle_stream  ",
                    "input": {
                        "mode": "on_change",
                        "on_change": {
                            "columns": ["speed"]
                        }
                    }
                }
            ],
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");

        assert_eq!(definition.sources()[0].stream, "vehicle_stream");
        assert!(definition.sources()[0].input.is_on_change());
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_duplicate_sources_after_trimming() {
        let instance = test_instance();
        install_json_stream(&instance, "vehicle_stream").await;

        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_1",
            "revision": 1,
            "sql": "SELECT speed FROM vehicle_stream",
            "sources": [
                {
                    "stream": "vehicle_stream"
                },
                {
                    "stream": "  vehicle_stream  "
                }
            ],
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nop",
                    "props": { "log": false },
                    "encoder": {
                        "type": "json"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let err =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("duplicate source stream should fail");
        assert!(
            err.contains("pipeline source `vehicle_stream` is configured more than once"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_allows_shared_mqtt_sink_without_broker_url() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_shared_mqtt_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "mqtt",
                    "props": {
                        "topic": "out/topic",
                        "qos": 1,
                        "connector_key": "shared_mqtt"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];
        let flow::pipeline::SinkProps::Mqtt(mqtt) = &sink.props else {
            panic!("expected mqtt sink props");
        };
        assert_eq!(mqtt.broker_url, "");
        assert_eq!(mqtt.topic.expose(), "out/topic");
        assert_eq!(mqtt.connector_key.as_deref(), Some("shared_mqtt"));
    }

    #[tokio::test]
    async fn build_pipeline_definition_accepts_static_mqtt_v5_user_properties() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_mqtt_v5_properties",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [{
                "id": "sink_1",
                "type": "mqtt",
                "props": {
                    "broker_url": "tcp://127.0.0.1:1883",
                    "topic": "out/topic",
                    "qos": 1,
                    "protocol_version": "v5",
                    "user_properties": [
                        { "key": "tag", "value": "primary" },
                        { "key": "tag", "value": "edge" }
                    ]
                }
            }],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": { "enabled": false, "late_tolerance_ms": 0 }
            }
        }))
        .expect("deserialize MQTT 5 pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build MQTT 5 pipeline definition");
        let flow::pipeline::SinkProps::Mqtt(mqtt) = &definition.sinks()[0].props else {
            panic!("expected MQTT sink props");
        };
        assert_eq!(mqtt.protocol_version, Some(flow::MqttProtocolVersion::V5));
        assert_eq!(mqtt.user_properties.len(), 2);
        assert_eq!(mqtt.user_properties[0].value, "primary");
        assert_eq!(mqtt.user_properties[1].value, "edge");
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_user_properties_for_mqtt_v3() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_mqtt_v3_properties",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [{
                "id": "sink_1",
                "type": "mqtt",
                "props": {
                    "broker_url": "tcp://127.0.0.1:1883",
                    "topic": "out/topic",
                    "user_properties": [{ "key": "source", "value": "veloflux" }]
                }
            }],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": { "enabled": false, "late_tolerance_ms": 0 }
            }
        }))
        .expect("deserialize MQTT v3 pipeline request");

        let error =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("MQTT v3 User Properties must be rejected");
        assert!(error.contains("require effective protocol_version `v5`"));
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_local_protocol_with_connector_key() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_shared_mqtt_protocol",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [{
                "id": "sink_1",
                "type": "mqtt",
                "props": {
                    "topic": "out/topic",
                    "connector_key": "shared_mqtt",
                    "protocol_version": "v5"
                }
            }],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": { "enabled": false, "late_tolerance_ms": 0 }
            }
        }))
        .expect("deserialize shared MQTT pipeline request");

        let error =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("connector-local protocol version must be rejected");
        assert!(error.contains("protocol_version is owned by connector_key"));
    }

    #[tokio::test]
    async fn build_pipeline_definition_accepts_nng_pubsub_sink() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_nng_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "nng_pubsub",
                    "props": {
                        "url": "inproc://manager-nng-sink",
                        "topic": "topic/can"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];
        let flow::pipeline::SinkProps::NngPubSub(nng) = &sink.props else {
            panic!("expected nng_pubsub sink props");
        };
        assert_eq!(nng.url, "inproc://manager-nng-sink");
        assert_eq!(nng.topic, "topic/can");
        assert_eq!(
            nng.topic_delimiter,
            flow::connector::nng_pubsub::DEFAULT_TOPIC_DELIMITER
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_accepts_file_sink() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_file_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "file",
                    "props": {
                        "path": "/tmp/veloflux-file-sink-test",
                        "filename_prefix": "speed_",
                        "filename_suffix": ".json",
                        "retention": {
                            "max_file_count": 10,
                            "max_file_age_days": 7
                        }
                    },
                    "encoder": {
                        "type": "json"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];
        let flow::pipeline::SinkProps::File(file) = &sink.props else {
            panic!("expected file sink props");
        };
        assert_eq!(file.path, "/tmp/veloflux-file-sink-test");
        assert_eq!(file.filename_prefix.expose(), "speed_");
        assert_eq!(file.filename_suffix.expose(), ".json");
        assert_eq!(file.retention.max_file_count, 10);
        assert_eq!(file.retention.max_file_age_days, 7);
    }

    #[tokio::test]
    async fn build_pipeline_definition_renders_file_name_properties() {
        let instance = test_instance();
        instance.set_property_context(flow::PropertyContext::new(BTreeMap::from([
            ("vin".to_string(), SecretString::new("VIN-123".to_string())),
            ("format".to_string(), SecretString::new("json".to_string())),
        ])));
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_file_properties",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [{
                "id": "file_sink",
                "type": "file",
                "props": {
                    "path": "/tmp/veloflux-file-properties",
                    "filename_prefix": "vehicle_{{ prop(\"vin\") }}_",
                    "filename_suffix": ".{{ prop(\"format\") }}"
                },
                "encoder": { "type": "json" }
            }],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let SinkProps::File(file) = &definition.sinks()[0].props else {
            panic!("expected file sink");
        };

        assert_eq!(file.filename_prefix.expose(), "vehicle_VIN-123_");
        assert_eq!(file.filename_suffix.expose(), ".json");
        assert!(file.filename_prefix.is_sensitive());
        assert!(file.filename_suffix.is_sensitive());
    }

    #[tokio::test]
    async fn build_pipeline_definition_validates_rendered_file_name_properties() {
        let instance = test_instance();
        instance.set_property_context(flow::PropertyContext::new(BTreeMap::from([(
            "vin".to_string(),
            SecretString::new("bad/name".to_string()),
        )])));
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_invalid_file_properties",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [{
                "id": "file_sink",
                "type": "file",
                "props": {
                    "path": "/tmp/veloflux-file-properties",
                    "filename_prefix": "{{ prop(\"vin\") }}_",
                    "filename_suffix": ".json"
                },
                "encoder": { "type": "json" }
            }],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let err =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("rendered path separator should fail");

        assert!(err.contains("filename_prefix must not contain path separators"));
        assert!(!err.contains("bad/name"));
    }

    #[tokio::test]
    async fn build_pipeline_definition_wires_delivery_encryption_and_compression() {
        let instance = test_instance();
        let key = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=";
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_file_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "file",
                    "props": {
                        "path": "/tmp/veloflux-file-sink-test",
                        "filename_suffix": ".json.gz.vfe"
                    },
                    "delivery": {
                        "compression": {
                            "codec": "gzip"
                        },
                        "encryption": {
                            "algorithm": "aes-gcm",
                            "key": key
                        }
                    },
                    "encoder": {
                        "type": "json"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let definition =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect("build pipeline definition");
        let sink = &definition.sinks()[0];
        assert!(matches!(
            sink.compression,
            Some(flow::codec::CompressionCodec::Gzip { .. })
        ));
        let encryption = sink.encryption.as_ref().expect("encryption");
        assert_eq!(encryption.algorithm.as_str(), "aes-gcm");
        // Inline key (no store name) derives a constant key_id.
        assert_eq!(encryption.key_id, "inline");
        assert_eq!(encryption.validate_and_resolve_key_bits().unwrap(), 256);
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_invalid_delivery_key_without_leaking_value() {
        let instance = test_instance();
        let raw_key = "this-is-not-valid-base64";
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_file_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "file",
                    "props": {
                        "path": "/tmp/veloflux-file-sink-test"
                    },
                    "delivery": {
                        "encryption": {
                            "algorithm": "aes-gcm",
                            "key": raw_key
                        }
                    },
                    "encoder": {
                        "type": "json"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let err =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("invalid key should fail");
        assert!(!err.contains(raw_key), "error leaked raw key: {err}");
        assert!(err.contains("invalid base64 inline encryption key"));
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_file_sink_encoder_none() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_file_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "file",
                    "props": {
                        "path": "/tmp/veloflux-file-sink-test"
                    },
                    "encoder": {
                        "type": "none"
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let err =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("file sink should reject encoder none");
        assert!(
            err.contains("encoder.type=none is not supported"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn build_pipeline_definition_rejects_mqtt_sink_without_broker_url_when_not_shared() {
        let instance = test_instance();
        let request = serde_json::from_value::<CreatePipelineRequest>(json!({
            "id": "pipe_invalid_mqtt_sink",
            "revision": 1,
            "sql": "SELECT 1 AS a",
            "sinks": [
                {
                    "id": "sink_1",
                    "type": "mqtt",
                    "props": {
                        "topic": "out/topic",
                        "qos": 1
                    }
                }
            ],
            "options": {
                "data_channel_capacity": 16,
                "eventtime": {
                    "enabled": false,
                    "late_tolerance_ms": 0
                }
            }
        }))
        .expect("deserialize pipeline request");

        let err =
            build_pipeline_definition(&request, instance.encoder_registry().as_ref(), &instance)
                .expect_err("standalone mqtt sink should require broker_url");
        assert_eq!(err, "mqtt sink requires broker_url");
    }
}
