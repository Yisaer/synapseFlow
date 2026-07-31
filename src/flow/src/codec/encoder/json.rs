use super::template_transform::JsonTemplateTransform;
use super::{EncodeError, SinkEncoder, SinkEncoderFactory};
use crate::model::{Collection, Tuple};
use crate::planner::physical::output_layout::{OutputLayout, OutputValueRef};
use crate::planner::sink::{SinkEncoderConfig, SinkEncoderTransformConfig};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use datatypes::Value;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::sync::Arc;

/// Encoder that emits the entire collection as a JSON array of row objects.
pub struct JsonEncoder {
    id: String,
    props: JsonMap<String, JsonValue>,
    omit_null_columns: bool,
    transform: Option<Arc<JsonTemplateTransform>>,
    output_layout: Option<Arc<OutputLayout>>,
}

impl JsonEncoder {
    /// Create a new JSON encoder with the provided identifier.
    pub fn new(id: impl Into<String>, config: &SinkEncoderConfig) -> Result<Self, EncodeError> {
        let id = id.into();
        let omit_null_columns = config
            .json_omit_null_columns()
            .map_err(EncodeError::Other)?;
        Ok(Self {
            id,
            omit_null_columns,
            transform: json_template_transform_from_config(config)?,
            props: config.props().clone(),
            output_layout: None,
        })
    }

    /// Access encoder props (currently unused by JSON encoder).
    pub fn props(&self) -> &JsonMap<String, JsonValue> {
        &self.props
    }
}

impl SinkEncoderFactory for JsonEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        Ok(Box::new(JsonEncoderRuntime::new(
            self.omit_null_columns,
            self.transform.clone(),
            self.output_layout.clone(),
        )))
    }

    fn with_output_layout(
        self: Arc<Self>,
        output_layout: Arc<OutputLayout>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Ok(Arc::new(Self {
            id: self.id.clone(),
            props: self.props.clone(),
            omit_null_columns: self.omit_null_columns,
            transform: self.transform.clone(),
            output_layout: Some(output_layout),
        }))
    }
}

struct JsonEncoderRuntime {
    is_first_row: bool,
    omit_null_columns: bool,
    transform: Option<Arc<JsonTemplateTransform>>,
    output_layout: Option<Arc<OutputLayout>>,
}

impl JsonEncoderRuntime {
    fn new(
        omit_null_columns: bool,
        transform: Option<Arc<JsonTemplateTransform>>,
        output_layout: Option<Arc<OutputLayout>>,
    ) -> Self {
        Self {
            is_first_row: true,
            omit_null_columns,
            transform,
            output_layout,
        }
    }
}

#[derive(Clone, Copy)]
struct JsonRowEncodeOptions<'a> {
    output_layout: Option<&'a OutputLayout>,
    output_mask_mode: OutputMaskMode,
    null_policy: NullColumnPolicy,
}

impl<'a> JsonRowEncodeOptions<'a> {
    fn native(output_layout: Option<&'a OutputLayout>, null_policy: NullColumnPolicy) -> Self {
        Self {
            output_layout,
            output_mask_mode: OutputMaskMode::HonorMask,
            null_policy,
        }
    }

    fn transform(output_layout: Option<&'a OutputLayout>) -> Self {
        Self {
            output_layout,
            output_mask_mode: OutputMaskMode::DenseForTemplate,
            null_policy: NullColumnPolicy::KeepNulls,
        }
    }
}

#[derive(Clone, Copy)]
enum OutputMaskMode {
    HonorMask,
    DenseForTemplate,
}

#[derive(Clone, Copy)]
enum NullColumnPolicy {
    KeepNulls,
    OmitNullObjectFields,
}

impl SinkEncoder for JsonEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        self.is_first_row = true;
        Ok(Some(bytes::Bytes::from_static(b"[")))
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<bytes::Bytes>, EncodeError> {
        if record.num_rows() == 0 {
            return Ok(None);
        }

        let mut chunk = Vec::new();
        for tuple in record.rows() {
            if !self.is_first_row {
                chunk.push(b',');
            }
            self.is_first_row = false;
            append_tuple_json(
                &mut chunk,
                self.omit_null_columns,
                tuple,
                self.transform.as_deref(),
                self.output_layout.as_deref(),
            )?;
        }
        Ok(Some(chunk.into()))
    }

    fn finish_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        Ok(Some(bytes::Bytes::from_static(b"]")))
    }

    fn abort_delivery(&mut self) {
        self.is_first_row = true;
    }
}

fn value_to_json(value: &Value, null_policy: NullColumnPolicy) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Bytes(v) => JsonValue::String(BASE64_STANDARD.encode(v.as_ref())),
        Value::Float32(v) => number_from_f64(*v as f64),
        Value::Float64(v) => number_from_f64(*v),
        Value::Int8(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int16(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int32(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Int64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint8(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint16(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint32(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Uint64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Timestamp(v) => v
            .to_rfc3339_utc()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::Struct(struct_value) => {
            let mut map = JsonMap::new();
            let fields = struct_value.fields().fields();
            for (field, item) in fields.iter().zip(struct_value.items().iter()) {
                insert_json_field(&mut map, field.name(), item, null_policy);
            }
            JsonValue::Object(map)
        }
        Value::List(list) => {
            let values = list
                .items()
                .iter()
                .map(|item| value_to_json(item, null_policy))
                .collect::<Vec<_>>();
            JsonValue::Array(values)
        }
    }
}

fn insert_json_field(
    json_row: &mut JsonMap<String, JsonValue>,
    key: &str,
    value: &Value,
    null_policy: NullColumnPolicy,
) {
    if matches!(null_policy, NullColumnPolicy::OmitNullObjectFields) && matches!(value, Value::Null)
    {
        return;
    }

    json_row.insert(key.to_string(), value_to_json(value, null_policy));
}

fn tuple_to_json_with_options(
    tuple: &Tuple,
    options: JsonRowEncodeOptions<'_>,
) -> Result<JsonValue, EncodeError> {
    let output_layout = options.output_layout.ok_or_else(|| {
        EncodeError::Other("JSON encoding requires the final output layout".to_string())
    })?;
    encode_row_from_output_layout(tuple, output_layout, options)
}

fn encode_row_from_output_layout(
    tuple: &Tuple,
    output_layout: &OutputLayout,
    options: JsonRowEncodeOptions<'_>,
) -> Result<JsonValue, EncodeError> {
    let mut json_row = JsonMap::new();
    match options.output_mask_mode {
        OutputMaskMode::HonorMask => {
            let output_mask = tuple.output_mask();
            if output_mask.is_some_and(|mask| mask.len() != output_layout.columns.len()) {
                return Err(EncodeError::Other(format!(
                    "output_mask width {} does not match output schema width {}",
                    output_mask.map_or(0, <[bool]>::len),
                    output_layout.columns.len()
                )));
            }

            for (index, column) in output_layout.columns.iter().enumerate() {
                if output_mask.is_some_and(|mask| !mask[index]) {
                    continue;
                }
                let value = resolve_output_value(tuple, &column.value_ref)?;
                let null_policy = if output_mask.is_some() {
                    NullColumnPolicy::KeepNulls
                } else {
                    options.null_policy
                };
                insert_json_field(&mut json_row, column.name.as_ref(), value, null_policy);
            }
        }
        OutputMaskMode::DenseForTemplate => {
            // Row-diff branches expose `.row` as the dense current output row. The mask controls
            // incremental emission and does not remove unchanged values from the template input.
            for column in output_layout.columns.iter() {
                let value = resolve_output_value(tuple, &column.value_ref)?;
                insert_json_field(
                    &mut json_row,
                    column.name.as_ref(),
                    value,
                    options.null_policy,
                );
            }
        }
    }

    Ok(JsonValue::Object(json_row))
}

fn resolve_output_value<'a>(
    tuple: &'a Tuple,
    value_ref: &OutputValueRef,
) -> Result<&'a Value, EncodeError> {
    value_ref.resolve(tuple).map_err(EncodeError::Other)
}

fn append_tuple_json(
    payload: &mut Vec<u8>,
    omit_null_columns: bool,
    tuple: &Tuple,
    transform: Option<&JsonTemplateTransform>,
    output_layout: Option<&OutputLayout>,
) -> Result<(), EncodeError> {
    let options = if transform.is_some() {
        JsonRowEncodeOptions::transform(output_layout)
    } else {
        JsonRowEncodeOptions::native(
            output_layout,
            if omit_null_columns {
                NullColumnPolicy::OmitNullObjectFields
            } else {
                NullColumnPolicy::KeepNulls
            },
        )
    };
    let row = tuple_to_json_with_options(tuple, options)?;
    if let Some(transform) = transform {
        payload.extend(transform.render_item(row)?);
    } else {
        serde_json::to_writer(payload, &row).map_err(EncodeError::Serialization)?;
    }
    Ok(())
}

fn json_template_transform_from_config(
    config: &SinkEncoderConfig,
) -> Result<Option<Arc<JsonTemplateTransform>>, EncodeError> {
    match config.transform() {
        Some(SinkEncoderTransformConfig::Template { template }) => {
            JsonTemplateTransform::compile(template, config.property_context().clone())
                .map(Arc::new)
                .map(Some)
        }
        None => Ok(None),
    }
}

fn number_from_f64(value: f64) -> JsonValue {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use crate::model::Collection;
    use crate::planner::physical::output_layout::OutputColumnLayout;
    use crate::planner::sink::SinkEncoderConfig;
    use crate::secret::SecretString;
    use datatypes::{
        ConcreteDatatype, Int64Type, ListType, ListValue, StringType, StructField, StructType,
        StructValue, TimestampValue,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn extend_optional(payload: &mut Vec<u8>, chunk: Option<bytes::Bytes>) {
        if let Some(chunk) = chunk {
            payload.extend_from_slice(&chunk);
        }
    }

    fn test_output_layout(collection: &dyn Collection) -> Arc<OutputLayout> {
        let Some(tuple) = collection.rows().first() else {
            return Arc::new(OutputLayout::new(Vec::new()));
        };
        let mut columns = Vec::new();
        for (message_index, message) in tuple.messages().iter().enumerate() {
            columns.extend(
                message
                    .entries()
                    .enumerate()
                    .map(|(value_index, (name, value))| OutputColumnLayout {
                        name: Arc::from(name),
                        data_type: value.datatype(),
                        value_ref: OutputValueRef::Message {
                            message_index,
                            value_index,
                        },
                    }),
            );
        }
        if let Some(affiliate) = tuple.affiliate() {
            columns.extend(affiliate.entries().enumerate().map(
                |(affiliate_index, (name, value))| OutputColumnLayout {
                    name: Arc::clone(name),
                    data_type: value.datatype(),
                    value_ref: OutputValueRef::Affiliate { affiliate_index },
                },
            ));
        }
        Arc::new(OutputLayout::new(columns))
    }

    fn test_runtime(encoder: &JsonEncoder, collection: &dyn Collection) -> JsonEncoderRuntime {
        JsonEncoderRuntime::new(
            encoder.omit_null_columns,
            encoder.transform.clone(),
            Some(test_output_layout(collection)),
        )
    }

    fn encode_collection(encoder: &JsonEncoder, collection: &dyn Collection) -> Vec<u8> {
        let mut runtime = test_runtime(encoder, collection);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        extend_optional(
            &mut payload,
            runtime.append(collection).expect("append record"),
        );
        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );
        payload
    }

    #[test]
    fn json_encoder_emits_single_payload() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10), Value::Int64(20)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![
                    Value::String("ok".to_string()),
                    Value::String("fail".to_string()),
                ],
            ),
        ])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"amount":10, "status":"ok"},
                {"amount":20, "status":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_formats_timestamp_as_rfc3339_utc() {
        let batch = batch_from_columns_simple(vec![(
            "events".to_string(),
            "event_time".to_string(),
            vec![Value::Timestamp(
                TimestampValue::parse_rfc3339("2026-05-08T10:20:30.123456Z")
                    .expect("valid timestamp"),
            )],
        )])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"event_time": "2026-05-08T10:20:30.123456Z"}
            ])
        );
    }

    #[test]
    fn json_encoder_omits_top_level_null_columns_by_default() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::Null],
            ),
        ])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "amount": 10 }]));
    }

    #[test]
    fn json_encoder_keeps_top_level_null_columns_when_disabled() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::Null],
            ),
        ])
        .expect("valid batch");

        let config = SinkEncoderConfig::json().with_json_omit_null_columns(false);
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "amount": 10, "status": null }]));
    }

    #[test]
    fn json_encoder_emits_bytes_as_base64_strings() {
        let batch = batch_from_columns_simple(vec![(
            "frames".to_string(),
            "payload".to_string(),
            vec![Value::Bytes(bytes::Bytes::from_static(b"hello"))],
        )])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([{ "payload": "aGVsbG8=" }]));
    }

    #[test]
    fn json_encoder_omits_nested_null_object_fields_and_keeps_array_nulls() {
        let nested_type = StructType::new(Arc::new(vec![
            StructField::new("b".to_string(), ConcreteDatatype::Int64(Int64Type), false),
            StructField::new("c".to_string(), ConcreteDatatype::String(StringType), true),
            StructField::new(
                "items".to_string(),
                ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Int64(Int64Type)))),
                true,
            ),
        ]));
        let nested_value = Value::Struct(StructValue::new(
            vec![
                Value::Int64(1),
                Value::Null,
                Value::List(ListValue::new(
                    vec![Value::Int64(1), Value::Null, Value::Int64(2)],
                    Arc::new(ConcreteDatatype::Int64(Int64Type)),
                )),
            ],
            nested_type,
        ));
        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "payload".to_string(),
            vec![nested_value],
        )])
        .expect("valid batch");

        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([{
                "payload": {
                    "b": 1,
                    "items": [1, null, 2]
                }
            }])
        );
    }

    #[test]
    fn json_encoder_rejects_non_boolean_omit_null_columns() {
        let mut props = JsonMap::new();
        props.insert(
            "omit_null_columns".to_string(),
            JsonValue::String("false".to_string()),
        );
        let config = SinkEncoderConfig::new("json", props);

        let err = match JsonEncoder::new("json", &config) {
            Ok(_) => panic!("invalid config should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("omit_null_columns must be a boolean"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn json_encoder_streaming() {
        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let batch1 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(1)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("ok".to_string())],
            ),
        ])
        .expect("batch1");
        let mut runtime = test_runtime(&encoder, &batch1);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        if let Some(chunk) = runtime.append(&batch1).expect("append batch1") {
            payload.extend_from_slice(&chunk);
        }

        let batch2 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(2)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("fail".to_string())],
            ),
        ])
        .expect("batch2");
        if let Some(chunk) = runtime.append(&batch2).expect("append batch2") {
            payload.extend_from_slice(&chunk);
        }

        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"amount":1, "status":"ok"},
                {"amount":2, "status":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_streaming_empty_payload_is_array() {
        let encoder = JsonEncoder::new("json", &SinkEncoderConfig::json()).expect("encoder");
        let empty = crate::model::RecordBatch::new(Vec::new()).expect("empty batch");
        let mut runtime = test_runtime(&encoder, &empty);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(json, serde_json::json!([]));
    }

    #[test]
    fn json_encoder_transform_renders_each_row() {
        let batch = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(10), Value::Int64(20)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![
                    Value::String("ok".to_string()),
                    Value::String("fail".to_string()),
                ],
            ),
        ])
        .expect("valid batch");

        let config = SinkEncoderConfig::json_with_transform_template(
            "{\"value\":{{ json(.row.amount) }},\"label\":{{ json(.row.status) }} }",
        );
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"value":10, "label":"ok"},
                {"value":20, "label":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_transform_streaming_renders_each_row() {
        let config = SinkEncoderConfig::json_with_transform_template(
            "{\"value\":{{ json(.row.amount) }},\"label\":{{ json(.row.status) }} }",
        );
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let batch1 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(1)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("ok".to_string())],
            ),
        ])
        .expect("batch1");
        let mut runtime = test_runtime(&encoder, &batch1);
        let mut payload = Vec::new();
        extend_optional(
            &mut payload,
            runtime.begin_delivery().expect("begin delivery"),
        );
        if let Some(chunk) = runtime.append(&batch1).expect("append batch1") {
            payload.extend_from_slice(&chunk);
        }

        let batch2 = batch_from_columns_simple(vec![
            (
                "orders".to_string(),
                "amount".to_string(),
                vec![Value::Int64(2)],
            ),
            (
                "orders".to_string(),
                "status".to_string(),
                vec![Value::String("fail".to_string())],
            ),
        ])
        .expect("batch2");
        if let Some(chunk) = runtime.append(&batch2).expect("append batch2") {
            payload.extend_from_slice(&chunk);
        }

        extend_optional(
            &mut payload,
            runtime.finish_delivery().expect("finish delivery"),
        );
        let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"value":1, "label":"ok"},
                {"value":2, "label":"fail"}
            ])
        );
    }

    #[test]
    fn json_encoder_transform_exposes_row_json_and_properties() {
        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(10)],
        )])
        .expect("valid batch");
        let properties = crate::PropertyContext::new(BTreeMap::from([(
            "vin".to_string(),
            SecretString::new("VIN-123".to_string()),
        )]));
        let config = SinkEncoderConfig::json_with_transform_template(
            r#"{"vin":{{ prop("vin") | json }},"amount":{{ json(.row.amount) }} }"#,
        )
        .with_property_context(properties);

        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let payload = encode_collection(&encoder, &batch);

        assert_eq!(
            serde_json::from_slice::<JsonValue>(&payload).expect("valid JSON"),
            serde_json::json!([{"vin": "VIN-123", "amount": 10}])
        );
    }

    #[test]
    fn json_encoder_transform_reports_missing_property_as_encode_error() {
        let batch = batch_from_columns_simple(vec![(
            "orders".to_string(),
            "amount".to_string(),
            vec![Value::Int64(10)],
        )])
        .expect("valid batch");
        let config = SinkEncoderConfig::json_with_transform_template(
            r#"{"vin":{{ prop("missing") | json }},"amount":{{ json(.row.amount) }} }"#,
        );
        let encoder = JsonEncoder::new("json", &config).expect("encoder");
        let mut runtime = test_runtime(&encoder, &batch);

        let err = runtime.append(&batch).expect_err("missing property");
        assert!(err
            .to_string()
            .contains("property `missing` is not defined"));
    }

    #[test]
    fn json_encoder_rejects_malformed_transform_template() {
        let config = SinkEncoderConfig::json_with_transform_template("{{ json(.row.amount) ");
        let err = match JsonEncoder::new("json", &config) {
            Ok(_) => panic!("invalid template should fail"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("invalid template"),
            "unexpected error: {err}"
        );
    }
}
