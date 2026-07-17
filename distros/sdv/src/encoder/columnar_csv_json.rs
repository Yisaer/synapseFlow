//! Columnar CSV JSON Encoder - Streaming columnar JSON encoder.
//!
//! This encoder accumulates record values into column buffers and emits
//! columnar JSON format. It supports streaming accumulation where the
//! pipeline processor handles buffering and flushing (e.g. based on batch count or time).
//!
//! Output format: `{"email":"a@b.com,c@d.com","id":"1,2","name":"foo,bar"}`

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datatypes::Value;
use flow::codec::encoder::{EncodeError, SinkEncoder, SinkEncoderFactory};
use flow::model::{Collection, Tuple};
use flow::planner::physical::output_layout::{OutputLayout, OutputValueRef};
use std::collections::HashSet;
use std::sync::Arc;

/// Columnar CSV JSON Encoder that accumulates records incrementally.
pub struct ColumnarCsvJsonEncoder {
    id: String,
    output_layout: Option<Arc<OutputLayout>>,
}

impl ColumnarCsvJsonEncoder {
    /// Create a new columnar CSV JSON encoder.
    ///
    /// # Arguments
    /// * `id` - Encoder identifier for logging/metrics
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            output_layout: None,
        }
    }
}

impl SinkEncoderFactory for ColumnarCsvJsonEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        let output_layout = self.output_layout.clone().ok_or_else(|| {
            EncodeError::Other("columnar_csv_json encoder requires output schema".to_string())
        })?;
        Ok(Box::new(ColumnarCsvJsonEncoderRuntime::new(output_layout)))
    }

    fn with_output_layout(
        self: Arc<Self>,
        output_layout: Arc<OutputLayout>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        validate_unique_output_names(&output_layout)?;
        Ok(Arc::new(Self {
            id: self.id.clone(),
            output_layout: Some(output_layout),
        }))
    }
}

struct ColumnarCsvJsonEncoderRuntime {
    agg: ColumnarAggregator,
    layout: Arc<ColumnarCsvJsonLayout>,
}

impl ColumnarCsvJsonEncoderRuntime {
    fn new(output_layout: Arc<OutputLayout>) -> Self {
        let layout = Arc::new(ColumnarCsvJsonLayout::from_output_layout(output_layout));
        Self {
            agg: ColumnarAggregator::new(Arc::clone(&layout)),
            layout,
        }
    }
}

impl SinkEncoder for ColumnarCsvJsonEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        self.agg = ColumnarAggregator::new(Arc::clone(&self.layout));
        Ok(None)
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<bytes::Bytes>, EncodeError> {
        for tuple in record.rows() {
            self.agg.append_tuple(tuple)?;
        }
        Ok(None)
    }

    fn finish_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        let agg = std::mem::replace(
            &mut self.agg,
            ColumnarAggregator::new(Arc::clone(&self.layout)),
        );
        agg.finish_bytes().map(bytes::Bytes::from).map(Some)
    }

    fn abort_delivery(&mut self) {
        self.agg = ColumnarAggregator::new(Arc::clone(&self.layout));
    }
}

struct ColumnarCsvJsonLayout {
    columns: Arc<[ColumnarOutputColumn]>,
}

struct ColumnarOutputColumn {
    name: String,
    value_ref: OutputValueRef,
}

impl ColumnarCsvJsonLayout {
    fn from_output_layout(output_layout: Arc<OutputLayout>) -> Self {
        Self {
            columns: build_output_columns(&output_layout).into(),
        }
    }
}

// TODO: Estimate buffer capacity dynamically based on first tuple size, then grow.
// This would reduce reallocations during batch encoding.
struct ColumnarAggregator {
    /// Column buffers: index -> accumulated values
    column_buffers: Vec<String>,
    layout: Arc<ColumnarCsvJsonLayout>,
}

impl ColumnarAggregator {
    fn new(layout: Arc<ColumnarCsvJsonLayout>) -> Self {
        let column_buffers = vec![String::new(); layout.columns.len()];
        Self {
            column_buffers,
            layout,
        }
    }

    fn append_tuple(&mut self, tuple: &Tuple) -> Result<(), EncodeError> {
        for idx in 0..self.layout.columns.len() {
            let value = self.layout.columns[idx]
                .value_ref
                .resolve(tuple)
                .map_err(EncodeError::Other)?;
            write_value_to_buffer(value, &mut self.column_buffers[idx]);
            self.column_buffers[idx].push(',');
        }
        Ok(())
    }

    fn finish_bytes(self) -> Result<Vec<u8>, EncodeError> {
        // Check if buffers are empty (no data accumulated)
        if self.column_buffers.iter().all(|b| b.is_empty()) {
            return Ok(Vec::new());
        }

        // Pre-calculate capacity: {"key":"val",} per column + braces
        let estimated_size: usize = self
            .layout
            .columns
            .iter()
            .zip(self.column_buffers.iter())
            .map(|(column, buf)| column.name.len() + buf.len() + 6) // "name":"val",
            .sum::<usize>()
            + 2;

        let mut result = String::with_capacity(estimated_size);
        result.push('{');

        for (i, column) in self.layout.columns.iter().enumerate() {
            if i > 0 {
                result.push(',');
            }

            result.push('"');
            result.push_str(&column.name);
            result.push_str("\":\"");

            // Get buffer value and trim trailing comma
            if i < self.column_buffers.len() {
                let buffer = &self.column_buffers[i];
                // Trim only the single trailing comma, preserving preceding commas for empty values
                let buffer_value = if !buffer.is_empty() {
                    &buffer[..buffer.len() - 1]
                } else {
                    buffer
                };
                result.push_str(buffer_value);
            }
            result.push('"');
        }

        result.push('}');
        Ok(result.into_bytes())
    }
}

fn build_output_columns(output_layout: &OutputLayout) -> Vec<ColumnarOutputColumn> {
    output_layout
        .columns
        .iter()
        .map(|column| ColumnarOutputColumn {
            name: column.name.to_string(),
            value_ref: column.value_ref.clone(),
        })
        .collect()
}

fn validate_unique_output_names(output_layout: &OutputLayout) -> Result<(), EncodeError> {
    let mut seen = HashSet::new();
    for column in output_layout.columns.iter() {
        if !seen.insert(column.name.as_ref()) {
            return Err(EncodeError::Other(format!(
                "columnar_csv_json encoder does not support duplicate output column `{}`",
                column.name
            )));
        }
    }
    Ok(())
}

/// Write a Value directly to a buffer using fast formatting.
/// Uses itoa for integers to avoid heap allocation.
fn write_value_to_buffer(value: &Value, buffer: &mut String) {
    match value {
        Value::Null => {} // Empty string, no allocation needed
        Value::Bool(b) => {
            buffer.push_str(if *b { "true" } else { "false" });
        }
        Value::String(s) => {
            buffer.push_str(s);
        }
        Value::Bytes(bytes) => {
            buffer.push_str(&BASE64_STANDARD.encode(bytes.as_ref()));
        }
        Value::Timestamp(ts) => {
            if let Some(value) = ts.to_rfc3339_utc() {
                buffer.push_str(&value);
            }
        }
        Value::Float32(v) => {
            // ryu is ~2x faster than the core::fmt float path on float-dense payloads.
            let mut b = ryu::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Float64(v) => {
            let mut b = ryu::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Int8(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Int16(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Int32(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Int64(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Uint8(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Uint16(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Uint32(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::Uint64(v) => {
            let mut b = itoa::Buffer::new();
            buffer.push_str(b.format(*v));
        }
        Value::List(l) => {
            // Manual JSON serialization for list
            buffer.push('[');
            let items = l.items();
            for (idx, v) in items.iter().enumerate() {
                if idx > 0 {
                    buffer.push(',');
                }
                if matches!(v, Value::String(_)) {
                    buffer.push('"');
                    write_value_to_buffer(v, buffer);
                    buffer.push('"');
                } else {
                    write_value_to_buffer(v, buffer);
                }
            }
            buffer.push(']');
        }
        Value::Struct(_s) => {
            buffer.push_str("{struct}");
        }
    }
}

/// Convert a Value to its string representation for columnar format.
/// This is kept for backward compatibility with tests.
#[allow(dead_code)]
fn value_to_string(value: &Value) -> String {
    let mut buffer = String::new();
    write_value_to_buffer(value, &mut buffer);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::model::{Message, RecordBatch};
    use flow::planner::physical::output_layout::{OutputColumnLayout, OutputValueRef};
    use std::sync::Arc;

    fn create_test_tuple(id: i64, name: &str) -> Tuple {
        let keys = vec![Arc::<str>::from("id"), Arc::<str>::from("name")];
        let values = vec![
            Arc::new(datatypes::Value::Int64(id)),
            Arc::new(datatypes::Value::String(name.to_string())),
        ];
        let message = Arc::new(Message::new(Arc::<str>::from("test"), keys, values));
        Tuple::new(vec![message])
    }

    fn create_empty_tuple() -> Tuple {
        let keys = vec![Arc::<str>::from("id"), Arc::<str>::from("name")];
        let values = vec![
            Arc::new(datatypes::Value::Null),
            Arc::new(datatypes::Value::Null),
        ];
        let message = Arc::new(Message::new(Arc::<str>::from("test"), keys, values));
        Tuple::new(vec![message])
    }

    fn encode_tuples(encoder: &ColumnarCsvJsonEncoder, tuples: Vec<Tuple>) -> Vec<u8> {
        let batch = RecordBatch::new(tuples).unwrap();
        encode_batch(encoder, &batch)
    }

    fn encode_batch(_encoder: &ColumnarCsvJsonEncoder, batch: &RecordBatch) -> Vec<u8> {
        let factory = Arc::new(ColumnarCsvJsonEncoder::new("test"))
            .with_output_layout(infer_message_schema(batch))
            .expect("attach output schema");
        let mut runtime = factory.start_encoder().unwrap();
        runtime.begin_delivery().unwrap();
        assert!(runtime.append(batch).unwrap().is_none());
        runtime.finish_delivery().unwrap().unwrap().to_vec()
    }

    fn infer_message_schema(batch: &RecordBatch) -> Arc<OutputLayout> {
        let columns = batch
            .rows()
            .first()
            .map(|tuple| {
                tuple
                    .messages()
                    .iter()
                    .enumerate()
                    .flat_map(|(message_index, message)| {
                        message
                            .entries()
                            .enumerate()
                            .map(move |(value_index, (name, _))| {
                                (
                                    name.to_string(),
                                    OutputValueRef::Message {
                                        message_index,
                                        value_index,
                                    },
                                )
                            })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        output_layout(
            columns
                .iter()
                .map(|(name, value_ref)| (name.as_str(), value_ref.clone()))
                .collect(),
        )
    }

    fn output_layout(columns: Vec<(&str, OutputValueRef)>) -> Arc<OutputLayout> {
        Arc::new(OutputLayout::new(
            columns
                .into_iter()
                .map(|(name, value_ref)| OutputColumnLayout {
                    name: Arc::from(name),
                    data_type: datatypes::ConcreteDatatype::Null,
                    value_ref,
                })
                .collect(),
        ))
    }

    fn encode_with_factory(factory: Arc<dyn SinkEncoderFactory>, batch: &RecordBatch) -> Vec<u8> {
        let mut runtime = factory.start_encoder().unwrap();
        runtime.begin_delivery().unwrap();
        assert!(runtime.append(batch).unwrap().is_none());
        runtime.finish_delivery().unwrap().unwrap().to_vec()
    }

    #[test]
    fn test_encoder_id() {
        let encoder = ColumnarCsvJsonEncoder::new("test_encoder");
        assert_eq!(encoder.id(), "test_encoder");
    }

    #[test]
    fn test_start_encoder() {
        let encoder = Arc::new(ColumnarCsvJsonEncoder::new("test"))
            .with_output_layout(output_layout(vec![(
                "id",
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            )]))
            .expect("attach output schema");
        assert!(encoder.start_encoder().is_ok());
    }

    #[test]
    fn test_encode_tuple_single() {
        let encoder = ColumnarCsvJsonEncoder::new("test");
        let tuple = create_test_tuple(123, "alice");
        let batch = RecordBatch::new(vec![tuple]).unwrap();

        let result = encode_batch(&encoder, &batch);
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["id"], "123");
        assert_eq!(json["name"], "alice");
    }

    #[test]
    fn test_runtime_append_and_finish() {
        let encoder = ColumnarCsvJsonEncoder::new("test");

        let result = encode_tuples(
            &encoder,
            vec![
                create_test_tuple(123, "alice"),
                create_test_tuple(456, "bob"),
            ],
        );
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["id"], "123,456");
        assert_eq!(json["name"], "alice,bob");
    }

    #[test]
    fn test_empty_aggregator() {
        let layout = Arc::new(ColumnarCsvJsonLayout::from_output_layout(output_layout(
            vec![(
                "id",
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            )],
        )));
        let aggregator = ColumnarAggregator::new(layout);
        let result = aggregator.finish_bytes().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_values_are_preserved() {
        let encoder = ColumnarCsvJsonEncoder::new("test");
        let result = encode_tuples(
            &encoder,
            vec![
                create_test_tuple(1, "a"),
                create_empty_tuple(),
                create_test_tuple(3, "c"),
                create_empty_tuple(),
                create_empty_tuple(),
            ],
        );
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["id"], "1,,3,,");
        assert_eq!(json["name"], "a,,c,,");
    }

    #[test]
    fn test_only_empty_values() {
        let encoder = ColumnarCsvJsonEncoder::new("test");
        let result = encode_tuples(&encoder, vec![create_empty_tuple(), create_empty_tuple()]);
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["id"], ",");
        assert_eq!(json["name"], ",");
    }

    #[test]
    fn test_value_to_string() {
        use datatypes::Value;
        assert_eq!(value_to_string(&Value::Null), "");
        assert_eq!(value_to_string(&Value::Bool(true)), "true");
        assert_eq!(value_to_string(&Value::Int64(42)), "42");
        assert_eq!(value_to_string(&Value::Float64(3.14)), "3.14");
        assert_eq!(value_to_string(&Value::String("test".to_string())), "test");
    }

    #[test]
    fn test_value_to_string_all_types() {
        use datatypes::{ListValue, StructField, StructType, StructValue, Value};

        // Bool false
        assert_eq!(value_to_string(&Value::Bool(false)), "false");

        // Integer types
        assert_eq!(value_to_string(&Value::Int8(-128)), "-128");
        assert_eq!(value_to_string(&Value::Int16(-32768)), "-32768");
        assert_eq!(value_to_string(&Value::Int32(-2147483648)), "-2147483648");
        assert_eq!(value_to_string(&Value::Uint8(255)), "255");
        assert_eq!(value_to_string(&Value::Uint16(65535)), "65535");
        assert_eq!(value_to_string(&Value::Uint32(4294967295)), "4294967295");
        assert_eq!(
            value_to_string(&Value::Uint64(18446744073709551615)),
            "18446744073709551615"
        );

        // Float32
        assert_eq!(value_to_string(&Value::Float32(3.14)), "3.14");

        // List with integers
        let int_list = Value::List(ListValue::new(
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)],
            Arc::new(datatypes::ConcreteDatatype::Int64(datatypes::Int64Type)),
        ));
        assert_eq!(value_to_string(&int_list), "[1,2,3]");

        // List with strings
        let str_list = Value::List(ListValue::new(
            vec![
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ],
            Arc::new(datatypes::ConcreteDatatype::String(datatypes::StringType)),
        ));
        assert_eq!(value_to_string(&str_list), "[\"a\",\"b\"]");

        // Empty list
        let empty_list = Value::List(ListValue::new(
            vec![],
            Arc::new(datatypes::ConcreteDatatype::Int64(datatypes::Int64Type)),
        ));
        assert_eq!(value_to_string(&empty_list), "[]");

        // Struct
        let struct_val = Value::Struct(StructValue::new(
            vec![Value::Int64(42)],
            StructType::new(Arc::new(vec![StructField::new(
                "x".to_string(),
                datatypes::ConcreteDatatype::Int64(datatypes::Int64Type),
                false,
            )])),
        ));
        assert_eq!(value_to_string(&struct_val), "{struct}");
    }

    #[test]
    fn test_encode_with_collection() {
        let encoder = ColumnarCsvJsonEncoder::new("test");
        let tuple1 = create_test_tuple(1, "alice");
        let tuple2 = create_test_tuple(2, "bob");
        let batch = RecordBatch::new(vec![tuple1, tuple2]).unwrap();

        let result = encode_batch(&encoder, &batch);
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["id"], "1,2");
        assert_eq!(json["name"], "alice,bob");
    }

    #[test]
    fn test_runtime_with_various_types() {
        let encoder = ColumnarCsvJsonEncoder::new("test");

        let keys = vec![
            Arc::<str>::from("int8"),
            Arc::<str>::from("int16"),
            Arc::<str>::from("bool"),
            Arc::<str>::from("float32"),
        ];
        let values = vec![
            Arc::new(datatypes::Value::Int8(42)),
            Arc::new(datatypes::Value::Int16(1000)),
            Arc::new(datatypes::Value::Bool(false)),
            Arc::new(datatypes::Value::Float32(1.5)),
        ];
        let message = Arc::new(Message::new(Arc::<str>::from("test"), keys, values));
        let tuple = Tuple::new(vec![message]);

        let result = encode_tuples(&encoder, vec![tuple]);
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["int8"], "42");
        assert_eq!(json["int16"], "1000");
        assert_eq!(json["bool"], "false");
        assert_eq!(json["float32"], "1.5");
    }

    #[test]
    fn test_runtime_with_uint_types() {
        let encoder = ColumnarCsvJsonEncoder::new("test");

        let keys = vec![
            Arc::<str>::from("u8"),
            Arc::<str>::from("u16"),
            Arc::<str>::from("u32"),
            Arc::<str>::from("u64"),
        ];
        let values = vec![
            Arc::new(datatypes::Value::Uint8(255)),
            Arc::new(datatypes::Value::Uint16(65535)),
            Arc::new(datatypes::Value::Uint32(4294967295)),
            Arc::new(datatypes::Value::Uint64(1234567890)),
        ];
        let message = Arc::new(Message::new(Arc::<str>::from("test"), keys, values));
        let tuple = Tuple::new(vec![message]);

        let result = encode_tuples(&encoder, vec![tuple]);
        let json_str = String::from_utf8(result).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["u8"], "255");
        assert_eq!(json["u16"], "65535");
        assert_eq!(json["u32"], "4294967295");
        assert_eq!(json["u64"], "1234567890");
    }

    #[test]
    fn output_layout_controls_order_and_by_index_value_access() {
        let keys = vec![
            Arc::<str>::from("a"),
            Arc::<str>::from("b"),
            Arc::<str>::from("c"),
        ];
        let values = vec![
            Arc::new(datatypes::Value::Int64(1)),
            Arc::new(datatypes::Value::Int64(2)),
            Arc::new(datatypes::Value::Int64(3)),
        ];
        let message = Arc::new(Message::new(Arc::<str>::from("test"), keys, values));
        let mut tuple = Tuple::new(vec![message]);
        tuple.add_affiliate_column(Arc::new("x".to_string()), datatypes::Value::Int64(9));
        let batch = RecordBatch::new(vec![tuple]).unwrap();
        let schema = output_layout(vec![
            (
                "a",
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            ),
            ("x", OutputValueRef::Affiliate { affiliate_index: 0 }),
            (
                "c",
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 2,
                },
            ),
        ]);
        let factory = Arc::new(ColumnarCsvJsonEncoder::new("test"))
            .with_output_layout(schema)
            .expect("attach output layout");

        let result = encode_with_factory(factory, &batch);
        assert_eq!(
            String::from_utf8(result).unwrap(),
            r#"{"a":"1","x":"9","c":"3"}"#
        );
    }

    #[test]
    fn output_layout_missing_values_are_encoded_as_empty_cells() {
        let tuple = create_test_tuple(1, "a");
        let batch = RecordBatch::new(vec![tuple]).unwrap();
        let schema = output_layout(vec![("missing", OutputValueRef::Null)]);
        let factory = Arc::new(ColumnarCsvJsonEncoder::new("test"))
            .with_output_layout(schema)
            .expect("attach output schema");

        let result = encode_with_factory(factory, &batch);
        assert_eq!(String::from_utf8(result).unwrap(), r#"{"missing":""}"#);
    }

    #[test]
    fn output_layout_empty_delivery_stays_empty_bytes() {
        let batch = RecordBatch::new(vec![]).unwrap();
        let schema = output_layout(vec![(
            "a",
            OutputValueRef::Message {
                message_index: 0,
                value_index: 0,
            },
        )]);
        let factory = Arc::new(ColumnarCsvJsonEncoder::new("test"))
            .with_output_layout(schema)
            .expect("attach output schema");

        let result = encode_with_factory(factory, &batch);
        assert!(result.is_empty());
    }

    #[test]
    fn output_layout_rejects_duplicate_names() {
        let schema = output_layout(vec![
            (
                "a",
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            ),
            (
                "a",
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 1,
                },
            ),
        ]);

        let err = match Arc::new(ColumnarCsvJsonEncoder::new("test")).with_output_layout(schema) {
            Ok(_) => panic!("duplicate names should be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("duplicate output column `a`"));
    }
}
