use std::collections::HashSet;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use flow::codec::encoder::{EncodeError, SinkEncoder, SinkEncoderFactory};
use flow::model::Collection;
use flow::planner::physical::output_schema::{OutputSchema, OutputValueGetter};

/// Columnar JSON encoder: batches rows into a single JSON object keyed by column name,
/// with array values per column. The output schema defines column names and order.
/// Supports streaming accumulation with incremental JSON writing.
pub struct ColumnarJsonEncoder {
    output_schema: Option<Arc<OutputSchema>>,
}

impl Default for ColumnarJsonEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarJsonEncoder {
    pub fn new() -> Self {
        Self {
            output_schema: None,
        }
    }
}

impl SinkEncoderFactory for ColumnarJsonEncoder {
    fn id(&self) -> &str {
        "columnar_json"
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        let output_schema = self.output_schema.clone().ok_or_else(|| {
            EncodeError::Other("columnar_json encoder requires output schema".to_string())
        })?;
        Ok(Box::new(ColumnarJsonEncoderRuntime::new(output_schema)))
    }

    fn with_output_schema(
        self: Arc<Self>,
        output_schema: Arc<OutputSchema>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        validate_unique_output_names(&output_schema)?;
        Ok(Arc::new(Self {
            output_schema: Some(output_schema),
        }))
    }
}

struct ColumnarJsonEncoderRuntime {
    agg: ColumnarAggregator,
    layout: Arc<ColumnarJsonLayout>,
}

impl ColumnarJsonEncoderRuntime {
    fn new(output_schema: Arc<OutputSchema>) -> Self {
        let layout = Arc::new(ColumnarJsonLayout::from_output_schema(output_schema));
        Self {
            agg: ColumnarAggregator::new(Arc::clone(&layout)),
            layout,
        }
    }
}

impl SinkEncoder for ColumnarJsonEncoderRuntime {
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

struct ColumnarJsonLayout {
    columns: Arc<[ColumnarJsonColumn]>,
}

struct ColumnarJsonColumn {
    name: Arc<str>,
    getter: OutputValueGetter,
}

impl ColumnarJsonLayout {
    fn from_output_schema(output_schema: Arc<OutputSchema>) -> Self {
        let columns = output_schema
            .columns
            .iter()
            .map(|column| ColumnarJsonColumn {
                name: Arc::clone(&column.name),
                getter: column.getter.clone(),
            })
            .collect::<Vec<_>>();
        Self {
            columns: Arc::from(columns),
        }
    }
}

struct ColumnarAggregator {
    layout: Arc<ColumnarJsonLayout>,
    buffers: Vec<ColumnBuf>,
    row_count: usize,
}

struct ColumnBuf {
    buf: Vec<u8>,
    first: bool,
}

impl ColumnarAggregator {
    fn new(layout: Arc<ColumnarJsonLayout>) -> Self {
        let buffers = (0..layout.columns.len())
            .map(|_| ColumnBuf {
                buf: vec![b'['],
                first: true,
            })
            .collect();
        Self {
            layout,
            buffers,
            row_count: 0,
        }
    }

    fn append_tuple(&mut self, tuple: &flow::model::Tuple) -> Result<(), EncodeError> {
        self.row_count += 1;
        for idx in 0..self.layout.columns.len() {
            let column = &self.layout.columns[idx];
            let value = value_from_getter(tuple, &column.getter);
            append_json_value(&mut self.buffers[idx], value)?;
        }
        Ok(())
    }

    fn finish_bytes(mut self) -> Result<Vec<u8>, EncodeError> {
        if self.row_count == 0 {
            return Ok(b"{}".to_vec());
        }

        let mut out = Vec::new();
        out.push(b'{');
        for (idx, column) in self.layout.columns.iter().enumerate() {
            if idx > 0 {
                out.push(b',');
            }
            serde_json::to_writer(&mut out, column.name.as_ref())
                .map_err(EncodeError::Serialization)?;
            out.push(b':');
            self.buffers[idx].buf.push(b']');
            out.extend_from_slice(&self.buffers[idx].buf);
        }
        out.push(b'}');
        Ok(out)
    }
}

fn append_json_value(
    column: &mut ColumnBuf,
    value: Option<&datatypes::Value>,
) -> Result<(), EncodeError> {
    if !column.first {
        column.buf.push(b',');
    }
    column.first = false;
    match value {
        Some(value) => write_json_value(&mut column.buf, value)?,
        None => column.buf.extend_from_slice(b"null"),
    }
    Ok(())
}

fn value_from_getter<'a>(
    tuple: &'a flow::model::Tuple,
    getter: &OutputValueGetter,
) -> Option<&'a datatypes::Value> {
    match getter {
        OutputValueGetter::MessageByName {
            source_name,
            column_name,
        } => tuple.value_by_name(source_name.as_ref(), column_name.as_ref()),
        OutputValueGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|affiliate| affiliate.value(column_name.as_ref())),
    }
}

fn validate_unique_output_names(output_schema: &OutputSchema) -> Result<(), EncodeError> {
    let mut seen = HashSet::new();
    for column in output_schema.columns.iter() {
        if !seen.insert(column.name.as_ref()) {
            return Err(EncodeError::Other(format!(
                "columnar_json encoder does not support duplicate output column `{}`",
                column.name
            )));
        }
    }
    Ok(())
}

fn write_json_value(buf: &mut Vec<u8>, val: &datatypes::Value) -> Result<(), EncodeError> {
    use datatypes::Value::*;
    match val {
        Null => buf.extend_from_slice(b"null"),
        Bool(v) => buf.extend_from_slice(if *v { b"true" } else { b"false" }),
        Int8(v) => write_json_integer(buf, *v),
        Int16(v) => write_json_integer(buf, *v),
        Int32(v) => write_json_integer(buf, *v),
        Int64(v) => write_json_integer(buf, *v),
        Uint8(v) => write_json_integer(buf, *v),
        Uint16(v) => write_json_integer(buf, *v),
        Uint32(v) => write_json_integer(buf, *v),
        Uint64(v) => write_json_integer(buf, *v),
        Float32(v) => write_json_float(buf, *v),
        Float64(v) => write_json_float(buf, *v),
        String(s) => serde_json::to_writer(buf, s).map_err(EncodeError::Serialization)?,
        Bytes(bytes) => {
            let encoded = BASE64_STANDARD.encode(bytes.as_ref());
            serde_json::to_writer(buf, &encoded).map_err(EncodeError::Serialization)?;
        }
        Timestamp(ts) => match ts.to_rfc3339_utc() {
            Some(value) => {
                serde_json::to_writer(buf, &value).map_err(EncodeError::Serialization)?
            }
            None => buf.extend_from_slice(b"null"),
        },
        List(list) => {
            buf.push(b'[');
            for (idx, item) in list.items().iter().enumerate() {
                if idx > 0 {
                    buf.push(b',');
                }
                write_json_value(buf, item)?;
            }
            buf.push(b']');
        }
        Struct(strct) => {
            buf.push(b'{');
            for (idx, (field, item)) in strct
                .fields()
                .fields()
                .iter()
                .zip(strct.items().iter())
                .enumerate()
            {
                if idx > 0 {
                    buf.push(b',');
                }
                serde_json::to_writer(&mut *buf, field.name())
                    .map_err(EncodeError::Serialization)?;
                buf.push(b':');
                write_json_value(buf, item)?;
            }
            buf.push(b'}');
        }
    }
    Ok(())
}

fn write_json_integer<T>(buf: &mut Vec<u8>, value: T)
where
    T: itoa::Integer,
{
    let mut int_buf = itoa::Buffer::new();
    buf.extend_from_slice(int_buf.format(value).as_bytes());
}

fn write_json_float<T>(buf: &mut Vec<u8>, value: T)
where
    T: ryu::Float,
{
    let mut float_buf = ryu::Buffer::new();
    let formatted = float_buf.format(value);
    if formatted == "NaN" || formatted == "inf" || formatted == "-inf" {
        buf.extend_from_slice(b"null");
    } else {
        buf.extend_from_slice(formatted.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::model::{Message, RecordBatch, Tuple};
    use flow::planner::physical::output_schema::{OutputColumn, OutputValueGetter};

    fn make_tuple(source: &str, keys: &[&str], vals: &[datatypes::Value]) -> Tuple {
        let keys_arc: Vec<Arc<str>> = keys.iter().map(|k| Arc::<str>::from(*k)).collect();
        let vals_arc: Vec<Arc<datatypes::Value>> = vals.iter().cloned().map(Arc::new).collect();
        let msg = Arc::new(Message::new(Arc::<str>::from(source), keys_arc, vals_arc));
        Tuple::new(vec![msg])
    }

    fn encode_batch(_enc: &ColumnarJsonEncoder, batch: &RecordBatch) -> Vec<u8> {
        let mut runtime = schema_factory(infer_message_schema(batch))
            .start_encoder()
            .unwrap();
        runtime.begin_delivery().unwrap();
        assert!(runtime.append(batch).unwrap().is_none());
        runtime.finish_delivery().unwrap().unwrap().to_vec()
    }

    fn infer_message_schema(batch: &RecordBatch) -> Arc<OutputSchema> {
        let columns = batch
            .rows()
            .first()
            .map(|tuple| {
                tuple
                    .messages()
                    .iter()
                    .flat_map(|message| {
                        message.entries().map(|(name, _)| {
                            (
                                name.to_string(),
                                OutputValueGetter::MessageByName {
                                    source_name: Arc::from(message.source()),
                                    column_name: Arc::from(name),
                                },
                            )
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        output_schema(
            columns
                .iter()
                .map(|(name, getter)| (name.as_str(), getter.clone()))
                .collect(),
        )
    }

    fn output_schema(columns: Vec<(&str, OutputValueGetter)>) -> Arc<OutputSchema> {
        Arc::new(OutputSchema::new(
            columns
                .into_iter()
                .map(|(name, getter)| OutputColumn {
                    name: Arc::from(name),
                    data_type: datatypes::ConcreteDatatype::Null,
                    getter,
                })
                .collect(),
        ))
    }

    fn schema_factory(schema: Arc<OutputSchema>) -> Arc<dyn SinkEncoderFactory> {
        Arc::new(ColumnarJsonEncoder::new())
            .with_output_schema(schema)
            .expect("attach output schema")
    }

    fn encode_with_factory(factory: Arc<dyn SinkEncoderFactory>, batch: &RecordBatch) -> Vec<u8> {
        let mut runtime = factory.start_encoder().unwrap();
        runtime.begin_delivery().unwrap();
        assert!(runtime.append(batch).unwrap().is_none());
        runtime.finish_delivery().unwrap().unwrap().to_vec()
    }

    #[test]
    fn columnar_json_encoder_outputs_arrays() {
        // Two rows, columns a(int), b(string)
        let t1 = make_tuple(
            "src",
            &["a", "b"],
            &[
                datatypes::Value::Int64(1),
                datatypes::Value::String("x".to_string()),
            ],
        );
        let t2 = make_tuple(
            "src",
            &["a", "b"],
            &[
                datatypes::Value::Int64(2),
                datatypes::Value::String("y".to_string()),
            ],
        );
        let batch = RecordBatch::new(vec![t1, t2]).unwrap();

        let enc = ColumnarJsonEncoder::new();
        let bytes = encode_batch(&enc, &batch);
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "a": [1, 2],
                "b": ["x", "y"]
            })
        );
    }

    #[test]
    fn encoder_id_returns_columnar_json() {
        let enc = ColumnarJsonEncoder::new();
        assert_eq!(enc.id(), "columnar_json");
    }

    #[test]
    fn encode_tuple_encodes_single_row() {
        let t = make_tuple(
            "src",
            &["x", "y"],
            &[datatypes::Value::Int64(42), datatypes::Value::Bool(true)],
        );
        let batch = RecordBatch::new(vec![t]).unwrap();
        let enc = ColumnarJsonEncoder::new();
        let bytes = encode_batch(&enc, &batch);
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json, serde_json::json!({"x": [42], "y": [true]}));
    }

    #[test]
    fn start_encoder_returns_runtime_encoder() {
        let enc = schema_factory(output_schema(vec![(
            "a",
            OutputValueGetter::MessageByName {
                source_name: Arc::from("src"),
                column_name: Arc::from("a"),
            },
        )]));
        assert!(enc.start_encoder().is_ok());
    }

    #[test]
    fn runtime_encoder_works() {
        let enc = schema_factory(output_schema(vec![(
            "a",
            OutputValueGetter::MessageByName {
                source_name: Arc::from("src"),
                column_name: Arc::from("a"),
            },
        )]));
        let mut runtime = enc.start_encoder().expect("encoder runtime");
        runtime.begin_delivery().expect("begin delivery");

        let t1 = make_tuple("src", &["a"], &[datatypes::Value::Int64(10)]);
        let t2 = make_tuple("src", &["a"], &[datatypes::Value::Int64(20)]);

        let batch = RecordBatch::new(vec![t1, t2]).unwrap();
        assert!(runtime.append(&batch).unwrap().is_none());

        let bytes = runtime.finish_delivery().unwrap().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json, serde_json::json!({"a": [10, 20]}));
    }

    #[test]
    fn write_json_value_handles_all_types() {
        fn encoded(value: &datatypes::Value) -> serde_json::Value {
            let mut buf = Vec::new();
            write_json_value(&mut buf, value).expect("write json value");
            serde_json::from_slice(&buf).expect("valid json")
        }

        // Test Null
        assert_eq!(encoded(&datatypes::Value::Null), serde_json::Value::Null);

        // Test Bool
        assert_eq!(
            encoded(&datatypes::Value::Bool(true)),
            serde_json::Value::Bool(true)
        );

        // Test various int types
        assert_eq!(
            encoded(&datatypes::Value::Int8(-1)),
            serde_json::Value::from(-1i8)
        );
        assert_eq!(
            encoded(&datatypes::Value::Int16(-100)),
            serde_json::Value::from(-100i16)
        );
        assert_eq!(
            encoded(&datatypes::Value::Int32(-1000)),
            serde_json::Value::from(-1000i32)
        );
        assert_eq!(
            encoded(&datatypes::Value::Uint8(255)),
            serde_json::Value::from(255u8)
        );
        assert_eq!(
            encoded(&datatypes::Value::Uint16(65535)),
            serde_json::Value::from(65535u16)
        );
        assert_eq!(
            encoded(&datatypes::Value::Uint32(4294967295u32)),
            serde_json::Value::from(4294967295u32)
        );
        assert_eq!(
            encoded(&datatypes::Value::Uint64(18446744073709551615u64)),
            serde_json::Value::from(18446744073709551615u64)
        );

        // Test floats
        let f32_val = encoded(&datatypes::Value::Float32(3.14));
        assert!(f32_val.is_number());

        let f64_val = encoded(&datatypes::Value::Float64(2.718281828));
        assert!(f64_val.is_number());

        // Test String
        assert_eq!(
            encoded(&datatypes::Value::String("hello".to_string())),
            serde_json::Value::String("hello".to_string())
        );
    }

    #[test]
    fn empty_batch_produces_empty_object() {
        let layout = Arc::new(ColumnarJsonLayout::from_output_schema(output_schema(vec![
            (
                "a",
                OutputValueGetter::MessageByName {
                    source_name: Arc::from("src"),
                    column_name: Arc::from("a"),
                },
            ),
        ])));
        let agg = ColumnarAggregator::new(layout);
        let bytes = agg.finish_bytes().unwrap();
        assert_eq!(bytes, b"{}");
    }

    #[test]
    fn output_schema_controls_order_aliases_and_affiliate_columns() {
        let mut tuple = make_tuple(
            "src",
            &["b", "a"],
            &[datatypes::Value::Int64(2), datatypes::Value::Int64(1)],
        );
        tuple.add_affiliate_column(Arc::new("x".to_string()), datatypes::Value::Int64(9));
        let batch = RecordBatch::new(vec![tuple]).unwrap();
        let schema = output_schema(vec![
            (
                "a",
                OutputValueGetter::MessageByName {
                    source_name: Arc::from("src"),
                    column_name: Arc::from("a"),
                },
            ),
            (
                "x",
                OutputValueGetter::Affiliate {
                    column_name: Arc::from("x"),
                },
            ),
            (
                "renamed_b",
                OutputValueGetter::MessageByName {
                    source_name: Arc::from("src"),
                    column_name: Arc::from("b"),
                },
            ),
        ]);

        let bytes = encode_with_factory(schema_factory(schema), &batch);
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            r#"{"a":[1],"x":[9],"renamed_b":[2]}"#
        );
    }

    #[test]
    fn output_schema_missing_values_are_encoded_as_null() {
        let tuple = make_tuple("src", &["a"], &[datatypes::Value::Int64(1)]);
        let batch = RecordBatch::new(vec![tuple]).unwrap();
        let schema = output_schema(vec![(
            "missing",
            OutputValueGetter::MessageByName {
                source_name: Arc::from("src"),
                column_name: Arc::from("missing"),
            },
        )]);

        let bytes = encode_with_factory(schema_factory(schema), &batch);
        assert_eq!(String::from_utf8(bytes).unwrap(), r#"{"missing":[null]}"#);
    }

    #[test]
    fn output_schema_empty_delivery_stays_empty_object() {
        let batch = RecordBatch::new(vec![]).unwrap();
        let schema = output_schema(vec![(
            "a",
            OutputValueGetter::MessageByName {
                source_name: Arc::from("src"),
                column_name: Arc::from("a"),
            },
        )]);

        let bytes = encode_with_factory(schema_factory(schema), &batch);
        assert_eq!(String::from_utf8(bytes).unwrap(), "{}");
    }

    #[test]
    fn output_schema_rejects_duplicate_names() {
        let schema = output_schema(vec![
            (
                "a",
                OutputValueGetter::MessageByName {
                    source_name: Arc::from("src"),
                    column_name: Arc::from("a"),
                },
            ),
            (
                "a",
                OutputValueGetter::MessageByName {
                    source_name: Arc::from("src"),
                    column_name: Arc::from("b"),
                },
            ),
        ]);

        let err = match Arc::new(ColumnarJsonEncoder::new()).with_output_schema(schema) {
            Ok(_) => panic!("duplicate names should be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("duplicate output column `a`"));
    }
}
