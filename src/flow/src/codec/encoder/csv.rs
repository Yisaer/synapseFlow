//! CSV sink encoder backed by the planner-defined final [`OutputLayout`].

use super::{EncodeError, SinkEncoder, SinkEncoderFactory};
use crate::model::Collection;
use crate::planner::physical::output_layout::OutputLayout;
use crate::planner::sink::SinkEncoderConfig;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datatypes::{TimestampValue, Value};
use parking_lot::Mutex;
use std::fmt::Write as FmtWrite;
use std::io::Write as IoWrite;
use std::sync::Arc;

/// Factory for fixed-width UTF-8 CSV sink payloads.
pub struct CsvEncoder {
    id: String,
    delimiter: u8,
    header: bool,
    output_layout: Option<Arc<OutputLayout>>,
    header_bytes: Option<Bytes>,
}

impl CsvEncoder {
    /// Create a CSV encoder factory from sink configuration.
    pub fn new(id: impl Into<String>, config: &SinkEncoderConfig) -> Result<Self, EncodeError> {
        Ok(Self {
            id: id.into(),
            delimiter: config.csv_delimiter().map_err(EncodeError::Other)?,
            header: config.csv_header().map_err(EncodeError::Other)?,
            output_layout: None,
            header_bytes: None,
        })
    }
}

impl SinkEncoderFactory for CsvEncoder {
    fn id(&self) -> &str {
        &self.id
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        let output_layout = self.output_layout.clone().ok_or_else(|| {
            EncodeError::Other("CSV encoding requires the final output layout".to_string())
        })?;
        Ok(Box::new(CsvEncoderRuntime::new(
            self.delimiter,
            output_layout,
            self.header_bytes.clone(),
        )))
    }

    fn with_output_layout(
        self: Arc<Self>,
        output_layout: Arc<OutputLayout>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        let header_bytes = if self.header {
            Some(encode_header(self.delimiter, output_layout.as_ref())?)
        } else {
            None
        };
        Ok(Arc::new(Self {
            id: self.id.clone(),
            delimiter: self.delimiter,
            header: self.header,
            output_layout: Some(output_layout),
            header_bytes,
        }))
    }
}

#[derive(Default)]
struct ChunkSink {
    bytes: Mutex<Vec<u8>>,
}

impl ChunkSink {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.bytes.lock())
    }
}

impl IoWrite for ChunkSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct CsvEncoderRuntime {
    delimiter: u8,
    output_layout: Arc<OutputLayout>,
    header_bytes: Option<Bytes>,
    writer: csv::Writer<ChunkSink>,
    field_scratch: Vec<u8>,
    text_scratch: String,
}

impl CsvEncoderRuntime {
    fn new(delimiter: u8, output_layout: Arc<OutputLayout>, header_bytes: Option<Bytes>) -> Self {
        Self {
            delimiter,
            output_layout,
            header_bytes,
            writer: new_chunk_writer(delimiter),
            field_scratch: Vec::new(),
            text_scratch: String::new(),
        }
    }

    fn reset_writer(&mut self) {
        self.writer = new_chunk_writer(self.delimiter);
        self.field_scratch.clear();
        self.text_scratch.clear();
    }

    fn take_chunk(&mut self) -> Result<Option<Bytes>, EncodeError> {
        self.writer
            .flush()
            .map_err(|err| EncodeError::Other(format!("failed to flush CSV output: {err}")))?;
        let bytes = self.writer.get_ref().take();
        if bytes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(bytes.into()))
        }
    }
}

impl SinkEncoder for CsvEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
        self.reset_writer();
        Ok(self.header_bytes.clone())
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<Bytes>, EncodeError> {
        for tuple in record.rows() {
            for column in self.output_layout.columns.iter() {
                let value = column
                    .value_ref
                    .resolve(tuple)
                    .map_err(EncodeError::Other)?;
                write_value(
                    &mut self.writer,
                    &mut self.field_scratch,
                    &mut self.text_scratch,
                    value,
                )?;
            }
            self.writer
                .write_record(std::iter::empty::<&[u8]>())
                .map_err(csv_error)?;
        }
        self.take_chunk()
    }

    fn finish_delivery(&mut self) -> Result<Option<Bytes>, EncodeError> {
        self.take_chunk()
    }

    fn abort_delivery(&mut self) {
        self.reset_writer();
    }
}

fn new_chunk_writer(delimiter: u8) -> csv::Writer<ChunkSink> {
    csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .from_writer(ChunkSink::default())
}

fn encode_header(delimiter: u8, output_layout: &OutputLayout) -> Result<Bytes, EncodeError> {
    let mut writer = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .from_writer(Vec::new());
    writer
        .write_record(
            output_layout
                .columns
                .iter()
                .map(|column| column.name.as_bytes()),
        )
        .map_err(csv_error)?;
    writer
        .into_inner()
        .map(Bytes::from)
        .map_err(|err| EncodeError::Other(format!("failed to finalize CSV header: {err}")))
}

fn csv_error(err: csv::Error) -> EncodeError {
    EncodeError::Other(format!("CSV serialization error: {err}"))
}

fn write_value(
    writer: &mut csv::Writer<ChunkSink>,
    field_scratch: &mut Vec<u8>,
    text_scratch: &mut String,
    value: &Value,
) -> Result<(), EncodeError> {
    match value {
        Value::Null => writer.write_field([]).map_err(csv_error),
        Value::Bool(value) => {
            let field: &[u8] = if *value { b"true" } else { b"false" };
            writer.write_field(field).map_err(csv_error)
        }
        Value::String(value) => writer.write_field(value.as_bytes()).map_err(csv_error),
        Value::Bytes(value) => {
            text_scratch.clear();
            BASE64_STANDARD.encode_string(value.as_ref(), text_scratch);
            writer
                .write_field(text_scratch.as_bytes())
                .map_err(csv_error)
        }
        Value::Float32(value) => write_float32(writer, *value),
        Value::Float64(value) => write_float64(writer, *value),
        Value::Int8(value) => write_integer(writer, *value),
        Value::Int16(value) => write_integer(writer, *value),
        Value::Int32(value) => write_integer(writer, *value),
        Value::Int64(value) => write_integer(writer, *value),
        Value::Uint8(value) => write_integer(writer, *value),
        Value::Uint16(value) => write_integer(writer, *value),
        Value::Uint32(value) => write_integer(writer, *value),
        Value::Uint64(value) => write_integer(writer, *value),
        Value::Timestamp(value) => {
            write_timestamp(text_scratch, *value)?;
            writer
                .write_field(text_scratch.as_bytes())
                .map_err(csv_error)
        }
        Value::Struct(_) | Value::List(_) => {
            field_scratch.clear();
            write_compact_json(field_scratch, text_scratch, value)?;
            writer.write_field(field_scratch).map_err(csv_error)
        }
    }
}

fn write_integer<I: itoa::Integer>(
    writer: &mut csv::Writer<ChunkSink>,
    value: I,
) -> Result<(), EncodeError> {
    let mut buffer = itoa::Buffer::new();
    writer.write_field(buffer.format(value)).map_err(csv_error)
}

fn write_float32(writer: &mut csv::Writer<ChunkSink>, value: f32) -> Result<(), EncodeError> {
    if value.is_nan() {
        return writer.write_field("NaN").map_err(csv_error);
    }
    if value == f32::INFINITY {
        return writer.write_field("Infinity").map_err(csv_error);
    }
    if value == f32::NEG_INFINITY {
        return writer.write_field("-Infinity").map_err(csv_error);
    }
    let mut buffer = ryu::Buffer::new();
    writer.write_field(buffer.format(value)).map_err(csv_error)
}

fn write_float64(writer: &mut csv::Writer<ChunkSink>, value: f64) -> Result<(), EncodeError> {
    if value.is_nan() {
        return writer.write_field("NaN").map_err(csv_error);
    }
    if value == f64::INFINITY {
        return writer.write_field("Infinity").map_err(csv_error);
    }
    if value == f64::NEG_INFINITY {
        return writer.write_field("-Infinity").map_err(csv_error);
    }
    let mut buffer = ryu::Buffer::new();
    writer.write_field(buffer.format(value)).map_err(csv_error)
}

fn write_timestamp(output: &mut String, value: TimestampValue) -> Result<(), EncodeError> {
    output.clear();
    let secs = value.epoch_micros().div_euclid(1_000_000);
    let micros = value.epoch_micros().rem_euclid(1_000_000) as u32;
    let timestamp = DateTime::<Utc>::from_timestamp(secs, micros * 1_000).ok_or_else(|| {
        EncodeError::Other(format!(
            "timestamp epoch micros {} is out of range",
            value.epoch_micros()
        ))
    })?;
    write!(output, "{}", timestamp.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
        .map_err(|err| EncodeError::Other(format!("failed to format timestamp: {err}")))
}

fn write_compact_json(
    output: &mut Vec<u8>,
    text_scratch: &mut String,
    value: &Value,
) -> Result<(), EncodeError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => {
            let field: &[u8] = if *value { b"true" } else { b"false" };
            output.extend_from_slice(field);
        }
        Value::String(value) => write_json_string(output, value)?,
        Value::Bytes(value) => {
            text_scratch.clear();
            BASE64_STANDARD.encode_string(value.as_ref(), text_scratch);
            write_json_string(output, text_scratch)?;
        }
        Value::Float32(value) => write_json_float(output, *value),
        Value::Float64(value) => write_json_float(output, *value),
        Value::Int8(value) => write_json_integer(output, *value),
        Value::Int16(value) => write_json_integer(output, *value),
        Value::Int32(value) => write_json_integer(output, *value),
        Value::Int64(value) => write_json_integer(output, *value),
        Value::Uint8(value) => write_json_integer(output, *value),
        Value::Uint16(value) => write_json_integer(output, *value),
        Value::Uint32(value) => write_json_integer(output, *value),
        Value::Uint64(value) => write_json_integer(output, *value),
        Value::Timestamp(value) => {
            write_timestamp(text_scratch, *value)?;
            write_json_string(output, text_scratch)?;
        }
        Value::Struct(value) => {
            if value.fields().fields().len() != value.items().len() {
                return Err(EncodeError::Other(
                    "struct field/value width mismatch during CSV encoding".to_string(),
                ));
            }
            output.push(b'{');
            for (index, (field, item)) in value
                .fields()
                .fields()
                .iter()
                .zip(value.items())
                .enumerate()
            {
                if index > 0 {
                    output.push(b',');
                }
                write_json_string(output, field.name())?;
                output.push(b':');
                write_compact_json(output, text_scratch, item)?;
            }
            output.push(b'}');
        }
        Value::List(value) => {
            output.push(b'[');
            for (index, item) in value.items().iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_compact_json(output, text_scratch, item)?;
            }
            output.push(b']');
        }
    }
    Ok(())
}

fn write_json_string(output: &mut Vec<u8>, value: &str) -> Result<(), EncodeError> {
    serde_json::to_writer(output, value).map_err(EncodeError::Serialization)
}

fn write_json_integer<I: itoa::Integer>(output: &mut Vec<u8>, value: I) {
    let mut buffer = itoa::Buffer::new();
    output.extend_from_slice(buffer.format(value).as_bytes());
}

fn write_json_float<F: ryu::Float>(output: &mut Vec<u8>, value: F) {
    let mut buffer = ryu::Buffer::new();
    let formatted = buffer.format(value);
    if matches!(formatted, "NaN" | "inf" | "-inf") {
        output.extend_from_slice(b"null");
    } else {
        output.extend_from_slice(formatted.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Message, RecordBatch, Tuple};
    use crate::planner::physical::output_layout::{OutputColumnLayout, OutputValueRef};
    use datatypes::{
        ConcreteDatatype, Int64Type, ListType, ListValue, StringType, StructField, StructType,
        StructValue,
    };
    use serde_json::{Map as JsonMap, Value as JsonValue};

    fn encode_delivery(
        config: SinkEncoderConfig,
        output_layout: OutputLayout,
        record: &RecordBatch,
    ) -> Vec<u8> {
        let factory = Arc::new(CsvEncoder::new("csv", &config).expect("CSV factory"));
        let factory = factory
            .with_output_layout(Arc::new(output_layout))
            .expect("attach output layout");
        let mut runtime = factory.start_encoder().expect("CSV runtime");
        let mut payload = Vec::new();
        for chunk in [
            runtime.begin_delivery().expect("begin delivery"),
            runtime.append(record).expect("append record"),
            runtime.finish_delivery().expect("finish delivery"),
        ]
        .into_iter()
        .flatten()
        {
            payload.extend_from_slice(&chunk);
        }
        payload
    }

    fn column(
        name: &str,
        data_type: ConcreteDatatype,
        value_ref: OutputValueRef,
    ) -> OutputColumnLayout {
        OutputColumnLayout {
            name: Arc::from(name),
            data_type,
            value_ref,
        }
    }

    // coverage-covers: sink.encoder.csv_output
    #[test]
    fn csv_encoder_uses_fixed_layout_for_header_values_and_escaping() {
        let message = Arc::new(Message::new(
            "vehicle",
            vec![Arc::from("source_name")],
            vec![Arc::new(Value::String("v1,\"fast\"\nnext".to_string()))],
        ));
        let mut tuple = Tuple::new(vec![message]);
        tuple.add_affiliate_columns([(Arc::new("computed".to_string()), Value::Int64(42))]);
        let batch = RecordBatch::new(vec![tuple]).expect("record batch");
        let layout = OutputLayout::new(vec![
            column(
                "vin",
                ConcreteDatatype::String(StringType),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            ),
            column(
                "answer",
                ConcreteDatatype::Int64(Int64Type),
                OutputValueRef::Affiliate { affiliate_index: 0 },
            ),
            column("missing", ConcreteDatatype::Null, OutputValueRef::Null),
        ]);
        let mut props = JsonMap::new();
        props.insert("header".to_string(), JsonValue::Bool(true));
        let payload = encode_delivery(SinkEncoderConfig::new("csv", props), layout, &batch);

        assert_eq!(
            String::from_utf8(payload).expect("UTF-8 CSV"),
            "vin,answer,missing\n\"v1,\"\"fast\"\"\nnext\",42,\n"
        );
    }

    #[test]
    fn csv_encoder_formats_bytes_timestamps_composites_and_special_floats() {
        let struct_type = StructType::new(Arc::new(vec![
            StructField::new(
                "label".to_string(),
                ConcreteDatatype::String(StringType),
                false,
            ),
            StructField::new(
                "items".to_string(),
                ConcreteDatatype::List(ListType::new(Arc::new(ConcreteDatatype::Int64(Int64Type)))),
                true,
            ),
        ]));
        let composite = Value::Struct(StructValue::new(
            vec![
                Value::String("a,b".to_string()),
                Value::List(ListValue::new(
                    vec![Value::Int64(1), Value::Null],
                    Arc::new(ConcreteDatatype::Int64(Int64Type)),
                )),
            ],
            struct_type.clone(),
        ));
        let timestamp =
            TimestampValue::parse_rfc3339("2026-05-08T10:20:30.123456Z").expect("valid timestamp");
        let values = vec![
            Value::Bytes(Bytes::from_static(b"hello")),
            Value::Timestamp(timestamp),
            composite,
            Value::Float64(f64::NAN),
            Value::Float64(f64::INFINITY),
            Value::Float64(f64::NEG_INFINITY),
        ];
        let keys = (0..values.len())
            .map(|index| Arc::from(format!("c{index}")))
            .collect();
        let message = Arc::new(Message::new(
            "values",
            keys,
            values.into_iter().map(Arc::new).collect(),
        ));
        let batch = RecordBatch::new(vec![Tuple::new(vec![message])]).expect("record batch");
        let layout = OutputLayout::new(vec![
            column(
                "bytes",
                ConcreteDatatype::Bytes(datatypes::BytesType),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 0,
                },
            ),
            column(
                "timestamp",
                ConcreteDatatype::Timestamp(datatypes::TimestampType),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 1,
                },
            ),
            column(
                "struct",
                ConcreteDatatype::Struct(struct_type),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 2,
                },
            ),
            column(
                "nan",
                ConcreteDatatype::Float64(datatypes::Float64Type),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 3,
                },
            ),
            column(
                "positive_infinity",
                ConcreteDatatype::Float64(datatypes::Float64Type),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 4,
                },
            ),
            column(
                "negative_infinity",
                ConcreteDatatype::Float64(datatypes::Float64Type),
                OutputValueRef::Message {
                    message_index: 0,
                    value_index: 5,
                },
            ),
        ]);
        let payload = encode_delivery(SinkEncoderConfig::csv(), layout, &batch);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(payload.as_slice());
        let record = reader
            .records()
            .next()
            .expect("one CSV row")
            .expect("valid CSV row");

        assert_eq!(record.get(0), Some("aGVsbG8="));
        assert_eq!(record.get(1), Some("2026-05-08T10:20:30.123456Z"));
        assert_eq!(record.get(2), Some(r#"{"label":"a,b","items":[1,null]}"#));
        assert_eq!(record.get(3), Some("NaN"));
        assert_eq!(record.get(4), Some("Infinity"));
        assert_eq!(record.get(5), Some("-Infinity"));
        assert!(reader.records().next().is_none());
    }

    #[test]
    fn csv_encoder_streams_multiple_appends_without_repeating_header() {
        let layout = Arc::new(OutputLayout::new(vec![column(
            "value",
            ConcreteDatatype::Int64(Int64Type),
            OutputValueRef::Message {
                message_index: 0,
                value_index: 0,
            },
        )]));
        let mut props = JsonMap::new();
        props.insert("header".to_string(), JsonValue::Bool(true));
        let factory = Arc::new(
            CsvEncoder::new("csv", &SinkEncoderConfig::new("csv", props)).expect("CSV factory"),
        )
        .with_output_layout(layout)
        .expect("attach output layout");
        let mut runtime = factory.start_encoder().expect("CSV runtime");
        let batch = |value| {
            RecordBatch::new(vec![Tuple::new(vec![Arc::new(Message::new(
                "stream",
                vec![Arc::from("value")],
                vec![Arc::new(Value::Int64(value))],
            ))])])
            .expect("record batch")
        };
        let mut payload = runtime
            .begin_delivery()
            .expect("begin delivery")
            .expect("header")
            .to_vec();
        payload.extend_from_slice(
            &runtime
                .append(&batch(1))
                .expect("first append")
                .expect("first chunk"),
        );
        payload.extend_from_slice(
            &runtime
                .append(&batch(2))
                .expect("second append")
                .expect("second chunk"),
        );
        assert!(runtime
            .finish_delivery()
            .expect("finish delivery")
            .is_none());

        assert_eq!(
            String::from_utf8(payload).expect("UTF-8 CSV"),
            "value\n1\n2\n"
        );
    }
}
