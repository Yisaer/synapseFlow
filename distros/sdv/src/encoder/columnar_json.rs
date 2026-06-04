use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use flow::codec::encoder::{EncodeError, SinkEncoder, SinkEncoderFactory};
use flow::model::Collection;
use flow::planner::physical::output_schema::OutputSchema;
use serde_json::Value as JsonValue;

/// Columnar JSON encoder: batches rows into a single JSON object keyed by column name,
/// with array values per column. Does not depend on schema; uses the columns present
/// in incoming tuples/messages. Supports streaming accumulation with incremental JSON writing.
pub struct ColumnarJsonEncoder;

impl Default for ColumnarJsonEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarJsonEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl SinkEncoderFactory for ColumnarJsonEncoder {
    fn id(&self) -> &str {
        "columnar_json"
    }

    fn start_encoder(&self) -> Result<Box<dyn SinkEncoder>, EncodeError> {
        Ok(Box::new(ColumnarJsonEncoderRuntime::default()))
    }

    fn with_output_schema(
        self: Arc<Self>,
        _output_schema: Arc<OutputSchema>,
    ) -> Result<Arc<dyn SinkEncoderFactory>, EncodeError> {
        Ok(self)
    }
}

#[derive(Default)]
struct ColumnarJsonEncoderRuntime {
    agg: ColumnarAggregator,
}

impl SinkEncoder for ColumnarJsonEncoderRuntime {
    fn begin_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        self.agg = ColumnarAggregator::default();
        Ok(None)
    }

    fn append(&mut self, record: &dyn Collection) -> Result<Option<bytes::Bytes>, EncodeError> {
        for tuple in record.rows() {
            self.agg.append_tuple(tuple)?;
        }
        Ok(None)
    }

    fn finish_delivery(&mut self) -> Result<Option<bytes::Bytes>, EncodeError> {
        let agg = std::mem::take(&mut self.agg);
        agg.finish_bytes().map(bytes::Bytes::from).map(Some)
    }

    fn abort_delivery(&mut self) {
        self.agg = ColumnarAggregator::default();
    }
}

#[derive(Default)]
struct ColumnarAggregator {
    columns: HashMap<Arc<str>, ColumnBuf>,
    order: Vec<Arc<str>>,
}

struct ColumnBuf {
    buf: Vec<u8>,
    first: bool,
}

impl ColumnarAggregator {
    fn append_tuple(&mut self, tuple: &flow::model::Tuple) -> Result<(), EncodeError> {
        // Only aggregate source messages; affiliate columns (if any) are skipped here.
        for msg in tuple.messages.iter() {
            for (name, value) in msg.entries() {
                let key: Arc<str> = Arc::<str>::from(name);
                let entry = self.columns.entry(key.clone()).or_insert_with(|| {
                    self.order.push(key.clone());
                    ColumnBuf {
                        buf: vec![b'['],
                        first: true,
                    }
                });
                if !entry.first {
                    entry.buf.push(b',');
                }
                entry.first = false;
                serde_json::to_writer(&mut entry.buf, &to_json_value(value))
                    .map_err(EncodeError::Serialization)?;
            }
        }
        Ok(())
    }

    fn finish_bytes(mut self) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        out.push(b'{');
        for (idx, key) in self.order.iter().enumerate() {
            if idx > 0 {
                out.push(b',');
            }
            serde_json::to_writer(&mut out, key.as_ref()).map_err(EncodeError::Serialization)?;
            out.push(b':');
            if let Some(mut col) = self.columns.remove(key) {
                col.buf.push(b']');
                out.extend_from_slice(&col.buf);
            } else {
                out.extend_from_slice(b"[]");
            }
        }
        out.push(b'}');
        Ok(out)
    }
}

fn to_json_value(val: &datatypes::Value) -> JsonValue {
    use datatypes::Value::*;
    match val {
        Null => JsonValue::Null,
        Bool(v) => JsonValue::Bool(*v),
        Int8(v) => JsonValue::from(*v),
        Int16(v) => JsonValue::from(*v),
        Int32(v) => JsonValue::from(*v),
        Int64(v) => JsonValue::from(*v),
        Uint8(v) => JsonValue::from(*v),
        Uint16(v) => JsonValue::from(*v),
        Uint32(v) => JsonValue::from(*v),
        Uint64(v) => JsonValue::from(*v),
        Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Float64(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        String(s) => JsonValue::String(s.clone()),
        Bytes(bytes) => JsonValue::String(BASE64_STANDARD.encode(bytes.as_ref())),
        Timestamp(ts) => ts
            .to_rfc3339_utc()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        List(list) => {
            let items = list.items().iter().map(to_json_value).collect::<Vec<_>>();
            JsonValue::Array(items)
        }
        Struct(strct) => {
            let mut map = serde_json::Map::new();
            for (field, item) in strct.fields().fields().iter().zip(strct.items().iter()) {
                map.insert(field.name().to_string(), to_json_value(item));
            }
            JsonValue::Object(map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::model::{Message, RecordBatch, Tuple};

    fn make_tuple(source: &str, keys: &[&str], vals: &[datatypes::Value]) -> Tuple {
        let keys_arc: Vec<Arc<str>> = keys.iter().map(|k| Arc::<str>::from(*k)).collect();
        let vals_arc: Vec<Arc<datatypes::Value>> = vals.iter().cloned().map(Arc::new).collect();
        let msg = Arc::new(Message::new(Arc::<str>::from(source), keys_arc, vals_arc));
        Tuple::new(vec![msg])
    }

    fn encode_batch(enc: &ColumnarJsonEncoder, batch: &RecordBatch) -> Vec<u8> {
        let mut runtime = enc.start_encoder().unwrap();
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
        let enc = ColumnarJsonEncoder::new();
        assert!(enc.start_encoder().is_ok());
    }

    #[test]
    fn runtime_encoder_works() {
        let enc = ColumnarJsonEncoder::new();
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
    fn to_json_value_handles_all_types() {
        // Test Null
        assert_eq!(to_json_value(&datatypes::Value::Null), JsonValue::Null);

        // Test Bool
        assert_eq!(
            to_json_value(&datatypes::Value::Bool(true)),
            JsonValue::Bool(true)
        );

        // Test various int types
        assert_eq!(
            to_json_value(&datatypes::Value::Int8(-1)),
            JsonValue::from(-1i8)
        );
        assert_eq!(
            to_json_value(&datatypes::Value::Int16(-100)),
            JsonValue::from(-100i16)
        );
        assert_eq!(
            to_json_value(&datatypes::Value::Int32(-1000)),
            JsonValue::from(-1000i32)
        );
        assert_eq!(
            to_json_value(&datatypes::Value::Uint8(255)),
            JsonValue::from(255u8)
        );
        assert_eq!(
            to_json_value(&datatypes::Value::Uint16(65535)),
            JsonValue::from(65535u16)
        );
        assert_eq!(
            to_json_value(&datatypes::Value::Uint32(4294967295u32)),
            JsonValue::from(4294967295u32)
        );
        assert_eq!(
            to_json_value(&datatypes::Value::Uint64(18446744073709551615u64)),
            JsonValue::from(18446744073709551615u64)
        );

        // Test floats
        let f32_val = to_json_value(&datatypes::Value::Float32(3.14));
        assert!(f32_val.is_number());

        let f64_val = to_json_value(&datatypes::Value::Float64(2.718281828));
        assert!(f64_val.is_number());

        // Test String
        assert_eq!(
            to_json_value(&datatypes::Value::String("hello".to_string())),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn empty_batch_produces_empty_object() {
        let agg = ColumnarAggregator::default();
        let bytes = agg.finish_bytes().unwrap();
        assert_eq!(bytes, b"{}");
    }
}
