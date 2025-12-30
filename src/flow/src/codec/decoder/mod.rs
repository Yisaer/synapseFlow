//! Decoder abstractions for turning raw bytes into RecordBatch collections.

use crate::model::{CollectionError, Message, RecordBatch, Tuple};
use crate::planner::decode_projection::{DecodeProjection, ProjectionNode};
use datatypes::{
    ConcreteDatatype, ListType, ListValue, Schema, StructField, StructType, StructValue, Value,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;

/// Errors that can occur while decoding payloads.
#[derive(thiserror::Error, Debug)]
pub enum CodecError {
    /// Payload was not valid UTF-8 (used by the simple string decoder).
    #[error("invalid utf8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// Payload was not valid JSON.
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    /// RecordBatch construction failed.
    #[error("collection error: {0}")]
    Collection(#[from] CollectionError),
    /// Custom decoder-specific failure.
    #[error("{0}")]
    Other(String),
}

/// Trait implemented by all record decoders.
pub trait RecordDecoder: Send + Sync + 'static {
    /// Convert raw bytes into a RecordBatch, optionally applying a decode projection.
    ///
    /// Projection semantics:
    /// - `None`: decode all schema columns.
    /// - `Some(p)`: only decode columns present in `p`; missing columns are treated as `NULL`.
    ///   Nested projection nodes (struct fields / list indexes) are applied when present.
    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError>;

    /// Convert raw bytes into a RecordBatch (no projection).
    fn decode(&self, payload: &[u8]) -> Result<RecordBatch, CodecError> {
        self.decode_with_projection(payload, None)
    }
}

/// Decoder that converts JSON documents (object or array) into a RecordBatch.
pub struct JsonDecoder {
    stream_name: Arc<str>,
    schema: Arc<Schema>,
    schema_keys: Arc<[Arc<str>]>,
    #[allow(dead_code)]
    props: JsonMap<String, JsonValue>,
}

impl JsonDecoder {
    pub fn new(
        stream_name: impl Into<String>,
        schema: Arc<Schema>,
        props: JsonMap<String, JsonValue>,
    ) -> Self {
        let stream_name: Arc<str> = Arc::<str>::from(stream_name.into());
        let schema_keys: Arc<[Arc<str>]> = Arc::from(
            schema
                .column_schemas()
                .iter()
                .map(|col| Arc::<str>::from(col.name.as_str()))
                .collect::<Vec<_>>(),
        );
        Self {
            stream_name,
            schema,
            schema_keys,
            props,
        }
    }

    fn decode_value_with_decode_projection(
        &self,
        json: JsonValue,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        match json {
            JsonValue::Object(map) => {
                self.build_from_object_rows_with_decode_projection(vec![map], decode_projection)
            }
            JsonValue::Array(items) => {
                self.decode_array_with_decode_projection(items, decode_projection)
            }
            other => Err(CodecError::Other(format!(
                "JSON root must be object or array, got {other:?}"
            ))),
        }
    }

    fn decode_array_with_decode_projection(
        &self,
        items: Vec<JsonValue>,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        if items.is_empty() {
            return Ok(RecordBatch::empty());
        }

        if !items.iter().all(|v| v.is_object()) {
            return Err(CodecError::Other(
                "JSON array must contain only objects".to_string(),
            ));
        }

        let rows: Vec<JsonMap<String, JsonValue>> = items
            .into_iter()
            .map(|v| match v {
                JsonValue::Object(map) => map,
                _ => unreachable!("validated object rows"),
            })
            .collect();
        self.build_from_object_rows_with_decode_projection(rows, decode_projection)
    }

    fn build_from_object_rows_with_decode_projection(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        if rows.is_empty() {
            return Ok(RecordBatch::empty());
        }

        let tuples =
            self.build_tuples_from_object_rows_with_decode_projection(rows, decode_projection)?;
        Ok(RecordBatch::new(tuples)?)
    }

    fn build_tuples_from_object_rows_with_decode_projection(
        &self,
        rows: Vec<JsonMap<String, JsonValue>>,
        decode_projection: Option<&DecodeProjection>,
    ) -> Result<Vec<Tuple>, CodecError> {
        let mut tuples = Vec::with_capacity(rows.len());
        for mut row in rows {
            let mut values = Vec::with_capacity(self.schema_keys.len());
            for column in self.schema.column_schemas().iter() {
                let projection_node =
                    decode_projection.and_then(|p| p.column(column.name.as_str()));
                let value = match decode_projection {
                    Some(_) => {
                        if let Some(node) = projection_node {
                            row.remove(&column.name)
                                .map(|json| {
                                    json_to_value_with_datatype_and_projection(
                                        &json,
                                        &column.data_type,
                                        Some(node),
                                    )
                                })
                                .unwrap_or(Value::Null)
                        } else {
                            let _ = row.remove(&column.name);
                            Value::Null
                        }
                    }
                    None => row
                        .remove(&column.name)
                        .map(|json| {
                            json_to_value_with_datatype_and_projection(
                                &json,
                                &column.data_type,
                                None,
                            )
                        })
                        .unwrap_or(Value::Null),
                };
                values.push(Arc::new(value));
            }
            drop(row);
            let message = Arc::new(Message::new_shared_keys(
                Arc::clone(&self.stream_name),
                Arc::clone(&self.schema_keys),
                values,
            ));
            tuples.push(Tuple::new(vec![message]));
        }
        Ok(tuples)
    }
}

impl RecordDecoder for JsonDecoder {
    fn decode_with_projection(
        &self,
        payload: &[u8],
        projection: Option<&DecodeProjection>,
    ) -> Result<RecordBatch, CodecError> {
        let json = serde_json::from_slice(payload)?;
        self.decode_value_with_decode_projection(json, projection)
    }
}

fn json_to_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(u) = n.as_u64() {
                Value::Uint64(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        JsonValue::String(s) => Value::String(s.clone()),
        JsonValue::Array(items) => {
            let converted: Vec<Value> = items.iter().map(json_to_value).collect();
            let element_type = converted
                .iter()
                .find(|v| !matches!(v, Value::Null))
                .map(Value::datatype)
                .unwrap_or(ConcreteDatatype::Null);
            Value::List(ListValue::new(converted, Arc::new(element_type)))
        }
        JsonValue::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            let mut values = Vec::with_capacity(map.len());

            for (key, val) in map {
                let converted = json_to_value(val);
                let datatype = converted.datatype();
                fields.push(StructField::new(key.clone(), datatype, true));
                values.push(converted);
            }

            Value::Struct(StructValue::new(values, StructType::new(Arc::new(fields))))
        }
    }
}

fn json_to_value_with_datatype(value: &JsonValue, datatype: &ConcreteDatatype) -> Value {
    match datatype {
        ConcreteDatatype::Null => Value::Null,
        ConcreteDatatype::Bool(_) => match value {
            JsonValue::Bool(b) => Value::Bool(*b),
            _ => Value::Null,
        },
        ConcreteDatatype::Int64(_) => match value {
            JsonValue::Number(n) => n.as_i64().map(Value::Int64).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ConcreteDatatype::Uint64(_) => match value {
            JsonValue::Number(n) => n.as_u64().map(Value::Uint64).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ConcreteDatatype::Int8(_)
        | ConcreteDatatype::Int16(_)
        | ConcreteDatatype::Int32(_)
        | ConcreteDatatype::Uint8(_)
        | ConcreteDatatype::Uint16(_)
        | ConcreteDatatype::Uint32(_)
        | ConcreteDatatype::Float32(_)
        | ConcreteDatatype::Float64(_)
        | ConcreteDatatype::String(_) => json_to_value(value),
        ConcreteDatatype::List(list_type) => json_to_list_value_with_datatype(value, list_type),
        ConcreteDatatype::Struct(struct_type) => {
            json_to_struct_value_with_datatype(value, struct_type)
        }
    }
}

fn json_to_value_with_datatype_and_projection(
    value: &JsonValue,
    datatype: &ConcreteDatatype,
    projection: Option<&ProjectionNode>,
) -> Value {
    match datatype {
        ConcreteDatatype::Null => Value::Null,
        ConcreteDatatype::Bool(_) => match value {
            JsonValue::Bool(b) => Value::Bool(*b),
            _ => Value::Null,
        },
        ConcreteDatatype::Int64(_) => match value {
            JsonValue::Number(n) => n.as_i64().map(Value::Int64).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ConcreteDatatype::Uint64(_) => match value {
            JsonValue::Number(n) => n.as_u64().map(Value::Uint64).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ConcreteDatatype::Int8(_)
        | ConcreteDatatype::Int16(_)
        | ConcreteDatatype::Int32(_)
        | ConcreteDatatype::Uint8(_)
        | ConcreteDatatype::Uint16(_)
        | ConcreteDatatype::Uint32(_)
        | ConcreteDatatype::Float32(_)
        | ConcreteDatatype::Float64(_)
        | ConcreteDatatype::String(_) => json_to_value(value),
        ConcreteDatatype::List(list_type) => {
            json_to_list_value_with_datatype_and_projection(value, list_type, projection)
        }
        ConcreteDatatype::Struct(struct_type) => {
            json_to_struct_value_with_datatype_and_projection(value, struct_type, projection)
        }
    }
}

fn json_to_list_value_with_datatype(value: &JsonValue, list_type: &ListType) -> Value {
    let JsonValue::Array(items) = value else {
        return Value::Null;
    };

    let element_type = list_type.item_type();
    let converted: Vec<Value> = items
        .iter()
        .map(|item| json_to_value_with_datatype(item, element_type))
        .collect();
    Value::List(ListValue::new(converted, Arc::new(element_type.clone())))
}

fn json_to_list_value_with_datatype_and_projection(
    value: &JsonValue,
    list_type: &ListType,
    projection: Option<&ProjectionNode>,
) -> Value {
    let JsonValue::Array(items) = value else {
        return Value::Null;
    };

    let element_type = list_type.item_type();
    match projection {
        Some(ProjectionNode::List { indexes, element }) => match indexes {
            crate::planner::decode_projection::ListIndexSelection::Indexes(required) => {
                let mut converted = Vec::with_capacity(items.len());
                for (idx, item) in items.iter().enumerate() {
                    if required.contains(&idx) {
                        converted.push(json_to_value_with_datatype_and_projection(
                            item,
                            element_type,
                            Some(element.as_ref()),
                        ));
                    } else {
                        converted.push(Value::Null);
                    }
                }
                Value::List(ListValue::new(converted, Arc::new(element_type.clone())))
            }
            crate::planner::decode_projection::ListIndexSelection::All => {
                let converted: Vec<Value> = items
                    .iter()
                    .map(|item| {
                        json_to_value_with_datatype_and_projection(
                            item,
                            element_type,
                            Some(element.as_ref()),
                        )
                    })
                    .collect();
                Value::List(ListValue::new(converted, Arc::new(element_type.clone())))
            }
        },
        _ => json_to_list_value_with_datatype(value, list_type),
    }
}

fn json_to_struct_value_with_datatype(value: &JsonValue, struct_type: &StructType) -> Value {
    let JsonValue::Object(map) = value else {
        return Value::Null;
    };

    let values: Vec<Value> = struct_type
        .fields()
        .iter()
        .map(|field| {
            map.get(field.name())
                .map(|v| json_to_value_with_datatype(v, field.data_type()))
                .unwrap_or(Value::Null)
        })
        .collect();

    Value::Struct(StructValue::new(values, struct_type.clone()))
}

fn json_to_struct_value_with_datatype_and_projection(
    value: &JsonValue,
    struct_type: &StructType,
    projection: Option<&ProjectionNode>,
) -> Value {
    let JsonValue::Object(map) = value else {
        return Value::Null;
    };

    let projection_fields = match projection {
        Some(ProjectionNode::Struct(fields)) => Some(fields),
        _ => None,
    };

    let values: Vec<Value> = struct_type
        .fields()
        .iter()
        .map(|field| {
            let child_proj = projection_fields.and_then(|fields| fields.get(field.name()));
            map.get(field.name())
                .map(|v| {
                    json_to_value_with_datatype_and_projection(v, field.data_type(), child_proj)
                })
                .unwrap_or(Value::Null)
        })
        .collect();

    Value::Struct(StructValue::new(values, struct_type.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{
        ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, StructField, StructType,
        Value,
    };
    use serde_json::Map as JsonMap;

    fn decode_one(decoder: &JsonDecoder, payload: &[u8]) -> Tuple {
        let mut rows = decoder.decode(payload).expect("decode batch").into_rows();
        assert_eq!(rows.len(), 1, "expected exactly one decoded tuple");
        rows.pop().expect("one row")
    }

    #[test]
    fn json_decoder_decodes_single_tuple() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "orders".to_string(),
                "amount".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "orders".to_string(),
                "status".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"amount":10,"status":"ok"}"#.as_ref();
        let tuple = decode_one(&decoder, payload);

        let mut columns: Vec<_> = tuple
            .entries()
            .into_iter()
            .map(|((src, col), _)| (src.to_string(), col.to_string()))
            .collect();
        columns.sort();
        assert_eq!(
            columns,
            vec![
                ("orders".to_string(), "amount".to_string()),
                ("orders".to_string(), "status".to_string())
            ]
        );
        assert_eq!(
            tuple.value_by_name("orders", "amount"),
            Some(&Value::Int64(10))
        );
        assert_eq!(
            tuple.value_by_name("orders", "status"),
            Some(&Value::String("ok".to_string()))
        );
    }

    #[test]
    fn json_decoder_ignores_unknown_fields() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "orders".to_string(),
                "amount".to_string(),
                ConcreteDatatype::Int64(Int64Type),
            ),
            ColumnSchema::new(
                "orders".to_string(),
                "status".to_string(),
                ConcreteDatatype::String(StringType),
            ),
        ]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"amount":10,"status":"ok","extra":42,"extra2":"ignored"}"#.as_ref();

        let batch = decoder.decode(payload).expect("decode batch");
        let tuples = batch.into_rows();
        assert_eq!(tuples.len(), 1);
        let tuple = &tuples[0];

        let mut columns: Vec<_> = tuple
            .entries()
            .into_iter()
            .map(|((src, col), _)| (src.to_string(), col.to_string()))
            .collect();
        columns.sort();
        assert_eq!(
            columns,
            vec![
                ("orders".to_string(), "amount".to_string()),
                ("orders".to_string(), "status".to_string())
            ]
        );

        assert_eq!(tuple.value_by_name("orders", "extra"), None);
        assert_eq!(tuple.value_by_name("orders", "extra2"), None);
    }

    #[test]
    fn json_decoder_decodes_multiple_rows() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "amount".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        )]));
        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"[{"amount":10},{"amount":20}]"#.as_ref();
        let rows = decoder.decode(payload).expect("decode batch").into_rows();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn json_decoder_respects_struct_schema_fields() {
        let struct_type =
            ConcreteDatatype::Struct(StructType::new(Arc::new(vec![StructField::new(
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            )])));

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "b".to_string(),
            struct_type,
        )]));

        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"b":{"c":10,"d":"ignore"}}"#.as_ref();
        let tuple = decode_one(&decoder, payload);

        let Some(Value::Struct(struct_val)) = tuple.value_by_name("orders", "b") else {
            panic!("expected struct value");
        };
        assert_eq!(struct_val.get_field("c"), Some(&Value::Int64(10)));
        assert_eq!(struct_val.get_field("d"), None);
    }

    #[test]
    fn json_decoder_respects_list_struct_schema_fields() {
        let element_type =
            ConcreteDatatype::Struct(StructType::new(Arc::new(vec![StructField::new(
                "c".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            )])));

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "items".to_string(),
            ConcreteDatatype::List(datatypes::ListType::new(Arc::new(element_type))),
        )]));

        let decoder = JsonDecoder::new("orders", schema, JsonMap::new());
        let payload = br#"{"items":[{"c":10,"d":"ignore"},{"c":20,"d":"ignore2"}]}"#.as_ref();
        let tuple = decode_one(&decoder, payload);

        let Some(Value::List(list_val)) = tuple.value_by_name("orders", "items") else {
            panic!("expected list value");
        };
        assert_eq!(list_val.len(), 2);

        let Some(Value::Struct(first)) = list_val.get(0) else {
            panic!("expected struct element");
        };
        assert_eq!(first.get_field("c"), Some(&Value::Int64(10)));
        assert_eq!(first.get_field("d"), None);
    }

    #[test]
    fn json_decoder_decode_projection_prunes_list_indexes_without_shrinking() {
        use crate::planner::decode_projection::{
            DecodeProjection, FieldPath, FieldPathSegment, ListIndex, ListIndexSelection,
            ProjectionNode,
        };
        use std::collections::{BTreeMap, BTreeSet};

        let element_type =
            ConcreteDatatype::Struct(StructType::new(Arc::new(vec![StructField::new(
                "x".to_string(),
                ConcreteDatatype::Int64(Int64Type),
                false,
            )])));

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "orders".to_string(),
            "items".to_string(),
            ConcreteDatatype::List(datatypes::ListType::new(Arc::new(element_type))),
        )]));

        let decoder = JsonDecoder::new("orders", Arc::clone(&schema), JsonMap::new());

        let mut indexes = BTreeSet::new();
        indexes.insert(0usize);
        indexes.insert(3usize);
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), ProjectionNode::All);
        let element = ProjectionNode::Struct(fields);
        let list_node = ProjectionNode::List {
            indexes: ListIndexSelection::Indexes(indexes),
            element: Box::new(element),
        };
        let mut projection = DecodeProjection::default();
        projection.mark_field_path_used(&FieldPath {
            column: "items".to_string(),
            segments: vec![
                FieldPathSegment::ListIndex(ListIndex::Const(0)),
                FieldPathSegment::StructField("x".to_string()),
            ],
        });
        projection.mark_field_path_used(&FieldPath {
            column: "items".to_string(),
            segments: vec![
                FieldPathSegment::ListIndex(ListIndex::Const(3)),
                FieldPathSegment::StructField("x".to_string()),
            ],
        });
        // Ensure the test uses the same semantics as mark_field_path_used.
        assert_eq!(projection.column("items"), Some(&list_node));

        let payload =
            br#"{"items":[{"x":10,"y":"ignore"},{"x":20},{"x":30},{"x":40},{"x":50}]}"#.as_ref();
        let tuple = decoder
            .decode_with_projection(payload, Some(&projection))
            .expect("decode batch")
            .into_rows()
            .into_iter()
            .next()
            .expect("one row");

        let Some(Value::List(list_val)) = tuple.value_by_name("orders", "items") else {
            panic!("expected list value");
        };
        assert_eq!(list_val.len(), 5);

        let Some(Value::Struct(first)) = list_val.get(0) else {
            panic!("expected struct element at 0");
        };
        assert_eq!(first.get_field("x"), Some(&Value::Int64(10)));

        assert_eq!(list_val.get(1), Some(&Value::Null));
        assert_eq!(list_val.get(2), Some(&Value::Null));

        let Some(Value::Struct(fourth)) = list_val.get(3) else {
            panic!("expected struct element at 3");
        };
        assert_eq!(fourth.get_field("x"), Some(&Value::Int64(40)));

        assert_eq!(list_val.get(4), Some(&Value::Null));
    }
}
