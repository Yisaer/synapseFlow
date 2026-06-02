use flow::{
    BooleanType, BytesType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int32Type,
    Int64Type, ListType, Schema, StringType, StructField, StructType, TimestampType, Uint32Type,
    Uint64Type,
};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::path::Path;
use std::sync::Arc;

const MAX_NESTING_DEPTH: usize = 10;
const WKT_TIMESTAMP_FQN: &str = "google.protobuf.Timestamp";

/// Parse a schema from a .proto file and a target message type.
///
/// Expected props:
///   - `proto_path`: path to the .proto file (required)
///   - `message_type`: fully qualified message name, e.g. `"Sensor"` or `"com.example.Sensor"` (required)
///   - `include_paths`: optional array of additional proto include directories
pub fn parse_proto_schema(
    stream_name: &str,
    props: &JsonMap<String, JsonValue>,
) -> Result<Schema, String> {
    let proto_path = get_string_prop(props, "proto_path")?;
    let message_type = get_string_prop(props, "message_type")?;

    let proto_dir = Path::new(&proto_path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());

    let mut include_paths: Vec<String> = vec![proto_dir];
    if let Some(extra) = props.get("include_paths").and_then(|v| v.as_array()) {
        for item in extra {
            if let Some(s) = item.as_str() {
                include_paths.push(s.to_string());
            }
        }
    }

    let mut compiler = protox::Compiler::new(&include_paths)
        .map_err(|e| format!("failed to create proto compiler: {e}"))?;
    compiler
        .open_file(&proto_path)
        .map_err(|e| format!("failed to parse proto file '{}': {e}", proto_path))?;

    let fds: FileDescriptorSet = compiler.file_descriptor_set();

    let target_msg = find_message_descriptor(&fds, &message_type).ok_or_else(|| {
        format!(
            "message type '{}' not found in '{}'",
            message_type, proto_path
        )
    })?;

    let columns = message_to_columns(stream_name, &proto_path, target_msg, &fds, 0)?;
    Ok(Schema::new(columns))
}

fn get_string_prop(props: &JsonMap<String, JsonValue>, key: &str) -> Result<String, String> {
    props
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("missing or empty '{}' in proto schema props", key))
}

/// Search for a message descriptor by fully qualified name across all files.
///
/// `full_name` may be package-qualified (e.g. `"com.example.Sensor"`) or
/// may include nesting (e.g. `"OuterMessage.InnerMessage"`).
fn find_message_descriptor<'a>(
    fds: &'a FileDescriptorSet,
    full_name: &str,
) -> Option<&'a DescriptorProto> {
    // Strip a leading dot (common in protobuf FQN notation like ".com.example.Sensor")
    let full_name = full_name.strip_prefix('.').unwrap_or(full_name);
    let parts: Vec<&str> = full_name.split('.').collect();
    if parts.is_empty() {
        return None;
    }

    for file in &fds.file {
        let package = file.package.as_deref().unwrap_or("");
        let package_parts: Vec<&str> = if package.is_empty() {
            vec![]
        } else {
            package.split('.').collect()
        };

        if parts.len() > package_parts.len() && parts[..package_parts.len()] == package_parts[..] {
            let message_path = &parts[package_parts.len()..];
            if let Some(msg) = find_message_in_descriptors(&file.message_type, message_path) {
                return Some(msg);
            }
        }

        if let Some(msg) = find_message_in_descriptors(&file.message_type, &parts) {
            return Some(msg);
        }
    }

    None
}

/// Recursively search for a message matching `path` within a list of descriptors.
fn find_message_in_descriptors<'a>(
    descriptors: &'a [DescriptorProto],
    path: &[&str],
) -> Option<&'a DescriptorProto> {
    if path.is_empty() {
        return None;
    }

    for desc in descriptors {
        if desc.name.as_deref() == Some(path[0]) {
            if path.len() == 1 {
                return Some(desc);
            }
            return find_message_in_descriptors(&desc.nested_type, &path[1..]);
        }
    }

    None
}

/// Convert a protobuf message descriptor into a list of `ColumnSchema`.
///
/// `stream_name` is used as the `source_name` for every column.
fn message_to_columns(
    stream_name: &str,
    proto_path: &str,
    msg: &DescriptorProto,
    fds: &FileDescriptorSet,
    depth: usize,
) -> Result<Vec<ColumnSchema>, String> {
    if depth > MAX_NESTING_DEPTH {
        let name = msg.name.as_deref().unwrap_or("<unknown>");
        return Err(format!(
            "maximum nesting depth ({}) exceeded at message '{}'",
            MAX_NESTING_DEPTH, name
        ));
    }

    let mut columns = Vec::new();

    for field in &msg.field {
        let field_name = field
            .name
            .as_deref()
            .ok_or_else(|| {
                let msg_name = msg.name.as_deref().unwrap_or("<unknown>");
                format!("field without name in message '{}'", msg_name)
            })?
            .to_string();

        let data_type = map_field_type(stream_name, proto_path, field, fds, depth)?;
        columns.push(ColumnSchema::new(
            stream_name.to_string(),
            field_name,
            data_type,
        ));
    }

    Ok(columns)
}

/// Map a proto `FieldDescriptorProto` to a `ConcreteDatatype`.
fn map_field_type(
    stream_name: &str,
    proto_path: &str,
    field: &FieldDescriptorProto,
    fds: &FileDescriptorSet,
    depth: usize,
) -> Result<ConcreteDatatype, String> {
    let type_id = field.r#type;
    let label_id = field.label;

    let base_type = match type_id {
        Some(id) if id == Type::Double as i32 => ConcreteDatatype::Float64(Float64Type),
        Some(id) if id == Type::Float as i32 => ConcreteDatatype::Float32(Float32Type),
        Some(id)
            if id == Type::Int64 as i32
                || id == Type::Sint64 as i32
                || id == Type::Sfixed64 as i32 =>
        {
            ConcreteDatatype::Int64(Int64Type)
        }
        Some(id) if id == Type::Uint64 as i32 || id == Type::Fixed64 as i32 => {
            ConcreteDatatype::Uint64(Uint64Type)
        }
        Some(id)
            if id == Type::Int32 as i32
                || id == Type::Sint32 as i32
                || id == Type::Sfixed32 as i32 =>
        {
            ConcreteDatatype::Int32(Int32Type)
        }
        Some(id) if id == Type::Uint32 as i32 || id == Type::Fixed32 as i32 => {
            ConcreteDatatype::Uint32(Uint32Type)
        }
        Some(id) if id == Type::Bool as i32 => ConcreteDatatype::Bool(BooleanType),
        Some(id) if id == Type::String as i32 => ConcreteDatatype::String(StringType),
        Some(id) if id == Type::Bytes as i32 => ConcreteDatatype::Bytes(BytesType),
        Some(id) if id == Type::Enum as i32 => ConcreteDatatype::Int32(Int32Type),
        Some(id) if id == Type::Message as i32 || id == Type::Group as i32 => {
            let msg_type_name = field
                .type_name
                .as_deref()
                .ok_or_else(|| "message field without type_name".to_string())?;
            resolve_message_type(stream_name, proto_path, msg_type_name, fds, depth + 1)?
        }
        Some(_) => {
            return Err(format!(
                "unsupported proto field type {:?} for field '{}'",
                field.r#type,
                field.name.as_deref().unwrap_or("<unknown>")
            ));
        }
        None => {
            return Err("proto field without type".to_string());
        }
    };

    let is_repeated = label_id == Some(Label::Repeated as i32);

    if is_repeated {
        Ok(ConcreteDatatype::List(ListType::new(Arc::new(base_type))))
    } else {
        Ok(base_type)
    }
}

/// Resolve a message type reference (from `type_name` like `".package.MessageName"`)
/// to a `ConcreteDatatype`.
///
/// Well-known types like `google.protobuf.Timestamp` get special treatment;
/// all others are expanded recursively as `Struct`.
fn resolve_message_type(
    stream_name: &str,
    proto_path: &str,
    type_name: &str,
    fds: &FileDescriptorSet,
    depth: usize,
) -> Result<ConcreteDatatype, String> {
    let fqn = type_name.strip_prefix('.').unwrap_or(type_name);

    if fqn == WKT_TIMESTAMP_FQN {
        return Ok(ConcreteDatatype::Timestamp(TimestampType));
    }

    let target = find_message_descriptor(fds, fqn).ok_or_else(|| {
        format!(
            "referenced message type '{}' not found (resolved from '{}' in '{}')",
            fqn, type_name, proto_path
        )
    })?;

    let fields = message_to_columns(stream_name, proto_path, target, fds, depth)?;
    let struct_fields: Vec<StructField> = fields
        .into_iter()
        .map(|col| StructField::new(col.name.clone(), col.data_type.clone(), false))
        .collect();

    Ok(ConcreteDatatype::Struct(StructType::new(Arc::new(
        struct_fields,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn testdata_path(name: &str) -> String {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        format!("{manifest_dir}/src/schema/testdata/{name}")
    }

    fn make_props(proto_path: &str, message_type: &str) -> JsonMap<String, JsonValue> {
        let mut props = JsonMap::new();
        props.insert("proto_path".to_string(), json!(proto_path));
        props.insert("message_type".to_string(), json!(message_type));
        props
    }

    fn assert_column(col: &ColumnSchema, name: &str, expected_type: &str) {
        assert_eq!(col.name, name, "column name mismatch");
        let type_str = concrete_datatype_name(&col.data_type);
        assert_eq!(type_str, expected_type, "column '{name}' type mismatch");
    }

    fn assert_struct_field<'a>(col: &'a ColumnSchema, expected_name: &str) -> &'a StructType {
        match &col.data_type {
            ConcreteDatatype::Struct(st) => {
                assert_eq!(col.name, expected_name);
                st
            }
            other => panic!("expected struct for '{expected_name}', got {other:?}"),
        }
    }

    fn assert_list_element<'a>(col: &'a ColumnSchema, expected_name: &str) -> &'a ConcreteDatatype {
        match &col.data_type {
            ConcreteDatatype::List(lt) => {
                assert_eq!(col.name, expected_name);
                lt.item_type()
            }
            other => panic!("expected list for '{expected_name}', got {other:?}"),
        }
    }

    fn concrete_datatype_name(dt: &ConcreteDatatype) -> String {
        match dt {
            ConcreteDatatype::Null => "null",
            ConcreteDatatype::Float32(_) => "float32",
            ConcreteDatatype::Float64(_) => "float64",
            ConcreteDatatype::Int8(_) => "int8",
            ConcreteDatatype::Int16(_) => "int16",
            ConcreteDatatype::Int32(_) => "int32",
            ConcreteDatatype::Int64(_) => "int64",
            ConcreteDatatype::Uint8(_) => "uint8",
            ConcreteDatatype::Uint16(_) => "uint16",
            ConcreteDatatype::Uint32(_) => "uint32",
            ConcreteDatatype::Uint64(_) => "uint64",
            ConcreteDatatype::String(_) => "string",
            ConcreteDatatype::Bytes(_) => "bytes",
            ConcreteDatatype::Timestamp(_) => "timestamp",
            ConcreteDatatype::Struct(_) => "struct",
            ConcreteDatatype::List(_) => "list",
            ConcreteDatatype::Bool(_) => "bool",
        }
        .to_string()
    }

    // ── primitive type tests ──────────────────────────────────────────

    #[test]
    fn parse_simple_primitive_types() {
        let path = testdata_path("simple.proto");
        let props = make_props(&path, "Simple");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 6);
        assert_column(&schema.column_schemas()[0], "name", "string");
        assert_column(&schema.column_schemas()[1], "count", "int32");
        assert_column(&schema.column_schemas()[2], "temperature", "float32");
        assert_column(&schema.column_schemas()[3], "score", "float64");
        assert_column(&schema.column_schemas()[4], "active", "bool");
        assert_column(&schema.column_schemas()[5], "payload", "bytes");
    }

    #[test]
    fn parse_integer_types() {
        let path = testdata_path("ints.proto");
        let props = make_props(&path, "Ints");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 10);
        assert_column(&schema.column_schemas()[0], "a", "int32");
        assert_column(&schema.column_schemas()[1], "b", "int64");
        assert_column(&schema.column_schemas()[2], "c", "uint32");
        assert_column(&schema.column_schemas()[3], "d", "uint64");
        assert_column(&schema.column_schemas()[4], "e", "int32");
        assert_column(&schema.column_schemas()[5], "f", "int64");
        assert_column(&schema.column_schemas()[6], "g", "uint32");
        assert_column(&schema.column_schemas()[7], "h", "uint64");
        assert_column(&schema.column_schemas()[8], "i", "int32");
        assert_column(&schema.column_schemas()[9], "j", "int64");
    }

    // ── nested / struct type tests ────────────────────────────────────

    #[test]
    fn parse_nested_struct_message() {
        let path = testdata_path("nested.proto");
        let props = make_props(&path, "Person");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 3);
        assert_column(&schema.column_schemas()[0], "name", "string");
        assert_column(&schema.column_schemas()[1], "age", "int32");

        let addr_struct = assert_struct_field(&schema.column_schemas()[2], "address");
        let fields = addr_struct.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "city");
        assert_eq!(concrete_datatype_name(fields[0].data_type()), "string");
        assert_eq!(fields[1].name(), "zip");
        assert_eq!(concrete_datatype_name(fields[1].data_type()), "int32");
    }

    // ── repeated / list type tests ────────────────────────────────────

    #[test]
    fn parse_repeated_field_as_list() {
        let path = testdata_path("list.proto");
        let props = make_props(&path, "WithList");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 2);

        let elem0 = assert_list_element(&schema.column_schemas()[0], "values");
        assert_eq!(concrete_datatype_name(elem0), "int32");

        let elem1 = assert_list_element(&schema.column_schemas()[1], "tags");
        assert_eq!(concrete_datatype_name(elem1), "string");
    }

    #[test]
    fn parse_map_field_as_list_of_struct() {
        let path = testdata_path("map.proto");
        let props = make_props(&path, "Scores");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 1);

        let elem_type = assert_list_element(&schema.column_schemas()[0], "scores");
        match elem_type {
            ConcreteDatatype::Struct(st) => {
                let fields = st.fields();
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "key");
                assert_eq!(concrete_datatype_name(fields[0].data_type()), "string");
                assert_eq!(fields[1].name(), "value");
                assert_eq!(concrete_datatype_name(fields[1].data_type()), "int32");
            }
            other => panic!("expected struct for map entry, got {other:?}"),
        }
    }

    // ── well-known types ──────────────────────────────────────────────

    #[test]
    fn parse_timestamp_well_known_type() {
        let path = testdata_path("timestamp.proto");
        let props = make_props(&path, "Event");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 2);
        assert_column(&schema.column_schemas()[0], "name", "string");
        assert_column(&schema.column_schemas()[1], "created_at", "timestamp");
    }

    // ── enum ──────────────────────────────────────────────────────────

    #[test]
    fn parse_enum_as_int32() {
        let path = testdata_path("enum.proto");
        let props = make_props(&path, "Record");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 2);
        assert_column(&schema.column_schemas()[0], "id", "string");
        assert_column(&schema.column_schemas()[1], "status", "int32");
    }

    // ── package-qualified names ───────────────────────────────────────

    #[test]
    fn parse_message_with_package() {
        let path = testdata_path("pkg.proto");
        let props = make_props(&path, "com.example.Sensor");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 2);
        assert_column(&schema.column_schemas()[0], "sensor_id", "string");
        assert_column(&schema.column_schemas()[1], "reading", "float64");
    }

    // ── comprehensive type coverage ──────────────────────────────────

    /// A single proto message exercising every mappable proto type,
    /// including nested structs, lists, maps, enums, Timestamp, and
    /// lists-of-structs.
    #[test]
    fn parse_comprehensive_all_types() {
        let path = testdata_path("all_types.proto");
        let props = make_props(&path, "AllTypes");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");

        assert_eq!(schema.column_schemas().len(), 23);
        let cols = schema.column_schemas();

        // Scalars (fields 1-15)
        assert_column(&cols[0], "double_val", "float64");
        assert_column(&cols[1], "float_val", "float32");
        assert_column(&cols[2], "int32_val", "int32");
        assert_column(&cols[3], "int64_val", "int64");
        assert_column(&cols[4], "uint32_val", "uint32");
        assert_column(&cols[5], "uint64_val", "uint64");
        assert_column(&cols[6], "sint32_val", "int32");
        assert_column(&cols[7], "sint64_val", "int64");
        assert_column(&cols[8], "fixed32_val", "uint32");
        assert_column(&cols[9], "fixed64_val", "uint64");
        assert_column(&cols[10], "sfixed32_val", "int32");
        assert_column(&cols[11], "sfixed64_val", "int64");
        assert_column(&cols[12], "bool_val", "bool");
        assert_column(&cols[13], "string_val", "string");
        assert_column(&cols[14], "bytes_val", "bytes");

        // Enum → int32 (field 16)
        assert_column(&cols[15], "priority", "int32");

        // Nested message → struct (field 17)
        let metadata_struct = assert_struct_field(&cols[16], "metadata");
        let m_fields = metadata_struct.fields();
        assert_eq!(m_fields.len(), 2);
        assert_eq!(concrete_datatype_name(m_fields[0].data_type()), "string"); // key
        assert_eq!(concrete_datatype_name(m_fields[1].data_type()), "string"); // value

        // Well-known Timestamp (field 18)
        assert_column(&cols[17], "created_at", "timestamp");

        // Repeated primitive → list<int32> (field 19)
        let list_elem = assert_list_element(&cols[18], "int32_list");
        assert_eq!(concrete_datatype_name(list_elem), "int32");

        // Repeated string → list<string> (field 20)
        let str_list_elem = assert_list_element(&cols[19], "string_list");
        assert_eq!(concrete_datatype_name(str_list_elem), "string");

        // Repeated message → list<struct> (field 21)
        let meta_list_elem = assert_list_element(&cols[20], "metadata_list");
        match meta_list_elem {
            ConcreteDatatype::Struct(st) => {
                assert_eq!(st.fields().len(), 2);
                assert_eq!(concrete_datatype_name(st.fields()[0].data_type()), "string");
                assert_eq!(concrete_datatype_name(st.fields()[1].data_type()), "string");
            }
            other => panic!("expected struct for repeated message, got {other:?}"),
        }

        // Map<string,int32> → list<struct<key:string, value:int32>> (field 22)
        let map_list_elem = assert_list_element(&cols[21], "scores");
        match map_list_elem {
            ConcreteDatatype::Struct(st) => {
                assert_eq!(st.fields().len(), 2);
                assert_eq!(concrete_datatype_name(st.fields()[0].data_type()), "string"); // key
                assert_eq!(concrete_datatype_name(st.fields()[1].data_type()), "int32"); // value
            }
            other => panic!("expected struct for map entry, got {other:?}"),
        }

        // Map<int32,Metadata> → list<struct<key:int32, value:struct<key,value>>> (field 23)
        let complex_map_elem = assert_list_element(&cols[22], "complex_map");
        match complex_map_elem {
            ConcreteDatatype::Struct(st) => {
                assert_eq!(st.fields().len(), 2);
                assert_eq!(concrete_datatype_name(st.fields()[0].data_type()), "int32"); // key
                match st.fields()[1].data_type() {
                    ConcreteDatatype::Struct(inner) => {
                        assert_eq!(inner.fields().len(), 2);
                        assert_eq!(
                            concrete_datatype_name(inner.fields()[0].data_type()),
                            "string"
                        );
                        assert_eq!(
                            concrete_datatype_name(inner.fields()[1].data_type()),
                            "string"
                        );
                    }
                    other => panic!("expected nested struct in map value, got {other:?}"),
                }
            }
            other => panic!("expected struct for map entry, got {other:?}"),
        }
    }

    // ── source_name and edge-case tests ───────────────────────────────

    #[test]
    fn source_name_is_stream_name_not_message_name() {
        let path = testdata_path("simple.proto");
        let props = make_props(&path, "Simple");
        let schema = parse_proto_schema("test_stream_name", &props).expect("parse schema");
        for col in schema.column_schemas() {
            assert_eq!(
                col.source_name(),
                "test_stream_name",
                "source_name should be the stream name, not the proto message name"
            );
        }
    }

    #[test]
    fn message_type_accepts_leading_dot() {
        let path = testdata_path("pkg.proto");
        // Use leading dot like protobuf tooling often does
        let props = make_props(&path, ".com.example.Sensor");
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");
        assert_eq!(schema.column_schemas().len(), 2);
        assert_column(&schema.column_schemas()[0], "sensor_id", "string");
        assert_column(&schema.column_schemas()[1], "reading", "float64");
    }

    // ── error cases ───────────────────────────────────────────────────

    #[test]
    fn error_missing_proto_path() {
        let props = {
            let mut m = JsonMap::new();
            m.insert("message_type".to_string(), json!("Foo"));
            m
        };
        let result = parse_proto_schema("test_stream", &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("proto_path"));
    }

    #[test]
    fn error_missing_message_type() {
        let props = {
            let mut m = JsonMap::new();
            m.insert("proto_path".to_string(), json!("some.proto"));
            m
        };
        let result = parse_proto_schema("test_stream", &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("message_type"));
    }

    #[test]
    fn error_message_not_found() {
        let path = testdata_path("notfound.proto");
        let props = make_props(&path, "NonExistent");
        let result = parse_proto_schema("test_stream", &props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn error_proto_file_not_found() {
        let mut props = JsonMap::new();
        props.insert("proto_path".to_string(), json!("/nonexistent/file.proto"));
        props.insert("message_type".to_string(), json!("Foo"));
        let result = parse_proto_schema("test_stream", &props);
        assert!(result.is_err());
    }

    #[test]
    fn error_malformed_proto() {
        let path = testdata_path("bad.proto");
        let props = make_props(&path, "Foo");
        let result = parse_proto_schema("test_stream", &props);
        assert!(result.is_err());
    }

    // ── include_paths test ────────────────────────────────────────────

    #[test]
    fn parse_with_include_paths() {
        let path = testdata_path("with_include.proto");
        let mut props = make_props(&path, "SimpleWithInclude");
        // Add an include path that doesn't exist – should still work
        // because the file's own directory is always included.
        props.insert(
            "include_paths".to_string(),
            json!(["/some/nonexistent/path"]),
        );
        let schema = parse_proto_schema("test_stream", &props).expect("parse schema");
        assert_eq!(schema.column_schemas().len(), 1);
        assert_column(&schema.column_schemas()[0], "value", "string");
    }
}
