use datatypes::types::{BooleanType, Int64Type, StringType};
use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::connector::{
    memory_pubsub_registry, MemoryData, MemoryPubSubRegistry, MemoryTopicKind,
    DEFAULT_MEMORY_PUBSUB_CAPACITY,
};
use flow::pipeline::{MemorySinkProps, PipelineDefinition};
use flow::planner::plan_cache::PlanCacheInputs;
use flow::{FlowInstance, PipelineStopMode, SinkDefinition, SinkProps, SinkType};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection, Row};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug, Deserialize)]
struct Config {
    sql_gen: SqlGenConfig,
    schema: SchemaConfig,
    sqlite: SqliteConfig,
    run: RunConfig,
}

#[derive(Debug, Deserialize)]
struct SqlGenConfig {
    seed: Option<u64>,
    output: String,
}

#[derive(Debug, Deserialize)]
struct SchemaConfig {
    int_columns: usize,
    bool_columns: usize,
    string_columns: usize,
    table_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SqliteConfig {
    path: String,
}

#[derive(Debug, Deserialize)]
struct RunConfig {
    rows_per_sql: Option<usize>,
    seed: Option<u64>,
    timeout_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnKind {
    Int,
    Bool,
    Str,
}

#[derive(Clone, Debug)]
struct ColumnSpec {
    name: String,
    kind: ColumnKind,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = parse_config_path();
    let (config, base_dir) = load_config(&config_path)?;

    let sql_path = resolve_path(&base_dir, &config.sql_gen.output);
    let sqls = load_sqls(&sql_path)?;
    if sqls.is_empty() {
        return Err(format!("no SQL statements found in {}", sql_path.display()).into());
    }

    let table_name = config
        .schema
        .table_name
        .clone()
        .unwrap_or_else(|| "stream".to_string());

    let columns = build_schema(&config.schema);
    let column_map = columns
        .iter()
        .map(|col| (col.name.clone(), col.kind))
        .collect::<HashMap<_, _>>();

    let mut rng = StdRng::seed_from_u64(run_seed(&config));
    let rows_per_sql = config.run.rows_per_sql.unwrap_or(10);
    let timeout_duration = Duration::from_millis(config.run.timeout_ms.unwrap_or(5_000));

    let sqlite_path = resolve_sqlite_path(&config.sqlite.path, &base_dir);
    let conn = Connection::open(sqlite_path.as_str())?;
    reset_sqlite_table(&conn, &table_name, &columns)?;

    for (index, sql) in sqls.iter().enumerate() {
        truncate_sqlite_table(&conn, &table_name)?;
        let instance = FlowInstance::new();
        let registry = memory_pubsub_registry().clone();

        let input_topic = format!("tests.daily.sql_assert.input.{index}");
        let output_topic = format!("tests.daily.sql_assert.output.{index}");
        declare_memory_topics(
            &registry,
            &input_topic,
            MemoryTopicKind::Bytes,
            &output_topic,
        );
        install_stream_schema(&instance, &input_topic, &table_name, &columns).await;

        let mut output = registry
            .open_subscribe_bytes(&output_topic)
            .expect("subscribe output bytes");

        let pipeline_id = format!("pipe_{index}");
        let pipeline = PipelineDefinition::new(
            pipeline_id.clone(),
            sql,
            vec![SinkDefinition::new(
                "mem_sink",
                SinkType::Memory,
                SinkProps::Memory(MemorySinkProps::new(output_topic.clone())),
            )],
        );
        instance
            .create_pipeline_with_plan_cache(
                pipeline,
                PlanCacheInputs {
                    pipeline_raw_json: String::new(),
                    streams_raw_json: Vec::new(),
                    snapshot: None,
                },
            )
            .unwrap_or_else(|_| panic!("failed to create pipeline for SQL: {sql}"));

        instance
            .start_pipeline(&pipeline_id)
            .unwrap_or_else(|_| panic!("failed to start pipeline for SQL: {sql}"));

        registry
            .wait_for_subscribers(&input_topic, MemoryTopicKind::Bytes, 1, timeout_duration)
            .await
            .expect("wait for memory source subscriber");

        let publisher = registry
            .open_publisher_bytes(&input_topic)
            .expect("open memory publisher");

        let mut previous_results: HashMap<String, usize> = HashMap::new();

        for row_index in 0..rows_per_sql {
            let row_values = generate_row(&columns, &mut rng);
            insert_sqlite_row(&conn, &table_name, &columns, &row_values)?;

            let current_rows = query_sqlite_rows(&conn, sql, &column_map)?;
            let current_multiset = multiset_from_rows(&current_rows);
            let expected_multiset = multiset_diff(&current_multiset, &previous_results);
            previous_results = current_multiset;

            publisher
                .publish_bytes(encode_row_as_json(&columns, &row_values)?)
                .expect("publish bytes");

            let actual_rows = recv_next_json_rows(&mut output, timeout_duration).await?;
            let actual_multiset = multiset_from_rows(&actual_rows);

            if expected_multiset != actual_multiset {
                let expected_debug = JsonValue::Array(expand_multiset(&expected_multiset));
                let actual_debug = JsonValue::Array(expand_multiset(&actual_multiset));
                return Err(format!(
                    "mismatch at sql index {index}, row {row_index}\nSQL: {sql}\nExpected: {expected_debug}\nActual: {actual_debug}"
                )
                .into());
            }
        }

        instance
            .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, timeout_duration)
            .await
            .unwrap_or_else(|_| panic!("failed to stop pipeline for SQL: {sql}"));
        instance
            .delete_pipeline(&pipeline_id)
            .await
            .unwrap_or_else(|_| panic!("failed to delete pipeline for SQL: {sql}"));
    }

    Ok(())
}

fn parse_config_path() -> String {
    let mut args = std::env::args().skip(1);
    let default_path = "tests/sql_assert/config.toml".to_string();
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().unwrap_or(default_path);
        }
    }
    default_path
}

fn load_config(path: &str) -> Result<(Config, PathBuf), Box<dyn std::error::Error>> {
    let config_path = resolve_input_path(path).map_err(|e| format!("{e}"))?;
    let raw = fs::read_to_string(&config_path)
        .map_err(|e| format!("read config {}: {e}", config_path.display()))?;
    let config: Config =
        toml::from_str(&raw).map_err(|e| format!("parse config {}: {e}", config_path.display()))?;
    let base_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok((config, base_dir))
}

fn resolve_path(base_dir: &Path, path: &str) -> PathBuf {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        base_dir.join(candidate)
    }
}

fn resolve_sqlite_path(path: &str, base_dir: &Path) -> String {
    if path == ":memory:" {
        return path.to_string();
    }
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        path.to_string()
    } else {
        base_dir.join(candidate).to_string_lossy().to_string()
    }
}

fn load_sqls(path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path).map_err(|e| {
        format!(
            "read SQL file {}: {e} (cwd: {})",
            path.display(),
            std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "<unknown>".to_string())
        )
    })?;
    let mut sqls = Vec::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("--") {
            continue;
        }
        sqls.push(trimmed.to_string());
    }
    Ok(sqls)
}

fn resolve_input_path(input: &str) -> Result<PathBuf, std::io::Error> {
    let p = PathBuf::from(input);
    if p.is_absolute() && p.exists() {
        return Ok(p);
    }

    let cwd_candidate = std::env::current_dir().ok().map(|cwd| cwd.join(&p));
    if let Some(candidate) = cwd_candidate.as_ref() {
        if candidate.exists() {
            return Ok(candidate.to_path_buf());
        }
    }

    // Allow running from any working directory by falling back to the repo root.
    let manifest_candidate = Path::new(env!("CARGO_MANIFEST_DIR")).join(&p);
    if manifest_candidate.exists() {
        return Ok(manifest_candidate);
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "path not found: {input} (tried: {} and {})",
            cwd_candidate
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<cwd unavailable>".to_string()),
            manifest_candidate.display()
        ),
    ))
}

fn run_seed(config: &Config) -> u64 {
    config.run.seed.or(config.sql_gen.seed).unwrap_or(42)
}

fn build_schema(schema: &SchemaConfig) -> Vec<ColumnSpec> {
    let mut columns = Vec::new();
    for idx in 1..=schema.int_columns {
        columns.push(ColumnSpec {
            name: format!("c_int_{idx}"),
            kind: ColumnKind::Int,
        });
    }
    for idx in 1..=schema.bool_columns {
        columns.push(ColumnSpec {
            name: format!("c_bool_{idx}"),
            kind: ColumnKind::Bool,
        });
    }
    for idx in 1..=schema.string_columns {
        columns.push(ColumnSpec {
            name: format!("c_str_{idx}"),
            kind: ColumnKind::Str,
        });
    }
    columns
}

fn reset_sqlite_table(
    conn: &Connection,
    table: &str,
    columns: &[ColumnSpec],
) -> Result<(), Box<dyn std::error::Error>> {
    let drop_stmt = format!("DROP TABLE IF EXISTS {table}");
    conn.execute(&drop_stmt, [])?;

    let col_defs: Vec<String> = columns
        .iter()
        .map(|col| match col.kind {
            ColumnKind::Int => format!("{} INTEGER", col.name),
            ColumnKind::Bool => format!("{} INTEGER", col.name),
            ColumnKind::Str => format!("{} TEXT", col.name),
        })
        .collect();
    let create_stmt = format!("CREATE TABLE {table} ({})", col_defs.join(", "));
    conn.execute(&create_stmt, [])?;
    Ok(())
}

fn truncate_sqlite_table(conn: &Connection, table: &str) -> Result<(), Box<dyn std::error::Error>> {
    let stmt = format!("DELETE FROM {table}");
    conn.execute(&stmt, [])?;
    Ok(())
}

fn insert_sqlite_row(
    conn: &Connection,
    table: &str,
    columns: &[ColumnSpec],
    values: &[Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let names: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();
    let placeholders = vec!["?"; columns.len()].join(", ");
    let stmt = format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        names.join(", "),
        placeholders
    );

    let params: Vec<SqlValue> = columns
        .iter()
        .zip(values.iter())
        .map(|(col, value)| match col.kind {
            ColumnKind::Int => match value {
                Value::Int64(v) => SqlValue::Integer(*v),
                _ => SqlValue::Null,
            },
            ColumnKind::Bool => match value {
                Value::Bool(v) => SqlValue::Integer(if *v { 1 } else { 0 }),
                _ => SqlValue::Null,
            },
            ColumnKind::Str => match value {
                Value::String(v) => SqlValue::Text(v.clone()),
                _ => SqlValue::Null,
            },
        })
        .collect();

    conn.execute(&stmt, params_from_iter(params))?;
    Ok(())
}

fn query_sqlite_rows(
    conn: &Connection,
    sql: &str,
    column_map: &HashMap<String, ColumnKind>,
) -> Result<Vec<JsonValue>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(sql)?;
    let column_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let mut rows_iter = stmt.query([])?;
    let mut rows = Vec::new();

    while let Some(row) = rows_iter.next()? {
        rows.push(row_to_json(row, &column_names, column_map)?);
    }

    Ok(rows)
}

fn row_to_json(
    row: &Row,
    column_names: &[String],
    column_map: &HashMap<String, ColumnKind>,
) -> Result<JsonValue, Box<dyn std::error::Error>> {
    let mut obj = JsonMap::with_capacity(column_names.len());
    for (idx, name) in column_names.iter().enumerate() {
        let kind = column_map.get(name).copied().unwrap_or(ColumnKind::Str);
        let value: SqlValue = row.get(idx)?;
        let json_value = sqlite_value_to_json(&value, kind)?;
        obj.insert(name.clone(), json_value);
    }
    Ok(JsonValue::Object(obj))
}

fn sqlite_value_to_json(
    value: &SqlValue,
    kind: ColumnKind,
) -> Result<JsonValue, Box<dyn std::error::Error>> {
    match kind {
        ColumnKind::Int => Ok(JsonValue::Number(sqlite_value_to_i64(value)?.into())),
        ColumnKind::Bool => Ok(JsonValue::Bool(sqlite_value_to_bool(value)?)),
        ColumnKind::Str => Ok(JsonValue::String(sqlite_value_to_string(value)?)),
    }
}

fn sqlite_value_to_i64(value: &SqlValue) -> Result<i64, Box<dyn std::error::Error>> {
    match value {
        SqlValue::Integer(v) => Ok(*v),
        SqlValue::Real(v) => Ok(*v as i64),
        SqlValue::Text(s) => Ok(s.parse::<i64>()?),
        SqlValue::Null => Ok(0),
        SqlValue::Blob(_) => Err("unexpected blob value for int".into()),
    }
}

fn sqlite_value_to_bool(value: &SqlValue) -> Result<bool, Box<dyn std::error::Error>> {
    match value {
        SqlValue::Integer(v) => Ok(*v != 0),
        SqlValue::Real(v) => Ok(*v != 0.0),
        SqlValue::Text(s) => match s.to_lowercase().as_str() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            other => Err(format!("unexpected bool value: {other}").into()),
        },
        SqlValue::Null => Ok(false),
        SqlValue::Blob(_) => Err("unexpected blob value for bool".into()),
    }
}

fn sqlite_value_to_string(value: &SqlValue) -> Result<String, Box<dyn std::error::Error>> {
    match value {
        SqlValue::Text(s) => Ok(s.clone()),
        SqlValue::Integer(v) => Ok(v.to_string()),
        SqlValue::Real(v) => Ok(v.to_string()),
        SqlValue::Null => Ok(String::new()),
        SqlValue::Blob(_) => Err("unexpected blob value for string".into()),
    }
}

fn generate_row(columns: &[ColumnSpec], rng: &mut StdRng) -> Vec<Value> {
    columns
        .iter()
        .map(|col| match col.kind {
            ColumnKind::Int => Value::Int64(rng.gen_range(-10..=10)),
            ColumnKind::Bool => Value::Bool(rng.gen_bool(0.5)),
            ColumnKind::Str => Value::String(random_string(rng, 1, 4)),
        })
        .collect()
}

fn random_string(rng: &mut StdRng, min_len: usize, max_len: usize) -> String {
    let alphabet = b"abcdef";
    let len = rng.gen_range(min_len..=max_len);
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        let idx = rng.gen_range(0..alphabet.len());
        out.push(alphabet[idx] as char);
    }
    out
}

fn declare_memory_topics(
    registry: &MemoryPubSubRegistry,
    input_topic: &str,
    input_kind: MemoryTopicKind,
    output_topic: &str,
) {
    registry
        .declare_topic(input_topic, input_kind, DEFAULT_MEMORY_PUBSUB_CAPACITY)
        .expect("declare input memory topic");
    registry
        .declare_topic(
            output_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare output memory topic");
}

fn encode_row_as_json(
    columns: &[ColumnSpec],
    values: &[Value],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut obj = JsonMap::with_capacity(columns.len());
    for (col, value) in columns.iter().zip(values.iter()) {
        let json = match value {
            Value::Null => JsonValue::Null,
            Value::Int64(v) => JsonValue::Number((*v).into()),
            Value::Bool(v) => JsonValue::Bool(*v),
            Value::String(v) => JsonValue::String(v.clone()),
            other => {
                return Err(format!(
                    "unsupported value type for json encoding (col={}): {other:?}",
                    col.name
                )
                .into());
            }
        };
        obj.insert(col.name.clone(), json);
    }
    Ok(serde_json::to_vec(&JsonValue::Object(obj))?)
}

async fn install_stream_schema(
    instance: &FlowInstance,
    input_topic: &str,
    table_name: &str,
    columns: &[ColumnSpec],
) {
    let schema_columns = columns
        .iter()
        .map(|col| {
            let datatype = match col.kind {
                ColumnKind::Int => ConcreteDatatype::Int64(Int64Type),
                ColumnKind::Bool => ConcreteDatatype::Bool(BooleanType),
                ColumnKind::Str => ConcreteDatatype::String(StringType),
            };
            ColumnSchema::new(table_name.to_string(), col.name.clone(), datatype)
        })
        .collect();
    let schema = Schema::new(schema_columns);
    let definition = StreamDefinition::new(
        table_name.to_string(),
        std::sync::Arc::new(schema),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.to_string())),
        // Use bytes + decoder, so schema pruning can be applied via decode projection.
        StreamDecoderConfig::new("json".to_string(), JsonMap::new()),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}

async fn recv_next_json_rows(
    output: &mut tokio::sync::broadcast::Receiver<MemoryData>,
    timeout_duration: Duration,
) -> Result<Vec<JsonValue>, Box<dyn std::error::Error>> {
    use tokio::sync::broadcast::error::RecvError;
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = match timeout(remaining, output.recv()).await? {
            Ok(item) => item,
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => {
                return Err("pipeline output topic closed".into());
            }
        };
        match item {
            MemoryData::Bytes(payload) => {
                let value: JsonValue = serde_json::from_slice(payload.as_ref())?;
                match value {
                    JsonValue::Array(items) => return Ok(items),
                    other => {
                        return Err(format!("unexpected JSON payload: {other}").into());
                    }
                }
            }
            MemoryData::Collection(_) => {
                return Err("unexpected collection payload on bytes topic".into());
            }
        }
    }
}

fn multiset_from_rows(rows: &[JsonValue]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for row in rows {
        let key = serde_json::to_string(&normalize_json(row.clone())).unwrap_or_default();
        *map.entry(key).or_insert(0) += 1;
    }
    map
}

fn multiset_diff(
    current: &HashMap<String, usize>,
    previous: &HashMap<String, usize>,
) -> HashMap<String, usize> {
    let mut diff = HashMap::new();
    for (key, current_count) in current {
        let prev_count = previous.get(key).copied().unwrap_or(0);
        if current_count > &prev_count {
            diff.insert(key.clone(), current_count - prev_count);
        }
    }
    diff
}

fn expand_multiset(multiset: &HashMap<String, usize>) -> Vec<JsonValue> {
    let mut expanded = Vec::new();
    for (key, count) in multiset {
        let Ok(value) = serde_json::from_str::<JsonValue>(key) else {
            continue;
        };
        for _ in 0..*count {
            expanded.push(value.clone());
        }
    }
    expanded
}

fn normalize_json(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(normalize_json).collect())
        }
        JsonValue::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut out = JsonMap::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(k, normalize_json(v));
            }
            JsonValue::Object(out)
        }
        other => other,
    }
}
