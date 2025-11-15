use datatypes::{StructField, StructType, StructValue, Value};
use flow::expr::{func::EvalError, StreamSqlConverter};
use flow::model::{Column, RecordBatch};
use parser::parse_sql;
use std::sync::Arc;

fn build_orders_batch() -> RecordBatch {
    let orders_id = Column::new(
        "orders".to_string(),
        "id".to_string(),
        vec![Value::Int64(1), Value::Int64(2)],
    );
    let orders_amount = Column::new(
        "orders".to_string(),
        "amount".to_string(),
        vec![Value::Int64(10), Value::Int64(20)],
    );
    RecordBatch::new(vec![orders_id, orders_amount]).expect("orders record batch")
}

fn build_orders_users_batch() -> RecordBatch {
    let mut columns = build_orders_batch().columns().to_vec();
    let users_name = Column::new(
        "users".to_string(),
        "name".to_string(),
        vec![Value::String("alice".into()), Value::String("bob".into())],
    );

    columns.push(users_name);
    RecordBatch::new(columns).expect("orders & users record batch")
}

fn eval_first_projection(sql: &str, batch: &RecordBatch) -> Result<Vec<Value>, EvalError> {
    let select_stmt = parse_sql(sql).expect("parse sql");
    let converter = StreamSqlConverter::new();
    let mut projections = converter
        .convert_select_stmt_to_scalar(&select_stmt)
        .expect("convert to scalar");
    let expr = projections
        .drain(..)
        .next()
        .expect("at least one projection");
    expr.eval_with_collection(batch)
}

fn expected_structs(batch: &RecordBatch, source_selector: Option<&str>) -> Vec<Value> {
    let columns = batch.columns();
    let selected_columns: Vec<_> = columns
        .iter()
        .filter(|column| {
            if let Some(prefix) = source_selector {
                column.source_name() == prefix
            } else {
                true
            }
        })
        .collect();

    let fields: Vec<StructField> = selected_columns
        .iter()
        .map(|column| {
            let datatype = column
                .data_type()
                .unwrap_or(datatypes::ConcreteDatatype::Null);
            StructField::new(column.name().to_string(), datatype, true)
        })
        .collect();
    let struct_type = StructType::new(Arc::new(fields));

    (0..batch.num_rows())
        .map(|row_idx| {
            let mut items = Vec::with_capacity(selected_columns.len());
            for column in &selected_columns {
                items.push(column.get(row_idx).cloned().unwrap_or(Value::Null));
            }
            Value::Struct(StructValue::new(items, struct_type.clone()))
        })
        .collect()
}

#[test]
fn wildcard_single_table_includes_orders_columns() {
    let batch = build_orders_batch();
    let values = eval_first_projection("SELECT * FROM orders", &batch).expect("evaluation");
    let expected = expected_structs(&batch, None);
    assert_eq!(values, expected);
}

#[test]
fn wildcard_multiple_tables_includes_orders_and_users() {
    let batch = build_orders_users_batch();
    let values = eval_first_projection("SELECT * FROM orders, users", &batch).expect("evaluation");
    let expected = expected_structs(&batch, None);
    assert_eq!(values, expected);
}

#[test]
fn qualified_wildcard_filters_orders_columns() {
    let batch = build_orders_users_batch();
    let values =
        eval_first_projection("SELECT orders.* FROM orders, users", &batch).expect("evaluation");
    let expected = expected_structs(&batch, Some("orders"));
    assert_eq!(values, expected);
}

#[test]
fn qualified_wildcard_without_columns_errors() {
    let batch = build_orders_batch();
    let err =
        eval_first_projection("SELECT missing.* FROM orders", &batch).expect_err("should error");

    match err {
        EvalError::ColumnNotFound { source, column } => {
            assert_eq!(source, "missing");
            assert_eq!(column, "*");
        }
        other => panic!("unexpected error: {other}"),
    }
}
