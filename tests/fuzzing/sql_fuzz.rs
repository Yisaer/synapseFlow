use proptest::prelude::*;
use sdk::{ManagerClient, PipelineCreateRequest, SdkError, StreamCreateRequest};
use serde_json::json;
use std::net::SocketAddr;

use super::{
    bind_manager_listener_or_skip, default_flow_instances, http_client, make_client, random_suffix,
    wait_for_server,
};

struct TestHarness {
    _temp_dir: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
    addr: SocketAddr,
    client: ManagerClient,
    http: reqwest::Client,
}

impl TestHarness {
    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn new() -> Option<Self> {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let storage =
            storage::StorageManager::new(temp_dir.path()).expect("create storage manager");

        let instance = manager::new_default_flow_instance();

        let Some(listener) = bind_manager_listener_or_skip().await else {
            return None;
        };

        let http = http_client();

        let addr = listener.local_addr().expect("read listener addr");

        let server = tokio::spawn(async move {
            manager::start_server_with_listener(
                listener,
                instance,
                storage,
                default_flow_instances(),
            )
            .await
            .expect("start manager server");
        });

        let client = make_client(addr);

        wait_for_server(&client).await;

        Some(Self {
            _temp_dir: temp_dir,
            server,
            addr,
            client,
            http,
        })
    }
}

async fn create_pipeline_raw(
    h: &TestHarness,
    pipeline_id: String,
    sql: String,
    sink_type: &str,
) -> (reqwest::StatusCode, String) {
    let resp = h
        .http
        .post(format!("{}/pipelines", h.base()))
        .json(&json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": sql,
            "sinks": [
                {
                    "type": sink_type,
                    "props": {},
                    "encoder": {
                        "type": "json",
                        "props": {}
                    }
                }
            ]
        }))
        .send()
        .await
        .expect("send raw create pipeline request");

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(Clone, Copy, Debug)]
enum ExpectedStage {
    Success,
    ParserFail,
    ResolveStreamFail,
    LogicalPlanFail,
    PhysicalBuildFail,
    MaySucceedOrControlledFail,
}

#[derive(Clone, Debug)]
struct SqlCase {
    sql: String,
    expected: ExpectedStage,
}

#[derive(Clone, Debug)]
enum Expr {
    Col(&'static str),
    Int(i64),
    Bool(bool),
    Binary {
        left: Box<Expr>,
        op: &'static str,
        right: Box<Expr>,
    },
    Func {
        name: &'static str,
        args: Vec<Expr>,
    },
    FieldAccess {
        base: &'static str,
        field: &'static str,
    },
}

impl Expr {
    fn render(&self) -> String {
        match self {
            Expr::Col(name) => name.to_string(),
            Expr::Int(value) => value.to_string(),
            Expr::Bool(value) => value.to_string(),
            Expr::Binary { left, op, right } => {
                format!("({} {} {})", left.render(), op, right.render())
            }
            Expr::Func { name, args } => {
                let args = args.iter().map(Expr::render).collect::<Vec<_>>().join(", ");
                format!("{name}({args})")
            }

            Expr::FieldAccess { base, field } => {
                format!("{base}.{field}")
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Projection {
    Star,
    Expr(Expr),
    Alias { expr: Expr, alias: &'static str },
}

impl Projection {
    fn render(&self) -> String {
        match self {
            Projection::Star => "*".to_string(),
            Projection::Expr(expr) => expr.render(),
            Projection::Alias { expr, alias } => {
                format!("{} AS {}", expr.render(), alias)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct SelectAst {
    projections: Vec<Projection>,
    stream_name: String,
    where_clause: Option<Expr>,
    group_by: Vec<Expr>,
    having: Option<Expr>,
    order_by: Option<Expr>,
}

impl SelectAst {
    fn render(&self) -> String {
        let projection_sql = self
            .projections
            .iter()
            .map(Projection::render)
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!("SELECT {} FROM {}", projection_sql, self.stream_name);

        if let Some(where_clause) = &self.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause.render());
        }

        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(
                &self
                    .group_by
                    .iter()
                    .map(Expr::render)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        if let Some(having) = &self.having {
            sql.push_str(" HAVING ");
            sql.push_str(&having.render());
        }

        if let Some(order_by) = &self.order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(&order_by.render());
        }

        sql
    }
}

fn basic_mock_stream_req(name: String) -> StreamCreateRequest {
    StreamCreateRequest {
        name,
        revision: 1,
        stream_type: "mock".to_string(),
        schema: json!({
            "type": "json",
            "props": {
                "columns": [
                    { "name": "value", "data_type": "int64" },
                    { "name": "flag", "data_type": "bool" },
                    { "name": "user_id", "data_type": "string" },
                    {
                        "name": "profile",
                        "data_type": "struct",
                        "fields": [
                            { "name": "age", "data_type": "int64" },
                            { "name": "name", "data_type": "string" }
                        ]
                    }
                ]
            }
        }),
        props: json!({}),
        shared: false,
        decoder: json!({ "type": "json", "props": {} }),
    }
}

fn assert_http_status(err: SdkError, expected: u16) -> String {
    match err {
        SdkError::Http { status, body, .. } => {
            assert_eq!(status.as_u16(), expected, "unexpected body: {body}");
            body
        }
        other => panic!("expected HTTP {expected}, got: {other:?}"),
    }
}

fn assert_error_matches_stage(body: &str, stage: ExpectedStage, sql: &str) {
    let lower = body.to_lowercase();

    match stage {
        ExpectedStage::Success => {}

        ExpectedStage::ParserFail => {
            assert!(
                lower.contains("parse")
                    || lower.contains("syntax")
                    || lower.contains("sql")
                    || lower.contains("parser"),
                "expected parser-related error, got body: {body}, sql: {sql}"
            );
        }

        ExpectedStage::ResolveStreamFail => {
            assert!(
                lower.contains("stream") && lower.contains("missing from storage"),
                "expected stream resolution error, got body: {body}, sql: {sql}"
            );
        }

        ExpectedStage::LogicalPlanFail => {
            assert!(
                lower.contains("column")
                    || lower.contains("schema")
                    || lower.contains("not found")
                    || lower.contains("unknown")
                    || lower.contains("logical")
                    || lower.contains("plan")
                    || lower.contains("build"),
                "expected logical/schema binding error, got body: {body}, sql: {sql}"
            );
        }
        ExpectedStage::PhysicalBuildFail => {
            assert!(
                lower.contains("sink")
                    || lower.contains("connector")
                    || lower.contains("not registered")
                    || lower.contains("physical")
                    || lower.contains("build"),
                "expected physical/build error, got body: {body}, sql: {sql}"
            );
        }
        ExpectedStage::MaySucceedOrControlledFail => {}
    }
}

fn valid_bool_expr() -> impl Strategy<Value = Expr> {
    prop_oneof![
        Just(Expr::Col("flag")),
        any::<bool>().prop_map(Expr::Bool),
        (
            Just(Expr::Col("value")),
            prop_oneof![Just("="), Just(">")],
            (-10i64..=10i64).prop_map(Expr::Int),
        )
            .prop_map(|(left, op, right)| Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }),
    ]
}

fn supported_select_case(stream_name: String) -> impl Strategy<Value = SqlCase> {
    (
        prop_oneof![
            Just(vec![Projection::Expr(Expr::Col("value"))]),
            Just(vec![Projection::Expr(Expr::Col("user_id"))]),
            Just(vec![Projection::Star]),
            Just(vec![Projection::Alias {
                expr: Expr::Col("value"),
                alias: "v",
            }]),
        ],
        prop::option::of(valid_bool_expr()),
    )
        .prop_map(move |(projections, where_clause)| {
            let ast = SelectAst {
                projections,
                stream_name: stream_name.clone(),
                where_clause,
                group_by: vec![],
                having: None,
                order_by: None,
            };

            SqlCase {
                sql: ast.render(),
                expected: ExpectedStage::Success,
            }
        })
}

fn parser_fail_case() -> impl Strategy<Value = SqlCase> {
    prop_oneof![
        Just("SELECT FROM".to_string()),
        Just("SELECT value FROM".to_string()),
        Just("SELECT value WHERE value > 0".to_string()),
        Just("SELECT value FROM source_stream WHERE".to_string()),
        Just("SELECT value FROM source_stream GROUP BY".to_string()),
        Just("SELECT value FROM source_stream ORDER BY".to_string()),
    ]
    .prop_map(|sql| SqlCase {
        sql,
        expected: ExpectedStage::ParserFail,
    })
}

fn resolve_stream_fail_case() -> impl Strategy<Value = SqlCase> {
    Just("SELECT value FROM definitely_missing_stream".to_string()).prop_map(|sql| SqlCase {
        sql,
        expected: ExpectedStage::ResolveStreamFail,
    })
}

fn logical_plan_fail_case(stream_name: String) -> impl Strategy<Value = SqlCase> {
    prop_oneof![
        Just(format!("SELECT missing_col FROM {stream_name}")),
        Just(format!(
            "SELECT value FROM {stream_name} WHERE missing_col > 0"
        )),
        Just(format!(
            "SELECT value FROM {stream_name} ORDER BY missing_col"
        )),
        Just(format!(
            "SELECT count(value) FROM {stream_name} GROUP BY missing_col"
        )),
        Just(format!(
            "SELECT count(value) FROM {stream_name} HAVING missing_col > 0"
        )),
    ]
    .prop_map(|sql| SqlCase {
        sql,
        expected: ExpectedStage::LogicalPlanFail,
    })
}

fn scalar_expr() -> BoxedStrategy<Expr> {
    let leaf = prop_oneof![
        Just(Expr::Col("value")),
        Just(Expr::Col("user_id")),
        Just(Expr::Col("flag")),
        (-10i64..=10i64).prop_map(Expr::Int),
        any::<bool>().prop_map(Expr::Bool),
    ];

    leaf.prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expr::Binary {
                left: Box::new(left),
                op: "+",
                right: Box::new(right),
            }),
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expr::Binary {
                left: Box::new(left),
                op: "=",
                right: Box::new(right),
            }),
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expr::Binary {
                left: Box::new(left),
                op: ">",
                right: Box::new(right),
            }),
            (inner.clone(), inner.clone()).prop_map(|(left, right)| Expr::Binary {
                left: Box::new(left),
                op: "AND",
                right: Box::new(right),
            }),
        ]
    })
    .boxed()
}

fn aggregate_expr() -> impl Strategy<Value = Expr> {
    prop_oneof![
        Just(Expr::Func {
            name: "count",
            args: vec![Expr::Col("value")],
        }),
        Just(Expr::Func {
            name: "sum",
            args: vec![Expr::Col("value")],
        }),
        Just(Expr::Func {
            name: "avg",
            args: vec![Expr::Col("value")],
        }),
    ]
}

fn stateful_expr() -> impl Strategy<Value = Expr> {
    prop_oneof![
        Just(Expr::Func {
            name: "last_value",
            args: vec![Expr::Col("value")],
        }),
        Just(Expr::Func {
            name: "unknown_stateful",
            args: vec![Expr::Col("value")],
        }),
    ]
}

fn near_boundary_select_case(stream_name: String) -> impl Strategy<Value = SqlCase> {
    prop_oneof![
        (
            prop_oneof![
                scalar_expr().prop_map(|expr| vec![Projection::Expr(expr)]),
                Just(vec![
                    Projection::Alias {
                        expr: Expr::Col("value"),
                        alias: "x",
                    },
                    Projection::Alias {
                        expr: Expr::Col("user_id"),
                        alias: "x",
                    },
                ]),
                Just(vec![
                    Projection::Alias {
                        expr: Expr::Col("value"),
                        alias: "v",
                    },
                    Projection::Expr(Expr::Col("v")),
                ]),
            ],
            prop::option::of(valid_bool_expr()),
        )
            .prop_map({
                let stream_name = stream_name.clone();
                move |(projections, where_clause)| {
                    let ast = SelectAst {
                        projections,
                        stream_name: stream_name.clone(),
                        where_clause,
                        group_by: vec![],
                        having: None,
                        order_by: None,
                    };

                    SqlCase {
                        sql: ast.render(),
                        expected: ExpectedStage::MaySucceedOrControlledFail,
                    }
                }
            }),
        aggregate_expr().prop_map({
            let stream_name = stream_name.clone();
            move |agg| {
                let ast = SelectAst {
                    projections: vec![Projection::Expr(agg)],
                    stream_name: stream_name.clone(),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: None,
                };

                SqlCase {
                    sql: ast.render(),
                    expected: ExpectedStage::MaySucceedOrControlledFail,
                }
            }
        }),
        (Just(Expr::Col("user_id")), aggregate_expr(),).prop_map(move |(group_key, agg)| {
            let ast = SelectAst {
                projections: vec![Projection::Expr(group_key.clone()), Projection::Expr(agg)],
                stream_name: stream_name.clone(),
                where_clause: None,
                group_by: vec![group_key],
                having: None,
                order_by: None,
            };

            SqlCase {
                sql: ast.render(),
                expected: ExpectedStage::MaySucceedOrControlledFail,
            }
        }),
    ]
}

fn struct_access_case(stream_name: String) -> impl Strategy<Value = SqlCase> {
    prop_oneof![
        Just(Expr::FieldAccess {
            base: "profile",
            field: "age",
        }),
        Just(Expr::FieldAccess {
            base: "profile",
            field: "name",
        }),
        Just(Expr::FieldAccess {
            base: "profile",
            field: "missing_field",
        }),
        Just(Expr::FieldAccess {
            base: "value",
            field: "age",
        }),
    ]
    .prop_map(move |expr| {
        let ast = SelectAst {
            projections: vec![Projection::Expr(expr)],
            stream_name: stream_name.clone(),
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: None,
        };

        SqlCase {
            sql: ast.render(),
            expected: ExpectedStage::MaySucceedOrControlledFail,
        }
    })
}
fn window_query_case(stream_name: String) -> impl Strategy<Value = SqlCase> {
    prop_oneof![
        Just(("TUMBLINGWINDOW(ss, 10)", "count(value)")),
        Just(("SLIDINGWINDOW(ss, 10, 5)", "count(value)")),
        Just(("COUNTWINDOW(10)", "count(value)")),
        Just(("TUMBLINGWINDOW(ss)", "count(value)")),
    ]
    .prop_map(move |(window, projection)| SqlCase {
        sql: format!("SELECT {projection} FROM {stream_name} GROUP BY {window}"),
        expected: ExpectedStage::MaySucceedOrControlledFail,
    })
}

fn stateful_aggregate_case(stream_name: String) -> impl Strategy<Value = SqlCase> {
    prop_oneof![
        stateful_expr().prop_map({
            let stream_name = stream_name.clone();
            move |expr| {
                let ast = SelectAst {
                    projections: vec![Projection::Expr(expr)],
                    stream_name: stream_name.clone(),
                    where_clause: None,
                    group_by: vec![],
                    having: None,
                    order_by: None,
                };

                SqlCase {
                    sql: ast.render(),
                    expected: ExpectedStage::MaySucceedOrControlledFail,
                }
            }
        }),
        Just(SqlCase {
            sql: format!("SELECT count(last_value(value)) FROM {stream_name}"),
            expected: ExpectedStage::MaySucceedOrControlledFail,
        }),
        Just(SqlCase {
            sql: format!("SELECT last_value(count(value)) FROM {stream_name}"),
            expected: ExpectedStage::MaySucceedOrControlledFail,
        }),
    ]
}

fn run_sql_case_proptest(
    rt: &tokio::runtime::Runtime,
    h: &TestHarness,
    strategy: impl Strategy<Value = SqlCase>,
    failure_msg: &'static str,
) {
    let config = proptest::test_runner::Config {
        cases: 64,
        failure_persistence: None,
        ..proptest::test_runner::Config::default()
    };

    let mut runner = proptest::test_runner::TestRunner::new(config);

    runner
        .run(&strategy, |case| {
            rt.block_on(async {
                let pipeline_id = format!("sql_fuzz_pipe_{}", random_suffix());

                let result = h
                    .client
                    .create_pipeline(&PipelineCreateRequest::nop(pipeline_id, case.sql.clone()))
                    .await;

                match case.expected {
                    ExpectedStage::Success => {
                        prop_assert!(
                            result.is_ok(),
                            "expected success, got error: {:?}, sql: {}",
                            result.err(),
                            case.sql
                        );
                    }

                    ExpectedStage::ParserFail
                    | ExpectedStage::ResolveStreamFail
                    | ExpectedStage::LogicalPlanFail
                    | ExpectedStage::PhysicalBuildFail => {
                        let err = result.expect_err("expected controlled failure, got success");
                        let body = assert_http_status(err, 400);
                        assert_error_matches_stage(&body, case.expected, &case.sql);
                    }
                    ExpectedStage::MaySucceedOrControlledFail => match result {
                        Ok(_) => {}

                        Err(err) => {
                            let body = assert_http_status(err, 400);
                            let lower = body.to_lowercase();

                            prop_assert!(
                                lower.contains("parse")
                                    || lower.contains("syntax")
                                    || lower.contains("sql")
                                    || lower.contains("column")
                                    || lower.contains("schema")
                                    || lower.contains("alias")
                                    || lower.contains("wildcard")
                                    || lower.contains("aggregate")
                                    || lower.contains("group")
                                    || lower.contains("having")
                                    || lower.contains("window")
                                    || lower.contains("stateful")
                                    || lower.contains("function")
                                    || lower.contains("field")
                                    || lower.contains("index")
                                    || lower.contains("logical")
                                    || lower.contains("plan")
                                    || lower.contains("build")
                                    || lower.contains("unsupported"),
                                "expected controlled boundary error, got body={}, sql={}",
                                body,
                                case.sql
                            );
                        }
                    },
                }

                Ok(())
            })
        })
        .expect(failure_msg);
}

#[test]
fn proptest_pipeline_sql_fuzz_via_create_pipeline_api() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("sql_fuzz_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    run_sql_case_proptest(
        &rt,
        &h,
        supported_select_case(stream_name.clone()),
        "supported select fuzz failed",
    );

    run_sql_case_proptest(&rt, &h, parser_fail_case(), "parser failure fuzz failed");

    run_sql_case_proptest(
        &rt,
        &h,
        resolve_stream_fail_case(),
        "resolve stream failure fuzz failed",
    );

    run_sql_case_proptest(
        &rt,
        &h,
        logical_plan_fail_case(stream_name.clone()),
        "logical plan failure fuzz failed",
    );
}

#[test]
fn proptest_physical_build_failures_return_400() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };

    let stream_name = format!("sql_fuzz_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    let config = proptest::test_runner::Config {
        cases: 16,
        failure_persistence: None,
        ..proptest::test_runner::Config::default()
    };

    let mut runner = proptest::test_runner::TestRunner::new(config);

    let strategy = Just(SqlCase {
        sql: format!("SELECT value FROM {stream_name}"),
        expected: ExpectedStage::PhysicalBuildFail,
    });

    runner
        .run(&strategy, |case| {
            rt.block_on(async {
                let pipeline_id = format!("sql_fuzz_physical_fail_{}", random_suffix());

                let (status, body) =
                    create_pipeline_raw(&h, pipeline_id, case.sql.clone(), "not_registered_sink")
                        .await;

                prop_assert_eq!(
                    status.as_u16(),
                    400,
                    "expected physical build failure HTTP 400, got status={}, body={}, sql={}",
                    status,
                    body,
                    case.sql
                );

                println!(
                    "\n[PHYSICAL BUILD FAIL]\nSQL: {}\nERROR BODY: {}\n",
                    case.sql, body
                );

                assert_error_matches_stage(&body, ExpectedStage::PhysicalBuildFail, &case.sql);

                Ok(())
            })
        })
        .expect("physical build failure fuzz failed");
}

#[test]
fn proptest_near_boundary_select_fragments_return_success_or_controlled_400() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };
    let stream_name = format!("sql_fuzz_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    run_sql_case_proptest(
        &rt,
        &h,
        near_boundary_select_case(stream_name),
        "near-boundary select fuzz failed",
    );
}

#[test]
fn proptest_struct_access_fragments_return_success_or_controlled_400() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };
    let stream_name = format!("sql_fuzz_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    run_sql_case_proptest(
        &rt,
        &h,
        struct_access_case(stream_name),
        "struct access fuzz failed",
    );
}

#[test]
fn proptest_window_queries_parse_validate_and_lower_or_reject_cleanly() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };
    let stream_name = format!("sql_fuzz_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    run_sql_case_proptest(
        &rt,
        &h,
        window_query_case(stream_name),
        "window query fuzz failed",
    );
}

#[test]
fn proptest_stateful_aggregate_interactions_return_success_or_controlled_400() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let Some(h) = rt.block_on(TestHarness::new()) else {
        return;
    };
    let stream_name = format!("sql_fuzz_stream_{}", random_suffix());

    rt.block_on(async {
        h.client
            .create_stream(&basic_mock_stream_req(stream_name.clone()))
            .await
            .expect("create stream");
    });

    run_sql_case_proptest(
        &rt,
        &h,
        stateful_aggregate_case(stream_name),
        "stateful/aggregate interaction fuzz failed",
    );
}
