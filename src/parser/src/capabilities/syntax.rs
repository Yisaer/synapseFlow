use serde::Serialize;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SyntaxFeatureStatus {
    Supported,
    Partial,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SyntaxConstructKind {
    Group,
    Feature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SyntaxPlacement {
    pub clause: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contexts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SyntaxConstruct {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: SyntaxConstructKind,
    pub title: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SyntaxFeatureStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantics: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<SyntaxPlacement>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workarounds: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub syntax: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emits_plan_nodes: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<SyntaxConstruct>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SyntaxCapabilities {
    pub dialect: String,
    pub ir: String,
    pub constructs: Vec<SyntaxConstruct>,
}

fn group(id: &str, title: &str, children: Vec<SyntaxConstruct>) -> SyntaxConstruct {
    SyntaxConstruct {
        id: id.to_string(),
        kind: SyntaxConstructKind::Group,
        title: title.to_string(),
        status: None,
        purpose: None,
        semantics: None,
        placement: None,
        constraints: Vec::new(),
        workarounds: Vec::new(),
        syntax: Vec::new(),
        examples: Vec::new(),
        emits_plan_nodes: Vec::new(),
        children,
    }
}

#[allow(clippy::too_many_arguments)]
fn feature(
    id: &str,
    title: &str,
    status: SyntaxFeatureStatus,
    purpose: Option<&str>,
    semantics: Option<&str>,
    placement: Option<SyntaxPlacement>,
    constraints: &[&str],
    workarounds: &[&str],
    syntax: &[&str],
    examples: &[&str],
    emits_plan_nodes: &[&str],
    children: Vec<SyntaxConstruct>,
) -> SyntaxConstruct {
    SyntaxConstruct {
        id: id.to_string(),
        kind: SyntaxConstructKind::Feature,
        title: title.to_string(),
        status: Some(status),
        purpose: purpose.map(|s| s.to_string()),
        semantics: semantics.map(|s| s.to_string()),
        placement,
        constraints: constraints.iter().map(|s| s.to_string()).collect(),
        workarounds: workarounds.iter().map(|s| s.to_string()).collect(),
        syntax: syntax.iter().map(|s| s.to_string()).collect(),
        examples: examples.iter().map(|s| s.to_string()).collect(),
        emits_plan_nodes: emits_plan_nodes.iter().map(|s| s.to_string()).collect(),
        children,
    }
}

fn build_syntax_capabilities() -> SyntaxCapabilities {
    let statements = group(
        "statement",
        "Statements",
        vec![feature(
            "statement.select",
            "SELECT statement",
            SyntaxFeatureStatus::Supported,
            Some("Read from one or more streams and compute derived columns."),
            Some(
                "veloFlux treats SQL as a query plan description. The supported surface form is a single SELECT query that the planner lowers into a logical plan.",
            ),
            None,
            &["exactly_one_statement", "select_only"],
            &[],
            &["SELECT <projection> FROM <stream> [WHERE ...] [GROUP BY ...]"],
            &["SELECT a FROM s"],
            &[],
            vec![],
        )],
    );

    let select_clauses = group(
        "select",
        "SELECT clauses",
        vec![
            feature(
                "select.projection",
                "Projection",
                SyntaxFeatureStatus::Supported,
                Some("Choose which columns/expressions to output."),
                None,
                Some(SyntaxPlacement {
                    clause: "SELECT".to_string(),
                    contexts: vec!["top_level".to_string()],
                }),
                &[],
                &[],
                &["SELECT <expr> [, <expr> ...]"],
                &["SELECT a, b + 1 FROM s"],
                &["Project"],
                vec![],
            ),
            feature(
                "select.projection.alias",
                "Projection alias",
                SyntaxFeatureStatus::Supported,
                Some("Name an output expression so downstream sinks and users can reference it."),
                None,
                Some(SyntaxPlacement {
                    clause: "SELECT".to_string(),
                    contexts: vec!["projection_item".to_string()],
                }),
                &[],
                &[],
                &["<expr> AS <alias>"],
                &["SELECT a + 1 AS x FROM s"],
                &[],
                vec![],
            ),
            feature(
                "select.projection.wildcard",
                "Wildcard projection",
                SyntaxFeatureStatus::Supported,
                Some("Output all columns from the input stream schema."),
                None,
                Some(SyntaxPlacement {
                    clause: "SELECT".to_string(),
                    contexts: vec!["projection_item".to_string()],
                }),
                &[],
                &[],
                &["*"],
                &["SELECT * FROM s"],
                &[],
                vec![],
            ),
            feature(
                "select.where",
                "WHERE filter",
                SyntaxFeatureStatus::Supported,
                Some("Filter rows before aggregation/output."),
                Some(
                    "Rows that do not satisfy the predicate are dropped. The predicate is evaluated per input row.",
                ),
                Some(SyntaxPlacement {
                    clause: "WHERE".to_string(),
                    contexts: vec!["select".to_string()],
                }),
                &[],
                &[],
                &["WHERE <predicate_expr>"],
                &["SELECT * FROM s WHERE a > 10"],
                &["Filter"],
                vec![],
            ),
            feature(
                "select.group_by",
                "GROUP BY",
                SyntaxFeatureStatus::Partial,
                Some("Group rows for aggregation, optionally with stream windows."),
                Some(
                    "GROUP BY defines grouping keys for aggregates. In veloFlux, window declarations also live in GROUP BY to define how the stream is segmented over time/count/state.",
                ),
                Some(SyntaxPlacement {
                    clause: "GROUP BY".to_string(),
                    contexts: vec!["select".to_string()],
                }),
                &["group_by_requires_aggregates", "at_most_one_window"],
                &[
                    "If you do not need aggregation, remove GROUP BY.",
                    "If you need windowed aggregation, include a window(...) declaration in GROUP BY.",
                ],
                &["GROUP BY <key_expr> [, <key_expr> ...]"],
                &["SELECT sum(a) FROM s GROUP BY b"],
                &["Aggregation"],
                vec![],
            ),
        ],
    );

    let from_constructs = group(
        "from",
        "FROM sources",
        vec![feature(
            "from.source",
            "FROM source",
            SyntaxFeatureStatus::Supported,
            Some("Choose which input stream(s) the query reads from."),
            Some("The source name must match a stream exposed by the runtime stream catalog."),
            Some(SyntaxPlacement {
                clause: "FROM".to_string(),
                contexts: vec!["select".to_string()],
            }),
            &["at_least_one_source_required"],
            &[],
            &["FROM <stream_name>"],
            &["SELECT * FROM s"],
            &["DataSource"],
            vec![],
        )],
    );

    let expr_constructs = group(
        "expr",
        "Expressions",
        vec![
            feature(
                "expr.identifier",
                "Column reference",
                SyntaxFeatureStatus::Supported,
                Some("Reference an input column (optionally qualified by source/alias)."),
                Some("Identifiers resolve against the input stream schema(s)."),
                None,
                &["compound_identifier_max_two_parts"],
                &[],
                &["a", "<source_or_alias>.<column>"],
                &["SELECT a FROM s", "SELECT s.a FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.wildcard",
                "Wildcard",
                SyntaxFeatureStatus::Supported,
                Some("Reference all columns (optionally qualified by a source/alias)."),
                None,
                None,
                &["wildcard_only_in_projection_or_function_args"],
                &[],
                &["*", "<source_or_alias>.*"],
                &["SELECT * FROM s", "SELECT s.* FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.literal",
                "Literal",
                SyntaxFeatureStatus::Partial,
                Some("Use constant values in expressions."),
                Some("Only a small set of literal forms are supported end-to-end."),
                None,
                &["literal_number_string_bool_null_only"],
                &[],
                &["1", "1.23", "'text'", "true", "NULL"],
                &["SELECT 42, 'ok', true FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.parentheses",
                "Parenthesized expression",
                SyntaxFeatureStatus::Supported,
                Some("Group expressions to control evaluation order."),
                None,
                None,
                &[],
                &[],
                &["(<expr>)"],
                &["SELECT (a + b) * 2 FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.unary.not",
                "Unary NOT",
                SyntaxFeatureStatus::Supported,
                Some("Negate a boolean expression."),
                None,
                None,
                &[],
                &[],
                &["NOT <expr>"],
                &["SELECT * FROM s WHERE NOT (a = 1)"],
                &[],
                vec![],
            ),
            feature(
                "expr.unary.minus",
                "Unary minus",
                SyntaxFeatureStatus::Unsupported,
                Some("Negate a numeric value or write negative literals."),
                None,
                None,
                &[],
                &["Rewrite as 0 - <expr>."],
                &["-<expr>"],
                &["SELECT 0 - a FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.binary.arithmetic",
                "Binary arithmetic operators",
                SyntaxFeatureStatus::Supported,
                Some("Compute derived numeric values."),
                None,
                None,
                &[],
                &[],
                &[
                    "<expr> + <expr>",
                    "<expr> - <expr>",
                    "<expr> * <expr>",
                    "<expr> / <expr>",
                    "<expr> % <expr>",
                ],
                &["SELECT a + 1, b * 2 FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.binary.comparison",
                "Binary comparison operators",
                SyntaxFeatureStatus::Supported,
                Some("Compare values to produce boolean results."),
                None,
                None,
                &[],
                &[],
                &[
                    "<expr> = <expr>",
                    "<expr> != <expr>",
                    "<expr> < <expr>",
                    "<expr> <= <expr>",
                    "<expr> > <expr>",
                    "<expr> >= <expr>",
                ],
                &["SELECT * FROM s WHERE a >= 10"],
                &[],
                vec![],
            ),
            feature(
                "expr.binary.logical",
                "Logical AND/OR",
                SyntaxFeatureStatus::Unsupported,
                Some("Combine boolean conditions."),
                None,
                None,
                &[],
                &[],
                &["<expr> AND <expr>", "<expr> OR <expr>"],
                &[],
                &[],
                vec![],
            ),
            feature(
                "expr.function_call",
                "Function call",
                SyntaxFeatureStatus::Supported,
                Some("Compute derived values using built-in functions."),
                Some(
                    "Function availability and constraints are defined by the runtime function catalog.",
                ),
                None,
                &[],
                &[],
                &["<func>(<arg> [, <arg> ...])"],
                &["SELECT concat(a, b) FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.between",
                "BETWEEN predicate",
                SyntaxFeatureStatus::Supported,
                Some("Check whether a value lies within a range."),
                None,
                None,
                &[],
                &[],
                &[
                    "<expr> BETWEEN <low> AND <high>",
                    "<expr> NOT BETWEEN <low> AND <high>",
                ],
                &["SELECT * FROM s WHERE a BETWEEN 1 AND 10"],
                &[],
                vec![],
            ),
            feature(
                "expr.in_list",
                "IN list predicate",
                SyntaxFeatureStatus::Supported,
                Some("Check whether a value is contained in a list."),
                None,
                None,
                &[],
                &[],
                &[
                    "<expr> IN (<expr> [, <expr> ...])",
                    "<expr> NOT IN (<expr> [, <expr> ...])",
                ],
                &["SELECT * FROM s WHERE a IN (1, 2, 3)"],
                &[],
                vec![],
            ),
            feature(
                "expr.case",
                "CASE expression",
                SyntaxFeatureStatus::Supported,
                Some("Conditional expression for branching logic."),
                None,
                None,
                &[],
                &[],
                &[
                    "CASE WHEN <cond> THEN <expr> [WHEN <cond> THEN <expr> ...] [ELSE <expr>] END",
                    "CASE <expr> WHEN <value> THEN <expr> [WHEN <value> THEN <expr> ...] [ELSE <expr>] END",
                ],
                &["SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.struct_field_access",
                "Struct field access",
                SyntaxFeatureStatus::Partial,
                Some("Access nested fields inside struct-like columns."),
                Some("Only the `->` operator is supported for field access."),
                None,
                &[
                    "json_access_arrow_only",
                    "field_access_right_must_be_identifier",
                ],
                &[],
                &["<expr> -> <field>", "<expr> -> <field> -> <field>"],
                &["SELECT user->name, data->profile->age FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.list_index",
                "List indexing",
                SyntaxFeatureStatus::Partial,
                Some("Index into list-like columns."),
                Some("Only a single index key is supported per access."),
                None,
                &["map_access_single_key_only"],
                &[],
                &["<expr>[<index_expr>]"],
                &["SELECT a[0] FROM s"],
                &[],
                vec![],
            ),
            feature(
                "expr.cast",
                "CAST expression",
                SyntaxFeatureStatus::Unsupported,
                Some("Convert a value to a different type."),
                None,
                None,
                &[],
                &[],
                &["CAST(<expr> AS <type>)"],
                &[],
                &[],
                vec![],
            ),
        ],
    );

    let window_constructs = group(
        "window",
        "Windowing",
        vec![feature(
            "window",
            "Window declaration (GROUP BY)",
            SyntaxFeatureStatus::Supported,
            Some(
                "Split an unbounded stream into finite windows so you can compute per-window results.",
            ),
            Some(
                "In streaming, queries often need results per time bucket (e.g., every 10 seconds) or per state change. A window declaration acts like a special grouping key that resets over time/count/state. It is declared inside GROUP BY and typically used together with aggregate functions.",
            ),
            Some(SyntaxPlacement {
                clause: "GROUP BY".to_string(),
                contexts: vec!["group_by_item".to_string()],
            }),
            &["window_only_in_group_by", "at_most_one_window"],
            &["If you need a global aggregate without windowing, omit the window declaration."],
            &[
                "tumblingwindow(<time_unit>, <length>)",
                "slidingwindow(<time_unit>, <lookback> [, <lookahead>])",
                "countwindow(<count>)",
                "statewindow(<open_predicate>, <emit_predicate>) [OVER (PARTITION BY <keys...>)]",
            ],
            &["SELECT count(*) FROM s GROUP BY tumblingwindow('ss', 10)"],
            &["Window"],
            vec![
                feature(
                    "window.tumbling",
                    "Tumbling window",
                    SyntaxFeatureStatus::Supported,
                    Some("Compute results for fixed, non-overlapping time buckets."),
                    Some(
                        "Each row belongs to exactly one time bucket. Use tumbling windows for periodic metrics (e.g., requests per 10 seconds).",
                    ),
                    None,
                    &["window_only_in_group_by", "at_most_one_window"],
                    &[],
                    &["tumblingwindow('ss'|'mm'|'hh', <length>)"],
                    &["... GROUP BY tumblingwindow('ss', 10)"],
                    &[],
                    vec![],
                ),
                feature(
                    "window.sliding",
                    "Sliding window",
                    SyntaxFeatureStatus::Supported,
                    Some("Compute results over overlapping moving time ranges."),
                    Some(
                        "A row can contribute to multiple windows depending on lookback/lookahead. Use sliding windows for moving averages or rolling metrics.",
                    ),
                    None,
                    &["window_only_in_group_by", "at_most_one_window"],
                    &[],
                    &["slidingwindow('ss'|'mm'|'hh', <lookback> [, <lookahead>])"],
                    &["... GROUP BY slidingwindow('ss', 10, 15)"],
                    &[],
                    vec![],
                ),
                feature(
                    "window.count",
                    "Count window",
                    SyntaxFeatureStatus::Supported,
                    Some("Compute results for every N rows."),
                    Some(
                        "Rows are grouped by count rather than time. Useful when input has no time column or for batching by record count.",
                    ),
                    None,
                    &["window_only_in_group_by", "at_most_one_window"],
                    &[],
                    &["countwindow(<count>)"],
                    &["... GROUP BY countwindow(3)"],
                    &[],
                    vec![],
                ),
                feature(
                    "window.state",
                    "State window",
                    SyntaxFeatureStatus::Supported,
                    Some("Compute results for dynamic segments defined by state predicates."),
                    Some(
                        "A state window uses two predicates: `open` starts a segment when it becomes true; `emit` decides when to output/close a segment. Use it when you want results per state transition (e.g., sessions).",
                    ),
                    None,
                    &["window_only_in_group_by", "at_most_one_window"],
                    &[],
                    &["statewindow(<open_predicate>, <emit_predicate>)"],
                    &["... GROUP BY statewindow(a > 0, b = 1)"],
                    &[],
                    vec![feature(
                        "window.state.over_partition_by",
                        "Partitioned state window",
                        SyntaxFeatureStatus::Supported,
                        Some("Run independent state windows per key (like per user/device)."),
                        Some(
                            "PARTITION BY keys isolate state tracking so each key maintains its own state window timeline. Use this to avoid mixing state across entities.",
                        ),
                        None,
                        &["window_only_in_group_by", "at_most_one_window"],
                        &[],
                        &["statewindow(<open>, <emit>) OVER (PARTITION BY <key> [, <key> ...])"],
                        &["... GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)"],
                        &[],
                        vec![],
                    )],
                ),
            ],
        )],
    );

    SyntaxCapabilities {
        dialect: "StreamDialect".to_string(),
        ir: "SelectStmt".to_string(),
        constructs: vec![
            statements,
            select_clauses,
            from_constructs,
            expr_constructs,
            window_constructs,
        ],
    }
}

static SYNTAX_CAPABILITIES: OnceLock<SyntaxCapabilities> = OnceLock::new();

pub fn syntax_capabilities() -> &'static SyntaxCapabilities {
    SYNTAX_CAPABILITIES.get_or_init(build_syntax_capabilities)
}

pub fn syntax_capabilities_owned() -> SyntaxCapabilities {
    syntax_capabilities().clone()
}
