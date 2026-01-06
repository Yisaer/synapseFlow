# SQL Syntax Capabilities Catalog (`GET /capabilities/syntax`)

This document defines a runtime capability catalog that tells agents (and other clients) which SQL
syntax constructs are supported by SynapseFlow.

## Background

SynapseFlow uses `sqlparser` to parse SQL into an AST. However, "sqlparser can parse it" does not
mean SynapseFlow supports the feature end-to-end.

SynapseFlow’s actual supported SQL subset is defined by the project-owned contract IR
`parser::SelectStmt` and by what the planner can lower from it. The capability catalog exists to
make this boundary explicit and machine-readable so agents can avoid generating unsupported SQL.

## Goals

- Provide a stable, machine-readable list of supported SQL constructs.
- Enable agents to generate SQL constrained to SynapseFlow’s supported subset.
- Provide constraints and examples for partially-supported constructs.
- Ensure validator errors can reference a construct id that exists in this catalog.

## Non-goals

- Describing stream schemas (see `docs/syntax/streams.md`).
- Describing functions (function catalog is separate).
- Guaranteeing that `sqlparser` will reject unsupported syntax (the catalog defines the contract;
  parser/validator must enforce it).

## API

### Endpoint

- `GET /capabilities/syntax`

### Response shape

The response has no version field. Agents always talk to the SynapseFlow process they are currently
connected to and must treat the runtime response as the source of truth.

```json
{
  "dialect": "StreamDialect",
  "ir": "SelectStmt",
  "constructs": []
}
```

### `SyntaxConstruct` shape

The response is a tree of constructs. Each construct can have `children` to express hierarchical
relationships (e.g. `window.state` contains `window.state.over_partition_by`).

Construct ids are stable identifiers used by agents and by `validate_sql` errors.

```json
{
  "id": "window.state.over_partition_by",
  "type": "feature",
  "title": "Partitioned state window",
  "status": "supported",
  "purpose": "Run independent state windows per key (like per user/device).",
  "semantics": "PARTITION BY keys isolate state tracking so each key maintains its own state window timeline.",
  "constraints": ["window_only_in_group_by", "at_most_one_window"],
  "syntax": ["statewindow(<open>, <emit>) OVER (PARTITION BY <key> [, <key> ...])"],
  "examples": ["... GROUP BY statewindow(a > 0, b = 1) OVER (PARTITION BY k1, k2)"],
  "emits_plan_nodes": ["Window"],
  "children": []
}
```

Fields:

- `id: string`: stable identifier used by agents and by `validate_sql` errors.
- `type: "group" | "feature"`: whether this node is a purely structural group or an actual feature.
- `title: string`: short label for UI/explanations.
- `status: "supported" | "partial" | "unsupported"` (optional; present for `type == "feature"`):
  - `supported`: allowed and intended to work end-to-end.
  - `partial`: supported only under explicit constraints.
  - `unsupported`: reserved for explicit negative entries (optional).
- `purpose: string` (optional): agent-friendly "why use this".
- `semantics: string` (optional): short explanation of meaning at a SQL/plan level.
- `placement: { clause: string, contexts: string[] }` (optional): where this construct may appear.
- `constraints: string[]` (optional): restrictions for `partial` (and sometimes `supported`).
- `workarounds: string[]` (optional): suggested rewrites or alternatives.
- `syntax: string[]` (optional): canonical syntax templates.
- `examples: string[]` (optional): canonical SQL snippets.
- `emits_plan_nodes: string[]` (optional): plan node kinds typically produced by this syntax.
- `children: SyntaxConstruct[]` (optional): sub-constructs.

## Naming conventions for construct ids

Construct ids are namespaced and should be fine-grained enough to avoid ambiguous "partial"
semantics. Recommended prefixes:

- `statement.*` (supported statements)
- `select.*` (SELECT clauses)
- `from.*` (FROM/source constructs)
- `expr.*` (expression forms)
- `window.*` (window declarations)

Examples:

- `statement.select`
- `select.projection`
- `select.group_by`
- `select.order_by`
- `from.join`
- `expr.case`
- `window.tumbling`

## Expressions (`expr.*`)

The catalog also includes an `expr.*` subtree to describe which expression forms and operators are
supported end-to-end (identifiers, literals, function calls, arithmetic/comparison operators, CASE,
struct field access, list indexing, etc.).

Agents should treat expression forms marked as `unsupported` as disallowed, even if `sqlparser` can
parse them.

## Meaning of `partial`

`partial` means the feature is supported only in a restricted subset. Clients must honor
`constraints` (and still rely on `validate_sql` for correctness).

Examples of `partial` constraints:

- `group_by_requires_aggregates`
- `window_only_in_group_by`
- `at_most_one_window`

## Relationship to `validate_sql`

When SQL is rejected due to unsupported syntax/constructs, `validate_sql` should return a
structured error that references the catalog construct id, for example:

- `code: "UNSUPPORTED_SYNTAX_CONSTRUCT"`
- `construct_id: "select.order_by"`

This allows agents to:

1) Look up the construct in `GET /capabilities/syntax`.
2) Apply `workarounds` or remove the unsupported clause.
3) Re-validate and iterate.

## Extensibility

The catalog is expected to grow as SynapseFlow expands its supported SQL subset. Clients should:

- Treat unknown feature keys as unsupported.
- Avoid hard-coding assumptions about which keys exist.
- Prefer `validate_sql` feedback for final enforcement.
