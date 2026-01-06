# Expressions

This document describes expression forms that are commonly used in SynapseFlow SQL.

Expressions can appear in:

- `SELECT` projection items
- `WHERE` predicates
- `GROUP BY` key expressions
- Window function arguments (see `user_docs/sql/window.md`)

## Core forms

- Identifiers: `a`, `device_id`
- Qualified identifiers: `t.a`
- Literals: numbers, strings, booleans, `NULL`
- Parenthesized expressions: `(a + 1)`

## Operators (typical)

- Arithmetic: `+`, `-`, `*`, `/`
- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Boolean: `AND`, `OR`, `NOT`
- Membership/ranges: `IN (...)`, `BETWEEN ... AND ...`

Planner/runtime may apply additional typing constraints; validate with `EXPLAIN` when in doubt.

## Nested data access

### Struct field access (`->`)

```sql
SELECT user->name FROM s
SELECT payload->profile->age FROM s
```

### List / map indexing (`[]`)

```sql
SELECT items[0] FROM s
SELECT data['key'] FROM s
```

### Dot access (`.`)

Dot access can be used for qualified columns such as `t.a`. It may also appear when working with
structured values depending on planner support; validate with `EXPLAIN`.

## Functions

Function calls are expressions. See `user_docs/sql/function.md` for the function catalog and usage
guidance.

