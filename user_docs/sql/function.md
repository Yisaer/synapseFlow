# Functions

This document describes the built-in SQL-visible functions in veloFlux.

## Overview

### Scalar functions

- `concat(a: string, b: string) -> string`: concatenate two strings.

### Aggregate functions

- `sum(x: numeric) -> numeric`: sum of numeric values.
- `ndv(x: any) -> int64`: number of distinct values.
- `last_row(x: any) -> any`: last observed value (by processing order).

### Stateful functions

- `lag(x: any) -> any`: previous row’s value (by processing order).

## Function details

### `concat(a, b)`

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Semantics: returns the concatenation of `a` and `b`.
- Constraints:
  - Requires exactly 2 arguments.
  - Both arguments must be strings.
  - Current implementation does not accept `NULL` as an argument.

Examples:

```sql
SELECT concat('hello', 'world') AS s FROM s
SELECT concat(first_name, last_name) AS full_name FROM s
```

### `sum(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: sum of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type matches the input numeric type.

Examples:

```sql
SELECT sum(x) AS total FROM s GROUP BY tumblingwindow('ss', 10)
SELECT sum(amount) FROM orders GROUP BY user_id
```

### `ndv(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Constraints:
  - Requires exactly 1 argument.
  - Ignores `NULL` inputs.
  - Return type is `int64`.

Examples:

```sql
SELECT ndv(user_id) AS unique_users FROM s GROUP BY tumblingwindow('ss', 10)
```

### `last_row(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: return the last observed value of `x` in the group/window.
- Constraints:
  - Requires exactly 1 argument.
  - Return type matches the argument type.
  - “Last” is defined by the pipeline processing order (no explicit `ORDER BY` support yet).

Examples:

```sql
SELECT last_row(status) AS last_status FROM s GROUP BY device_id
```

### `lag(x)`

- Kind: stateful
- Allowed clauses: `SELECT`, `WHERE`
- Semantics: return the previous row’s value of `x`.
- Constraints:
  - Requires exactly 1 argument.
  - First row returns `NULL`; subsequent rows return the previous row’s value.
  - Row order is the pipeline processing order (no explicit `ORDER BY` support yet).

Examples:

```sql
SELECT lag(x) AS prev_x, x FROM s
SELECT * FROM s WHERE lag(speed) > 0
```
