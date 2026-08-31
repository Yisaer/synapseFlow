# Functions

This document describes the built-in SQL-visible functions in veloFlux.

## Overview

### Scalar functions

- `concat(a: string, b: string) -> string`: concatenate two strings.
- `now() -> timestamp`: current UTC timestamp. Alias: `current_timestamp`.
- `cur_date() -> string`: current UTC date as `YYYY-MM-DD`. Alias: `current_date`.
- `cur_time() -> string`: current UTC time as `HH:MM:SS.ffffff`. Alias: `current_time`.
- `format_time(ts: timestamp, fmt: string) -> string`: format a UTC timestamp.
- `day_name(ts: timestamp) -> string`: English weekday name.
- `day_of_month(ts: timestamp) -> int64`: day of month. Alias: `day`.
- `day_of_week(ts: timestamp) -> int64`: weekday number, Sunday=1 through Saturday=7.
- `day_of_year(ts: timestamp) -> int64`: day of year.
- `from_unix_time(seconds: int64) -> timestamp`: convert Unix epoch seconds to UTC timestamp.
- `hour(ts: timestamp) -> int64`: hour component.
- `last_day(ts: timestamp) -> string`: last day of the UTC month as `YYYY-MM-DD`.
- `microsecond(ts: timestamp) -> int64`: microsecond component.
- `minute(ts: timestamp) -> int64`: minute component.
- `month(ts: timestamp) -> int64`: month number.
- `month_name(ts: timestamp) -> string`: English month name.
- `second(ts: timestamp) -> int64`: second component.

### Aggregate functions

- `avg(x: numeric) -> float64`: average of numeric values.
- `count(x: any) -> int64`: count rows or non-`NULL` values.
- `deduplicate(x: any) -> list<any>`: distinct non-`NULL` values in first-seen order.
- `max(x: any) -> any`: maximum non-`NULL` value.
- `median(x: numeric) -> float64`: exact median of numeric values.
- `min(x: any) -> any`: minimum non-`NULL` value.
- `sum(x: numeric) -> numeric`: sum of numeric values.
- `ndv(x: any) -> int64`: number of distinct values.
- `last_row(x: any) -> any`: last observed value (by processing order).
- `stddev(x: numeric) -> float64`: population standard deviation.
- `stddevs(x: numeric) -> float64`: sample standard deviation.
- `var(x: numeric) -> float64`: population variance.
- `vars(x: numeric) -> float64`: sample variance.

### Stateful functions

- `lag(x: any) -> any`: previous row’s value (by processing order).

### Pipeline state functions

- `last_hit_count() -> uint64`: number of previous rows accepted at the current pipeline state
  position.
- `last_agg_hit_count() -> uint64`: number of previous non-empty collections emitted by the
  `HAVING` filter.

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

### Date and time scalar functions

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Semantics:
  - Timestamp inputs are interpreted in UTC.
  - Date-only results use `YYYY-MM-DD`.
  - Time-only results use `HH:MM:SS.ffffff`.
  - `now()` and `current_timestamp()` are evaluated when each row is processed.
- Constraints:
  - Timestamp extractors require exactly 1 timestamp argument.
  - `format_time(ts, fmt)` requires a timestamp and a chrono strftime format string.
  - `from_unix_time(seconds)` accepts integer Unix epoch seconds.
  - Functions with input arguments return `NULL` if any required argument is `NULL`.

Examples:

```sql
SELECT now() AS processed_at FROM s
SELECT current_date() AS utc_date FROM s
SELECT format_time(event_time, '%Y-%m-%d %H:%M:%S') AS event_time_text FROM s
SELECT day_name(event_time), hour(event_time), minute(event_time) FROM s
SELECT last_day(event_time) AS month_end FROM s
SELECT from_unix_time(epoch_seconds) AS event_time FROM s
```

### `avg(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: average of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type is `float64`.

Examples:

```sql
SELECT avg(x) AS avg_x FROM s GROUP BY tumblingwindow('ss', 10)
SELECT avg(amount) FROM orders GROUP BY user_id
```

### `count(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics:
  - `count(*)` counts all rows in the group/window.
  - `count(x)` counts non-`NULL` values of `x`.
- Constraints:
  - Requires exactly 1 argument.
  - Return type is `int64`.
  - `DISTINCT` is not supported.

Examples:

```sql
SELECT count(*) AS rows FROM s GROUP BY tumblingwindow('ss', 10)
SELECT count(device_id) AS seen_devices FROM s GROUP BY site_id
```

### `deduplicate(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: collect distinct non-`NULL` values in first-seen order.
- Constraints:
  - Requires exactly 1 argument.
  - Ignores `NULL` inputs.
  - Returns `NULL` if all inputs are `NULL` or the group/window is empty.
  - Return type is `list<T>`, where `T` matches the input type.

Examples:

```sql
SELECT deduplicate(tag) AS tags FROM s GROUP BY tumblingwindow('ss', 10)
SELECT deduplicate(user_id) FROM s GROUP BY region
```

### `max(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: maximum non-`NULL` value in the group/window.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be comparable scalar data.
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type matches the input type.

Examples:

```sql
SELECT max(score) AS max_score FROM s GROUP BY tumblingwindow('ss', 10)
SELECT max(status) FROM s GROUP BY device_id
```

### `median(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: exact median of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type is `float64`.
  - When the number of non-`NULL` inputs is even, the result is the average of the two middle values.

Examples:

```sql
SELECT median(latency) AS p50 FROM s GROUP BY tumblingwindow('ss', 10)
SELECT median(amount) FROM orders GROUP BY user_id
```

### `min(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: minimum non-`NULL` value in the group/window.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be comparable scalar data.
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Return type matches the input type.

Examples:

```sql
SELECT min(score) AS min_score FROM s GROUP BY tumblingwindow('ss', 10)
SELECT min(status) FROM s GROUP BY device_id
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

### `stddev(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: population standard deviation of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Returns `0.0` when exactly one non-`NULL` value is present.
  - Return type is `float64`.

Examples:

```sql
SELECT stddev(latency) AS latency_stddev FROM s GROUP BY tumblingwindow('ss', 10)
```

### `stddevs(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: sample standard deviation of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if fewer than 2 non-`NULL` values are present.
  - Return type is `float64`.

Examples:

```sql
SELECT stddevs(latency) AS latency_stddev_sample FROM s GROUP BY tumblingwindow('ss', 10)
```

### `var(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: population variance of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if all inputs are `NULL`.
  - Returns `0.0` when exactly one non-`NULL` value is present.
  - Return type is `float64`.

Examples:

```sql
SELECT var(latency) AS latency_var FROM s GROUP BY tumblingwindow('ss', 10)
```

### `vars(x)`

- Kind: aggregate
- Allowed clauses: `SELECT` (in an aggregation context)
- Semantics: sample variance of numeric values.
- Constraints:
  - Requires exactly 1 argument.
  - Argument type must be numeric (`int*`, `uint*`, `float*`).
  - Ignores `NULL` inputs; returns `NULL` if fewer than 2 non-`NULL` values are present.
  - Return type is `float64`.

Examples:

```sql
SELECT vars(latency) AS latency_var_sample FROM s GROUP BY tumblingwindow('ss', 10)
```

### `lag(x)`

- Kind: stateful
- Allowed clauses: `SELECT`, `WHERE`
- Semantics: return the previous row's value of `x`.
- Constraints:
  - Requires exactly 1 argument.
  - First row returns `NULL`; subsequent rows return the previous row's value.
  - Row order is the pipeline processing order (no explicit `ORDER BY` support yet).

Examples:

```sql
SELECT lag(x) AS prev_x, x FROM s
SELECT * FROM s WHERE lag(speed) > 0
```

### `last_hit_count()`

- Kind: pipeline state
- Allowed clauses: `SELECT`, `WHERE`
- Semantics: returns the number of previous rows accepted at the current processor position.
- Constraints:
  - Requires zero arguments.
  - Not allowed in `HAVING`, `GROUP BY`, `ORDER BY`, aggregate arguments, or stateful function
    contexts.
  - The counter resets when the pipeline starts.

Examples:

```sql
SELECT a FROM s WHERE last_hit_count() < 3
SELECT last_hit_count() FROM s WHERE a > 10
```

### `last_agg_hit_count()`

- Kind: pipeline state
- Allowed clauses: `HAVING`
- Semantics: returns the number of previous non-empty collections emitted by the `HAVING` filter.
- Constraints:
  - Requires zero arguments.
  - Requires a windowed aggregation query.
  - Not an aggregate function and not allowed in `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`,
    aggregate arguments, or stateful function contexts.
  - Counts non-empty filtered collections, not rows inside a collection.
  - The counter resets when the pipeline starts.

Examples:

```sql
SELECT sum(a) AS s
FROM s
GROUP BY countwindow(4)
HAVING last_agg_hit_count() < 3

SELECT sum(a) AS s, device_id
FROM s
GROUP BY countwindow(4), device_id
HAVING sum(a) > 10 AND last_agg_hit_count() < 3
```

## Math functions

All math functions are scalar. They accept numeric arguments (`int*`, `uint*`, `float*`) and
return `float64` unless stated otherwise. Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`.

### `abs(x)`

Return the absolute value.

```sql
SELECT abs(-5)           -- 5
SELECT abs(price) FROM s
```

### Trigonometric functions

`sin(x)`, `cos(x)`, `tan(x)`, `cot(x)` — trigonometric functions (input in radians).
`asin(x)`, `acos(x)`, `atan(x)` — inverse trigonometric functions.
`atan2(y, x)` — two-argument arc tangent.
`sinh(x)`, `cosh(x)`, `tanh(x)` — hyperbolic functions.

```sql
SELECT sin(angle), cos(angle) FROM s
SELECT atan2(y, x) AS bearing FROM s
```

### `sqrt(x)`

Return the square root.

```sql
SELECT sqrt(area) FROM s
```

### `pow(x, y)` / `power(x, y)`

Return `x` raised to the power of `y`. `power` is an alias of `pow`.

```sql
SELECT pow(base, exp) FROM s
SELECT power(2, 10) FROM s
```

### `exp(x)`

Return `e` raised to `x`.

```sql
SELECT exp(rate) FROM s
```

### `ln(x)`

Return the natural logarithm (base `e`).

```sql
SELECT ln(ratio) FROM s
```

### `log(x)`

Return the common logarithm (base 10).

```sql
SELECT log(signal) FROM s
```

### `floor(x)`, `ceil(x)`, `ceiling(x)`

`floor` rounds down, `ceil`/`ceiling` round up. Return `float64`.

```sql
SELECT floor(3.7)      -- 3.0
SELECT ceil(3.2)       -- 4.0
SELECT ceiling(value) FROM s
```

### `round(x)`

Round to the nearest integer (toward zero for halves). Return `float64`.

```sql
SELECT round(2.5)  -- 2.0
SELECT round(3.5)  -- 3.0
```

### `sign(x)`

Return `-1.0`, `0.0`, or `1.0` based on the sign of `x`.

```sql
SELECT sign(delta) FROM s
```

### `radians(x)` / `degrees(x)`

Convert between degrees and radians.

```sql
SELECT radians(heading) FROM s
SELECT degrees(rad) FROM s
```

### `mod(x, y)`

Return the remainder of `x` divided by `y`.

```sql
SELECT mod(counter, 10) FROM s
```

### `pi()`

Return the constant π. Takes no arguments.

```sql
SELECT pi()
```

### `rand()`

Return a random `float64` in `[0, 1)`. Takes no arguments.

```sql
SELECT rand() FROM s
```

### `conv(num, from_base, to_base)`

Convert a numeric string between bases (2–36). Returns `string`.

```sql
SELECT conv('FF', 16, 10)   -- '255'
SELECT conv(hex_str, 16, 2) FROM s
```

### Bitwise functions

`bit_and(x, y)`, `bit_or(x, y)`, `bit_xor(x, y)`, `bit_not(x)` — bitwise operations
on integers. Return `int64`.

```sql
SELECT bit_and(flags, 0x0F) FROM s
SELECT bit_or(a, b), bit_xor(a, b) FROM s
```

## String functions

All string functions are scalar. Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`.

### `format(fmt, arg1, arg2, ...)`

Format a template string with positional `{}` placeholders. Variadic.

```sql
SELECT format('val={}', x) FROM s
SELECT format('{}.{}', a, b) FROM s
```

### `length(s)`

Return the character length of a string. Return `int64`.

```sql
SELECT length(name) FROM s
```

### `numbytes(s)`

Return the byte length of a string. Return `int64`.

```sql
SELECT numbytes(payload) FROM s
```

### `lower(s)` / `upper(s)`

Convert to lower/upper case.

```sql
SELECT lower(city), upper(code) FROM s
```

### `trim(s)`, `ltrim(s)`, `rtrim(s)`

Remove whitespace from both ends, left side, or right side.

```sql
SELECT trim(text), ltrim(text) FROM s
```

### `lpad(s, len, pad)` / `rpad(s, len, pad)`

Left/right pad `s` to length `len` using `pad` character.

```sql
SELECT lpad(id, 8, '0') FROM s
```

### `substring(s, start [, length])`

Extract a substring. `start` is 1-indexed. `length` is optional.

```sql
SELECT substring(title, 1, 10) FROM s
SELECT substring(vin, 5) FROM s
```

### `indexof(s, search)`

Return the 1-indexed position of `search` in `s`, or `0` if not found. Return `int64`.

```sql
SELECT indexof(path, '/') FROM s
```

### `startswith(s, prefix)` / `endswith(s, suffix)`

Check if `s` starts or ends with the given substring. Return `boolean`.

```sql
SELECT startswith(url, 'https') FROM s
SELECT endswith(filename, '.csv') FROM s
```

### `reverse(s)`

Return the reversed string.

```sql
SELECT reverse(code) FROM s
```

### `split_value(s, delimiter)`

Split `s` by `delimiter` and return a `list<string>`.

```sql
SELECT split_value(tags, ',') FROM s
```

### Regex functions

`regexp_matches(s, pattern)` — return `boolean` whether the regex matches.
`regexp_replace(s, pattern, replacement)` — replace matches.
`regexp_substr(s, pattern)` (alias `regexp_substring`) — extract the first match.

```sql
SELECT regexp_matches(email, '.*@.*') FROM s
SELECT regexp_replace(path, '/$', '') FROM s
SELECT regexp_substr(log, '\d+') FROM s
```

## Array functions

All array functions are scalar. Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`.

### `array_create(e1, e2, ...)`

Create a list from the given elements. Variadic.

```sql
SELECT array_create(1, 2, 3) FROM s
```

### `array_position(arr, elem)`

Return the 1-indexed position of `elem` in `arr`, or `0` if not found. Return `int64`.

### `array_last_position(arr, elem)`

Return the last 1-indexed position of `elem` in `arr`, or `0`. Return `int64`.

### `array_contains(arr, elem)`

Return `boolean` whether `arr` contains `elem`.

### `array_contains_any(arr, elems)`

Return `boolean` whether `arr` contains any element from `elems` (a list).

### `array_remove(arr, elem)`

Return a new list with all occurrences of `elem` removed.

### `element_at(arr, index)`

Return the element at `index` (1-indexed), or `NULL` if out of bounds.

```sql
SELECT element_at(items, 1) FROM s
```

### `repeat(elem, n)`

Return a list containing `elem` repeated `n` times.

```sql
SELECT repeat(0, 10) FROM s
```

### `sequence(start, stop [, step])`

Generate a list of integers from `start` to `stop` (inclusive) with optional `step`.

```sql
SELECT sequence(0, 9) FROM s
SELECT sequence(0, 100, 10) FROM s
```

### `array_concat(arr1, arr2)`

Concatenate two lists.

```sql
SELECT array_concat(a, b) FROM s
```

## Object functions

All object functions are scalar. Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`.

### `keys(obj)`

Return the top-level keys of `obj` as a `list<string>`.

### `values(obj)`

Return the top-level values of `obj` as a `list<any>` in key order.

### `items(obj)`

Return key-value pairs as a list of `struct(key, value)`.

### `object_size(obj)`

Return the number of top-level keys. Return `int64`.

### `object(obj)`

Cast a value to an object type.

```sql
SELECT keys(obj), values(obj) FROM s
```

### `object_concat(obj1, obj2)`

Merge two objects. Duplicate keys from `obj2` overwrite those from `obj1`.

### `object_construct(keys, values)`

Build an object from parallel `keys` and `values` lists.

```sql
SELECT object_construct(array_create('a', 'b'), array_create(1, 2))
```

### `object_pick(obj, key1, key2, ...)`

Return a new object containing only the specified keys. Variadic.

```sql
SELECT object_pick(sensor, 'temp', 'humidity') FROM s
```

### `erase(obj, key)`

Return a new object with the given key removed.

### `zip(arr1, arr2)`

Pair elements from two lists into a list of `struct(left, right)`.

### `obj_to_kv_pair_array(obj)`

Convert an object into a list of `struct(key, value)` pairs.

## Null and type functions

### `isnull(x)`

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Return `boolean`: `true` if `x` is `NULL`, `false` otherwise.
- Prefer the standard `x IS NULL` predicate in new SQL. The function form remains supported.

```sql
SELECT * FROM s WHERE isnull(error_code)
```

### `coalesce(x, y, ...)`

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Accept one or more arguments and return the first non-`NULL` argument. Return `NULL` when every
  argument is `NULL`.

```sql
SELECT coalesce(nickname, username, 'anonymous') AS display_name FROM s
SELECT * FROM s WHERE coalesce(primary_id, fallback_id) IS NOT NULL
```

### `cast(expr, target_type)`

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Cast `expr` to `target_type` (a string type name, e.g. `'int64'`, `'float64'`, `'string'`, `'bool'`).

```sql
SELECT cast(count_str, 'int64') FROM s
SELECT cast(price, 'string') FROM s
```

### `tstamp(s)`

- Kind: scalar
- Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`
- Parse a string as a timestamp. Returns `timestamp`.

```sql
SELECT tstamp('2025-01-15T10:30:00Z') FROM s
SELECT tstamp(ts_field) FROM s
```

## Hash and encoding functions

All scalar functions. Allowed clauses: `SELECT`, `WHERE`, `GROUP BY`.

### Hash functions

`md5(s)`, `sha1(s)`, `sha256(s)`, `sha384(s)`, `sha512(s)` — compute the hash of `s`
and return a hex-encoded `string`.

`crc32(s)` — compute CRC32 checksum and return an `int64`.

```sql
SELECT md5(payload), sha256(payload) FROM s
SELECT crc32(data) FROM s
```

### `encode(s, charset)` / `decode(s, charset)`

`encode` converts a string to bytes (`bytes` type) using the given charset (e.g. `'utf-8'`).
`decode` converts bytes back to a `string`.

```sql
SELECT encode(text, 'utf-8') FROM s
SELECT decode(raw, 'utf-8') FROM s
```

### `hex2dec(s)` / `dec2hex(n)`

Convert between hex string and decimal integer.

```sql
SELECT hex2dec('FF')   -- 255
SELECT dec2hex(255)    -- 'ff'
```

### `to_json(expr)` / `parse_json(s)`

`to_json` serializes a value to a JSON string.
`parse_json` parses a JSON string into a structured value.

```sql
SELECT to_json(obj) FROM s
SELECT parse_json(raw).field FROM s
```

## Miscellaneous scalar functions

### `chr(n)`

Return the character with Unicode code point `n`.

```sql
SELECT chr(65)   -- 'A'
```

### `trunc(x [, n])`

Truncate `x` to `n` decimal places (`n` defaults to 0). Return `float64`.

```sql
SELECT trunc(3.14159, 2)   -- 3.14
```

### `cardinality(expr)`

Return the number of elements in a list or bytes value. Return `int64`.

```sql
SELECT cardinality(items) FROM s
```

### `newuuid()`

Generate a random UUID v4 string. Takes no arguments.

```sql
SELECT newuuid() AS id FROM s
```

### `bypass(expr)`

Pass-through identity function. Return the argument unchanged.

```sql
SELECT bypass(x) FROM s
```

### `delay(expr)`

Delay a value by one row (processing-order lag, similar to `lag` but without stateful semantics).

```sql
SELECT delay(x) FROM s
```

## Stateful functions (continued)

Stateful functions maintain per-partition state across rows. They appear in
`SELECT` and `WHERE` clauses. Row ordering follows the pipeline processing order.

### Accumulators (`acc_*`)

Accumulate values over all rows seen so far in the partition.

- `acc_avg(x) -> float64` — running average.
- `acc_count(x) -> int64` — running count of non-`NULL` values.
- `acc_max(x) -> any` — running maximum.
- `acc_min(x) -> any` — running minimum.
- `acc_sum(x) -> numeric` — running sum.

All accumulators require exactly 1 argument and resume from scratch on pipeline
restart (no durable state).

```sql
SELECT acc_sum(amount) AS running_total FROM s
SELECT acc_avg(latency) AS running_avg FROM s
```

### Change detection

- `change_capture(x) -> boolean` — return `true` when `x` changes from the previous row.
- `change_to(x) -> any` — return the new value when `x` changes, `NULL` otherwise.
- `changed_col(col1, col2, ...) -> list<string>` — return the names of columns whose values changed. Variadic.
- `had_changed(x) -> boolean` — return `true` if `x` has ever changed since the partition started.

```sql
SELECT change_capture(status) AS status_changed FROM s
SELECT change_to(speed) AS new_speed FROM s
SELECT changed_col(temp, humidity, pressure) FROM s
SELECT * FROM s WHERE had_changed(mode)
```

### `consecutive_count(x)`

Return the consecutive count of identical values of `x`. Resets to 1 when the value
changes.

```sql
SELECT x, consecutive_count(x) AS run_length FROM s
```

### `consecutive_start(x)`

Return `true` when `consecutive_count` has reached `1` (i.e. at the start of a new run).

```sql
SELECT * FROM s WHERE consecutive_start(status)
```

### `latest(x)`

Return the most recent (latest) observed value of `x`. Updates on every row.

```sql
SELECT latest(speed) AS current_speed FROM s
```
