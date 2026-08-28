# `merge_agg` Aggregate Function

## Syntax

```sql
merge_agg(*)
merge_agg(struct_expression)
```

`merge_agg` performs a shallow merge of struct values across a group or window. Fields retain their
first-seen order. When the same top-level field occurs more than once, the value from the later row
overwrites the earlier value. Nested structs are replaced as whole values and are not merged
recursively.

The function accepts exactly one argument. `merge_agg(*)` evaluates the current row as a struct,
while `merge_agg(struct_expression)` merges the structs produced by that expression. Top-level
arguments that are `NULL` or are not structs are ignored. The function returns `NULL` if no struct
input is observed.

## NULL And Missing Fields

A `NULL` value inside an input struct is a value and overwrites an earlier value for the same field.
This is distinct from a top-level `NULL` argument, which is ignored.

veloFlux uses schema-driven decoding. A missing JSON field is decoded as `NULL`, so the aggregate
cannot distinguish a missing field from an explicitly supplied JSON `null`. In both cases, the
decoded `NULL` overwrites an earlier value for that field.

## Example

Given these rows in arrival order:

```json
{"a": 1, "b": 2, "c": null}
{"a": 1, "b": 3, "c": 4}
{"a": 5, "b": 3, "c": 4}
```

The query:

```sql
SELECT merge_agg(*) AS result
FROM stream
GROUP BY tumblingwindow('ss', 10)
```

produces the logical result:

```json
{
  "result": {
    "a": 5,
    "b": 3,
    "c": 4
  }
}
```
