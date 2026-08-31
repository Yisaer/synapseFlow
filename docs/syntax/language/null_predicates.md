# NULL Predicates

veloFlux supports the standard SQL predicates:

```sql
<expr> IS NULL
<expr> IS NOT NULL
```

Both predicates accept any expression and always produce a boolean value. `IS NULL` returns `true`
only when its operand evaluates to `NULL`. `IS NOT NULL` returns the inverse result.

During parsing, veloFlux lowers the standard syntax to the existing scalar expression forms:

- `x IS NULL` becomes `isnull(x)`.
- `x IS NOT NULL` becomes `NOT isnull(x)`.

Lowering happens before stateful and aggregate function rewriting. As a result, predicates around
nested function calls retain the same rewrite and execution behavior as other scalar expressions.
The exposed name of an unaliased projection remains the original SQL expression rather than the
lowered internal expression.

For example, `coalesce` can be used to test whether at least one candidate value is present:

```sql
SELECT coalesce(primary_id, fallback_id) IS NOT NULL AS has_id
FROM events
```
