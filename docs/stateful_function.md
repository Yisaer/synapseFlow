# Stateful Functions

## Background

veloFlux currently supports:

- Pure scalar expressions evaluated per row (stateless).
- Aggregate functions evaluated over windows/batches (stateful across rows within a grouping/window).

We also need to support **stateful functions** inside scalar expressions, where the output depends on:

- The current input arguments.
- The function's internal state, which evolves from:
  - An initialization state, and then
  - The state produced by the previous invocation.

Example:

```sql
SELECT lag(a) FROM stream
```

`lag(a)` returns the value of column `a` from the **previous** record in the stream.

Stateful functions are similar to aggregate functions in that they require a registry and a dedicated
execution stage, but they differ in that they are evaluated **per row** and must preserve ordering.

## Goals

- Add a `stateful_registry` to `FlowInstance` for registering and resolving stateful functions.
- During parsing, extract stateful function calls from expressions into `SelectStmt.stateful_mappings`
  using the same placeholder strategy as aggregates (`col_$index`), and rewrite the SQL AST to
  reference placeholders.
- Add `LogicalPlan::StatefulFunction` and `PhysicalPlan::StatefulFunction` nodes.
- Implement `StatefulFunctionProcessor` to evaluate stateful calls and attach outputs as derived
  columns (affiliate columns).

## Non-goals (Initial Scope)

- Stateful functions in `HAVING`.
- Persisting stateful function state across pipeline restarts.
- Distributed/exactly-once semantics for stateful function state.

## Key Design Decisions

### 1) Placeholder strategy and collision avoidance

Both aggregate and stateful rewrites use placeholder identifiers in the SQL AST such as `col_1`,
`col_2`, etc. These placeholders must not collide across rewrite passes.

We introduce a shared allocator in the parser module:

- `ColPlaceholderAllocator` owns the monotonically increasing counter.
- Aggregate rewrite and stateful rewrite both take `&mut ColPlaceholderAllocator`.
- This guarantees `col_$index` uniqueness within a single parsed query.

### 2) Deduplication of repeated stateful calls

For repeated occurrences of the same stateful function call:

```sql
SELECT lag(a), lag(a) FROM stream
```

We want:

- Only one mapping entry in `SelectStmt.stateful_mappings`.
- Both occurrences rewritten to the same placeholder:

```sql
SELECT col_1, col_1 FROM stream
```

This ensures the function is computed only once per row in the execution pipeline.

### 3) Plan placement

Stateful functions must be evaluated in a deterministic order before windowing and aggregation.
The plan topology is:

1. `DataSource(s)`
2. `StatefulFunction` (if any)
3. `Window` (if any)
4. `Aggregation` (if any)
5. `Filter` (WHERE, if any)
6. `Project`
7. `Sink/Tail`

This satisfies:

- `WHERE` can reference stateful placeholders because stateful is upstream of filter.
- Windowing sees the derived stateful columns if needed in later stages.

## Parser Changes

### `SelectStmt` extension

Add:

- `stateful_mappings: HashMap<String, Expr>` where the key is `col_$index` and the value is the
  original `sqlparser::ast::Expr::Function(...)`.

### Stateful rewrite pass

Add a transformer similar to `aggregate_transformer`:

- Input: `SelectStmt`, `Arc<dyn StatefulRegistry>`, `&mut ColPlaceholderAllocator`
- Visit and rewrite only:
  - `select_fields[*].expr`
  - `where_condition` (if present)
- For each `Expr::Function` that matches `StatefulRegistry::is_stateful_function(name)`:
  - Compute a stable deduplication key (e.g. `expr.to_string()`).
  - If seen before, replace with the existing placeholder.
  - Otherwise allocate a new `col_$index`, record `stateful_mappings[col] = original_expr`,
    and replace with `Expr::Identifier(col)`.

## Registries and Runtime APIs

### Parser-facing trait

Parser only needs to know whether a function name is stateful:

- `parser::stateful_registry::StatefulRegistry { fn is_stateful_function(&self, name: &str) -> bool }`

### Flow runtime registry

In `flow` we introduce a full registry capable of instantiating implementations:

- `StatefulFunctionRegistry`
  - `register_function(Arc<dyn StatefulFunction>)`
  - `get(name) -> Option<Arc<dyn StatefulFunction>>`
  - `is_registered(name) -> bool`

And a stateful function contract:

- `StatefulFunction`
  - `name()`
  - `return_type(input_types)`
  - `create_instance() -> Box<dyn StatefulFunctionInstance>`
- `StatefulFunctionInstance`
  - `eval(&mut self, args: &[Value]) -> Result<Value, String>`

The `FlowInstance` and `PipelineRegistries` carry `Arc<StatefulFunctionRegistry>`.

## Planner Changes

### Logical plan

Add:

- `LogicalPlan::StatefulFunction(StatefulFunctionPlan)`
- `StatefulFunctionPlan { stateful_mappings, base }`

Inserted after `DataSource` and before `Window`.

## Rewrite Order Note

The parser applies rewrites in this order:

1. Stateful rewrite (so nested calls like `last_row(lag(a))` get their `lag(a)` replaced first)
2. Aggregate rewrite

Both rewrites share the same `ColPlaceholderAllocator` to ensure `col_$index` does not collide.

### Physical plan

Add:

- `PhysicalPlan::StatefulFunction(PhysicalStatefulFunctionPlan)`
- `PhysicalStatefulFunctionPlan` contains a list of compiled calls:
  - `output_column: String` (e.g. `col_3`)
  - `func_name: String` (lowercased)
  - `arg_scalars: Vec<ScalarExpr>`

Physical plan builder:

- Compiles each `Expr::Function` arguments into `ScalarExpr` with schema bindings.
- Resolves each function implementation from the `StatefulFunctionRegistry`.

## Processor Changes

### `StatefulFunctionProcessor`

Add a processor similar to `FilterProcessor` / `AggregationProcessor`:

- It receives upstream `Collection` batches.
- For each row, it evaluates each unique `StatefulCall` once.
- Outputs are written to the tuple affiliate with key `col_$index` (no prefix), consistent with
  aggregate placeholders and existing derived-column semantics.
- Instances are stored in the processor so state persists across incoming batches.

### Example: `lag(x)`

Semantics:

- First row: output `NULL`
- Row N: output the value observed at row N-1

The instance holds a single `Value` as previous value and updates it each row.

## Validation

Add focused tests that assert:

- Parser rewrite:
  - Extracts stateful calls from SELECT and WHERE.
  - Deduplicates repeated calls.
  - Does not collide placeholders with aggregates.
- Planner topology:
  - `StatefulFunction` is inserted between `DataSource` and `Window`.
- End-to-end evaluation:
  - `SELECT lag(a) FROM stream` yields `[NULL, a1, a2, ...]` for an input sequence.
