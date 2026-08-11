# Pipeline State

## Background

In a streaming pipeline, there is often a need to observe pipeline-level runtime state —
quantities that are not derived from any single input row but rather reflect the cumulative
history of data flowing through the pipeline. Examples include:

- How many rows have been delivered to the sink so far (`last_hit_count`).
- How many non-empty aggregate result collections have passed `HAVING` so far
  (`last_agg_hit_count`).
- The timestamp of the previous operator-local hit (`last_hit_time_unix_ms`).
- (Future) The number of rows currently in-flight between two processors.

We call this concept **Pipeline State**: counters, timestamps, or other metrics that are
updated as data moves through the pipeline and can be queried from within SQL expressions.

### Example

```sql
SELECT last_hit_count() FROM stream WHERE last_hit_count() < 3
```

The user expects `last_hit_count()` to return how many rows have already been delivered to
the sink, and uses it in WHERE to limit total output to 3 rows.

---

## Problem: Why True Pipeline State Is Hard

True pipeline state — a counter whose value exactly equals the number of rows that have
reached the sink — is difficult to implement in a batch-oriented async pipeline:

1. **Write point is at the sink.** The counter can only be accurately incremented after the
   sink connector confirms the write.

2. **Read point may be upstream.** In `WHERE last_hit_count() < 3`, the counter must be
   read at the Filter processor, which is many stages before the sink.

3. **Batch semantics.** Multiple rows in a single `Collection` arrive at Filter
   simultaneously. If the counter lives at the sink, all rows in the batch would read the
   same stale value, losing row-level precision. Serializing the pipeline (one row at a
   time with sink ack) would devastate throughput.

4. **Intermediate data loss.** Sink-side row-diff paths (`output.mode=delta`) can change the
   sink-facing output after Filter by attaching `output_mask` metadata, and `omit_if_empty` after
   RowDiff can suppress all-false mask collections. A Filter-level counter would overcount relative
   to sink delivery on those paths.

---

## Solution: Approximate Pipeline State via Processor State

We do not implement a true cross-processor pipeline state. Instead, we **approximate** it
with **Processor State** — a counter maintained locally within specific processors —
backed by two design premises that guarantee the approximation is exact in all supported
configurations.

### The Idea

Each processor that needs to read pipeline state (Filter for WHERE/HAVING, Project for SELECT)
holds its own local `ProcessorState` containing shared `AtomicU64` counters. A counter is:

- **Read** by `ScalarExpr` during expression evaluation (lock-free, via `AtomicU64::load`).
- **Written** by the processor at the function-specific hit boundary
  (via `AtomicU64::fetch_add`).

Because the ScalarExpr and the processor share the same `Arc<AtomicU64>`, later evaluations in the
same processor can observe updates without sink ack, serialization, or cross-processor coordination.

### The Two Premises That Make the Approximation Exact

This approximation is only as good as the gap between "rows accepted by Filter" and "rows
delivered to Sink." To close that gap we enforce two constraints:

#### Premise 1: Filter is the last data-dropping processor before Sink

The default physical plan order is:

```
DataSource → StatefulFunction → Window → Aggregation → Filter → Order → Project → Sink
```

Between Filter and Sink:

| Processor | Drops rows? | When? |
|---|---|---|
| Order | No | |
| Project | No | |
| RowDiff | Unsafe | `output.mode=delta` creates row-diff output masks after Filter — rejected |
| EmptySuppress on full output | No accepted-row loss | suppresses zero-row collections only — allowed |
| EmptySuppress after RowDiff | Yes | can suppress all-false `output_mask` collections — rejected by the delta rule |

RowDiff is the post-Filter path that can make processor-local hit counters diverge from
sink-facing delivery. The planner rejects queries using pipeline state functions when
`output.mode=delta` is active.

Full-output `output.omit_if_empty=true` remains supported because it suppresses only zero-row
collections. `last_hit_count()` increments only for accepted rows, and `last_agg_hit_count()`
increments only for non-empty filtered collections, so suppressing zero-row collections does not
change the counters' supported meaning.

#### Premise 2: Pipeline state functions are only allowed at their owning processor positions

| Allowed context | Processor | What the counter means |
|---|---|---|
| `SELECT` fields (`last_hit_count`) | Project | Rows that produced output (post-projection) |
| `WHERE` conditions (`last_hit_count`) | Filter | Rows that passed the filter condition |
| `HAVING` conditions (`last_agg_hit_count`) | Filter after aggregation | Non-empty aggregate result collections that passed `HAVING` |

All other SQL clauses are rejected:

| Rejected context | Reason |
|---|---|
| `HAVING` for `last_hit_count` | Uses aggregate-result collections instead of row hits |
| `SELECT` / `WHERE` for `last_agg_hit_count` | Not evaluated by the aggregate-result filter |
| `ORDER BY` expressions | Only reorders, counter update meaningless |
| `GROUP BY` expressions | Before Filter, would count pre-filter rows |
| Stateful function arguments / FILTER / PARTITION BY | StatefulFunction processor runs before Filter |
| Aggregate function arguments | Aggregation runs before the `HAVING` filter |

The restriction guarantees that each pipeline state function is only read and written by the
processor that owns its hit semantics.

### Why Separate Counters Per Processor

Filter and Project each own independent `ProcessorState` instances. They are not shared
because their increment points differ:

- **Filter** increments after evaluating the WHERE condition and accepting the row.
- **Project** increments after evaluating the SELECT expression for the row.

Since no data loss occurs between Filter and Project, the two counters are always equal
after each batch completes. Keeping them independent avoids coupling and makes the
increment semantics clear per processor.

---

## First Consumer: `last_hit_count()`

`last_hit_count()` is the first SQL function that reads Pipeline State. It returns the
number of rows that have passed through the current processor position, which — under
Premise 1 and 2 — equals the number of rows delivered to the sink.

Syntax:

```sql
SELECT last_hit_count() FROM stream WHERE last_hit_count() < 3
```

- Zero arguments.
- No `OVER (PARTITION BY ...)` or `FILTER (WHERE ...)`.
- Allowed only in `SELECT` fields and `WHERE` conditions.

Future consumers will follow the same pattern, adding new fields to `ProcessorState`
and new variants to `ProcStateField`.

---

## HAVING Consumer: `last_agg_hit_count()`

`last_agg_hit_count()` reads aggregate-filter pipeline state. It returns the number of previous
non-empty collections emitted by the `HAVING` filter.

Syntax:

```sql
SELECT sum(a)
FROM stream
GROUP BY countwindow(4)
HAVING last_agg_hit_count() < 3
```

- Zero arguments.
- No `OVER (PARTITION BY ...)` or `FILTER (WHERE ...)`.
- Allowed only in `HAVING`.
- Not an aggregate function and not registered in the aggregate function registry.
- Counts non-empty filtered collections, not rows inside those collections.

For grouped aggregate output, one window can produce multiple rows in the same aggregate result
collection. If that collection remains non-empty after `HAVING`, `last_agg_hit_count` increments by
one regardless of how many grouped rows passed.

Example:

```sql
SELECT sum(a) AS s, device_id
FROM stream
GROUP BY countwindow(4), device_id
HAVING last_agg_hit_count() < 1
```

If the first finalized window produces two `device_id` groups and both pass `HAVING`, both rows are
emitted and the counter increments from `0` to `1` only after the collection is filtered. The next
aggregate result collection sees `last_agg_hit_count() = 1` and fails the predicate above.

---

## Layer Design

### 1. Parser Layer

`last_hit_count()` and `last_agg_hit_count()` are recognized as built-in pipeline state functions
with zero arguments.
Unlike stateful functions, they are **not** rewritten into placeholder columns — they pass through as
unmodified `Expr::Function` nodes.

The planner validates that each function appears only in its allowed context:

- `last_hit_count()` in `SELECT` fields and `WHERE` conditions.
- `last_agg_hit_count()` in `HAVING`.

### 2. Eval Layer (`ScalarExpr`)

#### 2.1 Data Structures

```rust
// src/flow/src/processor/processor_state.rs

pub struct ProcessorState {
    pub last_hit_count: Arc<AtomicU64>,
    pub last_agg_hit_count: Arc<AtomicU64>,
}
```

```rust
// src/flow/src/expr/scalar.rs

pub enum ProcStateField {
    LastHitCount,
    LastAggHitCount,
}

pub enum ScalarExpr {
    // ... existing variants ...

    /// Reads a value from processor-local state.
    ProcessorState {
        state: Arc<ProcessorState>,
        field: ProcStateField,
    },
}
```

`ProcessorState` is designed for extension: adding another pipeline state field only
requires a new field in `ProcessorState` and a new variant in `ProcStateField`.
`ScalarExpr` stays unchanged.

#### 2.2 Evaluation

`eval_with_tuple` reads the counter without any mutable access:

```rust
ScalarExpr::ProcessorState { state, field } => match field {
    ProcStateField::LastHitCount => {
        Ok(Value::Uint64(state.last_hit_count.load(Ordering::Relaxed)))
    }
    ProcStateField::LastAggHitCount => {
        Ok(Value::Uint64(state.last_agg_hit_count.load(Ordering::Relaxed)))
    }
},
```

The `&self` receiver is sufficient — `AtomicU64::load` is lock-free.

#### 2.3 PipelineState → Injection Strategy

`ProcessorState` is created during physical plan building, but pipeline state functions must
first pass through SQL-to-ScalarExpr conversion (`sql_conversion`), which has no access to
`ProcessorState`. We solve this with a two-step approach:

**Step 1 — `sql_conversion`:** Convert the SQL function into an unresolved pipeline-state read.

```rust
ScalarExpr::PipelineState { field }
```

**Step 2 — `physical_plan_builder`:** After creating `ProcessorState`, walk the expression
tree and replace every unresolved pipeline-state read with the real variant.

```rust
fn inject_processor_state(expr: &mut ScalarExpr, state: &Arc<ProcessorState>) {
    match expr {
        ScalarExpr::PipelineState { field } => {
            *expr = ScalarExpr::ProcessorState {
                state: Arc::clone(state),
                field: field.clone(),
            };
        }
        ScalarExpr::CallBinary { expr1, expr2, .. } => {
            inject_processor_state(expr1, state);
            inject_processor_state(expr2, state);
        }
        // ... recurse into all expression children ...
        _ => {}
    }
}
```

### 3. Planner Layer

#### 3.1 PhysicalFilter

```rust
pub struct PhysicalFilter {
    // ... existing fields ...
    pub processor_state: Option<Arc<ProcessorState>>,
}
```

#### 3.2 PhysicalProject

```rust
pub struct PhysicalProject {
    // ... existing fields ...
    pub processor_state: Option<Arc<ProcessorState>>,
}
```

Each receives an independent `ProcessorState`. The builder creates `ProcessorState` when it
detects `ScalarExpr::PipelineState` in the Filter predicate or Project expressions.

#### 3.3 Builder checks

- Create `ProcessorState`, inject into expressions via `inject_processor_state`.
- Store `ProcessorState` in the corresponding `PhysicalFilter` / `PhysicalProject` node.
- Reject the query if the sink uses `output.mode=delta`.
- Allow full-output `output.omit_if_empty=true`; it only suppresses zero-row collections.
- Reject if a pipeline state function appears in an unsupported clause.

### 4. Processor Layer

#### 4.1 FilterProcessor

When the filter predicate references `last_hit_count()`, switch from bulk
`collection.apply_filter()` to row-by-row iteration and increment after each accepted row:

```rust
fn apply_filter_with_state(
    input: &dyn Collection,
    predicate: &ScalarExpr,
    state: &ProcessorState,
) -> Result<Box<dyn Collection>, ProcessorError> {
    let mut kept = Vec::with_capacity(input.num_rows());
    for tuple in input.rows() {
        if matches!(predicate.eval_with_tuple(tuple)?, Value::Bool(true)) {
            state.last_hit_count.fetch_add(1, Ordering::Relaxed);
            kept.push(tuple.clone());
        }
    }
    Ok(Box::new(RecordBatch::new(kept)?))
}
```

Row-level precision: row N+1 reads the counter already incremented by row N.

When the filter predicate references `last_agg_hit_count()`, the processor also uses row-by-row
predicate evaluation, but increments at collection scope:

```rust
let mut kept = Vec::with_capacity(input.num_rows());
for tuple in input.rows() {
    if matches!(predicate.eval_with_tuple(tuple)?, Value::Bool(true)) {
        kept.push(tuple.clone());
    }
}
if !kept.is_empty() {
    state.last_agg_hit_count.fetch_add(1, Ordering::Relaxed);
}
```

All rows in the same aggregate result collection read the same pre-collection value. The counter is
updated only after the filtered collection is known to be non-empty.

#### 4.2 ProjectProcessor

When `processor_state` is `Some`, increment the counter after each row is projected.

---

## Execution Walkthrough

```
SELECT last_hit_count() FROM stream WHERE last_hit_count() < 3
```

**Physical Plan:**

```
DataSource
  → Filter  { predicate: ProcState(hit_count) < 3, state: A }
    → Project { expressions: [ProcState(hit_count)], state: B }
      → Sink
```

**Input:** batch of 4 rows. Both counters start at `0`.

**FilterProcessor:**

```
Row 1: state A.load() = 0, 0 < 3 → pass → A.fetch_add(1) → A = 1, kept
Row 2: state A.load() = 1, 1 < 3 → pass → A.fetch_add(1) → A = 2, kept
Row 3: state A.load() = 2, 2 < 3 → pass → A.fetch_add(1) → A = 3, kept
Row 4: state A.load() = 3, 3 < 3 → fail → dropped
```

Output: 3 rows.

**ProjectProcessor:**

```
Row 1: state B.load() = 0 → output 0 → B.fetch_add(1) → B = 1
Row 2: state B.load() = 1 → output 1 → B.fetch_add(1) → B = 2
Row 3: state B.load() = 2 → output 2 → B.fetch_add(1) → B = 3
```

Output: `[0, 1, 2]`.

Both counters converge to `3` after the batch completes.

`last_hit_time_unix_ms()` uses the same operator-local scope model, but stores
the previous hit timestamp as Unix milliseconds. The expression reads the old
timestamp first, then the owning operator updates it after a hit:

- `ProjectProcessor`: every projected row is a hit, so the state is updated
  after the row's `SELECT` fields are evaluated.
- `FilterProcessor`: only accepted rows are hits, so the state is updated after
  the `WHERE` predicate returns `true`.
- `slidingwindow OVER (WHEN ...)`: only trigger rows are hits, so the state is
  updated after the trigger condition returns `true`.

`HAVING last_hit_time_unix_ms()` is currently rejected.

---

## Restrictions

| Category | Restriction |
|---|---|
| Sink config | Reject `output.mode=delta` (RowDiff/output-mask semantics run after Filter) |
| Sink config | Allow full-output `output.omit_if_empty=true` |
| SQL context | `last_hit_count()` is allowed only in `SELECT` fields and `WHERE` conditions |
| SQL context | `last_agg_hit_count()` is allowed only in `HAVING` |
| SQL context | `last_hit_time_unix_ms()` is allowed in `SELECT`, `WHERE`, and `slidingwindow OVER (WHEN ...)`; `HAVING` is rejected |
| Syntax | No `OVER (PARTITION BY ...)` or `FILTER (WHERE ...)` |
| Lifecycle | Pipeline state fields reset to 0 on pipeline (re)start |

---

## Future Work

- Support `output.mode=delta` by placing the counter at RowDiff instead of Filter.
- Expose pipeline state counters as observable metrics.
- (Dropped for now) True cross-processor pipeline state with sink ack. Deferred due to the
  throughput vs. accuracy tradeoff described in [Problem](#problem-why-true-pipeline-state-is-hard).
