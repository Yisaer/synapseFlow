# Pipeline State

## Background

In a streaming pipeline, there is often a need to observe pipeline-level runtime state —
quantities that are not derived from any single input row but rather reflect the cumulative
history of data flowing through the pipeline. Examples include:

- How many rows have been delivered to the sink so far (`last_hit_count`).
- The timestamp of the most recent row that reached the sink (`last_hit_ts`).
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

4. **Intermediate data loss.** Processors such as RowDiff (`output.mode=delta`) or
   EmptySuppress (`output.omit_if_empty=true`) can drop rows between Filter and Sink,
   meaning a Filter-level counter would overcount relative to the sink.

---

## Solution: Approximate Pipeline State via Processor State

We do not implement a true cross-processor pipeline state. Instead, we **approximate** it
with **Processor State** — a counter maintained locally within specific processors —
backed by two design premises that guarantee the approximation is exact in all supported
configurations.

### The Idea

Each processor that needs to read pipeline state (Filter for WHERE, Project for SELECT)
holds its own local `ProcessorState` containing a shared `AtomicU64` counter. The counter is:

- **Read** by `ScalarExpr` during expression evaluation (lock-free, via `AtomicU64::load`).
- **Written** by the processor after each row it accepts (via `AtomicU64::fetch_add`).

Because the ScalarExpr and the processor share the same `Arc<AtomicU64>`, a row N+1 in the
same batch immediately sees the counter incremented by row N — no ack, no serialization,
no cross-processor coordination.

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
| RowDiff | Yes | `output.mode=delta` — rejected |
| EmptySuppress | Yes | `output.omit_if_empty=true` — rejected |

RowDiff and EmptySuppress are the only post-Filter processors that can discard rows, and
both are tied to specific sink configurations. The planner rejects queries using pipeline
state functions when these configurations are active.

Therefore, in all **supported** configurations: every row that survives Filter reaches Sink.

#### Premise 2: Pipeline state functions are only allowed in SELECT and WHERE

| Allowed context | Processor | What the counter means |
|---|---|---|
| `SELECT` fields | Project | Rows that produced output (post-projection) |
| `WHERE` conditions | Filter | Rows that passed the filter condition |

All other SQL clauses are rejected:

| Rejected context | Reason |
|---|---|
| `HAVING` (separate processor) | Sits on a different evaluation path |
| `ORDER BY` expressions | Only reorders, counter update meaningless |
| `GROUP BY` expressions | Before Filter, would count pre-filter rows |
| Stateful function arguments / FILTER / PARTITION BY | StatefulFunction processor runs before Filter |
| Aggregate function arguments | Aggregation runs before Filter |

The restriction guarantees that pipeline state is only read/written at processors that lie
on the single non-dropping path from Filter to Sink.

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

Future consumers (e.g. `last_hit_ts(column)`) will follow the same pattern, adding new
fields to `ProcessorState` and new variants to `ProcStateField`.

---

## Layer Design

### 1. Parser Layer

`last_hit_count()` is recognized as a built-in scalar function with zero arguments.
Unlike stateful functions, it is **not** rewritten into a placeholder column — it passes
through as an unmodified `Expr::Function` node.

The parser validates that the function appears only in allowed contexts (`SELECT` fields,
`WHERE` conditions) and rejects it elsewhere.

### 2. Eval Layer (`ScalarExpr`)

#### 2.1 Data Structures

```rust
// src/flow/src/processor/processor_state.rs

pub struct ProcessorState {
    pub last_hit_count: Arc<AtomicU64>,
}
```

```rust
// src/flow/src/expr/scalar.rs

pub enum ProcStateField {
    LastHitCount,
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

`ProcessorState` is designed for extension: adding `last_hit_ts` later only requires a new
field in `ProcessorState` and a new variant in `ProcStateField`. `ScalarExpr` stays
unchanged.

#### 2.2 Evaluation

`eval_with_tuple` reads the counter without any mutable access:

```rust
ScalarExpr::ProcessorState { state, field } => match field {
    ProcStateField::LastHitCount => {
        Ok(Value::Uint64(state.last_hit_count.load(Ordering::Relaxed)))
    }
},
```

The `&self` receiver is sufficient — `AtomicU64::load` is lock-free.

#### 2.3 Placeholder → Injection Strategy

`ProcessorState` is created during physical plan building, but `last_hit_count()` must
first pass through SQL-to-ScalarExpr conversion (`sql_conversion`), which has no access to
`ProcessorState`. We solve this with a two-step approach:

**Step 1 — `sql_conversion`:** Convert `last_hit_count()` to a temporary placeholder.

```rust
ScalarExpr::Placeholder
```

**Step 2 — `physical_plan_builder`:** After creating `ProcessorState`, walk the expression
tree and replace every placeholder with the real variant.

```rust
fn inject_processor_state(expr: &mut ScalarExpr, state: &Arc<ProcessorState>) {
    match expr {
        ScalarExpr::Placeholder => {
            *expr = ScalarExpr::ProcessorState {
                state: Arc::clone(state),
                field: ProcStateField::LastHitCount,
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
detects `ScalarExpr::Placeholder` in the Filter predicate or Project expressions.

#### 3.3 Builder checks

- Create `ProcessorState`, inject into expressions via `inject_processor_state`.
- Store `ProcessorState` in the corresponding `PhysicalFilter` / `PhysicalProject` node.
- Reject the query if the sink uses `output.mode=delta` or `output.omit_if_empty=true`.
- Reject if `last_hit_count()` appears in unsupported clauses (parsed but caught at plan
  time if parser missed it).

### 4. Processor Layer

#### 4.1 FilterProcessor

When `processor_state` is `Some`, switch from bulk `collection.apply_filter()` to
row-by-row iteration:

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

---

## Restrictions

| Category | Restriction |
|---|---|
| Sink config | Reject `output.mode=delta` (RowDiff drops rows after Filter) |
| Sink config | Reject `output.omit_if_empty=true` (EmptySuppress drops collections) |
| SQL context | Only `SELECT` fields and `WHERE` conditions allowed |
| Syntax | No `OVER (PARTITION BY ...)` or `FILTER (WHERE ...)` |
| Lifecycle | Counter resets to 0 on pipeline (re)start |

---

## Future Work

- `last_hit_ts(column)`: add `last_hit_ts: Arc<RwLock<Option<Value>>>` to `ProcessorState`
  and `LastHitTs` to `ProcStateField`.
- Support `output.mode=delta` by placing the counter at RowDiff instead of Filter.
- Expose pipeline state counters as observable metrics.
- (Dropped for now) True cross-processor pipeline state with sink ack. Deferred due to the
  throughput vs. accuracy tradeoff described in [Problem](#problem-why-true-pipeline-state-is-hard).
