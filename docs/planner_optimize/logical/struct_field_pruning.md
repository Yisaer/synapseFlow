# StructFieldPruning

## Overview

`StructFieldPruning` shrinks **nested datatypes** so that struct-typed values only keep the fields
that are actually referenced by SQL expressions.

In the current implementation, this rule also prunes `list<struct>` by shrinking the **element
struct** when the SQL accesses `items[...]->field` (the index does not matter for schema shrink).

Implementation: `src/flow/src/planner/logical_optimizer.rs` (`StructFieldPruning`).

## Inputs (What It Recognizes)

This rule scans the same logical nodes as `TopLevelColumnPruning` and recognizes nested accesses
through:
- `JsonAccess (->)` for struct field access (`b->c`)
- `MapAccess` for list indexing (`items[0]`, `items[i]`)

Internally it parses nested access chains into a field-path model and maps any list index segment
to a schema path segment named `element`.

## Outputs (What It Produces)

### Regular sources

- Updates the `SchemaBinding` by rewriting column datatypes:
  - `struct{...}` keeps only referenced fields
  - `list<...>` keeps list length/type shape, but rewrites the element datatype when nested usage
    exists (via the `element` pseudo-field in the usage tree)
- Applies the pruned schema back into `LogicalPlan::DataSource.schema`.

### Shared sources

- Pruning is disabled (keeps full schema) to avoid cross-consumer inconsistency.

## Degrade / Safety Behavior

Same safety rules as top-level pruning apply:
- wildcard and ambiguity disable pruning for the affected source(s)
- unresolved qualifiers disable pruning

This rule does not decide list index selection. It only shrinks datatypes based on referenced
fields.

## Explain Walkthrough (Continuation)

Using the same schema and SQL as in `top_level_column_pruning.md`:

After `StructFieldPruning` (regular source):
- `schema=[a, b{items[struct{x}]}]`
  - `b.c` is removed because it is not referenced
  - `items` element struct keeps only `x` because only `->x` is referenced
- `shared_required_schema` is still `None` (regular source)
- `decode_projection` is still `None` (produced by `ListElementPruning`)

