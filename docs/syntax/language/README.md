# Language Semantics

This directory contains SQL-visible semantics that are not specific to one connector or runtime
deployment detail.

- `pipeline_state.md`: Pipeline State concept — approximating pipeline-level runtime counters
  via processor-local state. Covers `last_hit_count()` and `last_agg_hit_count()`.
- `stateful_functions.md`: Stateful function semantics (lag, latest, acc_*, etc.).
- `alias_computing.md`: Alias resolution and computed column semantics.
- `null_predicates.md`: Standard `IS NULL` and `IS NOT NULL` predicate semantics and lowering.
