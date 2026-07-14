# Sink Output Behavior

This directory contains sink output modes and branch-level delivery policies.

## Features

- [Row Diff Output](row_diff_output.md) — `output.mode=delta` with tracked column change detection
- [Omit If Empty](omit_if_empty.md) — `output.omit_if_empty` suppression of empty diff rows
- [Column Filter](column_filter.md) — `output.include_columns` / `output.exclude_columns` per-sink column selection
