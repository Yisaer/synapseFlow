//! Micro-benchmarks comparing Dense vs Projected Message performance for
//! wide-schema shared stream scenarios (e.g. 10 000 columns, 3 active).

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datatypes::Value;
use flow::model::{Message, ProjectedLayout};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

/// Schema width for wide-schema benchmarks.
const SCHEMA_WIDTH: usize = 10_000;
/// Number of actively-decoded columns in the projected benchmarks.
const ACTIVE_COLUMNS: usize = 3;
/// Number of rows per benchmark iteration.
const ROWS_PER_ITER: usize = 1_000;
/// RNG seed for reproducibility.
const SEED: u64 = 0xdead_beef_2025_0601;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build full source-schema keys: `["col_0", "col_1", ..., "col_N-1"]`.
fn build_schema_keys(n: usize) -> Arc<[Arc<str>]> {
    let keys: Vec<Arc<str>> = (0..n)
        .map(|i| Arc::<str>::from(format!("col_{i}")))
        .collect();
    Arc::from(keys)
}

/// Active column names: 3 columns spread across the schema.
fn active_column_names() -> Vec<String> {
    vec![
        "col_0".to_string(),
        format!("col_{}", SCHEMA_WIDTH / 2),
        format!("col_{}", SCHEMA_WIDTH - 1),
    ]
}

/// Build a `ProjectedLayout` for the given schema keys and active columns.
fn build_layout(keys: &[Arc<str>], active: &[String]) -> Arc<ProjectedLayout> {
    Arc::new(ProjectedLayout::from_active_columns(keys, active))
}

/// Create an `Arc<Value>` with a synthetic Int64 value.
fn val(i: i64) -> Arc<Value> {
    Arc::new(Value::Int64(i))
}

/// Source name used for all messages in the benchmarks.
fn source_name() -> Arc<str> {
    Arc::<str>::from("bench_stream")
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

fn bench_build_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_build");
    let keys = build_schema_keys(SCHEMA_WIDTH);
    let active = active_column_names();
    let layout = build_layout(&keys, &active);

    // Dense: full-width values (SCHEMA_WIDTH elements per row).
    group.bench_function(BenchmarkId::new("dense", SCHEMA_WIDTH), |b| {
        b.iter_batched(
            || {
                // Build full-width values: active columns get data, rest get Null.
                let mut values = vec![Arc::new(Value::Null); SCHEMA_WIDTH];
                values[0] = val(1);
                values[SCHEMA_WIDTH / 2] = val(2);
                values[SCHEMA_WIDTH - 1] = val(3);
                (Arc::clone(&keys), values)
            },
            |(keys, values)| {
                let msg = Message::new_shared_keys(source_name(), keys, values);
                black_box(msg);
            },
            BatchSize::SmallInput,
        )
    });

    // Projected: compact values (ACTIVE_COLUMNS elements per row).
    group.bench_function(BenchmarkId::new("projected", SCHEMA_WIDTH), |b| {
        b.iter_batched(
            || {
                let compact = vec![val(1), val(2), val(3)];
                (Arc::clone(&keys), compact, Arc::clone(&layout))
            },
            |(keys, compact, layout)| {
                let msg = Message::new_projected(source_name(), keys, compact, layout);
                black_box(msg);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_iter_entries(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_entries_iter");
    let keys = build_schema_keys(SCHEMA_WIDTH);
    let active = active_column_names();
    let layout = build_layout(&keys, &active);

    // Build batches of messages once per iteration setup.
    group.bench_function(BenchmarkId::new("dense", SCHEMA_WIDTH), |b| {
        b.iter_batched(
            || {
                let mut batch = Vec::with_capacity(ROWS_PER_ITER);
                for i in 0..ROWS_PER_ITER {
                    let mut values = vec![Arc::new(Value::Null); SCHEMA_WIDTH];
                    values[0] = val(i as i64);
                    values[SCHEMA_WIDTH / 2] = val((i + 1) as i64);
                    values[SCHEMA_WIDTH - 1] = val((i + 2) as i64);
                    let msg = Message::new_shared_keys(source_name(), Arc::clone(&keys), values);
                    batch.push(msg);
                }
                batch
            },
            |batch| {
                for msg in &batch {
                    for (_, value) in msg.entries() {
                        black_box(value);
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("projected", SCHEMA_WIDTH), |b| {
        b.iter_batched(
            || {
                let mut batch = Vec::with_capacity(ROWS_PER_ITER);
                for i in 0..ROWS_PER_ITER {
                    let compact = vec![val(i as i64), val((i + 1) as i64), val((i + 2) as i64)];
                    let msg = Message::new_projected(
                        source_name(),
                        Arc::clone(&keys),
                        compact,
                        Arc::clone(&layout),
                    );
                    batch.push(msg);
                }
                batch
            },
            |batch| {
                for msg in &batch {
                    for (_, value) in msg.entries() {
                        black_box(value);
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_value_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_value_lookup");
    let keys = build_schema_keys(SCHEMA_WIDTH);
    let active = active_column_names();
    let layout = build_layout(&keys, &active);

    // Pre-build indices to look up: only active column logical indices.
    let lookup_idxs: Vec<usize> = vec![0, SCHEMA_WIDTH / 2, SCHEMA_WIDTH - 1];

    group.bench_function(BenchmarkId::new("dense", SCHEMA_WIDTH), |b| {
        b.iter_batched(
            || {
                let mut batch = Vec::with_capacity(ROWS_PER_ITER);
                for i in 0..ROWS_PER_ITER {
                    let mut values = vec![Arc::new(Value::Null); SCHEMA_WIDTH];
                    values[0] = val(i as i64);
                    values[SCHEMA_WIDTH / 2] = val((i + 1) as i64);
                    values[SCHEMA_WIDTH - 1] = val((i + 2) as i64);
                    let msg = Message::new_shared_keys(source_name(), Arc::clone(&keys), values);
                    batch.push(msg);
                }
                (batch, lookup_idxs.clone())
            },
            |(batch, idxs)| {
                for msg in &batch {
                    for &idx in &idxs {
                        black_box(msg.value_by_index(idx));
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("projected", SCHEMA_WIDTH), |b| {
        b.iter_batched(
            || {
                let mut batch = Vec::with_capacity(ROWS_PER_ITER);
                for i in 0..ROWS_PER_ITER {
                    let compact = vec![val(i as i64), val((i + 1) as i64), val((i + 2) as i64)];
                    let msg = Message::new_projected(
                        source_name(),
                        Arc::clone(&keys),
                        compact,
                        Arc::clone(&layout),
                    );
                    batch.push(msg);
                }
                (batch, lookup_idxs.clone())
            },
            |(batch, idxs)| {
                for msg in &batch {
                    for &idx in &idxs {
                        black_box(msg.value_by_index(idx));
                    }
                }
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_random_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_random_lookup");
    let keys = build_schema_keys(SCHEMA_WIDTH);
    let active = active_column_names();
    let layout = build_layout(&keys, &active);
    let mut rng = StdRng::seed_from_u64(SEED);

    // Pre-generate random indices: mix of active (50%) and inactive (50%) columns.
    let random_idxs: Vec<usize> = (0..ROWS_PER_ITER)
        .map(|_| {
            if rng.gen_bool(0.5) {
                // Active column
                let pick = rng.gen_range(0..ACTIVE_COLUMNS);
                [0, SCHEMA_WIDTH / 2, SCHEMA_WIDTH - 1][pick]
            } else {
                // Inactive column (random but not one of the active set)
                loop {
                    let idx = rng.gen_range(0..SCHEMA_WIDTH);
                    if idx != 0 && idx != SCHEMA_WIDTH / 2 && idx != SCHEMA_WIDTH - 1 {
                        break idx;
                    }
                }
            }
        })
        .collect();

    group.bench_function(BenchmarkId::new("dense", SCHEMA_WIDTH), |b| {
        b.iter_batched_ref(
            || {
                let mut batch = Vec::with_capacity(ROWS_PER_ITER);
                for i in 0..ROWS_PER_ITER {
                    let mut values = vec![Arc::new(Value::Null); SCHEMA_WIDTH];
                    values[0] = val(i as i64);
                    values[SCHEMA_WIDTH / 2] = val((i + 1) as i64);
                    values[SCHEMA_WIDTH - 1] = val((i + 2) as i64);
                    let msg = Message::new_shared_keys(source_name(), Arc::clone(&keys), values);
                    batch.push(msg);
                }
                batch
            },
            |batch| {
                for (i, msg) in batch.iter().enumerate() {
                    black_box(msg.value_by_index(random_idxs[i]));
                }
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function(BenchmarkId::new("projected", SCHEMA_WIDTH), |b| {
        b.iter_batched_ref(
            || {
                let mut batch = Vec::with_capacity(ROWS_PER_ITER);
                for i in 0..ROWS_PER_ITER {
                    let compact = vec![val(i as i64), val((i + 1) as i64), val((i + 2) as i64)];
                    let msg = Message::new_projected(
                        source_name(),
                        Arc::clone(&keys),
                        compact,
                        Arc::clone(&layout),
                    );
                    batch.push(msg);
                }
                batch
            },
            |batch| {
                for (i, msg) in batch.iter().enumerate() {
                    black_box(msg.value_by_index(random_idxs[i]));
                }
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_build_messages,
    bench_iter_entries,
    bench_value_lookup,
    bench_random_lookup,
);
criterion_main!(benches);
