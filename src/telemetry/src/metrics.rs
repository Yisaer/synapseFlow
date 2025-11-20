use once_cell::sync::Lazy;
use prometheus::{register_int_gauge, IntGauge};

pub static CPU_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("cpu_usage", "CPU usage in percentage").expect("create cpu usage gauge")
});

pub static MEMORY_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("memory_usage_bytes", "Resident memory usage in bytes")
        .expect("create memory usage gauge")
});

pub static TOKIO_TASKS_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "tokio_tasks_inflight",
        "Number of currently running Tokio spawn tasks"
    )
    .expect("create tokio task gauge")
});

pub static HEAP_IN_USE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "heap_in_use_bytes",
        "Bytes actively allocated by the global allocator"
    )
    .expect("create heap in use gauge")
});

pub static HEAP_IN_ALLOCATOR_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "heap_in_allocator_bytes",
        "Bytes reserved by the allocator from the operating system"
    )
    .expect("create heap in allocator gauge")
});
