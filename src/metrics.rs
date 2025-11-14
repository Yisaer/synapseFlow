use once_cell::sync::Lazy;
use prometheus::{register_counter, register_int_gauge, Counter, IntGauge};

pub static CPU_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("cpu_usage", "CPU usage in percentage").expect("create cpu usage gauge")
});

pub static MEMORY_USAGE_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("memory_usage_bytes", "Resident memory usage in bytes")
        .expect("create memory usage gauge")
});

pub static CPU_SECONDS_TOTAL_COUNTER: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "cpu_seconds_total",
        "Total CPU seconds consumed by the synapse-flow process"
    )
    .expect("create cpu seconds counter")
});
