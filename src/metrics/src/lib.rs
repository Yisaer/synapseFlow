#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

mod common;
mod flow_instance;
mod mqtt_shared_client;
mod mqtt_sink;
mod mqtt_source;
mod nng_pubsub_sink;
mod nng_pubsub_source;
mod processor;
mod runtime;
mod shared_stream;

use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

pub fn set_default_flow_instance_id(id: &str) {
    flow_instance::set_default_id(id);
}

pub fn default_flow_instance_id() -> &'static str {
    flow_instance::default_id()
}

pub fn mqtt_source_messages_in_total() -> &'static IntCounterVec {
    mqtt_source::messages_in_total()
}

pub fn mqtt_source_messages_out_total() -> &'static IntCounterVec {
    mqtt_source::messages_out_total()
}

pub fn mqtt_sink_messages_in_total() -> &'static IntCounterVec {
    mqtt_sink::messages_in_total()
}

pub fn mqtt_sink_messages_out_total() -> &'static IntCounterVec {
    mqtt_sink::messages_out_total()
}

pub fn nng_pubsub_source_messages_in_total() -> &'static IntCounterVec {
    nng_pubsub_source::messages_in_total()
}

pub fn nng_pubsub_source_messages_out_total() -> &'static IntCounterVec {
    nng_pubsub_source::messages_out_total()
}

pub fn nng_pubsub_sink_messages_in_total() -> &'static IntCounterVec {
    nng_pubsub_sink::messages_in_total()
}

pub fn nng_pubsub_sink_messages_out_total() -> &'static IntCounterVec {
    nng_pubsub_sink::messages_out_total()
}

pub fn mqtt_shared_client_connected() -> &'static IntGaugeVec {
    mqtt_shared_client::connected()
}

pub fn mqtt_shared_client_active_refs() -> &'static IntGaugeVec {
    mqtt_shared_client::active_refs()
}

pub fn mqtt_shared_client_rx_messages_total() -> &'static IntCounterVec {
    mqtt_shared_client::rx_messages_total()
}

pub fn mqtt_shared_client_tx_messages_total() -> &'static IntCounterVec {
    mqtt_shared_client::tx_messages_total()
}

pub fn mqtt_shared_client_errors_total() -> &'static IntCounterVec {
    mqtt_shared_client::errors_total()
}

pub fn mqtt_shared_client_reconnects_total() -> &'static IntCounterVec {
    mqtt_shared_client::reconnects_total()
}

pub fn shared_stream_running() -> &'static IntGaugeVec {
    shared_stream::running()
}

pub fn shared_stream_active_subscribers() -> &'static IntGaugeVec {
    shared_stream::active_subscribers()
}

pub fn shared_stream_starts_total() -> &'static IntCounterVec {
    shared_stream::starts_total()
}

pub fn shared_stream_errors_total() -> &'static IntCounterVec {
    shared_stream::errors_total()
}

pub fn shared_stream_messages_in_total() -> &'static IntCounterVec {
    shared_stream::messages_in_total()
}

pub fn shared_stream_messages_out_total() -> &'static IntCounterVec {
    shared_stream::messages_out_total()
}

pub fn processor_records_in_total() -> &'static IntCounterVec {
    processor::records_in_total()
}

pub fn processor_records_out_total() -> &'static IntCounterVec {
    processor::records_out_total()
}

pub fn processor_errors_total() -> &'static IntCounterVec {
    processor::errors_total()
}

pub fn processor_custom_gauge() -> &'static IntGaugeVec {
    processor::custom_gauge()
}

pub fn processor_custom_counter_total() -> &'static IntCounterVec {
    processor::custom_counter_total()
}

pub fn processor_handle_duration_seconds() -> &'static HistogramVec {
    processor::handle_duration_seconds()
}

pub fn processor_send_backpressure_waits_total() -> &'static IntCounterVec {
    processor::send_backpressure_waits_total()
}

pub fn flow_instance_cpu_usage_percent() -> &'static GaugeVec {
    flow_instance::cpu_usage_percent()
}

pub fn runtime_cpu_usage_percent() -> &'static Gauge {
    runtime::cpu_usage_percent()
}

pub fn runtime_memory_usage_bytes() -> &'static IntGauge {
    runtime::memory_usage_bytes()
}

pub fn runtime_tokio_tasks_inflight() -> &'static IntGauge {
    runtime::tokio_tasks_inflight()
}

pub fn runtime_heap_in_use_bytes() -> &'static IntGauge {
    runtime::heap_in_use_bytes()
}

pub fn runtime_heap_in_allocator_bytes() -> &'static IntGauge {
    runtime::heap_in_allocator_bytes()
}

// ── Child metric handle wrappers ────────────────────────────────────────────
//
// These wrap prometheus child handles so callers can cache the result of
// `with_label_values` without depending on prometheus concrete types.
// Each wrapper exposes only the operations needed for the recorded metric kind.

/// A cached handle for one label combination of an `IntCounterVec`.
#[derive(Debug, Clone)]
pub struct ChildCounter {
    inner: IntCounter,
}

impl ChildCounter {
    pub fn inc_by(&self, delta: u64) {
        self.inner.inc_by(delta);
    }

    pub fn inc(&self) {
        self.inner.inc();
    }

    pub fn bind(vec: &IntCounterVec, labels: &[&str]) -> Self {
        Self {
            inner: vec.with_label_values(labels),
        }
    }
}

/// A cached handle for one label combination of a `HistogramVec`.
#[derive(Debug, Clone)]
pub struct ChildHistogram {
    inner: Histogram,
}

impl ChildHistogram {
    pub fn observe(&self, v: f64) {
        self.inner.observe(v);
    }

    pub fn bind(vec: &HistogramVec, labels: &[&str]) -> Self {
        Self {
            inner: vec.with_label_values(labels),
        }
    }
}

/// A cached handle for one label combination of an `IntGaugeVec`.
#[derive(Debug, Clone)]
pub struct ChildGauge {
    inner: IntGauge,
}

impl ChildGauge {
    pub fn set(&self, v: i64) {
        self.inner.set(v);
    }

    pub fn bind(vec: &IntGaugeVec, labels: &[&str]) -> Self {
        Self {
            inner: vec.with_label_values(labels),
        }
    }
}
