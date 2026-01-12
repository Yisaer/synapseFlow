use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, register_int_gauge_vec, IntCounterVec, IntGaugeVec};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

static PROCESSOR_RECORDS_IN_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "processor_records_in_total",
        "Rows received by processors",
        &["pipeline_id", "processor_id", "kind"]
    )
    .expect("create processor records_in counter vec")
});

static PROCESSOR_RECORDS_OUT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "processor_records_out_total",
        "Rows emitted by processors",
        &["pipeline_id", "processor_id", "kind"]
    )
    .expect("create processor records_out counter vec")
});

static PROCESSOR_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "processor_errors_total",
        "Errors observed by processors",
        &["pipeline_id", "processor_id", "kind"]
    )
    .expect("create processor errors counter vec")
});

static PROCESSOR_METRIC_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "processor_metric_gauge",
        "Gauge metrics reported by processors",
        &["pipeline_id", "processor_id", "kind", "metric"]
    )
    .expect("create processor metric gauge vec")
});

static PROCESSOR_METRIC_COUNTER_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "processor_metric_counter_total",
        "Counter metrics reported by processors",
        &["pipeline_id", "processor_id", "kind", "metric"]
    )
    .expect("create processor metric counter vec")
});

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Gauge,
    Counter,
}

#[derive(Debug, Copy, Clone)]
pub struct MetricSpec {
    pub id: &'static str,
    pub flat_name: &'static str,
    pub kind: MetricKind,
}

#[derive(Debug)]
struct MetricEntry {
    id: &'static str,
    flat_name: &'static str,
    kind: MetricKind,
    value: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct GaugeHandle {
    pipeline_id: Arc<OnceLock<Arc<str>>>,
    processor_id: Arc<str>,
    kind: Arc<str>,
    entry: Arc<MetricEntry>,
}

impl GaugeHandle {
    pub fn set(&self, value: u64) {
        self.entry.value.store(value, Ordering::Relaxed);
        let Some(pipeline_id) = self.pipeline_id.get() else {
            return;
        };
        let prom_value = i64::try_from(value).unwrap_or(i64::MAX);
        PROCESSOR_METRIC_GAUGE
            .with_label_values(&[
                pipeline_id.as_ref(),
                self.processor_id.as_ref(),
                self.kind.as_ref(),
                self.entry.id,
            ])
            .set(prom_value);
    }
}

#[derive(Debug, Clone)]
pub struct CounterHandle {
    pipeline_id: Arc<OnceLock<Arc<str>>>,
    processor_id: Arc<str>,
    kind: Arc<str>,
    entry: Arc<MetricEntry>,
}

impl CounterHandle {
    pub fn inc_by(&self, delta: u64) {
        self.entry.value.fetch_add(delta, Ordering::Relaxed);
        let Some(pipeline_id) = self.pipeline_id.get() else {
            return;
        };
        PROCESSOR_METRIC_COUNTER_TOTAL
            .with_label_values(&[
                pipeline_id.as_ref(),
                self.processor_id.as_ref(),
                self.kind.as_ref(),
                self.entry.id,
            ])
            .inc_by(delta);
    }
}

#[derive(Debug)]
pub struct ProcessorStats {
    processor_id: Arc<str>,
    kind: Arc<str>,
    pipeline_id: Arc<OnceLock<Arc<str>>>,
    records_in: AtomicU64,
    records_out: AtomicU64,
    error_count: AtomicU64,
    last_error: RwLock<Option<Arc<str>>>,
    metrics: RwLock<BTreeMap<&'static str, Arc<MetricEntry>>>,
}

impl ProcessorStats {
    pub fn new(processor_id: impl Into<Arc<str>>, kind: impl Into<Arc<str>>) -> Self {
        Self {
            processor_id: processor_id.into(),
            kind: kind.into(),
            pipeline_id: Arc::new(OnceLock::new()),
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: RwLock::new(None),
            metrics: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn set_pipeline_id(&self, pipeline_id: &str) {
        let _ = self.pipeline_id.set(Arc::<str>::from(pipeline_id));
    }

    pub fn register_gauge(&self, spec: MetricSpec) -> GaugeHandle {
        let entry = self.register_metric(spec, MetricKind::Gauge);
        GaugeHandle {
            pipeline_id: Arc::clone(&self.pipeline_id),
            processor_id: Arc::clone(&self.processor_id),
            kind: Arc::clone(&self.kind),
            entry,
        }
    }

    pub fn register_counter(&self, spec: MetricSpec) -> CounterHandle {
        let entry = self.register_metric(spec, MetricKind::Counter);
        CounterHandle {
            pipeline_id: Arc::clone(&self.pipeline_id),
            processor_id: Arc::clone(&self.processor_id),
            kind: Arc::clone(&self.kind),
            entry,
        }
    }

    fn register_metric(&self, spec: MetricSpec, expected_kind: MetricKind) -> Arc<MetricEntry> {
        if spec.kind != expected_kind {
            panic!("metric kind mismatch: id={}", spec.id);
        }

        let reserved = ["records_in", "records_out", "error_count", "last_error"];
        if reserved.contains(&spec.flat_name) {
            panic!("metric flat_name is reserved: {}", spec.flat_name);
        }

        let mut guard = self
            .metrics
            .write()
            .expect("ProcessorStats metrics lock poisoned");

        for entry in guard.values() {
            if entry.flat_name == spec.flat_name && entry.id != spec.id {
                panic!(
                    "metric flat_name collision: flat_name={} ({} vs {})",
                    spec.flat_name, entry.id, spec.id
                );
            }
        }

        let entry = guard.entry(spec.id).or_insert_with(|| {
            Arc::new(MetricEntry {
                id: spec.id,
                flat_name: spec.flat_name,
                kind: spec.kind,
                value: AtomicU64::new(0),
            })
        });

        if entry.id != spec.id || entry.flat_name != spec.flat_name || entry.kind != spec.kind {
            panic!("metric registration mismatch: id={}", spec.id);
        }

        Arc::clone(entry)
    }

    pub fn record_in(&self, rows: u64) {
        self.records_in.fetch_add(rows, Ordering::Relaxed);
        if let Some(pipeline_id) = self.pipeline_id.get() {
            PROCESSOR_RECORDS_IN_TOTAL
                .with_label_values(&[
                    pipeline_id.as_ref(),
                    self.processor_id.as_ref(),
                    self.kind.as_ref(),
                ])
                .inc_by(rows);
        }
    }

    pub fn record_out(&self, rows: u64) {
        self.records_out.fetch_add(rows, Ordering::Relaxed);
        if let Some(pipeline_id) = self.pipeline_id.get() {
            PROCESSOR_RECORDS_OUT_TOTAL
                .with_label_values(&[
                    pipeline_id.as_ref(),
                    self.processor_id.as_ref(),
                    self.kind.as_ref(),
                ])
                .inc_by(rows);
        }
    }

    pub fn record_error(&self, message: impl Into<String>) {
        self.record_error_count(1, message);
    }

    pub fn record_error_count(&self, count: u64, message: impl Into<String>) {
        self.error_count.fetch_add(count, Ordering::Relaxed);
        if let Some(pipeline_id) = self.pipeline_id.get() {
            PROCESSOR_ERRORS_TOTAL
                .with_label_values(&[
                    pipeline_id.as_ref(),
                    self.processor_id.as_ref(),
                    self.kind.as_ref(),
                ])
                .inc_by(count);
        }
        let message: String = message.into();
        let mut guard = self
            .last_error
            .write()
            .expect("ProcessorStats error lock poisoned");
        *guard = Some(Arc::<str>::from(message));
    }

    pub fn clear_last_error(&self) {
        let mut guard = self
            .last_error
            .write()
            .expect("ProcessorStats error lock poisoned");
        *guard = None;
    }

    pub fn snapshot(&self) -> ProcessorStatsSnapshot {
        let last_error = self
            .last_error
            .read()
            .expect("ProcessorStats error lock poisoned")
            .as_deref()
            .map(ToString::to_string);
        let custom = self
            .metrics
            .read()
            .expect("ProcessorStats metrics lock poisoned")
            .values()
            .map(|entry| {
                (
                    entry.flat_name.to_string(),
                    entry.value.load(Ordering::Relaxed),
                )
            })
            .collect();
        ProcessorStatsSnapshot {
            records_in: self.records_in.load(Ordering::Relaxed),
            records_out: self.records_out.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_error,
            custom,
        }
    }
}

impl Default for ProcessorStats {
    fn default() -> Self {
        Self::new("unknown", "unknown")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessorStatsSnapshot {
    pub records_in: u64,
    pub records_out: u64,
    pub error_count: u64,
    pub last_error: Option<String>,
    #[serde(flatten)]
    pub custom: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct ProcessorStatsHandle {
    pub processor_id: String,
    pub stats: Arc<ProcessorStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessorStatsEntry {
    pub processor_id: String,
    pub stats: ProcessorStatsSnapshot,
}

impl ProcessorStatsHandle {
    pub fn snapshot(&self) -> ProcessorStatsEntry {
        ProcessorStatsEntry {
            processor_id: self.processor_id.clone(),
            stats: self.stats.snapshot(),
        }
    }
}
