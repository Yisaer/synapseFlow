use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::RwLock;
use veloflux_metrics::{ChildCounter, ChildGauge, ChildHistogram};

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
    flow_instance_id: Arc<str>,
    pipeline_id: Arc<OnceLock<Arc<str>>>,
    processor_id: Arc<str>,
    entry: Arc<MetricEntry>,
    /// Cached prometheus child gauge, resolved once on first `set()`.
    gauge: OnceLock<ChildGauge>,
}

impl GaugeHandle {
    pub fn set(&self, value: u64) {
        self.entry.value.store(value, Ordering::Relaxed);
        let prom_value = i64::try_from(value).unwrap_or(i64::MAX);
        let Some(gauge) = self.resolved_gauge() else {
            return;
        };
        gauge.set(prom_value);
    }

    fn resolved_gauge(&self) -> Option<&ChildGauge> {
        let pipeline_id = self.pipeline_id.get()?;
        Some(self.gauge.get_or_init(|| {
            ChildGauge::bind(
                veloflux_metrics::processor_custom_gauge(),
                &[
                    self.flow_instance_id.as_ref(),
                    pipeline_id.as_ref(),
                    self.processor_id.as_ref(),
                    self.entry.id,
                ],
            )
        }))
    }
}

/// Bundle of cached prometheus child handles for the standard processor metrics.
/// Resolved once when `pipeline_id` is first known; after that the per-batch
/// record path does a plain atomic add / histogram observe with no label-map lookup.
#[derive(Debug)]
struct BoundMetrics {
    in_counter: ChildCounter,
    out_counter: ChildCounter,
    error_counter: ChildCounter,
    handle_duration: ChildHistogram,
    backpressure_counter: ChildCounter,
}

impl BoundMetrics {
    fn new(labels: &[&str; 3]) -> Self {
        Self {
            in_counter: ChildCounter::bind(veloflux_metrics::processor_records_in_total(), labels),
            out_counter: ChildCounter::bind(
                veloflux_metrics::processor_records_out_total(),
                labels,
            ),
            error_counter: ChildCounter::bind(veloflux_metrics::processor_errors_total(), labels),
            handle_duration: ChildHistogram::bind(
                veloflux_metrics::processor_handle_duration_seconds(),
                labels,
            ),
            backpressure_counter: ChildCounter::bind(
                veloflux_metrics::processor_send_backpressure_waits_total(),
                labels,
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CounterHandle {
    flow_instance_id: Arc<str>,
    pipeline_id: Arc<OnceLock<Arc<str>>>,
    processor_id: Arc<str>,
    entry: Arc<MetricEntry>,
    /// Cached prometheus child counter, resolved once on first `inc_by()`.
    counter: OnceLock<ChildCounter>,
}

impl CounterHandle {
    pub fn inc_by(&self, delta: u64) {
        self.entry.value.fetch_add(delta, Ordering::Relaxed);
        let Some(counter) = self.resolved_counter() else {
            return;
        };
        counter.inc_by(delta);
    }

    fn resolved_counter(&self) -> Option<&ChildCounter> {
        let pipeline_id = self.pipeline_id.get()?;
        Some(self.counter.get_or_init(|| {
            ChildCounter::bind(
                veloflux_metrics::processor_custom_counter_total(),
                &[
                    self.flow_instance_id.as_ref(),
                    pipeline_id.as_ref(),
                    self.processor_id.as_ref(),
                    self.entry.id,
                ],
            )
        }))
    }
}

#[derive(Debug)]
pub struct ProcessorStats {
    flow_instance_id: Arc<str>,
    processor_id: Arc<str>,
    kind: Arc<str>,
    pipeline_id: Arc<OnceLock<Arc<str>>>,
    records_in: AtomicU64,
    records_out: AtomicU64,
    error_count: AtomicU64,
    last_error: RwLock<Option<Arc<str>>>,
    metrics: RwLock<BTreeMap<&'static str, Arc<MetricEntry>>>,
    /// Cached prometheus child handles for standard processor metrics.
    /// Resolved lazily on the first record call that observes a set `pipeline_id`;
    /// the labels are fixed for the lifetime of this processor.
    bound_metrics: OnceLock<BoundMetrics>,
}

impl ProcessorStats {
    pub fn new(
        flow_instance_id: impl Into<Arc<str>>,
        processor_id: impl Into<Arc<str>>,
        kind: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            flow_instance_id: flow_instance_id.into(),
            processor_id: processor_id.into(),
            kind: kind.into(),
            pipeline_id: Arc::new(OnceLock::new()),
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: RwLock::new(None),
            metrics: RwLock::new(BTreeMap::new()),
            bound_metrics: OnceLock::new(),
        }
    }

    /// Labels for this processor's prometheus child metrics, available once the
    /// pipeline id is assigned. Returns `None` before then (metrics are skipped).
    fn metric_labels(&self) -> Option<[&str; 3]> {
        let pipeline_id = self.pipeline_id.get()?;
        Some([
            self.flow_instance_id.as_ref(),
            pipeline_id.as_ref(),
            self.processor_id.as_ref(),
        ])
    }

    /// Returns the cached `BoundMetrics` handle, resolving it once when
    /// `pipeline_id` is first observed.
    fn bound_metrics(&self) -> Option<&BoundMetrics> {
        let labels = self.metric_labels()?;
        Some(
            self.bound_metrics
                .get_or_init(|| BoundMetrics::new(&labels)),
        )
    }

    pub fn set_pipeline_id(&self, pipeline_id: &str) {
        let _ = self.pipeline_id.set(Arc::<str>::from(pipeline_id));
    }

    pub fn register_gauge(&self, spec: MetricSpec) -> GaugeHandle {
        let entry = self.register_metric(spec, MetricKind::Gauge);
        GaugeHandle {
            flow_instance_id: Arc::clone(&self.flow_instance_id),
            pipeline_id: Arc::clone(&self.pipeline_id),
            processor_id: Arc::clone(&self.processor_id),
            entry,
            gauge: OnceLock::new(),
        }
    }

    pub fn register_counter(&self, spec: MetricSpec) -> CounterHandle {
        let entry = self.register_metric(spec, MetricKind::Counter);
        CounterHandle {
            flow_instance_id: Arc::clone(&self.flow_instance_id),
            pipeline_id: Arc::clone(&self.pipeline_id),
            processor_id: Arc::clone(&self.processor_id),
            entry,
            counter: OnceLock::new(),
        }
    }

    // Called only from processor constructors (new / new_with_channel_capacities), never during
    // data processing. All asserts below are init-time programming-error checks; a failure here
    // means the processor was wired up incorrectly and the pipeline will fail to start.
    fn register_metric(&self, spec: MetricSpec, expected_kind: MetricKind) -> Arc<MetricEntry> {
        assert_eq!(
            spec.kind, expected_kind,
            "metric kind mismatch: id={}",
            spec.id
        );

        let reserved = ["records_in", "records_out", "error_count", "last_error"];
        assert!(
            !reserved.contains(&spec.flat_name),
            "metric flat_name is reserved: {}",
            spec.flat_name
        );

        let mut guard = self.metrics.write();

        for entry in guard.values() {
            assert!(
                !(entry.flat_name == spec.flat_name && entry.id != spec.id),
                "metric flat_name collision: flat_name={} ({} vs {})",
                spec.flat_name,
                entry.id,
                spec.id
            );
        }

        let entry = guard.entry(spec.id).or_insert_with(|| {
            Arc::new(MetricEntry {
                id: spec.id,
                flat_name: spec.flat_name,
                kind: spec.kind,
                value: AtomicU64::new(0),
            })
        });

        assert!(
            entry.id == spec.id && entry.flat_name == spec.flat_name && entry.kind == spec.kind,
            "metric registration mismatch: id={}",
            spec.id
        );

        Arc::clone(entry)
    }

    pub fn record_in(&self, rows: u64) {
        self.records_in.fetch_add(rows, Ordering::Relaxed);
        if let Some(bound) = self.bound_metrics() {
            bound.in_counter.inc_by(rows);
        }
    }

    pub fn record_out(&self, rows: u64) {
        self.records_out.fetch_add(rows, Ordering::Relaxed);
        if let Some(bound) = self.bound_metrics() {
            bound.out_counter.inc_by(rows);
        }
    }

    pub fn record_error(&self, message: impl Into<String>) {
        self.record_error_count(1, message);
    }

    fn pipeline_id_label(&self) -> &str {
        self.pipeline_id
            .get()
            .map(|id| id.as_ref())
            .unwrap_or("<unassigned>")
    }

    pub fn record_error_logged(&self, context: &'static str, message: impl Into<String>) {
        self.record_error_count_logged(context, 1, message);
    }

    pub fn record_error_count_logged(
        &self,
        context: &'static str,
        count: u64,
        message: impl Into<String>,
    ) {
        let message = message.into();
        tracing::error!(
            flow_instance_id = %self.flow_instance_id,
            pipeline_id = self.pipeline_id_label(),
            processor_id = %self.processor_id,
            processor_kind = %self.kind,
            context,
            error = %message,
            error_count = count,
            "processor runtime error"
        );
        self.record_error_count(count, message);
    }

    pub fn record_error_count(&self, count: u64, message: impl Into<String>) {
        self.error_count.fetch_add(count, Ordering::Relaxed);
        if let Some(bound) = self.bound_metrics() {
            bound.error_counter.inc_by(count);
        }
        let message: String = message.into();
        let mut guard = self.last_error.write();
        *guard = Some(Arc::<str>::from(message));
    }

    pub fn record_handle_duration(&self, duration: Duration) {
        // Note: The semantic boundary of "handle duration" is defined by each processor.
        // - For processors that send outputs inline (awaiting downstream backpressure), the
        //   recommended definition is end-to-end wall time including the send wait.
        // - For processors that enqueue/update local state and emit asynchronously, the recommended
        //   definition is local work time only, excluding any later flush/send work.
        if let Some(bound) = self.bound_metrics() {
            bound.handle_duration.observe(duration.as_secs_f64());
        }
    }

    pub fn record_send_backpressure_wait_tick(&self) {
        if let Some(bound) = self.bound_metrics() {
            bound.backpressure_counter.inc();
        }
    }

    pub fn clear_last_error(&self) {
        let mut guard = self.last_error.write();
        *guard = None;
    }

    pub fn snapshot(&self) -> ProcessorStatsSnapshot {
        let last_error = self.last_error.read().as_deref().map(ToString::to_string);
        let custom = self
            .metrics
            .read()
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
        Self::new("unknown", "unknown", "unknown")
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
