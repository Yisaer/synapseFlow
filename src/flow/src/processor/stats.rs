use once_cell::sync::Lazy;
use prometheus::{register_int_counter_vec, IntCounterVec};
use serde::{Deserialize, Serialize};
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

#[derive(Debug)]
pub struct ProcessorStats {
    processor_id: Arc<str>,
    kind: Arc<str>,
    pipeline_id: OnceLock<Arc<str>>,
    records_in: AtomicU64,
    records_out: AtomicU64,
    error_count: AtomicU64,
    last_error: RwLock<Option<Arc<str>>>,
}

impl ProcessorStats {
    pub fn new(processor_id: impl Into<Arc<str>>, kind: impl Into<Arc<str>>) -> Self {
        Self {
            processor_id: processor_id.into(),
            kind: kind.into(),
            pipeline_id: OnceLock::new(),
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }
    }

    pub fn set_pipeline_id(&self, pipeline_id: &str) {
        let _ = self.pipeline_id.set(Arc::<str>::from(pipeline_id));
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
        ProcessorStatsSnapshot {
            records_in: self.records_in.load(Ordering::Relaxed),
            records_out: self.records_out.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_error,
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
