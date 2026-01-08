use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug, Default)]
pub struct ProcessorStats {
    records_in: AtomicU64,
    records_out: AtomicU64,
    error_count: AtomicU64,
    last_error: RwLock<Option<Arc<str>>>,
}

impl ProcessorStats {
    pub fn record_in(&self, rows: u64) {
        self.records_in.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_out(&self, rows: u64) {
        self.records_out.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_error(&self, message: impl Into<String>) {
        self.record_error_count(1, message);
    }

    pub fn record_error_count(&self, count: u64, message: impl Into<String>) {
        self.error_count.fetch_add(count, Ordering::Relaxed);
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
