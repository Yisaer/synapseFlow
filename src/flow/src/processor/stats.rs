use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug, Default)]
pub struct ProcessorStats {
    records_in: AtomicU64,
    records_out: AtomicU64,
    error: RwLock<Option<Arc<str>>>,
}

impl ProcessorStats {
    pub fn record_in(&self, rows: u64) {
        self.records_in.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_out(&self, rows: u64) {
        self.records_out.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn set_error(&self, message: impl Into<String>) {
        let message: String = message.into();
        let mut guard = self
            .error
            .write()
            .expect("ProcessorStats error lock poisoned");
        *guard = Some(Arc::<str>::from(message));
    }

    pub fn clear_error(&self) {
        let mut guard = self
            .error
            .write()
            .expect("ProcessorStats error lock poisoned");
        *guard = None;
    }

    pub fn snapshot(&self) -> ProcessorStatsSnapshot {
        let error = self
            .error
            .read()
            .expect("ProcessorStats error lock poisoned")
            .as_deref()
            .map(ToString::to_string);
        ProcessorStatsSnapshot {
            records_in: self.records_in.load(Ordering::Relaxed),
            records_out: self.records_out.load(Ordering::Relaxed),
            error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessorStatsSnapshot {
    pub records_in: u64,
    pub records_out: u64,
    pub error: Option<String>,
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
