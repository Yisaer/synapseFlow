use once_cell::sync::OnceCell;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::sleep;
use tokio_metrics::{RuntimeMetrics, RuntimeMonitor};

use crate::metrics::TOKIO_TASKS_GAUGE;

static TOKIO_METRICS_STARTED: OnceCell<()> = OnceCell::new();

/// Spawn a background task that samples Tokio runtime metrics and updates gauges.
pub fn spawn_tokio_metrics_collector(poll_interval: Duration) {
    if TOKIO_METRICS_STARTED.set(()).is_err() {
        return;
    }
    let handle = Handle::current();
    let monitor = RuntimeMonitor::new(&handle);
    let runtime_handle = handle.clone();
    tokio::spawn(async move {
        for interval in monitor.intervals() {
            let live_tasks = live_tasks_from_interval(&interval, &runtime_handle);
            TOKIO_TASKS_GAUGE.set(clamp_usize_to_i64(live_tasks));
            sleep(poll_interval).await;
        }
    });
}

fn clamp_usize_to_i64(value: usize) -> i64 {
    if value > i64::MAX as usize {
        i64::MAX
    } else {
        value as i64
    }
}

#[cfg(tokio_unstable)]
fn live_tasks_from_interval(interval: &RuntimeMetrics, _handle: &Handle) -> usize {
    interval.live_tasks_count
}

#[cfg(not(tokio_unstable))]
fn live_tasks_from_interval(_interval: &RuntimeMetrics, handle: &Handle) -> usize {
    handle.metrics().num_alive_tasks()
}
