use flow::FlowInstance;
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use parking_lot::Mutex;
use std::future::Future;
#[cfg(feature = "profiling")]
use std::io::{Read, Write};
#[cfg(feature = "profiling")]
use std::net::SocketAddr;
#[cfg(feature = "profiling")]
use std::net::{TcpListener, TcpStream};
#[cfg(any(feature = "metrics", feature = "profiling"))]
use std::process;
#[cfg(feature = "profiling")]
use std::thread;
#[cfg(feature = "metrics")]
use std::time::Duration as StdDuration;
use storage::StorageManager;

use crate::startup::StartupPhase;
#[cfg(feature = "profiling")]
use pprof::{protos::Message, ProfilerGuardBuilder};
#[cfg(feature = "metrics")]
use sysinfo::{Pid, System};
#[cfg(feature = "metrics")]
use telemetry::spawn_tokio_metrics_collector;
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use tikv_jemalloc_ctl::raw;
#[cfg(all(
    feature = "metrics",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use tikv_jemalloc_ctl::{epoch, stats};
#[cfg(feature = "metrics")]
use tokio::time::{sleep, Duration};
#[cfg(feature = "metrics")]
use veloflux_metrics::{
    runtime_cpu_usage_percent, runtime_heap_in_allocator_bytes, runtime_heap_in_use_bytes,
    runtime_memory_usage_bytes,
};

#[cfg(feature = "profiling")]
const DEFAULT_PROFILE_ADDR: &str = "0.0.0.0:6060";
#[cfg(feature = "profiling")]
const DEFAULT_CPU_PROFILE_FREQUENCY_HZ: i32 = 100;
#[cfg(feature = "profiling")]
const DEFAULT_CPU_PROFILE_BLOCKLIST: [&str; 4] = ["libc", "libgcc", "pthread", "vdso"];
#[cfg(feature = "metrics")]
const DEFAULT_METRICS_POLL_INTERVAL_SECS: u64 = 5;

pub const DEFAULT_DATA_DIR: &str = "./tmp";
pub const DEFAULT_MANAGER_ADDR: &str = "0.0.0.0:8080";

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
static PPROF_ENDPOINT_MUTEX: Mutex<()> = Mutex::new(());

#[cfg(feature = "metrics")]
struct MetricsExporterRuntime;

#[cfg(not(feature = "metrics"))]
struct MetricsExporterRuntime;

#[cfg(feature = "profiling")]
struct ProfileServerHandle {
    shutdown_tx: Option<std::sync::mpsc::Sender<()>>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

#[cfg(feature = "profiling")]
impl ProfileServerHandle {
    fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

#[cfg(feature = "profiling")]
impl Drop for ProfileServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(not(feature = "profiling"))]
struct ProfileServerHandle;

struct ServerRuntimeGuards {
    #[cfg(feature = "metrics")]
    _metrics_exporter: Option<MetricsExporterRuntime>,
    #[cfg(feature = "profiling")]
    _profile_server: Option<ProfileServerHandle>,
}

/// Options for initializing the server runtime.
#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    /// Enable profiling endpoints (feature-gated); if None, uses default (false).
    pub profiling_enabled: Option<bool>,
    /// Custom data directory for storage; if None, uses DEFAULT_DATA_DIR.
    pub data_dir: Option<String>,
    /// Optional startup resource directory supplied by `--init-dir`.
    pub init_dir: Option<String>,
    /// Manager listen address; if None, uses default.
    pub manager_addr: Option<String>,
    /// Profiling server bind address (feature-gated); if None, uses default.
    pub profile_addr: Option<String>,
    /// CPU profiling sample frequency in Hz (feature-gated); if None, uses default.
    pub cpu_profile_freq_hz: Option<i32>,
    /// Metrics polling interval in seconds (feature-gated); if None, uses default.
    pub metrics_poll_interval_secs: Option<u64>,
    /// Pipeline patrol scheduler interval in seconds. 0 disables.
    pub pipeline_patrol_interval_secs: Option<u64>,
    /// Declared flow instances loaded from config.
    pub flow_instances: Vec<manager::FlowInstanceSpec>,
    /// Process-wide static properties shared by every flow instance.
    pub property_context: flow::PropertyContext,
}

/// Runtime context returned by [`init`] and consumed by [`start`].
#[allow(dead_code)]
pub struct ServerContext {
    instance: FlowInstance,
    storage: StorageManager,
    init_dir: Option<String>,
    manager_addr: String,
    flow_instances: Vec<manager::FlowInstanceSpec>,
    pipeline_patrol_interval_secs: u64,
    runtime_guards: ServerRuntimeGuards,
}

impl ServerContext {
    /// Access the FlowInstance for custom registrations before starting.
    pub fn instance(&self) -> &FlowInstance {
        &self.instance
    }

    /// Mutable access to FlowInstance if needed for setup.
    pub fn instance_mut(&mut self) -> &mut FlowInstance {
        &mut self.instance
    }

    /// Manager address that will be used when starting the server.
    pub fn manager_addr(&self) -> &str {
        &self.manager_addr
    }
}

/// Initialize the Flow server: metrics/profiling, storage, FlowInstance, and catalog load.
pub async fn init(
    opts: ServerOptions,
    instance: FlowInstance,
) -> Result<ServerContext, Box<dyn std::error::Error + Send + Sync>> {
    let init_phase = StartupPhase::new("manager", "default", "server_init", None);
    flow::init_process_once();
    instance.set_property_context(opts.property_context.clone());
    veloflux_metrics::set_default_flow_instance_id("default");
    log_allocator();

    let profiling_enabled = opts.profiling_enabled.unwrap_or(false);
    if profiling_enabled {
        ensure_jemalloc_profiling();
    }

    #[cfg(feature = "metrics")]
    let metrics_exporter = match init_metrics_exporter_runtime(&opts).await {
        Ok(runtime) => runtime,
        Err(err) => {
            init_phase.log_failure(err.as_ref());
            return Err(err);
        }
    };
    #[cfg(feature = "profiling")]
    let profile_server = if profiling_enabled {
        start_profile_server_runtime(&opts)
    } else {
        None
    };

    let manager_addr = opts
        .manager_addr
        .unwrap_or_else(|| DEFAULT_MANAGER_ADDR.to_string());
    let data_dir = opts
        .data_dir
        .unwrap_or_else(|| DEFAULT_DATA_DIR.to_string());
    let init_dir = opts.init_dir;

    let storage = match StorageManager::new(&data_dir) {
        Ok(storage) => storage,
        Err(err) => {
            init_phase.log_failure(&err);
            return Err(err.into());
        }
    };

    // Load the encrypted secret store (VF-51) and install it on the instance so
    // `SecretRef` config fields resolve during pipeline application.
    match flow::secret::bootstrap(std::path::Path::new(&data_dir)) {
        Ok(ctx) => instance.set_secret_context(ctx),
        Err(err) => {
            init_phase.log_failure(&err);
            return Err(err.into());
        }
    }
    tracing::info!(
        mode = init_phase.mode(),
        flow_instance_id = %init_phase.flow_instance_id(),
        phase = init_phase.phase(),
        config_path = init_phase.config_path(),
        result = "succeeded",
        elapsed_ms = init_phase.elapsed_ms(),
        storage_dir = %storage.base_dir().display(),
        manager_addr = %manager_addr,
        profiling_enabled,
        declared_flow_instance_count = opts.flow_instances.len(),
        "startup phase"
    );

    let pipeline_patrol_interval_secs = opts.pipeline_patrol_interval_secs.unwrap_or(15);

    Ok(ServerContext {
        instance,
        storage,
        init_dir,
        manager_addr,
        flow_instances: opts.flow_instances,
        pipeline_patrol_interval_secs,
        runtime_guards: ServerRuntimeGuards {
            #[cfg(feature = "metrics")]
            _metrics_exporter: metrics_exporter,
            #[cfg(feature = "profiling")]
            _profile_server: profile_server,
        },
    })
}

/// Start the manager server and await termination (Ctrl+C or server error).
pub async fn start(ctx: ServerContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_with_shutdown(ctx, shutdown_signal()).await
}

pub async fn start_with_shutdown<F>(
    ctx: ServerContext,
    shutdown: F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    start_with_shutdown_and_signal(ctx, shutdown, None).await
}

pub async fn start_with_shutdown_and_signal<F>(
    ctx: ServerContext,
    shutdown: F,
    startup_tx: Option<std::sync::mpsc::SyncSender<Result<(), String>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let flow_instances = ctx.flow_instances;
    let instance = ctx.instance;
    let storage = ctx.storage;
    let init_dir = ctx.init_dir.map(std::path::PathBuf::from);
    let manager_addr = ctx.manager_addr;
    let patrol_interval_secs = ctx.pipeline_patrol_interval_secs;

    tracing::info!(manager_addr = %manager_addr, "starting manager");
    let (manager_shutdown_tx, manager_shutdown_rx) = tokio::sync::oneshot::channel();
    let manager_future = manager::start_server_with_shutdown(
        manager_addr.clone(),
        instance,
        storage,
        flow_instances,
        async move {
            let _ = manager_shutdown_rx.await;
        },
        startup_tx,
        manager::ManagerStartupOptions {
            patrol_interval_secs,
            init_dir,
        },
    );
    tokio::pin!(manager_future);
    tokio::pin!(shutdown);

    tokio::select! {
        result = &mut manager_future => {
            if let Err(err) = result {
                tracing::error!(error = %err, "manager server exited with error");
                return Err(err);
            }
            Ok(())
        }
        _ = &mut shutdown => {
            tracing::info!("shutdown signal received, shutting down");
            let _ = manager_shutdown_tx.send(());
            match manager_future.await {
                Ok(()) => Ok(()),
                Err(err) => {
                    tracing::error!(error = %err, "manager server exited with error during shutdown");
                    Err(err)
                }
            }
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::warn!(error = %err, "failed to register SIGINT handler; fallback to ctrl+c");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::warn!(error = %err, "failed to register SIGTERM handler; fallback to ctrl+c");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };

    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(all(feature = "allocator-jemalloc", not(target_env = "msvc")))]
fn log_allocator() {
    tracing::info!("global allocator: jemalloc");
}

#[cfg(all(feature = "allocator-jemalloc", target_env = "msvc"))]
fn log_allocator() {
    tracing::info!("allocator-jemalloc enabled but using system allocator on MSVC");
}

#[cfg(not(feature = "allocator-jemalloc"))]
fn log_allocator() {
    tracing::info!("global allocator: system default");
}

#[cfg(feature = "profiling")]
fn start_profile_server_runtime(opts: &ServerOptions) -> Option<ProfileServerHandle> {
    let addr_str = opts
        .profile_addr
        .clone()
        .unwrap_or_else(|| DEFAULT_PROFILE_ADDR.to_string());
    let default_freq_hz = opts
        .cpu_profile_freq_hz
        .filter(|freq| *freq > 0)
        .unwrap_or(DEFAULT_CPU_PROFILE_FREQUENCY_HZ);
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(err) => {
            tracing::error!(error = %err, profile_addr = %addr_str, "invalid profile addr");
            return None;
        }
    };

    let listener = match TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!(error = %err, profile_addr = %addr, "profile server bind error");
            return None;
        }
    };
    if let Err(err) = listener.set_nonblocking(true) {
        tracing::error!(error = %err, profile_addr = %addr, "failed to configure profile listener");
        return None;
    }

    tracing::info!(profile_addr = %addr, "enabling profiling endpoints");
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let join_handle = thread::spawn(move || {
        if let Err(err) = run_profile_server(listener, addr, default_freq_hz, shutdown_rx) {
            tracing::error!(error = %err, "profile server error");
        }
    });
    Some(ProfileServerHandle {
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    })
}

#[cfg(not(feature = "profiling"))]
fn start_profile_server_runtime(_opts: &ServerOptions) -> Option<ProfileServerHandle> {
    None
}

#[cfg(feature = "profiling")]
fn run_profile_server(
    listener: TcpListener,
    addr: SocketAddr,
    default_freq_hz: i32,
    shutdown_rx: std::sync::mpsc::Receiver<()>,
) -> std::io::Result<()> {
    tracing::info!(
        profile_addr = %addr,
        "CPU/heap endpoints at http://{addr}/debug/pprof/{{profile,heap}}"
    );
    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }
        match listener.accept() {
            Ok((stream, _peer_addr)) => {
                thread::spawn(move || {
                    disable_heap_profiling_for_current_thread();
                    if let Err(err) = handle_profile_connection(stream, default_freq_hz) {
                        tracing::error!(error = %err, "profile connection failed");
                    }
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(err) => tracing::error!(error = %err, "profile accept error"),
        }
    }
    Ok(())
}

#[cfg(feature = "profiling")]
fn handle_profile_connection(
    mut stream: TcpStream,
    default_freq_hz: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 2048];
    let len = stream.read(&mut buf)?;
    if len == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buf[..len]);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    if method != "GET" {
        write_response(&mut stream, 405, "text/plain", b"method not allowed")?;
        return Ok(());
    }
    let (path, query) = split_target(target);
    match path {
        "/debug/pprof/profile" => {
            let duration = parse_seconds(query).unwrap_or(30);
            let frequency_hz = parse_i32_param(query, "freq")
                .filter(|freq| *freq > 0)
                .unwrap_or(default_freq_hz);
            match generate_profile(duration, frequency_hz) {
                Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
                Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
            }
        }
        "/debug/pprof/heap" => match capture_heap_profile() {
            Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
            Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
        },
        _ => {
            write_response(&mut stream, 404, "text/plain", b"not found")?;
        }
    }
    Ok(())
}

#[cfg(feature = "profiling")]
fn generate_profile(duration: u64, frequency_hz: i32) -> Result<Vec<u8>, String> {
    // When jemalloc heap profiling is active, generating CPU pprof can allocate heavily and
    // pollute `/debug/pprof/heap` inuse samples. Disable heap profiling for the entire CPU profile
    // request; heap dumps will block until profiling finishes.
    suspend_jemalloc_heap_profiling(|| {
        let mut builder = ProfilerGuardBuilder::default().frequency(frequency_hz);
        #[cfg(any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64"
        ))]
        {
            builder = builder.blocklist(&DEFAULT_CPU_PROFILE_BLOCKLIST);
        }
        let guard = builder.build().map_err(|err| err.to_string())?;
        thread::sleep(std::time::Duration::from_secs(duration));

        let report = guard.report().build().map_err(|err| err.to_string())?;
        let profile = report.pprof().map_err(|err| err.to_string())?;
        let mut body = Vec::new();
        profile
            .write_to_vec(&mut body)
            .map_err(|err| err.to_string())?;
        drop(guard);
        Ok(body)
    })
}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
#[allow(unsafe_code)] // approved: jemalloc mallctl raw API (prof.active / prof.dump)
fn capture_heap_profile() -> Result<Vec<u8>, String> {
    let _lock = PPROF_ENDPOINT_MUTEX.lock();

    // Ensure profiling is active; if jemalloc lacks profiling support, return a clear error.
    if let Err(err) = unsafe { raw::write(b"prof.active\0", true) } {
        return Err(format!(
            "jemalloc heap profiling not enabled (need _RJEM_MALLOC_CONF='prof:true,prof_active:true' with tikv-jemallocator profiling). error: {}",
            err
        ));
    }

    let filename = format!("/tmp/veloflux.{}.heap", process::id());
    let c_path = CString::new(filename.clone()).map_err(|err| err.to_string())?;
    unsafe {
        raw::write(b"prof.dump\0", c_path.as_ptr()).map_err(|err| err.to_string())?;
    }
    let body = fs::read(&filename).map_err(|err| err.to_string())?;
    let _ = fs::remove_file(&filename);
    Ok(body)
}

#[cfg(all(
    feature = "profiling",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn capture_heap_profile() -> Result<Vec<u8>, String> {
    Err(
        "jemalloc heap profiling requires feature `allocator-jemalloc` and a non-MSVC build"
            .to_string(),
    )
}

#[cfg(feature = "profiling")]
fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",
        status,
        status_text,
        body.len(),
        content_type
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

#[cfg(feature = "profiling")]
fn split_target(target: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = target.split_once('?') {
        (path, Some(query))
    } else {
        (target, None)
    }
}

/// Prepare Flow registries/instance before initialization, so callers can
/// register custom encoders/decoders/connectors prior to loading storage.
pub fn prepare_registry(
    flow_instances: &[manager::FlowInstanceSpec],
) -> Result<FlowInstance, Box<dyn std::error::Error + Send + Sync>> {
    let default_spec = manager::find_default_flow_instance_spec(flow_instances)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    manager::build_in_process_flow_instance(default_spec, None)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err).into())
}

#[cfg(feature = "profiling")]
fn parse_seconds(query: Option<&str>) -> Option<u64> {
    query
        .and_then(|q| {
            q.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                if key == "seconds" {
                    Some(value)
                } else {
                    None
                }
            })
        })
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(feature = "profiling")]
fn parse_i32_param(query: Option<&str>, key: &str) -> Option<i32> {
    query
        .and_then(|q| {
            q.split('&').find_map(|pair| {
                let (k, value) = pair.split_once('=')?;
                if k == key {
                    Some(value)
                } else {
                    None
                }
            })
        })
        .and_then(|value| value.parse::<i32>().ok())
}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
#[allow(unsafe_code)] // approved: jemalloc mallctl raw API (prof.active)
fn ensure_jemalloc_profiling() {
    // Best-effort: try to activate runtime profiling. If jemalloc was built
    // without profiling, mallctl will return an error and heap endpoint will
    // later surface a clearer message.
    let _ = unsafe { raw::write(b"prof.active\0", true) };
}

#[cfg(any(
    not(feature = "profiling"),
    not(feature = "allocator-jemalloc"),
    target_env = "msvc"
))]
fn ensure_jemalloc_profiling() {}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
#[allow(unsafe_code)] // approved: jemalloc mallctl raw API (prof.thread_active)
fn disable_heap_profiling_for_current_thread() {
    let _ = unsafe { raw::write(b"prof.thread_active\0", false) };
}

#[cfg(all(
    feature = "profiling",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn disable_heap_profiling_for_current_thread() {}

#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
#[allow(unsafe_code)] // approved: jemalloc mallctl raw API (prof.active read/write)
fn suspend_jemalloc_heap_profiling<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    let _lock = PPROF_ENDPOINT_MUTEX.lock();

    let prev_active: Option<bool> = match unsafe { raw::read(b"prof.active\0") } {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!(error = %err, "failed to read jemalloc prof.active");
            None
        }
    };

    if let Err(err) = unsafe { raw::write(b"prof.active\0", false) } {
        tracing::warn!(error = %err, "failed to disable jemalloc prof.active");
        return f();
    }

    let result = f();

    let restore_active = prev_active.unwrap_or(true);
    if let Err(err) = unsafe { raw::write(b"prof.active\0", restore_active) } {
        tracing::warn!(error = %err, "failed to restore jemalloc prof.active");
    }

    result
}

#[cfg(all(
    feature = "profiling",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn suspend_jemalloc_heap_profiling<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    f()
}

#[cfg(feature = "metrics")]
async fn init_metrics_exporter_runtime(
    opts: &ServerOptions,
) -> Result<Option<MetricsExporterRuntime>, Box<dyn std::error::Error + Send + Sync>> {
    let poll_interval = opts
        .metrics_poll_interval_secs
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_METRICS_POLL_INTERVAL_SECS);

    tracing::info!(
        metrics_poll_interval_secs = poll_interval,
        "enabling metrics collection"
    );

    spawn_tokio_metrics_collector(StdDuration::from_secs(poll_interval));

    tokio::spawn(async move {
        let mut system = System::new();
        let pid = Pid::from_u32(process::id());
        loop {
            system.refresh_process(pid);
            if let Some(proc_info) = system.process(pid) {
                let cpu_usage_percent = proc_info.cpu_usage() as f64;
                runtime_cpu_usage_percent().set(cpu_usage_percent);
                runtime_memory_usage_bytes().set(proc_info.memory() as i64);
            } else {
                runtime_cpu_usage_percent().set(0.0);
                runtime_memory_usage_bytes().set(0);
            }
            flow::collect_flow_instance_cpu_metrics_once();
            update_heap_metrics();
            sleep(Duration::from_secs(poll_interval)).await;
        }
    });
    Ok(Some(MetricsExporterRuntime))
}

#[cfg(not(feature = "metrics"))]
async fn init_metrics_exporter_runtime(
    _opts: &ServerOptions,
) -> Result<Option<MetricsExporterRuntime>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(None)
}

#[cfg(feature = "metrics")]
pub async fn init_metrics_exporter(
    opts: &ServerOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(runtime) = init_metrics_exporter_runtime(opts).await? {
        Box::leak(Box::new(runtime));
    }
    Ok(())
}

#[cfg(not(feature = "metrics"))]
pub async fn init_metrics_exporter(
    _opts: &ServerOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Ok(())
}

#[cfg(feature = "profiling")]
pub struct ProfileServerGuard {
    _handle: Option<ProfileServerHandle>,
}

#[cfg(feature = "profiling")]
pub fn start_profile_server(opts: &ServerOptions) -> ProfileServerGuard {
    ProfileServerGuard {
        _handle: start_profile_server_runtime(opts),
    }
}

#[cfg(not(feature = "profiling"))]
pub struct ProfileServerGuard;

#[cfg(not(feature = "profiling"))]
pub fn start_profile_server(_opts: &ServerOptions) -> ProfileServerGuard {
    ProfileServerGuard
}

#[cfg(all(
    feature = "metrics",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
fn update_heap_metrics() {
    if epoch::advance().is_err() {
        return;
    }
    let allocated = stats::allocated::read().unwrap_or(0);
    let resident = stats::resident::read().unwrap_or(0);
    runtime_heap_in_use_bytes().set(clamp_usize_to_i64(allocated));
    runtime_heap_in_allocator_bytes().set(clamp_usize_to_i64(resident));
}

#[cfg(all(
    feature = "metrics",
    any(not(feature = "allocator-jemalloc"), target_env = "msvc")
))]
fn update_heap_metrics() {
    runtime_heap_in_use_bytes().set(0);
    runtime_heap_in_allocator_bytes().set(0);
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)]
fn update_heap_metrics() {}

#[cfg(all(feature = "metrics", feature = "allocator-jemalloc"))]
fn clamp_usize_to_i64(value: usize) -> i64 {
    if value > i64::MAX as usize {
        i64::MAX
    } else {
        value as i64
    }
}
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use std::ffi::CString;
#[cfg(all(
    feature = "profiling",
    feature = "allocator-jemalloc",
    not(target_env = "msvc")
))]
use std::fs;
