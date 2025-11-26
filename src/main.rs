#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
fn log_allocator() {
    println!("[synapse-flow] global allocator: jemalloc");
}

#[cfg(target_env = "msvc")]
fn log_allocator() {
    println!("[synapse-flow] global allocator: system default");
}

use pprof::protos::Message;
use pprof::ProfilerGuard;
use std::env;
use std::ffi::CString;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process;
use std::thread;
use std::time::Duration as StdDuration;
use sysinfo::{Pid, System};
use telemetry::{
    spawn_tokio_metrics_collector, CPU_USAGE_GAUGE, HEAP_IN_ALLOCATOR_GAUGE, HEAP_IN_USE_GAUGE,
    MEMORY_USAGE_GAUGE,
};
use tikv_jemalloc_ctl::raw;
#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::{epoch, stats};
use tokio::time::{sleep, Duration};

const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9898";
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 5;
const DEFAULT_PROFILE_ADDR: &str = "0.0.0.0:8080";

#[derive(Debug, Clone, Copy)]
struct CliFlags {
    profiling_enabled: Option<bool>,
}

impl CliFlags {
    fn parse() -> Self {
        let mut profiling_enabled = None;
        for arg in env::args().skip(1) {
            match arg.as_str() {
                "--enable-profiling" | "--profiling" => profiling_enabled = Some(true),
                "--disable-profiling" | "--no-profiling" => profiling_enabled = Some(false),
                _ => {}
            }
        }
        Self { profiling_enabled }
    }

    fn profiling_override(&self) -> Option<bool> {
        self.profiling_enabled
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log_allocator();
    let cli_flags = CliFlags::parse();
    let profiling_enabled = cli_flags
        .profiling_override()
        .unwrap_or_else(profile_server_enabled);
    if profiling_enabled {
        ensure_jemalloc_profiling();
    }

    init_metrics_exporter().await?;
    if profiling_enabled {
        start_profile_server();
    }

    let manager_addr = env::var("MANAGER_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    println!("Starting manager on {}", manager_addr);
    let manager_future = manager::start_server(manager_addr.clone());

    tokio::select! {
        result = manager_future => {
            if let Err(err) = result {
                eprintln!("Manager server exited with error: {}", err);
                return Err(err);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Ctrl+C received, shutting down...");
        }
    }

    Ok(())
}

async fn init_metrics_exporter() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = env::var("METRICS_ADDR")
        .unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string())
        .parse()?;
    println!("[synapse-flow] enabling metrics exporter at {}", addr);
    let exporter = prometheus_exporter::start(addr)?;
    // Leak exporter handle so the HTTP endpoint stays alive for the duration of the process.
    Box::leak(Box::new(exporter));

    let poll_interval = env::var("METRICS_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    spawn_tokio_metrics_collector(StdDuration::from_secs(poll_interval));

    tokio::spawn(async move {
        let mut system = System::new();
        let pid = Pid::from_u32(process::id());
        loop {
            system.refresh_process(pid);
            if let Some(proc_info) = system.process(pid) {
                let cpu_usage_percent = proc_info.cpu_usage() as f64;
                CPU_USAGE_GAUGE.set(cpu_usage_percent as i64);
                MEMORY_USAGE_GAUGE.set(proc_info.memory() as i64);
            } else {
                CPU_USAGE_GAUGE.set(0);
                MEMORY_USAGE_GAUGE.set(0);
            }
            update_heap_metrics();
            sleep(Duration::from_secs(poll_interval)).await;
        }
    });
    Ok(())
}

#[cfg(not(target_env = "msvc"))]
fn update_heap_metrics() {
    if epoch::advance().is_err() {
        return;
    }
    let allocated = stats::allocated::read().unwrap_or(0);
    let resident = stats::resident::read().unwrap_or(0);
    HEAP_IN_USE_GAUGE.set(clamp_usize_to_i64(allocated));
    HEAP_IN_ALLOCATOR_GAUGE.set(clamp_usize_to_i64(resident));
}

#[cfg(target_env = "msvc")]
fn update_heap_metrics() {
    HEAP_IN_USE_GAUGE.set(0);
    HEAP_IN_ALLOCATOR_GAUGE.set(0);
}

#[cfg(not(target_env = "msvc"))]
fn clamp_usize_to_i64(value: usize) -> i64 {
    if value > i64::MAX as usize {
        i64::MAX
    } else {
        value as i64
    }
}

fn start_profile_server() {
    let addr_str = env::var("PROFILE_ADDR").unwrap_or_else(|_| DEFAULT_PROFILE_ADDR.to_string());
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(err) => {
            eprintln!("[ProfileServer] invalid PROFILE_ADDR {addr_str}: {err}");
            return;
        }
    };

    println!("[ProfileServer] enabling profiling endpoints on {}", addr);
    thread::spawn(move || {
        if let Err(err) = run_profile_server(addr) {
            eprintln!("[ProfileServer] server error: {err}");
        }
    });
}

fn run_profile_server(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!(
        "[ProfileServer] CPU/heap endpoints at http://{addr}/debug/pprof/{{profile,flamegraph,heap}}"
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    if let Err(err) = handle_profile_connection(stream) {
                        eprintln!("[ProfileServer] connection failed: {err}");
                    }
                });
            }
            Err(err) => eprintln!("[ProfileServer] accept error: {err}"),
        }
    }
    Ok(())
}

fn handle_profile_connection(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
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
            match generate_profile(duration) {
                Ok(body) => write_response(&mut stream, 200, "application/octet-stream", &body)?,
                Err(err) => write_response(&mut stream, 500, "text/plain", err.as_bytes())?,
            }
        }
        "/debug/pprof/flamegraph" => {
            let duration = parse_seconds(query).unwrap_or(30);
            match generate_flamegraph(duration) {
                Ok(body) => write_response(&mut stream, 200, "image/svg+xml", &body)?,
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

fn generate_profile(duration: u64) -> Result<Vec<u8>, String> {
    let report = run_profiler(duration)?;
    let profile = report.pprof().map_err(|err| err.to_string())?;
    let mut body = Vec::new();
    profile.encode(&mut body).map_err(|err| err.to_string())?;
    Ok(body)
}

fn generate_flamegraph(duration: u64) -> Result<Vec<u8>, String> {
    let report = run_profiler(duration)?;
    let mut body = Vec::new();
    report
        .flamegraph(&mut body)
        .map_err(|err| err.to_string())?;
    Ok(body)
}

fn run_profiler(duration: u64) -> Result<pprof::Report, String> {
    let guard = ProfilerGuard::new(100).map_err(|err| err.to_string())?;
    thread::sleep(StdDuration::from_secs(duration));
    guard.report().build().map_err(|err| err.to_string())
}

fn capture_heap_profile() -> Result<Vec<u8>, String> {
    // Ensure profiling is active; if jemalloc lacks profiling support, return a clear error.
    if let Err(err) = unsafe { raw::write(b"prof.active\0", true) } {
        return Err(format!(
            "jemalloc heap profiling 未开启或不支持（需要带 profiling 的 tikv-jemallocator，且启动时设置 MALLOC_CONF=\"prof:true,prof_active:true\"）。错误: {}",
            err
        ));
    }

    let filename = format!("/tmp/synapse_flow.{}.heap", process::id());
    let c_path = CString::new(filename.clone()).map_err(|err| err.to_string())?;
    unsafe {
        raw::write(b"prof.dump\0", c_path.as_ptr()).map_err(|err| err.to_string())?;
    }
    let body = fs::read(&filename).map_err(|err| err.to_string())?;
    let _ = fs::remove_file(&filename);
    Ok(body)
}

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

fn split_target(target: &str) -> (&str, Option<&str>) {
    if let Some((path, query)) = target.split_once('?') {
        (path, Some(query))
    } else {
        (target, None)
    }
}

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

fn ensure_jemalloc_profiling() {
    // Best-effort: try to activate runtime profiling. If jemalloc was built
    // without profiling, mallctl will return an error and heap endpoint will
    // later surface a clearer message.
    let _ = unsafe { raw::write(b"prof.active\0", true) };
}

fn profile_server_enabled() -> bool {
    matches!(
        env::var("PROFILE_SERVER_ENABLE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}
