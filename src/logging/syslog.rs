use crate::config::SyslogLoggingConfig;
use std::io;
use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{Level, Metadata};
use tracing_subscriber::fmt::writer::MakeWriter;

#[cfg(unix)]
use {
    std::os::unix::net::UnixDatagram,
    std::path::{Path, PathBuf},
};

const SYSLOG_QUEUE_CAPACITY: usize = 8192;
const SYSLOG_USER_FACILITY_CODE: u8 = 1;
const SYSLOG_TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone)]
enum SyslogDestination {
    #[cfg(unix)]
    UnixDatagram {
        path: PathBuf,
    },
    Udp {
        address: String,
    },
    Tcp {
        address: String,
    },
}

enum SyslogConnection {
    #[cfg(unix)]
    UnixDatagram(UnixDatagram),
    Udp(UdpSocket),
    Tcp(TcpStream),
}

pub struct SyslogMakeWriter {
    sender: SyncSender<SyslogRecord>,
}

pub struct SyslogWorkerGuard {
    shutdown: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
}

struct SyslogRecord {
    severity: SyslogSeverity,
    body: Vec<u8>,
}

pub struct SyslogEventWriter {
    sender: SyncSender<SyslogRecord>,
    severity: SyslogSeverity,
    buf: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyslogSeverity {
    Error,
    Warning,
    Informational,
    Debug,
}

pub fn open_syslog(
    cfg: &SyslogLoggingConfig,
    effective_app_name: &str,
) -> io::Result<(SyslogMakeWriter, SyslogWorkerGuard)> {
    let destination = resolve_syslog_destination(cfg)?;
    open_syslog_with_destination(destination, effective_app_name)
}

fn open_syslog_with_destination(
    destination: SyslogDestination,
    effective_app_name: &str,
) -> io::Result<(SyslogMakeWriter, SyslogWorkerGuard)> {
    let connection = open_destination(&destination)?;

    let (sender, receiver) = sync_channel(SYSLOG_QUEUE_CAPACITY);
    let app_name = Arc::<str>::from(effective_app_name.to_string());
    let worker_destination = destination.clone();
    let worker_app_name = Arc::clone(&app_name);
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown);
    let join_handle = thread::Builder::new()
        .name("veloflux-syslog".to_string())
        .spawn(move || {
            run_syslog_worker(
                worker_destination,
                worker_app_name,
                receiver,
                worker_shutdown,
                connection,
            )
        })?;
    let make_writer = SyslogMakeWriter {
        sender: sender.clone(),
    };
    let guard = SyslogWorkerGuard {
        shutdown,
        join_handle: Some(join_handle),
    };
    Ok((make_writer, guard))
}

fn resolve_syslog_destination(cfg: &SyslogLoggingConfig) -> io::Result<SyslogDestination> {
    let network = cfg.network.trim();
    let address = cfg.address.trim();

    if network.is_empty() || address.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "logging.syslog.network and logging.syslog.address must both be explicitly configured",
        ));
    }

    match network.to_ascii_lowercase().as_str() {
        "unixgram" => {
            #[cfg(unix)]
            {
                Ok(SyslogDestination::UnixDatagram {
                    path: PathBuf::from(address),
                })
            }
            #[cfg(not(unix))]
            {
                Err(io::Error::other(
                    "logging.syslog.network=unixgram requires Unix-domain socket support",
                ))
            }
        }
        "udp" => Ok(SyslogDestination::Udp {
            address: address.to_string(),
        }),
        "tcp" => Ok(SyslogDestination::Tcp {
            address: address.to_string(),
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unsupported logging.syslog.network {network:?}; expected unixgram, udp, or tcp"
            ),
        )),
    }
}

impl SyslogMakeWriter {
    fn writer_for_level(&self, level: Level) -> SyslogEventWriter {
        SyslogEventWriter {
            sender: self.sender.clone(),
            severity: SyslogSeverity::from_level(level),
            buf: Vec::with_capacity(256),
        }
    }
}

impl<'a> MakeWriter<'a> for SyslogMakeWriter {
    type Writer = SyslogEventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.writer_for_level(Level::INFO)
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        self.writer_for_level(*meta.level())
    }
}

impl Write for SyslogEventWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for SyslogEventWriter {
    fn drop(&mut self) {
        trim_line_endings(&mut self.buf);
        if self.buf.is_empty() {
            return;
        }
        let record = SyslogRecord {
            severity: self.severity,
            body: std::mem::take(&mut self.buf),
        };
        match self.sender.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

impl Drop for SyslogWorkerGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

fn trim_line_endings(buf: &mut Vec<u8>) {
    while matches!(buf.last(), Some(b'\n' | b'\r')) {
        let _ = buf.pop();
    }
}

fn run_syslog_worker(
    destination: SyslogDestination,
    app_name: Arc<str>,
    receiver: Receiver<SyslogRecord>,
    shutdown: Arc<AtomicBool>,
    connection: SyslogConnection,
) {
    let mut connection = Some(connection);
    let pid = std::process::id();

    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let record = match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(record) => record,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let payload = format_syslog_message(&app_name, pid, record.severity, &record.body);
        if let Some(active_connection) = connection.as_mut() {
            if active_connection.send(&payload).is_ok() {
                continue;
            }
        }

        connection = open_destination(&destination).ok();
        if let Some(active_connection) = connection.as_mut() {
            if active_connection.send(&payload).is_err() {
                connection = None;
            }
        }
    }
}

fn open_destination(destination: &SyslogDestination) -> io::Result<SyslogConnection> {
    match destination {
        #[cfg(unix)]
        SyslogDestination::UnixDatagram { path } => {
            open_syslog_socket(path).map(SyslogConnection::UnixDatagram)
        }
        SyslogDestination::Udp { address } => open_udp_socket(address).map(SyslogConnection::Udp),
        SyslogDestination::Tcp { address } => open_tcp_stream(address).map(SyslogConnection::Tcp),
    }
}

fn open_tcp_stream(address: &str) -> io::Result<TcpStream> {
    let mut last_error = None;

    for destination in address.to_socket_addrs()? {
        match TcpStream::connect_timeout(&destination, SYSLOG_TCP_CONNECT_TIMEOUT) {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                return Ok(stream);
            }
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("syslog address {address:?} did not resolve to any socket address"),
        )
    }))
}

fn open_udp_socket(address: &str) -> io::Result<UdpSocket> {
    let mut last_error = None;
    let mut resolved_any = false;

    for destination in address.to_socket_addrs()? {
        resolved_any = true;
        let bind_address = if destination.is_ipv4() {
            SocketAddr::from(([0, 0, 0, 0], 0))
        } else {
            SocketAddr::from(([0_u16; 8], 0))
        };
        let socket = match UdpSocket::bind(bind_address) {
            Ok(socket) => socket,
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };
        match socket.connect(destination) {
            Ok(()) => return Ok(socket),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            if resolved_any {
                format!("failed to connect UDP syslog socket to {address}")
            } else {
                format!("syslog address {address:?} did not resolve to any socket address")
            },
        )
    }))
}

impl SyslogConnection {
    fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::UnixDatagram(socket) => ensure_datagram_sent(socket.send(payload), payload.len()),
            Self::Udp(socket) => ensure_datagram_sent(socket.send(payload), payload.len()),
            Self::Tcp(stream) => stream.write_all(&format_tcp_frame(payload)),
        }
    }
}

fn ensure_datagram_sent(result: io::Result<usize>, expected: usize) -> io::Result<()> {
    match result {
        Ok(actual) if actual == expected => Ok(()),
        Ok(actual) => Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("syslog datagram write sent {actual} of {expected} bytes"),
        )),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn open_syslog_socket(path: &Path) -> io::Result<UnixDatagram> {
    let socket = UnixDatagram::unbound()?;
    socket.connect(path)?;
    Ok(socket)
}

fn format_syslog_message(
    app_name: &str,
    pid: u32,
    severity: SyslogSeverity,
    body: &[u8],
) -> Vec<u8> {
    let pri = SYSLOG_USER_FACILITY_CODE * 8 + severity.code();
    let mut message = Vec::with_capacity(body.len() + app_name.len() + 32);
    let header = format!("<{pri}>{app_name}[{pid}]: ");
    message.extend_from_slice(header.as_bytes());
    message.extend_from_slice(body);
    message
}

fn format_tcp_frame(payload: &[u8]) -> Vec<u8> {
    let length = payload.len().to_string();
    let mut frame = Vec::with_capacity(length.len() + 1 + payload.len());
    frame.extend_from_slice(length.as_bytes());
    frame.push(b' ');
    frame.extend_from_slice(payload);
    frame
}

impl SyslogSeverity {
    fn from_level(level: Level) -> Self {
        match level {
            Level::ERROR => Self::Error,
            Level::WARN => Self::Warning,
            Level::INFO => Self::Informational,
            Level::DEBUG | Level::TRACE => Self::Debug,
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Error => 3,
            Self::Warning => 4,
            Self::Informational => 6,
            Self::Debug => 7,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_socket_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("veloflux_test.{name}.{nanos}.sock"))
    }

    #[test]
    fn formats_syslog_priority() {
        let message = format_syslog_message(
            "veloflux-manager",
            42,
            SyslogSeverity::Warning,
            b"connector failed",
        );
        assert_eq!(
            String::from_utf8(message).expect("valid utf8"),
            "<12>veloflux-manager[42]: connector failed"
        );
    }

    #[test]
    fn requires_network_and_address_to_be_set_together() {
        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "udp".to_string(),
            address: String::new(),
        };

        let err = resolve_syslog_destination(&cfg).expect_err("missing address should fail");
        assert!(err
            .to_string()
            .contains("must both be explicitly configured"));
    }

    #[test]
    fn rejects_implicit_local_syslog_destination() {
        let cfg = SyslogLoggingConfig::default();

        let err = resolve_syslog_destination(&cfg).expect_err("implicit destination should fail");
        assert!(err
            .to_string()
            .contains("must both be explicitly configured"));
    }

    #[test]
    fn rejects_unsupported_network() {
        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "tls".to_string(),
            address: "127.0.0.1:514".to_string(),
        };

        let err = resolve_syslog_destination(&cfg).expect_err("unknown transport should fail");
        assert!(err.to_string().contains("expected unixgram, udp, or tcp"));
    }

    #[cfg(unix)]
    #[test]
    fn worker_sends_record_to_custom_unix_datagram_socket() {
        let path = unique_socket_path("syslog");
        let receiver = UnixDatagram::bind(&path).expect("bind unix datagram");
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .expect("set read timeout");

        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "unixgram".to_string(),
            address: path.to_string_lossy().into_owned(),
        };
        let (make_writer, guard) =
            open_syslog(&cfg, "veloflux-worker-default").expect("open syslog");
        {
            let mut writer = make_writer.writer_for_level(Level::WARN);
            writer
                .write_all(b"worker failed to open connector")
                .expect("write warning");
        }

        let mut buf = [0u8; 512];
        let len = receiver.recv(&mut buf).expect("receive syslog datagram");
        let message = std::str::from_utf8(&buf[..len]).expect("utf8 syslog payload");
        assert!(message.starts_with("<12>veloflux-worker-default["));
        assert!(message.ends_with(": worker failed to open connector"));

        drop(guard);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn worker_sends_record_to_udp_server() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind UDP receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set read timeout");
        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "udp".to_string(),
            address: receiver
                .local_addr()
                .expect("UDP receiver address")
                .to_string(),
        };
        let (make_writer, guard) = open_syslog(&cfg, "veloflux-manager").expect("open syslog");

        {
            let mut writer = make_writer.writer_for_level(Level::INFO);
            writer.write_all(b"UDP syslog message").expect("write info");
        }

        let mut buf = [0_u8; 512];
        let (len, _) = receiver.recv_from(&mut buf).expect("receive UDP datagram");
        let message = std::str::from_utf8(&buf[..len]).expect("UTF-8 syslog payload");
        assert!(message.starts_with("<14>veloflux-manager["));
        assert!(message.ends_with(": UDP syslog message"));

        drop(guard);
    }

    #[test]
    fn worker_sends_octet_counted_record_to_tcp_server() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP listener");
        let cfg = SyslogLoggingConfig {
            enable: true,
            level: None,
            tag: "veloflux".to_string(),
            network: "tcp".to_string(),
            address: listener
                .local_addr()
                .expect("TCP listener address")
                .to_string(),
        };
        let (make_writer, guard) = open_syslog(&cfg, "veloflux-embedded").expect("open syslog");
        let (mut receiver, _) = listener.accept().expect("accept TCP connection");
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set read timeout");

        {
            let mut writer = make_writer.writer_for_level(Level::ERROR);
            writer
                .write_all(b"TCP syslog message")
                .expect("write error");
        }

        let message = receive_octet_counted_message(&mut receiver);
        let message = std::str::from_utf8(&message).expect("UTF-8 syslog payload");
        assert!(message.starts_with("<11>veloflux-embedded["));
        assert!(message.ends_with(": TCP syslog message"));

        drop(guard);
    }

    fn receive_octet_counted_message(stream: &mut TcpStream) -> Vec<u8> {
        let mut length = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte).expect("read frame length");
            if byte[0] == b' ' {
                break;
            }
            length.push(byte[0]);
        }
        let length = std::str::from_utf8(&length)
            .expect("UTF-8 frame length")
            .parse::<usize>()
            .expect("numeric frame length");
        let mut message = vec![0_u8; length];
        stream
            .read_exact(&mut message)
            .expect("read framed syslog message");
        message
    }
}
