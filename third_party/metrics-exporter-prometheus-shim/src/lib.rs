use std::fmt;
use std::net::SocketAddr;

#[derive(Debug, Default, Clone, Copy)]
pub struct PrometheusBuilder {
    http_listener: Option<SocketAddr>,
}

impl PrometheusBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_http_listener(mut self, addr: SocketAddr) -> Self {
        self.http_listener = Some(addr);
        self
    }

    pub fn install(self) -> Result<(), InstallError> {
        let _ = self.http_listener;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct InstallError;

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("prometheus metrics exporter shim install failed")
    }
}

impl std::error::Error for InstallError {}
