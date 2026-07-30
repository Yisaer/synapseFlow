//! HTTP sink connector for delivering encoded payloads to remote HTTP endpoints.

use super::{DeliveryResult, SinkConnector, SinkConnectorError};
use crate::pipeline::HttpBodyConfig;
use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use reqwest::{Client, Method, StatusCode};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for the HTTP sink connector.
///
/// Retry is handled at the SinkProcessor level, not inside the connector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpSinkConfig {
    /// Target URL (required).
    pub url: String,
    /// HTTP method to use for each delivery.
    pub method: HttpMethod,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Custom headers to include in every request.
    pub headers: HashMap<String, String>,
    /// Explicit Content-Type header value. When `None`, the value is inferred
    /// from the pipeline encoder kind during plan building.
    pub content_type: Option<String>,
    /// Maximum allowed body size (bytes) for a single delivery. Exceeding this
    /// limit aborts the delivery and returns an error.
    pub max_body_size: usize,
    /// HTTP request body mode.
    pub body: HttpBodyConfig,
}

impl HttpSinkConfig {
    /// Create a new HTTP sink config with the given URL and default values.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: HttpMethod::default(),
            timeout: Duration::from_secs(30),
            headers: HashMap::new(),
            content_type: None,
            max_body_size: 64 * 1024 * 1024,
            body: HttpBodyConfig::Raw,
        }
    }

    /// Set the HTTP method.
    pub fn with_method(mut self, method: HttpMethod) -> Self {
        self.method = method;
        self
    }

    /// Set the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Add a custom header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the Content-Type explicitly.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set the maximum body size for a single delivery.
    pub fn with_max_body_size(mut self, max_bytes: usize) -> Self {
        self.max_body_size = max_bytes;
        self
    }

    /// Set the HTTP request body mode.
    pub fn with_body(mut self, body: HttpBodyConfig) -> Self {
        self.body = body;
        self
    }

    /// Infer and set `content_type` from the pipeline encoder kind when it is
    /// not already explicitly configured. Called during physical plan building.
    pub fn with_inferred_content_type(mut self, encoder_kind: Option<&str>) -> Self {
        if matches!(self.body, HttpBodyConfig::Raw) && self.content_type.is_none() {
            self.content_type = infer_content_type_for_encoder(encoder_kind);
        }
        self
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        let HttpBodyConfig::Multipart(config) = &self.body else {
            return Ok(());
        };

        validate_multipart_name(&config.file_field_name, "file_field_name")?;
        validate_multipart_name(&config.file_name, "file_name")?;
        for name in config.fields.keys() {
            validate_multipart_name(name, "text field name")?;
            if name == &config.file_field_name {
                return Err(format!(
                    "http multipart text field `{name}` conflicts with file_field_name"
                ));
            }
        }
        if self.content_type.is_some() {
            return Err("http multipart body does not allow an explicit content_type".to_string());
        }
        if self
            .headers
            .keys()
            .any(|name| name.trim().eq_ignore_ascii_case("content-type"))
        {
            return Err("http multipart body does not allow a Content-Type header".to_string());
        }
        Ok(())
    }
}

fn validate_multipart_name(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("http multipart {field} must not be empty"));
    }
    if value
        .chars()
        .any(|character| matches!(character, '\r' | '\n' | '\0'))
    {
        return Err(format!(
            "http multipart {field} must not contain CR, LF, or NUL"
        ));
    }
    Ok(())
}

/// Infer a Content-Type header value from the encoder kind string.
fn infer_content_type_for_encoder(encoder_kind: Option<&str>) -> Option<String> {
    match encoder_kind {
        Some("csv") => Some("text/csv; charset=utf-8".to_string()),
        Some("json") => Some("application/json".to_string()),
        Some("protobuf") => Some("application/octet-stream".to_string()),
        _ => None,
    }
}

/// Returns `true` when the HTTP status code is transient and worth retrying.
fn is_transient_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

/// Returns `true` when a reqwest-level error represents a transient failure.
fn is_transient_reqwest_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

/// Supported HTTP methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    #[default]
    Post,
    /// HTTP PUT.
    Put,
    /// HTTP PATCH.
    Patch,
    /// HTTP DELETE.
    Delete,
}

impl HttpMethod {
    fn to_reqwest(self) -> Method {
        match self {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
            HttpMethod::Patch => Method::PATCH,
            HttpMethod::Delete => Method::DELETE,
        }
    }
}

/// Sink connector that delivers encoded payloads to an HTTP endpoint.
///
/// Each delivery (start_delivery → write_chunk* → finish_delivery) is sent
/// as a single HTTP request. The connector accumulates chunks into an
/// in-memory buffer and sends the complete body on finish_delivery.
///
/// This connector performs a single attempt per delivery. Retry is handled
/// by the SinkProcessor based on the error classification returned here.
pub(crate) struct HttpSinkConnector {
    id: String,
    config: HttpSinkConfig,
    client: Option<Client>,
    buffer: Vec<u8>,
    delivery_active: bool,
}

impl HttpSinkConnector {
    /// Create a new HTTP sink connector with the given identifier and config.
    pub fn new(id: impl Into<String>, config: HttpSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
            client: None,
            buffer: Vec::new(),
            delivery_active: false,
        }
    }

    fn validate_url(url: &str) -> Result<(), SinkConnectorError> {
        if url.trim().is_empty() {
            return Err(SinkConnectorError::Other(
                "http sink requires a non-empty url".to_string(),
            ));
        }
        url::Url::parse(url).map_err(|err| {
            SinkConnectorError::Other(format!("http sink has an invalid url: {err}"))
        })?;
        Ok(())
    }

    fn build_client(&self) -> Result<Client, SinkConnectorError> {
        reqwest::Client::builder()
            .timeout(self.config.timeout)
            .build()
            .map_err(|err| {
                SinkConnectorError::Other(format!(
                    "http sink `{}` failed to build http client: {err}",
                    self.id
                ))
            })
    }

    /// Send a single HTTP request. Returns `Ok(())` on 2xx.
    /// Errors are classified as `Transient` (5xx, 429, network errors) or
    /// `Permanent` (4xx client errors).
    async fn send_single_request(
        connector_id: &str,
        client: &Client,
        config: &HttpSinkConfig,
        body: bytes::Bytes,
    ) -> Result<(), SinkConnectorError> {
        let mut request = client.request(config.method.to_reqwest(), &config.url);

        for (key, value) in &config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        match &config.body {
            HttpBodyConfig::Raw => {
                request = request.body(body);
                if let Some(ct) = config.content_type.as_deref() {
                    request = request.header("Content-Type", ct);
                }
            }
            HttpBodyConfig::Multipart(config) => {
                let body_len = body.len() as u64;
                let file_part = Part::stream_with_length(body, body_len)
                    .file_name(config.file_name.clone())
                    .mime_str("application/octet-stream")
                    .map_err(|err| {
                        SinkConnectorError::Other(format!(
                            "http sink `{connector_id}` failed to build multipart file part: {err}"
                        ))
                    })?;
                let mut form = Form::new().part(config.file_field_name.clone(), file_part);
                for (name, value) in &config.fields {
                    form = form.text(name.clone(), value.clone());
                }
                request = request.multipart(form);
            }
        }

        let response = match request.send().await {
            Ok(r) => r,
            Err(err) => {
                let msg = format!("http sink `{connector_id}` request error: {err}");
                return if is_transient_reqwest_error(&err) {
                    Err(SinkConnectorError::Transient(msg))
                } else {
                    // Non-timeout, non-connect reqwest errors are treated as
                    // transient as well (e.g. DNS resolution may recover).
                    Err(SinkConnectorError::Transient(msg))
                };
            }
        };

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let message = format!(
            "http sink `{connector_id}` received non-success status: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("unknown")
        );

        if is_transient_status(status) {
            Err(SinkConnectorError::Transient(message))
        } else {
            Err(SinkConnectorError::Permanent(message))
        }
    }
}

#[async_trait]
impl SinkConnector for HttpSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn max_delivery_bytes(&self) -> Option<usize> {
        Some(self.config.max_body_size)
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        Self::validate_url(&self.config.url)?;
        self.config.validate().map_err(SinkConnectorError::Other)?;
        self.client = Some(self.build_client()?);
        Ok(())
    }

    async fn start_delivery(&mut self) -> Result<(), SinkConnectorError> {
        if self.delivery_active {
            return Err(SinkConnectorError::Other(format!(
                "http sink `{}` already has an active delivery",
                self.id
            )));
        }
        self.buffer.clear();
        self.delivery_active = true;
        Ok(())
    }

    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), SinkConnectorError> {
        if !self.delivery_active {
            return Err(SinkConnectorError::Other(format!(
                "http sink `{}` received a chunk without an active delivery",
                self.id
            )));
        }

        let new_len = self.buffer.len() + bytes.len();
        if new_len > self.config.max_body_size {
            self.buffer.clear();
            self.delivery_active = false;
            return Err(SinkConnectorError::Other(format!(
                "http sink `{}` body size {} exceeds max_body_size {}",
                self.id, new_len, self.config.max_body_size
            )));
        }

        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish_delivery(&mut self) -> Result<DeliveryResult, SinkConnectorError> {
        if !self.delivery_active {
            return Err(SinkConnectorError::Other(format!(
                "http sink `{}` finished without an active delivery",
                self.id
            )));
        }
        self.delivery_active = false;

        let client = self.client.as_ref().ok_or_else(|| {
            SinkConnectorError::Other(format!(
                "http sink `{}` client is not initialized; call ready() first",
                self.id
            ))
        })?;

        let body_bytes: bytes::Bytes = std::mem::take(&mut self.buffer).into();
        let bytes_written = body_bytes.len() as u64;

        // Single attempt — retry is managed by the SinkProcessor.
        Self::send_single_request(&self.id, client, &self.config, body_bytes).await?;

        Ok(DeliveryResult { bytes_written })
    }

    async fn abort_delivery(&mut self) {
        self.buffer.clear();
        self.delivery_active = false;
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.buffer.clear();
        self.delivery_active = false;
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::HttpMultipartConfig;
    use axum::{extract::Multipart, routing::post, Router};
    use std::collections::BTreeMap;

    #[test]
    fn config_new_sets_sensible_defaults() {
        let cfg = HttpSinkConfig::new("https://example.com/api");
        assert_eq!(cfg.url, "https://example.com/api");
        assert_eq!(cfg.method, HttpMethod::Post);
        assert_eq!(cfg.timeout, Duration::from_secs(30));
        assert!(cfg.headers.is_empty());
        assert_eq!(cfg.content_type, None);
        assert_eq!(cfg.max_body_size, 64 * 1024 * 1024);
    }

    #[test]
    fn config_builder_methods() {
        let cfg = HttpSinkConfig::new("https://example.com/api")
            .with_method(HttpMethod::Put)
            .with_timeout(Duration::from_secs(10))
            .with_header("Authorization", "Bearer token")
            .with_content_type("application/json")
            .with_max_body_size(1024);

        assert_eq!(cfg.method, HttpMethod::Put);
        assert_eq!(cfg.timeout, Duration::from_secs(10));
        assert_eq!(
            cfg.headers.get("Authorization").map(String::as_str),
            Some("Bearer token")
        );
        assert_eq!(cfg.content_type.as_deref(), Some("application/json"));
        assert_eq!(cfg.max_body_size, 1024);
    }

    #[test]
    fn transient_statuses_are_server_errors_and_429() {
        assert!(is_transient_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_transient_status(StatusCode::BAD_GATEWAY));
        assert!(is_transient_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_transient_status(StatusCode::GATEWAY_TIMEOUT));
        assert!(is_transient_status(StatusCode::TOO_MANY_REQUESTS));
    }

    #[test]
    fn non_transient_statuses_are_client_errors_except_429() {
        assert!(!is_transient_status(StatusCode::BAD_REQUEST));
        assert!(!is_transient_status(StatusCode::NOT_FOUND));
        assert!(!is_transient_status(StatusCode::UNAUTHORIZED));
        assert!(!is_transient_status(StatusCode::FORBIDDEN));
    }

    #[test]
    fn success_statuses_are_not_transient() {
        assert!(!is_transient_status(StatusCode::OK));
        assert!(!is_transient_status(StatusCode::CREATED));
        assert!(!is_transient_status(StatusCode::NO_CONTENT));
    }

    #[test]
    fn infer_content_type_json() {
        let cfg =
            HttpSinkConfig::new("https://example.com/api").with_inferred_content_type(Some("json"));
        assert_eq!(cfg.content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn infer_content_type_protobuf() {
        let cfg = HttpSinkConfig::new("https://example.com/api")
            .with_inferred_content_type(Some("protobuf"));
        assert_eq!(
            cfg.content_type.as_deref(),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn infer_content_type_csv() {
        let cfg =
            HttpSinkConfig::new("https://example.com/api").with_inferred_content_type(Some("csv"));
        assert_eq!(cfg.content_type.as_deref(), Some("text/csv; charset=utf-8"));
    }

    #[test]
    fn infer_content_type_unknown_is_none() {
        let cfg = HttpSinkConfig::new("https://example.com/api")
            .with_inferred_content_type(Some("custom_encoder"));
        assert_eq!(cfg.content_type, None);
    }

    #[test]
    fn infer_content_type_none_is_none() {
        let cfg = HttpSinkConfig::new("https://example.com/api").with_inferred_content_type(None);
        assert_eq!(cfg.content_type, None);
    }

    #[test]
    fn explicit_content_type_is_not_overwritten_by_inference() {
        let cfg = HttpSinkConfig::new("https://example.com/api")
            .with_content_type("text/plain")
            .with_inferred_content_type(Some("json"));
        assert_eq!(cfg.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn multipart_content_type_is_not_inferred() {
        let cfg = HttpSinkConfig::new("https://example.com/api")
            .with_body(HttpBodyConfig::Multipart(HttpMultipartConfig {
                file_field_name: "d".to_string(),
                file_name: "payload.bin".to_string(),
                fields: BTreeMap::new(),
            }))
            .with_inferred_content_type(Some("json"));
        assert_eq!(cfg.content_type, None);
    }

    #[tokio::test]
    async fn multipart_sends_file_and_static_text_fields() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        async fn receive(mut multipart: Multipart) {
            let mut file_seen = false;
            let mut text_fields = BTreeMap::new();

            while let Some(field) = multipart.next_field().await.unwrap() {
                let name = field.name().unwrap().to_string();
                if name == "d" {
                    assert_eq!(field.file_name(), Some("payload.bin"));
                    assert_eq!(
                        field.content_type().map(|value| value.as_ref()),
                        Some("application/octet-stream")
                    );
                    assert_eq!(field.bytes().await.unwrap().as_ref(), b"encoded-payload");
                    file_seen = true;
                } else {
                    text_fields.insert(name, field.text().await.unwrap());
                }
            }

            assert!(file_seen);
            assert_eq!(text_fields.get("rid").map(String::as_str), Some("cold"));
            assert_eq!(text_fields.get("tp").map(String::as_str), Some("1"));
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/upload", post(receive));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let fields = BTreeMap::from([
            ("rid".to_string(), "cold".to_string()),
            ("tp".to_string(), "1".to_string()),
        ]);
        let config = HttpSinkConfig::new(format!("http://{address}/upload")).with_body(
            HttpBodyConfig::Multipart(HttpMultipartConfig {
                file_field_name: "d".to_string(),
                file_name: "payload.bin".to_string(),
                fields,
            }),
        );
        let mut connector = HttpSinkConnector::new("multipart-test", config);
        connector.ready().await.unwrap();
        connector.start_delivery().await.unwrap();
        connector.write_chunk(b"encoded-").await.unwrap();
        connector.write_chunk(b"payload").await.unwrap();
        let result = connector.finish_delivery().await.unwrap();

        assert_eq!(result.bytes_written, 15);
        server.abort();
    }
}
