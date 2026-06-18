use bytes::Bytes;
use http::{Method, StatusCode};

use crate::config::ClientConfig;
use crate::error::SdkError;

#[derive(Clone)]
pub struct Transport {
    cfg: ClientConfig,
    http: reqwest::Client,
}

impl Transport {
    pub fn new(cfg: ClientConfig) -> Result<Self, SdkError> {
        crate::tls::install_default_crypto_provider();
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(|e| SdkError::transport("INIT", "<client>", e))?;
        Ok(Self { cfg, http })
    }

    pub fn config(&self) -> &ClientConfig {
        &self.cfg
    }

    pub async fn send_json<B: serde::Serialize>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&B>,
    ) -> Result<(StatusCode, Bytes), SdkError> {
        let method_s = method.as_str().to_string();
        let url = self
            .cfg
            .base_url
            .join(path)
            .map_err(|e| SdkError::url(&method_s, path, e))?;

        let mut rb = self.http.request(method.clone(), url);

        if !query.is_empty() {
            rb = rb.query(query);
        }
        if let Some(b) = body {
            rb = rb.json(b);
        }

        let resp = rb
            .send()
            .await
            .map_err(|e| SdkError::transport(&method_s, path, e))?;

        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SdkError::transport(&method_s, path, e))?;

        Ok((status, bytes))
    }

    pub async fn expect_empty_success<B: serde::Serialize>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&B>,
        ok_statuses: &[StatusCode],
    ) -> Result<(), SdkError> {
        let method_s = method.as_str().to_string();
        let (status, bytes) = self.send_json(method, path, query, body).await?;
        if ok_statuses.contains(&status) {
            return Ok(());
        }
        let body_text = String::from_utf8_lossy(&bytes).to_string();
        Err(SdkError::http(&method_s, path, status, body_text))
    }

    pub async fn expect_json_success<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&B>,
        ok_statuses: &[StatusCode],
    ) -> Result<T, SdkError> {
        let method_s = method.as_str().to_string();
        let (status, bytes) = self.send_json(method, path, query, body).await?;
        if !ok_statuses.contains(&status) {
            let body_text = String::from_utf8_lossy(&bytes).to_string();
            return Err(SdkError::http(&method_s, path, status, body_text));
        }

        if bytes.is_empty() {
            return Err(SdkError::decode(
                &method_s,
                path,
                serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "empty body",
                )),
                String::new(),
            ));
        }

        let raw = String::from_utf8_lossy(&bytes).to_string();
        serde_json::from_slice::<T>(&bytes).map_err(|e| SdkError::decode(&method_s, path, e, raw))
    }
}
