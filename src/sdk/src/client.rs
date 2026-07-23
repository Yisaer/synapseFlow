use http::{Method, StatusCode};

use crate::config::ClientConfig;
use crate::error::SdkError;
use crate::transport::Transport;

use crate::types::{
    PipelineCreateRequest, PipelineUpsertRequest, StopOptions, StreamCreateRequest,
};

#[derive(Clone)]
pub struct ManagerClient {
    t: Transport,
}

impl ManagerClient {
    pub fn new(cfg: ClientConfig) -> Result<Self, SdkError> {
        Ok(Self {
            t: Transport::new(cfg)?,
        })
    }

    pub async fn create_stream(
        &self,
        req: &StreamCreateRequest,
    ) -> Result<serde_json::Value, SdkError> {
        self.t
            .expect_json_success(
                Method::POST,
                "/streams",
                &[],
                Some(req),
                &[StatusCode::CREATED],
            )
            .await
    }

    pub async fn list_streams(&self) -> Result<Vec<serde_json::Value>, SdkError> {
        self.t
            .expect_json_success::<(), Vec<serde_json::Value>>(
                Method::GET,
                "/streams",
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn describe_stream(&self, name: &str) -> Result<serde_json::Value, SdkError> {
        let path = format!("/streams/describe/{}", urlencoding::encode(name));
        self.t
            .expect_json_success::<(), serde_json::Value>(
                Method::GET,
                &path,
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn delete_stream(&self, name: &str) -> Result<(), SdkError> {
        let path = format!("/streams/{}", urlencoding::encode(name));
        self.t
            .expect_empty_success(
                Method::DELETE,
                &path,
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn shared_stream_stats(&self, name: &str) -> Result<serde_json::Value, SdkError> {
        self.shared_stream_stats_in_instance(name, None).await
    }

    pub async fn shared_stream_stats_in_instance(
        &self,
        name: &str,
        flow_instance_id: Option<&str>,
    ) -> Result<serde_json::Value, SdkError> {
        let path = format!("/streams/{}/shared/stats", urlencoding::encode(name));
        let query = flow_instance_id
            .map(|id| vec![("flow_instance_id", id.to_string())])
            .unwrap_or_default();
        self.t
            .expect_json_success::<(), serde_json::Value>(
                Method::GET,
                &path,
                &query,
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn create_pipeline(
        &self,
        req: &PipelineCreateRequest,
    ) -> Result<serde_json::Value, SdkError> {
        self.t
            .expect_json_success(
                Method::POST,
                "/pipelines",
                &[],
                Some(req),
                &[StatusCode::CREATED],
            )
            .await
    }

    pub async fn upsert_pipeline(
        &self,
        id: &str,
        req: &PipelineUpsertRequest,
    ) -> Result<serde_json::Value, SdkError> {
        let path = format!("/pipelines/{}", urlencoding::encode(id));
        self.t
            .expect_json_success(Method::PUT, &path, &[], Some(req), &[StatusCode::OK])
            .await
    }

    pub async fn list_pipelines(&self) -> Result<Vec<serde_json::Value>, SdkError> {
        self.t
            .expect_json_success::<(), Vec<serde_json::Value>>(
                Method::GET,
                "/pipelines",
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn get_pipeline(&self, id: &str) -> Result<serde_json::Value, SdkError> {
        let path = format!("/pipelines/{}", urlencoding::encode(id));
        self.t
            .expect_json_success::<(), serde_json::Value>(
                Method::GET,
                &path,
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn start_pipeline(&self, id: &str) -> Result<(), SdkError> {
        let path = format!("/pipelines/{}/start", urlencoding::encode(id));
        self.t
            .expect_empty_success(
                Method::POST,
                &path,
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn stop_pipeline(&self, id: &str, opt: StopOptions) -> Result<(), SdkError> {
        let path = format!("/pipelines/{}/stop", urlencoding::encode(id));
        let q = opt.to_query_pairs();
        self.t
            .expect_empty_success(
                Method::POST,
                &path,
                &q,
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub async fn delete_pipeline(&self, id: &str) -> Result<(), SdkError> {
        let path = format!("/pipelines/{}", urlencoding::encode(id));
        self.t
            .expect_empty_success(
                Method::DELETE,
                &path,
                &[],
                Option::<&()>::None,
                &[StatusCode::OK],
            )
            .await
    }

    pub fn base_url(&self) -> &url::Url {
        &self.t.config().base_url
    }
}
