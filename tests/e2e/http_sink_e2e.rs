use super::{bind_manager_listener_or_skip, default_flow_instances, http_client, random_suffix};
use axum::body::Bytes;
use axum::extract::Multipart;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ── Request recorder for the test HTTP server ──────────────────────

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct RequestRecorder {
    requests: Mutex<Vec<RecordedRequest>>,
}

impl RequestRecorder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
        })
    }

    fn record(&self, method: String, headers: HeaderMap, body: Bytes) {
        let header_map = headers
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        self.requests.lock().unwrap().push(RecordedRequest {
            method,
            headers: header_map,
            body: body.to_vec(),
        });
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn take_requests(&self) -> Vec<RecordedRequest> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

#[derive(Debug, Clone)]
struct RecordedMultipartRequest {
    request_content_type: String,
    file_field_name: String,
    file_name: String,
    file_content_type: String,
    file_body: Vec<u8>,
    fields: BTreeMap<String, String>,
}

struct MultipartRequestRecorder {
    requests: Mutex<Vec<RecordedMultipartRequest>>,
}

impl MultipartRequestRecorder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
        })
    }

    fn record(&self, request: RecordedMultipartRequest) {
        self.requests.lock().unwrap().push(request);
    }

    fn requests(&self) -> Vec<RecordedMultipartRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn take_requests(&self) -> Vec<RecordedMultipartRequest> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

// ── Test ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_sink_raw_and_multipart_json_post() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();
    let injector = instance.clone();

    let Some(manager_listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let manager_addr = manager_listener
        .local_addr()
        .expect("read manager listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(
            manager_listener,
            instance,
            storage,
            default_flow_instances(),
        )
        .await
        .expect("start manager server");
    });

    let http = http_client();
    let manager_base = format!("http://{manager_addr}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── 1. Start test HTTP server as the sink target ───────────────

    let recorder = RequestRecorder::new();
    let recorder_clone = recorder.clone();
    let multipart_recorder = MultipartRequestRecorder::new();
    let multipart_recorder_clone = multipart_recorder.clone();

    let app = Router::new()
        .route(
            "/raw",
            post(move |headers: HeaderMap, body: Bytes| {
                let recorder = recorder_clone.clone();
                async move {
                    recorder.record("POST".to_string(), headers, body);
                    StatusCode::OK
                }
            }),
        )
        .route(
            "/multipart",
            post(move |headers: HeaderMap, mut multipart: Multipart| {
                let recorder = multipart_recorder_clone.clone();
                async move {
                    let request_content_type = headers
                        .get("content-type")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let mut file_field_name = String::new();
                    let mut file_name = String::new();
                    let mut file_content_type = String::new();
                    let mut file_body = Vec::new();
                    let mut fields = BTreeMap::new();

                    while let Some(field) = multipart.next_field().await.unwrap() {
                        let name = field.name().unwrap().to_string();
                        if field.file_name().is_some() {
                            file_field_name = name;
                            file_name = field.file_name().unwrap().to_string();
                            file_content_type = field
                                .content_type()
                                .map(ToString::to_string)
                                .unwrap_or_default();
                            file_body = field.bytes().await.unwrap().to_vec();
                        } else {
                            fields.insert(name, field.text().await.unwrap());
                        }
                    }

                    recorder.record(RecordedMultipartRequest {
                        request_content_type,
                        file_field_name,
                        file_name,
                        file_content_type,
                        file_body,
                        fields,
                    });
                    StatusCode::OK
                }
            }),
        );

    let sink_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sink test listener");
    let sink_port = sink_listener.local_addr().expect("read sink port").port();

    tokio::spawn(async move {
        axum::serve(sink_listener, app)
            .await
            .expect("start sink test server");
    });

    // ── 2. Create mock stream (JSON decoder) ───────────────────────

    let stream_name = format!("e2e_http_sink_stream_{}", random_suffix());
    let create_stream_resp = http
        .post(format!("{manager_base}/streams"))
        .json(&serde_json::json!({
            "name": stream_name,
            "revision": 1,
            "type": "mock",
            "schema": {
                "type": "json",
                "props": {
                    "columns": [
                        { "name": "amount", "data_type": "int64" },
                        { "name": "status", "data_type": "string" }
                    ]
                }
            },
            "props": {},
            "shared": true,
            "decoder": { "type": "json", "props": {} }
        }))
        .send()
        .await
        .expect("create stream request");
    assert_eq!(
        create_stream_resp.status(),
        StatusCode::CREATED,
        "create mock stream should return 201: {}",
        create_stream_resp.text().await.unwrap_or_default()
    );

    // ── 3. Create pipeline with HTTP sink ──────────────────────────

    let pipeline_id = format!("e2e_http_sink_pipe_{}", random_suffix());
    let create_pipeline_resp = http
        .post(format!("{manager_base}/pipelines"))
        .json(&serde_json::json!({
            "id": pipeline_id,
            "revision": 1,
            "sql": format!("SELECT * FROM {stream_name}"),
            "sinks": [
                {
                    "id": "raw",
                    "type": "http",
                    "props": {
                        "url": format!("http://127.0.0.1:{sink_port}/raw"),
                        "content_type": "application/json"
                    },
                    "encoder": {
                        "type": "json",
                        "props": {}
                    }
                },
                {
                    "id": "multipart",
                    "type": "http",
                    "props": {
                        "url": format!("http://127.0.0.1:{sink_port}/multipart"),
                        "body": {
                            "type": "multipart",
                            "file_field_name": " d ",
                            "fields": {
                                " rid ": "cold",
                                "tp": "1"
                            }
                        }
                    },
                    "encoder": {
                        "type": "json",
                        "props": {}
                    }
                }
            ]
        }))
        .send()
        .await
        .expect("create pipeline request");
    let create_status = create_pipeline_resp.status();
    let create_body = create_pipeline_resp.text().await.unwrap_or_default();
    assert_eq!(
        create_status,
        StatusCode::CREATED,
        "create pipeline with http sink should return 201, got {create_status}: {create_body}"
    );

    let start_resp = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/start"))
        .send()
        .await
        .expect("start pipeline request");
    let start_status = start_resp.status();
    let start_body = start_resp.text().await.unwrap_or_default();
    assert_eq!(
        start_status,
        StatusCode::OK,
        "start pipeline should return 200, got {start_status}: {start_body}"
    );

    // ── 4. Inject JSON data ────────────────────────────────────────

    const MAX_INJECT_ATTEMPTS: usize = 10;
    const INJECT_RETRY_DELAY: Duration = Duration::from_secs(3);
    for _attempt in 1..=MAX_INJECT_ATTEMPTS {
        injector
            .send_shared_mock_stream_payload(
                &stream_name,
                serde_json::to_vec(&serde_json::json!({
                    "amount": 42,
                    "status": "ok"
                }))
                .unwrap(),
            )
            .await
            .expect("inject payload into mock stream");

        tokio::time::sleep(INJECT_RETRY_DELAY).await;

        if !recorder.requests().is_empty() && !multipart_recorder.requests().is_empty() {
            break;
        }
    }

    // ── 5. Verify the HTTP sink delivered the encoded data ─────────

    let recorded = recorder.take_requests();
    assert!(
        !recorded.is_empty(),
        "timed out waiting for http sink to deliver data"
    );
    assert_eq!(recorded.len(), 1, "expected exactly 1 HTTP request");

    let req = &recorded[0];
    assert_eq!(req.method, "POST");

    let content_type = req
        .headers
        .get("content-type")
        .map(String::as_str)
        .unwrap_or("");
    assert!(
        content_type.contains("application/json"),
        "expected Content-Type application/json, got: {content_type}"
    );

    let body: serde_json::Value =
        serde_json::from_slice(&req.body).expect("http sink body should be valid JSON");
    assert!(
        body.is_array(),
        "http sink JSON body should be an array, got: {body}"
    );

    let expected = serde_json::json!([{"amount": 42, "status": "ok"}]);
    assert_eq!(body, expected, "http sink delivered unexpected body");

    let multipart_recorded = multipart_recorder.take_requests();
    assert_eq!(
        multipart_recorded.len(),
        1,
        "expected exactly 1 multipart HTTP request"
    );
    let multipart_req = &multipart_recorded[0];
    assert!(
        multipart_req
            .request_content_type
            .starts_with("multipart/form-data; boundary="),
        "unexpected multipart Content-Type: {}",
        multipart_req.request_content_type
    );
    assert_eq!(multipart_req.file_field_name, "d");
    assert_eq!(multipart_req.file_name, "payload.bin");
    assert_eq!(multipart_req.file_content_type, "application/octet-stream");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&multipart_req.file_body).unwrap(),
        expected
    );
    assert_eq!(
        multipart_req.fields,
        BTreeMap::from([
            ("rid".to_string(), "cold".to_string()),
            ("tp".to_string(), "1".to_string()),
        ])
    );

    // ── 6. Cleanup ─────────────────────────────────────────────────

    // Stop the manager server gracefully
    let _ = http
        .post(format!("{manager_base}/pipelines/{pipeline_id}/stop"))
        .send()
        .await;
    server.abort();
    let _ = server.await;
}
