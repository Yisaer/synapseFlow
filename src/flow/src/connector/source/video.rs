use crate::connector::{ConnectorError, ConnectorStream, SourceConnector};
use crate::processor::base::normalize_channel_capacity;
use std::time::Duration;

#[cfg(feature = "video_gstreamer")]
use {
    crate::codec::{raw_frame_to_record_batch, timestamp_from_system_time, VideoRawFrame},
    crate::connector::ConnectorEvent,
    bytes::Bytes,
    gstreamer as gst,
    gstreamer::prelude::*,
    gstreamer_app as gst_app,
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    std::thread::JoinHandle,
    std::time::SystemTime,
    tokio::sync::mpsc,
    tokio::sync::mpsc::error::TrySendError,
    tokio_stream::wrappers::ReceiverStream,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoSourceConfig {
    pub stream_name: String,
    pub url: String,
    pub rtsp_transport: VideoRtspTransportConfig,
    pub reconnect: VideoReconnectRuntimeConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoRtspTransportConfig {
    #[default]
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoReconnectRuntimeConfig {
    pub enabled: bool,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

pub struct VideoSourceConnector {
    id: String,
    config: VideoSourceConfig,
    subscribed: bool,
    channel_capacity: usize,
    #[cfg(feature = "video_gstreamer")]
    shutdown: Option<Arc<VideoSourceShutdown>>,
    #[cfg(feature = "video_gstreamer")]
    worker: Option<JoinHandle<()>>,
}

impl VideoSourceConnector {
    pub fn new(id: impl Into<String>, config: VideoSourceConfig) -> Self {
        Self {
            id: id.into(),
            config,
            subscribed: false,
            channel_capacity: crate::processor::base::DEFAULT_DATA_CHANNEL_CAPACITY,
            #[cfg(feature = "video_gstreamer")]
            shutdown: None,
            #[cfg(feature = "video_gstreamer")]
            worker: None,
        }
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = normalize_channel_capacity(capacity);
        self
    }

    #[cfg(feature = "video_gstreamer")]
    fn subscribe_gstreamer(&mut self) -> Result<ConnectorStream, ConnectorError> {
        let config = self.config.clone();
        let reconnect = self.config.reconnect.clone();
        let connector_id = self.id.clone();
        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        let shutdown = Arc::new(VideoSourceShutdown::new());
        let thread_shutdown = Arc::clone(&shutdown);
        let worker = std::thread::Builder::new()
            .name(format!("vf-video-source-{connector_id}"))
            .spawn(move || {
                run_gstreamer_source(connector_id, config, reconnect, thread_shutdown, sender);
            })
            .map_err(|err| {
                ConnectorError::Other(format!("failed to spawn video source thread: {err}"))
            })?;
        self.shutdown = Some(shutdown);
        self.worker = Some(worker);
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
}

impl SourceConnector for VideoSourceConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn subscribe(&mut self) -> Result<ConnectorStream, ConnectorError> {
        if self.subscribed {
            return Err(ConnectorError::AlreadySubscribed(self.id.clone()));
        }
        self.subscribed = true;

        #[cfg(feature = "video_gstreamer")]
        {
            self.subscribe_gstreamer()
        }

        #[cfg(not(feature = "video_gstreamer"))]
        {
            let target = crate::connector::mask_url_userinfo(&self.config.url);
            let stream = futures::stream::once(async move {
                Err(ConnectorError::Connection(format!(
                    "GStreamer video source backend is not available for `{target}` in this build"
                )))
            });
            Ok(Box::pin(stream))
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "video_gstreamer")]
        {
            if let Some(shutdown) = self.shutdown.take() {
                shutdown.signal();
            }
            if let Some(worker) = self.worker.take() {
                worker.join().map_err(|_| {
                    ConnectorError::Other("video source worker thread panicked".to_string())
                })?;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "video_gstreamer")]
impl Drop for VideoSourceConnector {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(feature = "video_gstreamer")]
struct VideoSourceShutdown {
    stopped: AtomicBool,
    mutex: Mutex<()>,
    condvar: Condvar,
}

#[cfg(feature = "video_gstreamer")]
impl VideoSourceShutdown {
    fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    fn is_set(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    fn signal(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    fn wait(&self, duration: Duration) {
        if self.is_set() {
            return;
        }
        let guard = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let _ = self
            .condvar
            .wait_timeout_while(guard, duration, |_| !self.is_set());
    }
}

#[cfg(feature = "video_gstreamer")]
enum SendEventResult {
    Sent,
    Closed,
    Interrupted,
}

#[cfg(feature = "video_gstreamer")]
fn send_event(
    shutdown: &VideoSourceShutdown,
    sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
    mut event: Result<ConnectorEvent, ConnectorError>,
) -> SendEventResult {
    const SEND_RETRY_WAIT: Duration = Duration::from_millis(10);

    loop {
        if shutdown.is_set() {
            return SendEventResult::Interrupted;
        }

        match sender.try_send(event) {
            Ok(()) => return SendEventResult::Sent,
            Err(TrySendError::Full(returned)) => {
                event = returned;
                shutdown.wait(SEND_RETRY_WAIT);
            }
            Err(TrySendError::Closed(_)) => return SendEventResult::Closed,
        }
    }
}

#[cfg(feature = "video_gstreamer")]
fn run_gstreamer_source(
    connector_id: String,
    config: VideoSourceConfig,
    reconnect: VideoReconnectRuntimeConfig,
    shutdown: Arc<VideoSourceShutdown>,
    sender: mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
) {
    if let Err(err) = gst::init() {
        let _ = send_event(
            &shutdown,
            &sender,
            Err(ConnectorError::Other(format!(
                "failed to initialize GStreamer: {err}"
            ))),
        );
        return;
    }

    let mut delay = reconnect.initial_delay;
    loop {
        if shutdown.is_set() {
            let _ = send_event(&shutdown, &sender, Ok(ConnectorEvent::EndOfStream));
            return;
        }
        match run_one_session(&config, &shutdown, &sender) {
            Ok(()) => {
                let _ = send_event(&shutdown, &sender, Ok(ConnectorEvent::EndOfStream));
                return;
            }
            Err(err) if reconnect.enabled && !shutdown.is_set() => {
                tracing::warn!(
                    connector_id = %connector_id,
                    error = %err,
                    delay_ms = delay.as_millis(),
                    "video source reconnecting"
                );
                shutdown.wait(delay);
                delay = std::cmp::min(delay.saturating_mul(2), reconnect.max_delay);
            }
            Err(err) => {
                let _ = send_event(&shutdown, &sender, Err(ConnectorError::Connection(err)));
                return;
            }
        }
    }
}

#[cfg(feature = "video_gstreamer")]
fn run_one_session(
    config: &VideoSourceConfig,
    shutdown: &VideoSourceShutdown,
    sender: &mpsc::Sender<Result<ConnectorEvent, ConnectorError>>,
) -> Result<(), String> {
    let launch = source_launch(config)?;
    let pipeline = gst::parse::launch(&launch)
        .map_err(|err| format!("failed to create GStreamer source pipeline: {err}"))?
        .dynamic_cast::<gst::Pipeline>()
        .map_err(|_| "GStreamer source launch did not create a pipeline".to_string())?;
    let appsink = pipeline
        .by_name("vf_appsink")
        .ok_or_else(|| "GStreamer source pipeline is missing appsink".to_string())?
        .dynamic_cast::<gst_app::AppSink>()
        .map_err(|_| "GStreamer source sink is not an appsink".to_string())?;
    let bus = pipeline
        .bus()
        .ok_or_else(|| "GStreamer source pipeline has no bus".to_string())?;

    pipeline
        .set_state(gst::State::Playing)
        .map_err(|err| format!("failed to start GStreamer source pipeline: {err:?}"))?;

    let result = loop {
        if shutdown.is_set() {
            break Ok(());
        }
        if let Some(message) = bus.timed_pop_filtered(
            gst::ClockTime::ZERO,
            &[gst::MessageType::Error, gst::MessageType::Eos],
        ) {
            match message.view() {
                gst::MessageView::Error(err) => {
                    break Err(format!(
                        "GStreamer source error from {:?}: {}",
                        err.src().map(|src| src.path_string()),
                        err.error()
                    ));
                }
                gst::MessageView::Eos(_) => break Ok(()),
                _ => {}
            }
        }

        match appsink.try_pull_sample(gst::ClockTime::from_mseconds(200)) {
            Some(sample) => {
                let frame = sample_to_raw_frame(&sample)?;
                let collection = raw_frame_to_record_batch(config.stream_name.as_str(), frame)
                    .map_err(|err| err.to_string())?;
                match send_event(
                    shutdown,
                    sender,
                    Ok(ConnectorEvent::Collection(Box::new(collection))),
                ) {
                    SendEventResult::Sent => {}
                    SendEventResult::Closed | SendEventResult::Interrupted => break Ok(()),
                }
            }
            None => continue,
        }
    };

    let _ = pipeline.set_state(gst::State::Null);
    result
}

#[cfg(feature = "video_gstreamer")]
fn source_launch(config: &VideoSourceConfig) -> Result<String, String> {
    if crate::pipeline::is_rtsp_video_url(&config.url) {
        let protocol = match config.rtsp_transport {
            VideoRtspTransportConfig::Tcp => "tcp",
            VideoRtspTransportConfig::Udp => "udp",
        };
        return Ok(format!(
            "rtspsrc location={} protocols={protocol} latency=200 ! decodebin ! videoconvert ! video/x-raw,format=RGB ! appsink name=vf_appsink emit-signals=false sync=false max-buffers=4 drop=true",
            quote_launch_value(&config.url)
        ));
    }
    if crate::pipeline::is_hls_video_url(&config.url) {
        return Ok(format!(
            "uridecodebin uri={} ! videoconvert ! video/x-raw,format=RGB ! appsink name=vf_appsink emit-signals=false sync=false max-buffers=4 drop=true",
            quote_launch_value(&config.url)
        ));
    }
    Err(format!(
        "unsupported video URL `{}` (expected rtsp://, rtsps://, or http(s)://...m3u8)",
        crate::connector::mask_url_userinfo(&config.url)
    ))
}

#[cfg(feature = "video_gstreamer")]
fn sample_to_raw_frame(sample: &gst::Sample) -> Result<VideoRawFrame, String> {
    let caps = sample
        .caps()
        .ok_or_else(|| "GStreamer sample has no caps".to_string())?;
    let structure = caps
        .structure(0)
        .ok_or_else(|| "GStreamer sample caps have no structure".to_string())?;
    let width = i64::from(
        structure
            .get::<i32>("width")
            .map_err(|_| "GStreamer video frame caps are missing width".to_string())?,
    );
    let height = i64::from(
        structure
            .get::<i32>("height")
            .map_err(|_| "GStreamer video frame caps are missing height".to_string())?,
    );
    let format = structure
        .get::<String>("format")
        .map_err(|_| "GStreamer video frame caps are missing format".to_string())?;
    let payload = sample
        .buffer_owned()
        .ok_or_else(|| "GStreamer sample has no buffer".to_string())?
        .into_mapped_buffer_readable()
        .map_err(|_| "failed to map GStreamer video buffer".to_string())?;
    let now = SystemTime::now();
    VideoRawFrame::new(
        Bytes::from_owner(payload),
        width,
        height,
        format,
        timestamp_from_system_time(now),
    )
    .map_err(|err| err.to_string())
}

#[cfg(feature = "video_gstreamer")]
fn quote_launch_value(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}
