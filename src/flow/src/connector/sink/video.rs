use crate::connector::sink::{SinkConnector, SinkConnectorError};
use crate::model::Collection;
use async_trait::async_trait;
use std::time::Duration;

#[cfg(feature = "video_gstreamer")]
use {
    crate::codec::raw_frames_from_collection, gstreamer as gst, gstreamer::prelude::*,
    gstreamer_app as gst_app, std::path::Path,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoSinkConfig {
    pub target: VideoSinkTargetConfig,
    pub codec: VideoCodecConfig,
    pub container: VideoContainerConfig,
    pub rolling: VideoRollingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VideoSinkTargetConfig {
    File(VideoFileSinkConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoFileSinkConfig {
    pub path: String,
    pub filename_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoCodecConfig {
    #[default]
    H264,
    H265,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoContainerConfig {
    #[default]
    Mp4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VideoRollingConfig {
    Duration { duration: Duration },
}

pub struct VideoSinkConnector {
    id: String,
    config: VideoSinkConfig,
    #[cfg(feature = "video_gstreamer")]
    runtime: Option<VideoSinkRuntime>,
}

impl VideoSinkConnector {
    pub fn new(id: impl Into<String>, config: VideoSinkConfig) -> Self {
        Self {
            id: id.into(),
            config,
            #[cfg(feature = "video_gstreamer")]
            runtime: None,
        }
    }

    #[cfg(feature = "video_gstreamer")]
    fn ensure_ready(&mut self) -> Result<(), SinkConnectorError> {
        if self.runtime.is_some() {
            return Ok(());
        }
        gst::init().map_err(|err| {
            SinkConnectorError::Other(format!("failed to initialize GStreamer: {err}"))
        })?;

        let VideoSinkTargetConfig::File(file) = &self.config.target;
        std::fs::create_dir_all(&file.path).map_err(|err| {
            SinkConnectorError::Other(format!(
                "failed to create video output directory `{}`: {err}",
                file.path
            ))
        })?;
        crate::pipeline::validate_video_filename_prefix(&file.filename_prefix).map_err(|err| {
            SinkConnectorError::Other(format!("invalid video filename_prefix: {err}"))
        })?;

        let location = Path::new(&escape_splitmux_location_part(&file.path))
            .join(format!(
                "{}-%05d.{}",
                escape_splitmux_location_part(&file.filename_prefix),
                self.config.container.file_extension()
            ))
            .to_string_lossy()
            .into_owned();
        let encoder = match self.config.codec {
            VideoCodecConfig::H264 => {
                "x264enc tune=zerolatency speed-preset=veryfast key-int-max=30 ! h264parse"
            }
            VideoCodecConfig::H265 => "x265enc tune=zerolatency speed-preset=veryfast ! h265parse",
        };
        let max_size_time = match &self.config.rolling {
            VideoRollingConfig::Duration { duration } => {
                u64::try_from(duration.as_nanos()).map_err(|_| {
                    SinkConnectorError::Other(format!(
                        "video rolling duration `{duration:?}` exceeds GStreamer u64 nanosecond limit"
                    ))
                })?
            }
        };
        let launch = format!(
            "appsrc name=vf_appsrc is-live=true format=time do-timestamp=true ! videoconvert ! {encoder} ! splitmuxsink name=vf_splitmux location={} max-size-time={max_size_time} muxer-factory={}",
            quote_launch_value(&location),
            self.config.container.muxer_factory()
        );
        let pipeline = gst::parse::launch(&launch)
            .map_err(|err| {
                SinkConnectorError::Other(format!(
                    "failed to create GStreamer video sink pipeline: {err}"
                ))
            })?
            .dynamic_cast::<gst::Pipeline>()
            .map_err(|_| {
                SinkConnectorError::Other(
                    "GStreamer video sink launch did not create a pipeline".to_string(),
                )
            })?;
        let appsrc = pipeline
            .by_name("vf_appsrc")
            .ok_or_else(|| {
                SinkConnectorError::Other(
                    "GStreamer video sink pipeline is missing appsrc".to_string(),
                )
            })?
            .dynamic_cast::<gst_app::AppSrc>()
            .map_err(|_| {
                SinkConnectorError::Other("GStreamer video sink is not an appsrc".to_string())
            })?;

        pipeline.set_state(gst::State::Playing).map_err(|err| {
            SinkConnectorError::Other(format!(
                "failed to start GStreamer video sink pipeline: {err:?}"
            ))
        })?;
        self.runtime = Some(VideoSinkRuntime {
            pipeline,
            appsrc,
            caps: None,
        });
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for VideoSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        #[cfg(feature = "video_gstreamer")]
        {
            return self.ensure_ready();
        }

        #[cfg(not(feature = "video_gstreamer"))]
        {
            let target = match &self.config.target {
                VideoSinkTargetConfig::File(file) => file.path.as_str(),
            };
            Err(SinkConnectorError::Other(format!(
                "GStreamer video sink backend is not available for `{target}` in this build"
            )))
        }
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        #[cfg(feature = "video_gstreamer")]
        {
            self.ensure_ready()?;
            let frames = raw_frames_from_collection(collection)
                .map_err(|err| SinkConnectorError::Other(err.to_string()))?;
            let runtime = self.runtime.as_mut().ok_or_else(|| {
                SinkConnectorError::Other("GStreamer video sink runtime is not ready".to_string())
            })?;
            for frame in frames {
                runtime.push_frame(frame)?;
            }
            return Ok(());
        }

        #[cfg(not(feature = "video_gstreamer"))]
        {
            let _ = collection;
            Err(SinkConnectorError::Other(
                "GStreamer video sink backend is not available in this build".to_string(),
            ))
        }
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        #[cfg(feature = "video_gstreamer")]
        if let Some(runtime) = self.runtime.take() {
            runtime.close()?;
        }
        Ok(())
    }
}

#[cfg(feature = "video_gstreamer")]
struct VideoSinkRuntime {
    pipeline: gst::Pipeline,
    appsrc: gst_app::AppSrc,
    caps: Option<VideoCaps>,
}

#[cfg(feature = "video_gstreamer")]
impl VideoSinkRuntime {
    fn push_frame(&mut self, frame: crate::codec::VideoRawFrame) -> Result<(), SinkConnectorError> {
        let width = i32::try_from(frame.width).map_err(|_| {
            SinkConnectorError::Other("video frame width is out of i32 range".to_string())
        })?;
        let height = i32::try_from(frame.height).map_err(|_| {
            SinkConnectorError::Other("video frame height is out of i32 range".to_string())
        })?;
        if let Some(current) = &self.caps {
            if current.width != width || current.height != height || current.format != frame.format
            {
                return Err(SinkConnectorError::Other(format!(
                    "video sink does not support changing raw frame caps from {}x{} {} to {}x{} {}",
                    current.width, current.height, current.format, width, height, frame.format
                )));
            }
        } else {
            let caps = gst::Caps::builder("video/x-raw")
                .field("format", frame.format.as_str())
                .field("width", width)
                .field("height", height)
                .field("framerate", gst::Fraction::new(30, 1))
                .build();
            self.appsrc.set_caps(Some(&caps));
            self.caps = Some(VideoCaps {
                width,
                height,
                format: frame.format.clone(),
            });
        }

        let buffer = gst::Buffer::from_slice(frame.payload);
        self.appsrc.push_buffer(buffer).map_err(|err| {
            SinkConnectorError::Other(format!("failed to push video frame to GStreamer: {err:?}"))
        })?;
        Ok(())
    }

    fn close(self) -> Result<(), SinkConnectorError> {
        self.appsrc.end_of_stream().map_err(|err| {
            SinkConnectorError::Other(format!(
                "failed to send video sink end-of-stream to GStreamer: {err:?}"
            ))
        })?;
        if let Some(bus) = self.pipeline.bus() {
            if let Some(message) = bus.timed_pop_filtered(
                gst::ClockTime::from_seconds(5),
                &[gst::MessageType::Eos, gst::MessageType::Error],
            ) {
                if let gst::MessageView::Error(err) = message.view() {
                    let _ = self.pipeline.set_state(gst::State::Null);
                    return Err(SinkConnectorError::Other(format!(
                        "GStreamer video sink error while finalizing segment: {}",
                        err.error()
                    )));
                }
            }
        }
        let _ = self.pipeline.set_state(gst::State::Null);
        Ok(())
    }
}

#[cfg(feature = "video_gstreamer")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct VideoCaps {
    width: i32,
    height: i32,
    format: String,
}

#[cfg(feature = "video_gstreamer")]
impl VideoContainerConfig {
    fn file_extension(self) -> &'static str {
        match self {
            VideoContainerConfig::Mp4 => "mp4",
        }
    }

    fn muxer_factory(self) -> &'static str {
        match self {
            VideoContainerConfig::Mp4 => "mp4mux",
        }
    }
}

#[cfg(feature = "video_gstreamer")]
fn escape_splitmux_location_part(part: &str) -> String {
    part.replace('%', "%%")
}

#[cfg(feature = "video_gstreamer")]
fn quote_launch_value(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}
