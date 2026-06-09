#![cfg(feature = "video_gstreamer")]

use flow::catalog::{
    StreamDecoderConfig, StreamDefinition, StreamProps, VideoReconnectConfig, VideoRtspTransport,
    VideoStreamProps,
};
use flow::connector::sink::video::{
    VideoCodecConfig, VideoContainerConfig, VideoFileSinkConfig, VideoRollingConfig as SinkRolling,
    VideoSinkConfig, VideoSinkConnector, VideoSinkTargetConfig,
};
use flow::connector::sink::SinkConnector;
use flow::connector::source::video::{
    VideoReconnectRuntimeConfig, VideoRtspTransportConfig, VideoSourceConfig, VideoSourceConnector,
};
use flow::connector::{ConnectorEvent, SourceConnector};
use flow::pipeline::{
    PipelineDefinition, VideoCodec, VideoContainer, VideoRollingConfig, VideoSinkProps,
};
use flow::{
    CreatePipelineRequest, FlowInstance, PipelineStopMode, SinkDefinition, SinkEncoderConfig,
    SinkProps, SinkType,
};
use serde_json::Map as JsonMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{timeout, Duration};

static VIDEO_GSTREAMER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const RTSP_URL: &str = "rtsp://127.0.0.1:8554/camera_1";
const SOURCE_SIZE: &str = "320x240";
const SOURCE_RATE: &str = "10";
const RUN_SECONDS: u64 = 6;
const ROLLING_SECONDS: u64 = 2;

struct ChildProcessGuard {
    child: Child,
}

impl ChildProcessGuard {
    fn spawn(mut command: Command) -> Self {
        Self {
            child: command.spawn().expect("start child process"),
        }
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn video_output_dir() -> PathBuf {
    std::env::var_os("VELOFLUX_VIDEO_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("veloflux-video-gstreamer-{}", std::process::id()))
        })
}

fn non_empty_mp4_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("mp4"))
        .filter(|path| path.metadata().map(|meta| meta.len() > 0).unwrap_or(false))
        .collect()
}

fn prepare_output_dir(name: &str) -> PathBuf {
    let output_dir = video_output_dir().join(name);
    let _ = fs::remove_dir_all(&output_dir);
    fs::create_dir_all(&output_dir).expect("create video output dir");
    output_dir
}

async fn start_rtsp_test_source(output_dir: &Path) -> (ChildProcessGuard, ChildProcessGuard) {
    let mediamtx_config = output_dir.join("mediamtx.yml");
    fs::write(&mediamtx_config, "paths:\n  all:\n").expect("write mediamtx config");

    let mut mediamtx_command = Command::new("mediamtx");
    mediamtx_command
        .arg(&mediamtx_config)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mediamtx = ChildProcessGuard::spawn(mediamtx_command);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut ffmpeg_command = Command::new("ffmpeg");
    ffmpeg_command
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-re",
            "-f",
            "lavfi",
            "-i",
            &format!("testsrc=size={SOURCE_SIZE}:rate={SOURCE_RATE}"),
            "-c:v",
            "libx264",
            "-tune",
            "zerolatency",
            "-g",
            SOURCE_RATE,
            "-pix_fmt",
            "yuv420p",
            "-f",
            "rtsp",
            RTSP_URL,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let ffmpeg = ChildProcessGuard::spawn(ffmpeg_command);
    tokio::time::sleep(Duration::from_secs(2)).await;
    (mediamtx, ffmpeg)
}

fn video_stream_definition() -> StreamDefinition {
    StreamDefinition::new(
        "camera_1",
        Arc::new(flow::codec::default_video_schema("camera_1")),
        StreamProps::Video(VideoStreamProps {
            url: RTSP_URL.to_string(),
            rtsp_transport: VideoRtspTransport::Tcp,
            reconnect: VideoReconnectConfig {
                enabled: true,
                initial_delay: Duration::from_millis(200),
                max_delay: Duration::from_secs(1),
            },
        }),
        StreamDecoderConfig::none(),
    )
}

fn video_sink_definition(output_dir: &Path) -> SinkDefinition {
    let sink_props = VideoSinkProps::new(
        output_dir.to_string_lossy().into_owned(),
        VideoRollingConfig::Duration {
            seconds: ROLLING_SECONDS,
        },
    )
    .with_filename_prefix("camera_1")
    .with_codec(VideoCodec::H264)
    .with_container(VideoContainer::Mp4);
    SinkDefinition::new("video_sink", SinkType::Video, SinkProps::Video(sink_props))
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))
}

fn video_sink_connector(output_dir: &Path) -> VideoSinkConnector {
    VideoSinkConnector::new(
        "video_file_sink",
        VideoSinkConfig {
            target: VideoSinkTargetConfig::File(VideoFileSinkConfig {
                path: output_dir.to_string_lossy().into_owned(),
                filename_prefix: "camera_1".to_string(),
            }),
            codec: VideoCodecConfig::H264,
            container: VideoContainerConfig::Mp4,
            rolling: SinkRolling::Duration {
                duration: Duration::from_secs(ROLLING_SECONDS),
            },
        },
    )
}

fn assert_non_empty_mp4_files(output_dir: &Path) -> Vec<PathBuf> {
    let files = non_empty_mp4_files(output_dir);
    assert!(
        !files.is_empty(),
        "expected video sink to create a non-empty mp4 under {}",
        output_dir.display()
    );
    println!("video_gstreamer_output_dir={}", output_dir.display());
    for file in &files {
        println!("video_gstreamer_output_file={}", file.display());
    }
    files
}

// coverage-covers: source.video.tuple_input, sink.connector.video_output
#[tokio::test]
async fn video_rtsp_smoke_records_through_flow_instance_pipeline() {
    let _guard = VIDEO_GSTREAMER_TEST_LOCK.lock().await;
    let output_dir = prepare_output_dir("flow_instance_pipeline");
    let (_mediamtx, _ffmpeg) = start_rtsp_test_source(&output_dir).await;

    let instance = FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "video_gstreamer",
        None,
    ))
    .expect("create flow instance");

    instance
        .create_stream(video_stream_definition(), false)
        .await
        .expect("create video stream");

    let pipeline_id = "video_gstreamer_rtsp_to_file";
    let pipeline = PipelineDefinition::new(
        pipeline_id,
        "SELECT * FROM camera_1",
        vec![video_sink_definition(&output_dir)],
    );

    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create video pipeline");
    instance
        .start_pipeline(pipeline_id)
        .await
        .expect("start video pipeline");

    tokio::time::sleep(Duration::from_secs(RUN_SECONDS)).await;

    tokio::time::timeout(
        Duration::from_secs(20),
        instance.stop_pipeline(
            pipeline_id,
            PipelineStopMode::Graceful,
            Duration::from_secs(10),
        ),
    )
    .await
    .expect("timeout stopping video pipeline")
    .expect("stop video pipeline");

    let files = assert_non_empty_mp4_files(&output_dir);
    assert!(
        files.len() >= 2,
        "expected at least two rolling MP4 segments under {}, wrote {}",
        output_dir.display(),
        files.len()
    );
}

// coverage-covers: source.video.tuple_input, sink.connector.video_output
#[tokio::test]
async fn video_rtsp_smoke_records_rolling_mp4_segments() {
    use futures::StreamExt;

    let _guard = VIDEO_GSTREAMER_TEST_LOCK.lock().await;
    let output_dir = prepare_output_dir("direct_connectors");
    let (_mediamtx, _ffmpeg) = start_rtsp_test_source(&output_dir).await;

    let mut source = VideoSourceConnector::new(
        "camera_1",
        VideoSourceConfig {
            stream_name: "camera_1".to_string(),
            url: RTSP_URL.to_string(),
            rtsp_transport: VideoRtspTransportConfig::Tcp,
            reconnect: VideoReconnectRuntimeConfig {
                enabled: true,
                initial_delay: Duration::from_millis(200),
                max_delay: Duration::from_secs(1),
            },
        },
    );
    let mut frames = source.subscribe().expect("subscribe video source");
    let mut sink = video_sink_connector(&output_dir);
    sink.ready().await.expect("video sink ready");

    let deadline = Instant::now() + Duration::from_secs(RUN_SECONDS);
    let mut frame_count = 0_u64;
    while Instant::now() < deadline {
        match timeout(Duration::from_secs(10), frames.next())
            .await
            .expect("wait for video source event")
            .expect("video source stream ended unexpectedly")
        {
            Ok(ConnectorEvent::Collection(collection)) => {
                sink.send_collection(collection.as_ref())
                    .await
                    .expect("write video payload");
                frame_count += 1;
            }
            Ok(other) => panic!("unexpected video source event: {other:?}"),
            Err(err) => panic!("video source error: {err}"),
        }
    }

    source.close().expect("close video source");
    sink.close().await.expect("close video sink");

    assert!(frame_count > 0, "smoke test did not record any frames");
    let files = assert_non_empty_mp4_files(&output_dir);
    assert!(
        files.len() >= 2,
        "expected at least two rolling MP4 segments under {}, wrote {}",
        output_dir.display(),
        files.len()
    );
}
