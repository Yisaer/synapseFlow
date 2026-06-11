//! Pipeline module.
//!
//! This module intentionally separates the public API surface (types you should construct/call)
//! from runtime implementation details.
//!
//! **Public API**
//! - Definition types: `PipelineDefinition`, `SinkDefinition`, `PipelineOptions`
//! - Runtime lifecycle is managed via `FlowInstance` (create/start/stop/delete/list/explain)
//!
//! **Internal**
//! - Runtime build plumbing lives in `internal` and is not part of the public API.

mod api;
mod context;
mod internal;

pub(crate) use api::PipelineManager;
pub use api::{
    is_hls_video_url, is_rtsp_video_url, validate_video_filename_prefix, CreatePipelineRequest,
    CreatePipelineResult, EventtimeOptions, ExplainPipelineTarget, FileRetentionConfig,
    FileSinkProps, KuksaSinkProps, KuraSinkProps, MemorySinkProps, MqttSinkProps,
    NngPubSubSinkProps, NopSinkProps, PipelineDefinition, PipelineError, PipelineOptions,
    PipelineScheduleConfig, PipelineSnapshot, PipelineStatus, PipelineStopMode, SinkDefinition,
    SinkProps, SinkType, SourceDefinition, SourceInputConfig, SourceInputMode,
    SourceOnChangeConfig, VideoCodec, VideoContainer, VideoRollingConfig, VideoSinkProps,
};
pub(crate) use context::PipelineContext;

#[cfg(test)]
mod tests;
