#[path = "pipeline/common.rs"]
mod common;

#[path = "pipeline/pipeline_tests.rs"]
mod pipeline_tests;

#[path = "pipeline/stateful_function_tests.rs"]
mod stateful_function_tests;

#[path = "pipeline/batching_tests.rs"]
mod batching_tests;

#[path = "pipeline/sampler_tests.rs"]
mod sampler_tests;

#[path = "pipeline/stats_tests.rs"]
mod stats_tests;

#[path = "pipeline/eventtime_tests.rs"]
mod eventtime_tests;

#[path = "pipeline/shared_stream_tests.rs"]
mod shared_stream_tests;

#[path = "pipeline/video_smoke_tests.rs"]
mod video_smoke_tests;

#[path = "pipeline/sink_compress_tests.rs"]
mod sink_compress_tests;

#[path = "pipeline/sink_encrypt_tests.rs"]
mod sink_encrypt_tests;
