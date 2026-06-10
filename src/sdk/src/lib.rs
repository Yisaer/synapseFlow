#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

pub mod client;
pub mod config;
pub mod error;
pub mod transport;
pub mod types;

pub use client::ManagerClient;
pub use config::ClientConfig;
pub use error::SdkError;
pub use types::{
    PipelineCreateRequest, StopMode, StopOptions, StreamCreateRequest, StreamUpsertRequest,
};
