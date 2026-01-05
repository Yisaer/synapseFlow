//! Pipeline module.
//!
//! This module intentionally separates the public API surface (types you should construct/call)
//! from runtime implementation details.
//!
//! **Public API**
//! - Definition types: `PipelineDefinition`, `SinkDefinition`, `PipelineOptions`
//! - Runtime manager: `PipelineManager` (create/start/stop/delete/list/explain)
//!
//! **Internal**
//! - Plan-cache helpers and runtime build plumbing live in `internal` and are not part of the
//!   public API.

mod api;
mod internal;

pub use api::*;

#[cfg(test)]
mod tests;
