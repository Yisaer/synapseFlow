#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![deny(unsafe_code)]

// Main library entry point
// The actual functionality is provided by individual crates:
// - parser: SQL parsing functionality
// - flow: Expression evaluation and SQL conversion
// - datatypes: Data type definitions

#[cfg(all(feature = "allocator-jemalloc", feature = "allocator-system"))]
compile_error!("features `allocator-jemalloc` and `allocator-system` are mutually exclusive");

pub mod bootstrap;
pub mod cgroup;
pub mod config;
pub mod distro;
pub mod embedded;
pub mod logging;
pub mod secrets_cli;
pub mod server;
pub mod startup;
pub use manager::{register_schema, schema_registry, SchemaParser};
