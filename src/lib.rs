// Main library entry point
// The actual functionality is provided by individual crates:
// - parser: SQL parsing functionality
// - flow: Expression evaluation and SQL conversion
// - datatypes: Data type definitions

pub mod bootstrap;
pub mod config;
pub mod logging;
pub mod server;
pub use manager::{register_schema, schema_registry, SchemaParser};
