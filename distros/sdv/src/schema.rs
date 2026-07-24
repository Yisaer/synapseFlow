//! Schema definitions for CAN and GBF decoding. See `docs/schema.md` for details.

pub mod arxml;
pub mod busmirror;
pub mod dbc;
pub mod gbf;
pub mod name_pattern;

pub use arxml::register_arxml_schema;
pub use busmirror::register_busmirror_schema;
pub use dbc::register_dbc_schema;
pub use gbf::register_gbf_schema;
