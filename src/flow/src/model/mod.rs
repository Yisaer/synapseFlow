pub mod collection;
pub mod record_batch;
#[cfg(debug_assertions)]
mod record_batch_debug;
pub mod record_batch_impl;
pub mod tuple;

pub use collection::{Collection, CollectionError, Column};
pub use record_batch::{
    batch_from_columns, batch_from_columns_simple, rows_from_columns, rows_from_columns_simple,
    RecordBatch,
};
pub use tuple::{AffiliateRow, Message, Tuple};
