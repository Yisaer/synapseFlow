//! Pre-compiled protobuf test messages generated from `proto/test/simple.proto`.
//!
//! These types are used by decoder integration tests and e2e tests to construct
//! well-formed protobuf payloads without hand-coding wire-format bytes.

/// Module containing the prost-generated `Simple` message type.
pub mod simple {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

/// Encode a `Simple` message to protobuf wire-format bytes.
///
/// This is a thin wrapper around `prost::Message::encode` so that
/// downstream crates (e.g. e2e tests) can use it without depending on `prost`.
pub fn encode_simple(msg: &simple::Simple) -> Vec<u8> {
    let mut buf = Vec::new();
    prost::Message::encode(msg, &mut buf).expect("encode Simple");
    buf
}
