pub mod compress;
pub mod decoder;
pub mod encoder;
pub mod encrypt;
pub mod merger;
pub mod registry;
pub mod video;

pub use compress::{CompressError, CompressWriter, CompressionCodec};
pub use decoder::{
    proto::ProtobufDecoder,
    proto_bundle::{ProtoDescriptorBundle, ProtoFieldInfo},
    CodecError, JsonDecoder, RecordDecoder,
};
pub use encoder::{
    CsvEncoder, EncodeError, JsonEncoder, ProtobufEncoder, SinkEncoder, SinkEncoderFactory,
};
pub use encrypt::{
    AesGcmStreamWriter, EncryptError, EncryptWriter, EncryptionAlgorithm, InlineEncryptionKey,
    SecretBytes, SecretEncoding, SinkEncryptionConfig,
};
pub use merger::{Merger, MergerOutputKind};
pub use registry::{DecoderRegistry, EncoderRegistry, MergerRegistry};
pub use video::{
    default_video_schema, raw_frame_from_tuple, raw_frame_to_record_batch,
    raw_frames_from_collection, timestamp_from_system_time, VideoCodecError, VideoRawFrame,
    VIDEO_FORMAT_COLUMN, VIDEO_HEIGHT_COLUMN, VIDEO_PAYLOAD_COLUMN, VIDEO_TIMESTAMP_COLUMN,
    VIDEO_WIDTH_COLUMN,
};
