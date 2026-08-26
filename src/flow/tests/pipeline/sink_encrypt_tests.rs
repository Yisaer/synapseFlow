use aead::stream::{DecryptorBE32, Nonce, StreamBE32};
use aead::{KeyInit, Payload};
use aes::Aes192;
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm};
use base64::Engine;
use datatypes::{ColumnSchema, ConcreteDatatype, Int64Type, Schema};
use flow::catalog::{MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::codec::{CompressionCodec, InlineEncryptionKey, SecretEncoding, SinkEncryptionConfig};
use flow::connector::{MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::pipeline::{FileSinkProps, PipelineDefinition};
use flow::planner::sink::CommonSinkProps;
use flow::{
    CreatePipelineRequest, FlowInstance, PipelineStopMode, SinkDefinition, SinkProps, SinkType,
};
use hkdf::Hkdf;
use serde_json::json;
use sha2::Sha256;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const ENCRYPT_TEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAGIC: &[u8; 4] = b"VFE1";
const VERSION: u8 = 1;
const ALGORITHM_AES_GCM_STREAM_BE32: u8 = 1;
const SALT_LEN: usize = 16;
const STREAM_NONCE: [u8; 7] = [0; 7];
const HKDF_INFO: &[u8] = b"veloflux:sink-encrypt:aes-gcm-stream:v1";

type Aes192Gcm = AesGcm<Aes192, aead::consts::U12>;

fn test_instance() -> FlowInstance {
    FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        "default", None,
    ))
    .expect("create flow instance")
}

fn json_schema(source_name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![ColumnSchema::new(
        source_name.to_string(),
        "v".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]))
}

async fn wait_for_files(dir: &std::path::Path, count: usize) -> Vec<std::path::PathBuf> {
    timeout(ENCRYPT_TEST_TIMEOUT, async {
        loop {
            let mut files = fs::read_dir(dir)
                .expect("read output dir")
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_file())
                .collect::<Vec<_>>();
            files.sort();
            if files.len() >= count {
                break files;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("file output timeout")
}

fn aes_config_hex(key_id: &str, key: &[u8]) -> SinkEncryptionConfig {
    SinkEncryptionConfig::aes_gcm(
        key_id,
        InlineEncryptionKey::new(hex::encode(key), SecretEncoding::Hex),
    )
    .expect("encryption config")
}

fn aes_config_base64(key_id: &str, key: &[u8]) -> SinkEncryptionConfig {
    SinkEncryptionConfig::aes_gcm(
        key_id,
        InlineEncryptionKey::new(
            base64::engine::general_purpose::STANDARD.encode(key),
            SecretEncoding::Base64,
        ),
    )
    .expect("encryption config")
}

struct ParsedHeader {
    header_len: usize,
    key_bits: u16,
    key_id: String,
    salt: [u8; SALT_LEN],
}

fn parse_header(data: &[u8]) -> ParsedHeader {
    assert_eq!(&data[0..4], MAGIC);
    assert_eq!(data[4], VERSION);
    assert_eq!(data[5], ALGORITHM_AES_GCM_STREAM_BE32);
    let key_bits = u16::from_be_bytes([data[6], data[7]]);
    let key_id_len = u16::from_be_bytes([data[8], data[9]]) as usize;
    let key_id_start = 10;
    let key_id_end = key_id_start + key_id_len;
    let key_id = std::str::from_utf8(&data[key_id_start..key_id_end])
        .expect("key id utf8")
        .to_string();
    assert_eq!(data[key_id_end], SALT_LEN as u8);
    let salt_start = key_id_end + 1;
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&data[salt_start..salt_start + SALT_LEN]);
    ParsedHeader {
        header_len: salt_start + SALT_LEN,
        key_bits,
        key_id,
        salt,
    }
}

fn split_frames(data: &[u8], mut offset: usize) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while offset < data.len() {
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        frames.push(data[offset..offset + len].to_vec());
        offset += len;
    }
    frames
}

enum AesGcmDecryptor {
    Aes128(DecryptorBE32<Aes128Gcm>),
    Aes192(DecryptorBE32<Aes192Gcm>),
    Aes256(DecryptorBE32<Aes256Gcm>),
}

impl AesGcmDecryptor {
    fn new(key_bits: u16, key: &[u8]) -> Self {
        match key_bits {
            128 => {
                let cipher = Aes128Gcm::new_from_slice(key).expect("aes-128 key");
                let nonce = Nonce::<Aes128Gcm, StreamBE32<Aes128Gcm>>::from_slice(&STREAM_NONCE);
                Self::Aes128(DecryptorBE32::from_aead(cipher, nonce))
            }
            192 => {
                let cipher = Aes192Gcm::new_from_slice(key).expect("aes-192 key");
                let nonce = Nonce::<Aes192Gcm, StreamBE32<Aes192Gcm>>::from_slice(&STREAM_NONCE);
                Self::Aes192(DecryptorBE32::from_aead(cipher, nonce))
            }
            256 => {
                let cipher = Aes256Gcm::new_from_slice(key).expect("aes-256 key");
                let nonce = Nonce::<Aes256Gcm, StreamBE32<Aes256Gcm>>::from_slice(&STREAM_NONCE);
                Self::Aes256(DecryptorBE32::from_aead(cipher, nonce))
            }
            other => panic!("unexpected key_bits {other}"),
        }
    }

    fn decrypt_next(&mut self, input: &[u8], aad: &[u8]) -> Result<Vec<u8>, aead::Error> {
        match self {
            Self::Aes128(decryptor) => decryptor.decrypt_next(Payload { msg: input, aad }),
            Self::Aes192(decryptor) => decryptor.decrypt_next(Payload { msg: input, aad }),
            Self::Aes256(decryptor) => decryptor.decrypt_next(Payload { msg: input, aad }),
        }
    }

    fn decrypt_last(self, input: &[u8], aad: &[u8]) -> Result<Vec<u8>, aead::Error> {
        match self {
            Self::Aes128(decryptor) => decryptor.decrypt_last(Payload { msg: input, aad }),
            Self::Aes192(decryptor) => decryptor.decrypt_last(Payload { msg: input, aad }),
            Self::Aes256(decryptor) => decryptor.decrypt_last(Payload { msg: input, aad }),
        }
    }
}

fn decrypt_delivery(data: &[u8], master_key: &[u8]) -> (ParsedHeader, Vec<u8>) {
    let parsed = parse_header(data);
    let hk = Hkdf::<Sha256>::new(Some(&parsed.salt), master_key);
    let mut stream_key = vec![0u8; master_key.len()];
    hk.expand(HKDF_INFO, &mut stream_key).expect("derive key");
    let mut frames = split_frames(data, parsed.header_len);
    let last = frames.pop().expect("last frame");
    let header = &data[..parsed.header_len];
    let mut decryptor = AesGcmDecryptor::new(parsed.key_bits, &stream_key);
    let mut out = Vec::new();
    for frame in &frames {
        out.extend_from_slice(
            &decryptor
                .decrypt_next(frame, header)
                .expect("decrypt next frame"),
        );
    }
    out.extend_from_slice(
        &decryptor
            .decrypt_last(&last, header)
            .expect("decrypt last frame"),
    );
    (parsed, out)
}

async fn run_file_pipeline(
    encryption: SinkEncryptionConfig,
    compression: Option<CompressionCodec>,
    suffix: &str,
    values: &[i64],
    batch_count: Option<usize>,
) -> (
    Vec<std::path::PathBuf>,
    tempfile::TempDir,
    FlowInstance,
    String,
) {
    let instance = test_instance();
    let source_name = format!("mem_{}", uuid::Uuid::new_v4().as_simple());
    let input_topic = format!("tests.encrypt.{}", uuid::Uuid::new_v4());

    instance
        .declare_memory_topic(
            &input_topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare input topic");
    let stream = StreamDefinition::new(
        source_name.clone(),
        json_schema(&source_name),
        StreamProps::Memory(MemoryStreamProps::new(input_topic.clone())),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create stream");

    let output_dir = tempfile::tempdir().expect("output tempdir");
    let pipeline_id = format!("encrypt_{}", uuid::Uuid::new_v4().as_simple());

    let mut sink = SinkDefinition::new(
        "file_sink",
        SinkType::File,
        SinkProps::File(FileSinkProps::new(
            output_dir.path().to_string_lossy(),
            format!("enc_{{write_start_ms}}_{{seq}}{suffix}"),
        )),
    )
    .with_encryption(encryption);
    if let Some(compression) = compression {
        sink = sink.with_compression(compression);
    }
    if let Some(batch_count) = batch_count {
        sink = sink.with_common_props(CommonSinkProps {
            batch_count: Some(batch_count),
            batch_duration: None,
        });
    }

    let pipeline = PipelineDefinition::new(
        pipeline_id.clone(),
        format!("SELECT v FROM {source_name}"),
        vec![sink],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create pipeline");
    instance
        .start_pipeline(&pipeline_id)
        .await
        .expect("start pipeline");

    instance
        .wait_for_memory_subscribers(
            &input_topic,
            MemoryTopicKind::Bytes,
            1,
            ENCRYPT_TEST_TIMEOUT,
        )
        .await
        .expect("wait for memory source");
    let publisher = instance
        .open_memory_publisher_bytes(&input_topic)
        .expect("open memory publisher");
    for value in values {
        publisher
            .publish_bytes(bytes::Bytes::from(format!(r#"{{"v":{value}}}"#)))
            .expect("publish bytes");
    }
    let expected_files = batch_count
        .map(|count| values.len().div_ceil(count))
        .unwrap_or(1);
    let files = wait_for_files(output_dir.path(), expected_files).await;
    (files, output_dir, instance, pipeline_id)
}

// coverage-covers: sink.encrypt.aes_gcm_delivery, sink.connector.file_output
#[tokio::test]
async fn memory_source_json_encrypt_file_sink_roundtrips() {
    let key = [11u8; 32];
    let (files, _dir, instance, pipeline_id) = run_file_pipeline(
        aes_config_hex("sink-aes-v1", &key),
        None,
        ".json.vfe",
        &[42],
        None,
    )
    .await;

    let encrypted = fs::read(&files[0]).expect("read encrypted file");
    assert!(!encrypted
        .windows(br#""v":42"#.len())
        .any(|w| w == br#""v":42"#));
    let (header, plaintext) = decrypt_delivery(&encrypted, &key);
    assert_eq!(header.key_bits, 256);
    assert_eq!(header.key_id, "sink-aes-v1");
    let json: serde_json::Value = serde_json::from_slice(&plaintext).expect("json parse");
    assert_eq!(json, json!([{"v": 42}]));

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, ENCRYPT_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
}

// coverage-covers: sink.compress.gzip_delivery, sink.encrypt.aes_gcm_delivery, sink.connector.file_output
#[tokio::test]
async fn memory_source_json_gzip_encrypt_file_sink_roundtrips() {
    let key = [22u8; 32];
    let (files, _dir, instance, pipeline_id) = run_file_pipeline(
        aes_config_base64("sink-aes-v1", &key),
        Some(CompressionCodec::gzip()),
        ".json.gz.vfe",
        &[99],
        None,
    )
    .await;

    let encrypted = fs::read(&files[0]).expect("read encrypted file");
    assert!(!encrypted
        .windows(br#""v":99"#.len())
        .any(|w| w == br#""v":99"#));
    let (_header, compressed) = decrypt_delivery(&encrypted, &key);
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(compressed.as_slice());
    let mut plaintext = Vec::new();
    decoder
        .read_to_end(&mut plaintext)
        .expect("gzip decompress");
    let json: serde_json::Value = serde_json::from_slice(&plaintext).expect("json parse");
    assert_eq!(json, json!([{"v": 99}]));

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, ENCRYPT_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
}

// coverage-covers: sink.encrypt.aes_gcm_delivery, sink.connector.file_output
#[tokio::test]
async fn encryption_preserves_batch_delivery_count() {
    let key = [33u8; 16];
    let (files, _dir, instance, pipeline_id) = run_file_pipeline(
        aes_config_hex("sink-aes-128-v1", &key),
        None,
        ".json.vfe",
        &[1, 2, 3, 4],
        Some(2),
    )
    .await;

    assert_eq!(files.len(), 2);
    for file in &files {
        let encrypted = fs::read(file).expect("read encrypted file");
        let (header, plaintext) = decrypt_delivery(&encrypted, &key);
        assert_eq!(header.key_bits, 128);
        let json: serde_json::Value = serde_json::from_slice(&plaintext).expect("json parse");
        assert_eq!(json.as_array().expect("array").len(), 2);
    }

    instance
        .stop_pipeline(&pipeline_id, PipelineStopMode::Quick, ENCRYPT_TEST_TIMEOUT)
        .await
        .expect("stop pipeline");
}
