use url::Url;

pub const DEFAULT_TOPIC_DELIMITER: &str = ":";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NngTransport {
    Tcp,
    Ipc,
    Inproc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NngPubSubSourceConfig {
    pub source_name: String,
    pub url: String,
    pub topic: String,
    pub topic_delimiter: String,
}

impl NngPubSubSourceConfig {
    pub fn new(
        source_name: impl Into<String>,
        url: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            url: url.into(),
            topic: topic.into(),
            topic_delimiter: DEFAULT_TOPIC_DELIMITER.to_string(),
        }
    }

    pub fn with_topic_delimiter(mut self, topic_delimiter: impl Into<String>) -> Self {
        self.topic_delimiter = topic_delimiter.into();
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_nng_url(&self.url)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NngPubSubSinkConfig {
    pub sink_name: String,
    pub url: String,
    pub topic: String,
    pub topic_delimiter: String,
}

impl NngPubSubSinkConfig {
    pub fn new(
        sink_name: impl Into<String>,
        url: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            sink_name: sink_name.into(),
            url: url.into(),
            topic: topic.into(),
            topic_delimiter: DEFAULT_TOPIC_DELIMITER.to_string(),
        }
    }

    pub fn with_topic_delimiter(mut self, topic_delimiter: impl Into<String>) -> Self {
        self.topic_delimiter = topic_delimiter.into();
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_nng_url(&self.url)?;
        if self.topic.trim().is_empty() {
            return Err("topic is required".to_string());
        }
        Ok(())
    }
}

pub fn validate_nng_url(raw_url: &str) -> Result<(), String> {
    parse_nng_transport(raw_url).map(|_| ())
}

pub fn parse_nng_transport(raw_url: &str) -> Result<NngTransport, String> {
    if raw_url.trim().is_empty() {
        return Err("url is required".to_string());
    }
    let parsed = Url::parse(raw_url).map_err(|err| format!("invalid url `{raw_url}`: {err}"))?;
    match parsed.scheme() {
        "tcp" => Ok(NngTransport::Tcp),
        "ipc" => Ok(NngTransport::Ipc),
        "inproc" => Ok(NngTransport::Inproc),
        scheme => Err(format!(
            "unsupported nng url scheme `{scheme}`; supported schemes are tcp, ipc, and inproc"
        )),
    }
}

pub fn build_frame(topic: &str, delimiter: &str, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(topic.len() + delimiter.len() + payload.len());
    frame.extend_from_slice(topic.as_bytes());
    frame.extend_from_slice(delimiter.as_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn strip_topic_prefix<'a>(raw: &'a [u8], delimiter: &[u8], topic: &[u8]) -> Option<&'a [u8]> {
    let rest = if topic.is_empty() {
        raw
    } else {
        raw.strip_prefix(topic)?
    };

    if !delimiter.is_empty() {
        let index = find_subslice(rest, delimiter)?;
        return Some(&rest[index + delimiter.len()..]);
    }

    if topic.is_empty() {
        Some(raw)
    } else {
        Some(rest)
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    match needle {
        [] => Some(0),
        [byte] => haystack.iter().position(|candidate| candidate == byte),
        _ => haystack
            .windows(needle.len())
            .position(|window| window == needle),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_frame, parse_nng_transport, strip_topic_prefix, validate_nng_url, NngTransport,
    };

    #[test]
    fn strip_topic_prefix_handles_exact_topic_without_delimiter() {
        let payload =
            strip_topic_prefix(b"topic/can{\"v\":1}", b"", b"topic/can").expect("strip payload");
        assert_eq!(payload, br#"{"v":1}"#);
    }

    #[test]
    fn strip_topic_prefix_handles_delimiter_and_preserves_payload_delimiter() {
        let payload = strip_topic_prefix(b"topic/can:{\"v\":\"a:b\"}", b":", b"topic/")
            .expect("strip payload");
        assert_eq!(payload, br#"{"v":"a:b"}"#);
    }

    #[test]
    fn strip_topic_prefix_searches_delimiter_after_topic_prefix() {
        let payload =
            strip_topic_prefix(b"topic/can/{\"v\":1}", b"/", b"topic/can").expect("strip payload");
        assert_eq!(payload, br#"{"v":1}"#);
    }

    #[test]
    fn strip_topic_prefix_handles_subscribe_all_with_delimiter() {
        let payload =
            strip_topic_prefix(b"topic/can\0{\"v\":1}", b"\0", b"").expect("strip payload");
        assert_eq!(payload, br#"{"v":1}"#);
    }

    #[test]
    fn strip_topic_prefix_rejects_missing_delimiter() {
        assert!(strip_topic_prefix(b"topic/can{\"v\":1}", b"\0", b"topic/can").is_none());
    }

    #[test]
    fn strip_topic_prefix_rejects_mismatched_topic() {
        assert!(strip_topic_prefix(b"topic/other:{\"v\":1}", b":", b"topic/can").is_none());
    }

    #[test]
    fn build_frame_prepends_topic_and_delimiter() {
        assert_eq!(
            build_frame("topic/can", ":", br#"{"v":1}"#),
            b"topic/can:{\"v\":1}".to_vec()
        );
    }

    #[test]
    fn validate_nng_url_accepts_supported_schemes() {
        for url in [
            "tcp://127.0.0.1:5563",
            "ipc:///tmp/veloflux-nng.ipc",
            "inproc://veloflux-nng",
        ] {
            validate_nng_url(url).expect("valid nng url");
        }
    }

    #[test]
    fn parse_nng_transport_classifies_supported_schemes() {
        assert_eq!(
            parse_nng_transport("tcp://127.0.0.1:5563"),
            Ok(NngTransport::Tcp)
        );
        assert_eq!(
            parse_nng_transport("ipc:///tmp/veloflux-nng.ipc"),
            Ok(NngTransport::Ipc)
        );
        assert_eq!(
            parse_nng_transport("inproc://veloflux-nng"),
            Ok(NngTransport::Inproc)
        );
    }

    #[test]
    fn validate_nng_url_rejects_unsupported_schemes() {
        let err = validate_nng_url("file:///tmp/data").expect_err("reject file scheme");
        assert!(err.contains("unsupported nng url scheme"));
    }

    #[test]
    fn parse_nng_transport_rejects_unsupported_schemes() {
        let err = parse_nng_transport("file:///tmp/data").expect_err("reject file scheme");
        assert!(err.contains("unsupported nng url scheme"));
    }
}
