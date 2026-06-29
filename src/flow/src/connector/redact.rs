//! Egress redaction helpers for connectors (VF-51 §6.3).
//!
//! Any URL that may reach a log line or error message must pass through
//! [`mask_url_userinfo`] first, so embedded `user:pass@` credentials never land
//! in a scannable on-disk artifact.

/// Replace any `userinfo` (`user:pass@`) embedded in a URL authority with a
/// fixed mask, leaving scheme/host/port/path intact. Non-URL or userinfo-free
/// strings are returned unchanged.
///
/// `mqtt://alice:s3cr3t@broker:1883/x` -> `mqtt://***:***@broker:1883/x`
pub fn mask_url_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let authority_start = scheme_end + 3;
    // Authority runs until the first '/', '?' or '#'.
    let rest = &url[authority_start..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_len];

    let Some(at) = authority.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &authority[..at];
    let mask = if userinfo.contains(':') {
        "***:***"
    } else {
        "***"
    };
    let mut out = String::with_capacity(url.len());
    out.push_str(&url[..authority_start]);
    out.push_str(mask);
    out.push_str(&authority[at..]); // includes '@' and host:port
    out.push_str(&rest[authority_len..]); // path/query/fragment
    out
}

/// True if a URL embeds credentials in its userinfo (`user:pass@host`). Used to
/// reject misplaced secrets (e.g. MQTT `broker_url`) as invalid config.
pub fn url_has_userinfo(url: &str) -> bool {
    let Some(scheme_end) = url.find("://") else {
        return false;
    };
    let rest = &url[scheme_end + 3..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    rest[..authority_len].contains('@')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_user_and_password() {
        assert_eq!(
            mask_url_userinfo("mqtt://alice:s3cr3t@broker:1883/topic"),
            "mqtt://***:***@broker:1883/topic"
        );
    }

    #[test]
    fn masks_user_only() {
        assert_eq!(
            mask_url_userinfo("rtsp://alice@cam:554/stream"),
            "rtsp://***@cam:554/stream"
        );
    }

    #[test]
    fn leaves_credential_free_urls_unchanged() {
        for u in [
            "mqtt://broker:1883",
            "tcp://127.0.0.1:5000",
            "ipc:///tmp/sock",
            "https://example.com/path?x=1",
            "not a url",
        ] {
            assert_eq!(mask_url_userinfo(u), u);
        }
    }

    #[test]
    fn does_not_mask_at_in_path_or_query() {
        assert_eq!(
            mask_url_userinfo("https://host/path@thing"),
            "https://host/path@thing"
        );
        assert_eq!(
            mask_url_userinfo("https://host/?q=a@b"),
            "https://host/?q=a@b"
        );
    }

    #[test]
    fn masked_output_drops_secret() {
        let masked = mask_url_userinfo("mqtt://u:p4ssw0rd@h:1");
        assert!(!masked.contains("p4ssw0rd"));
    }

    #[test]
    fn detects_userinfo() {
        assert!(url_has_userinfo("mqtt://u:p@h:1"));
        assert!(url_has_userinfo("rtsp://u@h"));
        assert!(!url_has_userinfo("mqtt://h:1"));
        assert!(!url_has_userinfo("https://h/path@x"));
    }
}
