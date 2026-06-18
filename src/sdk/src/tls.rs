use std::sync::Once;

static INSTALL_PROVIDER: Once = Once::new();

pub fn install_default_crypto_provider() {
    INSTALL_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
