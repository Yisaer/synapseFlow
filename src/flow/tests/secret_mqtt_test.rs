//! End-to-end test for MQTT password resolution via the encrypted secret store
//! (VF-51). Demonstrates that the store key is the `NAME` in `password:
//! "store:NAME"` (user-chosen), resolved when the shared client is registered.

use std::sync::Arc;

use flow::connector::SharedMqttClientConfig;
use flow::instance::FlowInstanceOptions;
use flow::secret::{SecretContext, SecretPolicy, SecretRef, SecretStore};
use flow::FlowInstance;

fn instance_with_secret(name: &str, value: &str) -> FlowInstance {
    let instance = FlowInstance::new(FlowInstanceOptions::shared_current_runtime("default", None))
        .expect("create flow instance");
    // The store key is `name` — exactly what `store:NAME` references.
    let mut store = SecretStore::empty();
    store.set(name, value);
    instance.set_secret_context(SecretContext::new(Arc::new(store), SecretPolicy::Warn));
    instance
}

fn mqtt_config(key: &str, password: Option<SecretRef>) -> SharedMqttClientConfig {
    SharedMqttClientConfig {
        key: key.to_string(),
        broker_url: "tcp://broker.example.com:1883".to_string(),
        topic: "fleet/+/telemetry".to_string(),
        client_id: "client".to_string(),
        qos: 0,
        max_packet_size: None,
        username: Some("device".to_string()),
        password,
        resolved_password: None,
    }
}

#[tokio::test]
async fn shared_mqtt_password_resolves_from_named_store_key() {
    // Store key `broker-pass` holds the secret; config refers to `store:broker-pass`.
    let instance = instance_with_secret("broker-pass", "p4ssw0rd");
    let config = mqtt_config("shared", Some(SecretRef::store("broker-pass")));

    instance
        .create_shared_mqtt_client(config)
        .await
        .expect("registering with a resolvable store key succeeds");

    // The registry/persisted view keeps the pointer, never the resolved value.
    let listed = instance.list_shared_mqtt_clients();
    let stored = listed.iter().find(|c| c.key == "shared").expect("listed");
    assert_eq!(stored.password, Some(SecretRef::store("broker-pass")));
    assert!(stored.resolved_password.is_none());
    let json = serde_json::to_string(stored).expect("serialize");
    assert!(json.contains("store:broker-pass"), "{json}");
    assert!(
        !json.contains("p4ssw0rd"),
        "persisted form leaked secret: {json}"
    );
}

#[tokio::test]
async fn shared_mqtt_unknown_store_key_is_rejected() {
    let instance = instance_with_secret("broker-pass", "p4ssw0rd");
    // Referencing a name that is not in the store fails at registration.
    let config = mqtt_config("shared", Some(SecretRef::store("missing-key")));

    let err = instance
        .create_shared_mqtt_client(config)
        .await
        .expect_err("unknown store key must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("missing-key"), "{msg}");
}

#[tokio::test]
async fn shared_mqtt_without_password_is_allowed() {
    let instance = instance_with_secret("unused", "x");
    let config = mqtt_config("shared", None);
    instance
        .create_shared_mqtt_client(config)
        .await
        .expect("no password is fine");
}
