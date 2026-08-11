use serde::{Deserialize, Serialize};
use std::time::Duration;

use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::{Packet as V5Packet, PublishProperties};
use rumqttc::v5::mqttbytes::QoS as V5Qos;
use rumqttc::v5::{
    AsyncClient as V5Client, ConnectionError as V5ConnectionError, Event as V5Event,
    EventLoop as V5EventLoop, MqttOptions as V5Options,
};
use rumqttc::{
    AsyncClient as V3Client, ConnectionError as V3ConnectionError, Event as V3Event,
    EventLoop as V3EventLoop, MqttOptions as V3Options, Packet as V3Packet, QoS as V3Qos,
    Transport,
};

/// MQTT wire protocol used by one client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MqttProtocolVersion {
    /// MQTT 3.1.1.
    #[default]
    V3,
    /// MQTT 5.0.
    V5,
}

/// One MQTT 5 User Property entry.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttUserProperty {
    pub key: String,
    pub value: String,
}

impl std::fmt::Debug for MqttUserProperty {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MqttUserProperty")
            .field("key", &self.key)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// Validate ordered MQTT 5 User Property entries.
pub fn validate_mqtt_user_properties(properties: &[MqttUserProperty]) -> Result<(), String> {
    for (index, property) in properties.iter().enumerate() {
        validate_mqtt_utf8_string(&property.key).map_err(|rule| {
            format!("user_properties[{index}].key violates MQTT UTF-8 String rules: {rule}")
        })?;
        validate_mqtt_utf8_string(&property.value).map_err(|rule| {
            format!("user_properties[{index}].value violates MQTT UTF-8 String rules: {rule}")
        })?;
    }
    Ok(())
}

fn validate_mqtt_utf8_string(value: &str) -> Result<(), &'static str> {
    if value.len() > u16::MAX as usize {
        return Err("encoded length exceeds 65535 bytes");
    }

    if value.chars().any(is_forbidden_mqtt_character) {
        return Err("contains a prohibited control or noncharacter code point");
    }

    Ok(())
}

fn is_forbidden_mqtt_character(character: char) -> bool {
    let codepoint = character as u32;
    character == '\0'
        || codepoint <= 0x1f
        || (0x7f..=0x9f).contains(&codepoint)
        || (0xfdd0..=0xfdef).contains(&codepoint)
        || codepoint & 0xffff == 0xfffe
        || codepoint & 0xffff == 0xffff
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MqttQos {
    Qos0,
    Qos1,
    Qos2,
}

impl TryFrom<u8> for MqttQos {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Qos0),
            1 => Ok(Self::Qos1),
            2 => Ok(Self::Qos2),
            other => Err(format!("unsupported MQTT QoS level: {other}")),
        }
    }
}

impl From<V3Qos> for MqttQos {
    fn from(value: V3Qos) -> Self {
        match value {
            V3Qos::AtMostOnce => Self::Qos0,
            V3Qos::AtLeastOnce => Self::Qos1,
            V3Qos::ExactlyOnce => Self::Qos2,
        }
    }
}

impl MqttQos {
    fn v3(self) -> V3Qos {
        match self {
            Self::Qos0 => V3Qos::AtMostOnce,
            Self::Qos1 => V3Qos::AtLeastOnce,
            Self::Qos2 => V3Qos::ExactlyOnce,
        }
    }

    fn v5(self) -> V5Qos {
        match self {
            Self::Qos0 => V5Qos::AtMostOnce,
            Self::Qos1 => V5Qos::AtLeastOnce,
            Self::Qos2 => V5Qos::ExactlyOnce,
        }
    }
}

// Keep the v3 variant inline so existing MQTT 3.1.1 connection setup does not
// gain an allocation. The larger v5 variant is boxed independently.
#[allow(clippy::large_enum_variant)]
pub(crate) enum MqttOptions {
    V3(V3Options),
    V5(Box<V5Options>),
}

impl MqttOptions {
    pub(crate) fn new(
        protocol_version: MqttProtocolVersion,
        client_id: String,
        host: &str,
        port: u16,
        max_packet_size: usize,
    ) -> Result<Self, String> {
        match protocol_version {
            MqttProtocolVersion::V3 => {
                let mut options = V3Options::new(client_id, host, port);
                options.set_max_packet_size(max_packet_size, max_packet_size);
                Ok(Self::V3(options))
            }
            MqttProtocolVersion::V5 => {
                let max_packet_size = u32::try_from(max_packet_size).map_err(|_| {
                    format!(
                        "MQTT 5 max_packet_size {max_packet_size} exceeds {}",
                        u32::MAX
                    )
                })?;
                let mut options = V5Options::new(client_id, host, port);
                options.set_max_packet_size(Some(max_packet_size));
                Ok(Self::V5(Box::new(options)))
            }
        }
    }

    pub(crate) fn set_keep_alive(&mut self, duration: Duration) {
        match self {
            Self::V3(options) => {
                options.set_keep_alive(duration);
            }
            Self::V5(options) => {
                options.set_keep_alive(duration);
            }
        }
    }

    pub(crate) fn set_transport(&mut self, transport: Transport) {
        match self {
            Self::V3(options) => {
                options.set_transport(transport);
            }
            Self::V5(options) => {
                options.set_transport(transport);
            }
        }
    }

    pub(crate) fn set_credentials(&mut self, username: String, password: String) {
        match self {
            Self::V3(options) => {
                options.set_credentials(username, password);
            }
            Self::V5(options) => {
                options.set_credentials(username, password);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn broker_address(&self) -> (String, u16) {
        match self {
            Self::V3(options) => options.broker_address(),
            Self::V5(options) => options.broker_address(),
        }
    }

    #[cfg(test)]
    pub(crate) fn client_id(&self) -> String {
        match self {
            Self::V3(options) => options.client_id().to_string(),
            Self::V5(options) => options.client_id(),
        }
    }

    #[cfg(test)]
    pub(crate) fn max_packet_size(&self) -> usize {
        match self {
            Self::V3(options) => options.max_packet_size(),
            Self::V5(options) => options.max_packet_size().unwrap_or_default() as usize,
        }
    }

    #[cfg(test)]
    pub(crate) fn transport(&self) -> Transport {
        match self {
            Self::V3(options) => options.transport(),
            Self::V5(options) => options.transport(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum MqttClient {
    V3(V3Client),
    V5(V5Client),
}

// The event loop is long-lived. Keeping v3 inline preserves its allocation
// behavior while boxing only the larger v5 implementation.
#[allow(clippy::large_enum_variant)]
pub(crate) enum MqttEventLoop {
    V3(V3EventLoop),
    V5(Box<V5EventLoop>),
}

pub(crate) enum MqttEvent {
    Connected,
    Publish { topic: String, payload: Bytes },
    Disconnected,
    Other,
}

pub(crate) enum MqttConnectionError {
    RequestsDone,
    Connection(String),
}

impl MqttClient {
    pub(crate) fn new(options: MqttOptions, capacity: usize) -> (Self, MqttEventLoop) {
        match options {
            MqttOptions::V3(options) => {
                let (client, event_loop) = V3Client::new(options, capacity);
                (Self::V3(client), MqttEventLoop::V3(event_loop))
            }
            MqttOptions::V5(options) => {
                let (client, event_loop) = V5Client::new(*options, capacity);
                (Self::V5(client), MqttEventLoop::V5(Box::new(event_loop)))
            }
        }
    }

    pub(crate) async fn subscribe(&self, topic: String, qos: MqttQos) -> Result<(), String> {
        match self {
            Self::V3(client) => client
                .subscribe(topic, qos.v3())
                .await
                .map_err(|err| err.to_string()),
            Self::V5(client) => client
                .subscribe(topic, qos.v5())
                .await
                .map_err(|err| err.to_string()),
        }
    }

    pub(crate) async fn publish(
        &self,
        topic: String,
        qos: MqttQos,
        retain: bool,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        match self {
            Self::V3(client) => client
                .publish(topic, qos.v3(), retain, payload)
                .await
                .map_err(|err| err.to_string()),
            Self::V5(client) => client
                .publish(topic, qos.v5(), retain, payload)
                .await
                .map_err(|err| err.to_string()),
        }
    }

    pub(crate) async fn publish_with_user_properties(
        &self,
        topic: String,
        qos: MqttQos,
        retain: bool,
        payload: Vec<u8>,
        user_properties: &[MqttUserProperty],
    ) -> Result<(), String> {
        match self {
            Self::V3(_) => {
                Err("MQTT 5 User Properties cannot be published over MQTT 3.1.1".to_string())
            }
            Self::V5(client) => {
                let properties = PublishProperties {
                    user_properties: user_properties
                        .iter()
                        .map(|property| (property.key.clone(), property.value.clone()))
                        .collect(),
                    ..PublishProperties::default()
                };
                client
                    .publish_with_properties(topic, qos.v5(), retain, payload, properties)
                    .await
                    .map_err(|err| err.to_string())
            }
        }
    }

    pub(crate) async fn disconnect(&self) -> Result<(), String> {
        match self {
            Self::V3(client) => client.disconnect().await.map_err(|err| err.to_string()),
            Self::V5(client) => client.disconnect().await.map_err(|err| err.to_string()),
        }
    }
}

impl MqttEventLoop {
    pub(crate) async fn poll(&mut self) -> Result<MqttEvent, MqttConnectionError> {
        match self {
            Self::V3(event_loop) => match event_loop.poll().await {
                Ok(V3Event::Incoming(V3Packet::ConnAck(_))) => Ok(MqttEvent::Connected),
                Ok(V3Event::Incoming(V3Packet::Publish(publish))) => Ok(MqttEvent::Publish {
                    topic: publish.topic,
                    payload: publish.payload,
                }),
                Ok(V3Event::Incoming(V3Packet::Disconnect)) => Ok(MqttEvent::Disconnected),
                Ok(V3Event::Incoming(_)) | Ok(V3Event::Outgoing(_)) => Ok(MqttEvent::Other),
                Err(V3ConnectionError::RequestsDone) => Err(MqttConnectionError::RequestsDone),
                Err(err) => Err(MqttConnectionError::Connection(err.to_string())),
            },
            Self::V5(event_loop) => match event_loop.poll().await {
                Ok(V5Event::Incoming(V5Packet::ConnAck(_))) => Ok(MqttEvent::Connected),
                Ok(V5Event::Incoming(V5Packet::Publish(publish))) => {
                    let topic = String::from_utf8(publish.topic.to_vec()).map_err(|_| {
                        MqttConnectionError::Connection(
                            "MQTT 5 PUBLISH topic is not valid UTF-8".to_string(),
                        )
                    })?;
                    Ok(MqttEvent::Publish {
                        topic,
                        payload: publish.payload,
                    })
                }
                Ok(V5Event::Incoming(V5Packet::Disconnect(_))) => Ok(MqttEvent::Disconnected),
                Ok(V5Event::Incoming(_)) | Ok(V5Event::Outgoing(_)) => Ok(MqttEvent::Other),
                Err(V5ConnectionError::RequestsDone) => Err(MqttConnectionError::RequestsDone),
                Err(err) => Err(MqttConnectionError::Connection(err.to_string())),
            },
        }
    }

    pub(crate) fn clean(&mut self) {
        match self {
            Self::V3(event_loop) => event_loop.clean(),
            Self::V5(event_loop) => event_loop.clean(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_mqtt_user_properties, MqttOptions, MqttProtocolVersion, MqttUserProperty,
    };

    #[test]
    fn mqtt_protocol_version_defaults_to_v3_and_uses_stable_json_values() {
        assert_eq!(MqttProtocolVersion::default(), MqttProtocolVersion::V3);
        assert_eq!(
            serde_json::to_string(&MqttProtocolVersion::V3).expect("serialize MQTT v3"),
            "\"v3\""
        );
        assert_eq!(
            serde_json::from_str::<MqttProtocolVersion>("\"v5\"").expect("deserialize MQTT v5"),
            MqttProtocolVersion::V5
        );
        assert!(serde_json::from_str::<MqttProtocolVersion>("\"mqtt5\"").is_err());
    }

    #[test]
    fn mqtt_user_property_validation_preserves_duplicate_keys() {
        let properties = vec![
            MqttUserProperty {
                key: "tag".to_string(),
                value: "primary".to_string(),
            },
            MqttUserProperty {
                key: "tag".to_string(),
                value: "edge".to_string(),
            },
        ];

        validate_mqtt_user_properties(&properties).expect("duplicate keys are valid");
        assert_eq!(properties[0].value, "primary");
        assert_eq!(properties[1].value, "edge");
    }

    #[test]
    fn mqtt_user_property_validation_rejects_invalid_strings_without_value_leakage() {
        let properties = vec![MqttUserProperty {
            key: "source".to_string(),
            value: "secret\0value".to_string(),
        }];

        let error = validate_mqtt_user_properties(&properties)
            .expect_err("NUL must be rejected in MQTT UTF-8 strings");
        assert!(error.contains("user_properties[0].value"));
        assert!(!error.contains("secret"));
        assert!(!format!("{properties:?}").contains("secret"));
    }

    #[test]
    fn mqtt_v5_options_use_configured_packet_limit() {
        let options = MqttOptions::new(
            MqttProtocolVersion::V5,
            "mqtt-v5-options".to_string(),
            "localhost",
            1883,
            64 * 1024 * 1024,
        )
        .expect("build MQTT 5 options");

        assert_eq!(options.max_packet_size(), 64 * 1024 * 1024);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn mqtt_v5_options_reject_packet_limit_larger_than_u32() {
        let error = MqttOptions::new(
            MqttProtocolVersion::V5,
            "mqtt-v5-options".to_string(),
            "localhost",
            1883,
            u32::MAX as usize + 1,
        )
        .err()
        .expect("oversized MQTT 5 packet limit must fail");

        assert!(error.contains("exceeds"));
    }
}
