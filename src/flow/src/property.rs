//! Process-wide static properties used by connector string templates.

use crate::secret::SecretString;
use crate::template::{engine_with_properties, ConnectorString};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// Immutable process property snapshot shared by all flow instances.
#[derive(Clone, Default)]
pub struct PropertyContext {
    values: Arc<BTreeMap<String, SecretString>>,
}

impl PropertyContext {
    /// Build a property context from already-redacted values.
    pub fn new(values: BTreeMap<String, SecretString>) -> Self {
        Self {
            values: Arc::new(values),
        }
    }

    /// Render one connector string using this property snapshot.
    pub fn render(&self, template: &str) -> Result<ConnectorString, PropertyTemplateError> {
        const TEMPLATE_NAME: &str = "connector";
        let (mut engine, profile) = engine_with_properties(self.clone());
        engine
            .add_template(TEMPLATE_NAME, template.to_string())
            .map_err(|err| PropertyTemplateError::Compile(err.to_string()))?;
        let rendered = engine
            .template(TEMPLATE_NAME)
            .render(upon::Value::Map(Default::default()))
            .to_string()
            .map_err(|err| PropertyTemplateError::Render(err.to_string()))?;
        if profile.was_property_used() {
            Ok(ConnectorString::sensitive(rendered))
        } else {
            Ok(ConnectorString::plain(rendered))
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<&SecretString> {
        self.values.get(key)
    }

    /// Return the number of configured properties without exposing their values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Return whether no properties are configured.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for PropertyContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyContext")
            .field("keys", &self.values.keys().collect::<Vec<_>>())
            .field("count", &self.values.len())
            .finish()
    }
}

/// Connector property template compilation or rendering error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PropertyTemplateError {
    #[error("invalid connector template: {0}")]
    Compile(String),
    #[error("connector template render failed: {0}")]
    Render(String),
    #[error("property key `{0}` is invalid; expected [a-z][a-z0-9_]*")]
    InvalidKey(String),
}

/// Validate the canonical lower-snake-case property key grammar.
pub fn validate_property_key(key: &str) -> Result<(), PropertyTemplateError> {
    let mut chars = key.chars();
    if !matches!(chars.next(), Some('a'..='z'))
        || !chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
    {
        return Err(PropertyTemplateError::InvalidKey(key.to_string()));
    }
    Ok(())
}

/// Validate an MQTT publish topic before a connector is created.
pub fn validate_mqtt_publish_topic(topic: &ConnectorString) -> Result<(), String> {
    let topic = topic.expose();
    if topic.is_empty() {
        return Err("MQTT publish topic must not be empty".to_string());
    }
    if topic.len() > u16::MAX as usize {
        return Err("MQTT publish topic exceeds 65535 bytes".to_string());
    }
    if topic.contains('\0') || !rumqttc::valid_topic(topic) {
        return Err("MQTT publish topic is invalid".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(entries: &[(&str, &str)]) -> PropertyContext {
        PropertyContext::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::new((*value).to_string())))
                .collect(),
        )
    }

    #[test]
    fn connector_templates_render_with_upon_property_profile() {
        struct Case {
            name: &'static str,
            template: &'static str,
            expected: &'static str,
            sensitive: bool,
        }

        let properties = context(&[
            ("vin", "VIN-123"),
            ("cs", "CS-9"),
            ("empty", ""),
            ("nested", r#"{{ prop("vin") }}"#),
        ]);
        let cases = [
            Case {
                name: "plain literal",
                template: "vehicles/static",
                expected: "vehicles/static",
                sensitive: false,
            },
            Case {
                name: "single property",
                template: r#"vehicles/{{ prop("vin") }}"#,
                expected: "vehicles/VIN-123",
                sensitive: true,
            },
            Case {
                name: "multiple properties and whitespace",
                template: r#"{{  prop( "vin" )  }}/{{ prop("cs") }}"#,
                expected: "VIN-123/CS-9",
                sensitive: true,
            },
            Case {
                name: "upon string expression",
                template: r#"{{ "literal" }}"#,
                expected: "literal",
                sensitive: false,
            },
            Case {
                name: "upon control block",
                template: "{% if true %}enabled{% endif %}",
                expected: "enabled",
                sensitive: false,
            },
            Case {
                name: "property values are not recursively expanded",
                template: r#"{{ prop("nested") }}"#,
                expected: r#"{{ prop("vin") }}"#,
                sensitive: true,
            },
            Case {
                name: "empty property",
                template: r#"prefix{{ prop("empty") }}suffix"#,
                expected: "prefixsuffix",
                sensitive: true,
            },
        ];

        for case in cases {
            let rendered = properties
                .render(case.template)
                .unwrap_or_else(|err| panic!("case `{}` failed: {err}", case.name));
            assert_eq!(rendered.expose(), case.expected, "case `{}`", case.name);
            assert_eq!(
                rendered.is_sensitive(),
                case.sensitive,
                "case `{}`",
                case.name
            );
        }
    }

    #[test]
    fn connector_templates_reject_invalid_or_unavailable_expressions() {
        struct Case {
            name: &'static str,
            template: &'static str,
            error_contains: &'static str,
        }

        let properties = context(&[("vin", "VIN-123")]);
        let cases = [
            Case {
                name: "uppercase property key",
                template: r#"{{ prop("Vin") }}"#,
                error_contains: "invalid",
            },
            Case {
                name: "numeric property key prefix",
                template: r#"{{ prop("1vin") }}"#,
                error_contains: "invalid",
            },
            Case {
                name: "empty property key",
                template: r#"{{ prop("") }}"#,
                error_contains: "invalid",
            },
            Case {
                name: "missing property",
                template: r#"{{ prop("missing") }}"#,
                error_contains: "not defined",
            },
            Case {
                name: "unclosed expression",
                template: r#"{{ prop("vin") "#,
                error_contains: "invalid connector template",
            },
            Case {
                name: "unknown function",
                template: r#"{{ foo() }}"#,
                error_contains: "function",
            },
            Case {
                name: "row is unavailable",
                template: r#"{{ .row }}"#,
                error_contains: "not found",
            },
        ];

        for case in cases {
            let err = match properties.render(case.template) {
                Ok(_) => panic!("case `{}` should fail", case.name),
                Err(err) => err,
            };
            assert!(
                err.to_string().contains(case.error_contains),
                "case `{}` returned unexpected error: {err}",
                case.name
            );
            assert!(
                !err.to_string().contains("VIN-123"),
                "case `{}` exposed a property value",
                case.name
            );
        }
    }
}
