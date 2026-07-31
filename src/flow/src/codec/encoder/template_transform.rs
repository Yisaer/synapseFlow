use super::EncodeError;
use crate::property::PropertyContext;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use upon::Engine;

const TEMPLATE_NAME: &str = "row_template";

pub(super) struct JsonTemplateTransform {
    engine: Engine<'static>,
}

impl JsonTemplateTransform {
    pub fn compile(template: &str, properties: PropertyContext) -> Result<Self, EncodeError> {
        let (mut engine, _) = crate::template::engine_with_properties(properties);
        engine.add_function("json", |value: &upon::Value| -> String {
            upon_value_to_json(value).to_string()
        });
        engine
            .add_template(TEMPLATE_NAME, template.to_string())
            .map_err(|err| EncodeError::Other(format!("invalid template: {err}")))?;
        Ok(Self { engine })
    }

    pub fn render_item(&self, row: JsonValue) -> Result<Vec<u8>, EncodeError> {
        let rendered = self
            .engine
            .template(TEMPLATE_NAME)
            .render(upon::value! {
                row: row
            })
            .to_string()
            .map_err(|err| EncodeError::Other(format!("template render error: {err}")))?;

        serde_json::from_str::<JsonValue>(&rendered).map_err(|err| {
            EncodeError::Other(format!("template rendered invalid json item: {err}"))
        })?;
        Ok(rendered.into_bytes())
    }
}

fn upon_value_to_json(value: &upon::Value) -> JsonValue {
    match value {
        upon::Value::None => JsonValue::Null,
        upon::Value::Bool(v) => JsonValue::Bool(*v),
        upon::Value::Integer(v) => JsonValue::Number(JsonNumber::from(*v)),
        upon::Value::Float(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        upon::Value::String(v) => JsonValue::String(v.clone()),
        upon::Value::List(items) => {
            JsonValue::Array(items.iter().map(upon_value_to_json).collect::<Vec<_>>())
        }
        upon::Value::Map(map) => {
            let mut json = JsonMap::with_capacity(map.len());
            for (key, value) in map.iter() {
                json.insert(key.clone(), upon_value_to_json(value));
            }
            JsonValue::Object(json)
        }
    }
}
