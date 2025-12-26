use datatypes::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventtimeParseError {
    pub message: String,
}

impl EventtimeParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for EventtimeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for EventtimeParseError {}

pub trait EventtimeTypeParser: Send + Sync {
    fn parse(&self, value: &Value) -> Result<SystemTime, EventtimeParseError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinEventtimeType {
    UnixtimestampSeconds,
    UnixtimestampMillis,
}

impl BuiltinEventtimeType {
    pub fn key(&self) -> &'static str {
        match self {
            BuiltinEventtimeType::UnixtimestampSeconds => "unixtimestamp_s",
            BuiltinEventtimeType::UnixtimestampMillis => "unixtimestamp_ms",
        }
    }
}

pub struct EventtimeTypeRegistry {
    parsers: RwLock<HashMap<String, Arc<dyn EventtimeTypeParser>>>,
}

impl Default for EventtimeTypeRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_types();
        registry
    }
}

impl EventtimeTypeRegistry {
    pub fn new() -> Self {
        Self {
            parsers: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtin_types() -> Arc<Self> {
        let registry = Arc::new(Self::new());
        registry.register_builtin_types();
        registry
    }

    pub fn register(&self, key: impl Into<String>, parser: Arc<dyn EventtimeTypeParser>) {
        self.parsers
            .write()
            .expect("eventtime registry poisoned")
            .insert(key.into(), parser);
    }

    pub fn resolve(&self, key: &str) -> Result<Arc<dyn EventtimeTypeParser>, EventtimeParseError> {
        let guard = self.parsers.read().expect("eventtime registry poisoned");
        guard.get(key).cloned().ok_or_else(|| {
            let available = guard.keys().cloned().collect::<Vec<_>>().join(", ");
            EventtimeParseError::new(format!(
                "eventtime type `{key}` not registered (available: {available})"
            ))
        })
    }

    pub fn is_registered(&self, key: &str) -> bool {
        let guard = self.parsers.read().expect("eventtime registry poisoned");
        guard.contains_key(key)
    }

    pub fn list(&self) -> Vec<String> {
        let guard = self.parsers.read().expect("eventtime registry poisoned");
        let mut keys = guard.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        keys
    }

    fn register_builtin_types(&self) {
        self.register(
            BuiltinEventtimeType::UnixtimestampSeconds.key(),
            Arc::new(UnixtimestampSecondsParser),
        );
        self.register(
            BuiltinEventtimeType::UnixtimestampMillis.key(),
            Arc::new(UnixtimestampMillisParser),
        );
    }
}

struct UnixtimestampSecondsParser;

impl EventtimeTypeParser for UnixtimestampSecondsParser {
    fn parse(&self, value: &Value) -> Result<SystemTime, EventtimeParseError> {
        let seconds = parse_integer(value)?;
        let seconds = u64::try_from(seconds).map_err(|_| {
            EventtimeParseError::new(format!(
                "unixtimestamp_s expects non-negative seconds, got {seconds}"
            ))
        })?;
        UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds))
            .ok_or_else(|| EventtimeParseError::new("unixtimestamp_s overflow".to_string()))
    }
}

struct UnixtimestampMillisParser;

impl EventtimeTypeParser for UnixtimestampMillisParser {
    fn parse(&self, value: &Value) -> Result<SystemTime, EventtimeParseError> {
        let millis = parse_integer(value)?;
        let millis = u64::try_from(millis).map_err(|_| {
            EventtimeParseError::new(format!(
                "unixtimestamp_ms expects non-negative milliseconds, got {millis}"
            ))
        })?;
        let secs = millis / 1_000;
        let sub_ms = millis % 1_000;
        let nanos = sub_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| EventtimeParseError::new("unixtimestamp_ms overflow".to_string()))?;
        UNIX_EPOCH
            .checked_add(Duration::from_secs(secs))
            .and_then(|t| t.checked_add(Duration::from_nanos(nanos)))
            .ok_or_else(|| EventtimeParseError::new("unixtimestamp_ms overflow".to_string()))
    }
}

fn parse_integer(value: &Value) -> Result<i128, EventtimeParseError> {
    match value {
        Value::Null => Err(EventtimeParseError::new(
            "eventtime value is null".to_string(),
        )),
        Value::Int8(v) => Ok(i128::from(*v)),
        Value::Int16(v) => Ok(i128::from(*v)),
        Value::Int32(v) => Ok(i128::from(*v)),
        Value::Int64(v) => Ok(i128::from(*v)),
        Value::Uint8(v) => Ok(i128::from(*v)),
        Value::Uint16(v) => Ok(i128::from(*v)),
        Value::Uint32(v) => Ok(i128::from(*v)),
        Value::Uint64(v) => Ok(i128::from(*v)),
        Value::String(v) => v.parse::<i128>().map_err(|err| {
            EventtimeParseError::new(format!("eventtime string `{v}` is not an integer: {err}"))
        }),
        other => Err(EventtimeParseError::new(format!(
            "eventtime value type {:?} not supported",
            other.datatype()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_builtins() {
        let registry = EventtimeTypeRegistry::with_builtin_types();
        assert!(registry.is_registered("unixtimestamp_s"));
        assert!(registry.is_registered("unixtimestamp_ms"));
        assert_eq!(
            registry.list(),
            vec![
                "unixtimestamp_ms".to_string(),
                "unixtimestamp_s".to_string()
            ]
        );
    }

    #[test]
    fn parses_seconds() {
        let registry = EventtimeTypeRegistry::with_builtin_types();
        let parser = registry.resolve("unixtimestamp_s").unwrap();
        let ts = parser.parse(&Value::Int64(1)).unwrap();
        assert_eq!(
            ts.duration_since(UNIX_EPOCH).unwrap(),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn parses_millis() {
        let registry = EventtimeTypeRegistry::with_builtin_types();
        let parser = registry.resolve("unixtimestamp_ms").unwrap();
        let ts = parser.parse(&Value::Int64(1500)).unwrap();
        assert_eq!(
            ts.duration_since(UNIX_EPOCH).unwrap(),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn unknown_type_lists_available() {
        let registry = EventtimeTypeRegistry::with_builtin_types();
        let err = match registry.resolve("missing") {
            Ok(_) => panic!("expected unknown type error"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("eventtime type `missing` not registered"));
        assert!(err.to_string().contains("unixtimestamp_s"));
    }
}
