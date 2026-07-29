use serde::{Deserialize, Deserializer};

pub const MAX_RESOURCE_REVISION: u64 = (1_u64 << 53) - 1;

pub fn deserialize_revision<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let revision = u64::deserialize(deserializer)?;
    validate_revision(revision).map_err(serde::de::Error::custom)?;
    Ok(revision)
}

pub fn validate_revision(revision: u64) -> Result<(), String> {
    if revision == 0 {
        return Err("revision must be greater than zero".to_string());
    }
    if revision > MAX_RESOURCE_REVISION {
        return Err(format!("revision must not exceed {MAX_RESOURCE_REVISION}"));
    }
    Ok(())
}

pub fn normalized_spec_without_revision<T: serde::Serialize>(
    value: &T,
) -> Result<serde_json::Value, String> {
    let mut value =
        serde_json::to_value(value).map_err(|err| format!("serialize resource spec: {err}"))?;
    if let Some(object) = value.as_object_mut() {
        object.remove("revision");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct RevisionValue {
        #[serde(deserialize_with = "deserialize_revision")]
        revision: u64,
    }

    #[test]
    fn accepts_positive_json_safe_integer() {
        let value: RevisionValue =
            serde_json::from_value(serde_json::json!({"revision": MAX_RESOURCE_REVISION}))
                .expect("safe revision");
        assert_eq!(value.revision, MAX_RESOURCE_REVISION);
    }

    #[test]
    fn rejects_missing_zero_fractional_negative_and_unsafe_values() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"revision": 0}),
            serde_json::json!({"revision": -1}),
            serde_json::json!({"revision": 1.5}),
            serde_json::json!({"revision": MAX_RESOURCE_REVISION + 1}),
        ] {
            assert!(
                serde_json::from_value::<RevisionValue>(value).is_err(),
                "invalid revision must be rejected"
            );
        }
    }
}
