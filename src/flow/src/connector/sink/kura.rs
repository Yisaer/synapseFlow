//! Kura sink connector — writes VSS signal values to a kura server via gRPC (yoriito VISS producer).

use super::{SinkConnector, SinkConnectorError};
use async_trait::async_trait;
use datatypes::Value;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::sync::Arc;

use crate::model::Collection;
use crate::runtime::TaskSpawner;

// ── generated protobuf types ────────────────────────────────────────────────
#[allow(clippy::enum_variant_names)]
mod kura_proto {
    tonic::include_proto!("yoriito.viss.v1");

    pub mod producer {
        tonic::include_proto!("yoriito.viss.v1.producer");
    }
}

use kura_proto::producer::viss_client::VissClient;
use kura_proto::producer::{set_current_response, SetCurrentRequest};
use kura_proto::{value_type, DataPackageCurrent, DataPackagesCurrent, DataPointCurrent};

/// Kura sink configuration.
#[derive(Debug, Clone)]
pub struct KuraSinkConfig {
    pub sink_name: String,
    /// Kura gRPC endpoint, e.g. `http://127.0.0.1:50051`
    pub addr: String,
    /// Path to the JSON column-to-VSS mapping file.
    pub mapping_path: String,
}

/// Target VISS/VSS scalar type for a mapping override.
///
/// JSON names follow the VSS catalog (`float`, `double`, `boolean`, …). `float32`,
/// `float64`, and `bool` are accepted as aliases of `float`, `double`, and `boolean`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum KuraMappingDataType {
    #[serde(rename = "string")]
    String,
    #[serde(alias = "bool", rename = "boolean")]
    Bool,
    #[serde(rename = "int8")]
    Int8,
    #[serde(rename = "int16")]
    Int16,
    #[serde(rename = "int32")]
    Int32,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint8")]
    Uint8,
    #[serde(rename = "uint16")]
    Uint16,
    #[serde(rename = "uint32")]
    Uint32,
    #[serde(rename = "uint64")]
    Uint64,
    #[serde(alias = "float32", rename = "float")]
    Float32,
    #[serde(alias = "float64", rename = "double")]
    Float64,
}

impl fmt::Display for KuraMappingDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::String => "string",
            Self::Bool => "boolean",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Uint8 => "uint8",
            Self::Uint16 => "uint16",
            Self::Uint32 => "uint32",
            Self::Uint64 => "uint64",
            Self::Float32 => "float",
            Self::Float64 => "double",
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum KuraMappingEntry {
    Path(String),
    Typed {
        path: String,
        data_type: KuraMappingDataType,
    },
}

impl KuraMappingEntry {
    fn path(&self) -> &str {
        match self {
            Self::Path(path) | Self::Typed { path, .. } => path,
        }
    }

    fn data_type(&self) -> Option<KuraMappingDataType> {
        match self {
            Self::Path(_) => None,
            Self::Typed { data_type, .. } => Some(*data_type),
        }
    }
}

pub(crate) struct KuraSinkConnector {
    id: String,
    config: KuraSinkConfig,
    mapping: Option<Arc<HashMap<String, KuraMappingEntry>>>,
    client: Option<VissClient<tonic::transport::Channel>>,
}

impl KuraSinkConnector {
    pub fn new(id: impl Into<String>, config: KuraSinkConfig, _spawner: TaskSpawner) -> Self {
        Self {
            id: id.into(),
            config,
            mapping: None,
            client: None,
        }
    }

    // ── mapping file ────────────────────────────────────────────────────────

    fn load_mapping(&self) -> Result<HashMap<String, KuraMappingEntry>, SinkConnectorError> {
        let raw = fs::read_to_string(&self.config.mapping_path).map_err(|err| {
            SinkConnectorError::Other(format!(
                "kura sink failed to read mapping file {}: {err}",
                self.config.mapping_path
            ))
        })?;
        let mapping: HashMap<String, KuraMappingEntry> =
            serde_json::from_str(&raw).map_err(|err| {
                SinkConnectorError::Other(format!(
                    "kura sink failed to parse mapping JSON {}: {err}",
                    self.config.mapping_path
                ))
            })?;
        Ok(mapping)
    }

    fn ensure_mapping_loaded(&mut self) -> Result<(), SinkConnectorError> {
        if self.mapping.is_some() {
            return Ok(());
        }
        self.mapping = Some(Arc::new(self.load_mapping()?));
        Ok(())
    }

    // ── gRPC client ─────────────────────────────────────────────────────────

    async fn ensure_client(&mut self) -> Result<(), SinkConnectorError> {
        if self.client.is_some() {
            return Ok(());
        }

        let addr = self.config.addr.trim();
        let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };

        let client = VissClient::connect(endpoint.clone()).await.map_err(|err| {
            SinkConnectorError::Other(format!(
                "kura sink failed to connect to `{}`: {err}",
                self.config.addr
            ))
        })?;

        self.client = Some(client);
        Ok(())
    }

    // ── value conversion ────────────────────────────────────────────────────

    /// Convert a `datatypes::Value` into the proto `ValueType` oneof.
    fn to_value_type(
        value: &Value,
        target_type: Option<KuraMappingDataType>,
    ) -> Result<kura_proto::ValueType, SinkConnectorError> {
        if let Some(target_type) = target_type {
            return Self::to_typed_value_type(value, target_type);
        }

        let vt = match value {
            Value::Null => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support null values".to_string(),
                ));
            }
            Value::Bool(b) => value_type::ValueType::Bool(*b),
            Value::Int8(v) => value_type::ValueType::Int8(*v as i32),
            Value::Int16(v) => value_type::ValueType::Int16(*v as i32),
            Value::Int32(v) => value_type::ValueType::Int32(*v),
            Value::Int64(v) => value_type::ValueType::Int64(*v),
            Value::Uint8(v) => value_type::ValueType::Uint8(*v as u32),
            Value::Uint16(v) => value_type::ValueType::Uint16(*v as u32),
            Value::Uint32(v) => value_type::ValueType::Uint32(*v),
            Value::Uint64(v) => value_type::ValueType::Uint64(*v),
            Value::Float32(v) => value_type::ValueType::Float(*v),
            Value::Float64(v) => value_type::ValueType::Double(*v),
            Value::String(s) => value_type::ValueType::String(s.clone()),
            // Struct, list, bytes, and timestamp values have no standard VSS
            // wire representation; reject them explicitly.
            Value::Struct(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support struct values".to_string(),
                ));
            }
            Value::List(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support list values".to_string(),
                ));
            }
            Value::Bytes(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support bytes values".to_string(),
                ));
            }
            Value::Timestamp(_) => {
                return Err(SinkConnectorError::Other(
                    "kura sink does not support timestamp values".to_string(),
                ));
            }
        };
        Ok(kura_proto::ValueType {
            value_type: Some(vt),
        })
    }

    fn to_typed_value_type(
        value: &Value,
        target_type: KuraMappingDataType,
    ) -> Result<kura_proto::ValueType, SinkConnectorError> {
        let value_type = match target_type {
            KuraMappingDataType::String => match value {
                Value::String(value) => value_type::ValueType::String(value.clone()),
                _ => return Err(conversion_error(value, target_type)),
            },
            KuraMappingDataType::Bool => match as_signed_integer(value) {
                Some(0) => value_type::ValueType::Bool(false),
                Some(1) => value_type::ValueType::Bool(true),
                Some(integer) => {
                    return Err(SinkConnectorError::Other(format!(
                        "kura sink value {integer} is not a boolean 0 or 1"
                    )));
                }
                None => return Err(conversion_error(value, target_type)),
            },
            KuraMappingDataType::Int8 => value_type::ValueType::Int8(convert_integer(
                value,
                target_type,
                i8::MIN as i128,
                i8::MAX as i128,
            )? as i32),
            KuraMappingDataType::Int16 => value_type::ValueType::Int16(convert_integer(
                value,
                target_type,
                i16::MIN as i128,
                i16::MAX as i128,
            )? as i32),
            KuraMappingDataType::Int32 => value_type::ValueType::Int32(convert_integer(
                value,
                target_type,
                i32::MIN as i128,
                i32::MAX as i128,
            )? as i32),
            KuraMappingDataType::Int64 => value_type::ValueType::Int64(convert_integer(
                value,
                target_type,
                i64::MIN as i128,
                i64::MAX as i128,
            )? as i64),
            KuraMappingDataType::Uint8 => value_type::ValueType::Uint8(convert_integer(
                value,
                target_type,
                0,
                u8::MAX as i128,
            )? as u32),
            KuraMappingDataType::Uint16 => value_type::ValueType::Uint16(convert_integer(
                value,
                target_type,
                0,
                u16::MAX as i128,
            )? as u32),
            KuraMappingDataType::Uint32 => value_type::ValueType::Uint32(convert_integer(
                value,
                target_type,
                0,
                u32::MAX as i128,
            )? as u32),
            KuraMappingDataType::Uint64 => value_type::ValueType::Uint64(convert_integer(
                value,
                target_type,
                0,
                u64::MAX as i128,
            )? as u64),
            KuraMappingDataType::Float32 => {
                let numeric = as_f64(value).ok_or_else(|| conversion_error(value, target_type))?;
                let converted = numeric as f32;
                if numeric.is_finite() && !converted.is_finite() {
                    return Err(range_error(numeric, target_type));
                }
                value_type::ValueType::Float(converted)
            }
            KuraMappingDataType::Float64 => {
                let numeric = as_f64(value).ok_or_else(|| conversion_error(value, target_type))?;
                value_type::ValueType::Double(numeric)
            }
        };
        Ok(kura_proto::ValueType {
            value_type: Some(value_type),
        })
    }

    /// Walk a collection row and resolve every non-null column that appears in
    /// the mapping file.  Returns `(path, DataPointCurrent)` pairs plus whether
    /// any output column matched a mapping key.
    fn iter_updates_for_row(
        mapping: &HashMap<String, KuraMappingEntry>,
        tuple: &crate::model::Tuple,
    ) -> Result<(Vec<(String, DataPointCurrent)>, bool), SinkConnectorError> {
        let mut out: Vec<(String, DataPointCurrent)> = Vec::new();
        let mut matched_column = false;
        for ((_, column_name), value) in tuple.entries() {
            let Some(mapping_entry) = mapping.get(column_name) else {
                continue;
            };
            matched_column = true;
            if value.is_null() {
                continue;
            }
            let vt = Self::to_value_type(value, mapping_entry.data_type())?;
            let dp = DataPointCurrent {
                stored_timestamp: 0,
                value: Some(vt),
                produced_timestamp: None,
                is_available_sensor: None,
                is_available_actuator: None,
            };
            out.push((mapping_entry.path().to_string(), dp));
        }
        Ok((out, matched_column))
    }
}

fn conversion_error(value: &Value, target_type: KuraMappingDataType) -> SinkConnectorError {
    SinkConnectorError::Other(format!(
        "kura sink cannot convert {value:?} to {target_type}"
    ))
}

fn range_error(numeric: impl fmt::Display, target_type: KuraMappingDataType) -> SinkConnectorError {
    SinkConnectorError::Other(format!(
        "kura sink value {numeric} is out of range for {target_type}"
    ))
}

fn convert_integer(
    value: &Value,
    target_type: KuraMappingDataType,
    min: i128,
    max: i128,
) -> Result<i128, SinkConnectorError> {
    let integer = as_signed_integer(value).ok_or_else(|| conversion_error(value, target_type))?;
    if integer < min || integer > max {
        return Err(range_error(integer, target_type));
    }
    Ok(integer)
}

fn as_signed_integer(value: &Value) -> Option<i128> {
    match value {
        Value::Int8(value) => Some(i128::from(*value)),
        Value::Int16(value) => Some(i128::from(*value)),
        Value::Int32(value) => Some(i128::from(*value)),
        Value::Int64(value) => Some(i128::from(*value)),
        Value::Uint8(value) => Some(i128::from(*value)),
        Value::Uint16(value) => Some(i128::from(*value)),
        Value::Uint32(value) => Some(i128::from(*value)),
        Value::Uint64(value) => Some(i128::from(*value)),
        Value::Bool(true) => Some(1),
        Value::Bool(false) => Some(0),
        Value::Float32(value) => exact_integer(f64::from(*value)),
        Value::Float64(value) => exact_integer(*value),
        _ => None,
    }
}

fn exact_integer(value: f64) -> Option<i128> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    let converted = value as i128;
    if converted as f64 != value {
        return None;
    }
    Some(converted)
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int8(value) => Some(f64::from(*value)),
        Value::Int16(value) => Some(f64::from(*value)),
        Value::Int32(value) => Some(f64::from(*value)),
        Value::Int64(value) => Some(*value as f64),
        Value::Uint8(value) => Some(f64::from(*value)),
        Value::Uint16(value) => Some(f64::from(*value)),
        Value::Uint32(value) => Some(f64::from(*value)),
        Value::Uint64(value) => Some(*value as f64),
        Value::Float32(value) => Some(f64::from(*value)),
        Value::Float64(value) => Some(*value),
        Value::Bool(true) => Some(1.0),
        Value::Bool(false) => Some(0.0),
        _ => None,
    }
}

// ── SinkConnector impl ──────────────────────────────────────────────────────

#[async_trait]
impl SinkConnector for KuraSinkConnector {
    fn id(&self) -> &str {
        &self.id
    }

    async fn ready(&mut self) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        self.ensure_client().await?;
        tracing::info!(
            connector_id = %self.id,
            addr = %self.config.addr,
            mapping_path = %self.config.mapping_path,
            "kura sink ready"
        );
        Ok(())
    }

    async fn send_collection(
        &mut self,
        collection: &dyn Collection,
    ) -> Result<(), SinkConnectorError> {
        self.ensure_mapping_loaded()?;
        let mapping = self
            .mapping
            .clone()
            .ok_or_else(|| SinkConnectorError::Other("kura sink mapping missing".to_string()))?;

        let mut packages: Vec<DataPackageCurrent> = Vec::new();
        let mut matched_column = false;
        let mut row_count = 0;

        // Collect all column → VSS-path mappings per row into a single batch.
        for tuple in collection.rows() {
            row_count += 1;
            let (entries, row_matched_column) =
                Self::iter_updates_for_row(mapping.as_ref(), tuple)?;
            matched_column |= row_matched_column;
            for (path, dp) in entries {
                packages.push(DataPackageCurrent { path, dp: vec![dp] });
            }
        }

        if row_count > 0 && !matched_column {
            return Err(SinkConnectorError::Other(
                "kura sink mapping does not match any pipeline output column".to_string(),
            ));
        }

        if packages.is_empty() {
            return Ok(());
        }

        let request = tonic::Request::new(SetCurrentRequest {
            data: Some(DataPackagesCurrent { data: packages }),
        });

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| SinkConnectorError::Other("kura client missing".to_string()))?;

        let response = client
            .set_current(request)
            .await
            .map_err(|err| SinkConnectorError::Other(format!("kura set_current: {err}")))?;

        let resp = response.into_inner();
        if let Some(result) = resp.result {
            match result {
                set_current_response::Result::Success(_) => {}
                set_current_response::Result::Error(err) => {
                    return Err(SinkConnectorError::Other(format!(
                        "kura set_current error: {:?} {:?} {}",
                        err.number(),
                        err.reason(),
                        err.description.as_deref().unwrap_or("")
                    )));
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkConnectorError> {
        self.client = None;
        tracing::info!(connector_id = %self.id, "kura sink closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        value_type, KuraMappingDataType, KuraMappingEntry, KuraSinkConfig, KuraSinkConnector,
    };
    use crate::connector::sink::SinkConnector;
    use crate::model::{Message, RecordBatch, Tuple};
    use datatypes::Value;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn tuple(entries: &[(&str, Value)]) -> Tuple {
        let keys = entries
            .iter()
            .map(|(key, _)| Arc::<str>::from(*key))
            .collect::<Vec<_>>();
        let values = entries
            .iter()
            .map(|(_, value)| Arc::new(value.clone()))
            .collect::<Vec<_>>();
        Tuple::new(vec![Arc::new(Message::new("", keys, values))])
    }

    fn path_mapping(path: &str) -> KuraMappingEntry {
        KuraMappingEntry::Path(path.to_string())
    }

    fn typed_mapping(path: &str, data_type: KuraMappingDataType) -> KuraMappingEntry {
        KuraMappingEntry::Typed {
            path: path.to_string(),
            data_type,
        }
    }

    fn convert(
        value: Value,
        data_type: KuraMappingDataType,
    ) -> Result<value_type::ValueType, String> {
        KuraSinkConnector::to_value_type(&value, Some(data_type))
            .map(|value| {
                value
                    .value_type
                    .expect("converted ValueType should set the oneof")
            })
            .map_err(|err| err.to_string())
    }

    #[test]
    fn typed_float32_mapping_converts_float64_to_viss_float() {
        let mapping = HashMap::from([(
            "speed".to_string(),
            typed_mapping("Vehicle.Speed", KuraMappingDataType::Float32),
        )]);

        let (updates, matched) = KuraSinkConnector::iter_updates_for_row(
            &mapping,
            &tuple(&[("speed", Value::Float64(27.75))]),
        )
        .expect("typed mapping should convert the value");

        assert!(matched);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, "Vehicle.Speed");
        assert!(matches!(
            updates[0]
                .1
                .value
                .as_ref()
                .and_then(|value| value.value_type.as_ref()),
            Some(value_type::ValueType::Float(value)) if *value == 27.75_f32
        ));
    }

    #[test]
    fn typed_mapping_converts_vss_scalar_targets() {
        assert!(matches!(
            convert(Value::Int64(42), KuraMappingDataType::Uint8).unwrap(),
            value_type::ValueType::Uint8(42)
        ));
        assert!(matches!(
            convert(Value::Int64(-7), KuraMappingDataType::Int16).unwrap(),
            value_type::ValueType::Int16(-7)
        ));
        assert!(matches!(
            convert(Value::Uint64(1_000), KuraMappingDataType::Int32).unwrap(),
            value_type::ValueType::Int32(1_000)
        ));
        assert!(matches!(
            convert(Value::Float64(27.0), KuraMappingDataType::Int32).unwrap(),
            value_type::ValueType::Int32(27)
        ));
        assert!(matches!(
            convert(Value::Int64(0), KuraMappingDataType::Bool).unwrap(),
            value_type::ValueType::Bool(false)
        ));
        assert!(matches!(
            convert(Value::Int64(1), KuraMappingDataType::Bool).unwrap(),
            value_type::ValueType::Bool(true)
        ));
        assert!(matches!(
            convert(Value::Bool(true), KuraMappingDataType::Uint8).unwrap(),
            value_type::ValueType::Uint8(1)
        ));
        assert!(matches!(
            convert(Value::String("VIN".into()), KuraMappingDataType::String).unwrap(),
            value_type::ValueType::String(value) if value == "VIN"
        ));
        assert!(matches!(
            convert(Value::Int32(3), KuraMappingDataType::Float64).unwrap(),
            value_type::ValueType::Double(value) if value == 3.0
        ));
    }

    #[test]
    fn typed_mapping_rejects_out_of_range_and_incompatible_values() {
        let uint8_range = convert(Value::Int64(300), KuraMappingDataType::Uint8).unwrap_err();
        assert!(uint8_range.contains("out of range for uint8"));

        let negative_uint = convert(Value::Int64(-1), KuraMappingDataType::Uint32).unwrap_err();
        assert!(negative_uint.contains("out of range for uint32"));

        let oversized_int64 =
            convert(Value::Uint64(u64::MAX), KuraMappingDataType::Int64).unwrap_err();
        assert!(oversized_int64.contains("out of range for int64"));

        let fractional = convert(Value::Float64(27.75), KuraMappingDataType::Int32).unwrap_err();
        assert!(fractional.contains("cannot convert"));

        let not_bool = convert(Value::Int64(2), KuraMappingDataType::Bool).unwrap_err();
        assert!(not_bool.contains("not a boolean 0 or 1"));

        let string_from_int = convert(Value::Int64(1), KuraMappingDataType::String).unwrap_err();
        assert!(string_from_int.contains("cannot convert"));
    }

    #[test]
    fn legacy_mapping_preserves_native_float64_type() {
        let mapping = HashMap::from([("speed".to_string(), path_mapping("Vehicle.Speed"))]);

        let (updates, matched) = KuraSinkConnector::iter_updates_for_row(
            &mapping,
            &tuple(&[("speed", Value::Float64(27.75))]),
        )
        .expect("legacy mapping should preserve native conversion");

        assert!(matched);
        assert!(matches!(
            updates[0]
                .1
                .value
                .as_ref()
                .and_then(|value| value.value_type.as_ref()),
            Some(value_type::ValueType::Double(value)) if *value == 27.75
        ));
    }

    #[test]
    fn mapping_json_accepts_legacy_and_typed_entries() {
        let mapping: HashMap<String, KuraMappingEntry> = serde_json::from_str(
            r#"{
                "vin": "Vehicle.VehicleIdentification.VIN",
                "speed": {"path": "Vehicle.Speed", "data_type": "float32"},
                "rpm": {"path": "Vehicle.Powertrain.CombustionEngine.Speed", "data_type": "uint16"},
                "open": {"path": "Vehicle.Cabin.Door.Row1.DriverSide.IsOpen", "data_type": "boolean"},
                "temp": {"path": "Vehicle.Cabin.HVAC.AmbientAirTemperature", "data_type": "float"}
            }"#,
        )
        .expect("legacy paths and VSS data_type names should deserialize");

        assert!(matches!(mapping["vin"], KuraMappingEntry::Path(_)));
        assert!(matches!(
            mapping["speed"],
            KuraMappingEntry::Typed {
                data_type: KuraMappingDataType::Float32,
                ..
            }
        ));
        assert!(matches!(
            mapping["rpm"],
            KuraMappingEntry::Typed {
                data_type: KuraMappingDataType::Uint16,
                ..
            }
        ));
        assert!(matches!(
            mapping["open"],
            KuraMappingEntry::Typed {
                data_type: KuraMappingDataType::Bool,
                ..
            }
        ));
        assert!(matches!(
            mapping["temp"],
            KuraMappingEntry::Typed {
                data_type: KuraMappingDataType::Float32,
                ..
            }
        ));
    }

    #[test]
    fn null_mapped_column_counts_as_a_mapping_match() {
        let mapping = HashMap::from([("speed".to_string(), path_mapping("Vehicle.Speed"))]);

        let (updates, matched) =
            KuraSinkConnector::iter_updates_for_row(&mapping, &tuple(&[("speed", Value::Null)]))
                .expect("null mapped values should be skipped");

        assert!(matched);
        assert!(updates.is_empty());
    }

    #[tokio::test]
    async fn send_collection_rejects_mapping_without_output_column_match() {
        let mapping = HashMap::from([("speed".to_string(), path_mapping("Vehicle.Speed"))]);
        let mut connector = KuraSinkConnector {
            id: "kura_test".to_string(),
            config: KuraSinkConfig {
                sink_name: "kura_test".to_string(),
                addr: "http://127.0.0.1:50053".to_string(),
                mapping_path: "unused.json".to_string(),
            },
            mapping: Some(Arc::new(mapping)),
            client: None,
        };
        let batch = RecordBatch::new(vec![tuple(&[("can_speed", Value::Float64(27.75))])])
            .expect("test batch should be valid");

        let error = connector
            .send_collection(&batch)
            .await
            .expect_err("a completely unmatched mapping must fail");

        assert!(error
            .to_string()
            .contains("mapping does not match any pipeline output column"));
    }
}
