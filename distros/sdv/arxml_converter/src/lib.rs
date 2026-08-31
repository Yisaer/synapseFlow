pub mod ast;
pub mod decoder;
pub mod parser;
pub mod util;

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use roxmltree::Document;

use crate::ast::types::{DataType, DataTypeKind};
use crate::decoder::Decoder;
pub use crate::decoder::value::Value;
use crate::parser::ap::ApParser;
use crate::parser::cp::CpParser;
use crate::util::xml;

/// High-level entry point for AUTOSAR ARXML type resolution and
/// binary decoding.  Supports both CP (Classic Platform) and AP
/// (Adaptive Platform), auto-detected on load.
///
/// # Usage
///
/// ```no_run
/// use arxml_converter::ArxmlCodec;
///
/// let codec = ArxmlCodec::load("path/to/file.arxml").unwrap();
///
/// // Resolve type definition
/// let _dt = codec.resolve_cp(10, 100).unwrap();       // CP: (svc_id, header_id)
/// let _dt = codec.resolve_ap(100, 1).unwrap();        // AP: (svc_id, event_id)
///
/// // Decode binary payload
/// let _value = codec.decode_cp(10, 100, &[0; 4]).unwrap();
/// let _value = codec.decode_ap(100, 1, &[0; 4]).unwrap();
/// ```
#[derive(Debug)]
pub struct ArxmlCodec {
    variant: CodecVariant,
}

#[derive(Debug)]
enum CodecVariant {
    Cp {
        parser: Box<CpParser>,
        types: HashMap<String, DataType>,
        decoder: Decoder,
    },
    Ap {
        parser: Box<ApParser>,
        decoder: Decoder,
    },
}

impl ArxmlCodec {
    /// Load an AUTOSAR ARXML file, auto-detecting CP vs AP.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let xml = fs::read_to_string(path.as_ref())
            .map_err(|e| format!("failed to read {}: {e}", path.as_ref().display()))?;

        let doc = Document::parse(&xml).map_err(|e| format!("XML parse error: {e}"))?;

        let root = doc.root_element();
        let autosar = if root.has_tag_name("AUTOSAR") {
            root
        } else {
            root.children()
                .find(|n| n.has_tag_name("AUTOSAR"))
                .ok_or("no <AUTOSAR> root element")?
        };

        let ar_packages = xml::require_child(autosar, "AR-PACKAGES")?;

        if is_ap_arxml(ar_packages) {
            Self::load_as_ap(&doc)
        } else if is_cp_arxml(ar_packages) {
            let mut parser = CpParser::new();
            parser.parse(&doc)?;
            let types = parser.merged_data_types();
            Ok(Self {
                variant: CodecVariant::Cp {
                    parser: Box::new(parser),
                    types,
                    decoder: Decoder::new(),
                },
            })
        } else {
            Err("ARXML does not appear to be CP or AP".into())
        }
    }

    /// Load an AUTOSAR ARXML file, forcing Adaptive Platform (AP) parsing.
    /// Searches nested AR-PACKAGES recursively.
    pub fn load_ap(path: impl AsRef<Path>) -> Result<Self, String> {
        let xml = fs::read_to_string(path.as_ref())
            .map_err(|e| format!("failed to read {}: {e}", path.as_ref().display()))?;
        let doc = Document::parse(&xml).map_err(|e| format!("XML parse error: {e}"))?;
        Self::load_as_ap(&doc)
    }

    fn load_as_ap(doc: &Document) -> Result<Self, String> {
        let mut parser = ApParser::new();
        parser.parse(doc)?;
        Ok(Self {
            variant: CodecVariant::Ap {
                parser: Box::new(parser),
                decoder: Decoder::new(),
            },
        })
    }

    /// CP type lookup: resolve `(service_id, header_id) → DataType`.
    ///
    /// Panics if this codec was loaded from an AP ARXML.
    pub fn resolve_cp(&self, service_id: u16, header_id: u32) -> Result<&DataType, String> {
        match &self.variant {
            CodecVariant::Cp { parser, .. } => parser.resolve_type(service_id, header_id),
            CodecVariant::Ap { .. } => panic!("resolve_cp called on AP codec"),
        }
    }

    /// AP type lookup: resolve `(service_id, event_id) → DataType`.
    ///
    /// Panics if this codec was loaded from a CP ARXML.
    pub fn resolve_ap(&self, service_id: u16, event_id: u16) -> Result<&DataType, String> {
        match &self.variant {
            CodecVariant::Ap { parser, .. } => parser.resolve_type(service_id, event_id),
            CodecVariant::Cp { .. } => panic!("resolve_ap called on CP codec"),
        }
    }

    /// CP decode: resolve `(service_id, header_id)` then decode `data`.
    pub fn decode_cp(&self, service_id: u16, header_id: u32, data: &[u8]) -> Result<Value, String> {
        match &self.variant {
            CodecVariant::Cp {
                parser,
                types,
                decoder,
            } => {
                let dt = parser.resolve_type(service_id, header_id)?;
                let (_consumed, value) = decoder.decode(types, data, dt)?;
                Ok(value)
            }
            CodecVariant::Ap { .. } => panic!("decode_cp called on AP codec"),
        }
    }

    /// AP decode: resolve `(service_id, event_id)` then decode `data`.
    pub fn decode_ap(&self, service_id: u16, event_id: u16, data: &[u8]) -> Result<Value, String> {
        match &self.variant {
            CodecVariant::Ap { parser, decoder } => {
                let dt = parser.resolve_type(service_id, event_id)?;
                let (_consumed, value) = decoder.decode(parser.data_types(), data, dt)?;
                Ok(value)
            }
            CodecVariant::Cp { .. } => panic!("decode_ap called on CP codec"),
        }
    }

    /// Unified decode: takes `(service_id, event_id)` and internally dispatches
    /// to the correct CP or AP path, matching the eKuiper arxml-converter API.
    ///
    /// - **CP**: merges `(service_id << 16) | event_id` into a 32-bit `header_id`,
    ///   then resolves the CP type and decodes.
    /// - **AP**: resolves and decodes directly via `(service_id, event_id)`.
    pub fn decode(&self, service_id: u16, event_id: u16, data: &[u8]) -> Result<Value, String> {
        match &self.variant {
            CodecVariant::Ap { parser, decoder } => {
                let dt = parser.resolve_type(service_id, event_id)?;
                let (_consumed, value) = decoder.decode(parser.data_types(), data, dt)?;
                Ok(value)
            }
            CodecVariant::Cp {
                parser,
                types,
                decoder,
            } => {
                let header_id = crate::util::convert::merge_u16_to_u32(service_id, event_id);
                let dt = parser.resolve_type(service_id, header_id)?;
                let (_consumed, value) = decoder.decode(types, data, dt)?;
                Ok(value)
            }
        }
    }

    /// Unified type resolution: returns the [`DataType`] for a
    /// `(service_id, event_id)` pair, dispatching to CP or AP internally.
    pub fn resolve(&self, service_id: u16, event_id: u16) -> Result<&DataType, String> {
        match &self.variant {
            CodecVariant::Ap { parser, .. } => parser.resolve_type(service_id, event_id),
            CodecVariant::Cp { parser, .. } => {
                let header_id = crate::util::convert::merge_u16_to_u32(service_id, event_id);
                parser.resolve_type(service_id, header_id)
            }
        }
    }

    /// Resolve a type reference path (e.g. an ARRAY's `element_ref`) to its
    /// [`DataType`], searching the same type map used by the binary decoder.
    pub fn resolve_ref(&self, ref_path: &str) -> Option<DataType> {
        match &self.variant {
            CodecVariant::Ap { parser, decoder } => {
                decoder.resolve_ref(parser.data_types(), ref_path)
            }
            CodecVariant::Cp { types, decoder, .. } => decoder.resolve_ref(types, ref_path),
        }
    }

    /// Resolve human-readable names for the service and entry (method/event)
    /// corresponding to a `(service_id, event_id)` pair.
    ///
    /// Returns `(service_name, entry_name)` for use with
    /// `signal_name_pattern`.
    pub fn resolve_entry_names(
        &self,
        service_id: u16,
        event_id: u16,
    ) -> Result<(String, String), String> {
        match &self.variant {
            CodecVariant::Ap { parser, .. } => {
                let svc = parser
                    .services
                    .get(&service_id)
                    .ok_or_else(|| format!("service {service_id} not found"))?;
                let iface_ref =
                    crate::util::convert::extract_last(&svc.service_interface_ref).to_lowercase();
                let iface = parser
                    .interfaces
                    .get(&iface_ref)
                    .ok_or_else(|| format!("interface '{iface_ref}' not found"))?;
                let service_name = iface.short_name.clone();

                let entry_name = if let Some(event) = svc.events.get(&event_id) {
                    let event_ref =
                        crate::util::convert::extract_last(&event.event_ref).to_lowercase();
                    iface
                        .events
                        .get(&event_ref)
                        .map(|e| e.short_name.clone())
                        .unwrap_or_else(|| format!("0x{event_id:04X}"))
                } else if let Some(field) = svc.field_notify.get(&event_id) {
                    let field_ref =
                        crate::util::convert::extract_last(&field.field_ref).to_lowercase();
                    iface
                        .fields
                        .get(&field_ref)
                        .map(|f| f.short_name.clone())
                        .unwrap_or_else(|| format!("0x{event_id:04X}"))
                } else {
                    format!("0x{event_id:04X}")
                };

                Ok((service_name, entry_name))
            }
            CodecVariant::Cp { parser, .. } => {
                let service_name = parser
                    .topology_parser()
                    .service_id_map
                    .get(&service_id)
                    .cloned()
                    .unwrap_or_else(|| format!("0x{service_id:04X}"));

                let header_id = crate::util::convert::merge_u16_to_u32(service_id, event_id);
                let dt = parser.resolve_type(service_id, header_id)?;
                let entry_name = dt.short_name.clone();

                Ok((service_name, entry_name))
            }
        }
    }

    /// Return the field names and their resolved base-type names for a
    /// given `(service_id, event_id)` entry.
    ///
    /// Unwraps ARRAY → element type and STRUCTURE → fields.
    /// Each field's `type_ref` is resolved through the type map to get
    /// the base type name (e.g. `"uint16"`, `"float"`).
    pub fn entry_fields(&self, service_id: u16, event_id: u16) -> Option<Vec<(String, String)>> {
        let dt = self.resolve(service_id, event_id).ok()?;
        // Unwrap ARRAY → element type.
        let dt = match &dt.kind {
            DataTypeKind::Array(arr) => self.resolve_ref(&arr.element_ref)?,
            _ => dt.clone(),
        };
        let fields = match &dt.kind {
            DataTypeKind::Structure(s) => &s.fields,
            _ => return None,
        };
        let mut result = Vec::with_capacity(fields.len());
        for f in fields {
            let type_name = self.resolve_base_type(&f.type_ref);
            result.push((f.name.clone(), type_name));
        }
        Some(result)
    }

    /// Resolve a type reference path to its base type name.
    /// E.g. `"ADT_ADAS_val_SlotID"` → `"uint16"`.
    /// Falls back to `"unknown"` when the type cannot be resolved.
    fn resolve_base_type(&self, type_ref: &str) -> String {
        let Some(resolved) = self.resolve_ref(type_ref) else {
            return "unknown".to_string();
        };
        match &resolved.kind {
            DataTypeKind::TypeReference(tr) => {
                // type_name may be a path like "/BaseTypes/uint16".
                tr.type_name
                    .rsplit('/')
                    .next()
                    .unwrap_or(&tr.type_name)
                    .to_lowercase()
            }
            DataTypeKind::Structure(_) => "struct".to_string(),
            DataTypeKind::Array(_) => "array".to_string(),
            DataTypeKind::Vector(_) => "vector".to_string(),
        }
    }

    /// Return all known `(service_id, event_id)` pairs that resolve
    /// to a valid type.
    pub fn known_entries(&self) -> Vec<(u16, u16)> {
        match &self.variant {
            CodecVariant::Cp { parser, .. } => parser
                .topology_parser()
                .header_id_ref
                .keys()
                .filter_map(|&header_id| {
                    let service_id = (header_id >> 16) as u16;
                    let event_id = header_id as u16;
                    self.resolve(service_id, event_id).ok()?;
                    Some((service_id, event_id))
                })
                .collect(),
            CodecVariant::Ap { parser, .. } => {
                let mut entries = Vec::new();
                for (&service_id, svc) in &parser.services {
                    for &event_id in svc.events.keys() {
                        entries.push((service_id, event_id));
                    }
                    for &field_id in svc.field_notify.keys() {
                        entries.push((service_id, field_id));
                    }
                }
                entries.retain(|&(sid, eid)| self.resolve(sid, eid).is_ok());
                entries
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Auto-detection helpers
// ---------------------------------------------------------------------------

fn has_ar_package(node: roxmltree::Node, name: &str) -> bool {
    // Check direct children first.
    if node
        .children()
        .filter(|c| c.has_tag_name("AR-PACKAGE"))
        .any(|c| xml::child_text(c, "SHORT-NAME") == Some(name))
    {
        return true;
    }
    // Recursively check nested <AR-PACKAGES>.
    node.children()
        .filter(|c| c.has_tag_name("AR-PACKAGE"))
        .any(|c| {
            c.children()
                .filter(|n| n.has_tag_name("AR-PACKAGES"))
                .any(|sub| has_ar_package(sub, name))
        })
}

fn is_ap_arxml(ar_packages: roxmltree::Node) -> bool {
    has_ar_package(ar_packages, "interfaces")
        && has_ar_package(ar_packages, "dataTypes")
        && has_ar_package(ar_packages, "IAUTOSAR")
}

fn is_cp_arxml(ar_packages: roxmltree::Node) -> bool {
    has_ar_package(ar_packages, "DataTypes")
        && has_ar_package(ar_packages, "Communication")
        && has_ar_package(ar_packages, "SoftwareTypes")
        && has_ar_package(ar_packages, "System")
        && has_ar_package(ar_packages, "Topology")
        && has_ar_package(ar_packages, "DataTypeMappingSets")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const CP_ARXML: &str = include_str!("../tests/test_data/minimal_cp.arxml");

    #[test]
    fn detect_cp_arxml() {
        let doc = Document::parse(CP_ARXML).unwrap();
        let ap = doc.root_element();
        let ar_packages = xml::require_child(ap, "AR-PACKAGES").unwrap();
        assert!(is_cp_arxml(ar_packages));
        assert!(!is_ap_arxml(ar_packages));
    }

    #[test]
    fn end_to_end_cp_decode() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_cp.arxml");
        fs::write(&path, CP_ARXML).unwrap();

        let codec = ArxmlCodec::load(&path).unwrap();

        // CP: service_id=10, header_id=100 → SpeedType (u32)
        let dt = codec.resolve_cp(10, 100).unwrap();
        assert_eq!(dt.short_name, "SpeedType");

        let v = codec.decode_cp(10, 100, &[0, 0, 0, 42]).unwrap();
        assert_eq!(v, Value::U32(42));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_entry_fields_and_types() {
        let arxml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let codec = ArxmlCodec::load(arxml_path).unwrap();

        let fields = codec.entry_fields(0xAB04, 0x8003).expect("entry_fields");

        // Verify the three leaf types we know from baq.arxml.
        let type_names: Vec<&str> = fields.iter().map(|(_, t)| t.as_str()).collect();
        assert!(
            type_names.contains(&"uint8"),
            "should contain uint8 fields: {type_names:?}"
        );
        assert!(
            type_names.contains(&"uint16"),
            "should contain uint16 fields: {type_names:?}"
        );
        assert!(
            type_names.contains(&"struct"),
            "should contain struct fields (nested types): {type_names:?}"
        );
    }

    /// Validate field names and data types for diverse entries from the
    /// production baq.arxml, covering all known type categories.
    #[test]
    fn test_baq_all_entries_field_types() {
        let arxml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let codec = ArxmlCodec::load(arxml_path).unwrap();

        // (0xAB04, 0x8003): uint8, uint16, struct — full field list.
        let fields = codec.entry_fields(0xAB04, 0x8003).unwrap();
        assert_eq!(
            fields,
            vec![
                ("DTE_SlotID".into(), "uint16".into()),
                ("DTE_SlotType".into(), "uint8".into()),
                ("DTE_SlotStatus".into(), "uint8".into()),
                ("DTE_SlotPointTop1".into(), "struct".into()),
                ("DTE_SlotPointTop2".into(), "struct".into()),
                ("DTE_SlotPointBottom1".into(), "struct".into()),
                ("DTE_SlotPointBottom2".into(), "struct".into()),
                ("DTE_SlotNum".into(), "uint8".into()),
                ("DTE_SlotWheelStopperInfo".into(), "struct".into()),
            ],
        );

        // (0xAA0B, 0x0001): uint8, string.
        let fields = codec.entry_fields(0xAA0B, 0x0001).unwrap();
        assert_eq!(
            fields,
            vec![
                ("DTE_CallType".into(), "uint8".into()),
                ("DTE_CallOperation".into(), "uint8".into()),
                ("DTE_Telephonenumber".into(), "string".into()),
            ],
        );

        // (0xAB10, 0x8005): float64, float32.
        let fields = codec.entry_fields(0xAB10, 0x8005).unwrap();
        assert_eq!(
            fields,
            vec![
                ("DTE_longitude".into(), "float64".into()),
                ("DTE_latitude".into(), "float64".into()),
                ("DTE_altitude".into(), "float32".into()),
                ("DTE_heading".into(), "float32".into()),
                ("DTE_vehicleSpeed".into(), "float32".into()),
                ("DTE_FusionlocalizationSD".into(), "float32".into()),
                ("DTE_altitudeSD".into(), "float32".into()),
                ("DTE_headingSD".into(), "float32".into()),
                ("DTE_vehicleSpeedSD".into(), "float32".into()),
                ("DTE_timestampmSec".into(), "float32".into()),
            ],
        );

        // (0xAC0C, 0x8008): sint64, sint32, struct, array.
        let fields = codec.entry_fields(0xAC0C, 0x8008).unwrap();
        assert_eq!(
            fields,
            vec![
                ("DTE_CongestionPathID".into(), "sint64".into()),
                ("DTE_TotalTimeOfSeconds".into(), "sint64".into()),
                ("DTE_TotalRemainDist".into(), "sint64".into()),
                ("DTE_Unobstructed".into(), "sint32".into()),
                ("DTE_CongestionInfos".into(), "array".into()),
                ("DTE_CongestionExtend".into(), "struct".into()),
                ("DTE_NaviCongestionReserved".into(), "array".into()),
            ],
        );

        // Verify known_entries contains expected entries.
        let entries: std::collections::HashSet<_> = codec.known_entries().into_iter().collect();
        assert!(entries.contains(&(0xAB04, 0x8003)));
        assert!(entries.contains(&(0xAA0B, 0x0001)));
        assert!(entries.contains(&(0xAB10, 0x8005)));
        assert!(entries.contains(&(0xAC0C, 0x8008)));
    }
}
