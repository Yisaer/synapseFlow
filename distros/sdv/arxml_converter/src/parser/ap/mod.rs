//! AP (Adaptive Platform) parser — orchestrates all AP sub-parsers
//! and implements the serviceID/eventID → data-type lookup.
//!
//! Corresponds to the Go `ap/parser/parser.go` and
//! `ap/converter/converter.go`.

pub mod datatypes;
pub mod iautosar;
pub mod interfaces;

use std::collections::HashMap;

use roxmltree::{Document, Node};

use crate::ast::types::DataType;
use crate::util::{convert, xml};

use self::iautosar::Service;
use self::interfaces::ServiceInterface;

/// Complete AP parser + type-lookup engine.
///
/// After [`parse`](ApParser::parse), use
/// [`resolve_type`](ApParser::resolve_type) to find the [`DataType`]
/// for a given `(service_id, event_id)` pair.
#[derive(Debug)]
pub struct ApParser {
    /// keyed by lowercased short-name
    pub data_types: HashMap<String, DataType>,
    /// keyed by lowercased short-name
    pub interfaces: HashMap<String, ServiceInterface>,
    /// keyed by service-ID (u16)
    pub services: HashMap<u16, Service>,
}

impl ApParser {
    pub fn new() -> Self {
        Self {
            data_types: HashMap::new(),
            interfaces: HashMap::new(),
            services: HashMap::new(),
        }
    }

    /// Parse a complete AP ARXML document.
    pub fn parse(&mut self, doc: &Document) -> Result<(), String> {
        let root = doc.root_element();
        let autosar = if root.tag_name().name() == "AUTOSAR" {
            root
        } else {
            root.children()
                .find(|n| n.tag_name().name() == "AUTOSAR")
                .ok_or("no <AUTOSAR> root element")?
        };

        let ar_packages = xml::require_child(autosar, "AR-PACKAGES")?;

        // Locate the 3 required AR-PACKAGEs, checking nested packages too
        // (some combined CP/AP files nest AP packages inside CP packages).
        let data_types_el = find_ar_package_by_name(ar_packages, "dataTypes")
            .or_else(|| find_ar_package_recursive(ar_packages, "dataTypes"))
            .ok_or("missing 'dataTypes' AR-PACKAGE")?;
        let interfaces_el = find_ar_package_by_name(ar_packages, "interfaces")
            .or_else(|| find_ar_package_recursive(ar_packages, "interfaces"))
            .ok_or("missing 'interfaces' AR-PACKAGE")?;
        let iautosar_el = find_ar_package_by_name(ar_packages, "IAUTOSAR")
            .or_else(|| find_ar_package_recursive(ar_packages, "IAUTOSAR"))
            .ok_or("missing 'IAUTOSAR' AR-PACKAGE")?;

        self.data_types = datatypes::parse_data_types(data_types_el)?;
        self.interfaces = interfaces::parse_interfaces(interfaces_el)?;
        self.services = iautosar::parse_i_autosar(iautosar_el)?;

        Ok(())
    }

    /// Reference to the parsed data types map.
    pub fn data_types(&self) -> &HashMap<String, DataType> {
        &self.data_types
    }

    /// Resolve a `(service_id, event_id)` pair to the corresponding
    /// [`DataType`] via the AP lookup chain:
    ///
    /// serviceID → Service → ServiceInterfaceRef → ServiceInterface
    /// eventID  → Event.eventRef  → ServiceInterface.Events\[ref\] → TypeRef
    ///         → FieldNotify.fieldRef → ServiceInterface.Fields\[ref\] → TypeRef
    /// TypeRef  → DataType
    pub fn resolve_type(&self, service_id: u16, event_id: u16) -> Result<&DataType, String> {
        let svc = self
            .services
            .get(&service_id)
            .ok_or_else(|| format!("service {service_id} not found"))?;

        let iface_ref = convert::extract_last(&svc.service_interface_ref).to_lowercase();
        let iface = self
            .interfaces
            .get(&iface_ref)
            .ok_or_else(|| format!("interface '{iface_ref}' not found for service {service_id}"))?;

        // Try event first
        if let Some(event) = svc.events.get(&event_id) {
            let event_ref = convert::extract_last(&event.event_ref).to_lowercase();
            let si_event = iface.events.get(&event_ref).ok_or_else(|| {
                format!(
                    "event '{event_ref}' not found in interface '{}'",
                    iface.short_name
                )
            })?;
            let type_ref = convert::extract_last(&si_event.type_ref).to_lowercase();
            return self
                .data_types
                .get(&type_ref)
                .ok_or_else(|| format!("type '{type_ref}' not found for event {event_id}"));
        }

        // Try field-notify
        if let Some(field_notify) = svc.field_notify.get(&event_id) {
            let field_ref = convert::extract_last(&field_notify.field_ref).to_lowercase();
            let si_field = iface.fields.get(&field_ref).ok_or_else(|| {
                format!(
                    "field '{field_ref}' not found in interface '{}'",
                    iface.short_name
                )
            })?;
            let type_ref = convert::extract_last(&si_field.type_ref).to_lowercase();
            return self
                .data_types
                .get(&type_ref)
                .ok_or_else(|| format!("type '{type_ref}' not found for field-notify {event_id}"));
        }

        Err(format!(
            "unknown event_id {event_id} in service {service_id}"
        ))
    }
}

impl Default for ApParser {
    fn default() -> Self {
        Self::new()
    }
}

fn find_ar_package_by_name<'a>(node: Node<'a, 'a>, name: &str) -> Option<Node<'a, 'a>> {
    node.children()
        .filter(|c| c.tag_name().name() == "AR-PACKAGE")
        .find(|c| xml::child_text(*c, "SHORT-NAME") == Some(name))
}

/// Recursively search nested AR-PACKAGE elements for one with the given
/// SHORT-NAME.  Some combined CP/AP ARXML files (like baq.arxml) nest AP
/// packages via `<AR-PACKAGE><AR-PACKAGES><AR-PACKAGE>...`.
fn find_ar_package_recursive<'a>(node: Node<'a, 'a>, name: &str) -> Option<Node<'a, 'a>> {
    for child in node
        .children()
        .filter(|c| c.tag_name().name() == "AR-PACKAGE")
    {
        if xml::child_text(child, "SHORT-NAME") == Some(name) {
            return Some(child);
        }
        // Search nested <AR-PACKAGES> within this <AR-PACKAGE>.
        if let Some(sub_packages) = child
            .children()
            .find(|c| c.tag_name().name() == "AR-PACKAGES")
            && let Some(found) = find_ar_package_recursive(sub_packages, name)
        {
            return Some(found);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const AP_FULL_XML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>dataTypes</SHORT-NAME>
      <ELEMENTS>
        <STD-CPP-IMPLEMENTATION-DATA-TYPE>
          <SHORT-NAME>BrakeCmd</SHORT-NAME>
          <CATEGORY>STRUCTURE</CATEGORY>
          <SUB-ELEMENTS>
            <CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT>
              <SHORT-NAME>pressure</SHORT-NAME>
              <TYPE-REFERENCE>
                <INPLACE>true</INPLACE>
                <TYPE-REFERENCE-REF>/PlatformTypes/uint32</TYPE-REFERENCE-REF>
              </TYPE-REFERENCE>
            </CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT>
          </SUB-ELEMENTS>
        </STD-CPP-IMPLEMENTATION-DATA-TYPE>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>interfaces</SHORT-NAME>
      <ELEMENTS>
        <SERVICE-INTERFACE>
          <SHORT-NAME>RadarSvc</SHORT-NAME>
          <EVENTS>
            <VARIABLE-DATA-PROTOTYPE>
              <SHORT-NAME>BrakeEvent</SHORT-NAME>
              <TYPE-TREF>/DataTypes/BrakeCmd</TYPE-TREF>
            </VARIABLE-DATA-PROTOTYPE>
          </EVENTS>
        </SERVICE-INTERFACE>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>IAUTOSAR</SHORT-NAME>
      <ELEMENTS>
        <SOMEIP-SERVICE-INTERFACE-DEPLOYMENT>
          <SHORT-NAME>RadarDeploy</SHORT-NAME>
          <SERVICE-INTERFACE-ID>100</SERVICE-INTERFACE-ID>
          <SERVICE-INTERFACE-REF>/Interfaces/RadarSvc</SERVICE-INTERFACE-REF>
          <EVENT-DEPLOYMENTS>
            <SOMEIP-EVENT-DEPLOYMENT>
              <SHORT-NAME>BrakeEvent</SHORT-NAME>
              <EVENT-ID>1</EVENT-ID>
              <EVENT-REF>/Interfaces/RadarSvc/BrakeEvent</EVENT-REF>
            </SOMEIP-EVENT-DEPLOYMENT>
          </EVENT-DEPLOYMENTS>
        </SOMEIP-SERVICE-INTERFACE-DEPLOYMENT>
      </ELEMENTS>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    #[test]
    fn full_ap_lookup_chain() {
        let doc = Document::parse(AP_FULL_XML).unwrap();
        let mut parser = ApParser::new();
        parser.parse(&doc).unwrap();

        let dt = parser.resolve_type(100, 1).unwrap();
        assert_eq!(dt.short_name, "BrakeCmd");
        assert_eq!(dt.category, "STRUCTURE");
    }

    #[test]
    fn unknown_service_error() {
        let doc = Document::parse(AP_FULL_XML).unwrap();
        let mut parser = ApParser::new();
        parser.parse(&doc).unwrap();

        let err = parser.resolve_type(999, 1).unwrap_err();
        assert!(err.contains("not found"));
    }
}
