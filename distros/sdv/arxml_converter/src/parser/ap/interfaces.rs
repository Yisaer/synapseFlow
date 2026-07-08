//! AP service-interface parser — extracts Event and Field type
//! references from `SERVICE-INTERFACE` elements.
//!
//! Corresponds to the Go `ap/parser/interfaces.go`.

use std::collections::HashMap;

use roxmltree::Node;

use crate::util::xml;

/// Parsed service interfaces, keyed by lowercased short-name.
pub type InterfaceMap = HashMap<String, ServiceInterface>;

/// A single SERVICE-INTERFACE definition.
#[derive(Debug, Clone)]
pub struct ServiceInterface {
    pub short_name: String,
    /// Event short-name (lowercased) → definition.
    pub events: HashMap<String, ServiceInterfaceEvent>,
    /// Field short-name (lowercased) → definition.
    pub fields: HashMap<String, ServiceInterfaceField>,
}

#[derive(Debug, Clone)]
pub struct ServiceInterfaceEvent {
    pub short_name: String,
    /// TYPE-TREF value (points to a data type).
    pub type_ref: String,
}

#[derive(Debug, Clone)]
pub struct ServiceInterfaceField {
    pub short_name: String,
    /// TYPE-TREF value (points to a data type).
    pub type_ref: String,
}

/// Parse `SERVICE-INTERFACE` elements from the `interfaces` AR-PACKAGE.
pub fn parse_interfaces(node: Node) -> Result<InterfaceMap, String> {
    let elements = xml::get_elements(node)?;
    let mut interfaces = HashMap::new();

    for (i, si_el) in elements
        .children()
        .filter(|n| n.tag_name().name() == "SERVICE-INTERFACE")
        .enumerate()
    {
        let si = parse_service_interface(si_el)
            .map_err(|e| format!("parse {i} SERVICE-INTERFACE: {e}"))?;
        interfaces.insert(si.short_name.to_lowercase(), si);
    }
    Ok(interfaces)
}

fn parse_service_interface(node: Node) -> Result<ServiceInterface, String> {
    let sn = xml::get_shortname(node)?;

    let mut si = ServiceInterface {
        short_name: sn.to_string(),
        events: HashMap::new(),
        fields: HashMap::new(),
    };

    // Events
    if let Some(es) = xml::find_child(node, "EVENTS") {
        for vdp in es
            .children()
            .filter(|n| n.tag_name().name() == "VARIABLE-DATA-PROTOTYPE")
        {
            let event_sn = xml::get_shortname(vdp)?;
            let typref = xml::require_child(vdp, "TYPE-TREF")?;
            si.events.insert(
                event_sn.to_lowercase(),
                ServiceInterfaceEvent {
                    short_name: event_sn.to_string(),
                    type_ref: typref.text().unwrap_or("").to_string(),
                },
            );
        }
    }

    // Fields
    if let Some(fss) = xml::find_child(node, "FIELDS") {
        for field in fss.children().filter(|n| n.tag_name().name() == "FIELD") {
            let field_sn = xml::get_shortname(field)?;
            let typref = xml::require_child(field, "TYPE-TREF")?;
            si.fields.insert(
                field_sn.to_lowercase(),
                ServiceInterfaceField {
                    short_name: field_sn.to_string(),
                    type_ref: typref.text().unwrap_or("").to_string(),
                },
            );
        }
    }

    if si.events.is_empty() && si.fields.is_empty() {
        return Err(format!("no EVENTS/FIELDS in {}", si.short_name));
    }

    Ok(si)
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const AP_IF_XML: &str = r#"
<AUTOSAR>
  <ELEMENTS>
    <SERVICE-INTERFACE>
      <SHORT-NAME>RadarSvc</SHORT-NAME>
      <EVENTS>
        <VARIABLE-DATA-PROTOTYPE>
          <SHORT-NAME>BrakeEvent</SHORT-NAME>
          <TYPE-TREF>/DataTypes/BrakeCmd</TYPE-TREF>
        </VARIABLE-DATA-PROTOTYPE>
      </EVENTS>
      <FIELDS>
        <FIELD>
          <SHORT-NAME>Speed</SHORT-NAME>
          <TYPE-TREF>/DataTypes/SpeedType</TYPE-TREF>
        </FIELD>
      </FIELDS>
    </SERVICE-INTERFACE>
  </ELEMENTS>
</AUTOSAR>
"#;

    #[test]
    fn parse_service_interface_with_event_and_field() {
        let doc = Document::parse(AP_IF_XML).unwrap();
        let interfaces = parse_interfaces(doc.root_element()).unwrap();

        let si = interfaces.get("radarsvc").unwrap();
        assert_eq!(si.short_name, "RadarSvc");

        let ev = si.events.get("brakeevent").unwrap();
        assert_eq!(ev.type_ref, "/DataTypes/BrakeCmd");

        let f = si.fields.get("speed").unwrap();
        assert_eq!(f.type_ref, "/DataTypes/SpeedType");
    }
}
