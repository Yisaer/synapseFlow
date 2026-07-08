//! AP IAUTOSAR deployment parser — extracts service-to-event/field
//! mappings with concrete numeric IDs.
//!
//! Corresponds to the Go `ap/parser/iautosar.go`.

use std::collections::HashMap;

use roxmltree::Node;

use crate::util::{convert, xml};

/// Parsed service deployment, keyed by service-ID.
pub type ServiceMap = HashMap<u16, Service>;

/// A deployed SOME/IP service instance.
#[derive(Debug, Clone)]
pub struct Service {
    pub short_name: String,
    pub service_id: u16,
    /// Dotted-path reference to the SERVICE-INTERFACE definition
    /// (e.g. `"/Interfaces/RadarSvc"`).
    pub service_interface_ref: String,
    /// event-ID → Event
    pub events: HashMap<u16, Event>,
    /// event-ID → FieldNotify
    pub field_notify: HashMap<u16, FieldNotify>,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub event_id: u16,
    pub short_name: String,
    /// Reference path to the corresponding SERVICE-INTERFACE event
    /// (e.g. `"/Interfaces/RadarSvc/BrakeEvent"`).
    pub event_ref: String,
}

#[derive(Debug, Clone)]
pub struct FieldNotify {
    pub event_id: u16,
    pub short_name: String,
    /// Reference path to the corresponding SERVICE-INTERFACE field.
    pub field_ref: String,
}

/// Parse `SOMEIP-SERVICE-INTERFACE-DEPLOYMENT` elements from the
/// `IAUTOSAR` AR-PACKAGE.
pub fn parse_i_autosar(node: Node) -> Result<ServiceMap, String> {
    let elements = xml::get_elements(node)?;
    let mut services = HashMap::new();

    for si_el in elements
        .children()
        .filter(|n| n.tag_name().name() == "SOMEIP-SERVICE-INTERFACE-DEPLOYMENT")
    {
        let svc = parse_service_deployment(si_el)?;
        services.insert(svc.service_id, svc);
    }
    Ok(services)
}

fn parse_service_deployment(node: Node) -> Result<Service, String> {
    let sn = xml::get_shortname(node)?;

    let sid_el = xml::require_child(node, "SERVICE-INTERFACE-ID")?;
    let service_id = parse_u16(sid_el, "SERVICE-INTERFACE-ID")?;

    let ref_el = xml::require_child(node, "SERVICE-INTERFACE-REF")?;
    let iface_ref = ref_el.text().unwrap_or("");
    if iface_ref.is_empty() {
        return Err(format!("empty SERVICE-INTERFACE-REF in {sn}"));
    }

    let events = parse_event_deployments(node, sn)?;
    let field_notify = parse_field_deployments(node, sn)?;

    if events.is_empty() && field_notify.is_empty() {
        return Err(format!("no events or fields in {sn}"));
    }

    let svc = Service {
        short_name: sn.to_string(),
        service_id,
        service_interface_ref: iface_ref.to_string(),
        events,
        field_notify,
    };

    // Validate: refs must start with the interface ref
    validate_refs(&svc)?;

    Ok(svc)
}

fn parse_event_deployments(node: Node, _svc_name: &str) -> Result<HashMap<u16, Event>, String> {
    let eds = match xml::find_child(node, "EVENT-DEPLOYMENTS") {
        Some(n) => n,
        None => return Ok(HashMap::new()),
    };

    let mut events = HashMap::new();
    for ed in eds
        .children()
        .filter(|n| n.tag_name().name() == "SOMEIP-EVENT-DEPLOYMENT")
    {
        let esn = xml::get_shortname(ed)?;
        let eid_el = xml::require_child(ed, "EVENT-ID")?;
        let event_id = parse_u16(eid_el, "EVENT-ID")?;
        let ref_el = xml::require_child(ed, "EVENT-REF")?;

        events.insert(
            event_id,
            Event {
                event_id,
                short_name: esn.to_string(),
                event_ref: ref_el.text().unwrap_or("").to_string(),
            },
        );
    }
    Ok(events)
}

fn parse_field_deployments(
    node: Node,
    svc_name: &str,
) -> Result<HashMap<u16, FieldNotify>, String> {
    let fds = match xml::find_child(node, "FIELD-DEPLOYMENTS") {
        Some(n) => n,
        None => return Ok(HashMap::new()),
    };

    let mut fields = HashMap::new();
    for fd in fds
        .children()
        .filter(|n| n.tag_name().name() == "SOMEIP-FIELD-DEPLOYMENT")
    {
        let fsn = xml::get_shortname(fd)?;
        let ref_el = xml::require_child(fd, "FIELD-REF")?;
        let field_ref = ref_el.text().unwrap_or("");
        if field_ref.is_empty() {
            return Err(format!("empty FIELD-REF in {svc_name} field {fsn}"));
        }

        if let Some(notifier) = xml::find_child(fd, "NOTIFIER") {
            let eid_el = xml::require_child(notifier, "EVENT-ID")?;
            let event_id = parse_u16(eid_el, "EVENT-ID")?;

            fields.insert(
                event_id,
                FieldNotify {
                    event_id,
                    short_name: fsn.to_string(),
                    field_ref: field_ref.to_string(),
                },
            );
        }
    }
    Ok(fields)
}

/// Ensure every event/field ref starts with the service's interface
/// ref (case-insensitive prefix check).
fn validate_refs(svc: &Service) -> Result<(), String> {
    let prefix = svc.service_interface_ref.to_lowercase();
    for event in svc.events.values() {
        if !event.event_ref.to_lowercase().starts_with(&prefix) {
            return Err(format!(
                "event ref mismatch in {}: interface={}, event={}",
                svc.short_name, svc.service_interface_ref, event.event_ref
            ));
        }
    }
    for field in svc.field_notify.values() {
        if !field.field_ref.to_lowercase().starts_with(&prefix) {
            return Err(format!(
                "field ref mismatch in {}: interface={}, field={}",
                svc.short_name, svc.service_interface_ref, field.field_ref
            ));
        }
    }
    Ok(())
}

fn parse_u16(node: Node, tag: &str) -> Result<u16, String> {
    let raw = node.text().unwrap_or("");
    convert::to_u16(raw).map_err(|e| format!("invalid {tag}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const AP_IAUTOSAR_XML: &str = r#"
<AUTOSAR>
  <ELEMENTS>
    <SOMEIP-SERVICE-INTERFACE-DEPLOYMENT>
      <SHORT-NAME>RadarSvcDeployment</SHORT-NAME>
      <SERVICE-INTERFACE-ID>1234</SERVICE-INTERFACE-ID>
      <SERVICE-INTERFACE-REF>/Interfaces/RadarSvc</SERVICE-INTERFACE-REF>
      <EVENT-DEPLOYMENTS>
        <SOMEIP-EVENT-DEPLOYMENT>
          <SHORT-NAME>BrakeEventDeploy</SHORT-NAME>
          <EVENT-ID>5</EVENT-ID>
          <EVENT-REF>/Interfaces/RadarSvc/BrakeEvent</EVENT-REF>
        </SOMEIP-EVENT-DEPLOYMENT>
      </EVENT-DEPLOYMENTS>
      <FIELD-DEPLOYMENTS>
        <SOMEIP-FIELD-DEPLOYMENT>
          <SHORT-NAME>SpeedFieldDeploy</SHORT-NAME>
          <FIELD-REF>/Interfaces/RadarSvc/Speed</FIELD-REF>
          <NOTIFIER>
            <EVENT-ID>10</EVENT-ID>
          </NOTIFIER>
        </SOMEIP-FIELD-DEPLOYMENT>
      </FIELD-DEPLOYMENTS>
    </SOMEIP-SERVICE-INTERFACE-DEPLOYMENT>
  </ELEMENTS>
</AUTOSAR>
"#;

    #[test]
    fn parse_service_deployment_with_event_and_field() {
        let doc = Document::parse(AP_IAUTOSAR_XML).unwrap();
        let services = parse_i_autosar(doc.root_element()).unwrap();

        let svc = services.get(&1234).unwrap();
        assert_eq!(svc.service_interface_ref, "/Interfaces/RadarSvc");

        let ev = svc.events.get(&5).unwrap();
        assert_eq!(ev.short_name, "BrakeEventDeploy");
        assert_eq!(ev.event_ref, "/Interfaces/RadarSvc/BrakeEvent");

        let fnf = svc.field_notify.get(&10).unwrap();
        assert_eq!(fnf.short_name, "SpeedFieldDeploy");
        assert_eq!(fnf.field_ref, "/Interfaces/RadarSvc/Speed");
    }
}
