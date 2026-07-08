//! CP System parser — extracts operation-ref / data-element mappings
//! from the `System` AR-PACKAGE.
//!
//! Corresponds to the Go `cp/parser/system/` package.

use std::collections::HashMap;

use roxmltree::Node;

use crate::util::xml;

/// Parsed system-level mappings used by the CP lookup chain.
///
/// Each entry maps a signal reference (key) to an operation / data-element
/// reference (value).
#[derive(Debug, Default)]
pub struct SystemParser {
    /// `SYSTEM-SIGNAL-REF` text → `TARGET-OPERATION-REF` or
    /// `TARGET-DATA-PROTOTYPE-REF` text.
    pub operation_ref: HashMap<String, String>,
}

impl SystemParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_operation_ref(&self) -> &HashMap<String, String> {
        &self.operation_ref
    }

    /// Parse the `System` AR-PACKAGE element.
    pub fn parse_system(&mut self, node: Node) -> Result<(), String> {
        let elements = xml::get_elements(node)?;
        let system_el = xml::require_child(elements, "SYSTEM")?;

        let sn = xml::get_shortname(system_el)?;
        let category = xml::child_text(system_el, "CATEGORY");

        if sn == "SystemDescription" || category == Some("SYSTEM_DESCRIPTION") {
            return self.parse_system_mapping(system_el);
        }

        Err(format!("unsupported system type: {sn}"))
    }

    fn parse_system_mapping(&mut self, system_el: Node) -> Result<(), String> {
        let mappings = xml::require_child(system_el, "MAPPINGS")?;
        let system_mapping = xml::require_child(mappings, "SYSTEM-MAPPING")?;
        let data_mappings = xml::require_child(system_mapping, "DATA-MAPPINGS")?;

        // CLIENT-SERVER-TO-SIGNAL-MAPPING
        for (i, cs_to_sig) in data_mappings
            .children()
            .filter(|n| n.tag_name().name() == "CLIENT-SERVER-TO-SIGNAL-MAPPING")
            .enumerate()
        {
            self.parse_client_server_to_signal_mapping(cs_to_sig)
                .map_err(|e| format!("parse {i} CLIENT-SERVER-TO-SIGNAL-MAPPING: {e}"))?;
        }

        // SENDER-RECEIVER-TO-SIGNAL-MAPPING
        for (i, sr_to_sig) in data_mappings
            .children()
            .filter(|n| n.tag_name().name() == "SENDER-RECEIVER-TO-SIGNAL-MAPPING")
            .enumerate()
        {
            self.parse_sender_receiver_to_signal_mapping(sr_to_sig)
                .map_err(|e| format!("parse {i} SENDER-RECEIVER-TO-SIGNAL-MAPPING: {e}"))?;
        }

        Ok(())
    }

    fn parse_client_server_to_signal_mapping(&mut self, node: Node) -> Result<(), String> {
        let call_signal_ref = match xml::find_child(node, "CALL-SIGNAL-REF") {
            Some(n) => n,
            None => return Ok(()),
        };
        let cs_op_iref = match xml::find_child(node, "CLIENT-SERVER-OPERATION-IREF") {
            Some(n) => n,
            None => return Ok(()),
        };
        let target_op_ref = match xml::find_child(cs_op_iref, "TARGET-OPERATION-REF") {
            Some(n) => n,
            None => return Ok(()),
        };

        self.operation_ref.insert(
            call_signal_ref.text().unwrap_or("").to_string(),
            target_op_ref.text().unwrap_or("").to_string(),
        );
        Ok(())
    }

    fn parse_sender_receiver_to_signal_mapping(&mut self, node: Node) -> Result<(), String> {
        let sr = match xml::find_child(node, "SYSTEM-SIGNAL-REF") {
            Some(n) => n,
            None => return Ok(()),
        };
        let data_elem_iref = match xml::find_child(node, "DATA-ELEMENT-IREF") {
            Some(n) => n,
            None => return Ok(()),
        };
        let target_proto_ref = match xml::find_child(data_elem_iref, "TARGET-DATA-PROTOTYPE-REF") {
            Some(n) => n,
            None => return Ok(()),
        };

        self.operation_ref.insert(
            sr.text().unwrap_or("").to_string(),
            target_proto_ref.text().unwrap_or("").to_string(),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const SYS_XML: &str = r#"
<AUTOSAR>
  <ELEMENTS>
    <SYSTEM>
      <SHORT-NAME>SystemDescription</SHORT-NAME>
      <MAPPINGS>
        <SYSTEM-MAPPING>
          <DATA-MAPPINGS>
            <SENDER-RECEIVER-TO-SIGNAL-MAPPING>
              <SYSTEM-SIGNAL-REF>/Sig/SS1</SYSTEM-SIGNAL-REF>
              <DATA-ELEMENT-IREF>
                <TARGET-DATA-PROTOTYPE-REF>/Data/Speed</TARGET-DATA-PROTOTYPE-REF>
              </DATA-ELEMENT-IREF>
            </SENDER-RECEIVER-TO-SIGNAL-MAPPING>
          </DATA-MAPPINGS>
        </SYSTEM-MAPPING>
      </MAPPINGS>
    </SYSTEM>
  </ELEMENTS>
</AUTOSAR>
"#;

    #[test]
    fn parse_sender_receiver_mapping() {
        let doc = Document::parse(SYS_XML).unwrap();
        let mut parser = SystemParser::new();
        parser.parse_system(doc.root_element()).unwrap();

        assert_eq!(parser.operation_ref.get("/Sig/SS1").unwrap(), "/Data/Speed");
    }
}
