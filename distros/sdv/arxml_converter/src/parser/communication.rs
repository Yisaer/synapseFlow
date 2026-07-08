//! CP Communication parser — extracts PDU-to-signal and signal-to-system-signal
//! mappings from the `Communication` AR-PACKAGE.
//!
//! Corresponds to the Go `cp/parser/communication/` package.

use std::collections::HashMap;

use roxmltree::Node;

use crate::util::xml;

/// Parsed communication mappings used by the CP lookup chain.
#[derive(Debug, Default)]
pub struct CommunicationParser {
    /// `I-SIGNAL-I-PDU.short_name` → `I-SIGNAL-REF` text
    pub pdu_ref_map: HashMap<String, String>,
    /// `I-SIGNAL.short_name` → `SYSTEM-SIGNAL-REF` text
    pub signal_ref_map: HashMap<String, String>,
}

impl CommunicationParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_pdu_ref_map(&self) -> &HashMap<String, String> {
        &self.pdu_ref_map
    }

    pub fn get_signal_ref_map(&self) -> &HashMap<String, String> {
        &self.signal_ref_map
    }

    /// Parse the `Communication` AR-PACKAGE element.
    pub fn parse_communication(&mut self, node: Node) -> Result<(), String> {
        let ar_packages = xml::require_child(node, "AR-PACKAGES")?;

        let pdus_el =
            find_ar_package_by_name(ar_packages, "PDUs").ok_or("no 'PDUs' AR-PACKAGE found")?;
        let signals_el = find_ar_package_by_name(ar_packages, "Signals")
            .ok_or("no 'Signals' AR-PACKAGE found")?;

        self.parse_pdus(pdus_el)?;
        self.parse_signals(signals_el)?;
        Ok(())
    }

    fn parse_pdus(&mut self, node: Node) -> Result<(), String> {
        let elements = xml::get_elements(node)?;
        for (i, isignal_ipdu) in elements
            .children()
            .filter(|n| n.tag_name().name() == "I-SIGNAL-I-PDU")
            .enumerate()
        {
            self.parse_i_signal_ipdu(isignal_ipdu)
                .map_err(|e| format!("parse {i} I-SIGNAL-I-PDU: {e}"))?;
        }
        Ok(())
    }

    fn parse_i_signal_ipdu(&mut self, node: Node) -> Result<(), String> {
        let sn = xml::get_shortname(node)?;

        let mappings = match xml::find_child(node, "I-SIGNAL-TO-PDU-MAPPINGS") {
            Some(n) => n,
            None => return Ok(()),
        };
        let mapping = match xml::find_child(mappings, "I-SIGNAL-TO-I-PDU-MAPPING") {
            Some(n) => n,
            None => return Ok(()),
        };
        let i_signal_ref = match xml::find_child(mapping, "I-SIGNAL-REF") {
            Some(n) => n,
            None => return Ok(()),
        };

        self.pdu_ref_map.insert(
            sn.to_string(),
            i_signal_ref.text().unwrap_or("").to_string(),
        );
        Ok(())
    }

    fn parse_signals(&mut self, node: Node) -> Result<(), String> {
        let elements = xml::get_elements(node)?;
        for (i, isignal) in elements
            .children()
            .filter(|n| n.tag_name().name() == "I-SIGNAL")
            .enumerate()
        {
            self.parse_i_signal(isignal)
                .map_err(|e| format!("parse {i} I-SIGNAL: {e}"))?;
        }
        Ok(())
    }

    fn parse_i_signal(&mut self, node: Node) -> Result<(), String> {
        let sn = xml::get_shortname(node)?;

        let system_signal_ref = match xml::find_child(node, "SYSTEM-SIGNAL-REF") {
            Some(n) => n,
            None => return Ok(()),
        };

        self.signal_ref_map.insert(
            sn.to_string(),
            system_signal_ref.text().unwrap_or("").to_string(),
        );
        Ok(())
    }
}

fn find_ar_package_by_name<'a>(node: Node<'a, 'a>, name: &str) -> Option<Node<'a, 'a>> {
    node.children()
        .filter(|c| c.tag_name().name() == "AR-PACKAGE")
        .find(|c| xml::child_text(*c, "SHORT-NAME") == Some(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const COMM_XML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>PDUs</SHORT-NAME>
      <ELEMENTS>
        <I-SIGNAL-I-PDU>
          <SHORT-NAME>SignalPDU</SHORT-NAME>
          <I-SIGNAL-TO-PDU-MAPPINGS>
            <I-SIGNAL-TO-I-PDU-MAPPING>
              <I-SIGNAL-REF>/Signals/SigA</I-SIGNAL-REF>
            </I-SIGNAL-TO-I-PDU-MAPPING>
          </I-SIGNAL-TO-PDU-MAPPINGS>
        </I-SIGNAL-I-PDU>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>Signals</SHORT-NAME>
      <ELEMENTS>
        <I-SIGNAL>
          <SHORT-NAME>SigA</SHORT-NAME>
          <SYSTEM-SIGNAL-REF>/SystemSig/SS1</SYSTEM-SIGNAL-REF>
        </I-SIGNAL>
      </ELEMENTS>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    #[test]
    fn parse_pdu_to_signal_mapping() {
        let doc = Document::parse(COMM_XML).unwrap();
        let mut parser = CommunicationParser::new();
        parser.parse_communication(doc.root_element()).unwrap();

        assert_eq!(
            parser.pdu_ref_map.get("SignalPDU").unwrap(),
            "/Signals/SigA"
        );
    }

    #[test]
    fn parse_signal_to_system_signal() {
        let doc = Document::parse(COMM_XML).unwrap();
        let mut parser = CommunicationParser::new();
        parser.parse_communication(doc.root_element()).unwrap();

        assert_eq!(parser.signal_ref_map.get("SigA").unwrap(), "/SystemSig/SS1");
    }
}
