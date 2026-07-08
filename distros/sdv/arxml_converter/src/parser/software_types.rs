//! CP SoftwareTypes parser — extracts client-server and sender-receiver
//! interface refs from the `SoftwareTypes` AR-PACKAGE.
//!
//! Corresponds to the Go `cp/parser/softwareTypes/` package.

use std::collections::HashMap;

use roxmltree::Node;

use crate::util::xml;

/// Two-level map: client-server interface short-name → operation
/// short-name → TYPE-TREF text.
#[derive(Debug, Default)]
pub struct SoftwareTypesParser {
    pub interface_ref_map: HashMap<String, HashMap<String, String>>,
}

impl SoftwareTypesParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_interface_ref_map(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.interface_ref_map
    }

    /// Parse the `SoftwareTypes` AR-PACKAGE element.
    pub fn parse_software_types(&mut self, node: Node) -> Result<(), String> {
        let ar_packages = xml::require_child(node, "AR-PACKAGES")?;
        let interfaces_el = find_ar_package_by_name(ar_packages, "Interfaces")
            .ok_or("no 'Interfaces' AR-PACKAGE found")?;
        self.parse_interfaces(interfaces_el)
    }

    fn parse_interfaces(&mut self, node: Node) -> Result<(), String> {
        let elements = xml::get_elements(node)?;

        // CLIENT-SERVER-INTERFACE
        for (i, csi) in elements
            .children()
            .filter(|n| n.tag_name().name() == "CLIENT-SERVER-INTERFACE")
            .enumerate()
        {
            self.parse_client_server_interface(csi)
                .map_err(|e| format!("parse {i} CLIENT-SERVER-INTERFACE: {e}"))?;
        }

        // SENDER-RECEIVER-INTERFACE
        for (i, sri) in elements
            .children()
            .filter(|n| n.tag_name().name() == "SENDER-RECEIVER-INTERFACE")
            .enumerate()
        {
            self.parse_sender_receiver_interface(sri)
                .map_err(|e| format!("parse {i} SENDER-RECEIVER-INTERFACE: {e}"))?;
        }

        Ok(())
    }

    fn parse_client_server_interface(&mut self, node: Node) -> Result<(), String> {
        let sn = xml::get_shortname(node)?;

        let operations = match xml::find_child(node, "OPERATIONS") {
            Some(n) => n,
            None => return Ok(()),
        };

        for (i, cso) in operations
            .children()
            .filter(|n| n.tag_name().name() == "CLIENT-SERVER-OPERATION")
            .enumerate()
        {
            let (cso_sn, tref) = self
                .parse_client_server_operation(cso)
                .map_err(|e| format!("parse {i} CLIENT-SERVER-OPERATION: {e}"))?;

            if !cso_sn.is_empty() && !tref.is_empty() {
                self.add_mapping(sn, &cso_sn, &tref);
            }
        }
        Ok(())
    }

    fn parse_client_server_operation(&self, node: Node) -> Result<(String, String), String> {
        let sn = xml::get_shortname(node)?;

        let arguments = match xml::find_child(node, "ARGUMENTS") {
            Some(n) => n,
            None => return Ok((String::new(), String::new())),
        };

        for argument in arguments
            .children()
            .filter(|n| n.tag_name().name() == "ARGUMENT-DATA-PROTOTYPE")
        {
            if xml::child_text(argument, "DIRECTION") == Some("IN") {
                let type_ref = xml::require_child(argument, "TYPE-TREF")?;
                return Ok((sn.to_string(), type_ref.text().unwrap_or("").to_string()));
            }
        }

        Ok((String::new(), String::new()))
    }

    fn parse_sender_receiver_interface(&mut self, node: Node) -> Result<(), String> {
        let sn = xml::get_shortname(node)?;

        let data_elements = match xml::find_child(node, "DATA-ELEMENTS") {
            Some(n) => n,
            None => return Ok(()),
        };

        for (i, vdp) in data_elements
            .children()
            .filter(|n| n.tag_name().name() == "VARIABLE-DATA-PROTOTYPE")
            .enumerate()
        {
            let (k, v) = self
                .parse_variable_data_prototype(vdp)
                .map_err(|e| format!("parse {i} VARIABLE-DATA-PROTOTYPE: {e}"))?;
            if !k.is_empty() && !v.is_empty() {
                self.add_mapping(sn, &k, &v);
            }
        }
        Ok(())
    }

    fn parse_variable_data_prototype(&self, node: Node) -> Result<(String, String), String> {
        let sn = xml::get_shortname(node)?;

        let type_ref = match xml::find_child(node, "TYPE-TREF") {
            Some(n) => n,
            None => return Ok((String::new(), String::new())),
        };

        Ok((sn.to_string(), type_ref.text().unwrap_or("").to_string()))
    }

    fn add_mapping(&mut self, csi: &str, op: &str, tref: &str) {
        self.interface_ref_map
            .entry(csi.to_string())
            .or_default()
            .insert(op.to_string(), tref.to_string());
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

    const SW_XML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>Interfaces</SHORT-NAME>
      <ELEMENTS>
        <SENDER-RECEIVER-INTERFACE>
          <SHORT-NAME>SensorIf</SHORT-NAME>
          <DATA-ELEMENTS>
            <VARIABLE-DATA-PROTOTYPE>
              <SHORT-NAME>speed</SHORT-NAME>
              <TYPE-TREF>/dt/SpeedType</TYPE-TREF>
            </VARIABLE-DATA-PROTOTYPE>
          </DATA-ELEMENTS>
        </SENDER-RECEIVER-INTERFACE>
      </ELEMENTS>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    #[test]
    fn parse_sender_receiver_interface_ref() {
        let doc = Document::parse(SW_XML).unwrap();
        let mut parser = SoftwareTypesParser::new();
        parser.parse_software_types(doc.root_element()).unwrap();

        let iface = parser.interface_ref_map.get("SensorIf").unwrap();
        assert_eq!(iface.get("speed").unwrap(), "/dt/SpeedType");
    }
}
