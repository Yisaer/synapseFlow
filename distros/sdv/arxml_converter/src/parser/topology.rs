//! CP Topology parser — extracts service-ID, header-ID, and
//! PDU-triggering mappings from the `Topology` AR-PACKAGE.
//!
//! Corresponds to the Go `cp/parser/topology/` package.

use std::collections::HashMap;

use roxmltree::Node;

use crate::util::{convert, xml};

/// Parsed topology metadata used by the CP lookup chain.
#[derive(Debug, Default)]
pub struct TopologyParser {
    /// `service_ID (u16)` → `short_name`
    pub service_id_map: HashMap<u16, String>,
    /// `header_ID (u32)` → `PDU_TRIGGERING_REF` text
    pub header_id_ref: HashMap<u32, String>,
    /// `PDU_TRIGGERING.short_name` → `I-PDU-REF` text
    pub pdu_triggering_ref: HashMap<String, String>,
}

impl TopologyParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_service_id_map(&self) -> &HashMap<u16, String> {
        &self.service_id_map
    }

    pub fn get_header_ref(&self) -> &HashMap<u32, String> {
        &self.header_id_ref
    }

    pub fn get_pdu_triggering_ref(&self) -> &HashMap<String, String> {
        &self.pdu_triggering_ref
    }

    /// Parse the `Topology` AR-PACKAGE element.
    pub fn parse_topology(&mut self, node: Node) -> Result<(), String> {
        let ar_packages = xml::require_child(node, "AR-PACKAGES")?;
        let cluster = find_ar_package_by_name(ar_packages, "Clusters")
            .ok_or("AR-PACKAGE 'Clusters' not found")?;
        self.parse_cluster(cluster)
    }

    // ---- cluster traversal ----

    fn parse_cluster(&mut self, cluster_ar_package: Node) -> Result<(), String> {
        let elements = xml::get_elements(cluster_ar_package)?;
        let eth_cluster = xml::require_child(elements, "ETHERNET-CLUSTER")?;
        let eth_var = xml::require_child(eth_cluster, "ETHERNET-CLUSTER-VARIANTS")?;
        let eth_cond = xml::require_child(eth_var, "ETHERNET-CLUSTER-CONDITIONAL")?;
        let phy_channels = xml::require_child(eth_cond, "PHYSICAL-CHANNELS")?;

        for (i, eth_phy_channel) in phy_channels
            .children()
            .filter(|n| n.tag_name().name() == "ETHERNET-PHYSICAL-CHANNEL")
            .enumerate()
        {
            self.parse_ethernet_physical_channel(eth_phy_channel)
                .map_err(|e| format!("parse {i} ETHERNET-PHYSICAL-CHANNEL: {e}"))?;
        }
        Ok(())
    }

    fn parse_ethernet_physical_channel(&mut self, node: Node) -> Result<(), String> {
        // SO-AD-CONFIG branch
        if let Some(so_ad) = xml::find_child(node, "SO-AD-CONFIG") {
            self.parse_so_ad_config(so_ad)?;
        }
        // PDU-TRIGGERINGS branch
        if let Some(pdu_trigs) = xml::find_child(node, "PDU-TRIGGERINGS") {
            self.parse_pdu_triggerings(pdu_trigs)?;
        }
        Ok(())
    }

    fn parse_so_ad_config(&mut self, so_ad: Node) -> Result<(), String> {
        // --- service IDs ---
        if let Some(sockets) = xml::find_child(so_ad, "SOCKET-ADDRESSS") {
            for (i, socket) in sockets
                .children()
                .filter(|n| n.tag_name().name() == "SOCKET-ADDRESS")
                .enumerate()
            {
                self.parse_socket_address(socket)
                    .map_err(|e| format!("parse {i} SOCKET-ADDRESS: {e}"))?;
            }
        }

        // --- header IDs ---
        if let Some(bundles) = xml::find_child(so_ad, "CONNECTION-BUNDLES") {
            for (i, bundle) in bundles
                .children()
                .filter(|n| n.tag_name().name() == "SOCKET-CONNECTION-BUNDLE")
                .enumerate()
            {
                self.parse_socket_connection_bundle(bundle)
                    .map_err(|e| format!("parse {i} SOCKET-CONNECTION-BUNDLE: {e}"))?;
            }
        }
        Ok(())
    }

    fn parse_socket_address(&mut self, node: Node) -> Result<(), String> {
        let app_endpoint = match xml::find_child(node, "APPLICATION-ENDPOINT") {
            Some(n) => n,
            None => return Ok(()),
        };
        let provided_instances = match xml::find_child(app_endpoint, "PROVIDED-SERVICE-INSTANCES") {
            Some(n) => n,
            None => return Ok(()),
        };
        for (i, instance) in provided_instances
            .children()
            .filter(|n| n.tag_name().name() == "PROVIDED-SERVICE-INSTANCE")
            .enumerate()
        {
            self.parse_provided_service_instance(instance)
                .map_err(|e| format!("parse {i} PROVIDED-SERVICE-INSTANCE: {e}"))?;
        }
        Ok(())
    }

    fn parse_provided_service_instance(&mut self, node: Node) -> Result<(), String> {
        let sn = xml::get_shortname(node)?;
        let service_id_el = match xml::find_child(node, "SERVICE-IDENTIFIER") {
            Some(n) => n,
            None => return Ok(()),
        };
        let id = convert::to_u16(service_id_el.text().unwrap_or(""))
            .map_err(|e| format!("invalid SERVICE-IDENTIFIER: {e}"))?;
        self.service_id_map.insert(id, sn.to_string());
        Ok(())
    }

    fn parse_socket_connection_bundle(&mut self, node: Node) -> Result<(), String> {
        let bundled = match xml::find_child(node, "BUNDLED-CONNECTIONS") {
            Some(n) => n,
            None => return Ok(()),
        };
        let sock_conn = match xml::find_child(bundled, "SOCKET-CONNECTION") {
            Some(n) => n,
            None => return Ok(()),
        };
        let pdus = match xml::find_child(sock_conn, "PDUS") {
            Some(n) => n,
            None => return Ok(()),
        };

        for (i, scipdui) in pdus
            .children()
            .filter(|n| n.tag_name().name() == "SOCKET-CONNECTION-IPDU-IDENTIFIER")
            .enumerate()
        {
            self.parse_socket_connection_ipdu_identifier(scipdui)
                .map_err(|e| format!("parse {i} SOCKET-CONNECTION-IPDU-IDENTIFIER: {e}"))?;
        }
        Ok(())
    }

    fn parse_socket_connection_ipdu_identifier(&mut self, node: Node) -> Result<(), String> {
        let header_id_el = xml::require_child(node, "HEADER-ID")?;
        let header_id = convert::to_u32(header_id_el.text().unwrap_or(""))
            .map_err(|e| format!("invalid HEADER-ID: {e}"))?;

        let pdu_ref_el = xml::require_child(node, "PDU-TRIGGERING-REF")?;
        let pdu_ref = pdu_ref_el.text().unwrap_or("");

        // Skip PDU-TRIGGERING-REFs that contain "return"
        if !pdu_ref.contains("return") {
            self.header_id_ref.insert(header_id, pdu_ref.to_string());
        }
        Ok(())
    }

    // ---- PDU Triggerings ----

    fn parse_pdu_triggerings(&mut self, node: Node) -> Result<(), String> {
        for (i, trig) in node
            .children()
            .filter(|n| n.tag_name().name() == "PDU-TRIGGERING")
            .enumerate()
        {
            self.parse_pdu_triggering(trig)
                .map_err(|e| format!("parse {i} PDU-TRIGGERING: {e}"))?;
        }
        Ok(())
    }

    fn parse_pdu_triggering(&mut self, node: Node) -> Result<(), String> {
        let sn = xml::get_shortname(node)?;
        let ipdu_ref = xml::require_child(node, "I-PDU-REF")?;
        let ipdu_text = ipdu_ref.text().unwrap_or("");
        self.pdu_triggering_ref
            .insert(sn.to_string(), ipdu_text.to_string());
        Ok(())
    }
}

// ---- helper ----

fn find_ar_package_by_name<'a>(node: Node<'a, 'a>, name: &str) -> Option<Node<'a, 'a>> {
    node.children()
        .filter(|c| c.tag_name().name() == "AR-PACKAGE")
        .find(|c| xml::child_text(*c, "SHORT-NAME") == Some(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const TOPO_XML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>Clusters</SHORT-NAME>
      <ELEMENTS>
        <ETHERNET-CLUSTER>
          <ETHERNET-CLUSTER-VARIANTS>
            <ETHERNET-CLUSTER-CONDITIONAL>
              <PHYSICAL-CHANNELS>
                <ETHERNET-PHYSICAL-CHANNEL>
                  <SO-AD-CONFIG>
                    <SOCKET-ADDRESSS>
                      <SOCKET-ADDRESS>
                        <APPLICATION-ENDPOINT>
                          <PROVIDED-SERVICE-INSTANCES>
                            <PROVIDED-SERVICE-INSTANCE>
                              <SHORT-NAME>svc1</SHORT-NAME>
                              <SERVICE-IDENTIFIER>42</SERVICE-IDENTIFIER>
                            </PROVIDED-SERVICE-INSTANCE>
                          </PROVIDED-SERVICE-INSTANCES>
                        </APPLICATION-ENDPOINT>
                      </SOCKET-ADDRESS>
                    </SOCKET-ADDRESSS>
                    <CONNECTION-BUNDLES>
                      <SOCKET-CONNECTION-BUNDLE>
                        <SHORT-NAME>bundle1</SHORT-NAME>
                        <BUNDLED-CONNECTIONS>
                          <SOCKET-CONNECTION>
                            <PDUS>
                              <SOCKET-CONNECTION-IPDU-IDENTIFIER>
                                <HEADER-ID>100</HEADER-ID>
                                <PDU-TRIGGERING-REF>/pdu/trig1</PDU-TRIGGERING-REF>
                              </SOCKET-CONNECTION-IPDU-IDENTIFIER>
                            </PDUS>
                          </SOCKET-CONNECTION>
                        </BUNDLED-CONNECTIONS>
                      </SOCKET-CONNECTION-BUNDLE>
                    </CONNECTION-BUNDLES>
                  </SO-AD-CONFIG>
                  <PDU-TRIGGERINGS>
                    <PDU-TRIGGERING>
                      <SHORT-NAME>trig1</SHORT-NAME>
                      <I-PDU-REF>/ipdu/SignalPDU</I-PDU-REF>
                    </PDU-TRIGGERING>
                  </PDU-TRIGGERINGS>
                </ETHERNET-PHYSICAL-CHANNEL>
              </PHYSICAL-CHANNELS>
            </ETHERNET-CLUSTER-CONDITIONAL>
          </ETHERNET-CLUSTER-VARIANTS>
        </ETHERNET-CLUSTER>
      </ELEMENTS>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    #[test]
    fn parse_service_id() {
        let doc = Document::parse(TOPO_XML).unwrap();
        let mut parser = TopologyParser::new();
        parser.parse_topology(doc.root_element()).unwrap();

        assert_eq!(parser.service_id_map.get(&42).unwrap(), "svc1");
    }

    #[test]
    fn parse_header_id() {
        let doc = Document::parse(TOPO_XML).unwrap();
        let mut parser = TopologyParser::new();
        parser.parse_topology(doc.root_element()).unwrap();

        assert_eq!(parser.header_id_ref.get(&100).unwrap(), "/pdu/trig1");
    }

    #[test]
    fn parse_pdu_triggering() {
        let doc = Document::parse(TOPO_XML).unwrap();
        let mut parser = TopologyParser::new();
        parser.parse_topology(doc.root_element()).unwrap();

        assert_eq!(
            parser.pdu_triggering_ref.get("trig1").unwrap(),
            "/ipdu/SignalPDU"
        );
    }
}
