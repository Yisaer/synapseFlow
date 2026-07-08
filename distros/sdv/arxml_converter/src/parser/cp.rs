//! CP (Classic Platform) main parser — orchestrates all sub-parsers and
//! implements the complete service-ID → data-type lookup chain.
//!
//! Corresponds to the Go `cp/parser/parser.go`.

use std::collections::HashMap;

use roxmltree::{Document, Node};

use crate::ast::types::DataType;
use crate::parser::{
    communication::CommunicationParser, datatypes::DataTypesParser,
    software_types::SoftwareTypesParser, system::SystemParser, topology::TopologyParser,
};
use crate::util::{convert, xml};

/// Orchestrator for the CP ARXML parsing pipeline.
///
/// After [`parse`](CpParser::parse), call
/// [`resolve_type`](CpParser::resolve_type) to follow the full
/// service-ID → header-ID → … → DataType lookup chain.
#[derive(Debug)]
pub struct CpParser {
    data_types_parser: DataTypesParser,
    topology_parser: TopologyParser,
    communication_parser: CommunicationParser,
    system_parser: SystemParser,
    software_types_parser: SoftwareTypesParser,
    tp_config_parser: Option<TpConfigParser>,
}

impl CpParser {
    pub fn new() -> Self {
        Self {
            data_types_parser: DataTypesParser::new(HashMap::new()),
            topology_parser: TopologyParser::new(),
            communication_parser: CommunicationParser::new(),
            system_parser: SystemParser::new(),
            software_types_parser: SoftwareTypesParser::new(),
            tp_config_parser: None,
        }
    }

    // ---- public entry points ----

    /// Parse a complete AUTOSAR ARXML document (CP).
    ///
    /// Traverses `AUTOSAR → AR-PACKAGES → AR-PACKAGE[…]` to locate the
    /// six required sub-packages, then delegates to each sub-parser.
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

        // Locate every required AR-PACKAGE
        let data_types_el = find_ar_package_by_name(ar_packages, "DataTypes")
            .ok_or("missing 'DataTypes' AR-PACKAGE")?;
        let mapping_sets_el = find_ar_package_by_name(ar_packages, "DataTypeMappingSets")
            .ok_or("missing 'DataTypeMappingSets' AR-PACKAGE")?;
        let topology_el = find_ar_package_by_name(ar_packages, "Topology")
            .ok_or("missing 'Topology' AR-PACKAGE")?;
        let communication_el = find_ar_package_by_name(ar_packages, "Communication")
            .ok_or("missing 'Communication' AR-PACKAGE")?;
        let system_el =
            find_ar_package_by_name(ar_packages, "System").ok_or("missing 'System' AR-PACKAGE")?;
        let sw_types_el = find_ar_package_by_name(ar_packages, "SoftwareTypes")
            .ok_or("missing 'SoftwareTypes' AR-PACKAGE")?;

        // 1) DataTypeMappingSets must be parsed first (data types need it)
        let mappings = parse_data_type_mapping_sets(mapping_sets_el)?;
        self.data_types_parser = DataTypesParser::new(mappings);

        // 2) DataTypes
        self.data_types_parser.parse_data_types(data_types_el)?;

        // 3) Topology
        self.topology_parser.parse_topology(topology_el)?;

        // 4) Communication
        self.communication_parser
            .parse_communication(communication_el)?;

        // 5) System
        self.system_parser.parse_system(system_el)?;

        // 6) SoftwareTypes
        self.software_types_parser
            .parse_software_types(sw_types_el)?;

        // 7) TpConfig (optional)
        if let Some(tp_config_el) = find_ar_package_by_name(ar_packages, "TpConfig") {
            let mut tp = TpConfigParser::new();
            tp.parse_tp_config(tp_config_el)?;
            self.tp_config_parser = Some(tp);
        }

        Ok(())
    }

    /// Get a reference to the parsed application data types map
    /// (lowercased short-name → DataType).
    pub fn application_data_types(&self) -> &HashMap<String, DataType> {
        &self.data_types_parser.application_data_types
    }

    /// Build a merged type map: application data types fall back to
    /// their implementation type counterparts via [`DataTypeMappingSet`].
    /// Use this for the binary decoder so that ADT references resolve
    /// to actual type definitions.
    pub fn merged_data_types(&self) -> HashMap<String, DataType> {
        self.data_types_parser.merged_data_types()
    }

    /// Access the topology parser (for service name lookups).
    pub fn topology_parser(&self) -> &TopologyParser {
        &self.topology_parser
    }

    /// Follow the full CP lookup chain to resolve a `(serviceID, headerID)`
    /// pair into the corresponding [`DataType`].
    ///
    /// The chain:
    /// `serviceID` → `headerID` → `PDU_TRIGGERING_REF` →
    /// `I_SIGNAL_I_PDU` → `I_SIGNAL_REF` → `PDU_REF` →
    /// `SYSTEM_SIGNAL_REF` → `OPERATION_REF` →
    /// `INTERFACE_REF` → `TYPE_TREF` → **DataType**.
    pub fn resolve_type(&self, service_id: u16, header_id: u32) -> Result<&DataType, String> {
        // 1. Verify service ID exists
        self.topology_parser
            .service_id_map
            .get(&service_id)
            .ok_or_else(|| format!("no service found for ID {service_id}"))?;

        // 2. headerID → PDUTRIGGERINGREF
        let pdu_triggering_ref = self.pdu_triggering_ref_by_header_id(header_id)?;

        // 3. Optional TP config override
        let i_signal_ipdu_short_name = self
            .tp_sdu_ref_by_pdu_triggering_ref(pdu_triggering_ref)
            .unwrap_or(pdu_triggering_ref);

        // 4. I-Signal-IPDU short name → I-SIGNAL-REF
        let i_signal_ref = self.i_signal_ref_by_i_signal_ipdu(i_signal_ipdu_short_name)?;

        // 5. extractLast(ISIGNALREF) → Communication PDURef
        let comm_pdu_ref = {
            let key = convert::extract_last(&i_signal_ref);
            self.communication_parser
                .pdu_ref_map
                .get(key)
                .ok_or_else(|| format!("no PDU ref for {i_signal_ref}"))?
        };

        // 6. extractLast(commPduRef) → SystemSignalRef
        let system_signal_ref = {
            let key = convert::extract_last(comm_pdu_ref);
            self.communication_parser
                .signal_ref_map
                .get(key)
                .ok_or_else(|| format!("no signal ref for {comm_pdu_ref}"))?
        };

        // 7. SystemSignalRef → OperationRef
        let operation_ref = self
            .system_parser
            .operation_ref
            .get(system_signal_ref)
            .ok_or_else(|| format!("no operation ref for {system_signal_ref}"))?;

        // 8. extractLast2(operationRef) → (csiKey, csoKey) → InterfaceRef → tRef
        let (csi_key, cso_key) = extract_last2(operation_ref)?;
        let cso_map = self
            .software_types_parser
            .interface_ref_map
            .get(&csi_key)
            .ok_or_else(|| format!("no interface ref for {operation_ref}"))?;
        let t_ref = cso_map
            .get(&cso_key)
            .ok_or_else(|| format!("no operation '{cso_key}' in interface '{csi_key}'"))?;

        // 9. extractLast(tRef) → DataType
        let dt_key = convert::extract_last(t_ref).to_lowercase();
        self.data_types_parser
            .application_data_types
            .get(&dt_key)
            .ok_or_else(|| format!("no data type for {t_ref}"))
    }

    // ---- internal lookup helpers ----

    fn pdu_triggering_ref_by_header_id(&self, header_id: u32) -> Result<&String, String> {
        self.topology_parser
            .header_id_ref
            .get(&header_id)
            .ok_or_else(|| format!("no header ref for ID {header_id}"))
    }

    fn tp_sdu_ref_by_pdu_triggering_ref<'a>(&'a self, pdu_ref: &str) -> Option<&'a String> {
        self.tp_config_parser
            .as_ref()
            .and_then(|tp| tp.pdu_map.get(pdu_ref))
    }

    fn i_signal_ref_by_i_signal_ipdu(&self, short_name: &str) -> Result<String, String> {
        let key = convert::extract_last(short_name);
        self.topology_parser
            .pdu_triggering_ref
            .get(key)
            .cloned()
            .ok_or_else(|| format!("no PDU triggered for {short_name}"))
    }
}

impl Default for CpParser {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TpConfig (tiny, lives here to avoid an extra file)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct TpConfigParser {
    /// TRANSPORT-PDU-REF → TP-SDU-REF
    pdu_map: HashMap<String, String>,
}

impl TpConfigParser {
    fn new() -> Self {
        Self::default()
    }

    fn parse_tp_config(&mut self, node: Node) -> Result<(), String> {
        for someip_tp in node
            .descendants()
            .filter(|n| n.tag_name().name() == "SOMEIP-TP-CONNECTION")
        {
            self.parse_someip_tp_connection(someip_tp);
        }
        Ok(())
    }

    fn parse_someip_tp_connection(&mut self, node: Node) {
        let tp_sdu = match xml::find_child(node, "TP-SDU-REF") {
            Some(n) => n,
            None => return,
        };
        let transport_pdu = match xml::find_child(node, "TRANSPORT-PDU-REF") {
            Some(n) => n,
            None => return,
        };
        self.pdu_map.insert(
            transport_pdu.text().unwrap_or("").to_string(),
            tp_sdu.text().unwrap_or("").to_string(),
        );
    }
}

// ---------------------------------------------------------------------------
// DataTypeMappingSet parser
// ---------------------------------------------------------------------------

fn parse_data_type_mapping_sets(node: Node) -> Result<HashMap<String, String>, String> {
    let elements = xml::get_elements(node)?;
    let dtms = xml::require_child(elements, "DATA-TYPE-MAPPING-SET")?;

    let sn = xml::get_shortname(dtms)?;
    if sn != "Data_Type_Mappings" {
        return Err(format!("expected 'Data_Type_Mappings', got '{sn}'"));
    }

    let dtm = xml::require_child(dtms, "DATA-TYPE-MAPS")?;
    let mut mappings = HashMap::new();

    for (i, sub_dtm) in dtm
        .children()
        .filter(|n| n.tag_name().name() == "DATA-TYPE-MAP")
        .enumerate()
    {
        let adtr = xml::require_child(sub_dtm, "APPLICATION-DATA-TYPE-REF")?;
        let idtr = xml::require_child(sub_dtm, "IMPLEMENTATION-DATA-TYPE-REF")?;
        let adtr_key = convert::extract_last(adtr.text().ok_or("empty APPLICATION-DATA-TYPE-REF")?);
        let idtr_key =
            convert::extract_last(idtr.text().ok_or("empty IMPLEMENTATION-DATA-TYPE-REF")?);

        if mappings
            .insert(adtr_key.to_string(), idtr_key.to_string())
            .is_some()
        {
            return Err(format!(
                "duplicate DataTypeMapping for '{adtr_key}' (map {i})"
            ));
        }
    }

    Ok(mappings)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn find_ar_package_by_name<'a>(node: Node<'a, 'a>, name: &str) -> Option<Node<'a, 'a>> {
    node.children()
        .filter(|c| c.tag_name().name() == "AR-PACKAGE")
        .find(|c| xml::child_text(*c, "SHORT-NAME") == Some(name))
}

/// Split a ref path like `"/A/B/C"` into its last two segments,
/// returning `(second-to-last, last)`.
fn extract_last2(r: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = r.split('/').collect();
    if parts.len() < 2 {
        return Err(format!("path has fewer than 2 segments: {r}"));
    }
    Ok((
        parts[parts.len() - 2].to_string(),
        parts[parts.len() - 1].to_string(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const CP_ARXML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>DataTypeMappingSets</SHORT-NAME>
      <ELEMENTS>
        <DATA-TYPE-MAPPING-SET>
          <SHORT-NAME>Data_Type_Mappings</SHORT-NAME>
          <DATA-TYPE-MAPS>
            <DATA-TYPE-MAP>
              <APPLICATION-DATA-TYPE-REF>/DataTypes/SpeedType</APPLICATION-DATA-TYPE-REF>
              <IMPLEMENTATION-DATA-TYPE-REF>/Impl/uint32_impl</IMPLEMENTATION-DATA-TYPE-REF>
            </DATA-TYPE-MAP>
            <DATA-TYPE-MAP>
              <APPLICATION-DATA-TYPE-REF>/DataTypes/HeaderType</APPLICATION-DATA-TYPE-REF>
              <IMPLEMENTATION-DATA-TYPE-REF>/Impl/uint16_impl</IMPLEMENTATION-DATA-TYPE-REF>
            </DATA-TYPE-MAP>
          </DATA-TYPE-MAPS>
        </DATA-TYPE-MAPPING-SET>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>DataTypes</SHORT-NAME>
      <AR-PACKAGES>
        <AR-PACKAGE>
          <SHORT-NAME>Impl</SHORT-NAME>
          <ELEMENTS>
            <IMPLEMENTATION-DATA-TYPE>
              <SHORT-NAME>uint32_impl</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
              <SW-DATA-DEF-PROPS>
                <SW-DATA-DEF-PROPS-VARIANTS>
                  <SW-DATA-DEF-PROPS-CONDITIONAL>
                    <BASE-TYPE-REF>/BaseTypes/uint32</BASE-TYPE-REF>
                  </SW-DATA-DEF-PROPS-CONDITIONAL>
                </SW-DATA-DEF-PROPS-VARIANTS>
              </SW-DATA-DEF-PROPS>
            </IMPLEMENTATION-DATA-TYPE>
            <IMPLEMENTATION-DATA-TYPE>
              <SHORT-NAME>uint16_impl</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
              <SW-DATA-DEF-PROPS>
                <SW-DATA-DEF-PROPS-VARIANTS>
                  <SW-DATA-DEF-PROPS-CONDITIONAL>
                    <BASE-TYPE-REF>/BaseTypes/uint16</BASE-TYPE-REF>
                  </SW-DATA-DEF-PROPS-CONDITIONAL>
                </SW-DATA-DEF-PROPS-VARIANTS>
              </SW-DATA-DEF-PROPS>
            </IMPLEMENTATION-DATA-TYPE>
          </ELEMENTS>
        </AR-PACKAGE>
        <AR-PACKAGE>
          <SHORT-NAME>App</SHORT-NAME>
          <ELEMENTS>
            <APPLICATION-PRIMITIVE-DATA-TYPE>
              <SHORT-NAME>SpeedType</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
            </APPLICATION-PRIMITIVE-DATA-TYPE>
            <APPLICATION-PRIMITIVE-DATA-TYPE>
              <SHORT-NAME>HeaderType</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
            </APPLICATION-PRIMITIVE-DATA-TYPE>
          </ELEMENTS>
        </AR-PACKAGE>
      </AR-PACKAGES>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>Topology</SHORT-NAME>
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
                                  <SHORT-NAME>TestService</SHORT-NAME>
                                  <SERVICE-IDENTIFIER>10</SERVICE-IDENTIFIER>
                                </PROVIDED-SERVICE-INSTANCE>
                              </PROVIDED-SERVICE-INSTANCES>
                            </APPLICATION-ENDPOINT>
                          </SOCKET-ADDRESS>
                        </SOCKET-ADDRESSS>
                        <CONNECTION-BUNDLES>
                          <SOCKET-CONNECTION-BUNDLE>
                            <BUNDLED-CONNECTIONS>
                              <SOCKET-CONNECTION>
                                <PDUS>
                                  <SOCKET-CONNECTION-IPDU-IDENTIFIER>
                                    <HEADER-ID>100</HEADER-ID>
                                    <PDU-TRIGGERING-REF>/Topology/trig1</PDU-TRIGGERING-REF>
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
                          <I-PDU-REF>/Com/SignalPDU</I-PDU-REF>
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
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>Communication</SHORT-NAME>
      <AR-PACKAGES>
        <AR-PACKAGE>
          <SHORT-NAME>PDUs</SHORT-NAME>
          <ELEMENTS>
            <I-SIGNAL-I-PDU>
              <SHORT-NAME>SignalPDU</SHORT-NAME>
              <I-SIGNAL-TO-PDU-MAPPINGS>
                <I-SIGNAL-TO-I-PDU-MAPPING>
                  <I-SIGNAL-REF>/Com/SigA</I-SIGNAL-REF>
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
              <SYSTEM-SIGNAL-REF>/Sys/SS1</SYSTEM-SIGNAL-REF>
            </I-SIGNAL>
          </ELEMENTS>
        </AR-PACKAGE>
      </AR-PACKAGES>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>System</SHORT-NAME>
      <ELEMENTS>
        <SYSTEM>
          <SHORT-NAME>SystemDescription</SHORT-NAME>
          <MAPPINGS>
            <SYSTEM-MAPPING>
              <DATA-MAPPINGS>
                <SENDER-RECEIVER-TO-SIGNAL-MAPPING>
                  <SYSTEM-SIGNAL-REF>/Sys/SS1</SYSTEM-SIGNAL-REF>
                  <DATA-ELEMENT-IREF>
                    <TARGET-DATA-PROTOTYPE-REF>/If/SensorIf/Speed</TARGET-DATA-PROTOTYPE-REF>
                  </DATA-ELEMENT-IREF>
                </SENDER-RECEIVER-TO-SIGNAL-MAPPING>
              </DATA-MAPPINGS>
            </SYSTEM-MAPPING>
          </MAPPINGS>
        </SYSTEM>
      </ELEMENTS>
    </AR-PACKAGE>
    <AR-PACKAGE>
      <SHORT-NAME>SoftwareTypes</SHORT-NAME>
      <AR-PACKAGES>
        <AR-PACKAGE>
          <SHORT-NAME>Interfaces</SHORT-NAME>
          <ELEMENTS>
            <SENDER-RECEIVER-INTERFACE>
              <SHORT-NAME>SensorIf</SHORT-NAME>
              <DATA-ELEMENTS>
                <VARIABLE-DATA-PROTOTYPE>
                  <SHORT-NAME>Speed</SHORT-NAME>
                  <TYPE-TREF>/DataTypes/SpeedType</TYPE-TREF>
                </VARIABLE-DATA-PROTOTYPE>
              </DATA-ELEMENTS>
            </SENDER-RECEIVER-INTERFACE>
          </ELEMENTS>
        </AR-PACKAGE>
      </AR-PACKAGES>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    #[test]
    fn full_lookup_chain() {
        let doc = Document::parse(CP_ARXML).unwrap();
        let mut parser = CpParser::new();
        parser.parse(&doc).unwrap();

        let dt = parser.resolve_type(10, 100).unwrap();
        assert_eq!(dt.short_name, "SpeedType");
        assert_eq!(dt.category, "VALUE");
    }

    #[test]
    fn unknown_service_id_error() {
        let doc = Document::parse(CP_ARXML).unwrap();
        let mut parser = CpParser::new();
        parser.parse(&doc).unwrap();

        let err = parser.resolve_type(999, 100).unwrap_err();
        assert!(err.contains("no service found"));
    }

    #[test]
    fn unknown_header_id_error() {
        let doc = Document::parse(CP_ARXML).unwrap();
        let mut parser = CpParser::new();
        parser.parse(&doc).unwrap();

        let err = parser.resolve_type(10, 999).unwrap_err();
        assert!(err.contains("no header ref"));
    }
}
