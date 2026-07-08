//! CP DataTypes parser — extracts IMPLEMENTATION-DATA-TYPE and
//! APPLICATION-*-DATA-TYPE definitions from ARXML.
//!
//! Corresponds to the Go `cp/parser/datatypes/` package.

use std::collections::HashMap;

use roxmltree::Node;

use crate::ast::types::DataType;
use crate::util::{convert, xml};

/// Parses the two-tier AUTOSAR data-type system:
///
/// # Fields
///
/// - `data_type_mappings`: application-name → implementation-name
///   map populated by the `DataTypeMappingSet` parser.
///
/// 1. **Implementation data types** — map to concrete platform types
///    (e.g. `uint8`, `float`).
/// 2. **Application data types** — semantic types that reference
///    implementation types via a `DataTypeMappingSet`.
#[derive(Debug)]
pub struct DataTypesParser {
    /// Parsed application data types keyed by short-name.
    pub application_data_types: HashMap<String, DataType>,
    /// Parsed implementation data types keyed by short-name.
    pub implementation_data_types: HashMap<String, DataType>,
    /// application-name → implementation-name map from
    /// `DataTypeMappingSets`.
    data_type_mappings: HashMap<String, String>,
}

impl DataTypesParser {
    /// `data_type_mappings`: application-name → implementation-name,
    /// populated by the `DataTypeMappingSet` parser earlier in the
    /// pipeline.
    pub fn new(data_type_mappings: HashMap<String, String>) -> Self {
        Self {
            application_data_types: HashMap::new(),
            implementation_data_types: HashMap::new(),
            data_type_mappings,
        }
    }

    /// Return the map of resolved application data types.
    pub fn get_application_data_types(&self) -> &HashMap<String, DataType> {
        &self.application_data_types
    }

    // ---- public entry point ----

    /// Parse both implementation and application data types from the
    /// `DataTypes` AR-PACKAGE element.
    pub fn parse_data_types(&mut self, root: Node) -> Result<(), String> {
        let ar_packages = xml::require_child(root, "AR-PACKAGES")?;
        self.parse_implementation_data_types(ar_packages)?;
        self.parse_application_data_types(ar_packages)?;
        Ok(())
    }

    // ---- implementation data types ----

    fn parse_implementation_data_types(&mut self, node: Node) -> Result<(), String> {
        for (i, idt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "IMPLEMENTATION-DATA-TYPE")
            .enumerate()
        {
            self.parse_implementation_value_data_type(idt)
                .map_err(|e| format!("parse {i} IMPLEMENTATION-DATA-TYPE failed: {e}"))?;
        }
        Ok(())
    }

    fn parse_implementation_value_data_type(&mut self, root: Node) -> Result<(), String> {
        let sn = xml::get_shortname(root)?;
        let category = xml::get_category(root)?;

        if category != "VALUE" {
            return Ok(());
        }

        let sddpc = xml::get_sw_data_def_props_conditional(root)?;
        let btr = xml::require_child(sddpc, "BASE-TYPE-REF")?;
        let r = btr.text().ok_or("empty BASE-TYPE-REF")?;

        xml::valid_basic_type(r)?;

        self.implementation_data_types.insert(
            sn.to_string(),
            DataType::new_type_reference(sn.to_string(), category.to_string(), r.to_string()),
        );
        Ok(())
    }

    // ---- application data types ----

    fn parse_application_data_types(&mut self, node: Node) -> Result<(), String> {
        // APPLICATION-PRIMITIVE-DATA-TYPE
        for (i, apdt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "APPLICATION-PRIMITIVE-DATA-TYPE")
            .enumerate()
        {
            self.parse_application_data_type(apdt)
                .map_err(|e| format!("parse {i} APPLICATION-PRIMITIVE-DATA-TYPE: {e}"))?;
        }
        // APPLICATION-ARRAY-DATA-TYPE
        for (i, aadt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "APPLICATION-ARRAY-DATA-TYPE")
            .enumerate()
        {
            self.parse_application_data_type(aadt)
                .map_err(|e| format!("parse {i} APPLICATION-ARRAY-DATA-TYPE: {e}"))?;
        }
        // APPLICATION-RECORD-DATA-TYPE
        for (i, ardt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "APPLICATION-RECORD-DATA-TYPE")
            .enumerate()
        {
            self.parse_application_data_type(ardt)
                .map_err(|e| format!("parse {i} APPLICATION-RECORD-DATA-TYPE: {e}"))?;
        }
        Ok(())
    }

    fn parse_application_data_type(&mut self, root: Node) -> Result<(), String> {
        let sn = xml::get_shortname(root)?;
        let category = xml::get_category(root)?;

        match category {
            "STRING" => self.parse_application_string(root, sn, category),
            "VALUE" => self.resolve_value_type(sn, category),
            "ARRAY" => self.parse_application_array(root, sn, category),
            "STRUCTURE" => self.parse_application_structure(root, sn, category),
            other => Err(format!("unknown category: {other}")),
        }
    }

    fn parse_application_string(
        &mut self,
        root: Node,
        sn: &str,
        category: &str,
    ) -> Result<(), String> {
        let sddpc = xml::get_sw_data_def_props_conditional(root)?;
        let stp = xml::require_child(sddpc, "SW-TEXT-PROPS")?;

        let is_dynamic = xml::get_array_size_semantics(stp)?;
        if !is_dynamic {
            return Err("fixed-length string not supported yet".into());
        }

        let btr = xml::require_child(stp, "BASE-TYPE-REF")?;
        let btr_raw = btr.text().ok_or("empty BASE-TYPE-REF")?;
        if !btr_raw.to_uppercase().contains("UTF_8") {
            return Err(format!("BASE-TYPE-REF should be UTF_8, got: {btr_raw}"));
        }

        self.application_data_types.insert(
            sn.to_lowercase(),
            DataType::new_string(sn.to_string(), category.to_string(), 0),
        );
        Ok(())
    }

    fn resolve_value_type(&mut self, sn: &str, _category: &str) -> Result<(), String> {
        let idtr_key = self
            .data_type_mappings
            .get(sn)
            .ok_or_else(|| format!("no DataTypeMapping for application type {sn}"))?;

        let mut dt = self
            .implementation_data_types
            .get(idtr_key)
            .ok_or_else(|| {
                format!("no implementation data type '{idtr_key}' for application key {sn}")
            })?
            .clone();

        dt.short_name = sn.to_string();
        self.application_data_types.insert(sn.to_lowercase(), dt);
        Ok(())
    }

    fn parse_application_array(
        &mut self,
        root: Node,
        sn: &str,
        category: &str,
    ) -> Result<(), String> {
        let element = xml::require_child(root, "ELEMENT")?;
        let type_ref = xml::require_child(element, "TYPE-TREF")?;
        let array_ref = convert::extract_last(type_ref.text().ok_or("empty TYPE-TREF")?);

        let is_dynamic = xml::get_array_size_semantics(element)?;
        let size = if !is_dynamic {
            let max_elems = xml::require_child(element, "MAX-NUMBER-OF-ELEMENTS")?;
            convert::to_u64(max_elems.text().ok_or("empty MAX-NUMBER-OF-ELEMENTS")?)
                .map_err(|e| format!("invalid MAX-NUMBER-OF-ELEMENTS: {e}"))?
        } else {
            0
        };

        self.application_data_types.insert(
            sn.to_lowercase(),
            DataType::new_array(
                sn.to_string(),
                category.to_string(),
                array_ref.to_string(),
                size,
            ),
        );
        Ok(())
    }

    fn parse_application_structure(
        &mut self,
        root: Node,
        sn: &str,
        category: &str,
    ) -> Result<(), String> {
        let elements = xml::require_child(root, "ELEMENTS")?;

        let mut fields = Vec::new();
        for record in elements
            .children()
            .filter(|n| n.tag_name().name() == "APPLICATION-RECORD-ELEMENT")
        {
            let field_name = xml::get_shortname(record)?;
            let type_ref = xml::require_child(record, "TYPE-TREF")?;
            let ref_text = convert::extract_last(type_ref.text().ok_or("empty TYPE-TREF")?);

            fields.push(crate::ast::types::StructureField {
                name: field_name.to_string(),
                type_ref: ref_text.to_string(),
                in_place: false,
            });
        }

        self.application_data_types.insert(
            sn.to_lowercase(),
            DataType::new_structure(sn.to_string(), category.to_string(), fields),
        );
        Ok(())
    }

    /// Build a merged type map: application data types fall back to
    /// their implementation type counterparts via [`DataTypeMappingSet`].
    pub fn merged_data_types(&self) -> HashMap<String, DataType> {
        let mut merged = self.application_data_types.clone();
        for (adt_name, idt_name) in &self.data_type_mappings {
            let adt_key = adt_name.to_lowercase();
            if !merged.contains_key(&adt_key)
                && let Some(idt) = self.implementation_data_types.get(idt_name)
            {
                merged.insert(adt_key, idt.clone());
            }
        }
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    /// Minimal ARXML with one IMPLEMENTATION-DATA-TYPE (VALUE) and
    /// one APPLICATION-PRIMITIVE-DATA-TYPE (VALUE) wired together by
    /// a DataTypeMapping.
    const MINI_ARXML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>DataTypes</SHORT-NAME>
      <AR-PACKAGES>
        <AR-PACKAGE>
          <SHORT-NAME>ImplementationDataTypes</SHORT-NAME>
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
          </ELEMENTS>
        </AR-PACKAGE>
        <AR-PACKAGE>
          <SHORT-NAME>ApplicationDataTypes</SHORT-NAME>
          <ELEMENTS>
            <APPLICATION-PRIMITIVE-DATA-TYPE>
              <SHORT-NAME>MySpeed</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
            </APPLICATION-PRIMITIVE-DATA-TYPE>
          </ELEMENTS>
        </AR-PACKAGE>
      </AR-PACKAGES>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    fn test_mappings() -> HashMap<String, String> {
        [("MySpeed".into(), "uint32_impl".into())]
            .into_iter()
            .collect()
    }

    #[test]
    fn parse_value_type_through_mapping() {
        let doc = Document::parse(MINI_ARXML).unwrap();
        let mut parser = DataTypesParser::new(test_mappings());
        parser.parse_data_types(doc.root_element()).unwrap();

        let dt = parser.application_data_types.get("myspeed").unwrap();
        assert_eq!(dt.short_name, "MySpeed");
        assert_eq!(dt.category, "VALUE");
    }

    #[test]
    fn missing_mapping_is_error() {
        let doc = Document::parse(MINI_ARXML).unwrap();
        let mut parser = DataTypesParser::new(HashMap::new());
        let err = parser.parse_data_types(doc.root_element()).unwrap_err();
        assert!(err.contains("DataTypeMapping"));
    }
}
