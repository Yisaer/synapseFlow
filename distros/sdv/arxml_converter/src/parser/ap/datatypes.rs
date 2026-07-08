//! AP (Adaptive Platform) data-type parser.
//!
//! Parses `STD-CPP-IMPLEMENTATION-DATA-TYPE` elements, which use a
//! different XML structure from the CP `IMPLEMENTATION-DATA-TYPE`.
//!
//! Corresponds to the Go `ap/parser/datatypes.go`.

use std::collections::HashMap;

use roxmltree::Node;

use crate::ast::types::{ArrayType, DataType, StructureField, VectorType};
use crate::util::{convert, xml};

/// Parse `STD-CPP-IMPLEMENTATION-DATA-TYPE` elements from the
/// `dataTypes` AR-PACKAGE.
pub fn parse_data_types(node: Node) -> Result<HashMap<String, DataType>, String> {
    let elements = xml::get_elements(node)?;
    let mut types = HashMap::new();

    for (i, dt_el) in elements
        .children()
        .filter(|n| n.tag_name().name() == "STD-CPP-IMPLEMENTATION-DATA-TYPE")
        .enumerate()
    {
        let dt = parse_single(dt_el).map_err(|e| format!("index {i}: {e}"))?;
        types.insert(dt.short_name.to_lowercase(), dt);
    }
    Ok(types)
}

fn parse_single(node: Node) -> Result<DataType, String> {
    let sn = xml::get_shortname(node)?;
    let category = xml::get_category(node)?;

    match category {
        "TYPE_REFERENCE" => parse_type_reference(node, sn, category),
        "VECTOR" => parse_vector(node, sn, category),
        "ARRAY" => parse_array(node, sn, category),
        "STRUCTURE" => parse_structure(node, sn, category),
        other => Err(format!("unknown AP category: {other}")),
    }
}

fn parse_type_reference(node: Node, sn: &str, category: &str) -> Result<DataType, String> {
    let ref_el = xml::require_child(node, "TYPE-REFERENCE-REF")?;
    let ref_text = ref_el.text().unwrap_or("").to_string();

    let string_size = if ref_text.to_lowercase().contains("string") {
        let array_size = xml::require_child(node, "ARRAY-SIZE")?;
        let raw = array_size.text().ok_or("empty ARRAY-SIZE")?;
        Some(convert::to_u64(raw).map_err(|e| format!("invalid ARRAY-SIZE: {e}"))?)
    } else {
        None
    };

    let mut dt = DataType::new_type_reference(sn.to_string(), category.to_string(), ref_text);
    if let Some(size) = string_size
        && let crate::ast::types::DataTypeKind::TypeReference(ref mut tr) = dt.kind
    {
        tr.string_size = Some(size);
    }
    Ok(dt)
}

fn parse_vector(node: Node, sn: &str, category: &str) -> Result<DataType, String> {
    let args = xml::require_child(node, "TEMPLATE-ARGUMENTS")?;
    let cpp_arg = xml::require_child(args, "CPP-TEMPLATE-ARGUMENT")?;
    let typ_ref = xml::require_child(cpp_arg, "TEMPLATE-TYPE-REF")?;

    Ok(DataType {
        short_name: sn.into(),
        category: category.into(),
        kind: crate::ast::types::DataTypeKind::Vector(VectorType {
            element_ref: typ_ref.text().unwrap_or("").to_string(),
        }),
    })
}

fn parse_array(node: Node, sn: &str, category: &str) -> Result<DataType, String> {
    let array_size_el = xml::require_child(node, "ARRAY-SIZE")?;
    let size = convert::to_u64(array_size_el.text().unwrap_or(""))
        .map_err(|e| format!("invalid ARRAY-SIZE: {e}"))?;

    let args = xml::require_child(node, "TEMPLATE-ARGUMENTS")?;
    let cpp_arg = xml::require_child(args, "CPP-TEMPLATE-ARGUMENT")?;

    let in_place = {
        let ip_el = xml::require_child(cpp_arg, "INPLACE")?;
        ip_el.text().unwrap_or("false") == "true"
    };

    let typ_ref = xml::require_child(cpp_arg, "TEMPLATE-TYPE-REF")?;

    Ok(DataType {
        short_name: sn.into(),
        category: category.into(),
        kind: crate::ast::types::DataTypeKind::Array(ArrayType {
            size,
            in_place,
            element_ref: typ_ref.text().unwrap_or("").to_string(),
        }),
    })
}

fn parse_structure(node: Node, sn: &str, category: &str) -> Result<DataType, String> {
    let sub = xml::require_child(node, "SUB-ELEMENTS")?;
    let mut fields = Vec::new();

    for cpp_el in sub
        .children()
        .filter(|n| n.tag_name().name() == "CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT")
    {
        let field_name = xml::get_shortname(cpp_el)?;
        let type_ref_el = xml::require_child(cpp_el, "TYPE-REFERENCE")?;

        let in_place = {
            let ip = xml::require_child(type_ref_el, "INPLACE")?;
            ip.text().unwrap_or("false") == "true"
        };

        let trd = xml::require_child(type_ref_el, "TYPE-REFERENCE-REF")?;

        fields.push(StructureField {
            name: field_name.to_string(),
            type_ref: trd.text().unwrap_or("").to_string(),
            in_place,
        });
    }

    Ok(DataType::new_structure(sn.into(), category.into(), fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const AP_DT_XML: &str = r#"
<AUTOSAR>
  <ELEMENTS>
    <STD-CPP-IMPLEMENTATION-DATA-TYPE>
      <SHORT-NAME>MyUint32</SHORT-NAME>
      <CATEGORY>TYPE_REFERENCE</CATEGORY>
      <TYPE-REFERENCE-REF>/PlatformTypes/uint32</TYPE-REFERENCE-REF>
    </STD-CPP-IMPLEMENTATION-DATA-TYPE>
    <STD-CPP-IMPLEMENTATION-DATA-TYPE>
      <SHORT-NAME>Point</SHORT-NAME>
      <CATEGORY>STRUCTURE</CATEGORY>
      <SUB-ELEMENTS>
        <CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT>
          <SHORT-NAME>x</SHORT-NAME>
          <TYPE-REFERENCE>
            <INPLACE>true</INPLACE>
            <TYPE-REFERENCE-REF>/PlatformTypes/uint32</TYPE-REFERENCE-REF>
          </TYPE-REFERENCE>
        </CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT>
        <CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT>
          <SHORT-NAME>y</SHORT-NAME>
          <TYPE-REFERENCE>
            <INPLACE>true</INPLACE>
            <TYPE-REFERENCE-REF>/PlatformTypes/uint32</TYPE-REFERENCE-REF>
          </TYPE-REFERENCE>
        </CPP-IMPLEMENTATION-DATA-TYPE-ELEMENT>
      </SUB-ELEMENTS>
    </STD-CPP-IMPLEMENTATION-DATA-TYPE>
    <STD-CPP-IMPLEMENTATION-DATA-TYPE>
      <SHORT-NAME>Arr</SHORT-NAME>
      <CATEGORY>ARRAY</CATEGORY>
      <ARRAY-SIZE>10</ARRAY-SIZE>
      <TEMPLATE-ARGUMENTS>
        <CPP-TEMPLATE-ARGUMENT>
          <INPLACE>false</INPLACE>
          <TEMPLATE-TYPE-REF>/PlatformTypes/uint8</TEMPLATE-TYPE-REF>
        </CPP-TEMPLATE-ARGUMENT>
      </TEMPLATE-ARGUMENTS>
    </STD-CPP-IMPLEMENTATION-DATA-TYPE>
    <STD-CPP-IMPLEMENTATION-DATA-TYPE>
      <SHORT-NAME>Names</SHORT-NAME>
      <CATEGORY>TYPE_REFERENCE</CATEGORY>
      <TYPE-REFERENCE-REF>string</TYPE-REFERENCE-REF>
      <ARRAY-SIZE>32</ARRAY-SIZE>
    </STD-CPP-IMPLEMENTATION-DATA-TYPE>
  </ELEMENTS>
</AUTOSAR>
"#;

    #[test]
    fn parse_type_reference_category() {
        let doc = Document::parse(AP_DT_XML).unwrap();
        let types = parse_data_types(doc.root_element()).unwrap();
        let dt = types.get("myuint32").unwrap();
        assert_eq!(dt.category, "TYPE_REFERENCE");
        assert_eq!(dt.short_name, "MyUint32");
    }

    #[test]
    fn parse_structure_category() {
        let doc = Document::parse(AP_DT_XML).unwrap();
        let types = parse_data_types(doc.root_element()).unwrap();
        let dt = types.get("point").unwrap();
        assert_eq!(dt.category, "STRUCTURE");
        if let crate::ast::types::DataTypeKind::Structure(ref s) = dt.kind {
            assert_eq!(s.fields.len(), 2);
            assert_eq!(s.fields[0].name, "x");
            assert_eq!(s.fields[1].name, "y");
        } else {
            panic!("expected Structure");
        }
    }

    #[test]
    fn parse_array_category() {
        let doc = Document::parse(AP_DT_XML).unwrap();
        let types = parse_data_types(doc.root_element()).unwrap();
        let dt = types.get("arr").unwrap();
        assert_eq!(dt.category, "ARRAY");
    }

    #[test]
    fn parse_fixed_string() {
        let doc = Document::parse(AP_DT_XML).unwrap();
        let types = parse_data_types(doc.root_element()).unwrap();
        let dt = types.get("names").unwrap();
        if let crate::ast::types::DataTypeKind::TypeReference(ref tr) = dt.kind {
            assert_eq!(tr.string_size, Some(32));
        } else {
            panic!("expected TypeReference");
        }
    }
}
