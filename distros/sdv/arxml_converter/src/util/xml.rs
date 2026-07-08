//! XML query helpers built on top of [`roxmltree`].
//!
//! These functions wrap common patterns (find-child-by-tag, read required
//! text, traverse nested paths) that are used repeatedly by the parser
//! modules.
//!
//! Corresponds to the Go `util/xml.go`.

use roxmltree::Node;

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------

/// Check whether the node's local name (ignoring XML namespace) equals
/// `tag`.  This is more robust than `has_tag_name()` because real
/// ARXML files use a default `xmlns`.
pub fn has_local_name(node: Node, tag: &str) -> bool {
    node.tag_name().name() == tag
}

/// Find the first child element whose local name equals `tag`.
pub fn find_child<'a>(node: Node<'a, 'a>, tag: &str) -> Option<Node<'a, 'a>> {
    node.children().find(|c| has_local_name(*c, tag))
}

/// Like [`find_child`], but returns an error when the element is missing.
pub fn require_child<'a>(node: Node<'a, 'a>, tag: &str) -> Result<Node<'a, 'a>, String> {
    find_child(node, tag).ok_or_else(|| format!("no <{}> found", tag))
}

/// Read the text content of the first child element with the given tag.
pub fn child_text<'a>(node: Node<'a, 'a>, tag: &str) -> Option<&'a str> {
    find_child(node, tag).and_then(|c| c.text())
}

/// Read the non-empty text content of the first child element, or return
/// a descriptive error.
pub fn require_child_text<'a>(node: Node<'a, 'a>, tag: &str) -> Result<&'a str, String> {
    child_text(node, tag)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| format!("missing or empty <{}>", tag))
}

// ---------------------------------------------------------------------------
// Domain helpers (mirror Go util/xml.go)
// ---------------------------------------------------------------------------

/// `<SHORT-NAME>` → `&str`
pub fn get_shortname<'a>(node: Node<'a, 'a>) -> Result<&'a str, String> {
    require_child_text(node, "SHORT-NAME")
}

/// `<CATEGORY>` → `&str`
pub fn get_category<'a>(node: Node<'a, 'a>) -> Result<&'a str, String> {
    require_child_text(node, "CATEGORY")
}

/// `<ELEMENTS>` → child `Node`
pub fn get_elements<'a>(node: Node<'a, 'a>) -> Result<Node<'a, 'a>, String> {
    require_child(node, "ELEMENTS")
}

/// Traverse `SW-DATA-DEF-PROPS → SW-DATA-DEF-PROPS-VARIANTS →
/// SW-DATA-DEF-PROPS-CONDITIONAL` and return the innermost element.
pub fn get_sw_data_def_props_conditional<'a>(node: Node<'a, 'a>) -> Result<Node<'a, 'a>, String> {
    let sddp = require_child(node, "SW-DATA-DEF-PROPS")?;
    let sddpv = require_child(sddp, "SW-DATA-DEF-PROPS-VARIANTS")?;
    require_child(sddpv, "SW-DATA-DEF-PROPS-CONDITIONAL")
}

/// `<ARRAY-SIZE-SEMANTICS>` → `true` for variable-size, `false` for fixed-size.
pub fn get_array_size_semantics(node: Node) -> Result<bool, String> {
    let text = require_child_text(node, "ARRAY-SIZE-SEMANTICS")?;
    match text {
        "VARIABLE-SIZE" => Ok(true),
        "FIXED-SIZE" => Ok(false),
        other => Err(format!("invalid ARRAY-SIZE-SEMANTICS: {}", other)),
    }
}

/// Check whether a type-reference path names a known basic type (uint8,
/// float, etc.).  Returns `Ok(())` when valid, `Err(...)` otherwise.
pub fn valid_basic_type(r: &str) -> Result<(), String> {
    let name = extract_last_segment(r).to_lowercase();
    // Use substring matching (same as Go's strings.Contains) so that
    // prefixed names like "sint32" / "uint8_t" are recognised.
    // Order: longer names first to avoid "uint16" shadowing "uint8".
    if name.contains("uint16")
        || name.contains("uint32")
        || name.contains("uint64")
        || name.contains("uint8")
        || name.contains("int16")
        || name.contains("int32")
        || name.contains("int64")
        || name.contains("int8")
        || name.contains("float")
        || name.contains("double")
        || name.contains("bool")
    {
        Ok(())
    } else {
        Err(format!("invalid basic type: {}", r))
    }
}

/// `<AR-PACKAGES>` → child `Node`
pub fn get_ar_packages_element<'a>(node: Node<'a, 'a>) -> Result<Node<'a, 'a>, String> {
    require_child(node, "AR-PACKAGES")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn extract_last_segment(r: &str) -> &str {
    r.rsplit('/').next().unwrap_or(r)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    const XML: &str = r#"
<ROOT>
    <SHORT-NAME>TestType</SHORT-NAME>
    <CATEGORY>STRUCTURE</CATEGORY>
    <ELEMENTS>
        <CHILD/>
    </ELEMENTS>
    <SW-DATA-DEF-PROPS>
        <SW-DATA-DEF-PROPS-VARIANTS>
            <SW-DATA-DEF-PROPS-CONDITIONAL>
                <X>1</X>
            </SW-DATA-DEF-PROPS-CONDITIONAL>
        </SW-DATA-DEF-PROPS-VARIANTS>
    </SW-DATA-DEF-PROPS>
    <ARRAY-SIZE-SEMANTICS>FIXED-SIZE</ARRAY-SIZE-SEMANTICS>
    <AR-PACKAGES>
        <AR-PACKAGE/>
    </AR-PACKAGES>
</ROOT>
"#;

    fn doc() -> Document<'static> {
        Document::parse(XML).unwrap()
    }

    #[test]
    fn read_shortname() {
        let d = doc();
        assert_eq!(get_shortname(d.root_element()).unwrap(), "TestType");
    }

    #[test]
    fn read_category() {
        let d = doc();
        assert_eq!(get_category(d.root_element()).unwrap(), "STRUCTURE");
    }

    #[test]
    fn read_elements() {
        let d = doc();
        assert!(get_elements(d.root_element()).is_ok());
    }

    #[test]
    fn traverse_sw_data_def_props() {
        let d = doc();
        let cond = get_sw_data_def_props_conditional(d.root_element()).unwrap();
        assert!(find_child(cond, "X").is_some());
    }

    #[test]
    fn read_array_size_semantics_fixed() {
        let d = doc();
        assert!(!get_array_size_semantics(d.root_element()).unwrap());
    }

    #[test]
    fn read_array_size_semantics_variable() {
        let xml = r#"<R><ARRAY-SIZE-SEMANTICS>VARIABLE-SIZE</ARRAY-SIZE-SEMANTICS></R>"#;
        let d = Document::parse(xml).unwrap();
        assert!(get_array_size_semantics(d.root_element()).unwrap());
    }

    #[test]
    fn check_valid_basic_types() {
        assert!(valid_basic_type("/Base/uint32").is_ok());
        assert!(valid_basic_type("float").is_ok());
        assert!(valid_basic_type("bool").is_ok());
    }

    #[test]
    fn check_invalid_basic_type() {
        assert!(valid_basic_type("/Some/WeirdType").is_err());
    }

    #[test]
    fn read_ar_packages() {
        let d = doc();
        let r = get_ar_packages_element(d.root_element()).unwrap();
        assert!(find_child(r, "AR-PACKAGE").is_some());
    }
}
