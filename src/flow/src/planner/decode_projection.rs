use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldPathSegment {
    StructField(String),
    ListIndex(ListIndex),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ListIndex {
    Const(usize),
    Dynamic,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath {
    pub column: String,
    pub segments: Vec<FieldPathSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListIndexSelection {
    All,
    Indexes(BTreeSet<usize>),
}

impl ListIndexSelection {
    fn add_index(&mut self, index: usize) {
        match self {
            ListIndexSelection::All => {}
            ListIndexSelection::Indexes(indexes) => {
                indexes.insert(index);
            }
        }
    }

    fn mark_all(&mut self) {
        *self = ListIndexSelection::All;
    }

    pub fn is_all(&self) -> bool {
        matches!(self, ListIndexSelection::All)
    }

    pub fn indexes(&self) -> Option<&BTreeSet<usize>> {
        match self {
            ListIndexSelection::All => None,
            ListIndexSelection::Indexes(indexes) => Some(indexes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionNode {
    All,
    Struct(BTreeMap<String, ProjectionNode>),
    List {
        indexes: ListIndexSelection,
        element: Box<ProjectionNode>,
    },
}

impl ProjectionNode {
    fn as_struct_mut(&mut self) -> Option<&mut BTreeMap<String, ProjectionNode>> {
        match self {
            ProjectionNode::Struct(fields) => Some(fields),
            _ => None,
        }
    }

    fn as_list_mut(&mut self) -> Option<(&mut ListIndexSelection, &mut ProjectionNode)> {
        match self {
            ProjectionNode::List { indexes, element } => Some((indexes, element.as_mut())),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecodeProjection {
    columns: BTreeMap<String, ProjectionNode>,
}

impl DecodeProjection {
    pub fn columns(&self) -> &BTreeMap<String, ProjectionNode> {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ProjectionNode> {
        self.columns.get(name)
    }

    pub fn mark_column_all(&mut self, name: &str) {
        self.columns.insert(name.to_string(), ProjectionNode::All);
    }

    pub fn mark_field_path_used(&mut self, path: &FieldPath) {
        self.mark_segments_used(path.column.as_str(), path.segments.as_slice());
    }

    pub fn mark_segments_used(&mut self, column: &str, segments: &[FieldPathSegment]) {
        let node = self
            .columns
            .entry(column.to_string())
            .or_insert_with(|| ProjectionNode::Struct(BTreeMap::new()));

        if segments.is_empty() {
            *node = ProjectionNode::All;
            return;
        }

        Self::mark_segments_used_in_node(node, segments);
    }

    fn mark_segments_used_in_node(node: &mut ProjectionNode, segments: &[FieldPathSegment]) {
        if matches!(node, ProjectionNode::All) {
            return;
        }

        let Some((first, rest)) = segments.split_first() else {
            *node = ProjectionNode::All;
            return;
        };

        match first {
            FieldPathSegment::StructField(field) => {
                if !matches!(node, ProjectionNode::Struct(_)) {
                    *node = ProjectionNode::Struct(BTreeMap::new());
                }
                let fields = node.as_struct_mut().expect("set above");
                let child = fields
                    .entry(field.to_string())
                    .or_insert_with(|| ProjectionNode::Struct(BTreeMap::new()));
                if rest.is_empty() {
                    *child = ProjectionNode::All;
                    return;
                }
                Self::mark_segments_used_in_node(child, rest);
            }
            FieldPathSegment::ListIndex(index) => {
                if !matches!(node, ProjectionNode::List { .. }) {
                    *node = ProjectionNode::List {
                        indexes: ListIndexSelection::Indexes(BTreeSet::new()),
                        element: Box::new(ProjectionNode::Struct(BTreeMap::new())),
                    };
                }
                let (indexes, element) = node.as_list_mut().expect("set above");
                match index {
                    ListIndex::Const(value) => indexes.add_index(*value),
                    ListIndex::Dynamic => indexes.mark_all(),
                }

                if rest.is_empty() {
                    *element = ProjectionNode::All;
                    return;
                }
                Self::mark_segments_used_in_node(element, rest);
            }
        }
    }
}
