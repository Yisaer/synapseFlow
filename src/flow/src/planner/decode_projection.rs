use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeProjection {
    version: u64,
    columns: BTreeMap<String, ProjectionNode>,
    /// VF-56: when set, the decoder emits a narrow, slot-ordered output row whose
    /// index space is exactly this list (slot == position), instead of a full-width
    /// schema-ordered row. `None` keeps the legacy full-width behavior. Carried here
    /// because the consumer decode path already reads this projection per-decode, so
    /// the slice width tracks the union live (append-only) with no extra plumbing.
    output_slots: Option<Arc<[Arc<str>]>>,
}

impl DecodeProjection {
    /// Monotonic version of the projection. Used by decoders to detect projection changes and
    /// refresh any cached decode state.
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn columns(&self) -> &BTreeMap<String, ProjectionNode> {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ProjectionNode> {
        self.columns.get(name)
    }

    pub fn mark_column_all(&mut self, name: &str) {
        self.columns.insert(name.to_string(), ProjectionNode::All);
    }

    /// Build a decode projection that includes the given top-level columns with an explicit
    /// projection version.
    pub fn from_top_level_columns_with_version(columns: &[String], version: u64) -> Self {
        let mut projection = Self {
            version,
            columns: BTreeMap::new(),
            output_slots: None,
        };
        for column in columns {
            projection.mark_column_all(column);
        }
        projection
    }

    /// Slot-ordered output columns (VF-56). When `Some`, the decoder emits a narrow
    /// row whose width and order match this list (slot == index).
    pub fn output_slots(&self) -> Option<&Arc<[Arc<str>]>> {
        self.output_slots.as_ref()
    }

    /// Attach a slot-ordered output layout (VF-56 shared-stream slice projection).
    pub fn with_output_slots(mut self, slots: Arc<[Arc<str>]>) -> Self {
        self.output_slots = Some(slots);
        self
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
                if let ProjectionNode::Struct(fields) = node {
                    let child = fields
                        .entry(field.to_string())
                        .or_insert_with(|| ProjectionNode::Struct(BTreeMap::new()));
                    if rest.is_empty() {
                        *child = ProjectionNode::All;
                        return;
                    }
                    Self::mark_segments_used_in_node(child, rest);
                }
            }
            FieldPathSegment::ListIndex(index) => {
                if !matches!(node, ProjectionNode::List { .. }) {
                    *node = ProjectionNode::List {
                        indexes: ListIndexSelection::Indexes(BTreeSet::new()),
                        element: Box::new(ProjectionNode::Struct(BTreeMap::new())),
                    };
                }
                if let ProjectionNode::List { indexes, element } = node {
                    match index {
                        ListIndex::Const(value) => indexes.add_index(*value),
                        ListIndex::Dynamic => indexes.mark_all(),
                    }

                    if rest.is_empty() {
                        **element = ProjectionNode::All;
                        return;
                    }
                    Self::mark_segments_used_in_node(element.as_mut(), rest);
                }
            }
        }
    }
}

impl Default for DecodeProjection {
    fn default() -> Self {
        Self {
            version: 1,
            columns: BTreeMap::new(),
            output_slots: None,
        }
    }
}
