use crate::model::ProjectedLayout;
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
    /// Pre-computed projected layout for the schema this projection applies to.
    /// Set by the shared stream when decoding columns change; `None` for other
    /// usage (e.g. planner projections that don't need sparse messages).
    projected_layout: Option<Arc<ProjectedLayout>>,
}

impl DecodeProjection {
    /// Monotonic version of the projection. Used by decoders to detect projection changes and
    /// refresh any cached decode state.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Attach a pre-computed projected layout for sparse message output.
    pub fn with_projected_layout(mut self, layout: Arc<ProjectedLayout>) -> Self {
        self.projected_layout = Some(layout);
        self
    }

    /// The projected layout, if one was attached.
    pub fn projected_layout(&self) -> Option<&Arc<ProjectedLayout>> {
        self.projected_layout.as_ref()
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
            projected_layout: None,
        };
        for column in columns {
            projection.mark_column_all(column);
        }
        projection
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
            projected_layout: None,
        }
    }
}
