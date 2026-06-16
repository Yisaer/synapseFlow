use datatypes::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Layout mapping between source-schema logical indices and compact physical indices
/// for projected (sparse) messages.
///
/// This struct is cheaply clonable via `Arc`; it is shared across all messages
/// decoded within the same projection generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedLayout {
    /// source-schema logical index -> compact physical index.
    /// `None` means the column is not materialised in this message.
    pub logical_to_physical: Arc<[Option<usize>]>,
    /// compact physical index -> source-schema logical index.
    /// Provides O(1) iteration over materialised columns for `entries()`.
    pub physical_to_logical: Arc<[usize]>,
}

impl ProjectedLayout {
    /// Build a projected layout from a full source-schema key list and a set of
    /// actively-decoded column names.
    pub fn from_active_columns(schema_keys: &[Arc<str>], active_columns: &[String]) -> Self {
        let n = schema_keys.len();
        let mut logical_to_physical = vec![None; n];
        let mut physical_to_logical = Vec::with_capacity(active_columns.len());
        for col_name in active_columns {
            if let Some(logical_idx) = schema_keys
                .iter()
                .position(|k| k.as_ref() == col_name.as_str())
            {
                logical_to_physical[logical_idx] = Some(physical_to_logical.len());
                physical_to_logical.push(logical_idx);
            }
        }
        ProjectedLayout {
            logical_to_physical: Arc::from(logical_to_physical),
            physical_to_logical: Arc::from(physical_to_logical),
        }
    }

    /// Number of materialised (decoded) columns in this layout.
    pub fn materialised_count(&self) -> usize {
        self.physical_to_logical.len()
    }
}

/// Physical value storage for a [`Message`].
#[derive(Debug)]
pub enum MessageValues {
    /// Default dense representation: `values[i]` maps directly to source-schema column `i`.
    Dense(Vec<Arc<Value>>),
    /// Projected/sparse representation: only actively-decoded columns are stored.
    /// Column lookups go through [`ProjectedLayout::logical_to_physical`].
    Projected {
        values: Vec<Arc<Value>>,
        layout: Arc<ProjectedLayout>,
    },
}

/// Iterator over [`Message`] entries, avoiding heap allocation through a concrete enum.
pub enum MessageEntries<'a> {
    /// Full source-schema iteration (Dense).
    Dense(std::iter::Zip<std::slice::Iter<'a, Arc<str>>, std::slice::Iter<'a, Arc<Value>>>),
    /// Materialised-columns-only iteration (Projected).
    Projected {
        keys: &'a [Arc<str>],
        values: &'a [Arc<Value>],
        logical_to_physical: &'a [Option<usize>],
        physical_to_logical: std::slice::Iter<'a, usize>,
    },
}

impl<'a> Iterator for MessageEntries<'a> {
    type Item = (&'a str, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MessageEntries::Dense(iter) => iter.next().map(|(k, v)| (k.as_ref(), v.as_ref())),
            MessageEntries::Projected {
                keys,
                values,
                logical_to_physical,
                physical_to_logical,
            } => {
                let logical = physical_to_logical.next()?;
                let phys = logical_to_physical[*logical].expect(
                    "physical_to_logical entry must have a valid logical_to_physical mapping",
                );
                Some((keys[*logical].as_ref(), values[phys].as_ref()))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            MessageEntries::Dense(iter) => iter.size_hint(),
            MessageEntries::Projected {
                physical_to_logical,
                ..
            } => physical_to_logical.size_hint(),
        }
    }
}

/// Immutable data from a single source.
#[derive(Debug)]
pub struct Message {
    source: Arc<str>,
    keys: Arc<[Arc<str>]>,
    values: MessageValues,
}

impl Message {
    pub fn new(source: impl Into<Arc<str>>, keys: Vec<Arc<str>>, values: Vec<Arc<Value>>) -> Self {
        Self::new_shared_keys(source, Arc::from(keys), values)
    }

    pub fn new_shared_keys(
        source: impl Into<Arc<str>>,
        keys: Arc<[Arc<str>]>,
        values: Vec<Arc<Value>>,
    ) -> Self {
        debug_assert_eq!(
            keys.len(),
            values.len(),
            "Message keys and values length must match"
        );
        Self {
            source: source.into(),
            keys,
            values: MessageValues::Dense(values),
        }
    }

    /// Create a projected message with compact values and a shared layout.
    ///
    /// `keys` must represent the full source-schema logical keys (same as for
    /// [`new_shared_keys`]). `compact_values` contains only the actively-decoded
    /// columns. `layout` maps between logical and physical indices.
    pub fn new_projected(
        source: impl Into<Arc<str>>,
        keys: Arc<[Arc<str>]>,
        compact_values: Vec<Arc<Value>>,
        layout: Arc<ProjectedLayout>,
    ) -> Self {
        debug_assert_eq!(
            compact_values.len(),
            layout.materialised_count(),
            "compact_values length must match layout materialised count"
        );
        Self {
            source: source.into(),
            keys,
            values: MessageValues::Projected {
                values: compact_values,
                layout,
            },
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the number of stored value slots (dense width or compact width).
    pub fn values_len(&self) -> usize {
        match &self.values {
            MessageValues::Dense(v) => v.len(),
            MessageValues::Projected { values, .. } => values.len(),
        }
    }

    /// Return the source-schema logical index for the given column name, if present.
    pub fn key_index(&self, column: &str) -> Option<usize> {
        self.keys.iter().position(|k| k.as_ref() == column)
    }

    /// Iterate over decoded columns.
    ///
    /// - Dense: yields all source-schema columns.
    /// - Projected: yields **only** materialised (actively-decoded) columns.
    pub fn entries(&self) -> MessageEntries<'_> {
        match &self.values {
            MessageValues::Dense(v) => MessageEntries::Dense(self.keys.iter().zip(v.iter())),
            MessageValues::Projected { values, layout } => MessageEntries::Projected {
                keys: &self.keys,
                values,
                logical_to_physical: &layout.logical_to_physical,
                physical_to_logical: layout.physical_to_logical.iter(),
            },
        }
    }

    /// Look up a (key, value) pair by source-schema logical `index`.
    pub fn entry_by_index(&self, index: usize) -> Option<(&Arc<str>, &Arc<Value>)> {
        match &self.values {
            MessageValues::Dense(v) => self.keys.get(index).zip(v.get(index)),
            MessageValues::Projected { values, layout } => {
                let phys = layout.logical_to_physical.get(index).copied()??;
                let value = values.get(phys)?;
                self.keys.get(index).map(|k| (k, value))
            }
        }
    }

    pub fn entry_by_name(&self, column: &str) -> Option<(&Arc<str>, &Arc<Value>)> {
        self.keys
            .iter()
            .position(|k| k.as_ref() == column)
            .and_then(|idx| self.entry_by_index(idx))
    }

    /// Look up a value by source-schema column name.
    pub fn value(&self, column: &str) -> Option<&Value> {
        let idx = self.keys.iter().position(|k| k.as_ref() == column)?;
        self.value_by_index(idx)
    }

    /// Look up a value by source-schema logical `index`.
    ///
    /// For projected messages this goes through [`ProjectedLayout::logical_to_physical`].
    /// If the column was not decoded in this projection generation, `None` is returned.
    pub fn value_by_index(&self, index: usize) -> Option<&Value> {
        match &self.values {
            MessageValues::Dense(v) => v.get(index).map(|v| v.as_ref()),
            MessageValues::Projected { values, layout } => {
                let phys = layout.logical_to_physical.get(index).copied()??;
                values.get(phys).map(|v: &Arc<Value>| v.as_ref())
            }
        }
    }
}

/// Derived columns without specific source binding.
#[derive(Debug, Clone)]
pub struct AffiliateRow {
    index: HashMap<Arc<str>, usize>,
    values: Vec<Value>,
}

impl AffiliateRow {
    pub fn new(entries: Vec<(Arc<String>, Value)>) -> Self {
        let mut index = HashMap::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            // Store affiliate keys as `Arc<str>` so lookup by `&str` doesn't allocate.
            index.insert(Arc::<str>::from(key.as_str()), values.len());
            values.push(value);
        }
        Self { index, values }
    }

    /// Insert or overwrite a derived column.
    pub fn insert(&mut self, key: Arc<String>, value: Value) {
        // Avoid allocating for the hot-path lookup (`HashMap<Arc<str>, _>::get(&str)`).
        if let Some(idx) = self.index.get(key.as_str()).copied() {
            self.values[idx] = value;
        } else {
            let idx = self.values.len();
            self.values.push(value);
            self.index.insert(Arc::<str>::from(key.as_str()), idx);
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Arc<str>, &Value)> {
        self.index
            .iter()
            .map(move |(key, idx)| (key, &self.values[*idx]))
    }

    pub fn value(&self, key: &str) -> Option<&Value> {
        self.index.get(key).and_then(|idx| self.values.get(*idx))
    }
}

/// Tuple combining source messages and optional derived columns.
#[derive(Debug, Clone)]
pub struct Tuple {
    pub messages: Arc<[Arc<Message>]>,
    affiliate: Option<Arc<AffiliateRow>>,
    output_mask: Option<Arc<[bool]>>,
    pub timestamp: SystemTime,
}

impl Tuple {
    pub fn empty_messages() -> Arc<[Arc<Message>]> {
        Arc::from(Vec::<Arc<Message>>::new())
    }

    pub fn new(messages: Vec<Arc<Message>>) -> Self {
        Self::with_timestamp(Arc::from(messages), SystemTime::now())
    }

    pub fn with_timestamp(messages: Arc<[Arc<Message>]>, timestamp: SystemTime) -> Self {
        Self {
            messages,
            affiliate: None,
            output_mask: None,
            timestamp,
        }
    }

    pub fn affiliate(&self) -> Option<&AffiliateRow> {
        self.affiliate.as_ref().map(|aff| aff.as_ref())
    }

    pub fn output_mask(&self) -> Option<&[bool]> {
        self.output_mask.as_deref()
    }

    pub fn output_mask_shared(&self) -> Option<Arc<[bool]>> {
        self.output_mask.as_ref().map(Arc::clone)
    }

    pub fn set_output_mask(&mut self, mask: Vec<bool>) {
        self.output_mask = Some(Arc::from(mask));
    }

    pub fn set_output_mask_shared(&mut self, mask: Arc<[bool]>) {
        self.output_mask = Some(mask);
    }

    pub fn clear_output_mask(&mut self) {
        self.output_mask = None;
    }

    pub(crate) fn affiliate_mut(&mut self) -> &mut AffiliateRow {
        let affiliate = self
            .affiliate
            .get_or_insert_with(|| Arc::new(AffiliateRow::new(Vec::new())));
        Arc::make_mut(affiliate)
    }

    pub fn add_affiliate_column(&mut self, column: Arc<String>, value: Value) {
        self.affiliate_mut().insert(column, value);
    }

    pub fn add_affiliate_columns(
        &mut self,
        entries: impl IntoIterator<Item = (Arc<String>, Value)>,
    ) {
        let affiliate = self.affiliate_mut();
        for (column, value) in entries {
            affiliate.insert(column, value);
        }
    }

    pub fn entries(&self) -> Vec<((&str, &str), &Value)> {
        let mut out = Vec::new();
        if let Some(aff) = self.affiliate() {
            for (key, value) in aff.entries() {
                out.push((("", key.as_ref()), value));
            }
        }
        for msg in self.messages.iter() {
            for (name, value) in msg.entries() {
                out.push(((msg.source(), name), value));
            }
        }
        out
    }

    pub fn value_by_name(&self, source: &str, column: &str) -> Option<&Value> {
        if source.is_empty() {
            if let Some(aff) = self.affiliate() {
                return aff.value(column);
            }
            return None;
        }
        self.messages.iter().find_map(|msg| {
            if msg.source() != source {
                return None;
            }
            msg.value(column)
        })
    }

    pub fn value_by_index(&self, source: &str, index: usize) -> Option<&Value> {
        if source.is_empty() {
            return None;
        }
        self.messages.iter().find_map(|msg| {
            if msg.source() != source {
                return None;
            }
            msg.value_by_index(index)
        })
    }

    pub fn len(&self) -> usize {
        let aff_len = self
            .affiliate
            .as_ref()
            .map(|aff| aff.index.len())
            .unwrap_or(0);
        let msg_len: usize = self.messages.iter().map(|msg| msg.values_len()).sum();
        aff_len + msg_len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn messages(&self) -> &[Arc<Message>] {
        self.messages.as_ref()
    }

    pub fn message_by_source(&self, source: &str) -> Option<&Arc<Message>> {
        if source.is_empty() && self.messages.len() == 1 {
            return self.messages.first();
        }
        self.messages.iter().find(|msg| msg.source() == source)
    }
}
