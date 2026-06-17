use datatypes::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Immutable data from a single source.
#[derive(Debug)]
pub struct Message {
    source: Arc<str>,
    keys: Arc<[Arc<str>]>,
    values: Vec<Arc<Value>>,
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
            values,
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.keys
            .iter()
            .zip(self.values.iter())
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    pub fn entry_by_index(&self, index: usize) -> Option<(&Arc<str>, &Arc<Value>)> {
        self.keys.get(index).zip(self.values.get(index))
    }

    pub fn entry_by_name(&self, column: &str) -> Option<(&Arc<str>, &Arc<Value>)> {
        self.keys
            .iter()
            .position(|k| k.as_ref() == column)
            .and_then(|idx| self.entry_by_index(idx))
    }

    pub fn value(&self, column: &str) -> Option<&Value> {
        self.keys
            .iter()
            .position(|k| k.as_ref() == column)
            .and_then(|idx| self.values.get(idx).map(|v| v.as_ref()))
    }

    pub fn value_by_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index).map(|v| v.as_ref())
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
        let msg_len: usize = self.messages.iter().map(|msg| msg.values.len()).sum();
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
