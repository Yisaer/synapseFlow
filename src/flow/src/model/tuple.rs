use datatypes::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Immutable data from a single source.
#[derive(Debug)]
pub struct Message {
    source: Arc<str>,
    keys: Vec<Arc<str>>,
    values: Vec<Arc<Value>>,
}

impl Message {
    pub fn new(
        source: impl Into<Arc<str>>,
        keys: Vec<Arc<str>>,
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
        self.keys
            .get(index)
            .and_then(|k| self.values.get(index).map(|v| (k, v)))
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
    index: HashMap<Arc<String>, usize>,
    values: Vec<Value>,
}

impl AffiliateRow {
    pub fn new(entries: Vec<(Arc<String>, Value)>) -> Self {
        let mut index = HashMap::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            index.insert(key.clone(), values.len());
            values.push(value);
        }
        Self { index, values }
    }

    /// Insert or overwrite a derived column.
    pub fn insert(&mut self, key: Arc<String>, value: Value) {
        if let Some(idx) = self.index.get(&key).copied() {
            self.values[idx] = value;
        } else {
            let idx = self.values.len();
            self.values.push(value);
            self.index.insert(key, idx);
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Arc<String>, &Value)> {
        self.index
            .iter()
            .map(move |(key, idx)| (key, &self.values[*idx]))
    }

    pub fn value(&self, key: &str) -> Option<&Value> {
        self.index
            .get(&Arc::new(key.to_string()))
            .and_then(|idx| self.values.get(*idx))
    }
}

/// Tuple combining source messages and optional derived columns.
#[derive(Debug, Clone)]
pub struct Tuple {
    pub messages: Vec<Arc<Message>>,
    pub affiliate: Option<AffiliateRow>,
}

impl Tuple {
    pub fn new(messages: Vec<Arc<Message>>) -> Self {
        Self {
            messages,
            affiliate: None,
        }
    }

    pub fn add_affiliate_column(&mut self, column: Arc<String>, value: Value) {
        match &mut self.affiliate {
            Some(aff) => aff.insert(column, value),
            None => self.affiliate = Some(AffiliateRow::new(vec![(column, value)])),
        }
    }

    pub fn entries(&self) -> Vec<((&str, &str), &Value)> {
        let mut out = Vec::new();
        if let Some(aff) = &self.affiliate {
            for (key, value) in aff.entries() {
                out.push((("", key.as_str()), value));
            }
        }
        for msg in &self.messages {
            for (name, value) in msg.entries() {
                out.push(((msg.source(), name), value));
            }
        }
        out
    }

    pub fn value_by_name(&self, source: &str, column: &str) -> Option<&Value> {
        if source.is_empty() {
            if let Some(aff) = &self.affiliate {
                return aff.value(column);
            }
            return None;
        }
        self.messages
            .iter()
            .find(|msg| msg.source() == source)
            .and_then(|msg| msg.value(column))
    }

    pub fn value_by_index(&self, source: &str, index: usize) -> Option<&Value> {
        if source.is_empty() {
            return None;
        }
        self.messages
            .iter()
            .find(|msg| msg.source() == source)
            .and_then(|msg| msg.value_by_index(index))
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
        &self.messages
    }

    pub fn message_by_source(&self, source: &str) -> Option<&Arc<Message>> {
        if source.is_empty() && self.messages.len() == 1 {
            return self.messages.first();
        }
        self.messages.iter().find(|msg| msg.source() == source)
    }
}
