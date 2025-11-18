use datatypes::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Immutable data from a single source.
#[derive(Debug)]
pub struct Message {
    source: String,
    index: HashMap<Arc<String>, usize>,
    values: Vec<Value>,
}

impl Message {
    pub fn new(source: impl Into<String>, columns: Vec<(Arc<String>, Value)>) -> Self {
        let mut index = HashMap::with_capacity(columns.len());
        let mut values = Vec::with_capacity(columns.len());
        for (name, value) in columns {
            index.insert(name.clone(), values.len());
            values.push(value);
        }
        Self {
            source: source.into(),
            index,
            values,
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Arc<String>, &Value)> {
        self.index
            .iter()
            .map(move |(name, idx)| (name, &self.values[*idx]))
    }

    pub fn value(&self, column: &str) -> Option<&Value> {
        self.index
            .get(&Arc::new(column.to_string()))
            .and_then(|idx| self.values.get(*idx))
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
                out.push(((msg.source(), name.as_str()), value));
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
}
