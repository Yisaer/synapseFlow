//! In-process memory pub/sub used by memory source/sink connectors.

use crate::model::Collection;
use bytes::Bytes;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

pub const DEFAULT_MEMORY_PUBSUB_CAPACITY: usize = 1024;

/// A cheaply cloneable `Collection` wrapper suitable for broadcast fan-out.
#[derive(Clone)]
pub struct SharedCollection(Arc<dyn Collection>);

impl SharedCollection {
    pub fn new(inner: Arc<dyn Collection>) -> Self {
        Self(inner)
    }

    pub fn from_box(inner: Box<dyn Collection>) -> Self {
        Self(Arc::from(inner))
    }

    pub fn inner(&self) -> &Arc<dyn Collection> {
        &self.0
    }
}

impl std::fmt::Debug for SharedCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedCollection").finish()
    }
}

impl Collection for SharedCollection {
    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn rows(&self) -> &[crate::model::Tuple] {
        self.0.rows()
    }

    fn slice(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.slice(start, end)
    }

    fn take(
        &self,
        indices: &[usize],
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.take(indices)
    }

    fn apply_projection(
        &self,
        fields: &[crate::planner::physical::PhysicalProjectField],
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.apply_projection(fields)
    }

    fn apply_filter(
        &self,
        filter_expr: &crate::expr::ScalarExpr,
    ) -> Result<Box<dyn Collection>, crate::model::CollectionError> {
        self.0.apply_filter(filter_expr)
    }

    fn clone_box(&self) -> Box<dyn Collection> {
        Box::new(self.clone())
    }

    fn into_rows(
        self: Box<Self>,
    ) -> Result<Vec<crate::model::Tuple>, crate::model::CollectionError> {
        self.0.clone_box().into_rows()
    }
}

/// Payload types that can be published via the memory pub/sub topics.
#[derive(Clone, Debug)]
pub enum MemoryData {
    Bytes(Bytes),
    Collection(SharedCollection),
}

#[derive(thiserror::Error, Debug)]
pub enum MemoryPubSubError {
    #[error("topic must not be empty")]
    InvalidTopic,
}

#[derive(Clone)]
pub struct MemoryPubSubRegistry {
    topics: Arc<RwLock<HashMap<String, TopicEntry>>>,
}

#[derive(Clone)]
struct TopicEntry {
    sender: broadcast::Sender<MemoryData>,
    capacity: usize,
}

impl MemoryPubSubRegistry {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn publisher(
        &self,
        topic: &str,
        capacity: Option<usize>,
    ) -> Result<broadcast::Sender<MemoryData>, MemoryPubSubError> {
        Ok(self.ensure_topic(topic, capacity)?.sender)
    }

    pub fn subscribe(
        &self,
        topic: &str,
        capacity: Option<usize>,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        Ok(self.ensure_topic(topic, capacity)?.sender.subscribe())
    }

    fn ensure_topic(
        &self,
        topic: &str,
        capacity: Option<usize>,
    ) -> Result<TopicEntry, MemoryPubSubError> {
        if topic.trim().is_empty() {
            return Err(MemoryPubSubError::InvalidTopic);
        }
        let cap = capacity.unwrap_or(DEFAULT_MEMORY_PUBSUB_CAPACITY).max(1);

        {
            let guard = self.topics.read().expect("memory pubsub registry poisoned");
            if let Some(entry) = guard.get(topic) {
                return Ok(entry.clone());
            }
        }

        let mut guard = self
            .topics
            .write()
            .expect("memory pubsub registry poisoned");
        Ok(guard
            .entry(topic.to_string())
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(cap);
                TopicEntry {
                    sender,
                    capacity: cap,
                }
            })
            .clone())
    }

    pub fn topic_capacity(&self, topic: &str) -> Option<usize> {
        let guard = self.topics.read().expect("memory pubsub registry poisoned");
        guard.get(topic).map(|entry| entry.capacity)
    }
}

impl Default for MemoryPubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Accessor for the process-wide memory pub/sub registry.
pub fn registry() -> &'static MemoryPubSubRegistry {
    static REGISTRY: Lazy<MemoryPubSubRegistry> = Lazy::new(MemoryPubSubRegistry::new);
    &REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use datatypes::Value;
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tokio::time::timeout;

    fn as_label(data: &MemoryData) -> String {
        match data {
            MemoryData::Bytes(bytes) => format!(
                "bytes:{}",
                std::str::from_utf8(bytes.as_ref()).expect("utf8 bytes")
            ),
            MemoryData::Collection(collection) => {
                format!("collection:rows={}", collection.num_rows())
            }
        }
    }

    fn batch_with_rows(rows: i64) -> Box<dyn Collection> {
        let values = (0..rows).map(Value::Int64).collect::<Vec<_>>();
        let batch =
            batch_from_columns_simple(vec![("memory".to_string(), "v".to_string(), values)])
                .expect("build batch");
        Box::new(batch)
    }

    async fn recv_labels(
        rx: &mut broadcast::Receiver<MemoryData>,
        count: usize,
    ) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for _ in 0..count {
            let item = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("receive timeout")
                .expect("receive item");
            out.insert(as_label(&item));
        }
        out
    }

    fn expect_single_publisher_payloads() -> (Vec<MemoryData>, BTreeSet<String>) {
        let mut items = Vec::new();
        let mut expected = BTreeSet::new();
        for idx in 0..5 {
            let label = format!("p1:b{idx}");
            expected.insert(format!("bytes:{label}"));
            items.push(MemoryData::Bytes(Bytes::from(label)));
        }
        expected.insert("collection:rows=1".to_string());
        items.push(MemoryData::Collection(SharedCollection::from_box(
            batch_with_rows(1),
        )));
        (items, expected)
    }

    fn expect_dual_publisher_payloads() -> (Vec<MemoryData>, Vec<MemoryData>, BTreeSet<String>) {
        let (p1_items, mut expected) = expect_single_publisher_payloads();

        let mut p2_items = Vec::new();
        for idx in 0..3 {
            let label = format!("p2:b{idx}");
            expected.insert(format!("bytes:{label}"));
            p2_items.push(MemoryData::Bytes(Bytes::from(label)));
        }
        expected.insert("collection:rows=2".to_string());
        p2_items.push(MemoryData::Collection(SharedCollection::from_box(
            batch_with_rows(2),
        )));

        (p1_items, p2_items, expected)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_single_pub_single_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "single_pub_single_sub";
        let mut rx = registry.subscribe(topic, None).expect("subscribe");
        let tx = registry.publisher(topic, None).expect("publisher");

        let (items, expected) = expect_single_publisher_payloads();
        for item in items {
            tx.send(item).expect("send");
        }

        let got = recv_labels(&mut rx, expected.len()).await;
        assert_eq!(got, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_two_pub_one_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "two_pub_one_sub";
        let mut rx = registry.subscribe(topic, None).expect("subscribe");
        let tx1 = registry.publisher(topic, None).expect("publisher1");
        let tx2 = registry.publisher(topic, None).expect("publisher2");

        let (p1_items, p2_items, expected) = expect_dual_publisher_payloads();

        let t1 = tokio::spawn(async move {
            for item in p1_items {
                tx1.send(item).expect("send p1");
            }
        });
        let t2 = tokio::spawn(async move {
            for item in p2_items {
                tx2.send(item).expect("send p2");
            }
        });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("publisher1 task");
        r2.expect("publisher2 task");

        let got = recv_labels(&mut rx, expected.len()).await;
        assert_eq!(got, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_one_pub_two_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "one_pub_two_sub";
        let mut rx1 = registry.subscribe(topic, None).expect("subscribe1");
        let mut rx2 = registry.subscribe(topic, None).expect("subscribe2");
        let tx = registry.publisher(topic, None).expect("publisher");

        let (items, expected) = expect_single_publisher_payloads();
        for item in items {
            tx.send(item).expect("send");
        }

        let got1 = recv_labels(&mut rx1, expected.len()).await;
        let got2 = recv_labels(&mut rx2, expected.len()).await;
        assert_eq!(got1, expected);
        assert_eq!(got2, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_two_pub_two_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "two_pub_two_sub";
        let mut rx1 = registry.subscribe(topic, None).expect("subscribe1");
        let mut rx2 = registry.subscribe(topic, None).expect("subscribe2");
        let tx1 = registry.publisher(topic, None).expect("publisher1");
        let tx2 = registry.publisher(topic, None).expect("publisher2");

        let (p1_items, p2_items, expected) = expect_dual_publisher_payloads();

        let t1 = tokio::spawn(async move {
            for item in p1_items {
                tx1.send(item).expect("send p1");
            }
        });
        let t2 = tokio::spawn(async move {
            for item in p2_items {
                tx2.send(item).expect("send p2");
            }
        });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("publisher1 task");
        r2.expect("publisher2 task");

        let got1 = recv_labels(&mut rx1, expected.len()).await;
        let got2 = recv_labels(&mut rx2, expected.len()).await;
        assert_eq!(got1, expected);
        assert_eq!(got2, expected);
    }

    // Lag semantics are tested at connector/processor layers where we decide how to log and recover.
}
