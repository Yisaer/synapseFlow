//! In-process memory pub/sub used by memory source/sink connectors.

use crate::model::Collection;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

pub const DEFAULT_MEMORY_PUBSUB_CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryTopicKind {
    Bytes,
    Collection,
}

impl std::fmt::Display for MemoryTopicKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryTopicKind::Bytes => f.write_str("bytes"),
            MemoryTopicKind::Collection => f.write_str("collection"),
        }
    }
}

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

    fn metadata(&self) -> &crate::model::CollectionMetadata {
        self.0.metadata()
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
    #[error("topic `{topic}` not found")]
    NotFound { topic: String },
    #[error("topic `{topic}` kind mismatch: expected {expected}, got {actual}")]
    TopicKindMismatch {
        topic: String,
        expected: MemoryTopicKind,
        actual: MemoryTopicKind,
    },
    #[error("topic `{topic}` capacity mismatch: expected {expected}, got {actual}")]
    TopicCapacityMismatch {
        topic: String,
        expected: usize,
        actual: usize,
    },
    #[error(
        "timeout waiting for subscribers on topic `{topic}`: expected >= {expected}, got {actual}"
    )]
    TimeoutWaitingForSubscribers {
        topic: String,
        expected: usize,
        actual: usize,
    },
}

#[derive(Clone, Debug)]
pub struct MemoryPublisher {
    topic: String,
    kind: MemoryTopicKind,
    sender: broadcast::Sender<MemoryData>,
}

impl MemoryPublisher {
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn kind(&self) -> MemoryTopicKind {
        self.kind
    }

    pub fn publish_bytes(&self, payload: impl Into<Bytes>) -> Result<usize, MemoryPubSubError> {
        if self.kind != MemoryTopicKind::Bytes {
            return Err(MemoryPubSubError::TopicKindMismatch {
                topic: self.topic.clone(),
                expected: self.kind,
                actual: MemoryTopicKind::Bytes,
            });
        }
        Ok(self
            .sender
            .send(MemoryData::Bytes(payload.into()))
            .unwrap_or(0))
    }

    pub fn publish_collection(
        &self,
        collection: SharedCollection,
    ) -> Result<usize, MemoryPubSubError> {
        if self.kind != MemoryTopicKind::Collection {
            return Err(MemoryPubSubError::TopicKindMismatch {
                topic: self.topic.clone(),
                expected: self.kind,
                actual: MemoryTopicKind::Collection,
            });
        }
        Ok(self
            .sender
            .send(MemoryData::Collection(collection))
            .unwrap_or(0))
    }
}

#[derive(Clone)]
pub struct MemoryPubSubRegistry {
    topics: Arc<RwLock<HashMap<String, TopicEntry>>>,
}

#[derive(Clone)]
struct TopicEntry {
    sender: broadcast::Sender<MemoryData>,
    capacity: usize,
    kind: MemoryTopicKind,
}

impl MemoryPubSubRegistry {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn declare_topic(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
        capacity: usize,
    ) -> Result<(), MemoryPubSubError> {
        if topic.trim().is_empty() {
            return Err(MemoryPubSubError::InvalidTopic);
        }
        let cap = capacity.max(1);

        {
            let guard = self.topics.read();
            if let Some(entry) = guard.get(topic) {
                if entry.kind != kind {
                    return Err(MemoryPubSubError::TopicKindMismatch {
                        topic: topic.to_string(),
                        expected: entry.kind,
                        actual: kind,
                    });
                }
                if entry.capacity != cap {
                    return Err(MemoryPubSubError::TopicCapacityMismatch {
                        topic: topic.to_string(),
                        expected: entry.capacity,
                        actual: cap,
                    });
                }
                return Ok(());
            }
        }

        let mut guard = self.topics.write();
        guard.entry(topic.to_string()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(cap);
            TopicEntry {
                sender,
                capacity: cap,
                kind,
            }
        });
        Ok(())
    }

    pub fn open_publisher_bytes(&self, topic: &str) -> Result<MemoryPublisher, MemoryPubSubError> {
        let entry = self.get_existing_topic(topic, MemoryTopicKind::Bytes)?;
        Ok(MemoryPublisher {
            topic: topic.to_string(),
            kind: MemoryTopicKind::Bytes,
            sender: entry.sender,
        })
    }

    pub fn open_publisher_collection(
        &self,
        topic: &str,
    ) -> Result<MemoryPublisher, MemoryPubSubError> {
        let entry = self.get_existing_topic(topic, MemoryTopicKind::Collection)?;
        Ok(MemoryPublisher {
            topic: topic.to_string(),
            kind: MemoryTopicKind::Collection,
            sender: entry.sender,
        })
    }

    fn get_existing_topic(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
    ) -> Result<TopicEntry, MemoryPubSubError> {
        if topic.trim().is_empty() {
            return Err(MemoryPubSubError::InvalidTopic);
        }
        let guard = self.topics.read();
        let entry = guard
            .get(topic)
            .ok_or_else(|| MemoryPubSubError::NotFound {
                topic: topic.to_string(),
            })?;
        if entry.kind != kind {
            return Err(MemoryPubSubError::TopicKindMismatch {
                topic: topic.to_string(),
                expected: entry.kind,
                actual: kind,
            });
        }
        Ok(entry.clone())
    }

    pub fn open_subscribe_bytes(
        &self,
        topic: &str,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        Ok(self
            .get_existing_topic(topic, MemoryTopicKind::Bytes)?
            .sender
            .subscribe())
    }

    pub fn open_subscribe_collection(
        &self,
        topic: &str,
    ) -> Result<broadcast::Receiver<MemoryData>, MemoryPubSubError> {
        Ok(self
            .get_existing_topic(topic, MemoryTopicKind::Collection)?
            .sender
            .subscribe())
    }

    pub fn topic_capacity(&self, topic: &str) -> Option<usize> {
        let guard = self.topics.read();
        guard.get(topic).map(|entry| entry.capacity)
    }

    pub fn topic_kind(&self, topic: &str) -> Option<MemoryTopicKind> {
        let guard = self.topics.read();
        guard.get(topic).map(|entry| entry.kind)
    }

    pub fn subscriber_count(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
    ) -> Result<usize, MemoryPubSubError> {
        let entry = self.get_existing_topic(topic, kind)?;
        Ok(entry.sender.receiver_count())
    }

    pub async fn wait_for_subscribers(
        &self,
        topic: &str,
        kind: MemoryTopicKind,
        min: usize,
        timeout: Duration,
    ) -> Result<(), MemoryPubSubError> {
        if min == 0 {
            return Ok(());
        }

        let entry = self.get_existing_topic(topic, kind)?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let receivers = entry.sender.receiver_count();
            if receivers >= min {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(MemoryPubSubError::TimeoutWaitingForSubscribers {
                    topic: topic.to_string(),
                    expected: min,
                    actual: receivers,
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Default for MemoryPubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::batch_from_columns_simple;
    use datatypes::Value;
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tokio::time::timeout;

    async fn recv_labels_bytes(
        rx: &mut broadcast::Receiver<MemoryData>,
        count: usize,
    ) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for _ in 0..count {
            let item = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("receive timeout")
                .expect("receive item");
            match item {
                MemoryData::Bytes(bytes) => out.insert(format!(
                    "bytes:{}",
                    std::str::from_utf8(bytes.as_ref()).expect("utf8 bytes")
                )),
                MemoryData::Collection(_) => panic!("bytes topic received collection"),
            };
        }
        out
    }

    fn expect_single_publisher_payloads_bytes() -> (Vec<Bytes>, BTreeSet<String>) {
        let mut items = Vec::new();
        let mut expected = BTreeSet::new();
        for idx in 0..5 {
            let label = format!("p1:b{idx}");
            expected.insert(format!("bytes:{label}"));
            items.push(Bytes::from(label));
        }
        (items, expected)
    }

    fn expect_dual_publisher_payloads_bytes() -> (Vec<Bytes>, Vec<Bytes>, BTreeSet<String>) {
        let (p1_items, mut expected) = expect_single_publisher_payloads_bytes();

        let mut p2_items = Vec::new();
        for idx in 0..3 {
            let label = format!("p2:b{idx}");
            expected.insert(format!("bytes:{label}"));
            p2_items.push(Bytes::from(label));
        }

        (p1_items, p2_items, expected)
    }

    fn collection_with_rows(rows: i64) -> SharedCollection {
        let values = (0..rows).map(Value::Int64).collect::<Vec<_>>();
        let batch =
            batch_from_columns_simple(vec![("memory".to_string(), "v".to_string(), values)])
                .expect("build batch");
        SharedCollection::from_box(Box::new(batch))
    }

    async fn recv_labels_collection(
        rx: &mut broadcast::Receiver<MemoryData>,
        count: usize,
    ) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for _ in 0..count {
            let item = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("receive timeout")
                .expect("receive item");
            match item {
                MemoryData::Collection(collection) => {
                    out.insert(format!("collection:rows={}", collection.num_rows()));
                }
                MemoryData::Bytes(_) => panic!("collection topic received bytes"),
            };
        }
        out
    }

    fn expect_single_publisher_payloads_collection() -> (Vec<SharedCollection>, BTreeSet<String>) {
        let mut items = Vec::new();
        let mut expected = BTreeSet::new();
        for rows in [1i64, 2, 3, 4, 5] {
            expected.insert(format!("collection:rows={rows}"));
            items.push(collection_with_rows(rows));
        }
        (items, expected)
    }

    fn expect_dual_publisher_payloads_collection() -> (
        Vec<SharedCollection>,
        Vec<SharedCollection>,
        BTreeSet<String>,
    ) {
        let (p1_items, mut expected) = expect_single_publisher_payloads_collection();

        let mut p2_items = Vec::new();
        for rows in [10i64, 11, 12] {
            expected.insert(format!("collection:rows={rows}"));
            p2_items.push(collection_with_rows(rows));
        }

        (p1_items, p2_items, expected)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_single_pub_single_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "single_pub_single_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx = registry.open_subscribe_bytes(topic).expect("subscribe");
        let tx = registry.open_publisher_bytes(topic).expect("publisher");

        let (items, expected) = expect_single_publisher_payloads_bytes();
        for item in items {
            tx.publish_bytes(item).expect("send");
        }

        let got = recv_labels_bytes(&mut rx, expected.len()).await;
        assert_eq!(got, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_two_pub_one_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "two_pub_one_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx = registry.open_subscribe_bytes(topic).expect("subscribe");
        let tx1 = registry.open_publisher_bytes(topic).expect("publisher1");
        let tx2 = registry.open_publisher_bytes(topic).expect("publisher2");

        let (p1_items, p2_items, expected) = expect_dual_publisher_payloads_bytes();

        let t1 = tokio::spawn(async move {
            for item in p1_items {
                tx1.publish_bytes(item).expect("send p1");
            }
        });
        let t2 = tokio::spawn(async move {
            for item in p2_items {
                tx2.publish_bytes(item).expect("send p2");
            }
        });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("publisher1 task");
        r2.expect("publisher2 task");

        let got = recv_labels_bytes(&mut rx, expected.len()).await;
        assert_eq!(got, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_one_pub_two_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "one_pub_two_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx1 = registry.open_subscribe_bytes(topic).expect("subscribe1");
        let mut rx2 = registry.open_subscribe_bytes(topic).expect("subscribe2");
        let tx = registry.open_publisher_bytes(topic).expect("publisher");

        let (items, expected) = expect_single_publisher_payloads_bytes();
        for item in items {
            tx.publish_bytes(item).expect("send");
        }

        let got1 = recv_labels_bytes(&mut rx1, expected.len()).await;
        let got2 = recv_labels_bytes(&mut rx2, expected.len()).await;
        assert_eq!(got1, expected);
        assert_eq!(got2, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_two_pub_two_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "two_pub_two_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx1 = registry.open_subscribe_bytes(topic).expect("subscribe1");
        let mut rx2 = registry.open_subscribe_bytes(topic).expect("subscribe2");
        let tx1 = registry.open_publisher_bytes(topic).expect("publisher1");
        let tx2 = registry.open_publisher_bytes(topic).expect("publisher2");

        let (p1_items, p2_items, expected) = expect_dual_publisher_payloads_bytes();

        let t1 = tokio::spawn(async move {
            for item in p1_items {
                tx1.publish_bytes(item).expect("send p1");
            }
        });
        let t2 = tokio::spawn(async move {
            for item in p2_items {
                tx2.publish_bytes(item).expect("send p2");
            }
        });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("publisher1 task");
        r2.expect("publisher2 task");

        let got1 = recv_labels_bytes(&mut rx1, expected.len()).await;
        let got2 = recv_labels_bytes(&mut rx2, expected.len()).await;
        assert_eq!(got1, expected);
        assert_eq!(got2, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_collection_single_pub_single_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "collection_single_pub_single_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx = registry
            .open_subscribe_collection(topic)
            .expect("subscribe");
        let tx = registry
            .open_publisher_collection(topic)
            .expect("publisher");

        let (items, expected) = expect_single_publisher_payloads_collection();
        for item in items {
            tx.publish_collection(item).expect("publish");
        }

        let got = recv_labels_collection(&mut rx, expected.len()).await;
        assert_eq!(got, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_collection_two_pub_one_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "collection_two_pub_one_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx = registry
            .open_subscribe_collection(topic)
            .expect("subscribe");
        let tx1 = registry
            .open_publisher_collection(topic)
            .expect("publisher1");
        let tx2 = registry
            .open_publisher_collection(topic)
            .expect("publisher2");

        let (p1_items, p2_items, expected) = expect_dual_publisher_payloads_collection();

        let t1 = tokio::spawn(async move {
            for item in p1_items {
                tx1.publish_collection(item).expect("publish p1");
            }
        });
        let t2 = tokio::spawn(async move {
            for item in p2_items {
                tx2.publish_collection(item).expect("publish p2");
            }
        });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("publisher1 task");
        r2.expect("publisher2 task");

        let got = recv_labels_collection(&mut rx, expected.len()).await;
        assert_eq!(got, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_collection_one_pub_two_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "collection_one_pub_two_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx1 = registry
            .open_subscribe_collection(topic)
            .expect("subscribe1");
        let mut rx2 = registry
            .open_subscribe_collection(topic)
            .expect("subscribe2");
        let tx = registry
            .open_publisher_collection(topic)
            .expect("publisher");

        let (items, expected) = expect_single_publisher_payloads_collection();
        for item in items {
            tx.publish_collection(item).expect("publish");
        }

        let got1 = recv_labels_collection(&mut rx1, expected.len()).await;
        let got2 = recv_labels_collection(&mut rx2, expected.len()).await;
        assert_eq!(got1, expected);
        assert_eq!(got2, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_collection_two_pub_two_sub() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "collection_two_pub_two_sub";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Collection,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let mut rx1 = registry
            .open_subscribe_collection(topic)
            .expect("subscribe1");
        let mut rx2 = registry
            .open_subscribe_collection(topic)
            .expect("subscribe2");
        let tx1 = registry
            .open_publisher_collection(topic)
            .expect("publisher1");
        let tx2 = registry
            .open_publisher_collection(topic)
            .expect("publisher2");

        let (p1_items, p2_items, expected) = expect_dual_publisher_payloads_collection();

        let t1 = tokio::spawn(async move {
            for item in p1_items {
                tx1.publish_collection(item).expect("publish p1");
            }
        });
        let t2 = tokio::spawn(async move {
            for item in p2_items {
                tx2.publish_collection(item).expect("publish p2");
            }
        });
        let (r1, r2) = tokio::join!(t1, t2);
        r1.expect("publisher1 task");
        r2.expect("publisher2 task");

        let got1 = recv_labels_collection(&mut rx1, expected.len()).await;
        let got2 = recv_labels_collection(&mut rx2, expected.len()).await;
        assert_eq!(got1, expected);
        assert_eq!(got2, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_rejects_mixed_topic_kind() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "reject_mixed_topic_kind";

        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let _ = registry
            .open_publisher_bytes(topic)
            .expect("bytes publisher");
        let err = registry
            .open_publisher_collection(topic)
            .expect_err("mismatch should error");
        assert!(matches!(err, MemoryPubSubError::TopicKindMismatch { .. }));

        let err = registry
            .open_subscribe_collection(topic)
            .expect_err("mismatch should error");
        assert!(matches!(err, MemoryPubSubError::TopicKindMismatch { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_publish_without_subscribers_is_ok() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "publish_without_subscribers_is_ok";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");
        let tx = registry.open_publisher_bytes(topic).expect("publisher");
        let delivered = tx
            .publish_bytes(Bytes::from_static(b"hello"))
            .expect("publish");
        assert_eq!(delivered, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_wait_for_subscribers_succeeds() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "wait_for_subscribers_succeeds";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");

        let registry_clone = registry.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _rx = registry_clone
                .open_subscribe_bytes(topic)
                .expect("subscribe");
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        registry
            .wait_for_subscribers(topic, MemoryTopicKind::Bytes, 1, Duration::from_secs(1))
            .await
            .expect("wait");
        handle.await.expect("subscriber task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_pubsub_wait_for_subscribers_timeout() {
        let registry = MemoryPubSubRegistry::new();
        let topic = "wait_for_subscribers_timeout";
        registry
            .declare_topic(
                topic,
                MemoryTopicKind::Bytes,
                DEFAULT_MEMORY_PUBSUB_CAPACITY,
            )
            .expect("declare");

        let err = registry
            .wait_for_subscribers(topic, MemoryTopicKind::Bytes, 1, Duration::from_millis(50))
            .await
            .expect_err("expected timeout");
        assert!(matches!(
            err,
            MemoryPubSubError::TimeoutWaitingForSubscribers { .. }
        ));
    }
}
